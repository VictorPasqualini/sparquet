/**
 * Pipeline JSON → graph.
 *
 * Input is arbitrary JSON — hand-written, pasted, or produced by a model — so
 * every read is guarded: malformed values become issues, never exceptions.
 */

import type { OnFailureMode, OutputSpec, SparkSettings } from '@/types/pipeline'
import { ON_FAILURE_MODES } from '@/types/pipeline'
import type {
  IssueSeverity,
  SinkNode,
  SinkNodeData,
  SourceNode,
  SourceNodeData,
  StudioEdge,
  StudioGraph,
  StudioNode,
  TransformNode,
  ValidationIssue,
  ValidationNode,
  ValidationPolicy,
  JobSettings,
} from '@/types/studio'
import { DEFAULT_VALIDATION_POLICY, HANDLE } from '@/types/studio'
import { makeEdge, newNodeId } from '@/lib/compiler/graph'
import { autoLayout } from '@/lib/compiler/layout'

export interface DecompileResult {
  graph: StudioGraph
  settings: JobSettings
  issues: ValidationIssue[]
}

type JsonRecord = Record<string, unknown>

const SIDE_INPUT_TYPES = new Set(['join', 'union'])
const SUB_PIPELINE_TYPES = new Set(['join'])
const MAX_CLONE_DEPTH = 32

/* --------------------------------------------------------------- helpers */

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function jsonClone(value: unknown, depth = 0): unknown {
  if (depth > MAX_CLONE_DEPTH) return null
  if (value === null) return null
  if (Array.isArray(value)) return value.map((item) => jsonClone(item, depth + 1))
  if (isRecord(value)) {
    const out: JsonRecord = {}
    for (const [key, item] of Object.entries(value)) {
      if (item === undefined || typeof item === 'function' || typeof item === 'symbol') continue
      out[key] = jsonClone(item, depth + 1)
    }
    return out
  }
  if (typeof value === 'function' || typeof value === 'symbol' || value === undefined)
    return null
  return value
}

function asText(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

interface IssueOptions {
  nodeId?: string
  field?: string
  hint?: string
}

function createIssues() {
  const items: ValidationIssue[] = []
  let seq = 0
  const push = (severity: IssueSeverity, message: string, options: IssueOptions = {}) => {
    seq += 1
    items.push({ id: `import-${seq}`, severity, message, ...options })
  }
  return {
    items,
    error: (message: string, options?: IssueOptions) => push('error', message, options),
    warning: (message: string, options?: IssueOptions) => push('warning', message, options),
    info: (message: string, options?: IssueOptions) => push('info', message, options),
  }
}

type Issues = ReturnType<typeof createIssues>

function emptySettings(): JobSettings {
  return { pipelineName: '', description: '', spark: {} }
}

function readSpark(value: unknown): SparkSettings {
  const spark: SparkSettings = {}
  if (!isRecord(value)) return spark
  if (asText(value.app_name)) spark.app_name = asText(value.app_name)
  if (asText(value.master)) spark.master = asText(value.master)
  if (isRecord(value.configs)) {
    const configs: Record<string, string> = {}
    for (const [key, item] of Object.entries(value.configs)) {
      if (typeof item === 'string') configs[key] = item
      else if (typeof item === 'number' || typeof item === 'boolean')
        configs[key] = String(item)
    }
    if (Object.keys(configs).length > 0) spark.configs = configs
  }
  return spark
}

/* ------------------------------------------------------------- factories */

function at(): { x: number; y: number } {
  return { x: 0, y: 0 }
}

function makeSourceNode(data: SourceNodeData): SourceNode {
  return { id: newNodeId('source'), type: 'source', position: at(), data }
}

function makeTransformNode(
  transform: string,
  params: JsonRecord,
  skipIfFalse?: string,
): TransformNode {
  return {
    id: newNodeId('transform'),
    type: 'transform',
    position: at(),
    data: { kind: 'transform', transform, params, ...(skipIfFalse ? { skipIfFalse } : {}) },
  }
}

function makeValidationNode(rule: JsonRecord): ValidationNode {
  const params: JsonRecord = {}
  for (const [key, value] of Object.entries(rule)) {
    if (key === 'type') continue
    params[key] = jsonClone(value)
  }
  return {
    id: newNodeId('validation'),
    type: 'validation',
    position: at(),
    data: { kind: 'validation', validator: asText(rule.type), params },
  }
}

function makeSinkNode(data: SinkNodeData): SinkNode {
  return { id: newNodeId('sink'), type: 'sink', position: at(), data }
}

function readSourceData(value: unknown, issues: Issues, label: string): SourceNodeData {
  if (!isRecord(value)) {
    issues.error(`${label} is missing or is not an object.`)
    return { kind: 'source', format: '', path: '', options: {} }
  }
  const format = asText(value.format)
  const path = asText(value.path)
  if (!format) issues.warning(`${label} has no format.`)
  if (!path) issues.warning(`${label} has no path.`)
  const options = isRecord(value.options) ? (jsonClone(value.options) as JsonRecord) : {}
  return { kind: 'source', format, path, options }
}

function readSinkData(value: unknown, issues: Issues, label: string): SinkNodeData {
  if (!isRecord(value)) {
    issues.error(`${label} is not an object.`)
    return {
      kind: 'sink',
      format: '',
      path: '',
      mode: '',
      partitionBy: [],
      columns: null,
      options: {},
    }
  }
  const format = asText(value.format)
  const path = asText(value.path)
  if (!format) issues.warning(`${label} has no format.`)
  if (!path) issues.warning(`${label} has no path.`)

  const partitionBy = Array.isArray(value.partition_by)
    ? value.partition_by.filter((entry): entry is string => typeof entry === 'string')
    : []
  const columns = Array.isArray(value.columns)
    ? value.columns.filter((entry): entry is string => typeof entry === 'string')
    : null
  const options = isRecord(value.options) ? (jsonClone(value.options) as JsonRecord) : {}

  return {
    kind: 'sink',
    format,
    path,
    mode: asText(value.mode),
    partitionBy,
    columns,
    options,
  }
}

/* ---------------------------------------------------------------- import */

interface BuildContext {
  nodes: StudioNode[]
  edges: StudioEdge[]
  issues: Issues
}

/**
 * Turns one `transformations` entry into a node, wiring the second input of a
 * join/union from its own source chain. Returns `null` for entries that carry
 * nothing importable.
 */
function importTransformation(
  entry: unknown,
  ctx: BuildContext,
  label: string,
): TransformNode | null {
  if (!isRecord(entry)) {
    ctx.issues.warning(`${label} is not an object and was skipped.`)
    return null
  }

  if ('$include' in entry) {
    const target = entry.$include
    if (typeof target !== 'string') {
      ctx.issues.warning(`${label} has an "$include" that is not a file path.`)
    }
    const node = makeTransformNode('$include', { $include: asText(target) })
    ctx.nodes.push(node)
    return node
  }

  const type = asText(entry.type)
  if (!type) {
    ctx.issues.warning(`${label} has no "type" and was skipped.`)
    return null
  }

  const hasSideInput = SIDE_INPUT_TYPES.has(type)
  const params: JsonRecord = {}
  for (const [key, value] of Object.entries(entry)) {
    if (key === 'type' || key === 'skip_if_false') continue
    if (hasSideInput && (key === 'with' || key === 'with_transformations')) continue
    params[key] = jsonClone(value)
  }

  const node = makeTransformNode(type, params, asText(entry.skip_if_false) || undefined)
  ctx.nodes.push(node)

  if (hasSideInput) {
    const sideData = readSourceData(entry.with, ctx.issues, `The \`${type}\` second source`)
    const sideSource = makeSourceNode(sideData)
    ctx.nodes.push(sideSource)

    let tail: StudioNode = sideSource
    const sideChain = Array.isArray(entry.with_transformations)
      ? entry.with_transformations
      : []
    if (sideChain.length > 0 && !SUB_PIPELINE_TYPES.has(type)) {
      // Materializing them would build a graph the compiler rejects outright.
      ctx.issues.warning(`A \`${type}\` never applies "with_transformations".`, {
        nodeId: node.id,
        hint: 'The framework reads the second source as-is, so they were left out.',
      })
    } else {
      sideChain.forEach((child, index) => {
        const childNode = importTransformation(
          child,
          ctx,
          `\`${type}\` sub-transformation #${index + 1}`,
        )
        if (!childNode) return
        ctx.edges.push(makeEdge(tail.id, childNode.id))
        tail = childNode
      })
    }
    ctx.edges.push(makeEdge(tail.id, node.id, HANDLE.inRight))
  }

  return node
}

/**
 * Splits the `validations` block in two: its rules become one node each (the
 * inverse of `buildValidations`), while `on_failure` / `report` / `outputs` are
 * job-wide policy and land in the job settings.
 */
function importValidations(
  value: unknown,
  issues: Issues,
): { policy: ValidationPolicy | null; rules: JsonRecord[] } {
  if (!isRecord(value)) {
    issues.warning('The "validations" block is not an object and was skipped.')
    return { policy: null, rules: [] }
  }

  const rules: JsonRecord[] = []
  const rawRules = Array.isArray(value.rules) ? value.rules : []
  rawRules.forEach((rule, index) => {
    if (!isRecord(rule) || !asText(rule.type)) {
      issues.warning(`Validation rule #${index + 1} has no "type" and was skipped.`)
      return
    }
    rules.push(rule)
  })

  if (rules.length === 0) {
    issues.warning('The "validations" block has no usable rules and was skipped.')
  }

  const rawMode = asText(value.on_failure)
  const onFailure: OnFailureMode = ON_FAILURE_MODES.includes(rawMode as OnFailureMode)
    ? (rawMode as OnFailureMode)
    : DEFAULT_VALIDATION_POLICY.onFailure
  if (rawMode && onFailure !== rawMode) {
    issues.warning(`Unknown "on_failure" mode "${rawMode}"; using "fail".`)
  }

  const policy: ValidationPolicy = { onFailure }
  if (isRecord(value.report)) {
    policy.report = jsonClone(value.report) as unknown as OutputSpec
  }
  if (isRecord(value.outputs)) {
    policy.outputs = jsonClone(value.outputs) as unknown as Record<string, OutputSpec>
  }

  return { policy, rules }
}

export function pipelineToGraph(pipeline: unknown): DecompileResult {
  const issues = createIssues()

  if (!isRecord(pipeline)) {
    issues.error('The pipeline JSON must be an object.')
    return { graph: { nodes: [], edges: [] }, settings: emptySettings(), issues: issues.items }
  }

  const settings: JobSettings = {
    pipelineName: asText(pipeline.name),
    description: asText(pipeline.description),
    spark: readSpark(pipeline.spark),
  }
  if (!settings.pipelineName) issues.warning('The pipeline has no "name".', { field: 'name' })

  const ctx: BuildContext = { nodes: [], edges: [], issues }

  const source = makeSourceNode(readSourceData(pipeline.input, issues, 'The pipeline "input"'))
  ctx.nodes.push(source)

  let tail: StudioNode = source
  const transformations = Array.isArray(pipeline.transformations)
    ? pipeline.transformations
    : []
  if (pipeline.transformations !== undefined && !Array.isArray(pipeline.transformations)) {
    issues.warning('"transformations" must be an array; it was ignored.')
  }
  transformations.forEach((entry, index) => {
    const node = importTransformation(entry, ctx, `Transformation #${index + 1}`)
    if (!node) return
    ctx.edges.push(makeEdge(tail.id, node.id))
    tail = node
  })

  if (pipeline.validations !== undefined) {
    const { policy, rules } = importValidations(pipeline.validations, issues)
    if (policy) settings.validations = policy
    for (const rule of rules) {
      const node = makeValidationNode(rule)
      ctx.nodes.push(node)
      ctx.edges.push(makeEdge(tail.id, node.id))
      tail = node
    }
  }

  const rawOutputs = Array.isArray(pipeline.outputs) ? pipeline.outputs : []
  const outputs: unknown[] =
    rawOutputs.length > 0 ? rawOutputs : pipeline.output !== undefined ? [pipeline.output] : []

  if (outputs.length === 0) {
    issues.error('The pipeline has no "output" or "outputs".', {
      hint: 'Add a destination so the pipeline can be run.',
    })
  }

  outputs.forEach((entry, index) => {
    const label = outputs.length > 1 ? `Output #${index + 1}` : 'The pipeline "output"'
    let localTail = tail
    if (isRecord(entry) && Array.isArray(entry.transformations)) {
      entry.transformations.forEach((child, childIndex) => {
        const childNode = importTransformation(
          child,
          ctx,
          `${label} transformation #${childIndex + 1}`,
        )
        if (!childNode) return
        ctx.edges.push(makeEdge(localTail.id, childNode.id))
        localTail = childNode
      })
    }
    const sink = makeSinkNode(readSinkData(entry, issues, label))
    ctx.nodes.push(sink)
    ctx.edges.push(makeEdge(localTail.id, sink.id))
  })

  const graph = autoLayout({ nodes: ctx.nodes, edges: ctx.edges })
  return { graph, settings, issues: issues.items }
}
