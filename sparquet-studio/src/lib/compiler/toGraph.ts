/**
 * Pipeline JSON → graph.
 *
 * Input is arbitrary JSON — hand-written, pasted, or produced by a model — so
 * every read is guarded: malformed values become issues, never exceptions.
 */

import type { OnFailureMode, SparkSettings } from '@/types/pipeline'
import { ruleCodes } from './ruleCode'
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
  ValidationSinkRole,
  JobSettings,
} from '@/types/studio'
import { DEFAULT_VALIDATION_POLICY, HANDLE, VALIDATION_SINK_ROLES } from '@/types/studio'
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
    if (hasSideInput && (key === 'input' || key === 'with' || key === 'with_transformations'))
      continue
    params[key] = jsonClone(value)
  }

  const node = makeTransformNode(type, params, asText(entry.skip_if_false) || undefined)
  ctx.nodes.push(node)

  if (hasSideInput) {
    // `with` was this key's old name. The framework refuses it, so a job still carrying
    // it is imported under the current name and the rename is reported: opening the file
    // is how the author finds out, and saving it writes `input`.
    if (entry.input === undefined && entry.with !== undefined)
      ctx.issues.warning(
        `${label} still uses \`with\` for its second source; it was renamed to \`input\` and the framework no longer reads \`with\`.`,
      )
    const sideData = readSourceData(
      entry.input ?? entry.with,
      ctx.issues,
      `The \`${type}\` second source`,
    )
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
 * Splits the `validations` block in three: its rules become one node each, its
 * three written datasets become standalone quality destinations, and `on_failure` —
 * run policy, not data — lands in the job settings. The inverse of
 * `buildValidations`.
 */
function importValidations(
  value: unknown,
  issues: Issues,
): {
  policy: ValidationPolicy | null
  rules: JsonRecord[]
  sideSinks: Map<ValidationSinkRole, unknown>
} {
  const sideSinks = new Map<ValidationSinkRole, unknown>()
  if (!isRecord(value)) {
    issues.warning('The "validations" block is not an object and was skipped.')
    return { policy: null, rules: [], sideSinks }
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

  if (isRecord(value.report)) sideSinks.set('report', value.report)
  if (isRecord(value.outputs)) {
    for (const [key, entry] of Object.entries(value.outputs)) {
      // The framework itself logs "chave desconhecida (use 'valid'/'invalid')" and
      // writes nothing, so an unknown key is dead config — say so instead of
      // drawing a box that can never receive a row.
      if (key !== 'valid' && key !== 'invalid') {
        issues.warning(
          `"validations.outputs.${key}" is not a quarantine key and was dropped.`,
          { hint: 'The framework only routes rows to "valid" and "invalid".' },
        )
        continue
      }
      if (!isRecord(entry)) {
        issues.warning(`"validations.outputs.${key}" is not an object and was dropped.`)
        continue
      }
      sideSinks.set(key, entry)
    }
  }

  return { policy, rules, sideSinks }
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
    const { policy, rules, sideSinks } = importValidations(pipeline.validations, issues)
    if (policy) settings.validations = policy
    let ruleCount = 0
    const ruleNodes: StudioNode[] = []
    for (const rule of rules) {
      const node = makeValidationNode(rule)
      ctx.nodes.push(node)
      ctx.edges.push(makeEdge(tail.id, node.id))
      ruleNodes.push(node)
      tail = node
      ruleCount += 1
    }

    // The three datasets become STANDALONE nodes: the block is job-scoped, so they
    // belong to no particular rule and take no connection. The main chain carries
    // on untouched — `tail` is deliberately left pointing at the last rule.
    for (const role of VALIDATION_SINK_ROLES) {
      const raw = sideSinks.get(role)
      if (raw === undefined) continue
      const label = `The validations "${role === 'report' ? 'report' : `outputs.${role}`}"`
      if (ruleCount === 0) {
        // No rules means no `validations` key at all, so the framework never wrote
        // this dataset either — drawing a box for it would promise a write that
        // cannot happen.
        issues.warning(`${label} destination has no validation rule and was dropped.`)
        continue
      }
      // `annotate`/`rules` live only on the rejected side; `readSinkData` knows the
      // generic destination keys, so these two are read here.
      const quarantine = isRecord(raw) && role === 'invalid' ? raw : null
      const annotate =
        quarantine && typeof quarantine.annotate === 'string' ? quarantine.annotate : undefined
      const dqRules = Array.isArray(quarantine?.rules)
        ? quarantine.rules.filter((code): code is string => typeof code === 'string')
        : undefined
      // A scope drawn as edges is a scope the author can see and change. Each rule node
      // is matched against the scope by ALL the codes it answers to — a `targets` rule
      // produces one per target, and an edge means the whole node, so a scope naming
      // only some of its targets is NOT that edge. Those codes stay on the node
      // verbatim, which keeps the round-trip exact and gives the linter something to
      // point at; the same holds for codes no rule on this canvas answers to.
      const scope = new Set(dqRules ?? [])
      const owners = ruleNodes.filter((candidate) => {
        const codes = ruleCodes(candidate).filter((code) => code !== '')
        return codes.length > 0 && codes.every((code) => scope.has(code))
      })
      const claimed = new Set(owners.flatMap((owner) => ruleCodes(owner)))
      const unmatched = (dqRules ?? []).filter((code) => !claimed.has(code))
      const sink = makeSinkNode({
        ...readSinkData(raw, issues, label),
        dqRole: role,
        ...(annotate !== undefined ? { annotate } : {}),
        ...(unmatched.length > 0 ? { dqRules: unmatched } : {}),
      })
      ctx.nodes.push(sink)
      if (owners.length > 0) {
        for (const owner of owners) {
          ctx.edges.push(makeEdge(owner.id, sink.id, HANDLE.inScope))
        }
      } else {
        // Nothing scopes this dataset, so anchor it to the last rule instead: the box
        // is written BY the validations, and a canvas that leaves it floating makes it
        // look like a destination someone forgot to wire. The compiler reads no scope
        // from the report or the valid side, and on the quarantine this single link
        // means "every rule", which is exactly what no scope means.
        const last = ruleNodes[ruleNodes.length - 1]
        if (last) ctx.edges.push(makeEdge(last.id, sink.id))
      }
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
