/**
 * Graph → pipeline JSON.
 *
 * The graph is the source of truth: `input` comes from the source node every
 * chain resolves to, the main `transformations` are the prefix every destination
 * shares, and whatever a destination does on its own becomes that output's own
 * `transformations`.
 */

import { getTransformation } from '@/catalog'
import type {
  InputSpec,
  OutputSpec,
  PipelineSpec,
  SparkSettings,
  TransformationSpec,
  TransformationSpecBase,
  ValidationRuleSpec,
  ValidationsSpec,
} from '@/types/pipeline'
import type {
  IssueSeverity,
  ParamDefinition,
  SinkNode,
  SourceNode,
  StudioGraph,
  StudioNode,
  TransformNode,
  ValidationIssue,
  ValidationsNode,
  WorkflowSettings,
} from '@/types/studio'
import {
  chainToSink,
  isCompilable,
  isDisabled,
  isNoteNode,
  isSinkNode,
  isSourceNode,
  isTransformNode,
  isValidationsNode,
  longestCommonPrefix,
  sideParent,
} from '@/lib/compiler/graph'

export interface CompileResult {
  /** `null` when a structural error makes the workflow impossible to express. */
  pipeline: PipelineSpec | null
  issues: ValidationIssue[]
}

type JsonRecord = Record<string, unknown>

/** Transforms whose right-hand side is a second incoming connection. */
const SIDE_INPUT_TYPES = new Set(['join', 'union'])

/** Only `join` forwards the right-hand chain; `union` reads its source as-is. */
const SUB_PIPELINE_TYPES = new Set(['join'])

const PIPELINE_KEY_ORDER = [
  'name',
  'description',
  'spark',
  'input',
  'transformations',
  'validations',
  'output',
  'outputs',
]
const INPUT_KEY_ORDER = ['format', 'path', 'options']
const OUTPUT_KEY_ORDER = [
  'format',
  'path',
  'mode',
  'partition_by',
  'columns',
  'options',
  'transformations',
]
const VALIDATIONS_KEY_ORDER = ['on_failure', 'report', 'rules']
/** `with` / `with_transformations` lead because they describe the second input. */
const TRANSFORM_KEY_ORDER = ['type', 'skip_if_false', 'with', 'with_transformations']

const MAX_CLONE_DEPTH = 32

/* --------------------------------------------------------------- helpers */

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/** JSON-safe deep copy: nothing from the store leaks into the compiled config. */
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

function isBlank(value: unknown): boolean {
  if (value === undefined || value === null) return true
  if (typeof value === 'string') return value.trim() === ''
  if (Array.isArray(value)) return value.length === 0
  if (isRecord(value)) return Object.keys(value).length === 0
  return false
}

/**
 * A param carries no user intent only when it is null/undefined or a blank string.
 * Unlike `isBlank`, an empty map/list is NOT unset: for structural params
 * (`struct.fields`, `group_by.agg`, `cast.columns`, `with_column.columns`,
 * `rename.mappings`, …) an empty container is meaningful and must round-trip.
 * Pruning it emits JSON the framework rejects (e.g. `with_column` with no `columns`
 * falls into the single-column branch and raises ValueError; `struct` without
 * `fields` raises KeyError).
 */
function isUnset(value: unknown): boolean {
  if (value === undefined || value === null) return true
  if (typeof value === 'string') return value.trim() === ''
  return false
}

function orderKeys(record: JsonRecord, preferred: readonly string[]): JsonRecord {
  const out: JsonRecord = {}
  for (const key of preferred) {
    if (Object.prototype.hasOwnProperty.call(record, key)) out[key] = record[key]
  }
  for (const key of Object.keys(record)) {
    if (!Object.prototype.hasOwnProperty.call(out, key)) out[key] = record[key]
  }
  return out
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
    items.push({ id: `compile-${seq}`, severity, message, ...options })
  }
  return {
    items,
    error: (message: string, options?: IssueOptions) => push('error', message, options),
    warning: (message: string, options?: IssueOptions) => push('warning', message, options),
    info: (message: string, options?: IssueOptions) => push('info', message, options),
  }
}

type Issues = ReturnType<typeof createIssues>

interface CompileContext {
  graph: StudioGraph
  issues: Issues
  /** Every node that made it into the JSON — the rest is reported as orphaned. */
  used: Set<string>
  /** Guards against a join whose sub-chain loops back into itself. */
  stack: Set<string>
}

/* ------------------------------------------------------------- fragments */

function buildInput(node: SourceNode, issues: Issues): InputSpec {
  const { format, path, options } = node.data
  if (isBlank(format)) {
    issues.warning('This source has no format.', { nodeId: node.id, field: 'format' })
  }
  if (isBlank(path)) {
    issues.warning('This source has no path.', { nodeId: node.id, field: 'path' })
  }
  const spec: InputSpec = { format, path }
  if (!isBlank(options)) spec.options = jsonClone(options) as JsonRecord
  return spec
}

function buildOutput(
  node: SinkNode,
  transformations: TransformationSpec[],
  issues: Issues,
): OutputSpec {
  const { format, path, mode, partitionBy, columns, options } = node.data
  if (isBlank(format)) {
    issues.warning('This destination has no format.', { nodeId: node.id, field: 'format' })
  }
  if (isBlank(path)) {
    issues.warning('This destination has no path.', { nodeId: node.id, field: 'path' })
  }
  const spec: OutputSpec = { format, path }
  if (!isBlank(mode)) spec.mode = mode
  if (!isBlank(partitionBy)) spec.partition_by = [...partitionBy]
  if (columns !== null && !isBlank(columns)) spec.columns = [...columns]
  if (!isBlank(options)) spec.options = jsonClone(options) as JsonRecord
  if (transformations.length > 0) spec.transformations = transformations
  return spec
}

function buildReport(report: OutputSpec): OutputSpec | null {
  if (!isRecord(report) || isBlank(report.format) || isBlank(report.path)) return null
  const cloned = jsonClone(report)
  if (!isRecord(cloned)) return null
  const pruned: JsonRecord = {}
  for (const [key, value] of Object.entries(cloned)) {
    if (isBlank(value)) continue
    pruned[key] = value
  }
  return orderKeys(pruned, OUTPUT_KEY_ORDER) as unknown as OutputSpec
}

function buildValidations(node: ValidationsNode, issues: Issues): ValidationsSpec | null {
  const rules: ValidationRuleSpec[] = []
  for (const rule of node.data.rules ?? []) {
    if (!isRecord(rule) || typeof rule.type !== 'string' || rule.type.trim() === '') {
      issues.warning('A validation rule without a type was left out.', { nodeId: node.id })
      continue
    }
    const cloned = jsonClone(rule) as JsonRecord
    const pruned: JsonRecord = {}
    for (const [key, value] of Object.entries(cloned)) {
      if (value === undefined || value === null) continue
      pruned[key] = value
    }
    rules.push(orderKeys(pruned, ['type']) as unknown as ValidationRuleSpec)
  }

  if (rules.length === 0) {
    issues.warning('This validations node has no rules and was left out.', { nodeId: node.id })
    return null
  }

  const spec: ValidationsSpec = { on_failure: node.data.onFailure ?? 'fail' }
  const report = node.data.report ? buildReport(node.data.report) : null
  if (report) spec.report = report
  spec.rules = rules
  return spec
}

function buildSpark(spark: SparkSettings | undefined): SparkSettings | null {
  if (!spark) return null
  const out: SparkSettings = {}
  if (!isBlank(spark.app_name)) out.app_name = spark.app_name
  if (!isBlank(spark.master)) out.master = spark.master
  if (spark.configs && !isBlank(spark.configs)) out.configs = { ...spark.configs }
  return Object.keys(out).length > 0 ? out : null
}

/* -------------------------------------------------------- transformations */

function orderTransformationSpec(spec: JsonRecord): JsonRecord {
  const type = typeof spec.type === 'string' ? spec.type : ''
  const fieldKeys = getTransformation(type)?.fields.map((field) => field.key) ?? []
  return orderKeys(spec, [...TRANSFORM_KEY_ORDER, ...fieldKeys])
}

function compileSideInput(
  node: TransformNode,
  ctx: CompileContext,
): { source: InputSpec | null; transformations: TransformationSpec[] } {
  const empty = { source: null, transformations: [] }
  const kind = node.data.transform
  const parent = sideParent(ctx.graph, node.id)
  if (!parent) {
    ctx.issues.error(`A \`${kind}\` needs a second source on its right input.`, {
      nodeId: node.id,
      hint: 'Connect a source node to the lower input handle.',
    })
    return empty
  }

  const walk = chainToSink(ctx.graph, parent.id)
  if (walk.problem) {
    reportChainProblem(walk.problem.code, walk.problem.nodeId, ctx.issues)
    return empty
  }

  const chain = walk.nodes.filter(isCompilable)
  const head = chain[0]
  if (!head || !isSourceNode(head)) {
    ctx.issues.error(`The right side of this \`${kind}\` does not start at a source.`, {
      nodeId: node.id,
      hint: 'Every branch must begin with a source node.',
    })
    return empty
  }
  for (const chained of chain) ctx.used.add(chained.id)

  const rest = chain.slice(1)
  if (rest.some(isSourceNode)) {
    ctx.issues.error('The right side of this node reads from more than one source.', {
      nodeId: node.id,
    })
    return empty
  }
  if (rest.some(isValidationsNode)) {
    ctx.issues.error('Validations cannot run inside the right side of a join.', {
      nodeId: node.id,
    })
    return empty
  }

  const transformNodes = rest.filter(isTransformNode)
  if (transformNodes.length > 0 && !SUB_PIPELINE_TYPES.has(kind)) {
    ctx.issues.error(`A \`${kind}\` ignores transformations placed on its second input.`, {
      nodeId: node.id,
      hint: 'Read the second source as-is, or reshape it in a separate pipeline or view.',
    })
    return { source: buildInput(head, ctx.issues), transformations: [] }
  }

  return {
    source: buildInput(head, ctx.issues),
    transformations: transformNodes.map((child) => compileTransform(child, ctx)),
  }
}

function compileTransform(node: TransformNode, ctx: CompileContext): TransformationSpec {
  ctx.used.add(node.id)
  const kind = node.data.transform
  const params = node.data.params ?? {}

  if (kind === '$include') {
    const target = params['$include']
    const path = typeof target === 'string' ? target : ''
    if (isBlank(path)) {
      ctx.issues.warning('This include has no file path.', {
        nodeId: node.id,
        field: '$include',
      })
    }
    return { $include: path }
  }

  if (isBlank(kind)) {
    ctx.issues.error('This node has no transformation type.', { nodeId: node.id })
  }

  const spec: JsonRecord = { type: kind }
  const guard = node.data.skipIfFalse
  if (typeof guard === 'string' && guard.trim() !== '') spec.skip_if_false = guard

  const hasSideInput = SIDE_INPUT_TYPES.has(kind)
  if (hasSideInput) {
    if (ctx.stack.has(node.id)) {
      ctx.issues.error('This node feeds itself through its second input.', { nodeId: node.id })
      return orderTransformationSpec(spec) as TransformationSpecBase
    }
    ctx.stack.add(node.id)
    const side = compileSideInput(node, ctx)
    ctx.stack.delete(node.id)
    if (side.source) spec.with = side.source
    if (side.transformations.length > 0) spec.with_transformations = side.transformations
  }

  for (const [key, value] of Object.entries(params)) {
    if (key === 'type' || key === 'skip_if_false') continue
    if (hasSideInput && (key === 'with' || key === 'with_transformations')) continue
    // Prune only "unset" params (null/undefined/blank string); keep empty maps/lists
    // so structural params round-trip instead of emitting framework-breaking JSON.
    if (isUnset(value)) continue
    spec[key] = jsonClone(value)
  }

  return orderTransformationSpec(spec) as TransformationSpecBase
}

function reportChainProblem(
  code: 'multiple-parents' | 'cycle',
  nodeId: string,
  issues: Issues,
) {
  if (code === 'multiple-parents') {
    issues.error('A node can only have one incoming main connection.', {
      nodeId,
      hint: 'Merge the branches with a join or a union instead.',
    })
    return
  }
  issues.error('These nodes form a loop, so the chain never reaches a source.', { nodeId })
}

/* ---------------------------------------------------------------- compile */

export function compileGraph(
  graph: StudioGraph,
  settings: WorkflowSettings,
  params?: readonly ParamDefinition[],
): CompileResult {
  const issues = createIssues()
  const sinks = graph.nodes.filter(isSinkNode).filter((node) => !isDisabled(node))

  if (sinks.length === 0) {
    issues.error('The workflow has no destination.', {
      hint: 'Add a destination node and connect the end of the chain to it.',
    })
    return { pipeline: null, issues: issues.items }
  }

  const ctx: CompileContext = {
    graph,
    issues,
    used: new Set<string>(),
    stack: new Set<string>(),
  }
  const chains: { sink: SinkNode; middle: StudioNode[] }[] = []
  const sourceIds = new Set<string>()
  let source: SourceNode | null = null

  for (const sink of sinks) {
    const walk = chainToSink(graph, sink.id)
    if (walk.problem) {
      reportChainProblem(walk.problem.code, walk.problem.nodeId, issues)
      continue
    }

    const chain = walk.nodes.filter(isCompilable)
    const head = chain[0]
    if (!head || !isSourceNode(head)) {
      issues.error('This destination is not connected to a source.', {
        nodeId: sink.id,
        hint: 'Every chain must start at a source node.',
      })
      continue
    }
    if (chain.slice(1).some(isSourceNode)) {
      issues.error('A chain can only read from one source.', { nodeId: sink.id })
      continue
    }

    for (const node of chain) ctx.used.add(node.id)
    sourceIds.add(head.id)
    if (!source) source = head
    chains.push({ sink, middle: chain.slice(1, chain.length - 1) })
  }

  if (sourceIds.size > 1) {
    issues.error('Every destination must read from the same source.', {
      hint: 'A pipeline has one `input`; bring the second source in with a join or a union.',
    })
  }

  if (!source || chains.length === 0) {
    return { pipeline: null, issues: issues.items }
  }

  const prefix = longestCommonPrefix(
    chains.map((entry) => entry.middle),
    (a, b) => a.id === b.id,
  )
  const prefixIds = new Set(prefix.map((node) => node.id))

  const validationNodes: ValidationsNode[] = []
  const seenValidations = new Set<string>()
  for (const entry of chains) {
    for (const node of entry.middle) {
      if (!isValidationsNode(node) || seenValidations.has(node.id)) continue
      seenValidations.add(node.id)
      validationNodes.push(node)
    }
  }

  if (validationNodes.length > 1) {
    issues.error('A pipeline can only have one validations block.', {
      nodeId: validationNodes[1].id,
      hint: 'Merge the rules into a single validations node on the shared part of the chain.',
    })
  }
  const validationsNode = validationNodes[0]
  if (validationsNode && !prefixIds.has(validationsNode.id)) {
    issues.error('Validations must run before the chain splits into several destinations.', {
      nodeId: validationsNode.id,
      hint: 'Move it upstream of the branch, where every destination passes through it.',
    })
  }

  // The framework runs main `transformations`, then validations, then each
  // output's own `transformations` — so the shared prefix stops at the
  // validations node and everything past it belongs to the destinations.
  const validationsIndex = prefix.findIndex(isValidationsNode)
  const mainPrefix = validationsIndex >= 0 ? prefix.slice(0, validationsIndex + 1) : prefix

  const input = buildInput(source, issues)
  const transformations = mainPrefix
    .filter(isTransformNode)
    .map((node) => compileTransform(node, ctx))

  const outputs: OutputSpec[] = chains.map((entry) => {
    const suffix = entry.middle.slice(mainPrefix.length)
    const suffixTransforms = suffix
      .filter(isTransformNode)
      .map((node) => compileTransform(node, ctx))
    return buildOutput(entry.sink, suffixTransforms, issues)
  })

  const validations = validationsNode ? buildValidations(validationsNode, issues) : null
  if (validationsNode) ctx.used.add(validationsNode.id)

  for (const node of graph.nodes) {
    if (isNoteNode(node) || isDisabled(node) || ctx.used.has(node.id)) continue
    issues.warning('This node does not reach a destination and was left out.', {
      nodeId: node.id,
    })
  }

  if (issues.items.some((issue) => issue.severity === 'error')) {
    return { pipeline: null, issues: issues.items }
  }

  const description = settings.description?.trim() ? settings.description : undefined
  const spark = buildSpark(settings.spark)
  if (isBlank(settings.pipelineName)) {
    issues.warning('The pipeline has no name.', { field: 'name' })
  }

  const pipeline: PipelineSpec = {
    name: settings.pipelineName ?? '',
    ...(description ? { description } : {}),
    ...(spark ? { spark } : {}),
    input,
    ...(transformations.length > 0 ? { transformations } : {}),
    ...(validations ? { validations } : {}),
    ...(outputs.length === 1 ? { output: outputs[0] } : { outputs }),
  }

  if (params) reportUnknownParams(pipeline, params, issues)

  return { pipeline, issues: issues.items }
}

/** `{{runtime}}` vars are excluded: only single-brace template params are checked. */
const TEMPLATE_PARAM = /(?<!\{)\{([A-Za-z_][A-Za-z0-9_]*)\}(?!\})/g

function reportUnknownParams(
  pipeline: PipelineSpec,
  params: readonly ParamDefinition[],
  issues: Issues,
): void {
  const defined = new Set(params.map((param) => param.key))
  const reported = new Set<string>()
  for (const match of JSON.stringify(pipeline).matchAll(TEMPLATE_PARAM)) {
    const key = match[1]
    if (defined.has(key) || reported.has(key)) continue
    reported.add(key)
    issues.warning(`The pipeline uses {${key}}, but no parameter with that name is defined.`, {
      hint: 'Add it to the workflow parameters, or the placeholder stays literal at run time.',
    })
  }
}

/* -------------------------------------------------------------- serialize */

function orderTransformationDeep(value: unknown): unknown {
  if (!isRecord(value)) return value
  if ('$include' in value) return { $include: value.$include }

  const ordered = orderTransformationSpec(value)
  if (isRecord(ordered.with)) ordered.with = orderKeys(ordered.with, INPUT_KEY_ORDER)
  if (Array.isArray(ordered.with_transformations)) {
    ordered.with_transformations = ordered.with_transformations.map(orderTransformationDeep)
  }
  if (Array.isArray(ordered.transformations)) {
    ordered.transformations = ordered.transformations.map(orderTransformationDeep)
  }
  return ordered
}

function orderOutputDeep(value: unknown): unknown {
  if (!isRecord(value)) return value
  const ordered = orderKeys(value, OUTPUT_KEY_ORDER)
  if (Array.isArray(ordered.transformations)) {
    ordered.transformations = ordered.transformations.map(orderTransformationDeep)
  }
  return ordered
}

/** Pretty JSON with the key order Studio guarantees, ready to paste into a repo. */
export function serializePipeline(pipeline: PipelineSpec): string {
  const raw: JsonRecord = { ...pipeline }

  if (isRecord(raw.input)) raw.input = orderKeys(raw.input, INPUT_KEY_ORDER)
  if (Array.isArray(raw.transformations)) {
    raw.transformations = raw.transformations.map(orderTransformationDeep)
  }
  if (isRecord(raw.validations)) {
    const validations = orderKeys(raw.validations, VALIDATIONS_KEY_ORDER)
    if (isRecord(validations.report)) validations.report = orderOutputDeep(validations.report)
    if (Array.isArray(validations.rules)) {
      validations.rules = validations.rules.map((rule) =>
        isRecord(rule) ? orderKeys(rule, ['type']) : rule,
      )
    }
    raw.validations = validations
  }
  if (raw.output !== undefined) raw.output = orderOutputDeep(raw.output)
  if (Array.isArray(raw.outputs)) raw.outputs = raw.outputs.map(orderOutputDeep)

  return JSON.stringify(orderKeys(raw, PIPELINE_KEY_ORDER), null, 2)
}
