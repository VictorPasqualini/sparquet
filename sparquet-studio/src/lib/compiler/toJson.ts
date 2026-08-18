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
  ValidationNode,
  ValidationPolicy,
  ValidationSinkRole,
  JobSettings,
} from '@/types/studio'
import { DEFAULT_VALIDATION_POLICY, VALIDATION_SINK_ROLES } from '@/types/studio'
import {
  chainToSink,
  isCompilable,
  isDisabled,
  isNoteNode,
  isSinkNode,
  isSourceNode,
  isTransformNode,
  isValidationNode,
  longestCommonPrefix,
  sideParent,
  validationSinkLink,
} from '@/lib/compiler/graph'

export interface CompileResult {
  /** `null` when a structural error makes the job impossible to express. */
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
const VALIDATIONS_KEY_ORDER = ['on_failure', 'report', 'outputs', 'rules']
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

/** `Array.prototype.findLastIndex` is ES2023; the build targets ES2022. */
function lastIndexOf<T>(items: readonly T[], match: (item: T) => boolean): number {
  for (let index = items.length - 1; index >= 0; index -= 1) {
    if (match(items[index])) return index
  }
  return -1
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

/** One rule node → one entry of `validations.rules`. */
function buildRule(node: ValidationNode, issues: Issues): ValidationRuleSpec | null {
  const type = node.data.validator
  if (typeof type !== 'string' || type.trim() === '') {
    issues.warning('This validation node has no rule type and was left out.', {
      nodeId: node.id,
    })
    return null
  }

  const spec: JsonRecord = { type }
  for (const [key, value] of Object.entries(node.data.params ?? {})) {
    if (key === 'type') continue
    // Only null/undefined go: an empty list (`columns: []`) is still a value the
    // engine reads, so it has to survive the round trip.
    if (value === undefined || value === null) continue
    spec[key] = jsonClone(value)
  }
  return orderKeys(spec, ['type']) as unknown as ValidationRuleSpec
}

/**
 * The rule nodes on the shared chain, the job-level run policy and the side-output
 * destinations → the single `validations` object. No rules means no `validations`
 * key at all, which is also why the side outputs cannot exist without one.
 */
function buildValidations(
  nodes: readonly ValidationNode[],
  policy: ValidationPolicy | undefined,
  sideSinks: ReadonlyMap<ValidationSinkRole, SinkNode>,
  issues: Issues,
): ValidationsSpec | null {
  const rules: ValidationRuleSpec[] = []
  for (const node of nodes) {
    const rule = buildRule(node, issues)
    if (rule) rules.push(rule)
  }
  if (rules.length === 0) return null

  const spec: ValidationsSpec = {
    on_failure: policy?.onFailure ?? DEFAULT_VALIDATION_POLICY.onFailure,
  }

  const report = sideSinks.get('report')
  if (report) spec.report = buildOutput(report, [], issues)

  // `valid` before `invalid`, always, so two identical canvases never produce two
  // different files.
  const quarantine: Record<string, OutputSpec> = {}
  for (const role of VALIDATION_SINK_ROLES) {
    if (role === 'report') continue
    const node = sideSinks.get(role)
    if (node) quarantine[role] = buildOutput(node, [], issues)
  }
  if (Object.keys(quarantine).length > 0) spec.outputs = quarantine

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
  if (rest.some(isValidationNode)) {
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

/**
 * Splits the destinations in two: the ones the job writes (`outputs[]`) and the
 * ones the validation step writes on the side. A side destination is recognised by
 * the handle its incoming link leaves from, so it is never counted twice.
 */
function partitionSinks(
  graph: StudioGraph,
  issues: Issues,
): { sinks: SinkNode[]; sideSinks: Map<ValidationSinkRole, SinkNode>; sideIds: Set<string> } {
  const sinks: SinkNode[] = []
  const sideSinks = new Map<ValidationSinkRole, SinkNode>()
  /** Every sink wired to a side handle, refused ones included — they are reported
   *  here, so the orphan sweep at the end must not report them a second time. */
  const sideIds = new Set<string>()

  for (const node of graph.nodes.filter(isSinkNode)) {
    if (isDisabled(node)) continue
    const link = validationSinkLink(graph, node.id)
    if (!link) {
      sinks.push(node)
      continue
    }
    sideIds.add(node.id)

    if (!isValidationNode(link.parent) || isDisabled(link.parent)) {
      issues.error('A validation side output must hang off a validation rule.', {
        nodeId: node.id,
        hint: 'Re-connect it to the last rule of the run, or wire it into the main chain to make it an ordinary destination.',
      })
      continue
    }

    const taken = sideSinks.get(link.role)
    if (taken) {
      issues.error('Two destinations claim the same validation side output.', {
        nodeId: node.id,
        hint: 'The block writes each of `report`, `outputs.valid` and `outputs.invalid` once. Delete one, or move it to a free handle.',
      })
      continue
    }
    sideSinks.set(link.role, node)
  }

  return { sinks, sideSinks, sideIds }
}

export function compileGraph(
  graph: StudioGraph,
  settings: JobSettings,
  params?: readonly ParamDefinition[],
): CompileResult {
  const issues = createIssues()
  const { sinks, sideSinks, sideIds } = partitionSinks(graph, issues)

  if (sinks.length === 0) {
    issues.error('The job has no destination.', {
      hint:
        sideSinks.size > 0
          ? 'The validation report and quarantine are SIDE outputs of the rules — they never replace the job’s own destination. Add one and connect the end of the chain to it.'
          : 'Add a destination node and connect the end of the chain to it.',
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

  // Rules only exist as a block, and the block runs once for the whole job: a rule
  // node sitting past the fan-out would belong to one destination only.
  const strayValidations = new Set<string>()
  for (const entry of chains) {
    for (const node of entry.middle) {
      if (!isValidationNode(node) || prefixIds.has(node.id) || strayValidations.has(node.id)) {
        continue
      }
      strayValidations.add(node.id)
      issues.error('Validations must run before the chain splits into several destinations.', {
        nodeId: node.id,
        hint: 'Move it upstream of the branch, where every destination passes through it.',
      })
    }
  }

  const validationNodes = prefix.filter(isValidationNode)
  const firstValidation = prefix.findIndex(isValidationNode)
  const lastValidation = lastIndexOf(prefix, isValidationNode)

  // The framework runs main `transformations`, then ONE validations block, then each
  // output's own `transformations`. A node wedged between two rules would therefore
  // change place in that order without the canvas showing it, so it is refused.
  for (let index = firstValidation + 1; index < lastValidation; index += 1) {
    const node = prefix[index]
    if (isValidationNode(node)) continue
    issues.error('This node runs between two validation rules.', {
      nodeId: node.id,
      hint: 'Rules compile into a single validations block — keep them next to each other and move this node before or after the whole run.',
    })
  }

  // The shared prefix stops at the last rule; everything past it belongs to the
  // destinations.
  const mainPrefix = lastValidation >= 0 ? prefix.slice(0, lastValidation + 1) : prefix

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

  // A side output is only meaningful when its rule is part of the compiled block:
  // otherwise no `validations` key is emitted and the destination is never written.
  const compiledRules = new Set(validationNodes.map((node) => node.id))
  for (const id of sideIds) ctx.used.add(id)
  for (const [role, node] of sideSinks) {
    const link = validationSinkLink(graph, node.id)
    if (link && compiledRules.has(link.parent.id)) continue
    sideSinks.delete(role)
    issues.error('This validation side output hangs off a rule that never runs.', {
      nodeId: node.id,
      hint: 'Rules only compile when they sit on the chain every destination shares. Move the rule onto the main chain and re-connect this destination to it.',
    })
  }

  const validations = buildValidations(
    validationNodes,
    settings.validations,
    sideSinks,
    issues,
  )

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
      hint: 'Add it to the job parameters, or the placeholder stays literal at run time.',
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
    if (isRecord(validations.outputs)) {
      validations.outputs = Object.fromEntries(
        Object.entries(validations.outputs).map(([key, value]) => [
          key,
          orderOutputDeep(value),
        ]),
      )
    }
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
