/**
 * Client-side pipeline linting.
 *
 * Every rule below is decided from the graph alone. Whatever cannot be established
 * statically stays silent — the reader/writer registries are extensible at runtime
 * and the schema becomes unknowable after `sql` / `join` / `group_by`, so a false
 * positive would cost more than a missing warning.
 */

import { getFormat, getTransformation, getValidationSink, getValidator } from '@/catalog'
import type { FieldSpec } from '@/catalog/types'
import {
  DEFAULT_VALIDATION_POLICY,
  HANDLE,
  validationSinkRoleOfHandle,
} from '@/types/studio'
import type {
  IssueSeverity,
  ParamDefinition,
  SinkNode,
  SourceNode,
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

/* ------------------------------------------------------------------ helpers */

const isSourceNode = (node: StudioNode): node is SourceNode => node.data.kind === 'source'
const isTransformNode = (node: StudioNode): node is TransformNode =>
  node.data.kind === 'transform'
const isSinkNode = (node: StudioNode): node is SinkNode => node.data.kind === 'sink'
const isValidationNode = (node: StudioNode): node is ValidationNode =>
  node.data.kind === 'validation'

const isPrimaryEdge = (edge: StudioEdge): boolean =>
  (edge.targetHandle ?? HANDLE.in) === HANDLE.in
const isSecondaryEdge = (edge: StudioEdge): boolean => edge.targetHandle === HANDLE.inRight

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

const isBlank = (value: unknown): boolean => {
  if (value === undefined || value === null) return true
  if (typeof value === 'string') return value.trim() === ''
  if (Array.isArray(value)) return value.length === 0
  if (isRecord(value)) return Object.keys(value).length === 0
  return false
}

const text = (value: unknown): string => (typeof value === 'string' ? value.trim() : '')

const nonEmptyStrings = (value: unknown): string[] =>
  Array.isArray(value)
    ? value.filter((item): item is string => typeof item === 'string' && item.trim() !== '')
    : []

const labelOf = (node: StudioNode): string => {
  const data = node.data
  switch (data.kind) {
    case 'transform':
      return data.label ?? getTransformation(data.transform)?.label ?? data.transform
    case 'source':
      return data.label ?? `${data.format || 'source'} input`
    case 'sink':
      return data.label ?? (data.path || `${data.format || 'output'} output`)
    case 'validation':
      return data.label ?? getValidator(data.validator)?.label ?? data.validator
    default:
      return data.label ?? node.id
  }
}

interface StringHit {
  /** Dotted path of the value inside the node, used as the issue `field`. */
  path: string
  value: string
}

const collectStrings = (value: unknown, path: string, out: StringHit[]): void => {
  if (typeof value === 'string') {
    out.push({ path, value })
    return
  }
  if (Array.isArray(value)) {
    value.forEach((item, index) => collectStrings(item, `${path}[${index}]`, out))
    return
  }
  if (isRecord(value)) {
    for (const [key, entry] of Object.entries(value)) {
      collectStrings(entry, path ? `${path}.${key}` : key, out)
    }
  }
}

/** Every string the compiler would emit for this node, with its JSON path. */
const scannableOf = (node: StudioNode): Record<string, unknown> => {
  const data = node.data
  switch (data.kind) {
    case 'source':
      return { format: data.format, path: data.path, options: data.options }
    case 'transform':
      return { ...data.params, skip_if_false: data.skipIfFalse }
    case 'sink':
      return {
        format: data.format,
        path: data.path,
        mode: data.mode,
        columns: data.columns,
        partition_by: data.partitionBy,
        options: data.options,
      }
    case 'validation':
      return { ...data.params }
    default:
      return {}
  }
}

const RUNTIME_VAR_PATTERN = /\{\{(\w+)\}\}/g
/** Same shape the compiler substitutes: a digit-only `{11}` is a regex quantifier. */
const TEMPLATE_PARAM_PATTERN = /\{([A-Za-z_]\w*)\}/g

const runtimeVarsIn = (value: string): string[] =>
  [...value.matchAll(RUNTIME_VAR_PATTERN)].map((match) => match[1])

/**
 * `apply_template` also matches the inner braces of `{{var}}`, but reporting those as
 * params would flag every correct runtime placeholder — strip them before scanning.
 */
const templateParamsIn = (value: string): string[] =>
  [...value.replace(RUNTIME_VAR_PATTERN, '').matchAll(TEMPLATE_PARAM_PATTERN)].map(
    (match) => match[1],
  )

const SEVERITY_ORDER: Record<IssueSeverity, number> = { error: 0, warning: 1, info: 2 }

/* ------------------------------------------------------------------ context */

interface LintContext {
  nodes: StudioNode[]
  nodeById: Map<string, StudioNode>
  incoming: Map<string, StudioEdge[]>
  outgoing: Map<string, StudioEdge[]>
  sources: SourceNode[]
  transforms: TransformNode[]
  /** Destinations the job itself writes — `outputs[]`. Side outputs are not here. */
  sinks: SinkNode[]
  /**
   * Destinations the validation step writes BESIDES `outputs[]`, by role. They are
   * copies taken on the side: the trunk still carries every row past them.
   */
  sideSinks: { role: ValidationSinkRole; node: SinkNode; parent: StudioNode }[]
  /** One node per `validations.rules` entry, in no particular order. */
  validations: ValidationNode[]
  /** Main chain: the input source plus the prefix every destination shares. */
  trunk: Set<string>
  /** Nodes feeding the right-hand handle of a join / union. */
  rightChain: Set<string>
  /** Nodes living in a branch created by a fan-out and ending in a sink. */
  outputChain: Set<string>
  issues: ValidationIssue[]
}

/**
 * Ids from the head of the chain down to `endId`, walked backwards along primary
 * edges. `null` for the two shapes the compiler refuses to walk: a node with two
 * main inputs, and a loop.
 */
const linearChainTo = (incoming: Map<string, StudioEdge[]>, endId: string): string[] | null => {
  const chain: string[] = []
  const seen = new Set<string>()
  let current: string | undefined = endId
  while (current !== undefined) {
    if (seen.has(current)) return null
    seen.add(current)
    chain.push(current)
    const parents: Set<string> = new Set(
      (incoming.get(current) ?? []).filter(isPrimaryEdge).map((edge) => edge.source),
    )
    if (parents.size > 1) return null
    current = [...parents][0]
  }
  return chain.reverse()
}

const sharedPrefix = (lists: readonly (readonly string[])[]): string[] => {
  const [first, ...rest] = lists
  if (!first) return []
  const prefix: string[] = []
  for (const candidate of first) {
    const index = prefix.length
    if (!rest.every((list) => list[index] === candidate)) break
    prefix.push(candidate)
  }
  return prefix
}

const buildContext = (graph: StudioGraph): LintContext => {
  const nodes = graph.nodes.filter((node) => node.data.kind !== 'note')
  const nodeById = new Map(nodes.map((node) => [node.id, node]))
  const edges = graph.edges.filter(
    (edge) => nodeById.has(edge.source) && nodeById.has(edge.target),
  )

  const incoming = new Map<string, StudioEdge[]>()
  const outgoing = new Map<string, StudioEdge[]>()
  for (const edge of edges) {
    const into = incoming.get(edge.target)
    if (into) into.push(edge)
    else incoming.set(edge.target, [edge])
    const from = outgoing.get(edge.source)
    if (from) from.push(edge)
    else outgoing.set(edge.source, [edge])
  }

  const transforms = nodes.filter(isTransformNode)
  const sources = nodes.filter(isSourceNode)
  const validations = nodes.filter(isValidationNode)

  /**
   * Side outputs are held apart from `sinks` for the same reason the compiler holds
   * them apart: they are NOT branches of the main chain, and folding them into the
   * shared-prefix computation would truncate the trunk at the rule they hang off.
   */
  const sideSinks: LintContext['sideSinks'] = []
  const sinks: SinkNode[] = []
  for (const node of nodes.filter(isSinkNode)) {
    const link = (incoming.get(node.id) ?? [])
      .map((edge) => ({ role: validationSinkRoleOfHandle(edge.sourceHandle), edge }))
      .find((candidate): candidate is { role: ValidationSinkRole; edge: StudioEdge } =>
        candidate.role !== null,
      )
    const parent = link ? nodeById.get(link.edge.source) : undefined
    if (link && parent) sideSinks.push({ role: link.role, node, parent })
    else sinks.push(node)
  }

  const rightChain = new Set<string>()
  for (const node of transforms) {
    if (!getTransformation(node.data.transform)?.secondaryInput) continue
    const secondary = (incoming.get(node.id) ?? []).find(isSecondaryEdge)
    if (!secondary) continue
    const stack = [secondary.source]
    while (stack.length) {
      const current = stack.pop()
      if (current === undefined || rightChain.has(current)) continue
      rightChain.add(current)
      for (const edge of incoming.get(current) ?? []) stack.push(edge.source)
    }
  }

  const isEnabled = (id: string): boolean => {
    const node = nodeById.get(id)
    return node !== undefined && !('disabled' in node.data && node.data.disabled === true)
  }

  /**
   * The main chain is defined exactly as the compiler defines it — the source plus
   * the longest prefix every sink chain shares — instead of by following the flow
   * until it forks. A fan-out is not always a split into per-output branches: the
   * same source can feed the chain and a join's right side, and a probe can hang off
   * a node without ever reaching a destination.
   */
  const sinkChains: string[][] = []
  for (const node of sinks) {
    const walked = linearChainTo(incoming, node.id)
    if (!walked) continue
    const chain = walked.filter(isEnabled)
    const headId = chain[0]
    const head = headId === undefined ? undefined : nodeById.get(headId)
    if (!head || !isSourceNode(head)) continue
    sinkChains.push(chain)
  }

  const trunk = new Set<string>()
  if (sinkChains.length > 0) {
    trunk.add(sinkChains[0][0])
    const middles = sinkChains.map((chain) => chain.slice(1, chain.length - 1))
    for (const id of sharedPrefix(middles)) trunk.add(id)
  } else {
    // Nothing reaches a destination yet: stay permissive rather than report every
    // node of a half-drawn graph as off-chain.
    const start = sources.find((node) => !rightChain.has(node.id)) ?? sources[0]
    const pending = start ? [start.id] : []
    while (pending.length) {
      const id = pending.pop()
      if (id === undefined || trunk.has(id)) continue
      trunk.add(id)
      for (const edge of outgoing.get(id) ?? []) {
        if (isPrimaryEdge(edge) && !rightChain.has(edge.target)) pending.push(edge.target)
      }
    }
  }

  const feedsSink = new Set<string>()
  const stack = sinks.map((sink) => sink.id)
  while (stack.length) {
    const id = stack.pop()
    if (id === undefined) continue
    for (const edge of incoming.get(id) ?? []) {
      if (feedsSink.has(edge.source)) continue
      feedsSink.add(edge.source)
      stack.push(edge.source)
    }
  }

  const outputChain = new Set(
    [...feedsSink].filter((id) => !trunk.has(id) && !rightChain.has(id)),
  )

  return {
    nodes,
    nodeById,
    incoming,
    outgoing,
    sources,
    transforms,
    sinks,
    sideSinks,
    validations,
    trunk,
    rightChain,
    outputChain,
    issues: [],
  }
}

const primaryParent = (ctx: LintContext, nodeId: string): string | undefined =>
  (ctx.incoming.get(nodeId) ?? []).find(isPrimaryEdge)?.source

const hasPrimaryInput = (ctx: LintContext, nodeId: string): boolean =>
  primaryParent(ctx, nodeId) !== undefined

/** Nodes from the input source down to `nodeId`, following primary edges. */
const chainTo = (ctx: LintContext, nodeId: string): StudioNode[] => {
  const chain: StudioNode[] = []
  const seen = new Set<string>()
  let current: string | undefined = nodeId
  while (current !== undefined && !seen.has(current)) {
    seen.add(current)
    const node = ctx.nodeById.get(current)
    if (node) chain.push(node)
    current = primaryParent(ctx, current)
  }
  return chain.reverse()
}

/* -------------------------------------------------------------------- rules */

const checkStructure = (ctx: LintContext): void => {
  if (ctx.sinks.length === 0) {
    ctx.issues.push({
      id: 'no-sink',
      severity: 'error',
      message: 'The pipeline has no output node.',
      hint: 'Add a sink and connect the end of the chain to it — the framework raises a ValueError when neither `output` nor `outputs` is present.',
    })
  }

  if (ctx.sources.length === 0) {
    ctx.issues.push({
      id: 'no-source',
      severity: 'error',
      message: 'The pipeline has no input node.',
      hint: 'Add a source: `input.format` and `input.path` are mandatory, even for pipelines that receive an injected DataFrame.',
    })
  }

  // Transformations and rules both need a DataFrame to work on.
  for (const node of [...ctx.transforms, ...ctx.validations]) {
    if (node.data.disabled) continue
    if (hasPrimaryInput(ctx, node.id)) continue
    ctx.issues.push({
      id: `orphan:${node.id}`,
      severity: 'error',
      message: `"${labelOf(node)}" has no incoming connection.`,
      nodeId: node.id,
      hint: 'Connect it to the chain or delete it — a disconnected node never reaches the compiled pipeline.',
    })
  }

  for (const node of ctx.sinks) {
    if (hasPrimaryInput(ctx, node.id)) continue
    ctx.issues.push({
      id: `orphan:${node.id}`,
      severity: 'error',
      message: `Output "${labelOf(node)}" has no incoming connection.`,
      nodeId: node.id,
      hint: 'Connect the last node of a chain to this output, otherwise it has nothing to write.',
    })
  }

  if (ctx.validations.length === 0) {
    ctx.issues.push({
      id: 'no-validations',
      severity: 'info',
      message: 'The pipeline has no validation rules.',
      hint: 'Drag a rule from the Quality section to report on data quality — rules never modify the DataFrame, they only measure it.',
    })
  }

  for (const node of ctx.validations) {
    if (ctx.trunk.has(node.id)) continue
    ctx.issues.push({
      id: `validations-branch:${node.id}`,
      severity: 'error',
      message: `"${labelOf(node)}" is not on the main chain.`,
      nodeId: node.id,
      hint: 'Validations run once, after every transformation and before any write — place every rule on the trunk, never inside a per-output or right-side branch.',
    })
  }

  // Rules compile into ONE block, so a transformation standing between two of them
  // would silently move ahead of the checks it looks like it runs after.
  for (const node of ctx.transforms) {
    if (node.data.disabled || !ctx.trunk.has(node.id)) continue
    if (!hasValidationBothWays(ctx, node.id)) continue
    ctx.issues.push({
      id: `validations-split:${node.id}`,
      severity: 'error',
      message: `"${labelOf(node)}" runs between two validation rules.`,
      nodeId: node.id,
      hint: 'The rules around it compile into a single validations block, which always runs after every main transformation. Group the rules together and move this node before or after the whole run.',
    })
  }
}

/** Is this trunk node sandwiched between validation rules, upstream and downstream? */
const hasValidationBothWays = (ctx: LintContext, nodeId: string): boolean => {
  const upstream = chainTo(ctx, nodeId)
    .slice(0, -1)
    .some((node) => isValidationNode(node) && ctx.trunk.has(node.id))
  if (!upstream) return false

  const seen = new Set<string>([nodeId])
  const pending = [nodeId]
  while (pending.length) {
    const current = pending.pop()
    if (current === undefined) continue
    for (const edge of ctx.outgoing.get(current) ?? []) {
      if (!isPrimaryEdge(edge) || seen.has(edge.target)) continue
      seen.add(edge.target)
      const node = ctx.nodeById.get(edge.target)
      if (node && isValidationNode(node) && ctx.trunk.has(node.id)) return true
      pending.push(edge.target)
    }
  }
  return false
}

const checkFieldSpecs = (
  ctx: LintContext,
  nodeId: string,
  ownerLabel: string,
  fields: FieldSpec[],
  values: Record<string, unknown>,
  idPrefix: string,
  fieldPrefix: string,
  skipKeys: ReadonlySet<string> = new Set<string>(),
): void => {
  for (const field of fields) {
    if (skipKeys.has(field.key)) continue
    if (field.visibleWhen && !field.visibleWhen(values)) continue

    const value = values[field.key]
    const scoped = `${fieldPrefix}${field.key}`

    if (field.required && isBlank(value)) {
      ctx.issues.push({
        id: `${idPrefix}:${field.key}`,
        severity: 'error',
        message: `${ownerLabel}: "${field.label}" is required.`,
        nodeId,
        field: scoped,
        hint:
          field.help ??
          'The framework reads this key directly and fails at runtime when it is missing.',
      })
      continue
    }

    const problem = field.validate?.(value, values)
    if (!problem) continue
    ctx.issues.push({
      id: `${idPrefix}:${field.key}`,
      severity: 'error',
      message: `${ownerLabel}: ${problem}`,
      nodeId,
      field: scoped,
      hint: field.help ?? 'Fix the value in the node inspector.',
    })
  }
}

const requirePath = (ctx: LintContext, node: StudioNode, path: string, label: string): void => {
  if (text(path) !== '') return
  ctx.issues.push({
    id: `field:${node.id}:path`,
    severity: 'error',
    message: `${labelOf(node)}: "${label}" is required.`,
    nodeId: node.id,
    field: 'path',
    hint: 'Both `format` and `path` are mandatory on every input, join/union source and output.',
  })
}

const requireFormat = (ctx: LintContext, node: StudioNode, format: string): boolean => {
  if (text(format) !== '') return true
  ctx.issues.push({
    id: `field:${node.id}:format`,
    severity: 'error',
    message: `${labelOf(node)}: a format is required.`,
    nodeId: node.id,
    field: 'format',
    hint: 'Pick the IO format this node reads from or writes to.',
  })
  return false
}

const checkSources = (ctx: LintContext): void => {
  for (const node of ctx.sources) {
    const { format, path, options } = node.data
    const named = requireFormat(ctx, node, format)
    const def = named ? getFormat(format) : undefined

    // Unknown ids are left alone: reader/writer registries are extensible at runtime.
    if (def && !def.canRead) {
      ctx.issues.push({
        id: `format-read:${node.id}`,
        severity: 'error',
        message: `${def.label} cannot be read.`,
        nodeId: node.id,
        field: 'format',
        hint: 'The reader registry has no entry for this format, so the run fails before any Spark call. Use it as an output only, and read a readable format here.',
      })
    }

    requirePath(ctx, node, path, def?.pathLabel ?? 'Path')
    if (def) {
      checkFieldSpecs(
        ctx,
        node.id,
        labelOf(node),
        def.readOptions,
        options,
        `field:${node.id}:option`,
        'options.',
      )
    }
  }
}

/**
 * `merge_keys` is skipped here and reported by the dedicated merge rule, which knows
 * the write mode and produces one precise message instead of two overlapping ones.
 */
const SINK_OPTION_SKIP: ReadonlySet<string> = new Set(['merge_keys'])

const checkSinks = (ctx: LintContext): void => {
  const destinations = new Map<string, string>()

  // Side outputs are real writes too — same format/path/mode rules, and the
  // duplicate-path check has to see them: a quality report pointed at the job's own
  // table would silently overwrite it.
  for (const node of [...ctx.sinks, ...ctx.sideSinks.map((entry) => entry.node)]) {
    const { format, path, mode, options } = node.data
    const named = requireFormat(ctx, node, format)
    const def = named ? getFormat(format) : undefined

    if (def && !def.canWrite) {
      ctx.issues.push({
        id: `format-write:${node.id}`,
        severity: 'error',
        message: `${def.label} cannot be written.`,
        nodeId: node.id,
        field: 'format',
        hint: 'The writer registry has no entry for this format — pick a writable destination.',
      })
    }

    requirePath(ctx, node, path, def?.pathLabel ?? 'Path')

    if (def) {
      // The option validators expect the write mode alongside the options.
      const optionValues: Record<string, unknown> = { ...options, mode }
      checkFieldSpecs(
        ctx,
        node.id,
        labelOf(node),
        def.writeOptions,
        optionValues,
        `field:${node.id}:option`,
        'options.',
        SINK_OPTION_SKIP,
      )
    }

    const writeMode = String(mode ?? '')
    if (writeMode.toLowerCase() === 'merge' && def) {
      if (!def.supportsMerge) {
        ctx.issues.push({
          id: `merge-unsupported:${node.id}`,
          severity: 'error',
          message: `Write mode "merge" is not supported by ${def.label}.`,
          nodeId: node.id,
          field: 'mode',
          hint: 'Only delta and iceberg implement MERGE. Every other writer hands the string to df.write.mode(), where Spark raises "Unknown save mode".',
        })
      } else if (def.id === 'iceberg' && writeMode !== 'merge') {
        ctx.issues.push({
          id: `merge-case:${node.id}`,
          severity: 'error',
          message: `Write mode "${writeMode}" does not merge into ${def.label}.`,
          nodeId: node.id,
          field: 'mode',
          hint: 'The Iceberg writer compares the mode against "merge" case-sensitively, so this takes the ordinary-write branch and the string reaches df.write.mode() as-is. Write it in lowercase — merge keys make no difference here.',
        })
      } else if (nonEmptyStrings(options.merge_keys).length === 0) {
        ctx.issues.push({
          id: `merge-keys:${node.id}`,
          severity: 'error',
          message: 'Merge mode needs at least one merge key.',
          nodeId: node.id,
          field: 'options.merge_keys',
          hint: 'Set options.merge_keys — it builds the ON clause (T.<key> = S.<key>) and the writer raises a ValueError before any Spark call when it is empty.',
        })
      }
    }

    const destination = `${text(format).toLowerCase()}|${text(path)}`
    if (text(path) === '') continue
    const first = destinations.get(destination)
    if (first === undefined) {
      destinations.set(destination, node.id)
      continue
    }
    ctx.issues.push({
      id: `duplicate-sink:${node.id}`,
      severity: 'warning',
      message: `Another output already writes ${text(format)} to "${text(path)}".`,
      nodeId: node.id,
      field: 'path',
      hint: 'Outputs run one after another over the same DataFrame, so the second write overwrites or duplicates the first. Give each destination its own path, or merge the two nodes.',
    })
  }
}

const collectAncestors = (ctx: LintContext, nodeId: string): Set<string> => {
  const seen = new Set<string>()
  const stack = [nodeId]
  while (stack.length) {
    const current = stack.pop()
    if (current === undefined || seen.has(current)) continue
    seen.add(current)
    for (const edge of ctx.incoming.get(current) ?? []) stack.push(edge.source)
  }
  return seen
}

const hasUpstreamCheckpoint = (ctx: LintContext, nodeId: string): boolean => {
  const seen = new Set<string>()
  let current = primaryParent(ctx, nodeId)
  while (current !== undefined && !seen.has(current)) {
    seen.add(current)
    const node = ctx.nodeById.get(current)
    if (
      node &&
      isTransformNode(node) &&
      !node.data.disabled &&
      node.data.transform === 'checkpoint'
    ) {
      return true
    }
    current = primaryParent(ctx, current)
  }
  return false
}

const checkTransforms = (ctx: LintContext): void => {
  for (const node of ctx.transforms) {
    if (node.data.disabled) continue
    const def = getTransformation(node.data.transform)
    if (!def) continue

    checkFieldSpecs(
      ctx,
      node.id,
      labelOf(node),
      def.fields,
      node.data.params,
      `field:${node.id}`,
      '',
    )

    if (def.secondaryInput) {
      const secondary = (ctx.incoming.get(node.id) ?? []).find(isSecondaryEdge)
      if (!secondary) {
        ctx.issues.push({
          id: `missing-right:${node.id}`,
          severity: 'error',
          message: `${labelOf(node)} has no second input.`,
          nodeId: node.id,
          field: 'with',
          hint: 'Connect a source to the right-hand handle: `with` is compiled from it and the run fails with a KeyError without it.',
        })
      } else if (def.type === 'union') {
        const reshaped = [...collectAncestors(ctx, secondary.source)]
          .map((id) => ctx.nodeById.get(id))
          .filter(
            (candidate): candidate is TransformNode =>
              candidate !== undefined && isTransformNode(candidate) && !candidate.data.disabled,
          )
        if (reshaped.length > 0) {
          ctx.issues.push({
            id: `union-right-chain:${node.id}`,
            severity: 'error',
            message: 'Union ignores the transformations connected to its right input.',
            nodeId: node.id,
            hint: 'Union compiles only `with` — it has no with_transformations. Reshape the second source in its own pipeline (or a view) and union that, or switch to a join.',
          })
        }
      }
    }

    if (def.type === 'union' && node.data.params.allow_missing_columns !== true) {
      ctx.issues.push({
        id: `union-positional:${node.id}`,
        severity: 'warning',
        message: 'Union matches columns by position, not by name.',
        nodeId: node.id,
        field: 'allow_missing_columns',
        hint: 'df.union() pairs column 1 with column 1 and writes silently wrong data when the schemas differ in order — and the main DataFrame carries the auto-added ingestion_ts that the second source lacks. Turn "Match columns by name" on.',
      })
    }

    if (def.type === 'collect' && !hasUpstreamCheckpoint(ctx, node.id)) {
      ctx.issues.push({
        id: `collect-checkpoint:${node.id}`,
        severity: 'warning',
        message: 'This collect is not preceded by a checkpoint.',
        nodeId: node.id,
        hint: 'collect is a driver-side action: without a checkpoint the whole lineage is recomputed to produce the list. Insert a checkpoint node right before it.',
      })
    }

    if (def.type === 'stop_if_empty') {
      const inOutputChain = ctx.outputChain.has(node.id)
      const inRightChain = ctx.rightChain.has(node.id)
      if (inOutputChain || inRightChain) {
        ctx.issues.push({
          id: `stop-if-empty-branch:${node.id}`,
          severity: 'warning',
          message: inOutputChain
            ? 'stop_if_empty runs inside a per-output chain.'
            : 'stop_if_empty runs inside a join/union right-side chain.',
          nodeId: node.id,
          hint: 'PipelineStop is caught at the top level, so it aborts the whole run and reports skipped=true even when earlier outputs were already written. Keep this node on the main chain.',
        })
      }
    }

    if (def.type === 'debug') {
      const debugLabel = text(node.data.params.label)
      ctx.issues.push({
        id: `debug-node:${node.id}`,
        severity: 'info',
        message: debugLabel
          ? `Debug node "${debugLabel}" is still in the pipeline.`
          : 'A debug node is still in the pipeline.',
        nodeId: node.id,
        hint: 'Fine while developing, noisy in production: count, show and explain are Spark actions that run on every execution and print to stdout instead of the structured log.',
      })
    }
  }
}

const checkValidationRules = (ctx: LintContext): void => {
  for (const node of ctx.validations) {
    if (node.data.disabled) continue
    const def = getValidator(node.data.validator)
    if (!def) continue
    checkFieldSpecs(
      ctx,
      node.id,
      `${def.label} rule`,
      def.fields,
      node.data.params,
      `field:${node.id}`,
      '',
    )
  }
}

/**
 * Row-level rules are the only ones that can label a ROW: they run per row and their
 * verdict is what `ValidationEngine.split()` routes on. An aggregate rule
 * (`row_count`, `schema`, `sql`, and the `check` metrics that reduce the whole
 * frame) answers about the dataset, so it can never mark an individual row.
 */
const ROW_LEVEL_VALIDATORS: ReadonlySet<string> = new Set([
  'not_null',
  'unique',
  'range',
  'regex',
])

/** `check` is row-level only for the metrics that count rows one by one. */
const ROW_LEVEL_CHECK_METRICS: ReadonlySet<string> = new Set([
  'missing_count',
  'missing_percent',
  'invalid_count',
  'invalid_percent',
])

const isRowLevelRule = (node: ValidationNode): boolean => {
  if (node.data.validator === 'check') {
    return ROW_LEVEL_CHECK_METRICS.has(text(node.data.params.metric))
  }
  return ROW_LEVEL_VALIDATORS.has(node.data.validator)
}

/**
 * The three destinations the validation step writes on the side.
 *
 * Their format/path/mode are checked with every other sink in `checkSinks`; what is
 * left here is what only makes sense for a SIDE output — whether the rules upstream
 * can actually fill it, and whether the projection it asks for exists.
 */
const checkValidationSinks = (ctx: LintContext): void => {
  const active = ctx.validations.filter((node) => !node.data.disabled)
  const activeIds = new Set(active.map((node) => node.id))

  for (const { role, node, parent } of ctx.sideSinks) {
    const def = getValidationSink(role)

    if (!isValidationNode(parent)) {
      ctx.issues.push({
        id: `dq-sink-parent:${node.id}`,
        severity: 'error',
        message: `"${labelOf(node)}" is wired to a side output of a node that is not a validation rule.`,
        nodeId: node.id,
        hint: `${def.label} is written by the validations block. Connect it to the last rule of the run, or wire it into the main chain to make it an ordinary destination.`,
      })
      continue
    }

    if (!activeIds.has(parent.id)) {
      ctx.issues.push({
        id: `dq-sink-muted:${node.id}`,
        severity: 'error',
        message: `"${labelOf(node)}" hangs off a muted rule.`,
        nodeId: node.id,
        hint: 'A muted rule is left out of the compiled JSON, and with it this destination.',
      })
      continue
    }

    if (!ctx.trunk.has(parent.id)) {
      ctx.issues.push({
        id: `dq-sink-branch:${node.id}`,
        severity: 'error',
        message: `"${labelOf(node)}" hangs off a rule that is not on the main chain.`,
        nodeId: node.id,
        hint: 'Only rules every destination passes through compile into the validations block; a rule off the trunk writes nothing.',
      })
      continue
    }

    // The quarantine split is decided by row-level rules alone. With none of them on
    // the chain every row counts as valid, so `invalid` is written empty and `valid`
    // is a full second copy of the data — a silent, expensive no-op.
    if (role !== 'report' && !active.some(isRowLevelRule)) {
      ctx.issues.push({
        id: `dq-sink-no-row-rule:${node.id}`,
        severity: 'warning',
        message: `"${labelOf(node)}" has no row-level rule to sort rows with.`,
        nodeId: node.id,
        hint: 'Only not_null, unique, range, regex and the missing/invalid checks label a single row. Aggregate rules (row_count, schema, sql, and metrics like avg or freshness) judge the whole DataFrame, so every row would come out valid.',
      })
    }

    // `_write_validation_report` builds its own DataFrame and writes it straight —
    // it never calls `_project_columns`, so a projection here is dead config.
    if (role === 'report' && node.data.columns !== null && node.data.columns.length > 0) {
      ctx.issues.push({
        id: `dq-report-columns:${node.id}`,
        severity: 'warning',
        message: 'The quality report ignores the column projection.',
        nodeId: node.id,
        field: 'columns',
        hint: 'The report has a fixed schema (pipeline, rule_type, check_name, severity, passed, failed_count, metric_value, message, validated_at) and is written without a select. Turn the projection off.',
      })
    }
  }
}

/**
 * What is left of the block-level policy after the three datasets became nodes:
 * `on_failure` alone. It is job-scoped, so the issue carries no `nodeId`.
 */
const checkValidationPolicy = (ctx: LintContext, settings: JobSettings): void => {
  const policy: ValidationPolicy | undefined = settings.validations
  if (!policy) return

  const active = ctx.validations.filter((node) => !node.data.disabled)
  if (policy.onFailure !== DEFAULT_VALIDATION_POLICY.onFailure && active.length === 0) {
    ctx.issues.push({
      id: 'settings:on-failure-unused',
      severity: 'info',
      message: `"On failure: ${policy.onFailure}" applies to no rule.`,
      hint: 'The policy only reaches the JSON alongside at least one validation rule.',
    })
  }
}

const checkPlaceholders = (
  ctx: LintContext,
  settings: JobSettings,
  params: ParamDefinition[],
): void => {
  const published = new Set<string>()
  for (const node of ctx.transforms) {
    if (node.data.disabled || node.data.transform !== 'collect') continue
    const name = text(node.data.params.as)
    if (name) published.add(name)
  }

  const declared = new Set(params.map((param) => param.key.trim()).filter(Boolean))
  const usedParams = new Map<string, { nodeId?: string; field?: string }>()

  const scan = (hits: StringHit[], nodeId?: string): void => {
    const reported = new Set<string>()
    for (const hit of hits) {
      for (const name of runtimeVarsIn(hit.value)) {
        if (published.has(name) || reported.has(name)) continue
        reported.add(name)
        ctx.issues.push({
          id: nodeId ? `runtime-var:${nodeId}:${name}` : `runtime-var:${name}`,
          severity: 'warning',
          message: `Runtime variable {{${name}}} is never published by a collect node.`,
          nodeId,
          field: hit.path,
          hint: 'Add a collect upstream whose "as" is exactly this name, or fix the spelling — an unresolved placeholder is left literal and reaches Spark as broken SQL.',
        })
      }
      for (const name of templateParamsIn(hit.value)) {
        if (usedParams.has(name)) continue
        usedParams.set(name, { nodeId, field: hit.path })
      }
    }
  }

  for (const node of ctx.nodes) {
    const hits: StringHit[] = []
    collectStrings(scannableOf(node), '', hits)
    scan(hits, node.id)
  }

  const settingsHits: StringHit[] = []
  collectStrings(
    {
      name: settings.pipelineName,
      description: settings.description,
      spark: settings.spark,
      // The quality report and the quarantine paths are as templated as any other
      // destination, so they are scanned like one.
      validations: settings.validations,
    },
    '',
    settingsHits,
  )
  scan(settingsHits)

  for (const [name, where] of usedParams) {
    if (declared.has(name)) continue
    ctx.issues.push({
      id: `param-undeclared:${name}`,
      severity: 'warning',
      message: `Template param {${name}} is used but not declared in the job params.`,
      nodeId: where.nodeId,
      field: where.field,
      hint: 'Declare it in the params panel or remove the placeholder — an unknown key is left literal in the JSON instead of being substituted.',
    })
  }

  for (const param of params) {
    const key = param.key.trim()
    if (!key || usedParams.has(key)) continue
    ctx.issues.push({
      id: `param-unused:${key}`,
      severity: 'info',
      message: `Param "${key}" is declared but never used.`,
      hint: `Reference it as {${key}} somewhere in the pipeline, or remove it from the params list.`,
    })
  }
}

/* ------------------------------------------------- static column projection */

/** Output name of one `select` entry, or null when it cannot be derived. */
const selectOutputName = (entry: string): string | null => {
  const value = entry.trim()
  if (!value) return null
  const aliased = /\s+as\s+(`[^`]+`|[A-Za-z_]\w*)$/i.exec(value)
  if (aliased) return aliased[1].replace(/`/g, '')
  if (/^[A-Za-z_]\w*$/.test(value)) return value
  const quoted = /^`([^`]+)`$/.exec(value)
  if (quoted) return quoted[1]
  return null
}

/**
 * Tracks the column set along a chain, returning null as soon as it stops being
 * knowable. Only a `select` can make it knowable in the first place, so a chain
 * without one never produces a projection warning.
 */
const trackColumns = (columns: Set<string> | null, node: TransformNode): Set<string> | null => {
  const { transform, params } = node.data

  switch (transform) {
    case 'select': {
      const entries = params.columns
      if (!Array.isArray(entries) || entries.length === 0) return null
      const names: string[] = []
      for (const entry of entries) {
        if (typeof entry !== 'string') return null
        const name = selectOutputName(entry)
        if (name === null) return null
        names.push(name)
      }
      return new Set(names)
    }
    case 'with_column': {
      if (!columns) return null
      const next = new Set(columns)
      const map = params.columns
      // The engine branches on the KEY being present (`if columns is not None`),
      // so an empty map produces no column at all — not the single-column form.
      if (isRecord(map)) {
        for (const key of Object.keys(map)) next.add(key)
        return next
      }
      const single = text(params.column) || text(params.name)
      if (!single) return null
      next.add(single)
      return next
    }
    case 'struct': {
      if (!columns) return null
      const single = text(params.column) || text(params.name)
      if (!single) return null
      const next = new Set(columns)
      next.add(single)
      return next
    }
    case 'rename': {
      if (!columns) return null
      const mappings = params.mappings
      if (!isRecord(mappings)) return null
      const next = new Set(columns)
      for (const [from, to] of Object.entries(mappings)) {
        if (typeof to !== 'string' || !to.trim()) return null
        if (!next.has(from)) continue
        next.delete(from)
        next.add(to)
      }
      return next
    }
    case 'drop': {
      if (!columns) return null
      const next = new Set(columns)
      for (const name of nonEmptyStrings(params.columns)) next.delete(name)
      return next
    }
    case 'filter':
    case 'cast':
    case 'fill_na':
    case 'sort':
    case 'distinct':
    case 'drop_duplicates':
    case 'checkpoint':
    case 'collect':
    case 'stop_if_empty':
    case 'debug':
      return columns
    default:
      // sql, join, union, group_by, $include and anything custom reshape the schema.
      return null
  }
}

const checkOutputColumns = (ctx: LintContext): void => {
  // The quarantine outputs go through the same `_project_columns`, over the same
  // DataFrame, so the projection is checked against the same chain. The report has
  // a schema of its own and is handled by `checkValidationSinks`.
  const quarantine = ctx.sideSinks
    .filter((entry) => entry.role !== 'report')
    .map((entry) => entry.node)

  for (const sink of [...ctx.sinks, ...quarantine]) {
    const requested = sink.data.columns
    if (!requested || requested.length === 0) continue

    let columns: Set<string> | null = null
    for (const node of chainTo(ctx, sink.id)) {
      if (!isTransformNode(node)) continue
      if (node.data.disabled) continue
      columns = trackColumns(columns, node)
    }
    if (!columns) continue

    for (const name of requested) {
      if (typeof name !== 'string' || columns.has(name)) continue
      ctx.issues.push({
        id: `output-column:${sink.id}:${name}`,
        severity: 'warning',
        message: `Column "${name}" is not produced by the chain feeding this output.`,
        nodeId: sink.id,
        field: 'columns',
        hint: 'The projection runs df.select() and raises a ValueError listing the missing names. Add the column upstream, or drop it from the output columns.',
      })
    }
  }
}

/* ---------------------------------------------------------------- entry point */

/**
 * Lints a job graph against the framework semantics the JSON schema cannot
 * express. Issue ids are derived from rule + node so they stay stable across runs
 * and can be used as React keys.
 */
export function lintJob(
  graph: StudioGraph,
  settings: JobSettings,
  params: ParamDefinition[],
): ValidationIssue[] {
  const ctx = buildContext(graph)

  checkStructure(ctx)
  checkSources(ctx)
  checkSinks(ctx)
  checkTransforms(ctx)
  checkValidationRules(ctx)
  checkValidationSinks(ctx)
  checkValidationPolicy(ctx, settings)
  checkPlaceholders(ctx, settings, params)
  checkOutputColumns(ctx)

  const seen = new Set<string>()
  return ctx.issues
    .filter((issue) => {
      if (seen.has(issue.id)) return false
      seen.add(issue.id)
      return true
    })
    .sort((a, b) => SEVERITY_ORDER[a.severity] - SEVERITY_ORDER[b.severity])
}
