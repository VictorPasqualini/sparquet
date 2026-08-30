/**
 * Describing a Job — what one pipeline FILE reads, writes and does.
 *
 * A Job is one JSON: an input, a chain of transformations, optional validations
 * and one or more outputs. This module reads that shape back out so a surface
 * that shows a Job WITHOUT opening its canvas — a Pipeline stage box, a picker —
 * can describe it the same way everywhere.
 *
 * The canvas graph is the stored source of truth (a compiled `PipelineSpec` is
 * never persisted), so a description is read off `compileGraph` — exactly the
 * JSON Sparquet would execute. A Job that does not compile yet (no destination,
 * dangling chain) is still described, straight from its canvas nodes, so an
 * unfinished file is never blank.
 *
 * `topologicalOrder` lives here too: the numbering a Pipeline puts on its stages.
 *
 * Pure: no React, no store, no IO.
 */

import { getFormat, getTransformation } from '@/catalog'
import {
  compileGraph,
  isDisabled,
  isSinkNode,
  isSourceNode,
  isTransformNode,
  isValidationNode,
} from '@/lib/compiler'
import type { OutputSpec, PipelineSpec, TransformationSpec } from '@/types/pipeline'
import { isIncludeDirective, outputsOf } from '@/types/pipeline'
import { DEFAULT_VALIDATION_POLICY } from '@/types/studio'
import type { JobSettings, StudioGraph, Job } from '@/types/studio'

/* ------------------------------------------------------------------- types */

/** Where a step sits in the execution order of a single file. */
export type JobStepKind = 'input' | 'transformation' | 'validations' | 'output'

export interface JobStep {
  /** Unique inside its job; used as a React key. */
  id: string
  kind: JobStepKind
  /** Raw identifier: a transformation `type`, or the IO format. */
  type: string
  /** Human name from the catalog, falling back to `type`. */
  label: string
  /** Compact one-liner (condition, path, rule count). Empty when there is none. */
  detail: string
}

export interface JobEndpoint {
  format: string
  path: string
  /** Write mode; outputs only. */
  mode?: string
}

/**
 * What one pipeline file reads, writes and does — everything a box needs to be
 * recognised without opening it.
 */
export interface JobDescription {
  input: JobEndpoint | null
  outputs: JobEndpoint[]
  steps: JobStep[]
  transformationCount: number
  hasValidations: boolean
  validationRuleCount: number
  /** `false` when the job does not compile and the description came off the canvas. */
  compiled: boolean
}

/* -------------------------------------------------------------- describing */

function asText(value: unknown): string {
  return typeof value === 'string' ? value.trim() : ''
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/** Keys worth showing under a step, most specific first. */
const DETAIL_KEYS = [
  'condition',
  'query',
  'expression',
  'columns',
  'column',
  'by',
  'mappings',
  'fields',
  'agg',
  'name',
  'as',
  'on',
  'message',
  'label',
]

/** Squashes a param into one readable line; returns '' when there is nothing to show. */
function compactValue(value: unknown): string {
  if (typeof value === 'string') return value.replace(/\s+/g, ' ').trim()
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  if (Array.isArray(value)) {
    return value.map(compactValue).filter(Boolean).join(', ')
  }
  if (isRecord(value)) return Object.keys(value).join(', ')
  return ''
}

function endpointOf(spec: { format?: unknown; path?: unknown; mode?: unknown }): JobEndpoint {
  const endpoint: JobEndpoint = { format: asText(spec.format), path: asText(spec.path) }
  const mode = asText(spec.mode)
  if (mode) endpoint.mode = mode
  return endpoint
}

function formatLabel(format: string): string {
  return getFormat(format)?.label ?? format ?? ''
}

function transformationStep(id: string, spec: TransformationSpec): JobStep {
  if (isIncludeDirective(spec)) {
    return {
      id,
      kind: 'transformation',
      type: '$include',
      label: 'Include',
      detail: asText(spec.$include),
    }
  }

  const type = asText(spec.type)
  const label = getTransformation(type)?.label ?? type
  const step = (detail: string): JobStep => ({
    id,
    kind: 'transformation',
    type,
    label,
    detail,
  })

  // `input` is the current key for the second source; `with` is the old name.
  const source = isRecord(spec.input) ? spec.input : isRecord(spec.with) ? spec.with : null
  const side = source ? asText(source.path) : ''
  if (side) {
    const how = asText(spec.how)
    return step(how ? `${how} · ${side}` : side)
  }

  // The two transformations whose meaning lives in a pair of keys, not in one.
  const column = compactValue(spec.column)
  if (type === 'with_column' && column) {
    const expression = compactValue(spec.expression)
    return step(expression ? `${column} = ${expression}` : column)
  }
  if (type === 'collect' && column) {
    const target = compactValue(spec.as)
    return step(target ? `${column} → {{${target}}}` : column)
  }

  for (const key of DETAIL_KEYS) {
    const detail = compactValue(spec[key])
    if (detail) return step(detail)
  }
  return step('')
}

function describeFromPipeline(pipeline: PipelineSpec): JobDescription {
  const steps: JobStep[] = []
  let transformationCount = 0

  const input = endpointOf(pipeline.input ?? {})
  steps.push({
    id: 'input',
    kind: 'input',
    type: input.format,
    label: formatLabel(input.format),
    detail: input.path,
  })

  const transformations = pipeline.transformations ?? []
  transformations.forEach((spec, index) => {
    transformationCount += 1
    steps.push(transformationStep(`t-${index}`, spec))
  })

  const validations = pipeline.validations
  const hasValidations = Boolean(validations)
  let validationRuleCount = 0
  if (validations) {
    const rules = validations.rules?.length ?? 0
    validationRuleCount = rules
    steps.push({
      id: 'validations',
      kind: 'validations',
      type: 'validations',
      label: 'Validations',
      detail: `${rules} ${rules === 1 ? 'rule' : 'rules'} · on failure: ${validations.on_failure ?? 'fail'}`,
    })
  }

  const outputs: JobEndpoint[] = []
  outputsOf(pipeline).forEach((output: OutputSpec, index) => {
    output.transformations?.forEach((spec, child) => {
      transformationCount += 1
      steps.push(transformationStep(`o-${index}-t-${child}`, spec))
    })
    const endpoint = endpointOf(output)
    outputs.push(endpoint)
    steps.push({
      id: `o-${index}`,
      kind: 'output',
      type: endpoint.format,
      label: formatLabel(endpoint.format),
      detail: endpoint.path,
    })
  })

  return {
    input,
    outputs,
    steps,
    transformationCount,
    hasValidations,
    validationRuleCount,
    compiled: true,
  }
}

/**
 * Degraded description for a job that does not compile yet. Node order on
 * the canvas is not execution order, so the steps follow the node list — enough
 * to recognise the file, and it disappears as soon as the graph compiles.
 */
function describeFromGraph(graph: StudioGraph, settings: JobSettings): JobDescription {
  const steps: JobStep[] = []
  const outputs: JobEndpoint[] = []
  let input: JobEndpoint | null = null
  let transformationCount = 0

  const live = graph.nodes.filter((node) => !isDisabled(node))

  for (const node of live.filter(isSourceNode)) {
    const endpoint = endpointOf(node.data)
    input ??= endpoint
    steps.push({
      id: node.id,
      kind: 'input',
      type: endpoint.format,
      label: formatLabel(endpoint.format),
      detail: endpoint.path,
    })
  }

  for (const node of live.filter(isTransformNode)) {
    transformationCount += 1
    steps.push(transformationStep(node.id, { type: node.data.transform, ...node.data.params }))
  }

  // One node per rule on the canvas, one `validations` block in the JSON: the
  // step reads like the compiled one, so a box does not change shape mid-edit.
  const validationRuleCount = live.filter(isValidationNode).length
  const hasValidations = validationRuleCount > 0
  if (hasValidations) {
    const onFailure = settings.validations?.onFailure ?? DEFAULT_VALIDATION_POLICY.onFailure
    steps.push({
      id: 'validations',
      kind: 'validations',
      type: 'validations',
      label: 'Validations',
      detail: `${validationRuleCount} ${validationRuleCount === 1 ? 'rule' : 'rules'} · on failure: ${onFailure}`,
    })
  }

  for (const node of live.filter(isSinkNode)) {
    const endpoint = endpointOf(node.data)
    outputs.push(endpoint)
    steps.push({
      id: node.id,
      kind: 'output',
      type: endpoint.format,
      label: formatLabel(endpoint.format),
      detail: endpoint.path,
    })
  }

  return {
    input,
    outputs,
    steps,
    transformationCount,
    hasValidations,
    validationRuleCount,
    compiled: false,
  }
}

/**
 * Describes one job the way the pipeline surfaces need it: from the compiled
 * pipeline when the graph compiles — exactly the JSON Sparquet would run — and
 * from the canvas nodes otherwise, so an unfinished file is still recognisable.
 */
export function describeJob(job: Job): JobDescription {
  const { pipeline } = compileGraph(job.graph, job.settings)
  return pipeline ? describeFromPipeline(pipeline) : describeFromGraph(job.graph, job.settings)
}

/* ----------------------------------------------------------------- order */

/**
 * Kahn's algorithm with an alphabetical tie-break, so the numbering is stable
 * between renders and machines. Nodes caught in a cycle keep their name order,
 * and are always emitted last — a cycle has no valid position in a sequence.
 *
 * Used by the pipeline editor to number the stages an author linked by hand.
 */
export function topologicalOrder(
  ids: readonly string[],
  nameById: ReadonlyMap<string, string>,
  edges: readonly { source: string; target: string }[],
): { ordered: string[]; cyclic: string[] } {
  const indegree = new Map(ids.map((id) => [id, 0]))
  const adjacency = new Map<string, string[]>()
  for (const edge of edges) {
    if (edge.source === edge.target) continue
    adjacency.set(edge.source, [...(adjacency.get(edge.source) ?? []), edge.target])
    indegree.set(edge.target, (indegree.get(edge.target) ?? 0) + 1)
  }

  const key = (id: string) => `${(nameById.get(id) ?? '').toLowerCase()} ${id}`
  const byName = (a: string, b: string) => key(a).localeCompare(key(b))

  const ready = ids.filter((id) => indegree.get(id) === 0).sort(byName)
  const ordered: string[] = []
  const placed = new Set<string>()

  while (ready.length > 0) {
    const id = ready.shift() as string
    ordered.push(id)
    placed.add(id)
    let queued = false
    for (const next of adjacency.get(id) ?? []) {
      const remaining = (indegree.get(next) ?? 0) - 1
      indegree.set(next, remaining)
      if (remaining === 0) {
        ready.push(next)
        queued = true
      }
    }
    if (queued) ready.sort(byName)
  }

  // Whatever Kahn could not place is on (or downstream of) a cycle.
  const cyclic = ids.filter((candidate) => !placed.has(candidate)).sort(byName)
  ordered.push(...cyclic)
  return { ordered, cyclic }
}
