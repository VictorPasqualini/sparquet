/**
 * Inferred pipeline — the map of a workflow's pipeline FILES.
 *
 * A real pipeline is usually several JSONs, each one a stage. This module turns
 * a workflow's workflows into one box per file plus the links between them: an
 * edge exists where a file writes something another file reads back.
 *
 * It is documentation derived from the configs, never an orchestrator — Sparquet
 * still runs every JSON on its own, and nothing here executes anything.
 *
 * The canvas graph is the stored source of truth (a compiled `PipelineSpec` is
 * never persisted), so every box is read off `compileGraph` — exactly the JSON
 * Sparquet would execute. A job that does not compile yet (no destination,
 * dangling chain) still gets a box, read straight from its canvas nodes, so a
 * workflow is never half-drawn.
 */

import { getFormat, getTransformation } from '@/catalog'
import {
  compileGraph,
  isDisabled,
  isSinkNode,
  isSourceNode,
  isTransformNode,
  isValidationsNode,
} from '@/lib/compiler'
import type { OutputSpec, PipelineSpec, TransformationSpec } from '@/types/pipeline'
import { isIncludeDirective, outputsOf } from '@/types/pipeline'
import type { StudioGraph, Job } from '@/types/studio'

/* ------------------------------------------------------------------- types */

/** Where a step sits in the execution order of a single file. */
export type JobStepKind = 'input' | 'transformation' | 'validations' | 'output'

export interface JobStep {
  /** Unique inside its node; used as a React key. */
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

export interface JobSummary {
  jobId: string
  name: string
  /** 1-based reading order: topological where links allow, by name otherwise. */
  order: number
  /** The pipeline `input`. `null` when the file has no source yet. */
  input: JobEndpoint | null
  outputs: JobEndpoint[]
  /** Main + per-output transformations, matching the `transformation` steps. */
  transformationCount: number
  hasValidations: boolean
  /** How many `validations.rules` the file declares; 0 when it has no block. */
  validationRuleCount: number
  /** Every step in execution order: input → transformations → validations → outputs. */
  steps: JobStep[]
  /** `false` when the job does not compile and the box was read off the canvas. */
  compiled: boolean
}

/** How two files are linked: through storage, or through a Spark temp view. */
export type JobLinkVia = 'storage' | 'view'

export interface JobLink {
  id: string
  /** `jobId` of the file that writes. */
  source: string
  /** `jobId` of the file that reads. */
  target: string
  via: JobLinkVia
  /** The locations both sides name, as written in the source file. */
  locations: string[]
}

export interface InferredPipeline {
  nodes: JobSummary[]
  edges: JobLink[]
}

/* --------------------------------------------------------------- matching */

/**
 * Formats addressed by a location rather than by a server: one of them writing
 * where another reads is the same data, whatever the file format on top of it.
 * Everything else (jdbc, kafka, warehouses) must match exactly, or two unrelated
 * tables that happen to share a name would look connected.
 */
const FILE_FORMATS = new Set([
  'parquet',
  'csv',
  'json',
  'orc',
  'avro',
  'txt',
  'xml',
  'binary',
  'delta',
  'iceberg',
  'hudi',
])

const GLOBAL_VIEW_PREFIX = 'global_temp.'

/** Paths are compared case-insensitively: Windows and table names both are. */
function normalizePath(value: string): string {
  return value.trim().replace(/\\/g, '/').replace(/\/+$/, '').toLowerCase()
}

/** `global_temp.orders` and `orders` name the same view from different scopes. */
function normalizeViewName(value: string): string {
  const name = normalizePath(value)
  return name.startsWith(GLOBAL_VIEW_PREFIX) ? name.slice(GLOBAL_VIEW_PREFIX.length) : name
}

/**
 * Returns how a write and a read are linked, or `null` when they are unrelated.
 * A blank path never links — an unfinished node must not glue files together.
 */
export function linkBetween(write: JobEndpoint, read: JobEndpoint): JobLinkVia | null {
  const writeFormat = write.format.trim().toLowerCase()
  const readFormat = read.format.trim().toLowerCase()

  // Views live in the Spark session, not on storage, so they only match views.
  if (writeFormat === 'view' || readFormat === 'view') {
    if (writeFormat !== readFormat) return null
    const name = normalizeViewName(write.path)
    return name !== '' && name === normalizeViewName(read.path) ? 'view' : null
  }

  const location = normalizePath(write.path)
  if (location === '' || location !== normalizePath(read.path)) return null
  if (writeFormat === readFormat) return 'storage'
  return FILE_FORMATS.has(writeFormat) && FILE_FORMATS.has(readFormat) ? 'storage' : null
}

/* -------------------------------------------------------------- describing */

/**
 * What one pipeline file reads, writes and does — everything a box needs to be
 * recognised without opening it. Shared by the read-only workflow map and by the
 * pipeline editor, so a stage and a file are always described the same way.
 */
export interface JobDescription {
  input: JobEndpoint | null
  outputs: JobEndpoint[]
  /** Every location the file reads: the `input` plus each join/union `with`. */
  reads: JobEndpoint[]
  steps: JobStep[]
  transformationCount: number
  hasValidations: boolean
  validationRuleCount: number
  /** `false` when the job does not compile and the description came off the canvas. */
  compiled: boolean
}

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

  const side = isRecord(spec.with) ? asText(spec.with.path) : ''
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

/** The `with` sources of a join/union, including the ones nested in a sub-chain. */
function collectSideSources(
  transformations: TransformationSpec[] | undefined,
  into: JobEndpoint[],
): void {
  for (const spec of transformations ?? []) {
    if (isIncludeDirective(spec)) continue
    if (isRecord(spec.with)) into.push(endpointOf(spec.with))
    if (Array.isArray(spec.with_transformations)) {
      collectSideSources(spec.with_transformations as TransformationSpec[], into)
    }
  }
}

function describeFromPipeline(pipeline: PipelineSpec): JobDescription {
  const steps: JobStep[] = []
  const reads: JobEndpoint[] = []
  let transformationCount = 0

  const input = endpointOf(pipeline.input ?? {})
  reads.push(input)
  steps.push({
    id: 'input',
    kind: 'input',
    type: input.format,
    label: formatLabel(input.format),
    detail: input.path,
  })

  const transformations = pipeline.transformations ?? []
  collectSideSources(transformations, reads)
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
    collectSideSources(output.transformations, reads)
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
    reads,
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
function describeFromGraph(graph: StudioGraph): JobDescription {
  const steps: JobStep[] = []
  const reads: JobEndpoint[] = []
  const outputs: JobEndpoint[] = []
  let transformationCount = 0

  const live = graph.nodes.filter((node) => !isDisabled(node))

  for (const node of live.filter(isSourceNode)) {
    const endpoint = endpointOf(node.data)
    reads.push(endpoint)
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

  const hasValidations = live.some(isValidationsNode)
  let validationRuleCount = 0
  for (const node of live.filter(isValidationsNode)) {
    const rules = node.data.rules?.length ?? 0
    validationRuleCount += rules
    steps.push({
      id: node.id,
      kind: 'validations',
      type: 'validations',
      label: 'Validations',
      detail: `${rules} ${rules === 1 ? 'rule' : 'rules'} · on failure: ${node.data.onFailure}`,
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
    input: reads[0] ?? null,
    outputs,
    reads,
    steps,
    transformationCount,
    hasValidations,
    validationRuleCount,
    compiled: false,
  }
}

/**
 * Describes one job the way both pipeline surfaces need it: from the compiled
 * pipeline when the graph compiles — exactly the JSON Sparquet would run — and
 * from the canvas nodes otherwise, so an unfinished file is still recognisable.
 */
export function describeJob(job: Job): JobDescription {
  const { pipeline } = compileGraph(job.graph, job.settings)
  return pipeline ? describeFromPipeline(pipeline) : describeFromGraph(job.graph)
}

/* ----------------------------------------------------------------- order */

/**
 * Kahn's algorithm with an alphabetical tie-break, so the numbering is stable
 * between renders and machines. Nodes caught in a cycle keep their name order,
 * and are always emitted last — a cycle has no valid position in a sequence.
 *
 * Shared with the pipeline editor: same contract, whether the edges were
 * inferred from paths or drawn by hand.
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

  const key = (id: string) => `${(nameById.get(id) ?? '').toLowerCase()} ${id}`
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

/* --------------------------------------------------------------- derive */

/**
 * Builds the file-level pipeline of a workflow. Pure: no React, no store, no IO —
 * hand it the jobs of one workflow and it returns the boxes and the links.
 *
 * Two files are linked when
 *  - `storage`: an output path of A is an input path of B (trimmed, trailing
 *    slashes dropped, compared case-insensitively) and the formats can address
 *    the same location, or
 *  - `view`: A writes a temp view B reads, matching `global_temp.x` with `x`.
 *
 * Reads include the `with` source of a join/union, because reading a location
 * for a lookup is still reading it. Anything else stays unconnected — a link is
 * never guessed from names.
 */
export function deriveInferredPipeline(jobs: readonly Job[]): InferredPipeline {
  const described = jobs.map((job) => ({
    job,
    description: describeJob(job),
  }))

  const edges: JobLink[] = []
  const byPair = new Map<string, JobLink>()

  for (const writer of described) {
    for (const reader of described) {
      if (writer.job.id === reader.job.id) continue
      for (const write of writer.description.outputs) {
        for (const read of reader.description.reads) {
          const via = linkBetween(write, read)
          if (!via) continue
          const id = `${writer.job.id}->${reader.job.id}:${via}`
          const existing = byPair.get(id)
          const location = write.path.trim()
          if (existing) {
            if (!existing.locations.includes(location)) existing.locations.push(location)
            continue
          }
          const edge: JobLink = {
            id,
            source: writer.job.id,
            target: reader.job.id,
            via,
            locations: [location],
          }
          byPair.set(id, edge)
          edges.push(edge)
        }
      }
    }
  }

  const nameById = new Map(described.map((entry) => [entry.job.id, entry.job.name]))
  const { ordered } = topologicalOrder(
    described.map((entry) => entry.job.id),
    nameById,
    edges,
  )
  const orderById = new Map(ordered.map((id, index) => [id, index + 1]))

  const nodes: JobSummary[] = described
    .map(({ job, description }) => ({
      jobId: job.id,
      name: job.name,
      order: orderById.get(job.id) ?? 0,
      input: description.input,
      outputs: description.outputs,
      transformationCount: description.transformationCount,
      hasValidations: description.hasValidations,
      validationRuleCount: description.validationRuleCount,
      steps: description.steps,
      compiled: description.compiled,
    }))
    .sort((a, b) => a.order - b.order)

  return { nodes, edges }
}
