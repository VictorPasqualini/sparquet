/**
 * Lineage: which datasets a Job reads, which it writes, and who else touches them.
 *
 * Read off the CANVAS, not off the compiled JSON. A graph that does not compile —
 * no destination yet, a chain left dangling — still says where its data comes
 * from, and that is often the job somebody opened the lineage screen to find. The
 * dataset set is the same one the runner persists per execution
 * (`server/history.py:lineage_of` → `job_run.lineage`): the pipeline input, every
 * source feeding a join or a union, every destination, and the datasets the
 * validations block writes (report and quarantine).
 *
 * The link between two Jobs is the ADDRESS. Nobody declares "job B runs after job
 * A": B reads the path A wrote, and that is the whole edge. So the address is the
 * identity here, and a dataset is whatever a path/table/topic names — one row on
 * the screen, however many jobs mention it.
 *
 * What this does NOT do, deliberately, and where it would go:
 *   - column-level lineage (which output column came from which input column) —
 *     needs the transformation chain read expression by expression;
 *   - runtime lineage — `job_run.lineage` says what actually ran, with dates
 *     substituted into `{param}`; this reads the JSON as written, so an address
 *     with a parameter in it stays parameterized, and two runs of the same job
 *     are one dataset here.
 */

import {
  HANDLE,
  type Job,
  type StudioEdge,
  type StudioGraph,
  type StudioNode,
} from '@/types/studio'

/**
 * Where a dataset sits in relation to a Job. Mirrors the roles the runner stores,
 * plus `union` — the framework reads a union's second source exactly like a
 * join's, so hiding it would lose a real edge.
 */
export type LineageRole =
  | 'input'
  | 'join'
  | 'union'
  | 'output'
  | 'validation:report'
  | 'validation:valid'
  | 'validation:invalid'

/** Roles that read. Everything else writes. */
const READ_ROLES: ReadonlySet<LineageRole> = new Set<LineageRole>(['input', 'join', 'union'])

export function isRead(role: LineageRole): boolean {
  return READ_ROLES.has(role)
}

/**
 * Where a dataset lives, in the order the formats prefer to say it. Same list as
 * the runner's `_ADDRESS_KEYS`: Kafka hides its topic in `options`, JDBC its table
 * in `dbtable`, which is why the lookup falls through to the options map.
 */
const ADDRESS_KEYS = [
  'path',
  'table',
  'view',
  'view_name',
  'topic',
  'dbtable',
  'url',
  'uri',
] as const

/** One end of the lineage: a dataset as one node of one Job names it. */
export interface LineageEndpoint {
  role: LineageRole
  format: string
  address: string
  /** Write mode of a destination (`overwrite`, `append`, `merge`, …). */
  mode?: string
  /** The canvas node, so the screen can point at the box that says this. */
  nodeId: string
  /** Node label, when the author gave it one. */
  label?: string
}

/** What one Job touches. */
export interface JobLineage {
  jobId: string
  jobName: string
  workflowId: string
  reads: LineageEndpoint[]
  writes: LineageEndpoint[]
}

/** One Job's mention of a dataset, as listed under that dataset. */
export interface DatasetMention {
  jobId: string
  jobName: string
  workflowId: string
  role: LineageRole
  format: string
  mode?: string
  nodeId: string
}

/**
 * How a dataset sits in the library as a whole:
 *   external     — read, never written here: it comes from outside the Studio.
 *   intermediate — written by one Job and read by another: a real handoff.
 *   terminal     — written, never read back: the end of a chain.
 *   isolated     — mentioned once, by nothing else. Usually a typo in an address,
 *                  or a Job nobody has wired up yet.
 */
export type DatasetPlace = 'external' | 'intermediate' | 'terminal' | 'isolated'

export interface LineageDataset {
  /**
   * Normalized address — the identity two Jobs are joined by, and what the screen
   * shows. Not the raw string any one node wrote: two Jobs that disagree about a
   * trailing slash mean the same directory, and one of the two spellings has to
   * win for the row to be one row. The raw string stays on the endpoint.
   */
  key: string
  /** Every format used to name it. More than one is worth looking at. */
  formats: string[]
  producers: DatasetMention[]
  consumers: DatasetMention[]
  place: DatasetPlace
  /**
   * True when every mention uses the `view` format: a temporary view lives in the
   * SparkSession, not in storage, so the handoff only holds inside one run.
   */
  sessionScoped: boolean
  /**
   * Every workflow that touches it, sorted. More than one is the interesting
   * case: the dataset is a contract between two workflows, and whoever owns the
   * writing side cannot see the reading side from inside their own workflow.
   */
  workflowIds: string[]
}

/** A dependency between two Jobs, and the dataset that creates it. */
export interface JobEdge {
  from: string
  to: string
  datasetKey: string
}

export interface LineageIndex {
  jobs: JobLineage[]
  datasets: LineageDataset[]
  edges: JobEdge[]
}

/**
 * Identity of a dataset. Trailing slashes go — `/data/orders` and `/data/orders/`
 * are one directory — and the rest is kept exactly: paths are case-sensitive
 * wherever this runs for real, and folding case here would merge two datasets that
 * the storage keeps apart.
 */
export function datasetKey(address: string): string {
  const trimmed = address.trim()
  const withoutTrailing = trimmed.replace(/\/+$/, '')
  return withoutTrailing.length > 0 ? withoutTrailing : trimmed
}

function addressOf(path: string, options: Record<string, unknown> | undefined): string {
  if (typeof path === 'string' && path.trim() !== '') return path.trim()
  if (!options) return ''
  for (const key of ADDRESS_KEYS) {
    const value = options[key]
    if (typeof value === 'string' && value.trim() !== '') return value.trim()
  }
  return ''
}

/**
 * Whether a source feeds the trunk or the second handle of a join/union — and
 * `null` when the step it feeds is muted, because a disabled join reads nothing.
 *
 * The answer is not one hop away. A join's `with_transformations` become their own
 * chain of nodes on the canvas, so the second source of a join that filters its
 * right side reads `source → filter → select → join(in-right)`. What marks a side
 * input is the `in-right` handle SOMEWHERE ahead of the source, never the first
 * edge out of it; the trunk reaches every step through `in` and so never crosses
 * one.
 */
function sourceRole(
  node: StudioNode,
  byId: Map<string, StudioNode>,
  outgoing: Map<string, StudioEdge[]>,
): LineageRole | null {
  const seen = new Set<string>([node.id])
  const queue = [node.id]

  while (queue.length > 0) {
    const current = queue.pop() as string
    for (const edge of outgoing.get(current) ?? []) {
      if (edge.targetHandle === HANDLE.inRight) {
        const target = byId.get(edge.target)
        if (!target || target.data.kind !== 'transform') continue
        if (target.data.disabled) return null
        return target.data.transform === 'union' ? 'union' : 'join'
      }
      if (seen.has(edge.target)) continue
      seen.add(edge.target)
      queue.push(edge.target)
    }
  }
  return 'input'
}

const DQ_ROLE: Record<string, LineageRole> = {
  report: 'validation:report',
  valid: 'validation:valid',
  invalid: 'validation:invalid',
}

/** The endpoints one canvas declares, in node order. */
export function endpointsOfGraph(graph: StudioGraph): LineageEndpoint[] {
  const endpoints: LineageEndpoint[] = []
  const byId = new Map(graph.nodes.map((node) => [node.id, node]))
  const outgoing = new Map<string, StudioEdge[]>()
  for (const edge of graph.edges) {
    const list = outgoing.get(edge.source)
    if (list) list.push(edge)
    else outgoing.set(edge.source, [edge])
  }

  for (const node of graph.nodes) {
    if (node.data.kind === 'source') {
      const role = sourceRole(node, byId, outgoing)
      if (role === null) continue
      const address = addressOf(node.data.path, node.data.options)
      if (address === '') continue
      endpoints.push({
        role,
        format: node.data.format,
        address,
        nodeId: node.id,
        label: node.data.label,
      })
      continue
    }

    if (node.data.kind === 'sink') {
      const address = addressOf(node.data.path, node.data.options)
      if (address === '') continue
      const role = node.data.dqRole ? DQ_ROLE[node.data.dqRole] : 'output'
      endpoints.push({
        role: role ?? 'output',
        format: node.data.format,
        address,
        mode: node.data.mode ? String(node.data.mode) : undefined,
        nodeId: node.id,
        label: node.data.label,
      })
    }
  }

  return endpoints
}

/** What one Job reads and writes. */
export function lineageOfJob(job: Job): JobLineage {
  const endpoints = endpointsOfGraph(job.graph)
  return {
    jobId: job.id,
    jobName: job.name,
    workflowId: job.workflowId,
    reads: endpoints.filter((endpoint) => isRead(endpoint.role)),
    writes: endpoints.filter((endpoint) => !isRead(endpoint.role)),
  }
}

function placeOf(dataset: Omit<LineageDataset, 'place'>): DatasetPlace {
  const produced = dataset.producers.length > 0
  const consumed = dataset.consumers.length > 0
  if (produced && consumed) return 'intermediate'
  if (produced) return 'terminal'
  if (consumed) return 'external'
  return 'isolated'
}

/** The workflows on either side of a dataset, deduped and sorted. */
function workflowsOf(dataset: LineageDataset): string[] {
  const ids = new Set<string>()
  for (const mention of [...dataset.producers, ...dataset.consumers]) {
    ids.add(mention.workflowId)
  }
  return [...ids].sort()
}

/**
 * The whole library as datasets and the Jobs around them.
 *
 * Jobs arrive in whatever order the library holds them; datasets come back sorted
 * by key so the screen is stable between renders and between sessions.
 */
export function buildLineage(jobs: readonly Job[]): LineageIndex {
  const perJob = jobs.map(lineageOfJob)
  const byKey = new Map<string, LineageDataset>()

  const touch = (key: string, format: string) => {
    let dataset = byKey.get(key)
    if (!dataset) {
      dataset = {
        key,
        formats: [],
        producers: [],
        consumers: [],
        place: 'isolated',
        sessionScoped: true,
        workflowIds: [],
      }
      byKey.set(key, dataset)
    }
    if (!dataset.formats.includes(format)) dataset.formats.push(format)
    if (format !== 'view') dataset.sessionScoped = false
    return dataset
  }

  for (const job of perJob) {
    const mention = (endpoint: LineageEndpoint): DatasetMention => ({
      jobId: job.jobId,
      jobName: job.jobName,
      workflowId: job.workflowId,
      role: endpoint.role,
      format: endpoint.format,
      mode: endpoint.mode,
      nodeId: endpoint.nodeId,
    })

    for (const endpoint of job.reads) {
      touch(datasetKey(endpoint.address), endpoint.format).consumers.push(mention(endpoint))
    }
    for (const endpoint of job.writes) {
      touch(datasetKey(endpoint.address), endpoint.format).producers.push(mention(endpoint))
    }
  }

  const datasets = [...byKey.values()]
    .map((dataset) => ({
      ...dataset,
      place: placeOf(dataset),
      workflowIds: workflowsOf(dataset),
    }))
    .sort((a, b) => a.key.localeCompare(b.key))

  const edges: JobEdge[] = []
  for (const dataset of datasets) {
    for (const producer of dataset.producers) {
      for (const consumer of dataset.consumers) {
        // A job that reads back what it wrote is a cycle on the canvas, not a
        // dependency between jobs — the edge would only add noise.
        if (producer.jobId === consumer.jobId) continue
        edges.push({ from: producer.jobId, to: consumer.jobId, datasetKey: dataset.key })
      }
    }
  }

  return { jobs: perJob, datasets, edges }
}

/** Datasets one Job touches, for the panel that opens beside a Job. */
export function datasetsOfJob(index: LineageIndex, jobId: string): LineageDataset[] {
  return index.datasets.filter(
    (dataset) =>
      dataset.producers.some((mention) => mention.jobId === jobId) ||
      dataset.consumers.some((mention) => mention.jobId === jobId),
  )
}

/* ------------------------------------------------------------------ trace */

/**
 * Node ids for the lineage graph. Datasets and Jobs live in one namespace there —
 * a path and a Job id could collide — so each keeps its own prefix.
 */
export const datasetNodeId = (key: string): string => `dataset:${key}`
export const jobNodeId = (jobId: string): string => `job:${jobId}`

/** Everything reachable from one node, in each direction. */
export interface LineageTrace {
  /** Node ids that feed the seed, transitively. Excludes the seed. */
  upstream: Set<string>
  /** Node ids the seed feeds, transitively. Excludes the seed. */
  downstream: Set<string>
  /** Upstream, downstream and the seed itself — the whole path through it. */
  all: Set<string>
}

/**
 * The whole path a dataset or a Job sits on: where its data came from and
 * everywhere it goes.
 *
 * A cycle — a Job that rewrites what it reads, or two Jobs feeding each other —
 * terminates like any other path, because a node already visited is not walked
 * twice.
 */
export function traceFrom(index: LineageIndex, seed: string): LineageTrace {
  const up = new Map<string, string[]>()
  const down = new Map<string, string[]>()

  const link = (from: string, to: string) => {
    const forward = down.get(from)
    if (forward) forward.push(to)
    else down.set(from, [to])
    const back = up.get(to)
    if (back) back.push(from)
    else up.set(to, [from])
  }

  for (const job of index.jobs) {
    const node = jobNodeId(job.jobId)
    for (const endpoint of job.reads) link(datasetNodeId(datasetKey(endpoint.address)), node)
    for (const endpoint of job.writes) link(node, datasetNodeId(datasetKey(endpoint.address)))
  }

  const walk = (edges: Map<string, string[]>): Set<string> => {
    const seen = new Set<string>()
    const queue = [seed]
    while (queue.length > 0) {
      const current = queue.pop() as string
      for (const next of edges.get(current) ?? []) {
        if (next === seed || seen.has(next)) continue
        seen.add(next)
        queue.push(next)
      }
    }
    return seen
  }

  const upstream = walk(up)
  const downstream = walk(down)
  return { upstream, downstream, all: new Set([seed, ...upstream, ...downstream]) }
}
