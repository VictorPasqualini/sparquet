/**
 * Persistence for workflows, jobs and pipelines.
 *
 * The library lives in the runner's WORKSPACE — real JSON files on disk, one per
 * record, that a user can diff, review and commit. That is the primary backend
 * (`remote.ts`). Browser storage is what is left when the runner is not running:
 * IndexedDB, then localStorage, then an in-memory map so the app never crashes
 * on boot. Those are a fallback, not the store — the browser cache is not a place
 * to keep work that took an afternoon to build.
 *
 * Every record lives under its own key, so a save touches exactly one entry and a
 * crash mid-write can never corrupt unrelated records. The AI API key is NOT stored
 * here: it belongs to the settings store (`sparquet-studio:settings`).
 *
 * Key names live in `keys.ts` and are frozen; the record FIELDS moved to the
 * current vocabulary in storage v3, where `upgradeJob` rewrites them in place.
 */

import {
  createStore,
  del as idbDel,
  get as idbGet,
  keys as idbKeys,
  set as idbSet,
  type UseStore,
} from 'idb-keyval'
import { nanoid } from 'nanoid'

import { upgradeJob } from '@/lib/storage/migrations'
import { toStorable, type StorageBackend, type StorageKind } from '@/lib/storage/backend'
import {
  FLOW_PREFIX,
  KEY,
  META_PREFIX,
  NS,
  PROJECT_PREFIX,
  STORAGE_PREFIX,
  WORKFLOW_PREFIX,
} from '@/lib/storage/keys'
import { workspaceBackend } from '@/lib/storage/remote'
import type { Pipeline, Workflow, StudioGraph, Job } from '@/types/studio'

/* ------------------------------------------------------------------- keys */

export { STORAGE_PREFIX }
export type { StorageKind }

const IDB_NAME = 'sparquet-studio'
const IDB_STORE = 'records'

export const APP_ID = 'sparquet-studio'

/** Bumped whenever the persisted record shape changes; drives `migrate()`. */
export const STORAGE_VERSION = 7

/* --------------------------------------------------------------- backends */

async function indexedDbBackend(): Promise<StorageBackend | null> {
  let store: UseStore
  try {
    store = createStore(IDB_NAME, IDB_STORE)
    // Private-mode failures surface on the first real operation, not on open.
    await idbGet<unknown>(KEY.probe, store)
  } catch {
    return null
  }

  return {
    kind: 'indexeddb',
    get: (key) => idbGet<unknown>(key, store),
    set: async (key, value) => {
      await idbSet(key, toStorable(value), store)
    },
    del: (key) => idbDel(key, store),
    keys: async (prefix) => {
      const all = await idbKeys(store)
      return all.filter(
        (key): key is string => typeof key === 'string' && key.startsWith(prefix),
      )
    },
  }
}

function localStorageBackend(): StorageBackend | null {
  try {
    const store = globalThis.localStorage as Storage | undefined
    if (!store) return null
    store.setItem(KEY.probe, '1')
    store.removeItem(KEY.probe)

    return {
      kind: 'localstorage',
      get: async (key) => {
        const raw = store.getItem(key)
        if (raw === null) return undefined
        try {
          return JSON.parse(raw) as unknown
        } catch {
          return undefined
        }
      },
      set: async (key, value) => {
        store.setItem(key, JSON.stringify(toStorable(value)))
      },
      del: async (key) => {
        store.removeItem(key)
      },
      keys: async (prefix) => {
        const found: string[] = []
        for (let index = 0; index < store.length; index += 1) {
          const key = store.key(index)
          if (key && key.startsWith(prefix)) found.push(key)
        }
        return found
      },
    }
  } catch {
    return null
  }
}

function memoryBackend(): StorageBackend {
  const map = new Map<string, unknown>()
  return {
    kind: 'memory',
    get: async (key) => map.get(key),
    set: async (key, value) => {
      map.set(key, toStorable(value))
    },
    del: async (key) => {
      map.delete(key)
    },
    keys: async (prefix) => [...map.keys()].filter((key) => key.startsWith(prefix)),
  }
}

/** Where to reach the workspace. Set by the app from the runner settings. */
let connection: { baseUrl?: string; token?: string } = {}

/**
 * Points storage at a runner. Called before the first read, and again whenever the
 * runner URL or token changes — the next call re-resolves the backend, so a user
 * who starts the runner after opening Studio gets their files without a reload.
 */
export function configureStorage(options: { baseUrl?: string; token?: string }): void {
  const changed =
    connection.baseUrl !== options.baseUrl || connection.token !== options.token
  connection = { ...options }
  if (changed) reconnectStorage()
}

/** Drops the resolved backend so the next operation picks one again. */
export function reconnectStorage(): void {
  backendPromise = null
  migrationPromise = null
}

let backendPromise: Promise<StorageBackend> | null = null

function backend(): Promise<StorageBackend> {
  if (!backendPromise) {
    backendPromise = (async () => {
      const remote = await workspaceBackend(connection)
      if (remote) {
        await adoptBrowserLibrary(remote)
        return remote
      }
      return (await indexedDbBackend()) ?? localStorageBackend() ?? memoryBackend()
    })()
  }
  return backendPromise
}

/**
 * Moves a library that only ever existed in this browser into an EMPTY workspace.
 *
 * Runs once, the first time a workspace answers: without it, everything built
 * before the runner existed would look deleted. It never overwrites — a workspace
 * that already holds a record is the source of truth and is left alone.
 */
async function adoptBrowserLibrary(remote: StorageBackend): Promise<void> {
  try {
    if ((await hasRecords(remote))) return
    const local = (await indexedDbBackend()) ?? localStorageBackend()
    if (!local || !(await hasRecords(local))) return

    const bundle = await readBundle(local, STORAGE_VERSION)
    await writeBundle(remote, bundle, true)
    const version = await local.get(KEY.version)
    if (typeof version === 'number') await remote.set(KEY.version, version)
    if ((await local.get(KEY.seeded)) === true) await remote.set(KEY.seeded, true)
    console.info(
      `Sparquet Studio: copied ${bundle.workflows.length} workflow(s), ${bundle.jobs.length} job(s) and ${bundle.pipelines.length} pipeline(s) from browser storage into the workspace.`,
    )
  } catch (error) {
    // The browser copy is still there and still readable next time. Failing the
    // adoption must not stop the workspace from being used.
    console.warn(`Sparquet Studio: could not copy browser storage into the workspace. ${messageOf(error)}`)
  }
}

/** Which backend actually took over; the UI warns when persistence is in-memory. */
export async function storageKind(): Promise<StorageKind> {
  return (await backend()).kind
}

/* ------------------------------------------------------------- migrations */

export interface StudioBundle {
  app: string
  version: number
  exportedAt: number
  workflows: Workflow[]
  jobs: Job[]
  /** Pipelines. Absent in bundles exported before storage v2. */
  pipelines: Pipeline[]
}

export interface ImportSummary {
  workflows: number
  jobs: number
  pipelines: number
  /** Records dropped because they did not match the expected shape. */
  skipped: number
  merged: boolean
}

let migrationPromise: Promise<number> | null = null

function ready(): Promise<number> {
  if (!migrationPromise) {
    migrationPromise = migrate().catch((error: unknown) => {
      // A transient failure must not brick the session: allow a later retry.
      migrationPromise = null
      throw error
    })
  }
  return migrationPromise
}

async function open(): Promise<StorageBackend> {
  await ready()
  return backend()
}

/**
 * Brings persisted records up to `STORAGE_VERSION`. No-op for the current version.
 * The previous bundle is copied to a backup key before anything is rewritten, and
 * restored if a step throws, so a failed migration cannot lose data.
 */
export async function migrate(): Promise<number> {
  const store = await backend()
  const raw = await store.get(KEY.version)
  const stored = typeof raw === 'number' ? raw : null
  const from = stored ?? ((await hasRecords(store)) ? 0 : STORAGE_VERSION)

  if (from > STORAGE_VERSION) {
    console.warn(
      `Sparquet Studio: stored data is v${from}, newer than this build (v${STORAGE_VERSION}). Leaving it untouched.`,
    )
    return from
  }

  if (from === STORAGE_VERSION) {
    if (stored !== STORAGE_VERSION) await store.set(KEY.version, STORAGE_VERSION)
    return STORAGE_VERSION
  }

  const backup = await readBundle(store, from)
  await store.set(KEY.backup, backup)

  try {
    let version = from
    while (version < STORAGE_VERSION) {
      version = await migrateStep(store, version)
    }
    await store.set(KEY.version, STORAGE_VERSION)
    return STORAGE_VERSION
  } catch (error) {
    await writeBundle(store, backup, false)
    throw new Error(
      `Storage migration from v${from} to v${STORAGE_VERSION} failed; the previous data was restored from the backup key. ${messageOf(error)}`,
    )
  }
}

/** Rewrites every stored job that `upgradeJob` actually changes. */
async function upgradeStoredJobs(store: StorageBackend): Promise<void> {
  const jobs = await readJobs(store)
  for (const job of jobs) {
    const upgraded = upgradeJob(job)
    if (upgraded !== job) await store.set(KEY.workflow(job.id), upgraded)
  }
}

/** One version hop. Add a `case` here whenever `STORAGE_VERSION` is bumped. */
async function migrateStep(store: StorageBackend, version: number): Promise<number> {
  switch (version) {
    case 0:
      // Records written before versioning already match the v1 shape.
      return 1
    case 1:
      // v2 only ADDS pipeline records; workflows and jobs are untouched.
      return 2
    case 2: {
      // v3 turns the single `validations` node into one node per rule and moves the
      // block-level policy (on_failure, report, quarantine outputs) into the job
      // settings. Jobs that never had a validations node are left byte-identical.
      await upgradeStoredJobs(store)
      return 3
    }
    case 3: {
      // v4 takes the three DATASETS back out of the settings — the quality report
      // and the two quarantine outputs become destination nodes on the canvas,
      // wired to the last rule. Only `on_failure` stays in the settings.
      await upgradeStoredJobs(store)
      return 4
    }
    case 4: {
      // v5 moves the role of those destinations off the EDGE and onto the node, and
      // drops the links: the validations block is job-scoped, so they belong to the
      // job rather than to whichever rule happened to be last.
      await upgradeStoredJobs(store)
      return 5
    }
    case 5: {
      // v6 puts a link BACK — but a different one. v5 was right about the JSON and
      // wrong on screen: with no handle at all, the report and the quarantine read as
      // destinations someone forgot to wire. The anchor lands on the plain input
      // handle, which the compiler reads nothing from, so the emitted JSON does not
      // change; scoping lives on its own `scope` handle and is untouched here.
      await upgradeStoredJobs(store)
      return 6
    }
    case 6: {
      // v7 rewrites a stored `check` node into the metric it measured: the wrapper was
      // removed from the engine, so a Job still holding it would compile a rule type
      // nothing answers to.
      await upgradeStoredJobs(store)
      return 7
    }
    default:
      throw new Error(
        `No migration path from storage version ${version} (${store.kind} backend).`,
      )
  }
}

/* --------------------------------------------------------------- workflows */

export async function listWorkflows(): Promise<Workflow[]> {
  const store = await open()
  return readWorkflows(store)
}

export async function getWorkflow(id: string): Promise<Workflow | null> {
  const store = await open()
  const value = await store.get(KEY.project(id))
  return isWorkflow(value) ? value : null
}

export async function saveWorkflow(workflow: Workflow): Promise<Workflow> {
  const store = await open()
  const record: Workflow = { ...workflow }
  await store.set(KEY.project(record.id), record)
  return record
}

/** Deletes the workflow and every job and pipeline that belongs to it. */
export async function deleteWorkflow(id: string): Promise<void> {
  const store = await open()
  const [jobs, pipelines] = await Promise.all([readJobs(store, id), readPipelines(store, id)])
  await Promise.all([
    ...jobs.map((job) => store.del(KEY.workflow(job.id))),
    ...pipelines.map((pipeline) => store.del(KEY.flow(pipeline.id))),
  ])
  await store.del(KEY.project(id))
}

/* -------------------------------------------------------------- jobs */

export async function listJobs(workflowId?: string): Promise<Job[]> {
  const store = await open()
  return readJobs(store, workflowId)
}

export async function getJob(id: string): Promise<Job | null> {
  const store = await open()
  const value = await store.get(KEY.workflow(id))
  return isJob(value) ? value : null
}

export async function saveJob(job: Job): Promise<Job> {
  const store = await open()
  const record: Job = { ...job }
  await store.set(KEY.workflow(record.id), record)
  return record
}

export async function deleteJob(id: string): Promise<void> {
  const store = await open()
  await store.del(KEY.workflow(id))
}

/**
 * Copies a job into the same workflow. `name` is a request: when it is already
 * taken the copy becomes `name (2)`, `name (3)`, and so on.
 */
export async function duplicateJob(id: string, name?: string): Promise<Job | null> {
  const store = await open()
  const source = await store.get(KEY.workflow(id))
  if (!isJob(source)) return null

  const siblings = await readJobs(store, source.workflowId)
  const now = Date.now()
  const copy: Job = {
    ...cloneRecord(source),
    id: nanoid(10),
    name: uniqueName(
      name ?? `Copy of ${source.name}`,
      siblings.map((job) => job.name),
    ),
    createdAt: now,
    updatedAt: now,
    revision: 1,
  }

  await store.set(KEY.workflow(copy.id), copy)
  return copy
}

/* ----------------------------------------------------------------- pipelines */

export async function listPipelines(workflowId?: string): Promise<Pipeline[]> {
  const store = await open()
  return readPipelines(store, workflowId)
}

export async function getPipeline(id: string): Promise<Pipeline | null> {
  const store = await open()
  const value = await store.get(KEY.flow(id))
  return isPipeline(value) ? value : null
}

export async function savePipeline(pipeline: Pipeline): Promise<Pipeline> {
  const store = await open()
  const record: Pipeline = { ...pipeline }
  await store.set(KEY.flow(record.id), record)
  return record
}

export async function deletePipeline(id: string): Promise<void> {
  const store = await open()
  await store.del(KEY.flow(id))
}

/* ---------------------------------------------------------- export/import */

export async function exportAll(): Promise<StudioBundle> {
  const store = await open()
  return readBundle(store, STORAGE_VERSION)
}

/**
 * Restores a bundle. `merge: true` (default) upserts by id and keeps everything
 * else; `merge: false` replaces the whole library, after copying the current
 * contents to the backup key. Either way the import is all-or-nothing: a failure
 * puts the previous workflows and jobs back before it rethrows.
 */
export async function importAll(
  bundle: unknown,
  options: { merge?: boolean } = {},
): Promise<ImportSummary> {
  const merge = options.merge !== false
  const store = await open()
  // The whole bundle is validated before a single key is touched: a replace wipes
  // the library, and a bundle that only fails later must never take it with it.
  const parsed = parseBundle(bundle)
  const backup = await readBundle(store, STORAGE_VERSION)

  if (!merge) await store.set(KEY.backup, backup)

  try {
    await writeBundle(store, parsed.bundle, merge)

    let version = parsed.bundle.version
    while (version < STORAGE_VERSION) {
      version = await migrateStep(store, version)
    }
    await store.set(KEY.version, STORAGE_VERSION)
  } catch (error) {
    await writeBundle(store, backup, false)
    throw new Error(
      `Import failed; your previous workflows and jobs were restored. ${messageOf(error)}`,
    )
  }

  return {
    workflows: parsed.bundle.workflows.length,
    jobs: parsed.bundle.jobs.length,
    pipelines: parsed.bundle.pipelines.length,
    skipped: parsed.skipped,
    merged: merge,
  }
}

/** Removes every workflow, job and backup. Meta keys survive by design. */
export async function clearAll(): Promise<void> {
  const store = await open()
  const keys = await store.keys(NS)
  await Promise.all(
    keys.filter((key) => !key.startsWith(META_PREFIX)).map((key) => store.del(key)),
  )
}

/* ------------------------------------------------------------------- seed */

export async function isSeeded(): Promise<boolean> {
  const store = await open()
  return (await store.get(KEY.seeded)) === true
}

export async function markSeeded(): Promise<void> {
  const store = await open()
  await store.set(KEY.seeded, true)
}

/* ---------------------------------------------------------------- helpers */

async function hasRecords(store: StorageBackend): Promise<boolean> {
  const keys = await store.keys(NS)
  return keys.some(
    (key) =>
      key.startsWith(PROJECT_PREFIX) ||
      key.startsWith(WORKFLOW_PREFIX) ||
      key.startsWith(FLOW_PREFIX),
  )
}

async function readWorkflows(store: StorageBackend): Promise<Workflow[]> {
  const keys = await store.keys(PROJECT_PREFIX)
  const values = await Promise.all(keys.map((key) => store.get(key)))
  const workflows = values.filter(isWorkflow)
  warnUnreadable(values.length - workflows.length, 'workflow')
  return workflows.sort(byRecency)
}

async function readJobs(store: StorageBackend, workflowId?: string): Promise<Job[]> {
  const keys = await store.keys(WORKFLOW_PREFIX)
  const values = await Promise.all(keys.map((key) => store.get(key)))
  const jobs = values.filter(isJob)
  warnUnreadable(values.length - jobs.length, 'job')
  const scoped = workflowId ? jobs.filter((w) => w.workflowId === workflowId) : jobs
  return scoped.sort(byRecency)
}

async function readPipelines(store: StorageBackend, workflowId?: string): Promise<Pipeline[]> {
  const keys = await store.keys(FLOW_PREFIX)
  const values = await Promise.all(keys.map((key) => store.get(key)))
  const pipelines = values.filter(isPipeline)
  warnUnreadable(values.length - pipelines.length, 'pipeline')
  const scoped = workflowId ? pipelines.filter((pipeline) => pipeline.workflowId === workflowId) : pipelines
  return scoped.sort(byRecency)
}

/** Dropped records are invisible in the UI; the console is the only trace left. */
function warnUnreadable(count: number, label: string): void {
  if (count > 0) {
    console.warn(`Sparquet Studio: ignored ${count} unreadable ${label} record(s) in storage.`)
  }
}

async function readBundle(store: StorageBackend, version: number): Promise<StudioBundle> {
  const [workflows, jobs, pipelines] = await Promise.all([
    readWorkflows(store),
    readJobs(store),
    readPipelines(store),
  ])
  return { app: APP_ID, version, exportedAt: Date.now(), workflows, jobs, pipelines }
}

async function writeBundle(
  store: StorageBackend,
  bundle: StudioBundle,
  merge: boolean,
): Promise<void> {
  if (!merge) {
    const keys = await store.keys(NS)
    await Promise.all(
      keys
        .filter(
          (key) =>
            key.startsWith(PROJECT_PREFIX) ||
            key.startsWith(WORKFLOW_PREFIX) ||
            key.startsWith(FLOW_PREFIX),
        )
        .map((key) => store.del(key)),
    )
  }
  await Promise.all([
    ...bundle.workflows.map((workflow) => store.set(KEY.project(workflow.id), workflow)),
    ...bundle.jobs.map((job) => store.set(KEY.workflow(job.id), job)),
    ...bundle.pipelines.map((pipeline) => store.set(KEY.flow(pipeline.id), pipeline)),
  ])
}

function parseBundle(value: unknown): { bundle: StudioBundle; skipped: number } {
  if (!isRecord(value)) throw new Error('Invalid bundle: expected a JSON object.')

  const version = typeof value.version === 'number' ? value.version : 0
  if (!Number.isInteger(version) || version < 0) {
    throw new Error(`Invalid bundle: \`version\` must be a whole number, got ${version}.`)
  }
  if (version > STORAGE_VERSION) {
    throw new Error(
      `This bundle was exported by a newer version of Studio (v${version} > v${STORAGE_VERSION}).`,
    )
  }

  if (
    !Array.isArray(value.workflows) &&
    !Array.isArray(value.jobs) &&
    !Array.isArray(value.pipelines)
  ) {
    throw new Error('Invalid bundle: expected a `workflows` or `jobs` array.')
  }

  const rawWorkflows = Array.isArray(value.workflows) ? value.workflows : []
  const rawJobs = Array.isArray(value.jobs) ? value.jobs : []
  // `pipelines` only exists from v2 on: an older bundle simply carries none.
  const rawPipelines = Array.isArray(value.pipelines) ? value.pipelines : []
  const workflows = rawWorkflows.filter(isWorkflow)
  const jobs = rawJobs.filter(isJob)
  const pipelines = rawPipelines.filter(isPipeline)

  return {
    bundle: {
      app: typeof value.app === 'string' ? value.app : APP_ID,
      version,
      exportedAt: typeof value.exportedAt === 'number' ? value.exportedAt : Date.now(),
      workflows,
      jobs,
      pipelines,
    },
    skipped:
      rawWorkflows.length -
      workflows.length +
      (rawJobs.length - jobs.length) +
      (rawPipelines.length - pipelines.length),
  }
}

function uniqueName(base: string, taken: string[]): string {
  if (!taken.includes(base)) return base
  let index = 2
  while (taken.includes(`${base} (${index})`)) index += 1
  return `${base} (${index})`
}

function byRecency<T extends { id: string; updatedAt: number }>(a: T, b: T): number {
  return b.updatedAt - a.updatedAt || a.id.localeCompare(b.id)
}

function cloneRecord<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isWorkflow(value: unknown): value is Workflow {
  return (
    isRecord(value) &&
    typeof value.id === 'string' &&
    typeof value.name === 'string' &&
    typeof value.createdAt === 'number' &&
    typeof value.updatedAt === 'number'
  )
}

/**
 * Every field checked here is dereferenced unguarded by the screens, so a record
 * that fails is dropped rather than allowed to crash the app on the next render.
 */
function isJob(value: unknown): value is Job {
  return (
    isRecord(value) &&
    typeof value.id === 'string' &&
    typeof value.workflowId === 'string' &&
    typeof value.name === 'string' &&
    typeof value.description === 'string' &&
    Array.isArray(value.tags) &&
    isRecord(value.settings) &&
    Array.isArray(value.params) &&
    isGraph(value.graph) &&
    typeof value.createdAt === 'number' &&
    typeof value.updatedAt === 'number'
  )
}

/**
 * A pipeline is only stages plus links, so every field the canvas walks is
 * checked here. `revision` is required: the editor derives the next one from it,
 * and a record without it would save `NaN` back over a good row.
 */
function isPipeline(value: unknown): value is Pipeline {
  return (
    isRecord(value) &&
    typeof value.id === 'string' &&
    typeof value.workflowId === 'string' &&
    typeof value.name === 'string' &&
    typeof value.description === 'string' &&
    Array.isArray(value.stages) &&
    value.stages.every(isPipelineStage) &&
    Array.isArray(value.links) &&
    // A link is `{id, source, target}`, exactly a graph edge's shape.
    value.links.every(isGraphEdge) &&
    typeof value.createdAt === 'number' &&
    typeof value.updatedAt === 'number' &&
    typeof value.revision === 'number'
  )
}

function isPipelineStage(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.id === 'string' &&
    typeof value.jobId === 'string' &&
    isRecord(value.position) &&
    typeof value.position.x === 'number' &&
    typeof value.position.y === 'number'
  )
}

function isGraph(value: unknown): value is StudioGraph {
  return (
    isRecord(value) &&
    Array.isArray(value.nodes) &&
    value.nodes.every(isGraphNode) &&
    Array.isArray(value.edges) &&
    value.edges.every(isGraphEdge)
  )
}

function isGraphNode(value: unknown): boolean {
  return isRecord(value) && typeof value.id === 'string' && isRecord(value.data)
}

function isGraphEdge(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.id === 'string' &&
    typeof value.source === 'string' &&
    typeof value.target === 'string'
  )
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
