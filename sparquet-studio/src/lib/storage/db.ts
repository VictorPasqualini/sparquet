/**
 * Local persistence for projects and workflows.
 *
 * IndexedDB (through idb-keyval) is the primary backend; when it is unavailable —
 * Safari/Firefox private windows, blocked storage, SSR — the same API runs on
 * localStorage, and finally on an in-memory map so the app never crashes on boot.
 *
 * Every record lives under its own key, so a save touches exactly one entry and a
 * crash mid-write can never corrupt unrelated records. The AI API key is NOT stored
 * here: it belongs to the settings store (`sparquet-studio:settings`).
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

import type { Project, StudioGraph, Workflow } from '@/types/studio'

/* ------------------------------------------------------------------- keys */

export const STORAGE_PREFIX = 'sparquet-studio:'

/** Records are namespaced under the prefix so `clearAll` never touches settings. */
const NS = `${STORAGE_PREFIX}db:`
const META_PREFIX = `${NS}meta:`
const PROJECT_PREFIX = `${NS}project:`
const WORKFLOW_PREFIX = `${NS}workflow:`

const KEY = {
  version: `${META_PREFIX}version`,
  seeded: `${META_PREFIX}seeded`,
  probe: `${META_PREFIX}probe`,
  backup: `${NS}backup`,
  project: (id: string) => `${PROJECT_PREFIX}${id}`,
  workflow: (id: string) => `${WORKFLOW_PREFIX}${id}`,
} as const

const IDB_NAME = 'sparquet-studio'
const IDB_STORE = 'records'

export const APP_ID = 'sparquet-studio'

/** Bumped whenever the persisted record shape changes; drives `migrate()`. */
export const STORAGE_VERSION = 1

/* --------------------------------------------------------------- backends */

export type StorageKind = 'indexeddb' | 'localstorage' | 'memory'

interface StorageBackend {
  kind: StorageKind
  get(key: string): Promise<unknown>
  set(key: string, value: unknown): Promise<void>
  del(key: string): Promise<void>
  keys(prefix: string): Promise<string[]>
}

/** Guarantees the value survives structured clone and strips `undefined` holes. */
function toStorable(value: unknown): unknown {
  if (value === undefined) return undefined
  return JSON.parse(JSON.stringify(value)) as unknown
}

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

let backendPromise: Promise<StorageBackend> | null = null

function backend(): Promise<StorageBackend> {
  if (!backendPromise) {
    backendPromise = (async () => {
      return (await indexedDbBackend()) ?? localStorageBackend() ?? memoryBackend()
    })()
  }
  return backendPromise
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
  projects: Project[]
  workflows: Workflow[]
}

export interface ImportSummary {
  projects: number
  workflows: number
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

/** One version hop. Add a `case` here whenever `STORAGE_VERSION` is bumped. */
async function migrateStep(store: StorageBackend, version: number): Promise<number> {
  switch (version) {
    case 0:
      // Records written before versioning already match the v1 shape.
      return 1
    default:
      throw new Error(
        `No migration path from storage version ${version} (${store.kind} backend).`,
      )
  }
}

/* --------------------------------------------------------------- projects */

export async function listProjects(): Promise<Project[]> {
  const store = await open()
  return readProjects(store)
}

export async function getProject(id: string): Promise<Project | null> {
  const store = await open()
  const value = await store.get(KEY.project(id))
  return isProject(value) ? value : null
}

export async function saveProject(project: Project): Promise<Project> {
  const store = await open()
  const record: Project = { ...project }
  await store.set(KEY.project(record.id), record)
  return record
}

/** Deletes the project and every workflow that belongs to it. */
export async function deleteProject(id: string): Promise<void> {
  const store = await open()
  const owned = await readWorkflows(store, id)
  await Promise.all(owned.map((workflow) => store.del(KEY.workflow(workflow.id))))
  await store.del(KEY.project(id))
}

/* -------------------------------------------------------------- workflows */

export async function listWorkflows(projectId?: string): Promise<Workflow[]> {
  const store = await open()
  return readWorkflows(store, projectId)
}

export async function getWorkflow(id: string): Promise<Workflow | null> {
  const store = await open()
  const value = await store.get(KEY.workflow(id))
  return isWorkflow(value) ? value : null
}

export async function saveWorkflow(workflow: Workflow): Promise<Workflow> {
  const store = await open()
  const record: Workflow = { ...workflow }
  await store.set(KEY.workflow(record.id), record)
  return record
}

export async function deleteWorkflow(id: string): Promise<void> {
  const store = await open()
  await store.del(KEY.workflow(id))
}

/**
 * Copies a workflow into the same project. `name` is a request: when it is already
 * taken the copy becomes `name (2)`, `name (3)`, and so on.
 */
export async function duplicateWorkflow(id: string, name?: string): Promise<Workflow | null> {
  const store = await open()
  const source = await store.get(KEY.workflow(id))
  if (!isWorkflow(source)) return null

  const siblings = await readWorkflows(store, source.projectId)
  const now = Date.now()
  const copy: Workflow = {
    ...cloneRecord(source),
    id: nanoid(10),
    name: uniqueName(
      name ?? `Copy of ${source.name}`,
      siblings.map((workflow) => workflow.name),
    ),
    createdAt: now,
    updatedAt: now,
    revision: 1,
  }

  await store.set(KEY.workflow(copy.id), copy)
  return copy
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
 * puts the previous projects and workflows back before it rethrows.
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
      `Import failed; your previous projects and workflows were restored. ${messageOf(error)}`,
    )
  }

  return {
    projects: parsed.bundle.projects.length,
    workflows: parsed.bundle.workflows.length,
    skipped: parsed.skipped,
    merged: merge,
  }
}

/** Removes every project, workflow and backup. Meta keys survive by design. */
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
  return keys.some((key) => key.startsWith(PROJECT_PREFIX) || key.startsWith(WORKFLOW_PREFIX))
}

async function readProjects(store: StorageBackend): Promise<Project[]> {
  const keys = await store.keys(PROJECT_PREFIX)
  const values = await Promise.all(keys.map((key) => store.get(key)))
  const projects = values.filter(isProject)
  warnUnreadable(values.length - projects.length, 'project')
  return projects.sort(byRecency)
}

async function readWorkflows(store: StorageBackend, projectId?: string): Promise<Workflow[]> {
  const keys = await store.keys(WORKFLOW_PREFIX)
  const values = await Promise.all(keys.map((key) => store.get(key)))
  const workflows = values.filter(isWorkflow)
  warnUnreadable(values.length - workflows.length, 'workflow')
  const scoped = projectId ? workflows.filter((w) => w.projectId === projectId) : workflows
  return scoped.sort(byRecency)
}

/** Dropped records are invisible in the UI; the console is the only trace left. */
function warnUnreadable(count: number, label: string): void {
  if (count > 0) {
    console.warn(`Sparquet Studio: ignored ${count} unreadable ${label} record(s) in storage.`)
  }
}

async function readBundle(store: StorageBackend, version: number): Promise<StudioBundle> {
  const [projects, workflows] = await Promise.all([readProjects(store), readWorkflows(store)])
  return { app: APP_ID, version, exportedAt: Date.now(), projects, workflows }
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
        .filter((key) => key.startsWith(PROJECT_PREFIX) || key.startsWith(WORKFLOW_PREFIX))
        .map((key) => store.del(key)),
    )
  }
  await Promise.all([
    ...bundle.projects.map((project) => store.set(KEY.project(project.id), project)),
    ...bundle.workflows.map((workflow) => store.set(KEY.workflow(workflow.id), workflow)),
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

  if (!Array.isArray(value.projects) && !Array.isArray(value.workflows)) {
    throw new Error('Invalid bundle: expected a `projects` or `workflows` array.')
  }

  const rawProjects = Array.isArray(value.projects) ? value.projects : []
  const rawWorkflows = Array.isArray(value.workflows) ? value.workflows : []
  const projects = rawProjects.filter(isProject)
  const workflows = rawWorkflows.filter(isWorkflow)

  return {
    bundle: {
      app: typeof value.app === 'string' ? value.app : APP_ID,
      version,
      exportedAt: typeof value.exportedAt === 'number' ? value.exportedAt : Date.now(),
      projects,
      workflows,
    },
    skipped: rawProjects.length - projects.length + (rawWorkflows.length - workflows.length),
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

function isProject(value: unknown): value is Project {
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
function isWorkflow(value: unknown): value is Workflow {
  return (
    isRecord(value) &&
    typeof value.id === 'string' &&
    typeof value.projectId === 'string' &&
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
