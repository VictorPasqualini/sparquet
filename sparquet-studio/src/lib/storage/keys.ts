/**
 * The key names every storage backend addresses records by.
 *
 * They live here rather than in `db.ts` so a backend can translate them without
 * importing the module that chooses between backends.
 *
 * The physical names predate the current vocabulary and are deliberately left
 * alone — renaming them would strand every library already on disk:
 *
 *   `…:db:project:<id>`   holds a Workflow (the container)
 *   `…:db:workflow:<id>`  holds a Job (one pipeline JSON)
 *   `…:db:flow:<id>`      holds a Pipeline (an ordered set of Jobs)
 *   `…:db:query:<id>`     holds a saved SQL query
 */

export const STORAGE_PREFIX = 'sparquet-studio:'

/** Records are namespaced under the prefix so `clearAll` never touches settings. */
export const NS = `${STORAGE_PREFIX}db:`
export const META_PREFIX = `${NS}meta:`
export const PROJECT_PREFIX = `${NS}project:`
export const WORKFLOW_PREFIX = `${NS}workflow:`
export const FLOW_PREFIX = `${NS}flow:`
/** Added after the vocabulary settled, so this one says what it holds. */
export const QUERY_PREFIX = `${NS}query:`

export const KEY = {
  version: `${META_PREFIX}version`,
  /**
   * The data catalog: one record holding every dataset annotation, addressed by
   * dataset key. Under `meta:` on purpose — the workspace backend already writes
   * meta entries as files (`.studio/meta.json`), so the catalog persists through
   * the same route the library does without a fourth record kind on the server.
   */
  catalog: `${META_PREFIX}catalog`,
  /**
   * Access rules over datasets, Jobs and Pipelines. Beside the catalog and for
   * the same reason: the workspace backend already mirrors meta records into
   * `.studio/meta.json`, which is the file the runner re-reads to enforce them.
   */
  grants: `${META_PREFIX}grants`,
  /**
   * Who owns each dataset, Job, Pipeline and Workflow. A separate record from
   * the grants because it answers a different question and changes far less
   * often: a grant is traffic, ownership is the deed.
   */
  owners: `${META_PREFIX}owners`,
  /*
   * There is deliberately no `secrets` key.
   *
   * Every other meta record is mirrored into `.studio/meta.json`, which is a
   * plaintext file in the workspace, and is read back into IndexedDB in the
   * browser. Connection secrets live on the runner and are reached over
   * `/secrets`, which never returns a value at any level; `src/store/secrets.ts`
   * holds the names in memory and writes none of it to disk. Adding a key here
   * would undo that in one line, so the absence is the design.
   */
  seeded: `${META_PREFIX}seeded`,
  probe: `${META_PREFIX}probe`,
  backup: `${NS}backup`,
  project: (id: string) => `${PROJECT_PREFIX}${id}`,
  workflow: (id: string) => `${WORKFLOW_PREFIX}${id}`,
  flow: (id: string) => `${FLOW_PREFIX}${id}`,
  query: (id: string) => `${QUERY_PREFIX}${id}`,
} as const

/** The record kinds the workspace stores as files, in the server's vocabulary. */
export type RecordKind = 'workflow' | 'job' | 'pipeline' | 'query'

export interface KeyAddress {
  kind: RecordKind
  id: string
}

/** `…:db:flow:abc` -> `{kind: 'pipeline', id: 'abc'}`; null for meta and backup keys. */
export function addressOf(key: string): KeyAddress | null {
  if (key.startsWith(PROJECT_PREFIX)) {
    return { kind: 'workflow', id: key.slice(PROJECT_PREFIX.length) }
  }
  if (key.startsWith(WORKFLOW_PREFIX)) {
    return { kind: 'job', id: key.slice(WORKFLOW_PREFIX.length) }
  }
  if (key.startsWith(FLOW_PREFIX)) {
    return { kind: 'pipeline', id: key.slice(FLOW_PREFIX.length) }
  }
  if (key.startsWith(QUERY_PREFIX)) {
    return { kind: 'query', id: key.slice(QUERY_PREFIX.length) }
  }
  return null
}

export function keyOf(kind: RecordKind, id: string): string {
  if (kind === 'workflow') return KEY.project(id)
  if (kind === 'job') return KEY.workflow(id)
  if (kind === 'query') return KEY.query(id)
  return KEY.flow(id)
}
