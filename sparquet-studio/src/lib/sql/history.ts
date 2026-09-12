/**
 * What each query has been run as, and how it went.
 *
 * The buffer only ever holds the current text, so the statement that worked
 * twenty minutes ago is gone the moment it is edited — and that statement is
 * routinely what somebody is trying to get back to. This keeps it: the exact
 * SQL sent to the runner, when, how long it took, how many rows came back, and
 * the error if there was one.
 *
 * Kept in `localStorage`, per browser, on purpose. A run history is a scratch
 * record of somebody's afternoon, not a record of the workspace: putting it in
 * the library would write a file per execution and make every teammate's
 * experiments part of the repository. What deserves to be shared is the query,
 * and the query is already a file.
 *
 * History belongs to a query, not to a tab: a saved query keeps its runs across
 * closing and reopening it, while a scratch tab keeps its own until it is
 * closed. That is what `historyKey` decides.
 */

const KEY = 'sparquet-studio:sql-history'

/** Runs remembered per query. Beyond this, the oldest is dropped. */
const PER_QUERY = 40

/** How many queries are remembered at all, newest first, so storage stays bounded. */
const QUERIES = 40

export interface QueryRun {
  id: string
  /** ISO-8601, from this browser's clock. */
  at: string
  /** Exactly what was sent — the selection, when a selection was run. */
  sql: string
  /** The row cap the run was sent with, since it changes what came back. */
  limit: number
  elapsedMs: number
  rows: number
  /** The runner cut the result short at the cap. */
  truncated: boolean
  /** The first line of the failure, or null when the run succeeded. */
  error: string | null
}

type Store = Record<string, QueryRun[]>

/**
 * Which history a tab writes to.
 *
 * A saved query is identified by its file, so its runs survive the tab being
 * closed and opened again — the same query, the same history. A tab nobody has
 * saved has nothing but itself to be identified by.
 */
export function historyKey(queryId: string | null, tabId: string): string {
  return queryId ? `q:${queryId}` : `t:${tabId}`
}

function isRun(value: unknown): value is QueryRun {
  if (typeof value !== 'object' || value === null) return false
  const run = value as Record<string, unknown>
  return typeof run.id === 'string' && typeof run.at === 'string' && typeof run.sql === 'string'
}

function read(): Store {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(KEY) ?? 'null')
    if (typeof parsed !== 'object' || parsed === null) return {}
    const store: Store = {}
    for (const [key, runs] of Object.entries(parsed as Record<string, unknown>)) {
      if (Array.isArray(runs)) store[key] = runs.filter(isRun)
    }
    return store
  } catch {
    // Storage disabled, or a shape from an older build: an editor with no
    // history is still an editor.
    return {}
  }
}

function write(store: Store): void {
  try {
    localStorage.setItem(KEY, JSON.stringify(store))
  } catch {
    // Full or disabled. Losing a history entry must never lose a query.
  }
}

/** The runs of one query, newest first. */
export function readHistory(key: string): QueryRun[] {
  return read()[key] ?? []
}

/** Records a run and answers the history it now has, newest first. */
export function addRun(key: string, run: Omit<QueryRun, 'id' | 'at'>): QueryRun[] {
  const store = read()
  const entry: QueryRun = {
    ...run,
    id: `r${Date.now().toString(36)}${Math.random().toString(36).slice(2, 7)}`,
    at: new Date().toISOString(),
  }
  const runs = [entry, ...(store[key] ?? [])].slice(0, PER_QUERY)
  const next: Store = { [key]: runs }
  // Rebuilt newest-first so the cap drops the query nobody has touched in
  // longest, rather than whichever one the object happened to list last.
  for (const [other, otherRuns] of Object.entries(store)) {
    if (other !== key && Object.keys(next).length < QUERIES) next[other] = otherRuns
  }
  write(next)
  return runs
}

/**
 * Carries a history from one key to another.
 *
 * Saving a scratch tab for the first time gives it a file, and a file is what
 * the history is keyed by from then on. Without this, the act of saving would
 * throw away everything the person had run to get to the query worth saving.
 */
export function moveHistory(from: string, to: string): void {
  if (from === to) return
  const store = read()
  const runs = store[from]
  if (!runs || runs.length === 0) return
  store[to] = [...runs, ...(store[to] ?? [])].slice(0, PER_QUERY)
  delete store[from]
  write(store)
}

/** Forgets one query's runs. */
export function clearHistory(key: string): void {
  const store = read()
  delete store[key]
  write(store)
}
