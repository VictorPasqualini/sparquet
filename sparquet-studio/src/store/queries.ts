/**
 * The saved queries of the SQL editor — its files.
 *
 * Before this store the editor held one draft in `localStorage`, which is the
 * shape a scratchpad has: it survives a reload and nothing else. It cannot be
 * named, so a second query overwrites the first; it cannot be reviewed, because
 * nothing about it reaches the repository; and it cannot be shared, because it
 * lives in one browser profile on one machine.
 *
 * A saved query goes through the same storage backend every other record does,
 * so on a runner it becomes `queries/<slug>.sql` in the library — a file a
 * reviewer reads in a pull request without a JSON viewer — and in a browser
 * without a runner it is a row in IndexedDB, like everything else.
 *
 * Unsaved work is deliberately still kept outside this store: a tab nobody has
 * named yet is a draft, and writing a file for every keystroke would fill the
 * library with `untitled-3.sql`.
 */

import { nanoid } from 'nanoid'
import { create } from 'zustand'

import { deleteQuery, listQueries, saveQuery } from '@/lib/storage/db'
import type { SavedQuery } from '@/types/studio'

/** What a query is called before anybody names it. */
export const UNTITLED = 'Untitled query'

interface QueriesState {
  items: SavedQuery[]
  loaded: boolean
  loading: boolean
  error: string | null

  load: (force?: boolean) => Promise<void>
  /** Creates a file. Returns the record so the caller can bind a tab to its id. */
  create: (fields: { name?: string; sql?: string; limit?: number }) => Promise<SavedQuery>
  /** Writes over an existing one. A patch, so renaming does not need the SQL. */
  update: (id: string, patch: Partial<Omit<SavedQuery, 'id' | 'createdAt'>>) => Promise<SavedQuery | null>
  remove: (id: string) => Promise<void>
  byId: (id: string) => SavedQuery | undefined
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function byRecency(a: SavedQuery, b: SavedQuery): number {
  return b.updatedAt - a.updatedAt
}

export const useQueriesStore = create<QueriesState>((set, get) => ({
  items: [],
  loaded: false,
  loading: false,
  error: null,

  load: async (force = false) => {
    if (!force && (get().loaded || get().loading)) return
    set({ loading: true, error: null })
    try {
      set({ items: (await listQueries()).sort(byRecency), loaded: true, loading: false })
    } catch (error) {
      set({ loading: false, error: message(error) })
    }
  },

  create: async (fields) => {
    const now = Date.now()
    const record: SavedQuery = {
      id: nanoid(10),
      name: (fields.name ?? '').trim() || UNTITLED,
      description: '',
      sql: fields.sql ?? '',
      limit: fields.limit,
      createdAt: now,
      updatedAt: now,
    }
    const saved = await saveQuery(record)
    set({ items: [saved, ...get().items].sort(byRecency), error: null })
    return saved
  },

  update: async (id, patch) => {
    const current = get().items.find((item) => item.id === id)
    if (!current) return null
    const record: SavedQuery = { ...current, ...patch, updatedAt: Date.now() }
    if (patch.name !== undefined) record.name = patch.name.trim() || UNTITLED
    const saved = await saveQuery(record)
    set({
      items: get()
        .items.map((item) => (item.id === id ? saved : item))
        .sort(byRecency),
      error: null,
    })
    return saved
  },

  remove: async (id) => {
    await deleteQuery(id)
    set({ items: get().items.filter((item) => item.id !== id), error: null })
  },

  byId: (id) => get().items.find((item) => item.id === id),
}))
