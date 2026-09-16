/**
 * The contract every storage backend implements.
 *
 * Deliberately a key/value interface: `db.ts` above it thinks in records, each
 * backend below it thinks in whatever it actually has — an HTTP workspace, an
 * IndexedDB store, a localStorage bucket, a map.
 */

export type StorageKind = 'workspace' | 'indexeddb' | 'localstorage' | 'memory'

export interface StorageBackend {
  kind: StorageKind
  get(key: string): Promise<unknown>
  set(key: string, value: unknown): Promise<void>
  del(key: string): Promise<void>
  keys(prefix: string): Promise<string[]>
  /**
   * Let the NEXT write to this key land even if the stored copy changed since it
   * was read. Only the workspace backend has anything to do here — it is the only
   * one that can be written from another machine — and it is deliberately opt-in
   * and one-shot: overwriting somebody else's save is a decision a person makes
   * once, about one record, never a mode the editor stays in.
   */
  overwriteNext?(key: string): void
  /**
   * Re-read one record from the store of record, ignoring anything cached, and
   * return it. Only the workspace backend implements it: everywhere else the
   * cache *is* the store. It is what "show me what is actually on disk" costs
   * after a conflict — and it also re-syncs whatever the backend tracks to know
   * a record moved, so the save that follows is not refused for being stale.
   */
  refresh?(key: string): Promise<unknown>
}

/** Guarantees the value survives structured clone and strips `undefined` holes. */
export function toStorable(value: unknown): unknown {
  if (value === undefined) return undefined
  return JSON.parse(JSON.stringify(value)) as unknown
}
