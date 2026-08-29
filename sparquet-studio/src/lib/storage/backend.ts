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
}

/** Guarantees the value survives structured clone and strips `undefined` holes. */
export function toStorable(value: unknown): unknown {
  if (value === undefined) return undefined
  return JSON.parse(JSON.stringify(value)) as unknown
}
