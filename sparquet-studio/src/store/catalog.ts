/**
 * The data catalog, held as one map from dataset address to annotation.
 *
 * Separate from the library store because it is keyed by ADDRESS, not by record
 * id: an annotation belongs to `/lake/silver/orders`, not to the Job that
 * happens to write it today. Deleting that Job must not take the description of
 * the table with it.
 *
 * Loaded lazily by the screens that need it — the editor never does — so boot
 * stays one round trip lighter.
 */

import { create } from 'zustand'

import { withAnnotation, withoutAnnotation } from '@/lib/datacatalog'
import type { CatalogAnnotations, DatasetAnnotation } from '@/lib/datacatalog'
import * as db from '@/lib/storage/db'

interface CatalogState {
  annotations: CatalogAnnotations
  loaded: boolean
  loading: boolean
  error: string | null

  /** Reads the catalog once; later calls are a no-op unless `force` is set. */
  load: (force?: boolean) => Promise<void>
  /** Writes one dataset's annotation. A patch that empties it deletes the entry. */
  annotate: (key: string, patch: Partial<DatasetAnnotation>) => Promise<void>
  forget: (key: string) => Promise<void>
}

export const useCatalogStore = create<CatalogState>((set, get) => ({
  annotations: {},
  loaded: false,
  loading: false,
  error: null,

  load: async (force = false) => {
    if (get().loading) return
    if (get().loaded && !force) return
    set({ loading: true, error: null })
    try {
      set({ annotations: await db.readCatalog(), loaded: true, loading: false })
    } catch (error) {
      set({
        loading: false,
        loaded: true,
        error: error instanceof Error ? error.message : String(error),
      })
    }
  },

  annotate: async (key, patch) => {
    const next = withAnnotation(get().annotations, key, patch)
    // Persist first: showing an edit the storage refused would be a lie, and the
    // one thing a catalog cannot afford is a description nobody else can see.
    await db.writeCatalog(next)
    set({ annotations: next, error: null })
  },

  forget: async (key) => {
    const next = withoutAnnotation(get().annotations, key)
    if (next === get().annotations) return
    await db.writeCatalog(next)
    set({ annotations: next, error: null })
  },
}))
