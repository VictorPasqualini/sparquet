/**
 * Grants over datasets, Jobs and Pipelines.
 *
 * Kept apart from `auth.ts` because the two answer different questions and are
 * owned by different sides. `auth.ts` mirrors the runner's own policies — who
 * may run, who may edit the library — and the runner is the only writer. This
 * store holds rules over resources the runner does not have records for: a
 * dataset is an address a Job mentions, so nothing on the server can list them.
 * The rules are written here and mirrored to the runner through the workspace,
 * where the same evaluation runs again before a query touches storage.
 *
 * Keyed by resource id, never by record id, for the same reason the catalog is:
 * a grant belongs to `/lake/silver/orders`, not to whichever Job writes it today.
 */

import { create } from 'zustand'

import {
  decide,
  grantId,
  grantsByResource,
  withGrant,
  withoutGrant,
  type AccessLevel,
  type Grant,
  type Identity,
  type ResourceKind,
} from '@/lib/iam'
import * as db from '@/lib/storage/db'
import { useAuthStore } from '@/store/auth'

interface IamState {
  grants: Grant[]
  loaded: boolean
  loading: boolean
  error: string | null

  load: (force?: boolean) => Promise<void>
  /** Adds a rule, or replaces the one with the same id. */
  grant: (input: Omit<Grant, 'id' | 'updatedAt'> & { id?: string }) => Promise<void>
  revoke: (id: string) => Promise<void>
  /** Every grant on one resource, indexed by resource id — what a list screen wants. */
  byResource: (kind: ResourceKind) => Map<string, Grant[]>
}

export const useIamStore = create<IamState>((set, get) => ({
  grants: [],
  loaded: false,
  loading: false,
  error: null,

  load: async (force = false) => {
    if (get().loading) return
    if (get().loaded && !force) return
    set({ loading: true, error: null })
    try {
      set({ grants: await db.readGrants(), loaded: true, loading: false })
    } catch (error) {
      set({
        loading: false,
        loaded: true,
        error: error instanceof Error ? error.message : String(error),
      })
    }
  },

  grant: async (input) => {
    const next = withGrant(get().grants, {
      ...input,
      id: input.id ?? grantId(),
      updatedAt: Date.now(),
    })
    // Persist first. A rule the screen shows but storage refused is the worst
    // possible outcome here: it reads as protection that does not exist.
    await db.writeGrants(next)
    set({ grants: next, error: null })
  },

  revoke: async (id) => {
    const next = withoutGrant(get().grants, id)
    if (next.length === get().grants.length) return
    await db.writeGrants(next)
    set({ grants: next, error: null })
  },

  byResource: (kind) => grantsByResource(get().grants, kind),
}))

/** Who the browser currently is, in the shape the evaluation wants. */
export function currentIdentity(): Identity {
  const principal = useAuthStore.getState().principal
  return {
    userId: principal?.userId ?? null,
    username: principal?.username ?? null,
    teamId: principal?.teamId ?? null,
  }
}

/**
 * Whether the person at the keyboard holds `level` on a resource.
 *
 * Optimistic by design: an ungoverned resource is allowed, and so is anything
 * this browser cannot decide. The runner re-evaluates every call, so a wrong
 * answer here costs a failed request, while a wrong `false` would hide work
 * somebody is entitled to do.
 */
export function mayAccess(kind: ResourceKind, resourceId: string, level: AccessLevel): boolean {
  const decision = decide(useIamStore.getState().grants, kind, resourceId, currentIdentity())
  if (!decision.governed) return true
  return decision.level !== null && LEVEL_ORDER[decision.level] >= LEVEL_ORDER[level]
}

const LEVEL_ORDER: Record<AccessLevel, number> = { read: 1, write: 2, admin: 3 }
