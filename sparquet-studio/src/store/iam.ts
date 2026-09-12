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
  CONTAINED_KINDS,
  decide,
  effectiveOwner,
  grantId,
  grantsByResource,
  mayAdminister,
  ownerOf,
  tagScopes,
  withGrant,
  withOwner,
  withoutGrant,
  withoutOwner,
  type AccessLevel,
  type Grant,
  type Identity,
  type Owner,
  type ResourceKind,
  type Scope,
} from '@/lib/iam'
import * as db from '@/lib/storage/db'
import { useAuthStore } from '@/store/auth'
import { useCatalogStore } from '@/store/catalog'
import { useLibraryStore } from '@/store/library'
import { useSecretsStore } from '@/store/secrets'

interface IamState {
  grants: Grant[]
  /** Who owns each resource. One record per resource, never a list. */
  owners: Owner[]
  loaded: boolean
  loading: boolean
  error: string | null

  load: (force?: boolean) => Promise<void>
  /** Adds a rule, or replaces the one with the same id. */
  grant: (input: Omit<Grant, 'id' | 'updatedAt'> & { id?: string }) => Promise<void>
  revoke: (id: string) => Promise<void>
  /** Hands a resource to a team or a user, replacing whoever held it. */
  setOwner: (input: Omit<Owner, 'updatedAt'>) => Promise<void>
  /** Drops the ownership record, leaving the resource to the grants alone. */
  clearOwner: (resource: ResourceKind, resourceId: string) => Promise<void>
  /** Every grant on one resource, indexed by resource id — what a list screen wants. */
  byResource: (kind: ResourceKind) => Map<string, Grant[]>
  /** The owner recorded directly on one resource, ignoring inheritance. */
  ownerFor: (resource: ResourceKind, resourceId: string) => Owner | null
}

export const useIamStore = create<IamState>((set, get) => ({
  grants: [],
  owners: [],
  loaded: false,
  loading: false,
  error: null,

  load: async (force = false) => {
    if (get().loading) return
    if (get().loaded && !force) return
    set({ loading: true, error: null })
    try {
      // Read together: a screen that showed grants before owners would flash an
      // "open to everyone" state over a resource that has an owner.
      const [grants, owners] = await Promise.all([db.readGrants(), db.readOwners()])
      set({ grants, owners, loaded: true, loading: false })
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

  setOwner: async (input) => {
    const next = withOwner(get().owners, { ...input, updatedAt: Date.now() })
    // Persist first, for the reason `grant` does: ownership the screen shows and
    // storage refused reads as protection that does not exist.
    await db.writeOwners(next)
    set({ owners: next, error: null })
  },

  clearOwner: async (resource, resourceId) => {
    const next = withoutOwner(get().owners, resource, resourceId)
    if (next.length === get().owners.length) return
    await db.writeOwners(next)
    set({ owners: next, error: null })
  },

  byResource: (kind) => grantsByResource(get().grants, kind),

  ownerFor: (resource, resourceId) => ownerOf(get().owners, resource, resourceId),
}))

/**
 * What one resource inherits from — the Workflow a Job or a Pipeline lives in,
 * and the tags the catalog gives a dataset.
 *
 * A dataset's path ancestors need nothing here: they are in the address itself,
 * and `scopeChain` derives them without asking anybody. Its tags are the
 * opposite — they exist only in the catalog entry somebody typed, which is why
 * they are looked up here and handed in. Mirrors `_parents_of` in
 * `server/main.py`, which reads the same two records out of the workspace.
 */
export function parentsOf(resource: ResourceKind, resourceId: string): Scope[] {
  if (resource === 'dataset') {
    const annotation = useCatalogStore.getState().annotations[resourceId]
    return annotation
      ? tagScopes(annotation.tags, {
          classification: annotation.classification,
          domain: annotation.domain,
        })
      : []
  }
  if (resource === 'secret') {
    // A credential joins the same tag chain a table does, so a deny on
    // `tag/pii` closes the connection as well as what it reaches.
    const secret = useSecretsStore.getState().byName(resourceId)
    return secret ? tagScopes(secret.tags, {}) : []
  }
  if (!CONTAINED_KINDS.includes(resource) || !resourceId) return []
  const library = useLibraryStore.getState()
  const record =
    resource === 'job'
      ? library.jobs.find((job) => job.id === resourceId)
      : library.pipelines.find((pipeline) => pipeline.id === resourceId)
  const workflowId = record?.workflowId
  return workflowId ? [['workflow', workflowId]] : []
}

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
  const decision = accessTo(kind, resourceId)
  if (!decision.governed) return true
  return decision.level !== null && LEVEL_ORDER[decision.level] >= LEVEL_ORDER[level]
}

/**
 * The owner a resource answers to, own record or inherited, with its source.
 *
 * Kept beside `accessTo` because a screen that shows one without the other
 * cannot explain itself: the level and the owner come from the same chain.
 */
export function effectiveOwnerOf(kind: ResourceKind, resourceId: string) {
  const { owners } = useIamStore.getState()
  return effectiveOwner(owners, kind, resourceId, parentsOf(kind, resourceId))
}

/** The whole answer on one resource — level, ownership, and where it came from. */
export function accessTo(kind: ResourceKind, resourceId: string) {
  const { grants, owners } = useIamStore.getState()
  return decide(grants, kind, resourceId, currentIdentity(), owners, parentsOf(kind, resourceId))
}

/**
 * Whether the person at the keyboard may change the rules on a resource.
 *
 * Two ways in, and the second is the point of ownership: `iam:ManageGrants` over
 * the runner, or owning this resource (or what contains it). The runner checks
 * exactly this again in `_owner_may_change_meta`.
 */
export function mayAdministerResource(kind: ResourceKind, resourceId: string): boolean {
  const principal = useAuthStore.getState().principal
  // No principal means Studio has not asked the runner yet, or the runner has no
  // users. Both are "do not start hiding things", the same default `can()` takes.
  if (!principal) return true
  if (useAuthStore.getState().can('iam:ManageGrants')) return true
  const { grants, owners } = useIamStore.getState()
  return mayAdminister(
    grants, owners, kind, resourceId, currentIdentity(), parentsOf(kind, resourceId),
  )
}

const LEVEL_ORDER: Record<AccessLevel, number> = { read: 1, write: 2, admin: 3 }
