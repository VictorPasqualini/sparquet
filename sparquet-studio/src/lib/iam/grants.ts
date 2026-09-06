/**
 * Who may touch which dataset, Job or Pipeline.
 *
 * The runner already has IAM for *actions*: a role says "may run things", "may
 * edit the library", "may query the catalog". That answers what a person can do
 * to the platform and says nothing about which TABLE they may read, which is the
 * question a data team actually gets asked. Roles cannot answer it either,
 * because the resources are not records the runner owns — a dataset is an
 * address that exists because some Job mentions it.
 *
 * So this is a second, narrower layer: a list of grants over named resources,
 * evaluated with the same two rules the runner's policies use, for the same
 * reason — they are the rules people already know:
 *
 *   1. Nothing is granted by default at the resource level. A resource with NO
 *      grant at all is open to whoever already holds the matching action, which
 *      keeps a runner that never opens this screen behaving exactly as before.
 *      The moment one grant names a resource, that resource is closed to
 *      everyone the grants do not reach.
 *   2. An explicit deny beats every allow, at any level, always.
 *
 * Levels are cumulative — `admin` implies `write` implies `read` — because the
 * alternative (three independent checkboxes) makes "who can read this?" a
 * question you answer by reading three lists.
 *
 * Storage is one record beside the catalog annotations, so it travels the same
 * route: IndexedDB on a laptop, `.studio/meta.json` on a runner with a
 * workspace. The runner re-reads it and enforces it server-side — a rule the
 * browser alone honoured would be decoration.
 */

/** What a grant can be attached to. */
export type ResourceKind = 'dataset' | 'job' | 'pipeline'

/** What the holder may do. Cumulative: admin implies write implies read. */
export type AccessLevel = 'read' | 'write' | 'admin'

export type GrantEffect = 'allow' | 'deny'

/** Who holds the grant. A team is the common case; a user is the exception. */
export type PrincipalKind = 'team' | 'user'

/** Every resource of a kind, in the position where an id would go. */
export const ANY = '*'

export interface Grant {
  id: string
  resource: ResourceKind
  /** Dataset address, Job id or Pipeline id. `*` is every resource of that kind. */
  resourceId: string
  principalKind: PrincipalKind
  /**
   * The team id or the user id — an id and not a name, so renaming a team does
   * not silently reopen a table. `*` is everyone. On a runner with no users at
   * all there are no ids, and the username stands in for one.
   */
  principalId: string
  /** The name that id had when the rule was written, for reading the list back. */
  principalLabel?: string
  level: AccessLevel
  effect: GrantEffect
  /** Why this exists — the line an auditor reads six months later. */
  note?: string
  updatedAt: number
}

/** A grant that happens to be on a dataset. Named for the screens that only see those. */
export type DatasetGrant = Grant

export const RESOURCE_KINDS: ResourceKind[] = ['dataset', 'job', 'pipeline']

export const LEVELS: AccessLevel[] = ['read', 'write', 'admin']

/** Higher wins when two allows overlap. */
export const LEVEL_RANK: Record<AccessLevel, number> = { read: 1, write: 2, admin: 3 }

export const LEVEL_HINT: Record<ResourceKind, Record<AccessLevel, string>> = {
  dataset: {
    read: 'Query it in the SQL editor and read its schema from storage.',
    write: 'Everything read allows, plus running a Job that writes to it.',
    admin: 'Everything write allows, plus editing its catalog entry and its grants.',
  },
  job: {
    read: 'Open the Job and see its canvas, JSON and run history.',
    write: 'Everything read allows, plus editing the Job and running it.',
    admin: 'Everything write allows, plus deleting the Job and changing its grants.',
  },
  pipeline: {
    read: 'Open the Pipeline and see its stages and run history.',
    write: 'Everything read allows, plus editing the Pipeline and running it.',
    admin: 'Everything write allows, plus deleting the Pipeline and changing its grants.',
  },
}

export const MAX_NOTE = 160

/** The principals one person is: themselves, their team, and everyone. */
export interface Identity {
  userId?: string | null
  username?: string | null
  teamId?: string | null
}

const KINDS = new Set<ResourceKind>(RESOURCE_KINDS)
const LEVEL_SET = new Set<AccessLevel>(LEVELS)
const PRINCIPALS = new Set<PrincipalKind>(['team', 'user'])

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function text(value: unknown, max: number): string {
  return typeof value === 'string' ? value.trim().slice(0, max) : ''
}

/**
 * Reads back whatever storage held, dropping anything that is not a grant.
 *
 * A malformed entry is discarded rather than repaired: a half-read access rule
 * is worse than a missing one, because it looks like a decision somebody made.
 */
export function sanitizeGrants(raw: unknown): Grant[] {
  if (!Array.isArray(raw)) return []
  const out: Grant[] = []
  const seen = new Set<string>()
  for (const item of raw) {
    if (!isRecord(item)) continue
    const resource = item.resource as ResourceKind
    const level = item.level as AccessLevel
    const principalKind = item.principalKind as PrincipalKind
    if (!KINDS.has(resource) || !LEVEL_SET.has(level) || !PRINCIPALS.has(principalKind)) continue
    const resourceId = text(item.resourceId, 512)
    const principalId = text(item.principalId, 128)
    const principalLabel = text(item.principalLabel, 128)
    if (!resourceId || !principalId) continue
    const id = text(item.id, 64) || `${resource}:${resourceId}:${principalKind}:${principalId}`
    if (seen.has(id)) continue
    seen.add(id)
    out.push({
      id,
      resource,
      resourceId,
      principalKind,
      principalId,
      principalLabel: principalLabel || undefined,
      level,
      effect: item.effect === 'deny' ? 'deny' : 'allow',
      note: text(item.note, MAX_NOTE) || undefined,
      updatedAt: typeof item.updatedAt === 'number' ? item.updatedAt : Date.now(),
    })
  }
  return out
}

export function grantId(): string {
  return `g-${Math.random().toString(36).slice(2, 10)}${Date.now().toString(36)}`
}

/** The grants that name this exact resource, plus the kind-wide ones. */
export function grantsFor(
  grants: readonly Grant[],
  resource: ResourceKind,
  resourceId: string,
): Grant[] {
  return grants.filter(
    (grant) =>
      grant.resource === resource &&
      (grant.resourceId === resourceId || grant.resourceId === ANY),
  )
}

/** Grants grouped by the resource they name, for a screen that lists many. */
export function grantsByResource(
  grants: readonly Grant[],
  resource: ResourceKind,
): Map<string, Grant[]> {
  const out = new Map<string, Grant[]>()
  for (const grant of grants) {
    if (grant.resource !== resource) continue
    const current = out.get(grant.resourceId)
    if (current) current.push(grant)
    else out.set(grant.resourceId, [grant])
  }
  return out
}

export interface GrantSummary {
  total: number
  allowed: number
  denied: number
}

/** How many rules touch a resource, and how many of them close a door. */
export function summarizeGrants(grants: readonly Grant[]): GrantSummary {
  let allowed = 0
  let denied = 0
  for (const grant of grants) {
    if (grant.effect === 'deny') denied += 1
    else allowed += 1
  }
  return { total: grants.length, allowed, denied }
}

function reaches(grant: Grant, identity: Identity): boolean {
  if (grant.principalId === ANY) return true
  if (grant.principalKind === 'team') {
    return Boolean(identity.teamId) && grant.principalId === identity.teamId
  }
  // The username is the fallback identity on a runner that has no user records,
  // where there is no id to match against.
  if (identity.userId && grant.principalId === identity.userId) return true
  return !identity.userId && Boolean(identity.username) && grant.principalId === identity.username
}

/**
 * The level this identity ends up with on one resource, or null for no access.
 *
 * `undefined` is impossible on purpose: the caller has to decide what an
 * ungoverned resource means, and that answer differs between the browser (show
 * it) and the runner (allow it, because the action-level policy already ran).
 * `governed` says which case it is.
 */
export interface Decision {
  /** True when at least one grant names this resource — so the default is closed. */
  governed: boolean
  level: AccessLevel | null
}

export function decide(
  grants: readonly Grant[],
  resource: ResourceKind,
  resourceId: string,
  identity: Identity,
): Decision {
  const relevant = grantsFor(grants, resource, resourceId)
  if (relevant.length === 0) return { governed: false, level: null }

  let best: AccessLevel | null = null
  let denyRank = 0
  for (const grant of relevant) {
    if (!reaches(grant, identity)) continue
    if (grant.effect === 'deny') {
      // A deny at `read` shuts the door completely; a deny at `write` leaves
      // reading intact. Denying the lowest level anyone denied is the safe read.
      denyRank = denyRank === 0 ? LEVEL_RANK[grant.level] : Math.min(denyRank, LEVEL_RANK[grant.level])
      continue
    }
    if (!best || LEVEL_RANK[grant.level] > LEVEL_RANK[best]) best = grant.level
  }

  if (!best) return { governed: true, level: null }
  if (denyRank === 0) return { governed: true, level: best }
  // Everything at or above the denied level is gone.
  const kept = LEVELS.filter(
    (level) => LEVEL_RANK[level] < denyRank && LEVEL_RANK[level] <= LEVEL_RANK[best],
  )
  return { governed: true, level: kept.length > 0 ? kept[kept.length - 1] : null }
}

/** Whether this identity holds at least `wanted` on the resource. */
export function allows(
  grants: readonly Grant[],
  resource: ResourceKind,
  resourceId: string,
  identity: Identity,
  wanted: AccessLevel,
  /** What an ungoverned resource means here. The runner passes true. */
  openByDefault = true,
): boolean {
  const decision = decide(grants, resource, resourceId, identity)
  if (!decision.governed) return openByDefault
  return decision.level !== null && LEVEL_RANK[decision.level] >= LEVEL_RANK[wanted]
}

/** Adds or replaces one grant, keyed by id. */
export function withGrant(grants: readonly Grant[], grant: Grant): Grant[] {
  const next = grants.filter((item) => item.id !== grant.id)
  next.push(grant)
  return next
}

export function withoutGrant(grants: readonly Grant[], id: string): Grant[] {
  return grants.filter((grant) => grant.id !== id)
}
