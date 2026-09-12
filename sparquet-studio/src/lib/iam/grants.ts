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

/**
 * What a grant can be attached to.
 *
 * A Workflow is in the list but is never asked about directly: it is the
 * container a Job and a Pipeline inherit from, which is what a catalog is to a
 * table.
 *
 * `tag` is the same idea turned sideways. A Workflow contains a Job because
 * somebody filed it there; a tag contains a dataset because somebody described
 * it that way in the catalog. One rule on `tag/pii` governs every table
 * classified as such, including the ones written next month — which is the only
 * way a rule keeps up with a lake that grows. Nothing is ever asked about a tag
 * directly either: it reaches a decision through the dataset that carries it.
 *
 * `secret` is the credential behind a connection, and it is governed here rather
 * than by a role because the question is the one datasets already ask: who may
 * reach this particular thing. `read` on a secret means "may be used by a run
 * this person starts" — never "may be looked at". No level, and no endpoint,
 * returns a value to anybody.
 */
export type ResourceKind = 'dataset' | 'job' | 'pipeline' | 'workflow' | 'tag' | 'secret'

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

/**
 * Who a securable belongs to — one principal, never a list.
 *
 * Ownership is the part of the Databricks model that makes the rest usable: the
 * owner holds every privilege on the object and may grant on it without being
 * an administrator of the platform, and no deny can shut them out of it. An
 * object whose owner can be locked out of it is an object nobody can fix.
 */
export interface Owner {
  resource: ResourceKind
  resourceId: string
  /** A team owner survives the person leaving, which is usually what you want. */
  principalKind: PrincipalKind
  principalId: string
  /** The name that id had when ownership was recorded, for reading the list back. */
  principalLabel?: string
  updatedAt: number
}

export const RESOURCE_KINDS: ResourceKind[] = [
  'dataset',
  'job',
  'pipeline',
  'workflow',
  'tag',
  'secret',
]

/** The kinds that live inside a Workflow, and therefore inherit from one. */
export const CONTAINED_KINDS: ResourceKind[] = ['job', 'pipeline']

/**
 * The kinds a deed can be written on.
 *
 * A tag is not one of them. Ownership is responsibility for a thing, and a tag
 * is a word that happens to be true of several things — "owner of everything
 * classified restricted" names no object anybody can hand over, and it would
 * hand out an admin nothing can deny on tables the owner has never seen.
 */
export const OWNABLE_KINDS: ResourceKind[] = [
  'dataset',
  'job',
  'pipeline',
  'workflow',
  'secret',
]

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
  workflow: {
    read: 'Open the Workflow and everything inside it.',
    write: 'Everything read allows, plus editing and running what is inside it.',
    admin: 'Everything write allows, plus changing who may reach anything in it.',
  },
  tag: {
    read: 'Query every dataset the catalog gives this tag, and read its schema.',
    write: 'Everything read allows, on every dataset with this tag, including the ones tagged later.',
    admin: 'Everything write allows, plus editing those catalog entries and their grants.',
  },
  secret: {
    read: 'Reference it from a Job, a query or a schema read — the run gets the value, the person never does.',
    write: 'Everything read allows, plus rotating its fields, retagging it and deleting it.',
    admin: 'Everything write allows, plus deciding who else may use it.',
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
    const raw_id = text(item.resourceId, 512)
    // One spelling per tag, written the same way the lookup will ask for it.
    const resourceId = resource === 'tag' ? normalizeTag(raw_id) : raw_id
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

/** Reads back the stored ownership records, dropping anything malformed. */
export function sanitizeOwners(raw: unknown): Owner[] {
  const items = Array.isArray(raw) ? raw : isRecord(raw) ? Object.values(raw) : []
  const out: Owner[] = []
  const seen = new Set<string>()
  for (const item of items) {
    if (!isRecord(item)) continue
    const resource = item.resource as ResourceKind
    const principalKind = item.principalKind as PrincipalKind
    // A tag owns nothing and is owned by nobody; see `OWNABLE_KINDS`.
    if (!OWNABLE_KINDS.includes(resource) || !PRINCIPALS.has(principalKind)) continue
    const resourceId = text(item.resourceId, 512)
    const principalId = text(item.principalId, 128)
    if (!resourceId || !principalId) continue
    const key = `${resource}:${resourceId}`
    // Two owners is not a state this model has an answer for, so the first wins.
    if (seen.has(key)) continue
    seen.add(key)
    out.push({
      resource,
      resourceId,
      principalKind,
      principalId,
      principalLabel: text(item.principalLabel, 128) || undefined,
      updatedAt: typeof item.updatedAt === 'number' ? item.updatedAt : Date.now(),
    })
  }
  return out
}

export function ownerKey(resource: ResourceKind, resourceId: string): string {
  return `${resource}:${resourceId}`
}

/** The separators an address nests with: a path, and a qualified name. */
const SEPARATORS = ['/', '.'] as const

/**
 * The containing addresses of one dataset, nearest first, without itself.
 *
 * `/lake/silver/orders` is contained by `/lake/silver` and by `/lake`, the way a
 * table is contained by a schema and a catalog. `*` is not returned: it is the
 * wildcard every kind already has, and `scopeChain` appends it once.
 *
 * Must agree with `ancestors()` in `server/grants.py` — the browser greys out a
 * control and the runner refuses the request, and the two answering differently
 * is worse than either answering wrongly.
 */
export function ancestors(resourceId: string): string[] {
  const trimmed = (resourceId ?? '').trim().replace(/\/+$/, '')
  if (!trimmed || trimmed === ANY) return []
  const separator = SEPARATORS.find((sep) => trimmed.includes(sep))
  if (!separator) return []

  // Cut from the right rather than split and re-join: `s3://bucket/...` has an
  // empty segment in it, and `s3:/bucket` is not the same bucket.
  const out: string[] = []
  let current = trimmed
  for (;;) {
    const cut = current.lastIndexOf(separator)
    if (cut === -1) break
    let head = current.slice(0, cut)
    while (head.endsWith(separator)) head = head.slice(0, -1)
    // An empty head is the root of a path; one ending in `:` is a URL scheme.
    // Neither is a container anybody can grant on.
    if (!head || head.endsWith(':')) break
    out.push(head)
    current = head
  }
  return out
}

/** A `[kind, id]` pair in the chain. */
export type Scope = [ResourceKind, string]

/**
 * The one spelling of a tag the rules are written against.
 *
 * Lower-cased and trimmed because the catalog is typed by people: `PII`, `pii `
 * and `Pii` are one tag to everybody except a string comparison, and a rule
 * that misses because of a capital letter fails open. Must agree with
 * `normalize_tag` in `server/grants.py`.
 */
export function normalizeTag(tag: string): string {
  return (tag ?? '').trim().toLowerCase()
}

/**
 * The tags of one dataset, as scopes a decision can inherit from.
 *
 * Kept here rather than read off the catalog: this module knows nothing about
 * annotations, and the caller — the browser store or the runner — is the one
 * that can look them up.
 *
 * `attributes` are the catalog fields that are not free tags but describe the
 * data just as well, and they enter as `field:value` — `classification:restricted`,
 * `domain:finance`. They are the two people most want a single rule on, and
 * spelling them into the same namespace means one screen, one rule and one
 * evaluation instead of a third mechanism.
 */
export function tagScopes(
  tags: readonly string[] = [],
  attributes: Readonly<Record<string, string | null | undefined>> = {},
): Scope[] {
  const out: Scope[] = []
  const seen = new Set<string>()
  const add = (value: string) => {
    const tag = normalizeTag(value)
    if (!tag || tag === ANY || seen.has(tag)) return
    seen.add(tag)
    out.push(['tag', tag])
  }
  for (const tag of tags) add(tag)
  for (const [field, value] of Object.entries(attributes)) {
    if (value) add(`${field}:${value}`)
  }
  return out
}

/**
 * Every place a rule could be written to reach this securable, nearest first.
 *
 * `parents` is how a Job says which Workflow it belongs to — the record knows,
 * and this module deliberately does not have to.
 */
export function scopeChain(
  resource: ResourceKind,
  resourceId: string,
  parents: readonly Scope[] = [],
): Scope[] {
  const chain: Scope[] = []
  const seen = new Set<string>()
  const add = (kind: ResourceKind, id: string) => {
    const key = `${kind}:${id}`
    if (seen.has(key)) return
    seen.add(key)
    chain.push([kind, id])
  }

  const clean = (resourceId ?? '').trim()
  if (clean) add(resource, clean)
  for (const ancestor of ancestors(clean)) add(resource, ancestor)
  add(resource, ANY)

  for (const [parentKind, parentId] of parents) {
    const parentClean = (parentId ?? '').trim()
    if (!parentClean || !KINDS.has(parentKind)) continue
    add(parentKind, parentClean)
    for (const ancestor of ancestors(parentClean)) add(parentKind, ancestor)
    add(parentKind, ANY)
  }

  return chain
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

function reachesPrincipal(
  principalKind: PrincipalKind,
  principalId: string,
  identity: Identity,
): boolean {
  if (principalId === ANY) return true
  if (principalKind === 'team') {
    return Boolean(identity.teamId) && principalId === identity.teamId
  }
  // The username is the fallback identity on a runner that has no user records,
  // where there is no id to match against.
  if (identity.userId && principalId === identity.userId) return true
  return !identity.userId && Boolean(identity.username) && principalId === identity.username
}

function reaches(grant: Grant, identity: Identity): boolean {
  return reachesPrincipal(grant.principalKind, grant.principalId, identity)
}

/** The owner recorded on this securable itself, ignoring inheritance. */
export function ownerOf(
  owners: readonly Owner[],
  resource: ResourceKind,
  resourceId: string,
): Owner | null {
  const clean = (resourceId ?? '').trim()
  return (
    owners.find((owner) => owner.resource === resource && owner.resourceId === clean) ?? null
  )
}

/**
 * The owner that actually applies to a securable, and where it is recorded.
 *
 * Its own record when it has one, otherwise the nearest one above it. The
 * screen has to say which: a person reading "owned by platform" on a table goes
 * looking for a record on the table, and there is none to find.
 */
export function effectiveOwner(
  owners: readonly Owner[],
  resource: ResourceKind,
  resourceId: string,
  parents: readonly Scope[] = [],
): { owner: Owner; source: string } | null {
  if (owners.length === 0) return null
  for (const [kind, id] of scopeChain(resource, resourceId, parents)) {
    const found = owners.find((owner) => owner.resource === kind && owner.resourceId === id)
    if (found) return { owner: found, source: `${kind}/${id}` }
  }
  return null
}

/**
 * The `kind/id` in the chain this identity owns, or null.
 *
 * Owning the container is owning what is inside it: whoever owns a Workflow owns
 * the Jobs in it, and whoever owns `/lake/silver` owns the tables under it.
 * Without that rule every new table arrives unowned and needs a second decision.
 */
export function owns(
  owners: readonly Owner[],
  resource: ResourceKind,
  resourceId: string,
  identity: Identity,
  parents: readonly Scope[] = [],
): string | null {
  if (owners.length === 0) return null
  for (const [kind, id] of scopeChain(resource, resourceId, parents)) {
    for (const owner of owners) {
      if (owner.resource !== kind || owner.resourceId !== id) continue
      if (reachesPrincipal(owner.principalKind, owner.principalId, identity)) {
        return `${kind}/${id}`
      }
    }
  }
  return null
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
  /**
   * True when at least one grant or owner names this resource, or something
   * that contains it — so the default is closed.
   */
  governed: boolean
  level: AccessLevel | null
  /** True when the level comes from owning it, or owning what contains it. */
  owned: boolean
  /**
   * The `kind/id` the winning rule sits on: the resource itself, or the
   * ancestor it was inherited from. A level with no explanation sends people to
   * the wrong screen to change it.
   */
  source: string | null
}

export function decide(
  grants: readonly Grant[],
  resource: ResourceKind,
  resourceId: string,
  identity: Identity,
  owners: readonly Owner[] = [],
  parents: readonly Scope[] = [],
): Decision {
  const chain = scopeChain(resource, resourceId, parents)

  const held = owns(owners, resource, resourceId, identity, parents)
  // Checked before the grants and not against them: the point of an owner is
  // that it is the one principal a rule cannot shut out.
  if (held) return { governed: true, level: 'admin', owned: true, source: held }

  // Naming an owner is itself a decision about the resource, so it governs it —
  // otherwise declaring ownership would leave the thing wide open to everyone.
  let governed = chain.some(([kind, id]) =>
    owners.some((owner) => owner.resource === kind && owner.resourceId === id),
  )

  let best: AccessLevel | null = null
  let bestSource: string | null = null
  let denyRank = 0
  for (const [kind, id] of chain) {
    for (const grant of grants) {
      if (grant.resource !== kind || grant.resourceId !== id) continue
      governed = true
      if (!reaches(grant, identity)) continue
      if (grant.effect === 'deny') {
        // A deny at `read` shuts the door completely; a deny at `write` leaves
        // reading intact. Denying the lowest level anyone denied is the safe read.
        denyRank =
          denyRank === 0 ? LEVEL_RANK[grant.level] : Math.min(denyRank, LEVEL_RANK[grant.level])
        continue
      }
      if (!best || LEVEL_RANK[grant.level] > LEVEL_RANK[best]) {
        best = grant.level
        bestSource = `${kind}/${id}`
      }
    }
  }

  if (!governed) return { governed: false, level: null, owned: false, source: null }
  if (!best) return { governed: true, level: null, owned: false, source: null }
  if (denyRank === 0) return { governed: true, level: best, owned: false, source: bestSource }
  // Everything at or above the denied level is gone.
  const kept = LEVELS.filter(
    (level) => LEVEL_RANK[level] < denyRank && LEVEL_RANK[level] <= LEVEL_RANK[best!],
  )
  const left = kept.length > 0 ? kept[kept.length - 1] : null
  return { governed: true, level: left, owned: false, source: left ? bestSource : null }
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
  owners: readonly Owner[] = [],
  parents: readonly Scope[] = [],
): boolean {
  const decision = decide(grants, resource, resourceId, identity, owners, parents)
  if (!decision.governed) return openByDefault
  return decision.level !== null && LEVEL_RANK[decision.level] >= LEVEL_RANK[wanted]
}

/**
 * Whether this identity may write the rules ON this resource.
 *
 * The reason ownership is worth having: the owner of an object administers it
 * without being an administrator of the platform. An `admin` grant says the
 * same thing — it is what handing over responsibility looks like before handing
 * over the object.
 */
export function mayAdminister(
  grants: readonly Grant[],
  owners: readonly Owner[],
  resource: ResourceKind,
  resourceId: string,
  identity: Identity,
  parents: readonly Scope[] = [],
): boolean {
  const decision = decide(grants, resource, resourceId, identity, owners, parents)
  if (decision.owned) return true
  return decision.governed && decision.level === 'admin'
}

/** Records an owner, replacing whoever held the resource before. */
export function withOwner(owners: readonly Owner[], owner: Owner): Owner[] {
  const next = owners.filter(
    (item) => !(item.resource === owner.resource && item.resourceId === owner.resourceId),
  )
  next.push(owner)
  return next
}

/** Drops the ownership record, which reopens the resource to the grants alone. */
export function withoutOwner(
  owners: readonly Owner[],
  resource: ResourceKind,
  resourceId: string,
): Owner[] {
  return owners.filter(
    (item) => !(item.resource === resource && item.resourceId === resourceId),
  )
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
