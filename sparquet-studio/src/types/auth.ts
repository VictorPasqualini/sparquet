/**
 * Identity and permissions, as the runner's `/auth/*` endpoints describe them.
 *
 * The runner has two modes. With no users it behaves as it always has — the
 * shared token is the whole of the authentication — and `loginRequired` is false.
 * Create a user and it starts demanding a session on top of the token; see
 * `server/auth.py`.
 */

/** One policy statement: what may (or may not) be done, and to what. */
export interface PolicyStatement {
  effect?: 'allow' | 'deny'
  /** `workspace:Write`, `run:*`, `*` — the action, or a pattern for a set of them. */
  actions?: string[]
  /** `job/j1`, `workflow/*`, `*`. Absent means every resource. */
  resources?: string[]
}

/** Whoever the runner believes is making the requests. */
export interface Principal {
  username: string
  displayName: string | null
  userId: string | null
  roles: string[]
  /** The statements behind those roles, so the UI can grey out what would 403. */
  statements: PolicyStatement[]
  /** True when there are no users and the shared token is the identity. */
  tokenOnly: boolean
}

export interface AuthStatus {
  /** Whether this runner has users at all. */
  loginRequired: boolean
  /** Who the current session belongs to, or null when there is none. */
  principal: Principal | null
}

export interface AuthSession {
  token: string
  expiresAt: string
  principal: Principal
}

export interface AuthUser {
  id: string
  username: string
  displayName: string | null
  roles: string[]
  disabled: boolean
  createdAt: string | null
  lastLoginAt: string | null
}

export interface AuthRole {
  name: string
  description: string
  statements: PolicyStatement[]
  /** False for the roles the runner ships and keeps up to date. */
  custom: boolean
}
