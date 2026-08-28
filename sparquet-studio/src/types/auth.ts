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
  /** The team that pays for this person's runs, and grants roles of its own. */
  teamId: string | null
  teamName: string | null
  /** Roles that come with the team. `roles` above are the personal ones; the
   *  statements are the union of both. */
  teamRoles: string[]
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
  teamId: string | null
  teamName: string | null
}

/**
 * A group of people that shares one credit account and, optionally, roles.
 *
 * Teams answer two questions with one object. Billing needs somebody to charge
 * who is not an individual — a squad has one budget, not one per person — and
 * permissions need a way to say "everyone here may run things" without repeating
 * it on each account.
 */
export interface AuthTeam {
  id: string
  name: string
  /** Roles every member gets on top of their own. A team only ever grants. */
  roles: string[]
  members: number
  createdAt: string | null
}

/** One thing a policy can name, with what it guards. Built from the runner's own
 *  catalogue so the role editor can never offer an action the server rejects. */
export interface PolicyAction {
  name: string
  description: string
  /** `run`, `workspace`, `iam`, `credits`, `history` — for grouping the editor. */
  service: string
}

export interface PolicyVocabulary {
  actions: PolicyAction[]
  resourceKinds: PolicyAction[]
}

export interface AuthRole {
  name: string
  description: string
  statements: PolicyStatement[]
  /** False for the roles the runner ships and keeps up to date. */
  custom: boolean
}

/**
 * A single-use password recovery code, as `POST /auth/users/{id}/recovery`
 * returns it. Shown once: the runner stores only its hash, so this object is the
 * only copy that will ever exist, and it has to be handed over out of band.
 */
export interface RecoveryCode {
  userId: string
  username: string
  code: string
  expiresAt: string
}
