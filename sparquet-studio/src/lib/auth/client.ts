/**
 * The runner's identity endpoints: who is logged in, logging in and out, and —
 * for an administrator — who else has access.
 *
 * Separate from `runner/client.ts` (which runs pipelines) and from
 * `runner/history.ts` (which reads past ones) for the same reason those are
 * separate from each other: three different questions, three different failure
 * modes. A runner that cannot run Spark can still say who you are.
 *
 * The shared token still travels on every one of these calls. The session, once
 * there is one, is attached by `authHeaders` in `runner/client.ts`.
 */

import { authHeaders, DEFAULT_RUNNER_URL, RunnerError, RUNNER_UNREACHABLE_MESSAGE } from '@/lib/runner/client'
import type {
  AuthRole,
  AuthSession,
  AuthStatus,
  AuthTeam,
  AuthUser,
  Principal,
  PolicyAction,
  PolicyStatement,
  PolicyVocabulary,
  RecoveryCode,
} from '@/types/auth'

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function asString(value: unknown, fallback = ''): string {
  return typeof value === 'string' ? value : fallback
}

function asNullableString(value: unknown): string | null {
  return typeof value === 'string' ? value : null
}

function asNumber(value: unknown, fallback = 0): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback
}

function asStringList(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : []
}

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, '')
}

function toStatements(value: unknown): PolicyStatement[] {
  if (!Array.isArray(value)) return []
  return value.filter(isRecord).map((statement) => ({
    effect: statement.effect === 'deny' ? 'deny' : 'allow',
    actions: asStringList(statement.actions),
    resources: asStringList(statement.resources),
  }))
}

export function toPrincipal(value: unknown): Principal | null {
  if (!isRecord(value)) return null
  return {
    username: asString(value.username),
    displayName: asNullableString(value.display_name),
    userId: asNullableString(value.user_id),
    roles: asStringList(value.roles),
    statements: toStatements(value.statements),
    tokenOnly: value.token_only === true,
    teamId: asNullableString(value.team_id),
    teamName: asNullableString(value.team_name),
    teamRoles: asStringList(value.team_roles),
  }
}

function toUser(value: unknown): AuthUser | null {
  if (!isRecord(value)) return null
  return {
    id: asString(value.id),
    username: asString(value.username),
    displayName: asNullableString(value.display_name),
    roles: asStringList(value.roles),
    disabled: value.disabled === true,
    createdAt: asNullableString(value.created_at),
    lastLoginAt: asNullableString(value.last_login_at),
    teamId: asNullableString(value.team_id),
    teamName: asNullableString(value.team_name),
  }
}

function toRole(value: unknown): AuthRole {
  const record = isRecord(value) ? value : {}
  return {
    name: asString(record.name),
    description: asString(record.description),
    statements: toStatements(record.statements),
    custom: record.custom === true,
  }
}

function toTeam(value: unknown): AuthTeam {
  const record = isRecord(value) ? value : {}
  return {
    id: asString(record.id),
    name: asString(record.name),
    roles: asStringList(record.roles),
    members: asNumber(record.members),
    createdAt: asNullableString(record.created_at),
  }
}

function toAction(value: unknown): PolicyAction {
  const record = isRecord(value) ? value : {}
  return {
    name: asString(record.name),
    description: asString(record.description),
    service: asString(record.service),
  }
}

async function readErrorMessage(response: Response): Promise<string> {
  let body = ''
  try {
    body = await response.text()
  } catch {
    body = ''
  }
  try {
    const parsed: unknown = JSON.parse(body)
    if (isRecord(parsed) && typeof parsed.detail === 'string' && parsed.detail.length > 0) {
      return parsed.detail
    }
  } catch {
    /* not JSON — fall through to the status line */
  }
  return `Local runner error (HTTP ${response.status})`
}

async function request(
  baseUrl: string,
  path: string,
  init: RequestInit,
  token?: string,
  signal?: AbortSignal,
): Promise<unknown> {
  let response: Response
  const headers: Record<string, string> = { ...authHeaders(token) }
  if (init.body !== undefined) headers['content-type'] = 'application/json'
  try {
    response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, { ...init, headers, signal })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error
    throw new RunnerError(RUNNER_UNREACHABLE_MESSAGE, 'unreachable', undefined, error)
  }
  if (!response.ok) {
    throw new RunnerError(await readErrorMessage(response), 'http', response.status)
  }
  try {
    return (await response.json()) as unknown
  } catch (error) {
    throw new RunnerError(
      'The local runner returned a malformed response.',
      'malformed',
      response.status,
      error,
    )
  }
}

/**
 * Whether this runner needs a login, and who it already believes you are.
 *
 * The one call that answers without a session, because Studio has to ask it
 * before it can have one.
 */
export async function getAuthStatus(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<AuthStatus> {
  const payload = await request(baseUrl, '/auth/status', { method: 'GET' }, token, signal)
  const record = isRecord(payload) ? payload : {}
  return {
    loginRequired: record.login_required === true,
    principal: toPrincipal(record.principal),
  }
}

export async function login(
  baseUrl: string = DEFAULT_RUNNER_URL,
  credentials: { username: string; password: string },
  token?: string,
  signal?: AbortSignal,
): Promise<AuthSession> {
  const payload = await request(
    baseUrl,
    '/auth/login',
    { method: 'POST', body: JSON.stringify(credentials) },
    token,
    signal,
  )
  const record = isRecord(payload) ? payload : {}
  const principal = toPrincipal(record.user)
  if (!principal || typeof record.token !== 'string') {
    throw new RunnerError('The local runner returned a malformed response.', 'malformed')
  }
  return { token: record.token, expiresAt: asString(record.expires_at), principal }
}

/** Ends the session. Never throws for "there was no session" — that is the goal. */
export async function logout(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<void> {
  await request(baseUrl, '/auth/logout', { method: 'POST' }, token, signal)
}

/** The full principal — roles and the statements behind them — for the session in play. */
export async function getMe(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<Principal | null> {
  return toPrincipal(await request(baseUrl, '/auth/me', { method: 'GET' }, token, signal))
}

export async function listUsers(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<AuthUser[]> {
  const payload = await request(baseUrl, '/auth/users', { method: 'GET' }, token, signal)
  return Array.isArray(payload)
    ? payload.map(toUser).filter((user): user is AuthUser => user !== null)
    : []
}

export async function listRoles(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<AuthRole[]> {
  const payload = await request(baseUrl, '/auth/roles', { method: 'GET' }, token, signal)
  return Array.isArray(payload) ? payload.map(toRole) : []
}

/**
 * Everything a policy statement may name, as the runner itself lists it.
 *
 * The role editor is built from this rather than from a copy kept in the client,
 * so an action added to the runner appears in the UI without a second change —
 * and the editor can never offer one the server would reject.
 */
export async function getPolicyVocabulary(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<PolicyVocabulary> {
  const payload = await request(baseUrl, '/auth/policy', { method: 'GET' }, token, signal)
  const record = isRecord(payload) ? payload : {}
  return {
    actions: Array.isArray(record.actions) ? record.actions.map(toAction) : [],
    resourceKinds: Array.isArray(record.resource_kinds)
      ? record.resource_kinds.map(toAction)
      : [],
  }
}

/** Creates a role. Built-in names are refused by the runner. Needs `iam:ManageRoles`. */
export async function createRole(
  baseUrl: string = DEFAULT_RUNNER_URL,
  body: { name: string; description: string; statements: PolicyStatement[] },
  token?: string,
  signal?: AbortSignal,
): Promise<AuthRole> {
  return toRole(
    await request(baseUrl, '/auth/roles', { method: 'POST', body: JSON.stringify(body) }, token, signal),
  )
}

export async function updateRole(
  baseUrl: string = DEFAULT_RUNNER_URL,
  name: string,
  changes: { description?: string; statements?: PolicyStatement[] },
  token?: string,
  signal?: AbortSignal,
): Promise<AuthRole> {
  return toRole(
    await request(
      baseUrl,
      `/auth/roles/${encodeURIComponent(name)}`,
      { method: 'PATCH', body: JSON.stringify(changes) },
      token,
      signal,
    ),
  )
}

export async function deleteRole(
  baseUrl: string = DEFAULT_RUNNER_URL,
  name: string,
  token?: string,
  signal?: AbortSignal,
): Promise<void> {
  await request(baseUrl, `/auth/roles/${encodeURIComponent(name)}`, { method: 'DELETE' }, token, signal)
}

export async function listTeams(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<AuthTeam[]> {
  const payload = await request(baseUrl, '/auth/teams', { method: 'GET' }, token, signal)
  return Array.isArray(payload) ? payload.map(toTeam) : []
}

export async function createTeam(
  baseUrl: string = DEFAULT_RUNNER_URL,
  body: { name: string; roles: string[] },
  token?: string,
  signal?: AbortSignal,
): Promise<AuthTeam> {
  return toTeam(
    await request(baseUrl, '/auth/teams', { method: 'POST', body: JSON.stringify(body) }, token, signal),
  )
}

export async function updateTeam(
  baseUrl: string = DEFAULT_RUNNER_URL,
  teamId: string,
  changes: { name?: string; roles?: string[] },
  token?: string,
  signal?: AbortSignal,
): Promise<AuthTeam> {
  return toTeam(
    await request(
      baseUrl,
      `/auth/teams/${encodeURIComponent(teamId)}`,
      { method: 'PATCH', body: JSON.stringify(changes) },
      token,
      signal,
    ),
  )
}

export async function deleteTeam(
  baseUrl: string = DEFAULT_RUNNER_URL,
  teamId: string,
  token?: string,
  signal?: AbortSignal,
): Promise<void> {
  await request(baseUrl, `/auth/teams/${encodeURIComponent(teamId)}`, { method: 'DELETE' }, token, signal)
}

export async function createUser(
  baseUrl: string = DEFAULT_RUNNER_URL,
  body: {
    username: string
    password: string
    roles: string[]
    displayName?: string
    /** Team id or name. Omitted puts them in the default team. */
    team?: string
  },
  token?: string,
  signal?: AbortSignal,
): Promise<AuthUser | null> {
  return toUser(
    await request(
      baseUrl,
      '/auth/users',
      {
        method: 'POST',
        body: JSON.stringify({
          username: body.username,
          password: body.password,
          roles: body.roles,
          display_name: body.displayName ?? null,
          team: body.team ?? null,
        }),
      },
      token,
      signal,
    ),
  )
}

export async function updateUser(
  baseUrl: string = DEFAULT_RUNNER_URL,
  userId: string,
  changes: { roles?: string[]; disabled?: boolean; team?: string },
  token?: string,
  signal?: AbortSignal,
): Promise<AuthUser | null> {
  return toUser(
    await request(
      baseUrl,
      `/auth/users/${encodeURIComponent(userId)}`,
      { method: 'PATCH', body: JSON.stringify(changes) },
      token,
      signal,
    ),
  )
}

/**
 * Sets a password: your own (with the current one) or, for an administrator,
 * somebody else's (without it — that is what a reset is).
 */
export async function setPassword(
  baseUrl: string = DEFAULT_RUNNER_URL,
  userId: string,
  body: { password: string; currentPassword?: string },
  token?: string,
  signal?: AbortSignal,
): Promise<void> {
  await request(
    baseUrl,
    `/auth/users/${encodeURIComponent(userId)}/password`,
    {
      method: 'POST',
      body: JSON.stringify({
        password: body.password,
        current_password: body.currentPassword ?? null,
      }),
    },
    token,
    signal,
  )
}

export async function deleteUser(
  baseUrl: string = DEFAULT_RUNNER_URL,
  userId: string,
  token?: string,
  signal?: AbortSignal,
): Promise<void> {
  await request(
    baseUrl,
    `/auth/users/${encodeURIComponent(userId)}`,
    { method: 'DELETE' },
    token,
    signal,
  )
}

/**
 * Mints a single-use recovery code for somebody who cannot log in.
 *
 * The code comes back once and is never readable again — the runner keeps only
 * its hash — so whoever calls this has to hand it over immediately.
 *
 * `password` is the **caller's own**, re-entered: minting a code is a way to take
 * over an account, and an unattended session should not be enough on its own. It
 * cannot be the password of the person being recovered — they are the one who
 * cannot supply it.
 */
export async function issueRecovery(
  baseUrl: string = DEFAULT_RUNNER_URL,
  userId: string,
  password: string,
  token?: string,
  signal?: AbortSignal,
): Promise<RecoveryCode> {
  const payload = await request(
    baseUrl,
    `/auth/users/${encodeURIComponent(userId)}/recovery`,
    { method: 'POST', body: JSON.stringify({ password }) },
    token,
    signal,
  )
  const record = isRecord(payload) ? payload : {}
  return {
    userId: asString(record.user_id),
    username: asString(record.username),
    code: asString(record.code),
    expiresAt: asString(record.expires_at),
  }
}

/**
 * Trades a recovery code for a new password. Called from the login screen, so it
 * carries no session — the whole point is that the caller has none.
 */
export async function recoverPassword(
  baseUrl: string = DEFAULT_RUNNER_URL,
  body: { code: string; password: string },
  token?: string,
  signal?: AbortSignal,
): Promise<void> {
  await request(baseUrl, '/auth/recover', { method: 'POST', body: JSON.stringify(body) }, token, signal)
}
