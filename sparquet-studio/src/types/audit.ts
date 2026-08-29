/**
 * The runner's audit log, as `GET /audit` describes it.
 *
 * One row per mutation, plus one per refused request — a 401, a 402 or a 403 is
 * exactly the thing somebody looks for afterwards, so it is recorded even though
 * it never reached a route. The log is append-only and written by the server: a
 * client can read it and nothing else.
 */

export interface AuditEvent {
  id: string
  /** ISO-8601, UTC, ending in `Z`. */
  at: string
  /** The username, or `token` when a shared runner token was used. */
  actor: string
  actorId: string | null
  team: string | null
  roles: string[]
  /** `iam:CreateUser`, `run:Execute`, … The service before the colon. */
  action: string
  method: string
  path: string
  /** What the action was about, when the route names one — a user id, a run id. */
  resource: string | null
  /** `allowed` or `denied`. */
  outcome: string
  /** The HTTP status, when the request got one. */
  status: number | null
  detail: Record<string, unknown> | null
  ip: string | null
}

export interface AuditQuery {
  limit?: number
  actorId?: string
  resource?: string
  outcome?: string
  /** Accepts a service wildcard: `iam:*` is everything that touched access. */
  action?: string
  /** ISO-8601. Only events at or after it. */
  since?: string
}
