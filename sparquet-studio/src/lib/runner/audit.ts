/**
 * Client for the audit log (`GET /audit`).
 *
 * The log is the runner's own record of who did what: every mutation, and every
 * request it refused with 401, 402 or 403. It is written by the server, never by
 * this browser, which is the point — an audit trail a client could write is not
 * an audit trail. Reading it needs `iam:ReadAudit`, so this module treats a 403
 * as an ordinary answer ("you may not see this") rather than as a failure.
 */

import type { AuditEvent, AuditQuery } from '@/types/audit'

import {
  authHeaders,
  DEFAULT_RUNNER_URL,
  RunnerError,
  RUNNER_UNREACHABLE_MESSAGE,
} from './client'

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function asString(value: unknown, fallback = ''): string {
  return typeof value === 'string' ? value : fallback
}

function asNullableString(value: unknown): string | null {
  return typeof value === 'string' ? value : null
}

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, '')
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

function toEvent(value: unknown): AuditEvent {
  const record = isRecord(value) ? value : {}
  return {
    id: asString(record.id),
    at: asString(record.at),
    actor: asString(record.actor),
    actorId: asNullableString(record.actor_id),
    team: asNullableString(record.team),
    roles: Array.isArray(record.roles) ? record.roles.map((role) => asString(role)) : [],
    action: asString(record.action),
    method: asString(record.method),
    path: asString(record.path),
    resource: asNullableString(record.resource),
    outcome: asString(record.outcome, 'allowed'),
    status: typeof record.status === 'number' ? record.status : null,
    detail: isRecord(record.detail) ? record.detail : null,
    ip: asNullableString(record.ip),
  }
}

/** The audit log, newest first. Needs `iam:ReadAudit`. */
export async function listAuditEvents(
  baseUrl: string = DEFAULT_RUNNER_URL,
  query: AuditQuery = {},
  token?: string,
  signal?: AbortSignal,
): Promise<AuditEvent[]> {
  const params = new URLSearchParams()
  params.set('limit', String(query.limit ?? 100))
  if (query.actorId) params.set('actor_id', query.actorId)
  if (query.resource) params.set('resource', query.resource)
  if (query.outcome) params.set('outcome', query.outcome)
  if (query.action) params.set('action', query.action)
  if (query.since) params.set('since', query.since)

  let response: Response
  try {
    response = await fetch(`${normalizeBaseUrl(baseUrl)}/audit?${params.toString()}`, {
      method: 'GET',
      headers: authHeaders(token),
      signal,
    })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error
    throw new RunnerError(RUNNER_UNREACHABLE_MESSAGE, 'unreachable', undefined, error)
  }
  if (!response.ok) {
    throw new RunnerError(await readErrorMessage(response), 'http', response.status)
  }
  try {
    const payload: unknown = await response.json()
    return Array.isArray(payload) ? payload.map(toEvent) : []
  } catch (error) {
    throw new RunnerError(
      'The local runner returned a malformed response.',
      'malformed',
      response.status,
      error,
    )
  }
}

/** Whether this failure means "you are not allowed to read the audit log". */
export function isForbidden(error: unknown): boolean {
  return error instanceof RunnerError && error.status === 403
}
