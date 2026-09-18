/**
 * Client for the connection secrets (`/secrets`).
 *
 * The runner is the only place a secret exists, which makes this one of the few
 * parts of Studio with no offline half: with no runner there are no secrets, and
 * a Job that references one simply cannot run. That is the right failure. The
 * alternative — keeping the material in the browser so the canvas works alone —
 * is exactly what this feature was built to avoid.
 *
 * Reading the list needs `secrets:Read` and writing needs `secrets:Write`, so a
 * 403 here is an ordinary answer rather than a fault.
 */

import type { AccessLevel } from '@/lib/iam'
import type { Secret, SecretCheck, SecretProvider, SecretWrite } from '@/types/secrets'

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

function asStrings(value: unknown): string[] {
  return Array.isArray(value) ? value.map((item) => asString(item)).filter(Boolean) : []
}

function asStringMap(value: unknown): Record<string, string> {
  if (!isRecord(value)) return {}
  const out: Record<string, string> = {}
  for (const [key, item] of Object.entries(value)) out[key] = asString(item)
  return out
}

function asLevel(value: unknown): AccessLevel | null {
  return value === 'read' || value === 'write' || value === 'admin' ? value : null
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

function toSecret(value: unknown): Secret {
  const record = isRecord(value) ? value : {}
  const provider = asString(record.provider, 'local')
  return {
    name: asString(record.name),
    provider: (provider === 'env' ? 'env' : 'local') as SecretProvider,
    description: asString(record.description),
    tags: asStrings(record.tags),
    fields: asStrings(record.fields),
    binding: asStringMap(record.binding),
    updatedAt: typeof record.updated_at === 'number' ? record.updated_at : 0,
    updatedBy: asString(record.updated_by),
    governed: record.governed === true,
    level: asLevel(record.level),
    owned: record.owned === true,
  }
}

async function call(
  baseUrl: string,
  path: string,
  init: RequestInit,
  token?: string,
  signal?: AbortSignal,
): Promise<unknown> {
  let response: Response
  try {
    response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, {
      ...init,
      headers: { ...authHeaders(token), ...(init.headers ?? {}) },
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
 * Every secret this caller may reach, by name.
 *
 * A secret somebody else closed off is not in the list at all rather than shown
 * greyed: the name of a credential is itself information, and a deny on
 * `secret/pg-prod` should not leave the production database on the screen.
 */
export async function listSecrets(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<Secret[]> {
  const payload = await call(baseUrl, '/secrets', { method: 'GET' }, token, signal)
  return Array.isArray(payload) ? payload.map(toSecret) : []
}

/** Creates a secret, or changes one — including rotating a single field. */
export async function putSecret(
  name: string,
  body: SecretWrite,
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<Secret> {
  const payload = await call(
    baseUrl,
    `/secrets/${encodeURIComponent(name)}`,
    {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    },
    token,
    signal,
  )
  return toSecret(payload)
}

export async function deleteSecret(
  name: string,
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<void> {
  await call(baseUrl, `/secrets/${encodeURIComponent(name)}`, { method: 'DELETE' }, token, signal)
}

/**
 * Whether every field still resolves — the master key, the variable, the file.
 *
 * Deliberately not a connection test: reaching the database needs its driver on
 * the classpath and a network route, and a failure there says nothing about the
 * secret. This answers the part the store is responsible for.
 */
export async function checkSecret(
  name: string,
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<SecretCheck> {
  const payload = await call(
    baseUrl,
    `/secrets/${encodeURIComponent(name)}/check`,
    { method: 'POST' },
    token,
    signal,
  )
  const record = isRecord(payload) ? payload : {}
  return {
    name: asString(record.name, name),
    fields: asStringMap(record.fields),
    healthy: record.healthy === true,
  }
}

/** Whether this failure means "you may not see the secrets". */
export function isForbidden(error: unknown): boolean {
  return error instanceof RunnerError && error.status === 403
}
