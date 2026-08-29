/**
 * Client for where the runner keeps the library (`/workspace/root`).
 *
 * The JSON files are the product. Somebody who cannot find their Jobs is almost
 * always looking at a different directory than the runner is, so this reads back
 * the path *and* the reason it is that one, and lets an administrator point the
 * runner somewhere else.
 *
 * Changing the root does not copy anything. The runner starts reading and
 * writing the new place, which makes this the way to *adopt* a directory that
 * already holds a library — a shared checkout, a synced folder, a mounted
 * volume. Moving the files is the operator's job: a half-finished copy with no
 * way back is worse than a move nobody made.
 */

import type { WorkspaceLocation } from '@/types/workspace'

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

function toLocation(value: unknown): WorkspaceLocation {
  const record = isRecord(value) ? value : {}
  return {
    root: asString(record.root),
    source: asString(record.source, 'default') as WorkspaceLocation['source'],
    default: asString(record.default),
    settingsFile: asString(record.settings_file),
    writable: record.writable !== false,
    insideSourceTree: record.inside_source_tree === true,
    locked: record.locked === true,
  }
}

async function call(
  baseUrl: string,
  init: RequestInit,
  token?: string,
  signal?: AbortSignal,
): Promise<WorkspaceLocation> {
  let response: Response
  try {
    response = await fetch(`${normalizeBaseUrl(baseUrl)}/workspace/root`, {
      ...init,
      headers: { 'Content-Type': 'application/json', ...authHeaders(token) },
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
    return toLocation(await response.json())
  } catch (error) {
    throw new RunnerError(
      'The local runner returned a malformed response.',
      'malformed',
      response.status,
      error,
    )
  }
}

/** Where the library is, and why it is there. Needs `workspace:Read`. */
export function getWorkspaceRoot(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<WorkspaceLocation> {
  return call(baseUrl, { method: 'GET' }, token, signal)
}

/**
 * Points the runner at another directory. `null` goes back to the default.
 *
 * Needs `runner:Configure`, which is deliberately outside the `workspace:*`
 * family: deciding where the runner writes on its host is an administrator's
 * call, and `editor` holds `workspace:*`.
 */
export function setWorkspaceRoot(
  baseUrl: string = DEFAULT_RUNNER_URL,
  root: string | null,
  token?: string,
  signal?: AbortSignal,
): Promise<WorkspaceLocation> {
  return call(
    baseUrl,
    { method: 'PUT', body: JSON.stringify({ root }) },
    token,
    signal,
  )
}
