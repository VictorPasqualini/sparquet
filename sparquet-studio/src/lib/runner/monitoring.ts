/**
 * Client for the health of the library and the rules that alert on it
 * (`/health/jobs`, `/monitors`).
 *
 * Like `secrets.ts`, this has no offline half, and for the same kind of reason:
 * the question it answers is "what did the runs actually do", and the runs are
 * the runner's. A browser with no runner has no history to be healthy or unwell.
 *
 * Reading needs `monitoring:Read` and changing a rule needs `monitoring:Manage`,
 * so a 403 from either is an ordinary answer rather than a fault.
 */

import {
  authHeaders,
  DEFAULT_RUNNER_URL,
  RunnerError,
  RUNNER_UNREACHABLE_MESSAGE,
} from './client'

/** The four questions a rule can ask. Mirrors `monitoring.KINDS` on the runner. */
export type MonitorKind = 'failed' | 'late' | 'duration' | 'volume'

/** Whether the threshold is a number or a multiple of the Job's own median. */
export type MonitorBaseline = 'absolute' | 'median'

/** The rule that stands for "every Job in the library". */
export const ANY_JOB = '*'

export const MONITOR_KINDS: readonly MonitorKind[] = ['failed', 'late', 'duration', 'volume']

export interface Monitor {
  id: string
  kind: MonitorKind
  jobId: string
  threshold: number
  baseline: MonitorBaseline
  window: number
  enabled: boolean
  name: string | null
  createdAt: string
  updatedAt: string
  /** The rule in words, written by the runner so every surface says it the same. */
  rule: string
}

export interface MonitorDraft {
  kind: MonitorKind
  jobId?: string
  threshold?: number
  baseline?: MonitorBaseline
  window?: number
  enabled?: boolean
  name?: string | null
}

export interface MonitorState {
  monitorId: string
  jobId: string
  firing: boolean
  reason: string
  since: string | null
  checkedAt: string | null
  value: number | null
  baseline: number | null
  runId: string | null
  kind: MonitorKind | null
  rule: string | null
  name: string | null
  jobName: string | null
}

export interface MonitorEvent {
  id: string
  monitorId: string
  jobId: string
  at: string
  firing: boolean
  reason: string
  value: number | null
  baseline: number | null
  runId: string | null
}

export interface MonitorSweep {
  checked: number
  firing: number
  transitions: MonitorEvent[]
}

export interface JobHealth {
  jobId: string
  name: string | null
  workflowId: string | null
  lastRunId: string | null
  lastStatus: string | null
  lastStartedAt: string | null
  lastFinishedAt: string | null
  lastDurationMs: number | null
  lastRowsRead: number | null
  lastRowsWritten: number | null
  lastError: string | null
  lastSuccessAt: string | null
  consecutiveFailures: number
  runs: number
  failures: number
  /** Past successful runs, newest first — what the sparkline draws. */
  durations: number[]
  volumes: number[]
}

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

function asNullableNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function asNumbers(value: unknown): number[] {
  return Array.isArray(value)
    ? value.filter((item): item is number => typeof item === 'number' && Number.isFinite(item))
    : []
}

function asKind(value: unknown): MonitorKind {
  const kind = asString(value)
  return (MONITOR_KINDS as readonly string[]).includes(kind) ? (kind as MonitorKind) : 'failed'
}

function asBaseline(value: unknown): MonitorBaseline {
  return value === 'median' ? 'median' : 'absolute'
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
    // Not JSON. The status line below says as much as there is to say.
  }
  return `Local runner error (HTTP ${response.status})`
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
      headers: {
        ...authHeaders(token),
        ...(init.body ? { 'Content-Type': 'application/json' } : {}),
        ...(init.headers ?? {}),
      },
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

function toMonitor(value: unknown): Monitor | null {
  if (!isRecord(value)) return null
  return {
    id: asString(value.id),
    kind: asKind(value.kind),
    jobId: asString(value.job_id, ANY_JOB),
    threshold: asNumber(value.threshold, 1),
    baseline: asBaseline(value.baseline),
    window: asNumber(value.window, 10),
    enabled: value.enabled !== false,
    name: asNullableString(value.name),
    createdAt: asString(value.created_at),
    updatedAt: asString(value.updated_at),
    rule: asString(value.rule),
  }
}

function toState(value: unknown): MonitorState | null {
  if (!isRecord(value)) return null
  return {
    monitorId: asString(value.monitor_id),
    jobId: asString(value.job_id),
    firing: value.firing === true,
    reason: asString(value.reason),
    since: asNullableString(value.since),
    checkedAt: asNullableString(value.checked_at),
    value: asNullableNumber(value.value),
    baseline: asNullableNumber(value.baseline),
    runId: asNullableString(value.run_id),
    kind: typeof value.kind === 'string' ? asKind(value.kind) : null,
    rule: asNullableString(value.rule),
    name: asNullableString(value.name),
    jobName: asNullableString(value.job_name),
  }
}

function toEvent(value: unknown): MonitorEvent | null {
  if (!isRecord(value)) return null
  return {
    id: asString(value.id),
    monitorId: asString(value.monitor_id),
    jobId: asString(value.job_id),
    at: asString(value.at),
    firing: value.firing === true,
    reason: asString(value.reason),
    value: asNullableNumber(value.value),
    baseline: asNullableNumber(value.baseline),
    runId: asNullableString(value.run_id),
  }
}

function toHealth(value: unknown): JobHealth | null {
  if (!isRecord(value)) return null
  return {
    jobId: asString(value.job_id),
    name: asNullableString(value.name),
    workflowId: asNullableString(value.workflow_id),
    lastRunId: asNullableString(value.last_run_id),
    lastStatus: asNullableString(value.last_status),
    lastStartedAt: asNullableString(value.last_started_at),
    lastFinishedAt: asNullableString(value.last_finished_at),
    lastDurationMs: asNullableNumber(value.last_duration_ms),
    lastRowsRead: asNullableNumber(value.last_rows_read),
    lastRowsWritten: asNullableNumber(value.last_rows_written),
    lastError: asNullableString(value.last_error),
    lastSuccessAt: asNullableString(value.last_success_at),
    consecutiveFailures: asNumber(value.consecutive_failures),
    runs: asNumber(value.runs),
    failures: asNumber(value.failures),
    durations: asNumbers(value.durations),
    volumes: asNumbers(value.volumes),
  }
}

function notNull<T>(value: T | null): value is T {
  return value !== null
}

/**
 * A 403 rather than a fault: the runner works, this person may not read this.
 *
 * Worth telling apart from every other failure, because "nothing is being
 * watched" and "you may not see what is being watched" call for different
 * screens.
 */
export function isForbidden(error: unknown): boolean {
  return error instanceof RunnerError && error.status === 403
}

/** Every Job in the library and what its own runs say about it. */
export async function fetchJobHealth(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<JobHealth[]> {
  const payload = await call(baseUrl, '/health/jobs', { method: 'GET' }, token, signal)
  return Array.isArray(payload) ? payload.map(toHealth).filter(notNull) : []
}

export async function listMonitors(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<Monitor[]> {
  const payload = await call(baseUrl, '/monitors', { method: 'GET' }, token, signal)
  return Array.isArray(payload) ? payload.map(toMonitor).filter(notNull) : []
}

export async function createMonitor(
  draft: MonitorDraft,
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<Monitor | null> {
  const body: Record<string, unknown> = { kind: draft.kind }
  if (draft.jobId !== undefined) body.job_id = draft.jobId
  if (draft.threshold !== undefined) body.threshold = draft.threshold
  if (draft.baseline !== undefined) body.baseline = draft.baseline
  if (draft.window !== undefined) body.window = draft.window
  if (draft.enabled !== undefined) body.enabled = draft.enabled
  if (draft.name !== undefined) body.name = draft.name
  const payload = await call(
    baseUrl,
    '/monitors',
    { method: 'POST', body: JSON.stringify(body) },
    token,
    signal,
  )
  return toMonitor(payload)
}

/**
 * Changes only the fields given.
 *
 * The runner drops the rule's recorded verdict whenever the question changes, so
 * a threshold edit clears an alert that was answering the old one rather than
 * leaving it on screen asserting something nothing is checking.
 */
export async function updateMonitor(
  id: string,
  changes: Partial<MonitorDraft>,
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<Monitor | null> {
  const body: Record<string, unknown> = {}
  if (changes.kind !== undefined) body.kind = changes.kind
  if (changes.jobId !== undefined) body.job_id = changes.jobId
  if (changes.threshold !== undefined) body.threshold = changes.threshold
  if (changes.baseline !== undefined) body.baseline = changes.baseline
  if (changes.window !== undefined) body.window = changes.window
  if (changes.enabled !== undefined) body.enabled = changes.enabled
  if (changes.name !== undefined) body.name = changes.name
  const payload = await call(
    baseUrl,
    `/monitors/${encodeURIComponent(id)}`,
    { method: 'PATCH', body: JSON.stringify(body) },
    token,
    signal,
  )
  return toMonitor(payload)
}

export async function deleteMonitor(
  id: string,
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<void> {
  await call(baseUrl, `/monitors/${encodeURIComponent(id)}`, { method: 'DELETE' }, token, signal)
}

/** What every rule is currently saying, one row per Job it watches. */
export async function fetchMonitorStatus(
  baseUrl: string = DEFAULT_RUNNER_URL,
  options: { firingOnly?: boolean } = {},
  token?: string,
  signal?: AbortSignal,
): Promise<MonitorState[]> {
  const query = options.firingOnly ? '?firing_only=true' : ''
  const payload = await call(baseUrl, `/monitors/status${query}`, { method: 'GET' }, token, signal)
  return Array.isArray(payload) ? payload.map(toState).filter(notNull) : []
}

/** The transitions, newest first: when each alert started and when it cleared. */
export async function fetchMonitorEvents(
  baseUrl: string = DEFAULT_RUNNER_URL,
  options: { limit?: number; monitorId?: string } = {},
  token?: string,
  signal?: AbortSignal,
): Promise<MonitorEvent[]> {
  const params = new URLSearchParams()
  if (options.limit) params.set('limit', String(options.limit))
  if (options.monitorId) params.set('monitor_id', options.monitorId)
  const query = params.toString()
  const payload = await call(
    baseUrl,
    `/monitors/events${query ? `?${query}` : ''}`,
    { method: 'GET' },
    token,
    signal,
  )
  return Array.isArray(payload) ? payload.map(toEvent).filter(notNull) : []
}

/**
 * Runs the sweep now instead of waiting for the runner's timer.
 *
 * What somebody who has just fixed a Job presses, rather than watching a stale
 * alert for a minute to find out whether it cleared.
 */
export async function evaluateMonitors(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<MonitorSweep> {
  const payload = await call(baseUrl, '/monitors/evaluate', { method: 'POST' }, token, signal)
  if (!isRecord(payload)) return { checked: 0, firing: 0, transitions: [] }
  return {
    checked: asNumber(payload.checked),
    firing: asNumber(payload.firing),
    transitions: Array.isArray(payload.transitions)
      ? payload.transitions.map(toEvent).filter(notNull)
      : [],
  }
}

/**
 * The rule in words, for a draft that has not been saved yet.
 *
 * A saved rule carries `rule` from the runner, which is the version that also
 * reaches the webhook; this is only for the form, where there is no rule yet to
 * ask about. The two wordings are kept in step by a test.
 */
export function describeDraft(draft: MonitorDraft): string {
  const where = !draft.jobId || draft.jobId === ANY_JOB ? 'any Job' : `Job ${draft.jobId}`
  const threshold = draft.threshold ?? 1
  const median = draft.baseline === 'median'
  switch (draft.kind) {
    case 'failed': {
      const times = Math.trunc(threshold)
      return times > 1
        ? `${where}: last ${times} consecutive runs failed`
        : `${where}: last run failed`
    }
    case 'late':
      return `${where}: no successful run in ${Math.trunc(threshold)} minutes`
    case 'duration':
      return median
        ? `${where}: last run took more than ${threshold}x its median`
        : `${where}: last run took more than ${Math.trunc(threshold)} ms`
    case 'volume':
      return median
        ? `${where}: last run wrote less than ${threshold}x its median`
        : `${where}: last run wrote fewer than ${Math.trunc(threshold)} rows`
    default:
      return where
  }
}
