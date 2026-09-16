/**
 * Client for the schedules the runner fires on its own (`/schedules`).
 *
 * Like `monitoring.ts`, this has no offline half. A schedule is written into the
 * library record and travels with the project, so the browser already knows what
 * *should* happen; what only the runner knows is what will happen next and what
 * happened last, and a browser with no runner has neither.
 *
 * Reading needs `workspace:Read` — a schedule is part of the record, so anybody
 * who can see the Job can see when it runs. Firing what is due needs
 * `run:Execute`, because it starts executions. A 403 from either is an ordinary
 * answer rather than a fault.
 */

import {
  authHeaders,
  DEFAULT_RUNNER_URL,
  RunnerError,
  RUNNER_UNREACHABLE_MESSAGE,
} from './client'

/** What a schedule can be attached to. Mirrors `scheduling.KINDS` on the runner. */
export type ScheduleKind = 'job' | 'pipeline'

/** The timezone value meaning "the clock of the machine running the runner". */
export const LOCAL_ZONE = 'local'

export interface ScheduleEntry {
  kind: ScheduleKind
  id: string
  name: string
  cron: string
  timezone: string
  enabled: boolean
  runAs: string
  workflowId: string
  /** Why this schedule cannot fire, written by the runner. Null when it can. */
  error: string | null
  /** The schedule in words, so every surface says it the same way. */
  rule: string
  /** Null when the schedule is paused or its expression cannot be read. */
  nextFire: string | null
  lastFire: string | null
  lastRunId: string | null
  lastStatus: string | null
}

/** One schedule the runner tried to start during a sweep. */
export interface ScheduleFire {
  kind: ScheduleKind
  id: string
  name: string
  dueAt: string
  started: boolean
  runId: string | null
  /** Why it did not start — no compiled file, a run already in flight, an
   * unknown `runAs`. Null when it did. */
  error: string | null
}

export interface ScheduleSweep {
  checked: number
  fired: number
  fires: ScheduleFire[]
}

const KINDS: readonly ScheduleKind[] = ['job', 'pipeline']

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

function asKind(value: unknown): ScheduleKind {
  const kind = asString(value)
  return (KINDS as readonly string[]).includes(kind) ? (kind as ScheduleKind) : 'job'
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

function toEntry(value: unknown): ScheduleEntry | null {
  if (!isRecord(value)) return null
  return {
    kind: asKind(value.kind),
    id: asString(value.id),
    name: asString(value.name),
    cron: asString(value.cron),
    timezone: asString(value.timezone, LOCAL_ZONE),
    enabled: value.enabled !== false,
    runAs: asString(value.run_as),
    workflowId: asString(value.workflow_id),
    error: asNullableString(value.error),
    rule: asString(value.rule),
    nextFire: asNullableString(value.next_fire),
    lastFire: asNullableString(value.last_fire),
    lastRunId: asNullableString(value.last_run_id),
    lastStatus: asNullableString(value.last_status),
  }
}

function toFire(value: unknown): ScheduleFire | null {
  if (!isRecord(value)) return null
  return {
    kind: asKind(value.kind),
    id: asString(value.id),
    name: asString(value.name),
    dueAt: asString(value.due_at),
    started: value.started === true,
    runId: asNullableString(value.run_id),
    error: asNullableString(value.error),
  }
}

function notNull<T>(value: T | null): value is T {
  return value !== null
}

/**
 * A 403 rather than a fault: the runner works, this person may not see this.
 *
 * Worth telling apart from every other failure, because "nothing is scheduled"
 * and "you may not see what is scheduled" call for different screens.
 */
export function isForbidden(error: unknown): boolean {
  return error instanceof RunnerError && error.status === 403
}

/** Every schedule in the library, with what the runner intends to do next. */
export async function listSchedules(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<ScheduleEntry[]> {
  const payload = await call(baseUrl, '/schedules', { method: 'GET' }, token, signal)
  return Array.isArray(payload) ? payload.map(toEntry).filter(notNull) : []
}

/**
 * Runs the sweep now instead of waiting for the timer.
 *
 * This is not "run this Job" — it is "do whatever was already due". A schedule
 * that is not due fires nothing, which is why the answer counts what it checked
 * as well as what it started.
 */
export async function evaluateSchedules(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<ScheduleSweep> {
  const payload = await call(baseUrl, '/schedules/evaluate', { method: 'POST' }, token, signal)
  if (!isRecord(payload)) return { checked: 0, fired: 0, fires: [] }
  return {
    checked: asNumber(payload.checked),
    fired: asNumber(payload.fired),
    fires: Array.isArray(payload.fires) ? payload.fires.map(toFire).filter(notNull) : [],
  }
}
