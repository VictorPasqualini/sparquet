/**
 * Client for the runner's persisted execution history (`GET /runs`, `GET /runs/{id}`).
 *
 * Separate from `client.ts` on purpose: that module speaks to the STREAM of a run
 * in progress; this one reads back what the runner already committed to its
 * SQLite database, which is why it survives closing and reopening Studio.
 */

import type {
  ExecutionStatus,
  JobRunConfig,
  JobRunRecord,
  LineageDataset,
  PipelineRunRecord,
  RunLineage,
  RunLogPage,
  RunLogRecord,
  StepRunRecord,
} from '@/types/history'

import { toRunCharge } from './credits'

import {
  authHeaders,
  DEFAULT_RUNNER_URL,
  isRunnerError,
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

function asNullableNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

const EXECUTION_STATUSES: readonly string[] = [
  'pending',
  'running',
  'success',
  'failed',
  'skipped',
  'cancelled',
]

function asStatus(value: unknown): ExecutionStatus {
  const status = asString(value)
  // A status this client does not know is not a failure: `pending` is the one
  // reading that claims nothing about how the run ended.
  return EXECUTION_STATUSES.includes(status) ? (status as ExecutionStatus) : 'pending'
}

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, '')
}

function safeParse(text: string): unknown {
  try {
    return JSON.parse(text) as unknown
  } catch {
    return undefined
  }
}

async function readErrorMessage(response: Response): Promise<string> {
  let body = ''
  try {
    body = await response.text()
  } catch {
    body = ''
  }
  const parsed = safeParse(body)
  if (isRecord(parsed) && typeof parsed.detail === 'string' && parsed.detail.length > 0) {
    return parsed.detail
  }
  return `Local runner error (HTTP ${response.status})`
}

async function getJson(
  baseUrl: string,
  path: string,
  signal?: AbortSignal,
  token?: string,
): Promise<unknown> {
  const headers = authHeaders(token)

  let response: Response
  try {
    response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, { method: 'GET', headers, signal })
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

function toStepRun(value: unknown): StepRunRecord | null {
  if (!isRecord(value)) return null
  return {
    id: asString(value.id),
    jobRunId: asString(value.job_run_id),
    scope: asString(value.scope),
    stepIndex: asNullableNumber(value.step_index) ?? 0,
    type: asString(value.type),
    status: asStatus(value.status),
    startedAt: asNullableString(value.started_at),
    finishedAt: asNullableString(value.finished_at),
    durationMs: asNullableNumber(value.duration_ms),
    errorMessage: asNullableString(value.error_message),
    errorDetails: asNullableString(value.error_details),
    role: asNullableString(value.role),
    details: asNullableString(value.details),
  }
}

function toLineageDataset(value: unknown): LineageDataset | null {
  if (!isRecord(value)) return null
  const address = asNullableString(value.address)
  const format = asNullableString(value.format)
  // A side with neither an address nor a format describes nothing.
  if (address === null && format === null) return null
  const dataset: LineageDataset = { role: asString(value.role, 'input'), format, address }
  const mode = asNullableString(value.mode)
  if (mode !== null) dataset.mode = mode
  return dataset
}

/**
 * The lineage the runner stored as a JSON string, or null.
 *
 * Null covers three cases that all read the same way on screen — a run from
 * before lineage was recorded, a JSON that named no dataset, and a payload this
 * client cannot make sense of.
 */
function toLineage(value: unknown): RunLineage | null {
  const raw = asNullableString(value)
  if (raw === null) return null
  const parsed = safeParse(raw)
  if (!isRecord(parsed)) return null
  const inputs = Array.isArray(parsed.inputs)
    ? parsed.inputs.map(toLineageDataset).filter(notNull)
    : []
  const outputs = Array.isArray(parsed.outputs)
    ? parsed.outputs.map(toLineageDataset).filter(notNull)
    : []
  if (inputs.length === 0 && outputs.length === 0) return null
  return { inputs, outputs }
}

const LAUNCH_KINDS: readonly string[] = ['manual', 'scheduled', 'api', 'external']

function asLaunched(value: unknown): PipelineRunRecord['launched'] {
  const launched = asString(value)
  return LAUNCH_KINDS.includes(launched)
    ? (launched as NonNullable<PipelineRunRecord['launched']>)
    : null
}

function toJobRun(value: unknown): JobRunRecord | null {
  if (!isRecord(value)) return null
  const steps = Array.isArray(value.steps) ? value.steps.map(toStepRun).filter(notNull) : []
  return {
    id: asString(value.id),
    pipelineRunId: asString(value.pipeline_run_id),
    jobId: asNullableString(value.job_id),
    name: asNullableString(value.name),
    stageIndex: asNullableNumber(value.stage_index) ?? 0,
    status: asStatus(value.status),
    startedAt: asNullableString(value.started_at),
    finishedAt: asNullableString(value.finished_at),
    durationMs: asNullableNumber(value.duration_ms),
    error: asNullableString(value.error),
    rowsRead: asNullableNumber(value.rows_read),
    rowsWritten: asNullableNumber(value.rows_written),
    lineage: toLineage(value.lineage),
    configHash: asNullableString(value.config_hash),
    credits: toRunCharge(value.credits),
    steps,
  }
}

function notNull<T>(value: T | null): value is T {
  return value !== null
}

function toPipelineRun(value: unknown): PipelineRunRecord | null {
  if (!isRecord(value)) return null
  const kind = value.kind === 'pipeline' ? 'pipeline' : 'job'
  const jobs = Array.isArray(value.jobs) ? value.jobs.map(toJobRun).filter(notNull) : []
  return {
    id: asString(value.id),
    kind,
    workflowId: asNullableString(value.workflow_id),
    pipelineId: asNullableString(value.pipeline_id),
    jobId: asNullableString(value.job_id),
    name: asNullableString(value.name),
    status: asStatus(value.status),
    startedAt: asNullableString(value.started_at),
    finishedAt: asNullableString(value.finished_at),
    durationMs: asNullableNumber(value.duration_ms),
    error: asNullableString(value.error),
    runAs: asNullableString(value.run_as),
    launched: asLaunched(value.launched),
    pinned: value.pinned === true,
    jobs,
  }
}

export interface ListRunsFilter {
  workflowId?: string
  pipelineId?: string
  jobId?: string
  limit?: number
}

/** Past executions, most recent first. `jobs`/`steps` come back empty — fetch `getRun` for detail. */
export async function listRuns(
  baseUrl: string = DEFAULT_RUNNER_URL,
  filter: ListRunsFilter = {},
  signal?: AbortSignal,
  token?: string,
): Promise<PipelineRunRecord[]> {
  const params = new URLSearchParams()
  if (filter.workflowId) params.set('workflow_id', filter.workflowId)
  if (filter.pipelineId) params.set('pipeline_id', filter.pipelineId)
  if (filter.jobId) params.set('job_id', filter.jobId)
  if (filter.limit) params.set('limit', String(filter.limit))
  const query = params.toString()
  const payload = await getJson(baseUrl, `/runs${query ? `?${query}` : ''}`, signal, token)
  return Array.isArray(payload) ? payload.map(toPipelineRun).filter(notNull) : []
}

function toLogRecord(value: unknown): RunLogRecord | null {
  if (!isRecord(value)) return null
  return {
    seq: asNullableNumber(value.seq) ?? 0,
    timestamp: asString(value.timestamp),
    level: asString(value.level, 'INFO'),
    source: asString(value.source, 'pipeline'),
    message: asString(value.message),
    context: isRecord(value.context) ? value.context : {},
  }
}

/**
 * What one job execution printed, page by page.
 *
 * `after` is a `seq`, not an offset: the runner only ever appends, so paging this
 * way never re-reads or skips a line, even while the run is still going.
 */
export async function getJobRunLogs(
  baseUrl: string = DEFAULT_RUNNER_URL,
  jobRunId: string,
  options: { after?: number; limit?: number } = {},
  signal?: AbortSignal,
  token?: string,
): Promise<RunLogPage> {
  const params = new URLSearchParams()
  if (options.after) params.set('after', String(options.after))
  if (options.limit) params.set('limit', String(options.limit))
  const query = params.toString()
  const payload = await getJson(
    baseUrl,
    `/job-runs/${encodeURIComponent(jobRunId)}/logs${query ? `?${query}` : ''}`,
    signal,
    token,
  )
  const record = isRecord(payload) ? payload : {}
  const lines = Array.isArray(record.lines) ? record.lines.map(toLogRecord).filter(notNull) : []
  return {
    jobRunId: asString(record.job_run_id, jobRunId),
    lines,
    total: asNullableNumber(record.total) ?? lines.length,
    nextAfter: asNullableNumber(record.next_after),
  }
}

/**
 * The version of the JSON one execution ran.
 *
 * Fetched per execution rather than carried in the listings: a configuration is
 * far larger than the row describing the run, and a reader opens one at a time.
 * Null for an execution the runner does not know.
 */
export async function getJobRunConfig(
  baseUrl: string = DEFAULT_RUNNER_URL,
  jobRunId: string,
  signal?: AbortSignal,
  token?: string,
): Promise<JobRunConfig | null> {
  try {
    const payload = await getJson(
      baseUrl,
      `/job-runs/${encodeURIComponent(jobRunId)}/config`,
      signal,
      token,
    )
    const record = isRecord(payload) ? payload : {}
    return {
      jobRunId: asString(record.job_run_id, jobRunId),
      configHash: asNullableString(record.config_hash),
      config: isRecord(record.config) ? record.config : null,
    }
  } catch (error) {
    if (isRunnerError(error) && error.status === 404) return null
    throw error
  }
}

/** One execution in full: every job it ran (or skipped) and every step of each. */
/**
 * Marks a run as kept forever, or unmarks it. Returns the new state; null when
 * the runner does not know that run any more.
 */
export async function pinRun(
  baseUrl: string = DEFAULT_RUNNER_URL,
  runId: string,
  pinned: boolean,
  token?: string,
  signal?: AbortSignal,
): Promise<boolean | null> {
  const headers = { ...authHeaders(token), 'content-type': 'application/json' }
  let response: Response
  try {
    response = await fetch(`${normalizeBaseUrl(baseUrl)}/runs/${encodeURIComponent(runId)}/pin`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ pinned }),
      signal,
    })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error
    throw new RunnerError(RUNNER_UNREACHABLE_MESSAGE, 'unreachable', undefined, error)
  }
  if (response.status === 404) return null
  if (!response.ok) {
    throw new RunnerError(await readErrorMessage(response), 'http', response.status)
  }
  return pinned
}

export async function getRun(
  baseUrl: string = DEFAULT_RUNNER_URL,
  runId: string,
  signal?: AbortSignal,
  token?: string,
): Promise<PipelineRunRecord | null> {
  try {
    const payload = await getJson(baseUrl, `/runs/${encodeURIComponent(runId)}`, signal, token)
    return toPipelineRun(payload)
  } catch (error) {
    if (isRunnerError(error) && error.status === 404) return null
    throw error
  }
}
