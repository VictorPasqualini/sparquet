/**
 * Client for the optional local execution bridge (`sparquet-studio/server`).
 *
 * The runner is never required: every call fails with a `RunnerError` carrying a
 * message the UI can show verbatim, so an offline Studio degrades cleanly.
 */

import type { PipelineSpec } from '@/types/pipeline'
import type { RunLogLine, RunResult, RunStatus } from '@/types/studio'

export const DEFAULT_RUNNER_URL = 'http://127.0.0.1:8787'

/**
 * Canonical setup commands, run from the `sparquet-studio` directory.
 * server/main.py adds the repository root to sys.path itself, so `sparquet`
 * resolves without installing anything.
 */
export const RUNNER_INSTALL_COMMAND = 'pip install -r server/requirements.txt'
export const RUNNER_START_COMMAND = 'uvicorn server.main:app --port 8787'

export const RUNNER_UNREACHABLE_MESSAGE =
  `Local runner not detected — start it with \`${RUNNER_START_COMMAND}\` ` +
  'from the sparquet-studio directory.'

export type RunnerErrorKind = 'unreachable' | 'http' | 'malformed'

export class RunnerError extends Error {
  readonly kind: RunnerErrorKind
  readonly status?: number

  constructor(message: string, kind: RunnerErrorKind, status?: number, cause?: unknown) {
    super(message)
    this.name = 'RunnerError'
    this.kind = kind
    this.status = status
    if (cause !== undefined) this.cause = cause
  }
}

export function isRunnerError(value: unknown): value is RunnerError {
  return value instanceof RunnerError
}

export interface RunnerHealth {
  status: string
  version: string
  sparkAvailable: boolean
  frameworkVersion?: string
  /** The runner requires a token on /run and /validate. */
  authRequired?: boolean
}

/**
 * Header carrying the runner's shared secret. The runner prints the token when
 * it starts; without it /run and /validate answer 401, which stops any web page
 * the developer happens to visit from driving Spark on their machine.
 */
export const RUNNER_TOKEN_HEADER = 'x-sparquet-token'

export interface RunnerCapabilities {
  transformations: string[]
  readers: string[]
  writers: string[]
  validators: string[]
}

export interface RunnerValidation {
  valid: boolean
  error?: string
}

/** Values accepted by the framework's `{param}` template substitution. */
export type RunParamValue = string | number | boolean | string[] | number[]

export interface RunPipelineRequest {
  pipeline: PipelineSpec
  params?: Record<string, RunParamValue>
  /** Preview rows requested from `PipelineResult.output_df`. Server default: 50. */
  limit?: number
  /** Parse the config and return without touching Spark. */
  dryRun?: boolean
}

/* ------------------------------------------------------------------ narrow */

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function asString(value: unknown, fallback = ''): string {
  return typeof value === 'string' ? value : fallback
}

function asNumber(value: unknown, fallback = 0): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback
}

function asBoolean(value: unknown, fallback = false): boolean {
  return typeof value === 'boolean' ? value : fallback
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? (value as unknown[]) : []
}

function asStringArray(value: unknown): string[] {
  return asArray(value).filter((item): item is string => typeof item === 'string')
}

function optionalString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

/* --------------------------------------------------------------- transport */

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
  if (isRecord(parsed)) {
    const detail = parsed.detail
    if (typeof detail === 'string' && detail.length > 0) return detail
    // FastAPI request-validation errors arrive as a list of {loc, msg, type}
    const first = asArray(detail).find(isRecord)
    const message = first ? optionalString(first.msg) : undefined
    if (message) return message
    const error = optionalString(parsed.error)
    if (error) return error
  }

  const trimmed = body.trim()
  return trimmed
    ? `Local runner error (HTTP ${response.status}): ${trimmed.slice(0, 300)}`
    : `Local runner error (HTTP ${response.status})`
}

async function requestJson(
  baseUrl: string,
  path: string,
  init: RequestInit,
  signal?: AbortSignal,
): Promise<unknown> {
  let response: Response
  try {
    response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, { ...init, signal })
  } catch (error) {
    // An aborted request is the caller's own doing, not a missing runner.
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

function expectRecord(payload: unknown): Record<string, unknown> {
  if (!isRecord(payload)) {
    throw new RunnerError('The local runner returned a malformed response.', 'malformed')
  }
  return payload
}

function jsonPost(body: unknown, token?: string): RequestInit {
  const headers: Record<string, string> = { 'content-type': 'application/json' }
  if (token) headers[RUNNER_TOKEN_HEADER] = token
  return { method: 'POST', headers, body: JSON.stringify(body) }
}

/* ------------------------------------------------------------------- calls */

export async function checkRunnerHealth(
  baseUrl: string = DEFAULT_RUNNER_URL,
  signal?: AbortSignal,
): Promise<RunnerHealth> {
  const payload = expectRecord(await requestJson(baseUrl, '/health', { method: 'GET' }, signal))
  return {
    status: asString(payload.status, 'unknown'),
    version: asString(payload.version),
    sparkAvailable: asBoolean(payload.spark_available),
    frameworkVersion: optionalString(payload.framework_version),
    // Absent on runners predating the token: those accept requests unauthenticated.
    authRequired: asBoolean(payload.auth_required, false),
  }
}

export async function fetchCapabilities(
  baseUrl: string = DEFAULT_RUNNER_URL,
  signal?: AbortSignal,
): Promise<RunnerCapabilities> {
  const payload = expectRecord(
    await requestJson(baseUrl, '/capabilities', { method: 'GET' }, signal),
  )
  return {
    transformations: asStringArray(payload.transformations),
    readers: asStringArray(payload.readers),
    writers: asStringArray(payload.writers),
    validators: asStringArray(payload.validators),
  }
}

export async function validatePipeline(
  baseUrl: string = DEFAULT_RUNNER_URL,
  pipeline: PipelineSpec,
  signal?: AbortSignal,
  token?: string,
): Promise<RunnerValidation> {
  const payload = expectRecord(
    await requestJson(baseUrl, '/validate', jsonPost({ pipeline }, token), signal),
  )
  return {
    valid: asBoolean(payload.valid),
    error: optionalString(payload.error),
  }
}

export async function runPipeline(
  baseUrl: string = DEFAULT_RUNNER_URL,
  body: RunPipelineRequest,
  signal?: AbortSignal,
  token?: string,
): Promise<RunResult> {
  const payload = expectRecord(
    await requestJson(
      baseUrl,
      '/run',
      jsonPost(
        {
          pipeline: body.pipeline,
          params: body.params,
          limit: body.limit,
          dry_run: body.dryRun,
        },
        token,
      ),
      signal,
    ),
  )
  return toRunResult(payload)
}

/* ------------------------------------------------------------------ mapping */

const LOG_LEVELS: Record<string, RunLogLine['level']> = {
  debug: 'debug',
  info: 'info',
  warning: 'warning',
  warn: 'warning',
  error: 'error',
  critical: 'error',
}

function toLogLine(value: unknown): RunLogLine | null {
  if (!isRecord(value)) return null
  const parsed = Date.parse(asString(value.timestamp))
  const context = isRecord(value.context) ? value.context : undefined
  return {
    ts: Number.isNaN(parsed) ? Date.now() : parsed,
    level: LOG_LEVELS[asString(value.level).toLowerCase()] ?? 'info',
    message: asString(value.message),
    ...(context ? { context } : {}),
  }
}

function toValidations(value: unknown): RunResult['validations'] {
  return asArray(value)
    .filter(isRecord)
    .map((item) => ({
      type: asString(item.type, 'unknown'),
      passed: asBoolean(item.passed),
      message: optionalString(item.message),
      failedCount: asNumber(item.failed_count),
    }))
}

function toOutputMetrics(value: unknown): RunResult['outputMetrics'] {
  return asArray(value)
    .filter(isRecord)
    .map((item) => ({
      format: asString(item.format, ''),
      path: asString(item.path, ''),
      mode: optionalString(item.mode),
      rowsWritten: asNumber(item.rows_written),
    }))
}

function toPreview(value: unknown): RunResult['preview'] {
  if (!isRecord(value)) return undefined
  return {
    columns: asStringArray(value.columns),
    rows: asArray(value.rows).map(asArray),
    truncated: asBoolean(value.truncated),
  }
}

function toStatus(payload: Record<string, unknown>): RunStatus {
  if (!asBoolean(payload.success)) return 'error'
  return asBoolean(payload.skipped) ? 'skipped' : 'success'
}

function toRunResult(payload: Record<string, unknown>): RunResult {
  const logs = asArray(payload.logs)
    .map(toLogLine)
    .filter((line): line is RunLogLine => line !== null)

  return {
    status: toStatus(payload),
    pipelineName: optionalString(payload.pipeline_name),
    rowsRead: asNumber(payload.rows_read),
    rowsWritten: asNumber(payload.rows_written),
    durationMs: asNumber(payload.duration_ms),
    skipped: asBoolean(payload.skipped),
    error: optionalString(payload.error),
    validations: toValidations(payload.validations),
    outputMetrics: toOutputMetrics(payload.output_metrics),
    preview: toPreview(payload.preview),
    logs,
  }
}
