/**
 * Client for the optional local execution bridge (`sparquet-studio/server`).
 *
 * The runner is never required: every call fails with a `RunnerError` carrying a
 * message the UI can show verbatim, so an offline Studio degrades cleanly.
 */

import type { PipelineSpec } from '@/types/pipeline'
import type {
  PipelineRunResult,
  PipelineStageResult,
  RunLogLine,
  RunResult,
  RunStatus,
  StepStatus,
} from '@/types/studio'

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

/**
 * The logged-in session, when the runner has users. Sent alongside the token, not
 * instead of it: the token says a request may reach the runner at all, the session
 * says who is making it.
 */
export const RUNNER_SESSION_HEADER = 'x-sparquet-session'

/**
 * Held here rather than passed through every call.
 *
 * A session belongs to the browser tab, the way a cookie would — every request
 * this app makes is made by the same person, and threading it through each
 * signature would only create places to forget it. `src/store/auth.ts` owns the
 * value; this module owns attaching it.
 */
let sessionToken = ''

export function setRunnerSession(token: string | null | undefined): void {
  sessionToken = token ?? ''
}

export function runnerSession(): string {
  return sessionToken
}

/** The headers that authenticate a runner call: the shared token, and the session. */
export function authHeaders(token?: string): Record<string, string> {
  const headers: Record<string, string> = {}
  if (token) headers[RUNNER_TOKEN_HEADER] = token
  if (sessionToken) headers[RUNNER_SESSION_HEADER] = sessionToken
  return headers
}

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

export interface RunJobRequest {
  pipeline: PipelineSpec
  params?: Record<string, RunParamValue>
  /** Preview rows requested from `PipelineResult.output_df`. Server default: 50. */
  limit?: number
  /** Parse the config and return without touching Spark. */
  dryRun?: boolean
  /** Studio ids, sent only so the persisted execution history links back to them. */
  workflowId?: string
  jobId?: string
  jobName?: string
  /** Who to record the run against; the runner uses its own OS account if absent. */
  runAs?: string
  /** How it started. Studio always presses the button, so: `manual`. */
  launched?: RunLaunch
}

/** How a run was started, as the history records it. */
export type RunLaunch = 'manual' | 'scheduled' | 'api'

/** One stage of a pipeline run, already in execution order. */
export interface RunPipelineStageRequest {
  /** Echoed back on every stage event, so the canvas can find the box. */
  id: string
  name?: string
  pipeline: PipelineSpec
  params?: Record<string, RunParamValue>
  /** Studio job id this stage runs, for the persisted execution history. */
  jobId?: string
}

export interface RunPipelineRequest {
  stages: RunPipelineStageRequest[]
  /** Preview rows requested from the LAST stage. Server default: 50. */
  limit?: number
  /** Stop at the first failing stage. Server default: true. */
  stopOnError?: boolean
  /** Studio ids, sent only so the persisted execution history links back to them. */
  workflowId?: string
  pipelineId?: string
  name?: string
  runAs?: string
  launched?: RunLaunch
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
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    ...authHeaders(token),
  }
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

export async function validateJob(
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

/**
 * Stops a run that is in flight.
 *
 * Aborting the stream only drops the client's end of the socket — the runner
 * keeps working, and Spark with it. This is what actually ends the run: it kills
 * the Spark jobs and stops a pipeline at its next stage.
 *
 * Never throws: Stop is a button, and there is nothing useful to say when the run
 * has already finished on its own (HTTP 409) or the runner has gone away. The
 * outcome that matters — the run's own `cancelled` status — arrives through the
 * stream. Returns whether the runner accepted the cancellation.
 */
export async function cancelRun(
  baseUrl: string = DEFAULT_RUNNER_URL,
  runId: string,
  token?: string,
): Promise<boolean> {
  try {
    await requestJson(baseUrl, `/runs/${encodeURIComponent(runId)}/cancel`, jsonPost({}, token))
    return true
  } catch {
    return false
  }
}

export async function runJob(
  baseUrl: string = DEFAULT_RUNNER_URL,
  body: RunJobRequest,
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
          workflow_id: body.workflowId,
          job_id: body.jobId,
          job_name: body.jobName,
          run_as: body.runAs,
          launched: body.launched,
        },
        token,
      ),
      signal,
    ),
  )
  return toRunResult(payload)
}

/* ---------------------------------------------------------------- streaming */

/**
 * One step marker from the runner, already interpreted.
 *
 * A step is addressed in ONE of two ways, never both:
 *
 * - by `index` inside its `scope`'s lane — `input`, `transformation`, `output`,
 *   `validation`: the runner counts these in the order the compiler emitted them;
 * - by `role` — the datasets the `validations` block writes (`report`, `valid`,
 *   `invalid`). Those have no order to count in: on the canvas they are standalone
 *   declarations with no incoming link, so an index would point at nothing.
 *
 * `ts` is the marker's own timestamp, taken from the log line. Two markers bracket
 * every step, so the pair is all a caller needs to time it — see `createStepTimer`.
 */
export interface RunStepEvent {
  status: StepStatus
  /** `input` | `transformation` | `output` | `validation` | `validation_sink`. */
  scope: string
  /** 0-based position inside the scope's lane; absent on a role-keyed step. */
  index?: number
  /** Quality dataset this marker belongs to; absent on an index-keyed step. */
  role?: string
  /** Transformation / rule type, when the runner named one. */
  type?: string
  /** Epoch ms of the log line that carried the marker. */
  ts: number
}

/**
 * The `start` event: the runner accepted the run and opened the history rows.
 *
 * `runId` is what `cancelRun` addresses — it arrives before the first log line
 * precisely so Stop can reach a run that has barely begun.
 */
export interface RunStreamStart {
  pipelineName?: string
  runId?: string
  jobRunId?: string
}

export interface JobStreamHandlers {
  /** The runner accepted the run and started the worker thread. */
  onStart?: (start: RunStreamStart) => void
  onLog?: (line: RunLogLine) => void
  /** Progress of one step of the pipeline. */
  onStep?: (step: RunStepEvent) => void
  onResult: (result: RunResult) => void
  /** The runner emitted an `error` event (the stream still ends normally). */
  onError?: (message: string) => void
}

/**
 * Step markers are ordinary pipeline logs flagged with `context.step`; the
 * message decides the status. Kept verbatim — the runner emits these strings.
 */
const STEP_STATUS_BY_MESSAGE: Record<string, StepStatus> = {
  'Transformation started': 'running',
  'Transformation applied': 'success',
  'Transformation skipped': 'skipped',
  // The read and the writes are the steps that really touch data, so they carry
  // their own markers (scope 'input' / 'output') alongside the transformations.
  'Input started': 'running',
  'Input read': 'success',
  'Output started': 'running',
  'Output written': 'success',
  // Validation rules are real actions, not lazy plan-building, so these two
  // genuinely bracket work being done.
  'Validation started': 'running',
  'Validation finished': 'success',
  // The datasets the `validations` block writes: the quality report and the
  // valid/invalid quarantine. Keyed by `context.role`, not by an index.
  'Validation output started': 'running',
  'Validation output written': 'success',
}

/**
 * Times steps from the marker pair the runner already emits, so no framework
 * field has to carry a duration.
 *
 * What the number means: wall-clock between a step's `started` and `finished` log
 * lines. Spark is lazy, so a *transformation* only builds a plan and reads ~0 ms —
 * correct, not a bug. Real time lands on the read, on every validation rule (each
 * one is a Spark action) and on the writes. The per-step numbers therefore do NOT
 * add up to the run's duration, and must never be presented as if they did.
 */
export function createStepTimer(): {
  start: (key: string, ts: number) => void
  finish: (key: string, ts: number) => number | undefined
  reset: () => void
} {
  const startedAt = new Map<string, number>()
  return {
    start: (key, ts) => {
      startedAt.set(key, ts)
    },
    finish: (key, ts) => {
      const started = startedAt.get(key)
      if (started === undefined) return undefined
      startedAt.delete(key)
      // A clock that ticks backwards between two lines (or a coarse timestamp)
      // must not surface as a negative duration.
      return Math.max(0, ts - started)
    },
    reset: () => startedAt.clear(),
  }
}

interface SseFrame {
  event: string
  data: string
}

/**
 * Splits one `\n\n`-delimited SSE frame into its event name and payload.
 * Comment lines (`:`) and unknown fields are ignored; multiple `data:` lines
 * are joined with newlines, as the spec requires.
 */
function parseSseFrame(chunk: string): SseFrame | null {
  let event = 'message'
  const data: string[] = []

  for (const raw of chunk.split('\n')) {
    const line = raw.endsWith('\r') ? raw.slice(0, -1) : raw
    if (!line || line.startsWith(':')) continue
    const separator = line.indexOf(':')
    const field = separator === -1 ? line : line.slice(0, separator)
    const value = separator === -1 ? '' : line.slice(separator + 1).replace(/^ /, '')
    if (field === 'event') event = value
    else if (field === 'data') data.push(value)
  }

  return data.length > 0 ? { event, data: data.join('\n') } : null
}

/** Emits `onStep` when the log line is one of the runner's step markers. */
function dispatchStep(line: RunLogLine, handlers: JobStreamHandlers): void {
  const context = line.context
  if (!handlers.onStep || !context || context.step !== true) return
  const status = STEP_STATUS_BY_MESSAGE[line.message]
  if (!status) return

  // `scope` tells the panel which lane the marker belongs to: the main
  // transformation chain (default), the source node, the outputs, the rules, or
  // the datasets the validations block writes.
  const scope = optionalString(context.scope) ?? 'transformation'
  const role = optionalString(context.role)
  const rawIndex = context.index
  const index =
    typeof rawIndex === 'number' && Number.isFinite(rawIndex) ? rawIndex : undefined
  // A marker addressed by neither an index nor a role points at no node at all.
  if (index === undefined && role === undefined) return

  handlers.onStep({
    status,
    scope,
    ...(index !== undefined ? { index } : {}),
    ...(role !== undefined ? { role } : {}),
    ...(optionalString(context.type) ? { type: optionalString(context.type) } : {}),
    ts: line.ts,
  })
}

function dispatchSseFrame(frame: SseFrame, handlers: JobStreamHandlers): void {
  const payload = safeParse(frame.data)

  switch (frame.event) {
    case 'start':
      handlers.onStart?.(
        isRecord(payload)
          ? {
              pipelineName: optionalString(payload.pipeline_name),
              runId: optionalString(payload.pipeline_run_id),
              jobRunId: optionalString(payload.job_run_id),
            }
          : {},
      )
      return
    case 'log': {
      const line = toLogLine(payload)
      if (!line) return
      handlers.onLog?.(line)
      dispatchStep(line, handlers)
      return
    }
    case 'result':
      // The `result` payload is byte-for-byte what POST /run returns.
      handlers.onResult(toRunResult(expectRecord(payload)))
      return
    case 'error':
      handlers.onError?.(
        (isRecord(payload) ? optionalString(payload.error) : undefined) ??
          'The local runner failed to execute the job.',
      )
      return
    default:
      // Forward compatibility: unknown events are simply skipped.
      return
  }
}

/**
 * POSTs to an SSE endpoint and hands every frame to `onFrame`. Resolves when the
 * stream ends; the outcome arrives through the frames, never as a return value.
 *
 * Shared by the job and the pipeline streams: they differ only in the path they
 * open and the events they understand, never in the transport or its failures.
 */
async function postEventStream(
  baseUrl: string,
  path: string,
  body: unknown,
  onFrame: (frame: SseFrame) => void,
  signal?: AbortSignal,
  token?: string,
): Promise<void> {
  const init = jsonPost(body, token)
  const headers = { ...(init.headers as Record<string, string>), accept: 'text/event-stream' }

  let response: Response
  try {
    response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, { ...init, headers, signal })
  } catch (error) {
    // An aborted request is the caller's own doing, not a missing runner.
    if (error instanceof DOMException && error.name === 'AbortError') throw error
    throw new RunnerError(RUNNER_UNREACHABLE_MESSAGE, 'unreachable', undefined, error)
  }

  if (!response.ok) {
    throw new RunnerError(await readErrorMessage(response), 'http', response.status)
  }

  if (!response.body) {
    throw new RunnerError(
      'The local runner returned a malformed response.',
      'malformed',
      response.status,
    )
  }

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  const drain = (final: boolean) => {
    // SSE frames are separated by a blank line; anything after the last one is a
    // partial frame and stays in the buffer until more bytes arrive.
    let boundary = buffer.indexOf('\n\n')
    while (boundary !== -1) {
      const chunk = buffer.slice(0, boundary)
      buffer = buffer.slice(boundary + 2)
      const frame = parseSseFrame(chunk)
      if (frame) onFrame(frame)
      boundary = buffer.indexOf('\n\n')
    }
    if (final && buffer.trim()) {
      const frame = parseSseFrame(buffer)
      buffer = ''
      if (frame) onFrame(frame)
    }
  }

  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      drain(false)
    }
    buffer += decoder.decode()
    drain(true)
  } catch (error) {
    if (isRunnerError(error)) throw error
    if (error instanceof DOMException && error.name === 'AbortError') throw error
    throw new RunnerError(
      'The local runner closed the stream unexpectedly.',
      'malformed',
      response.status,
      error,
    )
  } finally {
    // Releasing lets an aborted fetch tear the connection down immediately.
    reader.releaseLock()
  }
}

/**
 * Streams a run over Server-Sent Events, so the UI can paint per-step status and
 * logs while Spark works. Resolves when the stream ends; the outcome arrives
 * through `onResult` (or `onError`), never as a return value.
 *
 * Fails with the same `RunnerError` kinds as `runJob` — notably HTTP 409
 * when another run already holds the runner's lock.
 */
export async function runJobStream(
  baseUrl: string = DEFAULT_RUNNER_URL,
  body: RunJobRequest,
  handlers: JobStreamHandlers,
  signal?: AbortSignal,
  token?: string,
): Promise<void> {
  await postEventStream(
    baseUrl,
    '/run/stream',
    {
      pipeline: body.pipeline,
      params: body.params,
      limit: body.limit,
      dry_run: body.dryRun,
      workflow_id: body.workflowId,
      job_id: body.jobId,
      job_name: body.jobName,
      run_as: body.runAs,
      launched: body.launched,
    },
    (frame) => dispatchSseFrame(frame, handlers),
    signal,
    token,
  )
}

/* ----------------------------------------------------------- pipeline streaming */

export interface PipelineStreamStart {
  /** How many stages the runner was handed. */
  total: number
  /** The execution `cancelRun` addresses, known before the first stage starts. */
  runId?: string
}

export interface PipelineStreamHandlers {
  /** The runner accepted the pipeline and knows how many stages it holds. */
  onStart?: (start: PipelineStreamStart) => void
  /** A stage began. `index` is 0-based in the sequence that was submitted. */
  onStageStart?: (stage: { index: number; id: string; name?: string }) => void
  /** A log line, carrying `stageId` when the runner attributed it to a stage. */
  onLog?: (line: RunLogLine) => void
  onStageResult?: (result: PipelineStageResult) => void
  onResult: (result: PipelineRunResult) => void
  /** The runner emitted a fatal `error` event; the stream ends after it. */
  onError?: (message: string) => void
}

function dispatchPipelineFrame(frame: SseFrame, handlers: PipelineStreamHandlers): void {
  const payload = safeParse(frame.data)

  switch (frame.event) {
    case 'start':
      handlers.onStart?.(
        isRecord(payload)
          ? { total: asNumber(payload.total), runId: optionalString(payload.pipeline_run_id) }
          : { total: 0 },
      )
      return
    case 'stage_start': {
      if (!isRecord(payload)) return
      const id = optionalString(payload.id)
      if (!id) return
      handlers.onStageStart?.({
        index: asNumber(payload.index),
        id,
        name: optionalString(payload.name),
      })
      return
    }
    case 'log': {
      const line = toLogLine(payload)
      if (line) handlers.onLog?.(line)
      return
    }
    case 'stage_result': {
      const stage = toPipelineStageResult(payload)
      if (stage) handlers.onStageResult?.(stage)
      return
    }
    case 'stage_skipped':
    case 'stage_cancelled': {
      if (!isRecord(payload)) return
      const id = optionalString(payload.id)
      if (!id) return
      handlers.onStageResult?.({
        index: asNumber(payload.index),
        id,
        ...(optionalString(payload.name) ? { name: optionalString(payload.name) } : {}),
        status: frame.event === 'stage_cancelled' ? 'cancelled' : 'skipped',
      })
      return
    }
    case 'result':
      handlers.onResult(toPipelineRunResult(expectRecord(payload)))
      return
    case 'error':
      handlers.onError?.(
        (isRecord(payload) ? optionalString(payload.error) : undefined) ??
          'The local runner failed to execute the pipeline.',
      )
      return
    default:
      // Forward compatibility: unknown events are simply skipped.
      return
  }
}

/**
 * Runs several pipelines in sequence on the runner, streaming per-stage progress.
 *
 * `body.stages` is already in execution order — the runner does not reorder and
 * does not know about links; ordering is the Studio's job (`planPipelineRun`).
 */
export async function runPipelineStream(
  baseUrl: string = DEFAULT_RUNNER_URL,
  body: RunPipelineRequest,
  handlers: PipelineStreamHandlers,
  signal?: AbortSignal,
  token?: string,
): Promise<void> {
  try {
    await postEventStream(
      baseUrl,
      '/run/flow/stream',
      {
        stages: body.stages.map((stage) => ({
          id: stage.id,
          name: stage.name,
          pipeline: stage.pipeline,
          params: stage.params,
          job_id: stage.jobId,
        })),
        limit: body.limit,
        stop_on_error: body.stopOnError,
        workflow_id: body.workflowId,
        pipeline_id: body.pipelineId,
        name: body.name,
        run_as: body.runAs,
        launched: body.launched,
      },
      (frame) => dispatchPipelineFrame(frame, handlers),
      signal,
      token,
    )
  } catch (error) {
    // A runner that predates pipeline runs answers 404 with FastAPI's bare "Not Found",
    // which reads like a broken URL. Name the actual problem instead.
    if (isRunnerError(error) && error.status === 404) {
      throw new RunnerError(
        'This runner cannot run pipelines yet: it has no /run/flow/stream endpoint. Update the local runner and try again.',
        'http',
        404,
        error,
      )
    }
    throw error
  }
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

const LOG_SOURCES: RunLogLine['source'][] = ['pipeline', 'stdout', 'spark']

function toLogSource(value: unknown): RunLogLine['source'] | undefined {
  return LOG_SOURCES.find((source) => source === value)
}

function toLogLine(value: unknown): RunLogLine | null {
  if (!isRecord(value)) return null
  const parsed = Date.parse(asString(value.timestamp))
  const context = isRecord(value.context) ? value.context : undefined
  // `source` only exists on the streaming endpoint; /run logs stay unlabelled.
  const source = toLogSource(value.source)
  // `stage_id` only exists on a pipeline stream, where a line belongs to one stage.
  const stageId = optionalString(value.stage_id)
  return {
    ts: Number.isNaN(parsed) ? Date.now() : parsed,
    level: LOG_LEVELS[asString(value.level).toLowerCase()] ?? 'info',
    message: asString(value.message),
    ...(source ? { source } : {}),
    ...(context ? { context } : {}),
    ...(stageId ? { stageId } : {}),
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
  // Cancelled first: the run also comes back unsuccessful, and reporting it as an
  // error would send the user looking for a bug they caused on purpose.
  if (asBoolean(payload.cancelled)) return 'cancelled'
  if (!asBoolean(payload.success)) return 'error'
  return asBoolean(payload.skipped) ? 'skipped' : 'success'
}

/** `null` when the payload carries no stage id — an event nothing can be pinned to. */
function toPipelineStageResult(value: unknown): PipelineStageResult | null {
  if (!isRecord(value)) return null
  const id = optionalString(value.id)
  if (!id) return null

  const success = asBoolean(value.success)
  const skipped = asBoolean(value.skipped)
  const cancelled = asBoolean(value.cancelled)
  return {
    index: asNumber(value.index),
    id,
    ...(optionalString(value.name) ? { name: optionalString(value.name) } : {}),
    // A skipped stage still succeeded: `stop_if_empty` is a graceful early exit.
    status: cancelled ? 'cancelled' : !success ? 'error' : skipped ? 'skipped' : 'success',
    rowsRead: asNumber(value.rows_read),
    rowsWritten: asNumber(value.rows_written),
    durationMs: asNumber(value.duration_ms),
    error: optionalString(value.error),
    validations: toValidations(value.validations),
    outputMetrics: toOutputMetrics(value.output_metrics),
  }
}

/**
 * The `result` event of a pipeline. Logs are NOT part of it — they arrived one by one
 * as `log` events — so the caller keeps the lines it streamed.
 */
function toPipelineRunResult(payload: Record<string, unknown>): PipelineRunResult {
  const stages = asArray(payload.stages)
    .map(toPipelineStageResult)
    .filter((stage): stage is PipelineStageResult => stage !== null)

  return {
    status: asBoolean(payload.cancelled)
      ? 'cancelled'
      : asBoolean(payload.success)
        ? 'success'
        : 'error',
    durationMs: asNumber(payload.duration_ms),
    stages,
    preview: toPreview(payload.preview),
    error: optionalString(payload.error),
    logs: [],
    runId: optionalString(payload.id),
  }
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
    runId: optionalString(payload.pipeline_run_id),
    jobRunId: optionalString(payload.job_run_id),
  }
}
