/**
 * Client for the optional local execution bridge (`sparquet-studio/server`).
 *
 * The runner is never required: every call fails with a `RunnerError` carrying a
 * message the UI can show verbatim, so an offline Studio degrades cleanly.
 */

import type { PipelineSpec, SparkSettings } from '@/types/pipeline'
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
  /**
   * Whether the installed framework is inside the range this Studio was built
   * against. Absent on an older runner, and true there by default: a runner
   * that does not answer the question has not answered "no".
   */
  frameworkSupported?: boolean
  /** What to do about a mismatch, written by the runner that found it. */
  frameworkMessage?: string
  /** The range itself, e.g. `sparquet>=0.12,<0.13`. */
  frameworkRequirement?: string
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

/** One field of a schema the runner read from the storage itself. */
export interface RunnerSchemaField {
  name: string
  /** Spark's own rendering of the type, such as `decimal(18,2)`. */
  type: string
  nullable: boolean
}

export interface RunnerDatasetSchema {
  format: string
  path: string
  fields: RunnerSchemaField[]
  /** When the runner opened the dataset, ISO-8601. */
  readAt: string
  /**
   * The runner rebuilt its SparkSession to honour the `spark` block sent with
   * this request. Worth saying out loud: connector jars and SQL extensions are
   * read only when a session is created, so the rebuild is what made a Delta or
   * Iceberg dataset readable at all.
   */
  sessionRestarted: boolean
}

/** The dataset to open, in the same shape a Job's `input` block has. */
export interface DatasetSchemaRequest {
  format: string
  path: string
  options?: Record<string, unknown>
  /** Session config for this read — see `RunQueryRequest.spark`. */
  spark?: SparkSettings
}

/** One dataset the SQL may name: opened by the runner and registered as a temp view. */
export interface QuerySource {
  /** The view name the SQL writes in its FROM clause. */
  alias: string
  format: string
  path: string
  options?: Record<string, unknown>
}

export interface RunQueryRequest {
  sql: string
  sources: QuerySource[]
  /** Rows the runner may return. It reads one more, to know it cut the result. */
  limit?: number
  /**
   * Chosen here rather than by the runner: the response only arrives once the
   * query is over, so cancelling it needs the id up front.
   */
  queryId?: string
  timeoutSeconds?: number
  /**
   * Session config the runner should honour for this query. Connector jars and
   * SQL extensions are read only when a SparkSession is created, so the runner
   * rebuilds its session when the live one is missing what this asks for, and
   * says so through `sessionRestarted`.
   */
  spark?: SparkSettings
  /**
   * The library file this statement came from, when it came from one.
   *
   * A saved query is a securable of its own, so the runner checks the grants on
   * the file as well as on every table the statement names. An unsaved buffer
   * sends nothing here — there is no file to have a rule about, and the tables
   * are checked either way.
   */
  savedQueryId?: string
  /**
   * The editor tab this ran from, which is what the run is filed under while the
   * buffer has no file yet.
   *
   * A run that names neither a saved query nor a tab is executed and not
   * recorded — the catalog's row sample goes through here too, and a storage
   * read is not part of anybody's query history.
   */
  tab?: string
}

export interface RunnerQueryResult {
  queryId: string
  columns: string[]
  fields: RunnerSchemaField[]
  rows: unknown[][]
  /** The result had more rows than the limit asked for. */
  truncated: boolean
  elapsedMs: number
  /** The runner rebuilt its SparkSession for this query — see `RunnerDatasetSchema`. */
  sessionRestarted: boolean
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

/**
 * One stage of a pipeline run, already in execution order.
 *
 * Exactly one of `pipeline` and `path`: the compiled JSON for a stage backed by
 * a Job, or a `.json` in the library — relative to its root — that the runner
 * reads when the stage starts. It refuses both, and it refuses neither.
 */
export interface RunPipelineStageRequest {
  /** Echoed back on every stage event, so the canvas can find the box. */
  id: string
  name?: string
  pipeline?: PipelineSpec
  /** A file in the library, read at run time. The file is the source, not a copy. */
  path?: string
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
    frameworkSupported: asBoolean(payload.framework_supported, true),
    frameworkMessage: optionalString(payload.framework_message),
    frameworkRequirement: optionalString(payload.framework_requirement),
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
 * The schema a dataset really has, read by the runner.
 *
 * The catalog derives a schema from the canvas, which says what a Job intends to
 * write. This says what is actually there, so the two can be compared. Reads no
 * rows: the runner builds a reader and takes `df.schema`.
 */
export async function fetchDatasetSchema(
  baseUrl: string = DEFAULT_RUNNER_URL,
  body: DatasetSchemaRequest,
  signal?: AbortSignal,
  token?: string,
): Promise<RunnerDatasetSchema> {
  let payload: Record<string, unknown>
  try {
    payload = expectRecord(
      await requestJson(baseUrl, '/dataset/schema', jsonPost(body, token), signal),
    )
  } catch (error) {
    // A runner started before this route existed answers 404 for the ROUTE, and
    // FastAPI's bare "Not Found" reads as if the dataset were missing — the
    // opposite of what happened. Say which of the two it is.
    if (isRunnerError(error) && error.status === 404) {
      throw new RunnerError(
        'This runner has no /dataset/schema route: it was started from a version that ' +
          'predates it. Restart the runner and try again.',
        'http',
        404,
        error,
      )
    }
    throw error
  }
  return {
    format: asString(payload.format, body.format),
    path: asString(payload.path, body.path),
    fields: asArray(payload.fields)
      .filter(isRecord)
      .map((field) => ({
        name: asString(field.name),
        type: asString(field.type),
        nullable: asBoolean(field.nullable, true),
      }))
      .filter((field) => field.name.length > 0),
    readAt: asString(payload.read_at),
    sessionRestarted: asBoolean(payload.session_restarted),
  }
}

/**
 * Runs one read-only SQL statement on the runner.
 *
 * Each source is opened by the framework's own `ReaderFactory` and registered as
 * a temp view, so the query reads a Delta or Iceberg table exactly as a Job
 * would, and the SQL only has to name the alias.
 */
export async function runQuery(
  baseUrl: string = DEFAULT_RUNNER_URL,
  body: RunQueryRequest,
  signal?: AbortSignal,
  token?: string,
): Promise<RunnerQueryResult> {
  const payloadBody = {
    sql: body.sql,
    sources: body.sources.map((source) => ({
      alias: source.alias,
      format: source.format,
      path: source.path,
      options: source.options ?? {},
    })),
    limit: body.limit,
    query_id: body.queryId,
    timeout_seconds: body.timeoutSeconds,
    spark: body.spark,
    saved_query_id: body.savedQueryId,
    tab: body.tab,
  }

  let payload: Record<string, unknown>
  try {
    payload = expectRecord(await requestJson(baseUrl, '/query', jsonPost(payloadBody, token), signal))
  } catch (error) {
    // FastAPI answers a bare "Not Found" for an unknown ROUTE, which reads as if
    // the data were missing. A runner older than this route is the real cause.
    if (isRunnerError(error) && error.status === 404) {
      throw new RunnerError(
        'This runner has no /query route: it was started from a version that predates ' +
          'the SQL editor. Restart the runner and try again.',
        'http',
        404,
        error,
      )
    }
    throw error
  }

  return {
    queryId: asString(payload.query_id, body.queryId ?? ''),
    columns: asStringArray(payload.columns),
    fields: asArray(payload.fields)
      .filter(isRecord)
      .map((field) => ({
        name: asString(field.name),
        type: asString(field.type),
        nullable: asBoolean(field.nullable, true),
      })),
    rows: asArray(payload.rows).map((row) => asArray(row)),
    truncated: asBoolean(payload.truncated),
    elapsedMs: asNumber(payload.elapsed_ms),
    sessionRestarted: asBoolean(payload.session_restarted),
  }
}

/** What the runner's parser made of a statement it was asked to check. */
export interface QueryValidation {
  /** False when no SparkSession was up to ask; the editor then marks nothing. */
  checked: boolean
  ok: boolean
  message: string
  /** 1-based line, as the parser and the editor both count them. */
  line: number | null
  /** 0-based column, which is how Spark reports `pos`. */
  column: number | null
  /** Why nothing was checked, when nothing was. */
  reason: string
}

/**
 * Asks the runner to parse a statement without running it.
 *
 * The syntax of Spark SQL is only known for certain by the parser that will run
 * the query, so the marker in the editor comes from there rather than from a
 * second implementation in the browser that would disagree with it.
 *
 * Never throws. A runner that is off, older than this route, or busy is a
 * reason to mark nothing — not a reason to interrupt the typing with an error.
 */
export async function validateQuery(
  baseUrl: string = DEFAULT_RUNNER_URL,
  sql: string,
  signal?: AbortSignal,
  token?: string,
): Promise<QueryValidation> {
  try {
    const payload = expectRecord(
      await requestJson(baseUrl, '/query/validate', jsonPost({ sql }, token), signal),
    )
    return {
      checked: asBoolean(payload.checked),
      ok: asBoolean(payload.ok, true),
      message: asString(payload.message),
      line: typeof payload.line === 'number' ? payload.line : null,
      column: typeof payload.column === 'number' ? payload.column : null,
      reason: asString(payload.reason),
    }
  } catch (error) {
    return {
      checked: false,
      ok: true,
      message: '',
      line: null,
      column: null,
      reason: error instanceof Error ? error.message : String(error),
    }
  }
}

/** One past execution of a query, as the runner recorded it. */
export interface QueryRun {
  id: string
  /** ISO-8601, from the runner's clock — one clock for everybody reading it. */
  at: string
  /** Exactly what was sent, which is the selection when a selection was run. */
  sql: string
  /** The row cap it ran under, since that changes what came back. */
  limit: number
  elapsedMs: number
  rows: number
  /** The runner cut the result short at the cap. */
  truncated: boolean
  /** The first line of the failure, or null when the run succeeded. */
  error: string | null
  /** Who ran it. Empty on a runner with no login. */
  runAs: string
}

/** Which history is being read: a saved query's, or a scratch tab's. */
export interface HistoryScope {
  savedQueryId?: string
  tab?: string
}

function historyQuery(scope: HistoryScope): string {
  const params = new URLSearchParams()
  if (scope.savedQueryId) params.set('saved_query_id', scope.savedQueryId)
  else if (scope.tab) params.set('tab', scope.tab)
  return params.toString()
}

function toQueryRun(value: unknown): QueryRun | null {
  if (!isRecord(value)) return null
  const id = asString(value.id)
  if (!id) return null
  return {
    id,
    at: asString(value.at),
    sql: asString(value.sql),
    limit: asNumber(value.limit),
    elapsedMs: asNumber(value.elapsed_ms),
    rows: asNumber(value.rows),
    truncated: asBoolean(value.truncated),
    error: typeof value.error === 'string' && value.error.length > 0 ? value.error : null,
    runAs: asString(value.run_as),
  }
}

/**
 * What a query has been run as, newest first.
 *
 * Kept by the runner rather than by the browser: a saved query is a file two
 * people can open, and what it has been run as is part of it. A buffer nobody
 * has saved has no file to share, so the runner keys its runs by the tab and
 * the person, and only that person reads them back.
 *
 * Never throws. A runner that is off or older than this route means an empty
 * history, which is what an editor with no runs shows anyway.
 */
export async function fetchQueryHistory(
  baseUrl: string = DEFAULT_RUNNER_URL,
  scope: HistoryScope,
  signal?: AbortSignal,
  token?: string,
): Promise<QueryRun[]> {
  const search = historyQuery(scope)
  if (!search) return []
  try {
    const payload = expectRecord(
      await requestJson(
        baseUrl,
        `/query/history?${search}`,
        { method: 'GET', headers: authHeaders(token) },
        signal,
      ),
    )
    return asArray(payload.runs)
      .map(toQueryRun)
      .filter((run): run is QueryRun => run !== null)
  } catch {
    return []
  }
}

/** Forgets one query's runs — everybody's, since the history is shared. */
export async function clearQueryHistory(
  baseUrl: string = DEFAULT_RUNNER_URL,
  scope: HistoryScope,
  signal?: AbortSignal,
  token?: string,
): Promise<void> {
  const search = historyQuery(scope)
  if (!search) return
  await requestJson(
    baseUrl,
    `/query/history?${search}`,
    { method: 'DELETE', headers: authHeaders(token) },
    signal,
  )
}

/**
 * Carries a scratch buffer's runs onto the file it was just saved as.
 *
 * Without this, saving the query somebody worked their way to would throw away
 * the work that got them there. Never throws: a history that did not follow is
 * not a reason to fail a save that already happened.
 */
export async function moveQueryHistory(
  baseUrl: string = DEFAULT_RUNNER_URL,
  tab: string,
  savedQueryId: string,
  token?: string,
): Promise<void> {
  if (!tab || !savedQueryId) return
  try {
    await requestJson(
      baseUrl,
      '/query/history/move',
      jsonPost({ tab, saved_query_id: savedQueryId }, token),
    )
  } catch {
    // The runs stay under the tab, where they still read back for this person.
  }
}

/**
 * Stops a query that is still running.
 *
 * Aborting the request only drops this end of the socket; Spark keeps computing
 * until someone cancels the job group. Never throws: a query that already
 * finished has nothing left to interrupt.
 */
export async function cancelQuery(
  baseUrl: string = DEFAULT_RUNNER_URL,
  queryId: string,
  token?: string,
): Promise<boolean> {
  try {
    await requestJson(baseUrl, `/query/${encodeURIComponent(queryId)}/cancel`, jsonPost({}, token))
    return true
  } catch {
    return false
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

export interface SseFrame {
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
 * Shared by the job, pipeline and assistant streams: they differ only in the
 * path they open and the events they understand, never in the transport or its
 * failures.
 */
export async function postEventStream(
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
          // Exactly one of the two: the compiled JSON for a stage backed by a
          // Job, or the library path for one backed by a file — which the runner
          // reads when the stage starts, so the file stays the source.
          ...(stage.path ? { path: stage.path } : { pipeline: stage.pipeline }),
          params: stage.params,
          // A file-backed stage has no Job, and an empty id in the history would
          // read as one that was deleted.
          job_id: stage.jobId || undefined,
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
