/**
 * Persisted execution history, mirroring `server/history.py` / the
 * `PipelineRunOut` / `JobRunOut` / `StepRunOut` models `GET /runs` and
 * `GET /runs/{id}` return.
 *
 * Vocabulary follows the server: `PipelineRunRecord` is the top-level execution
 * instance — a solo Job run (`kind: 'job'`, one `JobRunRecord`) or a Studio
 * Pipeline run (`kind: 'pipeline'`, one `JobRunRecord` per stage). Unrelated to
 * the in-memory `RunResult` / `PipelineRunResult` in `types/studio.ts`, which
 * describe the CURRENT stream only — these describe a run that already ended
 * and lives in the runner's SQLite database.
 */

export type ExecutionStatus =
  | 'pending'
  | 'running'
  | 'success'
  | 'failed'
  | 'skipped'
  /** Stopped on request while it was running — not a failure, not a skip. */
  | 'cancelled'

import type { RunCharge } from '@/types/credits'

export interface StepRunRecord {
  id: string
  jobRunId: string
  /** `input` | `transformation` | `output` | `validation` | `validation_sink`. */
  scope: string
  /**
   * Position in the scope's lane. On a role-keyed step (`validation_sink`) it is
   * arrival order only — `role` is the identity there.
   */
  stepIndex: number
  type: string
  status: ExecutionStatus
  startedAt: string | null
  finishedAt: string | null
  durationMs: number | null
  errorMessage: string | null
  errorDetails: string | null
  /** `report` | `valid` | `invalid` on the quality datasets; null everywhere else. */
  role: string | null
  /**
   * What the framework reported about the step, as a JSON object: rows, path,
   * format, whether a rule passed. Parsed with `stepDetails()`.
   */
  details: string | null
}

/** One dataset a run touched, as `lineage_of()` in `server/history.py` records it. */
export interface LineageDataset {
  /** `input` | `join` on the read side; `output` | `validation:report|valid|invalid` on the write side. */
  role: string
  format: string | null
  /** Path, table, view or topic — whichever the format addresses it by. */
  address: string | null
  /** Writes only: `overwrite` | `append` | `merge`. */
  mode?: string
}

export interface RunLineage {
  inputs: LineageDataset[]
  outputs: LineageDataset[]
}

export interface JobRunRecord {
  id: string
  pipelineRunId: string
  jobId: string | null
  name: string | null
  stageIndex: number
  status: ExecutionStatus
  startedAt: string | null
  finishedAt: string | null
  durationMs: number | null
  error: string | null
  rowsRead: number | null
  rowsWritten: number | null
  /**
   * What this execution read and wrote, taken from the submitted JSON — so it is
   * there even for a run that failed before writing anything. Null on runs
   * recorded before lineage existed.
   */
  lineage: RunLineage | null
  /**
   * `sha256:<hex>` over the JSON this execution ran, or null for a run recorded
   * before the runner kept it.
   *
   * The history points at a Job, and a Job keeps being edited: this is what says
   * two runs a month apart executed different JSON, and what matches a run
   * against a file in git. The configuration itself is fetched on demand with
   * `getJobRunConfig` — it is too large to carry in a listing.
   */
  configHash: string | null
  /**
   * What this execution cost its team: one credit per successful write to a
   * cluster. Null for a local run, which is free, and for a run recorded before
   * credits existed. Populated only on `getRun`.
   */
  credits: RunCharge | null
  /** Populated only on `getRun` — a list row never carries the nested detail. */
  steps: StepRunRecord[]
}

/** The version of the JSON one execution ran, as `GET /job-runs/{id}/config` returns it. */
export interface JobRunConfig {
  jobRunId: string
  configHash: string | null
  /**
   * The JSON itself, or null when the run predates the column or the
   * configuration was over the size the runner stores — `configHash` still
   * identifies it in that case.
   */
  config: Record<string, unknown> | null
}

/**
 * One line a job execution printed, as the runner recorded it.
 *
 * Same shape as the live `RunLogLine` in `types/studio.ts`, plus `seq` — the
 * per-job-run sequence the history pages by, since a burst of lines shares one
 * timestamp and an offset would re-read or skip lines on a run still in flight.
 */
export interface RunLogRecord {
  seq: number
  timestamp: string
  /** As the runner stored it: `INFO` | `WARNING` | `ERROR` | `DEBUG` | `WARN`. */
  level: string
  /** `pipeline` (the framework) | `spark` (the JVM) | `stdout` | `runner`. */
  source: string
  message: string
  context: Record<string, unknown>
}

export interface RunLogPage {
  jobRunId: string
  lines: RunLogRecord[]
  /** How many lines the runner holds for this job execution, in total. */
  total: number
  /** Pass back as `after` to read the next page; null at the end. */
  nextAfter: number | null
}

export interface PipelineRunRecord {
  id: string
  kind: 'job' | 'pipeline'
  workflowId: string | null
  pipelineId: string | null
  jobId: string | null
  name: string | null
  status: ExecutionStatus
  startedAt: string | null
  finishedAt: string | null
  durationMs: number | null
  error: string | null
  /**
   * Who the run is attributed to. The runner authenticates a token, not a
   * person, so this is what the caller claimed — or the OS account the runner
   * itself runs under. Null on runs recorded before it was captured.
   */
  runAs: string | null
  /**
   * How it started: a person pressed Run, a timer fired, the API was called, or
   * — `external` — the framework reported a run this runner never executed, from
   * `sparquet.cli`, Airflow, Databricks or anywhere else it runs.
   */
  launched: 'manual' | 'scheduled' | 'api' | 'external' | null
  /**
   * Kept forever. The runner expires history by age; a pinned run is skipped
   * whatever its age, which is how the execution of an incident survives.
   */
  pinned: boolean
  /** Populated only on `getRun` — a list row never carries the nested detail. */
  jobs: JobRunRecord[]
}
