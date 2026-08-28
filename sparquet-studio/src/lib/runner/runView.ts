/**
 * Which persisted execution the Job canvas shows, and which of its job runs
 * describes the Job on screen.
 *
 * Split out of `RunViewBanner` because the ordering rule here is the whole
 * feature and needs a test: the request a Pipeline stage makes must survive an
 * attempt that never finishes. React runs every effect twice in development and
 * a token or URL change restarts the loader, so a resolver that cleared the
 * request up front lost the stage's run on the discarded pass and fell back to
 * the Job's latest one. Hence: this reads the request and never consumes it —
 * the caller clears it only after the load it belongs to has painted.
 */

import type { JobRunRecord, PipelineRunRecord } from '@/types/history'

import { getRun as defaultGetRun, listRuns as defaultListRuns } from './history'

/** What a Pipeline stage passes when it opens a Job at one specific execution. */
export interface RunViewRequest {
  runId?: string
  jobRunId?: string
}

export interface ResolvedRunView {
  run: PipelineRunRecord
  jobRun: JobRunRecord
  /** The run was asked for by id, so nothing replaces it until the user leaves it. */
  pinned: boolean
}

export interface ResolveRunViewOptions {
  jobId: string
  /** Read, never cleared — see the module note. */
  request?: RunViewRequest | null
  runnerUrl: string
  runnerToken?: string
  signal?: AbortSignal
  /**
   * Fall back to the Job's most recent execution when the request names none.
   *
   * Off is what opening a Job does: the canvas shows the graph as it stands, and
   * the Runs tab is where an execution is picked. On is for the moment a live run
   * ends, where the persisted rows say more than the stream did.
   */
  allowLatest?: boolean
  /** Injectable for tests; the runner client otherwise. */
  listRuns?: typeof defaultListRuns
  getRun?: typeof defaultGetRun
}

/**
 * The job run of `run` that belongs to `jobId`.
 *
 * A Pipeline execution holds one per stage, so a stage names the one it ran;
 * otherwise the Job's own id matches. `jobId` is null on runs recorded before
 * Studio started sending one, where a solo run's single job run is the only
 * candidate there is.
 */
export function pickJobRun(
  run: PipelineRunRecord,
  jobId: string,
  request?: RunViewRequest | null,
): JobRunRecord | undefined {
  const named =
    request?.jobRunId !== undefined
      ? run.jobs.find((entry) => entry.id === request.jobRunId)
      : run.jobs.find((entry) => entry.jobId === jobId)
  return named ?? (run.jobs.length === 1 ? run.jobs[0] : undefined)
}

/**
 * The execution to paint: the one asked for, or — only when `allowLatest` — the
 * Job's most recent.
 *
 * Returns null when there is nothing to show — no run asked for, no run yet, a run
 * the runner no longer has, or an execution that never touched this Job. Errors (no
 * runner, a rejected token) propagate: the canvas stays silent about those, the Run
 * panel reports them.
 */
export async function resolveRunView(
  options: ResolveRunViewOptions,
): Promise<ResolvedRunView | null> {
  const { jobId, request = null, runnerUrl, runnerToken, signal, allowLatest = true } = options
  const listRuns = options.listRuns ?? defaultListRuns
  const getRun = options.getRun ?? defaultGetRun

  let runId = request?.runId
  if (runId === undefined) {
    if (!allowLatest) return null
    const recent = await listRuns(runnerUrl, { jobId, limit: 1 }, signal, runnerToken)
    runId = recent[0]?.id
  }
  if (runId === undefined) return null

  const run = await getRun(runnerUrl, runId, signal, runnerToken)
  if (!run) return null

  const jobRun = pickJobRun(run, jobId, request)
  if (!jobRun) return null

  return { run, jobRun, pinned: request?.runId !== undefined }
}
