/**
 * Which box on a PIPELINE canvas a persisted job execution belongs to.
 *
 * Sibling of `stepNodes.ts`, one level up: that module maps a step of a job onto a
 * node of the job canvas, this one maps a whole `job_run` onto a stage of the
 * pipeline canvas. Both exist so a past execution can be painted back onto the
 * boxes that ran it, instead of living only in a list — and so a stage can hand
 * the job editor the exact execution to open.
 */

import type { JobRunRecord } from '@/types/history'
import type {
  PipelineStageOutcome,
  PipelineStageResult,
  StepStatus,
} from '@/types/studio'

import { toStepStatus } from './stepNodes'

/** What a stage needs to be matched with an execution: its box id and its Job. */
export interface StageRef {
  id: string
  jobId: string
}

export interface StageRunMatch {
  /** The job execution of each stage, by stage id. */
  byStage: Record<string, JobRunRecord>
  /** Job executions that belong to no stage on the canvas as it stands. */
  unmatched: JobRunRecord[]
}

/**
 * Pairs the job executions of one pipeline run with the stages on the canvas.
 *
 * Matched by the Job each side points at, never by position: a stage can be added,
 * removed or reordered after the run, and position would then paint the wrong box
 * while looking perfectly plausible. Two stages running the SAME job are paired in
 * execution order (`stageIndex`) against canvas order, which is as far as any
 * mapping can go once both boxes run the same file.
 *
 * A run recorded before Studio sent a `job_id` per stage carries none, so nothing
 * matches and every execution lands in `unmatched`: showing no state is honest,
 * showing a guessed one is not.
 */
export function matchStageRuns(
  stages: readonly StageRef[],
  jobs: readonly JobRunRecord[],
): StageRunMatch {
  const byStage: Record<string, JobRunRecord> = {}
  const unmatched: JobRunRecord[] = []

  // Stage ids still up for grabs, per job id, in canvas order.
  const free = new Map<string, string[]>()
  for (const stage of stages) {
    const queue = free.get(stage.jobId)
    if (queue) queue.push(stage.id)
    else free.set(stage.jobId, [stage.id])
  }

  const ordered = [...jobs].sort((a, b) => a.stageIndex - b.stageIndex)
  for (const job of ordered) {
    const stageId = job.jobId !== null ? free.get(job.jobId)?.shift() : undefined
    if (stageId === undefined) {
      unmatched.push(job)
      continue
    }
    byStage[stageId] = job
  }

  return { byStage, unmatched }
}

/** The settled outcomes; a run still in flight has no numbers to report yet. */
const OUTCOME: Partial<Record<StepStatus, PipelineStageOutcome>> = {
  success: 'success',
  error: 'error',
  skipped: 'skipped',
}

export interface StageRunStatuses {
  /** Per stage id, the status the pipeline canvas paints. */
  status: Record<string, StepStatus>
  /** Per stage id, the rows and duration that stage ended with. */
  results: Record<string, PipelineStageResult>
  /** Per stage id, the `job_run` id — what opens that Job at this execution. */
  jobRunIds: Record<string, string>
  /** Executions with no box left to paint: the pipeline was edited after the run. */
  unmatched: JobRunRecord[]
}

/**
 * A past pipeline execution projected onto the stage boxes.
 *
 * Every stage starts `pending`, which draws no badge, so a stage the run never
 * reached is not claimed to have succeeded. `results` only carries stages that
 * settled — a `pending` or `running` execution has no rows or duration to show.
 */
export function stageRunStatuses(
  stages: readonly StageRef[],
  jobs: readonly JobRunRecord[],
): StageRunStatuses {
  const { byStage, unmatched } = matchStageRuns(stages, jobs)

  const status: Record<string, StepStatus> = Object.fromEntries(
    stages.map((stage) => [stage.id, 'pending' as StepStatus]),
  )
  const results: Record<string, PipelineStageResult> = {}
  const jobRunIds: Record<string, string> = {}

  for (const [stageId, job] of Object.entries(byStage)) {
    const stepStatus = toStepStatus(job.status)
    status[stageId] = stepStatus
    jobRunIds[stageId] = job.id

    const outcome = OUTCOME[stepStatus]
    if (!outcome) continue
    results[stageId] = {
      index: job.stageIndex,
      id: stageId,
      status: outcome,
      ...(job.name !== null ? { name: job.name } : {}),
      ...(job.rowsRead !== null ? { rowsRead: job.rowsRead } : {}),
      ...(job.rowsWritten !== null ? { rowsWritten: job.rowsWritten } : {}),
      ...(job.durationMs !== null ? { durationMs: job.durationMs } : {}),
      ...(job.error !== null ? { error: job.error } : {}),
    }
  }

  return { status, results, jobRunIds, unmatched }
}
