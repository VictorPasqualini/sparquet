import { describe, expect, it } from 'vitest'

import { matchStageRuns, stageRunStatuses, type StageRef } from '@/lib/runner/stageRuns'
import type { ExecutionStatus, JobRunRecord } from '@/types/history'

const STAGES: StageRef[] = [
  { id: 'stage-a', jobId: 'job-ingest' },
  { id: 'stage-b', jobId: 'job-clean' },
]

function jobRun(overrides: Partial<JobRunRecord> = {}): JobRunRecord {
  return {
    id: 'jr-1',
    pipelineRunId: 'run-1',
    jobId: 'job-ingest',
    name: 'ingest',
    stageIndex: 0,
    status: 'success' as ExecutionStatus,
    startedAt: '2026-08-20T10:00:00Z',
    finishedAt: '2026-08-20T10:00:02Z',
    durationMs: 2000,
    error: null,
    rowsRead: 120,
    rowsWritten: 118,
    lineage: null,
    configHash: null,
    credits: null,
    steps: [],
    ...overrides,
  }
}

describe('matchStageRuns', () => {
  it('pairs each execution with the stage that points at the same job', () => {
    const { byStage, unmatched } = matchStageRuns(STAGES, [
      jobRun({ id: 'jr-2', jobId: 'job-clean', stageIndex: 1 }),
      jobRun({ id: 'jr-1', jobId: 'job-ingest', stageIndex: 0 }),
    ])

    expect(byStage['stage-a'].id).toBe('jr-1')
    expect(byStage['stage-b'].id).toBe('jr-2')
    expect(unmatched).toEqual([])
  })

  it('gives two stages on the same job one execution each, in execution order', () => {
    const stages: StageRef[] = [
      { id: 'first', jobId: 'job-ingest' },
      { id: 'second', jobId: 'job-ingest' },
    ]
    const { byStage, unmatched } = matchStageRuns(stages, [
      jobRun({ id: 'jr-late', stageIndex: 1 }),
      jobRun({ id: 'jr-early', stageIndex: 0 }),
    ])

    expect(byStage.first.id).toBe('jr-early')
    expect(byStage.second.id).toBe('jr-late')
    expect(unmatched).toEqual([])
  })

  it('reports an execution whose stage is no longer on the canvas', () => {
    const { byStage, unmatched } = matchStageRuns(STAGES, [
      jobRun({ id: 'jr-gone', jobId: 'job-deleted', stageIndex: 0 }),
    ])

    expect(byStage).toEqual({})
    expect(unmatched.map((job) => job.id)).toEqual(['jr-gone'])
  })

  it('never guesses a stage for a run recorded without a job id', () => {
    const { byStage, unmatched } = matchStageRuns(STAGES, [
      jobRun({ id: 'jr-legacy', jobId: null, stageIndex: 0 }),
    ])

    expect(byStage).toEqual({})
    expect(unmatched.map((job) => job.id)).toEqual(['jr-legacy'])
  })
})

describe('stageRunStatuses', () => {
  it('paints the outcome of each stage and leaves the unreached ones pending', () => {
    const view = stageRunStatuses(STAGES, [
      jobRun({ id: 'jr-1', jobId: 'job-ingest', stageIndex: 0, status: 'failed' }),
    ])

    expect(view.status).toEqual({ 'stage-a': 'error', 'stage-b': 'pending' })
    expect(view.jobRunIds).toEqual({ 'stage-a': 'jr-1' })
    expect(view.results['stage-b']).toBeUndefined()
  })

  it('carries the rows, the duration and the error onto the stage result', () => {
    const view = stageRunStatuses(STAGES, [
      jobRun({
        status: 'failed',
        error: 'Python worker exited unexpectedly (crashed)',
      }),
    ])

    expect(view.results['stage-a']).toEqual({
      index: 0,
      id: 'stage-a',
      status: 'error',
      name: 'ingest',
      rowsRead: 120,
      rowsWritten: 118,
      durationMs: 2000,
      error: 'Python worker exited unexpectedly (crashed)',
    })
  })

  it('keeps a running stage out of the results: it has no numbers yet', () => {
    const view = stageRunStatuses(STAGES, [jobRun({ status: 'running' })])

    expect(view.status['stage-a']).toBe('running')
    expect(view.results['stage-a']).toBeUndefined()
  })

  it('keeps a skipped stage, which is an outcome and not a gap', () => {
    const view = stageRunStatuses(STAGES, [jobRun({ status: 'skipped' })])

    expect(view.status['stage-a']).toBe('skipped')
    expect(view.results['stage-a'].status).toBe('skipped')
  })
})
