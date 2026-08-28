import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { JobRunRecord, PipelineRunRecord } from '@/types/history'
import type { PipelineStage } from '@/types/studio'

import { usePipelineEditorStore } from './pipelineEditor'

// Nothing here saves: the run view only reads the stages already in the store.
vi.mock('@/lib/storage/db', () => ({}))

const STAGES: PipelineStage[] = [
  { id: 'stage-a', jobId: 'job-ingest', position: { x: 0, y: 0 } },
  { id: 'stage-b', jobId: 'job-clean', position: { x: 420, y: 0 } },
]

function jobRun(overrides: Partial<JobRunRecord> = {}): JobRunRecord {
  return {
    id: 'jr-1',
    pipelineRunId: 'run-1',
    jobId: 'job-ingest',
    name: 'ingest',
    stageIndex: 0,
    status: 'success',
    startedAt: '2026-08-20T10:00:00+00:00',
    finishedAt: '2026-08-20T10:00:02+00:00',
    durationMs: 2000,
    error: null,
    rowsRead: 120,
    rowsWritten: 118,
    lineage: null,
    configHash: null,
    steps: [],
    ...overrides,
  }
}

function pipelineRun(jobs: JobRunRecord[]): PipelineRunRecord {
  return {
    id: 'run-1',
    kind: 'pipeline',
    workflowId: 'wf-1',
    pipelineId: 'pl-1',
    jobId: null,
    name: 'nightly',
    status: 'failed',
    startedAt: '2026-08-20T10:00:00+00:00',
    finishedAt: '2026-08-20T10:00:06+00:00',
    durationMs: 6000,
    error: 'Stage 2 failed',
    runAs: null,
    launched: 'manual',
    jobs,
  }
}

describe('a past run on the pipeline canvas', () => {
  beforeEach(() => {
    usePipelineEditorStore.setState({
      stages: STAGES,
      running: false,
      stageStatus: {},
      stageResults: {},
      runView: null,
    })
  })

  it('paints each stage and records how to open its job at that execution', () => {
    usePipelineEditorStore.getState().showRunView(
      pipelineRun([
        jobRun(),
        jobRun({
          id: 'jr-2',
          jobId: 'job-clean',
          name: 'clean',
          stageIndex: 1,
          status: 'failed',
          error: 'Stage 2 failed',
          rowsWritten: 0,
        }),
      ]),
      { pinned: true },
    )

    const state = usePipelineEditorStore.getState()
    expect(state.stageStatus).toEqual({ 'stage-a': 'success', 'stage-b': 'error' })
    expect(state.stageResults['stage-b']).toMatchObject({
      status: 'error',
      error: 'Stage 2 failed',
    })
    expect(state.runView).toMatchObject({
      runId: 'run-1',
      runName: 'nightly',
      status: 'failed',
      jobRunIds: { 'stage-a': 'jr-1', 'stage-b': 'jr-2' },
      unmatchedJobs: 0,
      pinned: true,
    })
  })

  it('counts an execution whose stage is no longer on the canvas', () => {
    usePipelineEditorStore
      .getState()
      .showRunView(pipelineRun([jobRun({ jobId: 'job-deleted' })]))

    const state = usePipelineEditorStore.getState()
    expect(state.stageStatus).toEqual({ 'stage-a': 'pending', 'stage-b': 'pending' })
    expect(state.runView?.unmatchedJobs).toBe(1)
    expect(state.runView?.pinned).toBe(false)
  })

  it('refuses to replace the canvas of a run in flight', () => {
    usePipelineEditorStore.setState({ running: true, stageStatus: { 'stage-a': 'running' } })

    usePipelineEditorStore.getState().showRunView(pipelineRun([jobRun()]))

    const state = usePipelineEditorStore.getState()
    expect(state.runView).toBeNull()
    expect(state.stageStatus).toEqual({ 'stage-a': 'running' })
  })

  it('forgets the run when it is dismissed', () => {
    const store = usePipelineEditorStore.getState()
    store.showRunView(pipelineRun([jobRun()]))
    store.clearRunView()

    const state = usePipelineEditorStore.getState()
    expect(state.runView).toBeNull()
    expect(state.stageStatus).toEqual({})
    expect(state.stageResults).toEqual({})
  })
})
