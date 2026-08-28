import { describe, expect, it } from 'vitest'

import {
  nodeIdForStep,
  normalizeStepScope,
  runViewStatuses,
  stepDetails,
  stepLabel,
  type StepNodeLanes,
} from '@/lib/runner/stepNodes'
import type { ExecutionStatus, JobRunRecord, StepRunRecord } from '@/types/history'

const LANES: StepNodeLanes = {
  sourceId: 'src',
  transformIds: ['t0', 't1'],
  // One rule node with two `targets` reports two indices.
  validationIds: ['v0', 'v0', 'v1'],
  sinkIds: ['out0', 'out1'],
  dqSinkIds: { report: 'dq-report', invalid: 'dq-quarantine' },
}

function step(overrides: Partial<StepRunRecord> = {}): StepRunRecord {
  return {
    id: 's',
    jobRunId: 'job-1',
    scope: 'transformation',
    stepIndex: 0,
    type: 'filter',
    status: 'success',
    startedAt: '2026-08-20T10:00:00Z',
    finishedAt: '2026-08-20T10:00:01Z',
    durationMs: 1000,
    errorMessage: null,
    errorDetails: null,
    role: null,
    details: null,
    ...overrides,
  }
}

function jobRun(steps: StepRunRecord[], status: ExecutionStatus = 'success'): JobRunRecord {
  return {
    id: 'job-1',
    pipelineRunId: 'run-1',
    jobId: 'j1',
    name: 'orders',
    stageIndex: 0,
    status,
    startedAt: '2026-08-20T10:00:00Z',
    finishedAt: '2026-08-20T10:00:02Z',
    durationMs: 2000,
    error: null,
    rowsRead: 10,
    rowsWritten: 10,
    lineage: null,
    configHash: null,
    credits: null,
    steps,
  }
}

describe('normalizeStepScope', () => {
  it('folds the older persisted name onto the one the stream uses', () => {
    expect(normalizeStepScope('transform')).toBe('transformation')
    expect(normalizeStepScope('transformation')).toBe('transformation')
    expect(normalizeStepScope('validation_sink')).toBe('validation_sink')
  })
})

describe('nodeIdForStep', () => {
  it('counts each index-keyed scope in its own lane', () => {
    expect(nodeIdForStep(LANES, { scope: 'input', index: 0 })).toBe('src')
    expect(nodeIdForStep(LANES, { scope: 'transformation', index: 1 })).toBe('t1')
    expect(nodeIdForStep(LANES, { scope: 'validation', index: 1 })).toBe('v0')
    expect(nodeIdForStep(LANES, { scope: 'validation', index: 2 })).toBe('v1')
    expect(nodeIdForStep(LANES, { scope: 'output', index: 1 })).toBe('out1')
  })

  it('addresses the quality datasets by role, ignoring the arrival index', () => {
    expect(nodeIdForStep(LANES, { scope: 'validation_sink', index: 0, role: 'report' })).toBe(
      'dq-report',
    )
    expect(nodeIdForStep(LANES, { scope: 'validation_sink', index: 1, role: 'invalid' })).toBe(
      'dq-quarantine',
    )
    // Declared by the framework but not present on this canvas.
    expect(
      nodeIdForStep(LANES, { scope: 'validation_sink', index: 2, role: 'valid' }),
    ).toBeUndefined()
    expect(nodeIdForStep(LANES, { scope: 'validation_sink', index: 0, role: null })).toBeUndefined()
  })

  it('reads a scope-less step as a transformation, like the stream does', () => {
    expect(nodeIdForStep(LANES, { scope: 'transform', index: 0 })).toBe('t0')
  })

  it('maps nothing when the index is past the end of its lane', () => {
    expect(nodeIdForStep(LANES, { scope: 'output', index: 9 })).toBeUndefined()
    expect(nodeIdForStep(LANES, { scope: 'transformation' })).toBeUndefined()
  })
})

describe('runViewStatuses', () => {
  it('paints every box the run touched and leaves the rest pending', () => {
    const view = runViewStatuses(
      LANES,
      jobRun([
        step({ id: 'a', scope: 'input', stepIndex: 0, type: 'csv' }),
        step({ id: 'b', scope: 'transform', stepIndex: 0 }),
        step({ id: 'c', scope: 'output', stepIndex: 0, type: 'parquet' }),
      ]),
    )

    expect(view.status).toEqual({
      src: 'success',
      t0: 'success',
      t1: 'pending',
      v0: 'pending',
      v1: 'pending',
      'dq-report': 'pending',
      'dq-quarantine': 'pending',
      out0: 'success',
      out1: 'pending',
    })
    expect(view.unmatched).toEqual([])
  })

  it('translates the server word for a failure into the canvas one', () => {
    const view = runViewStatuses(
      LANES,
      jobRun(
        [step({ scope: 'validation_sink', stepIndex: 0, role: 'report', status: 'failed' })],
        'failed',
      ),
    )

    expect(view.status['dq-report']).toBe('error')
  })

  it('reports the worst status when one box carries several steps', () => {
    const view = runViewStatuses(
      LANES,
      jobRun([
        step({ id: 'r0', scope: 'validation', stepIndex: 0, status: 'success', durationMs: 200 }),
        step({ id: 'r1', scope: 'validation', stepIndex: 1, status: 'failed', durationMs: 300 }),
      ]),
    )

    expect(view.status.v0).toBe('error')
    // Both rules ran on the same box, so the box shows the time both took.
    expect(view.duration.v0).toBe(500)
    expect(view.steps.v0.map((entry) => entry.id)).toEqual(['r0', 'r1'])
  })

  it('never lets a skipped sibling hide a success', () => {
    const view = runViewStatuses(
      LANES,
      jobRun([
        step({ scope: 'validation', stepIndex: 1, status: 'success' }),
        step({ scope: 'validation', stepIndex: 0, status: 'skipped', durationMs: null }),
      ]),
    )

    expect(view.status.v0).toBe('success')
  })

  it('sets aside steps whose box no longer exists', () => {
    const edited: StepNodeLanes = { ...LANES, transformIds: [] }

    const view = runViewStatuses(edited, jobRun([step({ id: 'gone', scope: 'transformation' })]))

    expect(view.unmatched.map((entry) => entry.id)).toEqual(['gone'])
    expect(view.status.t0).toBeUndefined()
  })
})

describe('stepDetails', () => {
  it('parses what the framework reported about the step', () => {
    expect(stepDetails(step({ details: '{"rows": 42, "path": "data/in.csv"}' }))).toEqual({
      rows: 42,
      path: 'data/in.csv',
    })
  })

  it('is empty rather than throwing on absent or malformed JSON', () => {
    expect(stepDetails(step({ details: null }))).toEqual({})
    expect(stepDetails(step({ details: 'not json' }))).toEqual({})
    expect(stepDetails(step({ details: '[1, 2]' }))).toEqual({})
  })
})

describe('stepLabel', () => {
  it('names a step by its scope and what it did', () => {
    expect(stepLabel(step({ scope: 'transform', type: 'filter' }))).toBe('transformation · filter')
    expect(stepLabel(step({ scope: 'validation_sink', role: 'report', type: 'csv' }))).toBe(
      'validation_sink · report',
    )
    expect(stepLabel(step({ scope: 'input', type: '' }))).toBe('input')
  })
})
