import { describe, expect, it, vi } from 'vitest'

import { pickJobRun, resolveRunView } from '@/lib/runner/runView'
import type { RunViewRequest } from '@/lib/runner/runView'
import type { JobRunRecord, PipelineRunRecord } from '@/types/history'

const RUNNER_URL = 'http://127.0.0.1:8787'

function jobRun(overrides: Partial<JobRunRecord> = {}): JobRunRecord {
  return {
    id: 'job-run-1',
    pipelineRunId: 'run-1',
    jobId: 'job-a',
    name: 'Ingestion',
    stageIndex: 0,
    status: 'success',
    startedAt: '2026-08-27T10:00:00Z',
    finishedAt: '2026-08-27T10:00:02Z',
    durationMs: 2000,
    error: null,
    rowsRead: 6,
    rowsWritten: 5,
    lineage: null,
    configHash: null,
    credits: null,
    steps: [],
    ...overrides,
  }
}

function pipelineRun(overrides: Partial<PipelineRunRecord> = {}): PipelineRunRecord {
  return {
    id: 'run-1',
    kind: 'job',
    workflowId: 'wf-1',
    pipelineId: null,
    jobId: 'job-a',
    name: 'Ingestion',
    status: 'success',
    startedAt: '2026-08-27T10:00:00Z',
    finishedAt: '2026-08-27T10:00:02Z',
    durationMs: 2000,
    error: null,
    runAs: null,
    launched: 'manual',
    pinned: false,
    jobs: [jobRun()],
    ...overrides,
  }
}

/** A two-stage pipeline execution: one job run per stage, both jobs distinct. */
function stagedRun(): PipelineRunRecord {
  return pipelineRun({
    id: 'run-9',
    kind: 'pipeline',
    pipelineId: 'pipe-1',
    jobId: null,
    name: 'History pipeline',
    jobs: [
      jobRun({ id: 'job-run-9a', pipelineRunId: 'run-9', jobId: 'job-a', stageIndex: 0 }),
      jobRun({ id: 'job-run-9b', pipelineRunId: 'run-9', jobId: 'job-b', stageIndex: 1 }),
    ],
  })
}

function loaders(runs: PipelineRunRecord[], detail: PipelineRunRecord | null) {
  return {
    listRuns: vi.fn(async () => runs),
    getRun: vi.fn(async () => detail),
  }
}

describe('pickJobRun', () => {
  it('takes the job run a stage named, not the one whose job id matches', () => {
    const run = stagedRun()
    // Same job on two stages is legal, so the id is what tells them apart.
    const picked = pickJobRun(run, 'job-a', { runId: 'run-9', jobRunId: 'job-run-9b' })
    expect(picked?.id).toBe('job-run-9b')
  })

  it('falls back to the job own id when no job run was named', () => {
    expect(pickJobRun(stagedRun(), 'job-b')?.id).toBe('job-run-9b')
  })

  it('accepts a solo run recorded before Studio sent a job id', () => {
    const run = pipelineRun({ jobId: null, jobs: [jobRun({ jobId: null })] })
    expect(pickJobRun(run, 'job-a')?.id).toBe('job-run-1')
  })

  it('returns nothing when the execution never touched this job', () => {
    expect(pickJobRun(stagedRun(), 'job-z')).toBeUndefined()
  })
})

describe('resolveRunView', () => {
  it('loads the job latest run, unpinned, when nothing was requested', async () => {
    const detail = pipelineRun()
    const client = loaders([pipelineRun({ jobs: [] })], detail)

    const resolved = await resolveRunView({ jobId: 'job-a', runnerUrl: RUNNER_URL, ...client })

    expect(client.listRuns).toHaveBeenCalledWith(
      RUNNER_URL,
      { jobId: 'job-a', limit: 1 },
      undefined,
      undefined,
    )
    expect(resolved?.run.id).toBe('run-1')
    expect(resolved?.jobRun.id).toBe('job-run-1')
    expect(resolved?.pinned).toBe(false)
  })

  it('asks the runner nothing when latest is off and nothing was requested', async () => {
    const client = loaders([pipelineRun({ jobs: [] })], pipelineRun())

    const resolved = await resolveRunView({
      jobId: 'job-a',
      runnerUrl: RUNNER_URL,
      allowLatest: false,
      ...client,
    })

    // Opening a job shows the job: no history call, nothing painted on the canvas.
    expect(resolved).toBeNull()
    expect(client.listRuns).not.toHaveBeenCalled()
    expect(client.getRun).not.toHaveBeenCalled()
  })

  it('still honours a requested run while latest is off', async () => {
    const client = loaders([], stagedRun())

    const resolved = await resolveRunView({
      jobId: 'job-a',
      request: { runId: 'run-9', jobRunId: 'job-run-9b' },
      runnerUrl: RUNNER_URL,
      allowLatest: false,
      ...client,
    })

    expect(resolved?.jobRun.id).toBe('job-run-9b')
    expect(resolved?.pinned).toBe(true)
    expect(client.listRuns).not.toHaveBeenCalled()
  })

  it('loads the requested run directly and pins it', async () => {
    const client = loaders([], stagedRun())

    const resolved = await resolveRunView({
      jobId: 'job-a',
      request: { runId: 'run-9', jobRunId: 'job-run-9b' },
      runnerUrl: RUNNER_URL,
      ...client,
    })

    // Asking for a run by id means never asking which run is the latest.
    expect(client.listRuns).not.toHaveBeenCalled()
    expect(client.getRun).toHaveBeenCalledWith(RUNNER_URL, 'run-9', undefined, undefined)
    expect(resolved?.jobRun.id).toBe('job-run-9b')
    expect(resolved?.pinned).toBe(true)
  })

  it('leaves the request intact for the attempt that replaces an abandoned one', async () => {
    // React runs every effect twice in development: the first pass is torn down
    // mid-flight. It must not eat the stage's request, or the second pass shows
    // the job latest run instead of the execution the stage was coloured by.
    const request: RunViewRequest = { runId: 'run-9', jobRunId: 'job-run-9b' }
    const abandoned = loaders([], stagedRun())

    const controller = new AbortController()
    const first = resolveRunView({
      jobId: 'job-a',
      request,
      runnerUrl: RUNNER_URL,
      signal: controller.signal,
      ...abandoned,
    })
    controller.abort()
    await first

    expect(request).toEqual({ runId: 'run-9', jobRunId: 'job-run-9b' })

    const retry = loaders([pipelineRun({ id: 'run-latest' })], stagedRun())
    const resolved = await resolveRunView({
      jobId: 'job-a',
      request,
      runnerUrl: RUNNER_URL,
      ...retry,
    })

    expect(retry.listRuns).not.toHaveBeenCalled()
    expect(resolved?.run.id).toBe('run-9')
    expect(resolved?.jobRun.id).toBe('job-run-9b')
    expect(resolved?.pinned).toBe(true)
  })

  it('returns nothing when the job has never run', async () => {
    const client = loaders([], pipelineRun())
    const resolved = await resolveRunView({ jobId: 'job-a', runnerUrl: RUNNER_URL, ...client })

    expect(resolved).toBeNull()
    expect(client.getRun).not.toHaveBeenCalled()
  })

  it('returns nothing when the runner no longer has the requested run', async () => {
    const client = loaders([], null)
    const resolved = await resolveRunView({
      jobId: 'job-a',
      request: { runId: 'gone' },
      runnerUrl: RUNNER_URL,
      ...client,
    })

    expect(resolved).toBeNull()
  })

  it('passes the abort signal and the token through to both calls', async () => {
    const client = loaders([pipelineRun()], pipelineRun())
    const signal = new AbortController().signal

    await resolveRunView({
      jobId: 'job-a',
      runnerUrl: RUNNER_URL,
      runnerToken: 'secret',
      signal,
      ...client,
    })

    expect(client.listRuns).toHaveBeenCalledWith(
      RUNNER_URL,
      { jobId: 'job-a', limit: 1 },
      signal,
      'secret',
    )
    expect(client.getRun).toHaveBeenCalledWith(RUNNER_URL, 'run-1', signal, 'secret')
  })
})
