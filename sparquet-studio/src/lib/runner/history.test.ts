import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DEFAULT_RUNNER_URL, isRunnerError, RUNNER_TOKEN_HEADER, RunnerError } from '@/lib/runner/client'
import { getJobRunConfig, getJobRunLogs, getRun, listRuns } from '@/lib/runner/history'

const fetchMock = vi.fn()

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}

function lastCall(): [string, RequestInit] {
  const call = fetchMock.mock.calls.at(-1)
  if (!call) throw new Error('fetch was not called')
  return call as [string, RequestInit]
}

const STEP_RUN = {
  id: 'step-1',
  job_run_id: 'job-1',
  scope: 'transformation',
  step_index: 0,
  type: 'filter',
  status: 'success',
  started_at: '2026-08-20T10:00:00Z',
  finished_at: '2026-08-20T10:00:01Z',
  duration_ms: 1000,
  error_message: null,
  error_details: null,
  role: null,
  details: '{"rows": 42}',
}

const JOB_RUN = {
  id: 'job-1',
  pipeline_run_id: 'run-1',
  job_id: 'j1',
  name: 'orders',
  stage_index: 0,
  status: 'success',
  started_at: '2026-08-20T10:00:00Z',
  finished_at: '2026-08-20T10:00:01Z',
  duration_ms: 1000,
  error: null,
  rows_read: 10,
  rows_written: 10,
  steps: [STEP_RUN],
}

const PIPELINE_RUN = {
  id: 'run-1',
  kind: 'job',
  workflow_id: 'w1',
  pipeline_id: null,
  job_id: 'j1',
  name: 'orders',
  status: 'success',
  started_at: '2026-08-20T10:00:00Z',
  finished_at: '2026-08-20T10:00:01Z',
  duration_ms: 1000,
  error: null,
  jobs: [JOB_RUN],
}

beforeEach(() => {
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('listRuns', () => {
  it('maps a list of run summaries, most recent first', async () => {
    fetchMock.mockResolvedValue(jsonResponse([{ ...PIPELINE_RUN, jobs: [] }]))

    const runs = await listRuns(DEFAULT_RUNNER_URL, { jobId: 'j1', limit: 10 })

    expect(runs).toEqual([
      {
        id: 'run-1',
        kind: 'job',
        workflowId: 'w1',
        pipelineId: null,
        jobId: 'j1',
        name: 'orders',
        status: 'success',
        startedAt: '2026-08-20T10:00:00Z',
        finishedAt: '2026-08-20T10:00:01Z',
        durationMs: 1000,
        error: null,
        runAs: null,
        launched: null,
        jobs: [],
      },
    ])

    const [url] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/runs?job_id=j1&limit=10`)
  })

  it('sends only the filters that were set', async () => {
    fetchMock.mockResolvedValue(jsonResponse([]))

    await listRuns(DEFAULT_RUNNER_URL, {})

    const [url] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/runs`)
  })

  it('defaults an unknown status to pending rather than throwing', async () => {
    fetchMock.mockResolvedValue(jsonResponse([{ ...PIPELINE_RUN, status: 'bogus', jobs: [] }]))

    const runs = await listRuns(DEFAULT_RUNNER_URL)

    expect(runs[0].status).toBe('pending')
  })

  it('keeps a cancelled run cancelled, instead of reading it as pending', async () => {
    fetchMock.mockResolvedValue(jsonResponse([{ ...PIPELINE_RUN, status: 'cancelled', jobs: [] }]))

    const runs = await listRuns(DEFAULT_RUNNER_URL)

    expect(runs[0].status).toBe('cancelled')
  })
})

describe('getRun', () => {
  it('maps the full run, including nested jobs and steps', async () => {
    fetchMock.mockResolvedValue(jsonResponse(PIPELINE_RUN))

    const run = await getRun(DEFAULT_RUNNER_URL, 'run-1')

    expect(run?.jobs).toHaveLength(1)
    expect(run?.jobs[0].steps).toEqual([
      {
        id: 'step-1',
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
        details: '{"rows": 42}',
      },
    ])
    const [url] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/runs/run-1`)
  })

  it('keeps the role of a step the runner addressed by role instead of index', async () => {
    const report = {
      ...STEP_RUN,
      id: 'step-2',
      scope: 'validation_sink',
      role: 'report',
      type: 'csv',
      details: '{"path": "out/report", "rows": 3}',
    }
    fetchMock.mockResolvedValue(
      jsonResponse({ ...PIPELINE_RUN, jobs: [{ ...JOB_RUN, steps: [report] }] }),
    )

    const run = await getRun(DEFAULT_RUNNER_URL, 'run-1')

    expect(run?.jobs[0].steps[0].role).toBe('report')
    expect(run?.jobs[0].steps[0].details).toBe('{"path": "out/report", "rows": 3}')
  })

  it('leaves role and details null when an older runner never wrote them', async () => {
    const legacy = { ...STEP_RUN }
    delete (legacy as Record<string, unknown>).role
    delete (legacy as Record<string, unknown>).details
    fetchMock.mockResolvedValue(
      jsonResponse({ ...PIPELINE_RUN, jobs: [{ ...JOB_RUN, steps: [legacy] }] }),
    )

    const run = await getRun(DEFAULT_RUNNER_URL, 'run-1')

    expect(run?.jobs[0].steps[0].role).toBeNull()
    expect(run?.jobs[0].steps[0].details).toBeNull()
  })

  it('parses the lineage the runner stored as a JSON string', async () => {
    const lineage = JSON.stringify({
      inputs: [{ role: 'input', format: 'csv', address: 'data/orders.csv' }],
      outputs: [{ role: 'output', format: 'parquet', address: 'out/orders', mode: 'overwrite' }],
    })
    fetchMock.mockResolvedValue(
      jsonResponse({ ...PIPELINE_RUN, jobs: [{ ...JOB_RUN, lineage }] }),
    )

    const run = await getRun(DEFAULT_RUNNER_URL, 'run-1')

    expect(run?.jobs[0].lineage).toEqual({
      inputs: [{ role: 'input', format: 'csv', address: 'data/orders.csv' }],
      outputs: [{ role: 'output', format: 'parquet', address: 'out/orders', mode: 'overwrite' }],
    })
  })

  it('reads a lineage it cannot parse as no lineage at all', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ ...PIPELINE_RUN, jobs: [{ ...JOB_RUN, lineage: 'not json' }] }),
    )

    const run = await getRun(DEFAULT_RUNNER_URL, 'run-1')

    expect(run?.jobs[0].lineage).toBeNull()
  })

  it('leaves lineage, run as and launched null on a run recorded before they existed', async () => {
    fetchMock.mockResolvedValue(jsonResponse(PIPELINE_RUN))

    const run = await getRun(DEFAULT_RUNNER_URL, 'run-1')

    expect(run?.jobs[0].lineage).toBeNull()
    expect(run?.runAs).toBeNull()
    expect(run?.launched).toBeNull()
  })

  it('maps who a run was attributed to and how it was launched', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ ...PIPELINE_RUN, run_as: 'victor', launched: 'scheduled' }),
    )

    const run = await getRun(DEFAULT_RUNNER_URL, 'run-1')

    expect(run?.runAs).toBe('victor')
    expect(run?.launched).toBe('scheduled')
  })

  it('drops a launch kind it does not know rather than showing it', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ ...PIPELINE_RUN, launched: 'telepathy' }))

    const run = await getRun(DEFAULT_RUNNER_URL, 'run-1')

    expect(run?.launched).toBeNull()
  })

  it('returns null for a run that no longer exists, instead of throwing', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: 'Execution not found.' }, 404))

    const run = await getRun(DEFAULT_RUNNER_URL, 'missing')

    expect(run).toBeNull()
  })

  it('still throws on other HTTP errors', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: 'boom' }, 500))

    const error = await getRun(DEFAULT_RUNNER_URL, 'run-1').catch((caught: unknown) => caught)

    expect(isRunnerError(error)).toBe(true)
    expect((error as RunnerError).status).toBe(500)
  })

  it('reports an unreachable runner', async () => {
    fetchMock.mockRejectedValue(new TypeError('fetch failed'))

    const error = await getRun(DEFAULT_RUNNER_URL, 'run-1').catch((caught: unknown) => caught)

    expect(isRunnerError(error)).toBe(true)
    expect((error as RunnerError).kind).toBe('unreachable')
  })
})

describe('getJobRunLogs', () => {
  const LOG_PAGE = {
    job_run_id: 'job-1',
    total: 3,
    next_after: 2,
    lines: [
      {
        seq: 1,
        timestamp: '2026-08-20T10:00:00Z',
        level: 'INFO',
        source: 'pipeline',
        message: 'Reading source',
        context: { rows: 42 },
      },
      {
        seq: 2,
        timestamp: '2026-08-20T10:00:01Z',
        level: 'ERROR',
        source: 'spark',
        message: 'Python worker exited unexpectedly (crashed)',
        context: {},
      },
    ],
  }

  it('maps a page of log lines, keeping the order the runner recorded', async () => {
    fetchMock.mockResolvedValue(jsonResponse(LOG_PAGE))

    const page = await getJobRunLogs(DEFAULT_RUNNER_URL, 'job-1')

    expect(page).toEqual({
      jobRunId: 'job-1',
      total: 3,
      nextAfter: 2,
      lines: [
        {
          seq: 1,
          timestamp: '2026-08-20T10:00:00Z',
          level: 'INFO',
          source: 'pipeline',
          message: 'Reading source',
          context: { rows: 42 },
        },
        {
          seq: 2,
          timestamp: '2026-08-20T10:00:01Z',
          level: 'ERROR',
          source: 'spark',
          message: 'Python worker exited unexpectedly (crashed)',
          context: {},
        },
      ],
    })
    const [url] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/job-runs/job-1/logs`)
  })

  it('pages by seq, not by offset', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ ...LOG_PAGE, lines: [], next_after: null }))

    const page = await getJobRunLogs(DEFAULT_RUNNER_URL, 'job 1', { after: 2, limit: 100 })

    expect(page.nextAfter).toBeNull()
    const [url] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/job-runs/job%201/logs?after=2&limit=100`)
  })

  it('fills in what an older runner left out of a line', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ job_run_id: 'job-1', lines: [{ seq: 7, message: 'bare' }, 'nonsense'] }),
    )

    const page = await getJobRunLogs(DEFAULT_RUNNER_URL, 'job-1')

    expect(page.lines).toEqual([
      { seq: 7, timestamp: '', level: 'INFO', source: 'pipeline', message: 'bare', context: {} },
    ])
    // No `total` in the payload: what came back is all there is to show.
    expect(page.total).toBe(1)
  })
})

describe('getJobRunConfig', () => {
  beforeEach(() => {
    fetchMock.mockReset()
    vi.stubGlobal('fetch', fetchMock)
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('reads back the version of the JSON one execution ran', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        job_run_id: 'job-1',
        config_hash: 'sha256:abc',
        config: { name: 'orders', input: { format: 'csv' } },
      }),
    )

    const stored = await getJobRunConfig(DEFAULT_RUNNER_URL, 'job 1', undefined, 'secret')

    expect(stored).toEqual({
      jobRunId: 'job-1',
      configHash: 'sha256:abc',
      config: { name: 'orders', input: { format: 'csv' } },
    })
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/job-runs/job%201/config`)
    expect((init.headers as Record<string, string>)[RUNNER_TOKEN_HEADER]).toBe('secret')
  })

  it('keeps the fingerprint when the runner did not keep the JSON', async () => {
    // A configuration over the stored size, or a run from before the column: the
    // hash still says whether two executions ran the same thing.
    fetchMock.mockResolvedValue(
      jsonResponse({ job_run_id: 'job-1', config_hash: 'sha256:abc', config: null }),
    )

    const stored = await getJobRunConfig(DEFAULT_RUNNER_URL, 'job-1')

    expect(stored?.configHash).toBe('sha256:abc')
    expect(stored?.config).toBeNull()
  })

  it('reads an execution the runner does not know as absent, not as a failure', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: 'Unknown job run.' }, 404))

    await expect(getJobRunConfig(DEFAULT_RUNNER_URL, 'gone')).resolves.toBeNull()
  })
})
