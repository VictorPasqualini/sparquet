import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DEFAULT_RUNNER_URL, RUNNER_TOKEN_HEADER } from '@/lib/runner/client'
import {
  ANY_JOB,
  createMonitor,
  deleteMonitor,
  describeDraft,
  evaluateMonitors,
  fetchJobHealth,
  fetchMonitorEvents,
  fetchMonitorStatus,
  isForbidden,
  listMonitors,
  updateMonitor,
} from '@/lib/runner/monitoring'

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

const MONITOR = {
  id: 'm1',
  kind: 'failed',
  job_id: '*',
  threshold: 2,
  baseline: 'absolute',
  window: 10,
  enabled: true,
  name: 'Anything that fails',
  created_at: '2026-09-01T10:00:00Z',
  updated_at: '2026-09-01T10:00:00Z',
  rule: 'any Job: last 2 consecutive runs failed',
}

const HEALTH = {
  job_id: 'j1',
  name: 'Daily sales',
  workflow_id: 'wf1',
  last_run_id: 'r9',
  last_status: 'failed',
  last_started_at: '2026-09-13T09:00:00Z',
  last_finished_at: '2026-09-13T09:01:00Z',
  last_duration_ms: 60000,
  last_rows_read: 10,
  last_rows_written: 0,
  last_error: 'boom',
  last_success_at: '2026-09-12T09:00:00Z',
  consecutive_failures: 2,
  runs: 7,
  failures: 2,
  durations: [60000, 58000],
  volumes: [1000, 1100],
}

beforeEach(() => {
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('fetchJobHealth', () => {
  it('maps a record and sends the token', async () => {
    fetchMock.mockResolvedValue(jsonResponse([HEALTH]))

    const health = await fetchJobHealth(DEFAULT_RUNNER_URL, 'secret')

    expect(health).toEqual([
      {
        jobId: 'j1',
        name: 'Daily sales',
        workflowId: 'wf1',
        lastRunId: 'r9',
        lastStatus: 'failed',
        lastStartedAt: '2026-09-13T09:00:00Z',
        lastFinishedAt: '2026-09-13T09:01:00Z',
        lastDurationMs: 60000,
        lastRowsRead: 10,
        lastRowsWritten: 0,
        lastError: 'boom',
        lastSuccessAt: '2026-09-12T09:00:00Z',
        consecutiveFailures: 2,
        runs: 7,
        failures: 2,
        durations: [60000, 58000],
        volumes: [1000, 1100],
      },
    ])
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/health/jobs`)
    expect((init.headers as Record<string, string>)[RUNNER_TOKEN_HEADER]).toBe('secret')
  })

  it('survives a row that is not an object', async () => {
    // The runner is trusted, but a proxy in between is not, and half a list is
    // worth more than a screen that throws.
    fetchMock.mockResolvedValue(jsonResponse([null, HEALTH, 'nope']))
    expect(await fetchJobHealth()).toHaveLength(1)
  })

  it('answers with nothing when the payload is not a list', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: 'hm' }))
    expect(await fetchJobHealth()).toEqual([])
  })
})

describe('listMonitors', () => {
  it('maps a rule, rule text included', async () => {
    fetchMock.mockResolvedValue(jsonResponse([MONITOR]))

    const [monitor] = await listMonitors()

    expect(monitor).toMatchObject({
      id: 'm1',
      kind: 'failed',
      jobId: ANY_JOB,
      threshold: 2,
      baseline: 'absolute',
      enabled: true,
      rule: 'any Job: last 2 consecutive runs failed',
    })
  })

  it('falls back rather than inventing a kind it does not know', async () => {
    fetchMock.mockResolvedValue(jsonResponse([{ ...MONITOR, kind: 'vibes', baseline: 'mean' }]))
    const [monitor] = await listMonitors()
    expect(monitor.kind).toBe('failed')
    expect(monitor.baseline).toBe('absolute')
  })
})

describe('createMonitor', () => {
  it('sends only the fields the draft carries', async () => {
    fetchMock.mockResolvedValue(jsonResponse(MONITOR))

    await createMonitor({ kind: 'late', threshold: 120 }, DEFAULT_RUNNER_URL, 'secret')

    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/monitors`)
    expect(init.method).toBe('POST')
    expect(JSON.parse(String(init.body))).toEqual({ kind: 'late', threshold: 120 })
  })
})

describe('updateMonitor', () => {
  it('patches one field without resending the rest', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ ...MONITOR, enabled: false }))

    const monitor = await updateMonitor('m1', { enabled: false })

    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/monitors/m1`)
    expect(init.method).toBe('PATCH')
    expect(JSON.parse(String(init.body))).toEqual({ enabled: false })
    expect(monitor?.enabled).toBe(false)
  })

  it('escapes the id in the path', async () => {
    fetchMock.mockResolvedValue(jsonResponse(MONITOR))
    await updateMonitor('a/b', { enabled: true })
    expect(lastCall()[0]).toBe(`${DEFAULT_RUNNER_URL}/monitors/a%2Fb`)
  })
})

describe('deleteMonitor', () => {
  it('asks for a delete', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ deleted: true }))
    await deleteMonitor('m1')
    expect(lastCall()[1].method).toBe('DELETE')
  })
})

describe('fetchMonitorStatus', () => {
  it('maps a verdict and the denormalised rule text', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse([
        {
          monitor_id: 'm1',
          job_id: 'j1',
          firing: true,
          reason: '2 consecutive failed runs.',
          since: '2026-09-13T09:01:00Z',
          checked_at: '2026-09-13T09:02:00Z',
          value: 2,
          baseline: 2,
          run_id: 'r9',
          kind: 'failed',
          rule: 'any Job: last 2 consecutive runs failed',
          name: 'Anything that fails',
          job_name: 'Daily sales',
        },
      ]),
    )

    const [state] = await fetchMonitorStatus()

    expect(state).toMatchObject({
      monitorId: 'm1',
      jobId: 'j1',
      firing: true,
      jobName: 'Daily sales',
      kind: 'failed',
    })
  })

  it('asks the server for the firing ones only when told to', async () => {
    fetchMock.mockResolvedValue(jsonResponse([]))
    await fetchMonitorStatus(DEFAULT_RUNNER_URL, { firingOnly: true })
    expect(lastCall()[0]).toBe(`${DEFAULT_RUNNER_URL}/monitors/status?firing_only=true`)
  })
})

describe('fetchMonitorEvents', () => {
  it('passes the limit and the rule through as query parameters', async () => {
    fetchMock.mockResolvedValue(jsonResponse([]))
    await fetchMonitorEvents(DEFAULT_RUNNER_URL, { limit: 10, monitorId: 'm1' })
    expect(lastCall()[0]).toBe(`${DEFAULT_RUNNER_URL}/monitors/events?limit=10&monitor_id=m1`)
  })
})

describe('evaluateMonitors', () => {
  it('maps the report and its transitions', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        checked: 3,
        firing: 1,
        transitions: [
          {
            id: 'e1',
            monitor_id: 'm1',
            job_id: 'j1',
            at: '2026-09-13T09:02:00Z',
            firing: true,
            reason: 'boom',
            value: 1,
            baseline: 1,
            run_id: 'r9',
          },
        ],
      }),
    )

    const report = await evaluateMonitors()

    expect(report.checked).toBe(3)
    expect(report.firing).toBe(1)
    expect(report.transitions[0]).toMatchObject({ id: 'e1', jobId: 'j1', firing: true })
    expect(lastCall()[1].method).toBe('POST')
  })

  it('answers with an empty report rather than throwing on nonsense', async () => {
    fetchMock.mockResolvedValue(jsonResponse([]))
    expect(await evaluateMonitors()).toEqual({ checked: 0, firing: 0, transitions: [] })
  })
})

describe('isForbidden', () => {
  it('tells a 403 apart from every other failure', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: 'no' }, 403))
    await expect(listMonitors()).rejects.toSatisfy(isForbidden)

    fetchMock.mockResolvedValue(jsonResponse({ detail: 'no' }, 500))
    await expect(listMonitors()).rejects.not.toSatisfy(isForbidden)
  })
})

describe('describeDraft', () => {
  // The saved rule's wording comes from the runner and reaches the webhook; this
  // one is only for a draft that has no rule yet. They have to agree, or the
  // sentence changes the moment somebody presses Add.
  it('words a failure rule the way the runner does', () => {
    expect(describeDraft({ kind: 'failed', threshold: 1 })).toBe('any Job: last run failed')
    expect(describeDraft({ kind: 'failed', threshold: 2 })).toBe(
      'any Job: last 2 consecutive runs failed',
    )
  })

  it('names the Job when the rule has one', () => {
    expect(describeDraft({ kind: 'failed', jobId: 'j1', threshold: 1 })).toBe(
      'Job j1: last run failed',
    )
  })

  it('words the other three kinds', () => {
    expect(describeDraft({ kind: 'late', threshold: 120 })).toBe(
      'any Job: no successful run in 120 minutes',
    )
    expect(describeDraft({ kind: 'duration', threshold: 2, baseline: 'median' })).toBe(
      'any Job: last run took more than 2x its median',
    )
    expect(describeDraft({ kind: 'duration', threshold: 300000, baseline: 'absolute' })).toBe(
      'any Job: last run took more than 300000 ms',
    )
    expect(describeDraft({ kind: 'volume', threshold: 0.5, baseline: 'median' })).toBe(
      'any Job: last run wrote less than 0.5x its median',
    )
    expect(describeDraft({ kind: 'volume', threshold: 1000, baseline: 'absolute' })).toBe(
      'any Job: last run wrote fewer than 1000 rows',
    )
  })
})
