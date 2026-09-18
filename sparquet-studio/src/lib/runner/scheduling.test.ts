import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DEFAULT_RUNNER_URL, RUNNER_TOKEN_HEADER } from '@/lib/runner/client'
import { evaluateSchedules, isForbidden, listSchedules } from '@/lib/runner/scheduling'

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

const ENTRY = {
  kind: 'job',
  id: 'j1',
  name: 'Daily sales',
  cron: '0 6 * * *',
  timezone: 'America/Sao_Paulo',
  enabled: true,
  run_as: 'ana',
  workflow_id: 'wf1',
  error: null,
  rule: '0 6 * * * (America/Sao_Paulo)',
  next_fire: '2026-09-14T09:00:00Z',
  last_fire: '2026-09-13T09:00:00Z',
  last_run_id: 'r42',
  last_status: 'success',
}

beforeEach(() => {
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('listSchedules', () => {
  it('maps a record and sends the token', async () => {
    fetchMock.mockResolvedValue(jsonResponse([ENTRY]))

    const schedules = await listSchedules(DEFAULT_RUNNER_URL, 'secret')

    expect(schedules).toEqual([
      {
        kind: 'job',
        id: 'j1',
        name: 'Daily sales',
        cron: '0 6 * * *',
        timezone: 'America/Sao_Paulo',
        enabled: true,
        runAs: 'ana',
        workflowId: 'wf1',
        error: null,
        rule: '0 6 * * * (America/Sao_Paulo)',
        nextFire: '2026-09-14T09:00:00Z',
        lastFire: '2026-09-13T09:00:00Z',
        lastRunId: 'r42',
        lastStatus: 'success',
      },
    ])
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/schedules`)
    expect((init.headers as Record<string, string>)[RUNNER_TOKEN_HEADER]).toBe('secret')
  })

  it('keeps a schedule the runner could not read, with its reason', async () => {
    // A schedule with an unreadable expression is exactly the one somebody
    // needs to see: dropping it here would hide a Job that never runs.
    fetchMock.mockResolvedValue(
      jsonResponse([{ ...ENTRY, error: 'not a schedule: @daily', next_fire: null, rule: '' }]),
    )

    const [schedule] = await listSchedules()

    expect(schedule.error).toBe('not a schedule: @daily')
    expect(schedule.nextFire).toBeNull()
  })

  it('falls back to the local zone when the runner omits one', async () => {
    fetchMock.mockResolvedValue(jsonResponse([{ ...ENTRY, timezone: undefined }]))

    const [schedule] = await listSchedules()

    expect(schedule.timezone).toBe('local')
  })

  it('drops entries that are not records, and survives a body that is not a list', async () => {
    fetchMock.mockResolvedValue(jsonResponse([ENTRY, 'nonsense', null]))
    expect(await listSchedules()).toHaveLength(1)

    fetchMock.mockResolvedValue(jsonResponse({ schedules: [] }))
    expect(await listSchedules()).toEqual([])
  })
})

describe('evaluateSchedules', () => {
  it('posts and maps what fired', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        checked: 4,
        fired: 1,
        fires: [
          {
            kind: 'pipeline',
            id: 'p1',
            name: 'Nightly',
            due_at: '2026-09-13T09:00:00Z',
            started: true,
            run_id: 'r42',
            error: null,
          },
        ],
      }),
    )

    const sweep = await evaluateSchedules(DEFAULT_RUNNER_URL, 'secret')

    expect(sweep.checked).toBe(4)
    expect(sweep.fired).toBe(1)
    expect(sweep.fires[0]).toEqual({
      kind: 'pipeline',
      id: 'p1',
      name: 'Nightly',
      dueAt: '2026-09-13T09:00:00Z',
      started: true,
      runId: 'r42',
      error: null,
    })
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/schedules/evaluate`)
    expect(init.method).toBe('POST')
  })

  it('reports a schedule that came due and did not start', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        checked: 1,
        fired: 1,
        fires: [
          {
            kind: 'job',
            id: 'j1',
            name: 'Daily sales',
            due_at: '2026-09-13T09:00:00Z',
            started: false,
            run_id: null,
            error: 'A run is already in progress.',
          },
        ],
      }),
    )

    const sweep = await evaluateSchedules()

    expect(sweep.fires[0].started).toBe(false)
    expect(sweep.fires[0].error).toBe('A run is already in progress.')
  })

  it('answers an empty sweep when the body is not a record', async () => {
    fetchMock.mockResolvedValue(jsonResponse([]))

    expect(await evaluateSchedules()).toEqual({ checked: 0, fired: 0, fires: [] })
  })
})

describe('isForbidden', () => {
  it('tells a 403 apart from every other failure', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: 'Requires run:Execute.' }, 403))

    await expect(evaluateSchedules()).rejects.toSatisfy(isForbidden)

    fetchMock.mockResolvedValue(jsonResponse({ detail: 'boom' }, 500))
    await expect(listSchedules()).rejects.not.toSatisfy(isForbidden)
  })
})
