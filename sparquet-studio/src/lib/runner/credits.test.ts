import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DEFAULT_RUNNER_URL, RUNNER_TOKEN_HEADER } from '@/lib/runner/client'
import {
  getCreditLedger,
  getMyCredits,
  grantCredits,
  isOutOfCredits,
  listCreditAccounts,
} from '@/lib/runner/credits'

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

const ACCOUNT = {
  id: 'u1',
  username: 'ana',
  balance: 7,
  spent: 3,
  created_at: '2026-08-20T10:00:00Z',
  updated_at: '2026-08-20T11:00:00Z',
}

const ENTRY = {
  id: 'e1',
  account_id: 'u1',
  amount: -1,
  reason: 'run',
  applied: true,
  balance_after: 7,
  created_at: '2026-08-20T11:00:00Z',
  job_run_id: 'job-1',
  pipeline_run_id: 'run-1',
  target: 'spark://cluster:7077',
  job_name: 'orders',
  note: null,
}

beforeEach(() => {
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('getMyCredits', () => {
  it('maps the balance and whether it is being enforced', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ account: ACCOUNT, enforced: true, credits_per_job: 2 }),
    )

    const status = await getMyCredits(DEFAULT_RUNNER_URL, 'secret')

    expect(status).toEqual({
      account: {
        id: 'u1',
        username: 'ana',
        balance: 7,
        spent: 3,
        createdAt: '2026-08-20T10:00:00Z',
        updatedAt: '2026-08-20T11:00:00Z',
      },
      enforced: true,
      creditsPerJob: 2,
    })
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/credits/me`)
    expect((init.headers as Record<string, string>)[RUNNER_TOKEN_HEADER]).toBe('secret')
  })

  it('reads a metering-only runner as not enforced, one credit per Job', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ account: ACCOUNT }))

    const status = await getMyCredits()

    expect(status.enforced).toBe(false)
    expect(status.creditsPerJob).toBe(1)
  })
})

describe('listCreditAccounts', () => {
  it('maps every account', async () => {
    fetchMock.mockResolvedValue(jsonResponse([ACCOUNT, { ...ACCOUNT, id: 'u2', username: 'bo' }]))

    const accounts = await listCreditAccounts()

    expect(accounts.map((account) => account.id)).toEqual(['u1', 'u2'])
  })

  it('survives a runner that answers with something that is not a list', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ oops: true }))

    await expect(listCreditAccounts()).resolves.toEqual([])
  })
})

describe('getCreditLedger', () => {
  it('maps entries and passes the limit', async () => {
    fetchMock.mockResolvedValue(jsonResponse([ENTRY]))

    const entries = await getCreditLedger(DEFAULT_RUNNER_URL, 'u1', 20)

    expect(entries[0]).toEqual({
      id: 'e1',
      accountId: 'u1',
      amount: -1,
      reason: 'run',
      applied: true,
      balanceAfter: 7,
      createdAt: '2026-08-20T11:00:00Z',
      jobRunId: 'job-1',
      pipelineRunId: 'run-1',
      target: 'spark://cluster:7077',
      jobName: 'orders',
      note: null,
    })
    expect(lastCall()[0]).toBe(`${DEFAULT_RUNNER_URL}/credits/u1/ledger?limit=20`)
  })

  it('keeps a metered row distinguishable from a charged one', async () => {
    fetchMock.mockResolvedValue(jsonResponse([{ ...ENTRY, applied: false }]))

    const entries = await getCreditLedger(DEFAULT_RUNNER_URL, 'u1')

    expect(entries[0].applied).toBe(false)
  })
})

describe('grantCredits', () => {
  it('posts the amount and the note', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ ...ACCOUNT, balance: 17 }))

    const account = await grantCredits(DEFAULT_RUNNER_URL, 'u1', 10, 'quarter budget')

    expect(account.balance).toBe(17)
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/credits/u1/grant`)
    expect(init.method).toBe('POST')
    expect(JSON.parse(String(init.body))).toEqual({ amount: 10, note: 'quarter budget' })
  })

  it('sends a null note rather than omitting it', async () => {
    fetchMock.mockResolvedValue(jsonResponse(ACCOUNT))

    await grantCredits(DEFAULT_RUNNER_URL, 'u1', -5)

    expect(JSON.parse(String(lastCall()[1].body))).toEqual({ amount: -5, note: null })
  })
})

describe('isOutOfCredits', () => {
  it('is true only for the runner refusing to charge, not for other failures', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: 'out of credits' }, 402))
    const refused = await getMyCredits().catch((error: unknown) => error)
    expect(isOutOfCredits(refused)).toBe(true)
    expect((refused as Error).message).toBe('out of credits')

    fetchMock.mockResolvedValue(jsonResponse({ detail: 'nope' }, 403))
    const forbidden = await getMyCredits().catch((error: unknown) => error)
    expect(isOutOfCredits(forbidden)).toBe(false)

    expect(isOutOfCredits(new Error('boom'))).toBe(false)
  })
})
