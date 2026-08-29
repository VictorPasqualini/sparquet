import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DEFAULT_RUNNER_URL, RUNNER_TOKEN_HEADER } from '@/lib/runner/client'
import {
  getCreditLedger,
  getMyCredits,
  grantCredits,
  isOutOfCredits,
  listCreditAccounts,
  toRunCharge,
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
  id: 't1',
  username: 'platform',
  balance: 7,
  spent: 3,
  period: '2026-08',
  free_used: 9,
  free_monthly: 40,
  free_remaining: 31,
  available: 38,
  created_at: '2026-08-20T10:00:00Z',
  updated_at: '2026-08-20T11:00:00Z',
}

const USAGE = { period: '2026-08', writes: 12, charged: 3, waived: 9 }

const ENTRY = {
  id: 'e1',
  account_id: 't1',
  amount: -2,
  reason: 'run',
  applied: true,
  balance_after: 7,
  created_at: '2026-08-20T11:00:00Z',
  writes: 2,
  free_amount: 1,
  shortfall: 0,
  period: '2026-08',
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
  it('maps the team account, its allowance and whether it is being enforced', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        account: ACCOUNT,
        enforced: true,
        credits_per_write: 2,
        free_monthly: 40,
        usage: USAGE,
      }),
    )

    const status = await getMyCredits(DEFAULT_RUNNER_URL, 'secret')

    expect(status).toEqual({
      account: {
        id: 't1',
        username: 'platform',
        balance: 7,
        spent: 3,
        period: '2026-08',
        freeUsed: 9,
        freeMonthly: 40,
        freeRemaining: 31,
        available: 38,
        createdAt: '2026-08-20T10:00:00Z',
        updatedAt: '2026-08-20T11:00:00Z',
      },
      enforced: true,
      creditsPerWrite: 2,
      freeMonthly: 40,
      usage: { period: '2026-08', writes: 12, charged: 3, waived: 9 },
    })
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/credits/me`)
    expect((init.headers as Record<string, string>)[RUNNER_TOKEN_HEADER]).toBe('secret')
  })

  it('reads a metering-only runner as not enforced, one credit per write', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ account: ACCOUNT }))

    const status = await getMyCredits()

    expect(status.enforced).toBe(false)
    expect(status.creditsPerWrite).toBe(1)
    // A runner that reports no usage block is a zeroed month, not a crash.
    expect(status.usage).toEqual({ period: '', writes: 0, charged: 0, waived: 0 })
  })
})

describe('toRunCharge', () => {
  it('maps what a run cost, keeping a metered charge distinguishable', () => {
    expect(
      toRunCharge({
        amount: 3,
        writes: 3,
        applied: false,
        free_amount: 3,
        shortfall: 0,
        target: 'spark://cluster:7077',
        balance_after: null,
      }),
    ).toEqual({
      amount: 3,
      writes: 3,
      applied: false,
      freeAmount: 3,
      shortfall: 0,
      target: 'spark://cluster:7077',
      balanceAfter: null,
    })
  })

  it('is null for a run that was never charged — a local run, or an old record', () => {
    expect(toRunCharge(null)).toBeNull()
    expect(toRunCharge(undefined)).toBeNull()
  })
})

describe('listCreditAccounts', () => {
  it('maps every account', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse([ACCOUNT, { ...ACCOUNT, id: 't2', username: 'data' }]),
    )

    const accounts = await listCreditAccounts()

    expect(accounts.map((account) => account.id)).toEqual(['t1', 't2'])
  })

  it('survives a runner that answers with something that is not a list', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ oops: true }))

    await expect(listCreditAccounts()).resolves.toEqual([])
  })
})

describe('getCreditLedger', () => {
  it('maps entries and passes the limit', async () => {
    fetchMock.mockResolvedValue(jsonResponse([ENTRY]))

    const entries = await getCreditLedger(DEFAULT_RUNNER_URL, 't1', 20)

    expect(entries[0]).toEqual({
      id: 'e1',
      accountId: 't1',
      amount: -2,
      reason: 'run',
      applied: true,
      balanceAfter: 7,
      createdAt: '2026-08-20T11:00:00Z',
      writes: 2,
      freeAmount: 1,
      shortfall: 0,
      period: '2026-08',
      jobRunId: 'job-1',
      pipelineRunId: 'run-1',
      target: 'spark://cluster:7077',
      jobName: 'orders',
      note: null,
    })
    expect(lastCall()[0]).toBe(`${DEFAULT_RUNNER_URL}/credits/t1/ledger?limit=20`)
  })

  it('keeps a metered row distinguishable from a charged one', async () => {
    fetchMock.mockResolvedValue(jsonResponse([{ ...ENTRY, applied: false }]))

    const entries = await getCreditLedger(DEFAULT_RUNNER_URL, 't1')

    expect(entries[0].applied).toBe(false)
  })
})

describe('grantCredits', () => {
  it('posts the amount and the note', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ ...ACCOUNT, balance: 17 }))

    const account = await grantCredits(DEFAULT_RUNNER_URL, 't1', 10, 'quarter budget')

    expect(account.balance).toBe(17)
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/credits/t1/grant`)
    expect(init.method).toBe('POST')
    expect(JSON.parse(String(init.body))).toEqual({ amount: 10, note: 'quarter budget' })
  })

  it('sends a null note rather than omitting it', async () => {
    fetchMock.mockResolvedValue(jsonResponse(ACCOUNT))

    await grantCredits(DEFAULT_RUNNER_URL, 't1', -5)

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
