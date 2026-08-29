/**
 * Client for execution credits (`GET /credits/me`, `POST /credits/{id}/grant`).
 *
 * Its own module rather than part of `client.ts` for the same reason `history.ts`
 * is: running a Job, reading past runs and asking what a run will cost are three
 * questions with three different failure modes. A runner whose Spark is broken
 * can still say what the balance is, and the balance is exactly what somebody
 * wants to know when a run has just been refused.
 */

import type {
  CreditAccount,
  CreditEntry,
  CreditStatus,
  CreditUsage,
  RunCharge,
  SpendBreakdown,
  SpendGroup,
  SpendGroupBy,
  SpendPeriod,
  SpendTimeline,
} from '@/types/credits'

import {
  authHeaders,
  DEFAULT_RUNNER_URL,
  RunnerError,
  RUNNER_UNREACHABLE_MESSAGE,
} from './client'

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function asString(value: unknown, fallback = ''): string {
  return typeof value === 'string' ? value : fallback
}

function asNullableString(value: unknown): string | null {
  return typeof value === 'string' ? value : null
}

function asNumber(value: unknown, fallback = 0): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback
}

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, '')
}

async function readErrorMessage(response: Response): Promise<string> {
  let body = ''
  try {
    body = await response.text()
  } catch {
    body = ''
  }
  try {
    const parsed: unknown = JSON.parse(body)
    if (isRecord(parsed) && typeof parsed.detail === 'string' && parsed.detail.length > 0) {
      return parsed.detail
    }
  } catch {
    /* not JSON — fall through to the status line */
  }
  return `Local runner error (HTTP ${response.status})`
}

async function request(
  baseUrl: string,
  path: string,
  init: RequestInit,
  token?: string,
  signal?: AbortSignal,
): Promise<unknown> {
  let response: Response
  const headers: Record<string, string> = { ...authHeaders(token) }
  if (init.body !== undefined) headers['content-type'] = 'application/json'
  try {
    response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, { ...init, headers, signal })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error
    throw new RunnerError(RUNNER_UNREACHABLE_MESSAGE, 'unreachable', undefined, error)
  }
  if (!response.ok) {
    throw new RunnerError(await readErrorMessage(response), 'http', response.status)
  }
  try {
    return (await response.json()) as unknown
  } catch (error) {
    throw new RunnerError(
      'The local runner returned a malformed response.',
      'malformed',
      response.status,
      error,
    )
  }
}

/** The HTTP status the runner answers with when an account cannot pay for a run. */
export const NO_CREDITS_STATUS = 402

/** Whether this failure is "you are out of credits" rather than anything else. */
export function isOutOfCredits(error: unknown): boolean {
  return error instanceof RunnerError && error.status === NO_CREDITS_STATUS
}

function toAccount(value: unknown): CreditAccount {
  const record = isRecord(value) ? value : {}
  return {
    id: asString(record.id),
    username: asString(record.username),
    balance: asNumber(record.balance),
    spent: asNumber(record.spent),
    period: asString(record.period),
    freeUsed: asNumber(record.free_used),
    freeMonthly: asNumber(record.free_monthly),
    freeRemaining: asNumber(record.free_remaining),
    available: asNumber(record.available),
    held: asNumber(record.held),
    createdAt: asNullableString(record.created_at),
    updatedAt: asNullableString(record.updated_at),
  }
}

function toUsage(value: unknown): CreditUsage {
  const record = isRecord(value) ? value : {}
  return {
    period: asString(record.period),
    writes: asNumber(record.writes),
    charged: asNumber(record.charged),
    waived: asNumber(record.waived),
  }
}

/** What a run cost, as `/run` and the execution history report it. */
export function toRunCharge(value: unknown): RunCharge | null {
  if (!isRecord(value)) return null
  return {
    amount: asNumber(value.amount),
    writes: asNumber(value.writes),
    applied: value.applied === true,
    freeAmount: asNumber(value.free_amount),
    shortfall: asNumber(value.shortfall),
    target: asNullableString(value.target),
    balanceAfter: typeof value.balance_after === 'number' ? value.balance_after : null,
  }
}

function toEntry(value: unknown): CreditEntry {
  const record = isRecord(value) ? value : {}
  return {
    id: asString(record.id),
    accountId: asString(record.account_id),
    amount: asNumber(record.amount),
    reason: asString(record.reason),
    applied: record.applied === true,
    balanceAfter: asNumber(record.balance_after),
    createdAt: asString(record.created_at),
    writes: asNumber(record.writes),
    freeAmount: asNumber(record.free_amount),
    shortfall: asNumber(record.shortfall),
    period: asNullableString(record.period),
    jobRunId: asNullableString(record.job_run_id),
    pipelineRunId: asNullableString(record.pipeline_run_id),
    target: asNullableString(record.target),
    jobName: asNullableString(record.job_name),
    note: asNullableString(record.note),
    workflowId: asNullableString(record.workflow_id),
    actor: asNullableString(record.actor),
    tags: Array.isArray(record.tags) ? record.tags.filter((tag): tag is string => typeof tag === 'string') : [],
  }
}

function toGroup(value: unknown): SpendGroup {
  const record = isRecord(value) ? value : {}
  return {
    key: asNullableString(record.key),
    label: asNullableString(record.label),
    writes: asNumber(record.writes),
    charged: asNumber(record.charged),
    waived: asNumber(record.waived),
    runs: asNumber(record.runs),
    lastAt: asNullableString(record.last_at),
  }
}

/** Your own balance, and whether it is being enforced at all. */
export async function getMyCredits(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<CreditStatus> {
  const payload = await request(baseUrl, '/credits/me', { method: 'GET' }, token, signal)
  const record = isRecord(payload) ? payload : {}
  return {
    account: toAccount(record.account),
    enforced: record.enforced === true,
    creditsPerWrite: asNumber(record.credits_per_write, 1),
    freeMonthly: asNumber(record.free_monthly),
    usage: toUsage(record.usage),
  }
}

/**
 * A month of spending, grouped by team, user, workflow, job or tag.
 *
 * Your own team is always readable; the whole runner needs `credits:Read`, and
 * asking for somebody else's account without it is refused rather than quietly
 * answered about yourself.
 */
export async function getSpendBreakdown(
  baseUrl: string = DEFAULT_RUNNER_URL,
  options: { groupBy?: SpendGroupBy; period?: string; accountId?: string } = {},
  token?: string,
  signal?: AbortSignal,
): Promise<SpendBreakdown> {
  const params = new URLSearchParams()
  params.set('group_by', options.groupBy ?? 'workflow')
  if (options.period) params.set('period', options.period)
  if (options.accountId) params.set('account_id', options.accountId)
  const payload = await request(
    baseUrl,
    `/credits/usage?${params.toString()}`,
    { method: 'GET' },
    token,
    signal,
  )
  const record = isRecord(payload) ? payload : {}
  return {
    period: asString(record.period),
    groupBy: (asString(record.group_by, 'workflow') as SpendGroupBy),
    scope: asString(record.scope, 'all'),
    total: toGroup(record.total),
    groups: Array.isArray(record.groups) ? record.groups.map(toGroup) : [],
    overlapping: record.overlapping === true,
  }
}

/**
 * Spending month by month, oldest first.
 *
 * One month says how much; only the series says whether that is normal, which is
 * the question somebody looking at a bill actually has.
 */
export async function getSpendTimeline(
  baseUrl: string = DEFAULT_RUNNER_URL,
  options: { months?: number; accountId?: string } = {},
  token?: string,
  signal?: AbortSignal,
): Promise<SpendTimeline> {
  const params = new URLSearchParams()
  params.set('months', String(options.months ?? 6))
  if (options.accountId) params.set('account_id', options.accountId)
  const payload = await request(
    baseUrl,
    `/credits/timeline?${params.toString()}`,
    { method: 'GET' },
    token,
    signal,
  )
  const record = isRecord(payload) ? payload : {}
  return {
    scope: asString(record.scope, 'all'),
    periods: Array.isArray(record.periods) ? record.periods.map(toPeriod) : [],
  }
}

function toPeriod(value: unknown): SpendPeriod {
  const record = isRecord(value) ? value : {}
  return {
    period: asString(record.period),
    writes: asNumber(record.writes),
    charged: asNumber(record.charged),
    waived: asNumber(record.waived),
    runs: asNumber(record.runs),
  }
}

/** Every account. Needs `credits:Read`. */
export async function listCreditAccounts(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<CreditAccount[]> {
  const payload = await request(baseUrl, '/credits', { method: 'GET' }, token, signal)
  return Array.isArray(payload) ? payload.map(toAccount) : []
}

/** What an account was charged, newest first. Your own needs no permission. */
export async function getCreditLedger(
  baseUrl: string = DEFAULT_RUNNER_URL,
  accountId: string,
  limit = 50,
  token?: string,
  signal?: AbortSignal,
): Promise<CreditEntry[]> {
  const payload = await request(
    baseUrl,
    `/credits/${encodeURIComponent(accountId)}/ledger?limit=${encodeURIComponent(String(limit))}`,
    { method: 'GET' },
    token,
    signal,
  )
  return Array.isArray(payload) ? payload.map(toEntry) : []
}

/** Adds credits, or takes them back with a negative amount. Needs `credits:Manage`. */
export async function grantCredits(
  baseUrl: string = DEFAULT_RUNNER_URL,
  accountId: string,
  amount: number,
  note?: string,
  token?: string,
  signal?: AbortSignal,
): Promise<CreditAccount> {
  return toAccount(
    await request(
      baseUrl,
      `/credits/${encodeURIComponent(accountId)}/grant`,
      { method: 'POST', body: JSON.stringify({ amount, note: note ?? null }) },
      token,
      signal,
    ),
  )
}
