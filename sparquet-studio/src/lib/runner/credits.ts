/**
 * Client for execution credits (`GET /credits/me`, `POST /credits/{id}/grant`).
 *
 * Its own module rather than part of `client.ts` for the same reason `history.ts`
 * is: running a Job, reading past runs and asking what a run will cost are three
 * questions with three different failure modes. A runner whose Spark is broken
 * can still say what the balance is, and the balance is exactly what somebody
 * wants to know when a run has just been refused.
 */

import type { CreditAccount, CreditEntry, CreditStatus } from '@/types/credits'

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
    createdAt: asNullableString(record.created_at),
    updatedAt: asNullableString(record.updated_at),
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
    jobRunId: asNullableString(record.job_run_id),
    pipelineRunId: asNullableString(record.pipeline_run_id),
    target: asNullableString(record.target),
    jobName: asNullableString(record.job_name),
    note: asNullableString(record.note),
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
    creditsPerJob: asNumber(record.credits_per_job, 1),
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
