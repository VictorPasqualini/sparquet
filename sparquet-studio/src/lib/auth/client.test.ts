import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { issueRecovery, recoverPassword } from '@/lib/auth/client'
import { DEFAULT_RUNNER_URL, RUNNER_TOKEN_HEADER } from '@/lib/runner/client'

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

beforeEach(() => {
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('issueRecovery', () => {
  it('mints a code for one user and maps it', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        user_id: 'u1',
        username: 'ana',
        code: 'abc123',
        expires_at: '2026-08-20T11:30:00Z',
      }),
    )

    const recovery = await issueRecovery(DEFAULT_RUNNER_URL, 'u1', 'secret')

    expect(recovery).toEqual({
      userId: 'u1',
      username: 'ana',
      code: 'abc123',
      expiresAt: '2026-08-20T11:30:00Z',
    })
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/auth/users/u1/recovery`)
    expect(init.method).toBe('POST')
    expect((init.headers as Record<string, string>)[RUNNER_TOKEN_HEADER]).toBe('secret')
  })
})

describe('recoverPassword', () => {
  it('spends the code and the new password together', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ ok: true }))

    await recoverPassword(DEFAULT_RUNNER_URL, { code: 'abc123', password: 'longenough' })

    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/auth/recover`)
    expect(JSON.parse(String(init.body))).toEqual({ code: 'abc123', password: 'longenough' })
  })

  it('surfaces the runner refusal, which never says which half was wrong', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ detail: 'That recovery code is not usable.' }, 400),
    )

    await expect(
      recoverPassword(DEFAULT_RUNNER_URL, { code: 'stale', password: 'longenough' }),
    ).rejects.toThrow('That recovery code is not usable.')
  })
})
