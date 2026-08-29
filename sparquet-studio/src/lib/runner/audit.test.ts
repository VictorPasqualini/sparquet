import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { isForbidden, listAuditEvents } from '@/lib/runner/audit'
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

const EVENT = {
  id: 'a1',
  at: '2026-08-29T14:03:11Z',
  actor: 'ana',
  actor_id: 'u1',
  team: 'platform',
  roles: ['admin'],
  action: 'iam:CreateUser',
  method: 'POST',
  path: '/auth/users',
  resource: 'u2',
  outcome: 'allowed',
  status: 200,
  detail: { username: 'bruno' },
  ip: '127.0.0.1',
}

beforeEach(() => {
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('listAuditEvents', () => {
  it('maps an event and sends the token', async () => {
    fetchMock.mockResolvedValue(jsonResponse([EVENT]))

    const events = await listAuditEvents(DEFAULT_RUNNER_URL, {}, 'secret')

    expect(events).toEqual([
      {
        id: 'a1',
        at: '2026-08-29T14:03:11Z',
        actor: 'ana',
        actorId: 'u1',
        team: 'platform',
        roles: ['admin'],
        action: 'iam:CreateUser',
        method: 'POST',
        path: '/auth/users',
        resource: 'u2',
        outcome: 'allowed',
        status: 200,
        detail: { username: 'bruno' },
        ip: '127.0.0.1',
      },
    ])
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/audit?limit=100`)
    expect((init.headers as Record<string, string>)[RUNNER_TOKEN_HEADER]).toBe('secret')
  })

  it('passes the filters the panel offers, and only those it was given', async () => {
    fetchMock.mockResolvedValue(jsonResponse([]))

    await listAuditEvents(DEFAULT_RUNNER_URL, {
      limit: 25,
      action: 'iam:*',
      outcome: 'denied',
      actorId: 'u1',
    })

    const [url] = lastCall()
    expect(url).toBe(
      `${DEFAULT_RUNNER_URL}/audit?limit=25&actor_id=u1&outcome=denied&action=iam%3A*`,
    )
  })

  it('reads a refusal as an anonymous event rather than crashing on it', async () => {
    // A row written before the caller had a session names no user; the panel
    // still has to render it, because a denied request is the whole point.
    fetchMock.mockResolvedValue(
      jsonResponse([{ id: 'a2', at: '2026-08-29T14:04:00Z', outcome: 'denied', status: 403 }]),
    )

    const [event] = await listAuditEvents()

    expect(event.actor).toBe('')
    expect(event.actorId).toBeNull()
    expect(event.roles).toEqual([])
    expect(event.detail).toBeNull()
    expect(event.outcome).toBe('denied')
  })

  it('defaults an event with no outcome to allowed', async () => {
    fetchMock.mockResolvedValue(jsonResponse([{ id: 'a3' }]))
    expect((await listAuditEvents())[0].outcome).toBe('allowed')
  })

  it('treats a non-array payload as no events', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: 'nope' }))
    expect(await listAuditEvents()).toEqual([])
  })
})

describe('isForbidden', () => {
  it('separates "you may not read this" from every other failure', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: 'needs iam:ReadAudit' }, 403))
    const refused = await listAuditEvents().catch((error: unknown) => error)
    expect(isForbidden(refused)).toBe(true)
    expect((refused as Error).message).toBe('needs iam:ReadAudit')

    fetchMock.mockResolvedValue(jsonResponse({ detail: 'boom' }, 500))
    const broken = await listAuditEvents().catch((error: unknown) => error)
    expect(isForbidden(broken)).toBe(false)

    expect(isForbidden(new Error('offline'))).toBe(false)
  })
})
