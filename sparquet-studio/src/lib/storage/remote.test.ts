import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { keyOf, KEY } from '@/lib/storage/keys'
import { workspaceBackend } from '@/lib/storage/remote'

const fetchMock = vi.fn()

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}

const SNAPSHOT = {
  root: '/repo/sparquet-workspace',
  workflows: [{ kind: 'workflow', id: 'w1', record: { id: 'w1', name: 'Vendas' }, path: null }],
  jobs: [
    {
      kind: 'job',
      id: 'j1',
      record: { id: 'j1', workflowId: 'w1', name: 'Ingestão' },
      path: 'vendas/jobs/ingestao.json',
    },
  ],
  pipelines: [],
  meta: { seeded: true, version: 4 },
}

function calls(): [string, RequestInit][] {
  return fetchMock.mock.calls as [string, RequestInit][]
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

describe('workspaceBackend', () => {
  it('serves reads from the snapshot it loaded on connect', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(SNAPSHOT))

    const backend = await workspaceBackend({ baseUrl: 'http://127.0.0.1:8765' })
    expect(backend).not.toBeNull()
    expect(backend?.kind).toBe('workspace')

    // One request for the whole library: the editor cannot wait on a round trip
    // per record it draws.
    expect(calls()).toHaveLength(1)
    expect(await backend?.get(keyOf('job', 'j1'))).toEqual(SNAPSHOT.jobs[0].record)
    expect(await backend?.get(keyOf('workflow', 'w1'))).toEqual(SNAPSHOT.workflows[0].record)
    expect(calls()).toHaveLength(1)
  })

  it('brings the meta keys back as storage keys', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(SNAPSHOT))
    const backend = await workspaceBackend({})

    // Why they travel with the library: a second checkout must not re-seed or
    // re-migrate a library that is already current.
    expect(await backend?.get(KEY.seeded)).toBe(true)
    expect(await backend?.get(KEY.version)).toBe(4)
  })

  it('is unavailable rather than broken when nothing answers', async () => {
    fetchMock.mockRejectedValueOnce(new TypeError('Failed to fetch'))
    expect(await workspaceBackend({})).toBeNull()
  })

  it('is unavailable when the runner is too old to know the route', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse({ detail: 'Not Found' }, 404))
    expect(await workspaceBackend({})).toBeNull()
  })

  it('sends the token on every call once it has one', async () => {
    fetchMock.mockResolvedValue(jsonResponse(SNAPSHOT))
    const backend = await workspaceBackend({ token: 'secret' })
    await backend?.set(keyOf('workflow', 'w1'), { id: 'w1', name: 'Vendas BR' })

    for (const [, init] of calls()) {
      expect((init.headers as Record<string, string>)['x-sparquet-token']).toBe('secret')
    }
  })

  it('writes a record through to its own route and caches only after that', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(SNAPSHOT))
    const backend = await workspaceBackend({ baseUrl: 'http://127.0.0.1:8765/' })

    fetchMock.mockResolvedValueOnce(jsonResponse({ kind: 'workflow', id: 'w2', record: {} }))
    await backend?.set(keyOf('workflow', 'w2'), { id: 'w2', name: 'Compras' })

    const [url, init] = lastCall()
    expect(url).toBe('http://127.0.0.1:8765/workspace/workflow/w2')
    expect(init.method).toBe('PUT')
    expect(JSON.parse(String(init.body))).toEqual({
      record: { id: 'w2', name: 'Compras' },
      config: null,
    })
    expect(await backend?.get(keyOf('workflow', 'w2'))).toEqual({ id: 'w2', name: 'Compras' })
  })

  it('keeps a refused write out of the cache', async () => {
    // Otherwise the editor shows a value the files do not have — the one failure
    // mode a file-backed library cannot afford.
    fetchMock.mockResolvedValueOnce(jsonResponse(SNAPSHOT))
    const backend = await workspaceBackend({})

    fetchMock.mockResolvedValueOnce(jsonResponse({ detail: 'id must not be a path' }, 400))
    await expect(backend?.set(keyOf('workflow', 'w2'), { id: 'w2' })).rejects.toThrow(
      /id must not be a path/,
    )
    expect(await backend?.get(keyOf('workflow', 'w2'))).toBeUndefined()
  })

  it('compiles a Job so the readable file is the pipeline the framework runs', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(SNAPSHOT))
    const backend = await workspaceBackend({})

    fetchMock.mockResolvedValueOnce(jsonResponse({ kind: 'job', id: 'j2', record: {} }))
    await backend?.set(keyOf('job', 'j2'), {
      id: 'j2',
      workflowId: 'w1',
      name: 'Ingestão',
      settings: { name: 'orders', description: '' },
      graph: { nodes: [], edges: [] },
      params: [],
    })

    const body = JSON.parse(String(lastCall()[1].body)) as { config: unknown }
    // A graph with no source does not compile, and a half-built job must still save.
    expect('config' in body).toBe(true)
  })

  it('routes a meta key to the meta endpoint, not to a record', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(SNAPSHOT))
    const backend = await workspaceBackend({ baseUrl: 'http://127.0.0.1:8765' })

    fetchMock.mockResolvedValueOnce(jsonResponse({ ok: true }))
    await backend?.set(KEY.seeded, true)

    const [url, init] = lastCall()
    expect(url).toBe('http://127.0.0.1:8765/workspace/meta/seeded')
    expect(JSON.parse(String(init.body))).toEqual({ value: true })
  })

  it('deletes a record on the server and forgets it locally', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(SNAPSHOT))
    const backend = await workspaceBackend({})

    fetchMock.mockResolvedValueOnce(jsonResponse({ deleted: true }))
    await backend?.del(keyOf('job', 'j1'))

    expect(lastCall()[1].method).toBe('DELETE')
    expect(await backend?.get(keyOf('job', 'j1'))).toBeUndefined()
  })

  it('keeps the migration backup in memory instead of writing it to the repository', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(SNAPSHOT))
    const backend = await workspaceBackend({})

    await backend?.set(KEY.backup, { records: [] })

    // A copy of the whole library, committed for one session's rollback, is not
    // something anyone should find in a diff.
    expect(calls()).toHaveLength(1)
    expect(await backend?.get(KEY.backup)).toEqual({ records: [] })
  })

  it('lists the keys it holds under a prefix', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(SNAPSHOT))
    const backend = await workspaceBackend({})

    expect(await backend?.keys(keyOf('job', ''))).toEqual([keyOf('job', 'j1')])
  })
})
