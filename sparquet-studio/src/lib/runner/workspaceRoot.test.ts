import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DEFAULT_RUNNER_URL, RUNNER_TOKEN_HEADER, RunnerError } from '@/lib/runner/client'
import { getWorkspaceRoot, setWorkspaceRoot } from '@/lib/runner/workspaceRoot'

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

const LOCATION = {
  root: '/home/ana/.local/share/sparquet/workspace',
  source: 'default',
  default: '/home/ana/.local/share/sparquet/workspace',
  settings_file: '/home/ana/.local/share/sparquet/studio.json',
  writable: true,
  inside_source_tree: false,
  locked: false,
}

beforeEach(() => {
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('getWorkspaceRoot', () => {
  it('reads the path and the reason it is that one', async () => {
    fetchMock.mockResolvedValue(jsonResponse(LOCATION))

    const location = await getWorkspaceRoot(DEFAULT_RUNNER_URL, 'secret')

    expect(location).toEqual({
      root: '/home/ana/.local/share/sparquet/workspace',
      source: 'default',
      default: '/home/ana/.local/share/sparquet/workspace',
      settingsFile: '/home/ana/.local/share/sparquet/studio.json',
      writable: true,
      insideSourceTree: false,
      locked: false,
    })
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/workspace/root`)
    expect(init.method).toBe('GET')
    expect((init.headers as Record<string, string>)[RUNNER_TOKEN_HEADER]).toBe('secret')
  })

  it('trims a trailing slash off the base URL', async () => {
    fetchMock.mockResolvedValue(jsonResponse(LOCATION))

    await getWorkspaceRoot('http://127.0.0.1:8000/')

    expect(lastCall()[0]).toBe('http://127.0.0.1:8000/workspace/root')
  })

  it('reports a runner that is locked by its environment', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ ...LOCATION, source: 'env', locked: true, root: '/srv/library' }),
    )

    const location = await getWorkspaceRoot()

    expect(location.source).toBe('env')
    expect(location.locked).toBe(true)
  })

  it('flags a library that is still inside a checkout', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ ...LOCATION, source: 'legacy', inside_source_tree: true }),
    )

    expect((await getWorkspaceRoot()).insideSourceTree).toBe(true)
  })

  it('turns an unreachable runner into a RunnerError', async () => {
    fetchMock.mockRejectedValue(new TypeError('Failed to fetch'))

    await expect(getWorkspaceRoot()).rejects.toMatchObject({ kind: 'unreachable' })
  })

  it('surfaces the runner’s own explanation of a refusal', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ detail: 'This action requires runner:Configure.' }, 403),
    )

    await expect(getWorkspaceRoot()).rejects.toMatchObject({
      message: 'This action requires runner:Configure.',
      status: 403,
    })
  })

  it('does not invent fields the runner left out', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ root: '/srv/library' }))

    const location = await getWorkspaceRoot()

    expect(location.root).toBe('/srv/library')
    expect(location.source).toBe('default')
    expect(location.locked).toBe(false)
  })
})

describe('setWorkspaceRoot', () => {
  it('sends the chosen directory', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ ...LOCATION, root: '/srv/library', source: 'settings' }),
    )

    const location = await setWorkspaceRoot(DEFAULT_RUNNER_URL, '/srv/library', 'secret')

    expect(location.root).toBe('/srv/library')
    expect(location.source).toBe('settings')
    const [, init] = lastCall()
    expect(init.method).toBe('PUT')
    expect(JSON.parse(String(init.body))).toEqual({ root: '/srv/library' })
  })

  it('sends null to go back to the default', async () => {
    fetchMock.mockResolvedValue(jsonResponse(LOCATION))

    const location = await setWorkspaceRoot(DEFAULT_RUNNER_URL, null)

    expect(JSON.parse(String(lastCall()[1].body))).toEqual({ root: null })
    expect(location.source).toBe('default')
  })

  it('keeps the refusal of a path inside the source tree readable', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse(
        { detail: 'That path is inside the source tree of this runner.' },
        400,
      ),
    )

    await expect(setWorkspaceRoot(DEFAULT_RUNNER_URL, '/opt/sparquet/x')).rejects.toBeInstanceOf(
      RunnerError,
    )
  })

  it('reports a malformed response rather than a half-read location', async () => {
    fetchMock.mockResolvedValue(
      new Response('not json', { status: 200, headers: { 'content-type': 'text/plain' } }),
    )

    await expect(setWorkspaceRoot(DEFAULT_RUNNER_URL, '/srv/library')).rejects.toMatchObject({
      kind: 'malformed',
    })
  })
})
