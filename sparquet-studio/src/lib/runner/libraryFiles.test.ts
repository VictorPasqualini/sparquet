import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { DEFAULT_RUNNER_URL, RUNNER_TOKEN_HEADER, RunnerError } from '@/lib/runner/client'
import { listLibraryFiles, readLibraryFile } from '@/lib/runner/libraryFiles'

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

const LISTING = {
  root: '/home/ana/.local/share/sparquet/workspace',
  files: [
    { path: 'vendas/jobs/ingestao.json', name: 'ingestao', size: 812, modified: 1756400000 },
    { path: 'compras/limpeza.json', name: 'limpeza', size: 240, modified: 1756300000 },
  ],
}

beforeEach(() => {
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('listLibraryFiles', () => {
  it('reads the runnable files and the root they are relative to', async () => {
    fetchMock.mockResolvedValue(jsonResponse(LISTING))

    const listing = await listLibraryFiles(DEFAULT_RUNNER_URL, 'secret')

    expect(listing.root).toBe('/home/ana/.local/share/sparquet/workspace')
    expect(listing.files.map((file) => file.path)).toEqual([
      'vendas/jobs/ingestao.json',
      'compras/limpeza.json',
    ])
    expect(listing.files[0]).toEqual({
      path: 'vendas/jobs/ingestao.json',
      name: 'ingestao',
      size: 812,
      modified: 1756400000,
    })
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/workspace/files`)
    expect(init.method).toBe('GET')
    expect((init.headers as Record<string, string>)[RUNNER_TOKEN_HEADER]).toBe('secret')
  })

  it('drops a row it cannot make a runnable reference out of', async () => {
    // A stage needs the path; a row without one could only ever fail at run time.
    fetchMock.mockResolvedValue(
      jsonResponse({ root: '/library', files: [{ name: 'orphan' }, LISTING.files[0]] }),
    )

    const listing = await listLibraryFiles()

    expect(listing.files.map((file) => file.path)).toEqual(['vendas/jobs/ingestao.json'])
  })

  it('falls back to the path when the runner names no display name', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ root: '/library', files: [{ path: 'a/b.json' }] }),
    )

    const listing = await listLibraryFiles()

    expect(listing.files[0]).toEqual({ path: 'a/b.json', name: 'a/b.json', size: 0, modified: 0 })
  })

  it('reads an empty library as empty, not as a failure', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ root: '/library', files: [] }))

    await expect(listLibraryFiles()).resolves.toEqual({ root: '/library', files: [] })
  })

  it('reports a runner that is not running as unreachable', async () => {
    fetchMock.mockRejectedValue(new TypeError('Failed to fetch'))

    await expect(listLibraryFiles()).rejects.toMatchObject({ kind: 'unreachable' })
  })

  it('keeps the reason the runner gave for refusing', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: 'Missing permission workspace:Read' }, 403))

    await expect(listLibraryFiles()).rejects.toMatchObject({
      kind: 'http',
      status: 403,
      message: 'Missing permission workspace:Read',
    })
  })
})

describe('readLibraryFile', () => {
  it('returns the JSON as it is on disk', async () => {
    const pipeline = { name: 'ingestao', input: { format: 'csv', path: 'vendas.csv' } }
    fetchMock.mockResolvedValue(jsonResponse({ path: 'vendas/jobs/ingestao.json', pipeline }))

    const spec = await readLibraryFile(DEFAULT_RUNNER_URL, 'vendas/jobs/ingestao.json', 'secret')

    expect(spec).toEqual(pipeline)
    expect(lastCall()[0]).toBe(`${DEFAULT_RUNNER_URL}/workspace/files/vendas/jobs/ingestao.json`)
  })

  it('encodes each segment without turning the separators into text', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ path: 'x', pipeline: { name: 'x' } }))

    await readLibraryFile(DEFAULT_RUNNER_URL, 'vendas 2026/relatório final.json')

    expect(lastCall()[0]).toBe(
      `${DEFAULT_RUNNER_URL}/workspace/files/vendas%202026/relat%C3%B3rio%20final.json`,
    )
  })

  it('refuses a response that carries no pipeline', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ path: 'a.json' }))

    await expect(readLibraryFile(DEFAULT_RUNNER_URL, 'a.json')).rejects.toBeInstanceOf(RunnerError)
  })

  it('keeps the refusal of a bad path', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: 'Give a path relative to the library root.' }, 400))

    await expect(readLibraryFile(DEFAULT_RUNNER_URL, '../outside.json')).rejects.toMatchObject({
      status: 400,
      message: 'Give a path relative to the library root.',
    })
  })
})
