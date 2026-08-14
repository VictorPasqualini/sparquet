import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  DEFAULT_RUNNER_URL,
  RunnerError,
  checkRunnerHealth,
  fetchCapabilities,
  isRunnerError,
  RUNNER_TOKEN_HEADER,
  runPipeline,
  validatePipeline,
} from '@/lib/runner/client'
import type { PipelineSpec } from '@/types/pipeline'

const fetchMock = vi.fn()

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}

const PIPELINE: PipelineSpec = {
  name: 'demo',
  input: { format: 'csv', path: '/data/in' },
  output: { format: 'parquet', path: '/data/out' },
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

describe('checkRunnerHealth', () => {
  it('maps a healthy response', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        status: 'ok',
        version: '0.1.0',
        spark_available: true,
        framework_version: '0.2.3',
      }),
    )

    const health = await checkRunnerHealth()

    expect(health).toEqual({
      authRequired: false,
      status: 'ok',
      version: '0.1.0',
      sparkAvailable: true,
      frameworkVersion: '0.2.3',
    })
    expect(lastCall()[0]).toBe(`${DEFAULT_RUNNER_URL}/health`)
  })

  it('strips a trailing slash from the base url', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ status: 'degraded', version: '0.1.0' }))

    const health = await checkRunnerHealth('http://127.0.0.1:9000/')

    expect(lastCall()[0]).toBe('http://127.0.0.1:9000/health')
    expect(health.sparkAvailable).toBe(false)
    expect(health.frameworkVersion).toBeUndefined()
  })

  it('reports a friendly error when the runner is unreachable', async () => {
    fetchMock.mockRejectedValue(new TypeError('fetch failed'))

    const error = await checkRunnerHealth().catch((caught: unknown) => caught)

    expect(isRunnerError(error)).toBe(true)
    expect((error as RunnerError).kind).toBe('unreachable')
    expect((error as RunnerError).message).toContain('Local runner not detected')
    expect((error as RunnerError).message).toContain('uvicorn server.main:app --port 8787')
  })
})

describe('fetchCapabilities', () => {
  it('returns the live registries', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        transformations: ['filter', 'join'],
        readers: ['csv', 'delta'],
        writers: ['csv', 'kafka'],
        validators: ['not_null'],
      }),
    )

    await expect(fetchCapabilities()).resolves.toEqual({
      transformations: ['filter', 'join'],
      readers: ['csv', 'delta'],
      writers: ['csv', 'kafka'],
      validators: ['not_null'],
    })
  })
})

describe('validatePipeline', () => {
  it('forwards the pipeline and returns the parse error', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ valid: false, error: "KeyError: 'name'" }))

    const result = await validatePipeline(DEFAULT_RUNNER_URL, PIPELINE)

    expect(result).toEqual({ valid: false, error: "KeyError: 'name'" })
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/validate`)
    expect(JSON.parse(String(init.body))).toEqual({ pipeline: PIPELINE })
  })
})

describe('runPipeline', () => {
  it('maps a successful run into a RunResult', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        success: true,
        skipped: false,
        pipeline_name: 'demo',
        rows_read: 120,
        rows_written: 118,
        duration_ms: 4210,
        error: null,
        validations: [
          { type: 'not_null', passed: false, message: '3 nulls in id', failed_count: 3 },
        ],
        preview: { columns: ['id', 'name'], rows: [[1, 'a']], truncated: true },
        logs: [
          {
            timestamp: '2026-08-13T14:03:11.482913+00:00',
            level: 'INFO',
            message: 'Pipeline concluido',
            context: { pipeline: 'demo', linhas_escritas: 118 },
          },
          { timestamp: 'not-a-date', level: 'WARNING', message: 'deferred', context: {} },
        ],
      }),
    )

    const result = await runPipeline(DEFAULT_RUNNER_URL, {
      pipeline: PIPELINE,
      params: { tipo_ativo: 'NC', ids: ['A1', 'A2'], aplicar_join: true },
      limit: 10,
    })

    expect(result.status).toBe('success')
    expect(result.pipelineName).toBe('demo')
    expect(result.rowsRead).toBe(120)
    expect(result.rowsWritten).toBe(118)
    expect(result.durationMs).toBe(4210)
    expect(result.error).toBeUndefined()
    expect(result.validations).toEqual([
      { type: 'not_null', passed: false, message: '3 nulls in id', failedCount: 3 },
    ])
    expect(result.preview).toEqual({
      columns: ['id', 'name'],
      rows: [[1, 'a']],
      truncated: true,
    })
    expect(result.logs).toHaveLength(2)
    expect(result.logs[0]).toEqual({
      ts: Date.parse('2026-08-13T14:03:11.482913+00:00'),
      level: 'info',
      message: 'Pipeline concluido',
      context: { pipeline: 'demo', linhas_escritas: 118 },
    })
    expect(result.logs[1].level).toBe('warning')
    expect(Number.isNaN(result.logs[1].ts)).toBe(false)

    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/run`)
    expect(init.method).toBe('POST')
    expect(JSON.parse(String(init.body))).toEqual({
      pipeline: PIPELINE,
      params: { tipo_ativo: 'NC', ids: ['A1', 'A2'], aplicar_join: true },
      limit: 10,
    })
  })

  it('reports a graceful stop as skipped', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ success: true, skipped: true, rows_read: 0, rows_written: 0, logs: [] }),
    )

    const result = await runPipeline(DEFAULT_RUNNER_URL, { pipeline: PIPELINE })

    expect(result.status).toBe('skipped')
    expect(result.skipped).toBe(true)
    expect(result.preview).toBeUndefined()
    expect(result.logs).toEqual([])
  })

  it('maps a failed run into an error RunResult', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        success: false,
        skipped: false,
        error: "ValueError: Colunas inexistentes no output '/out'",
        logs: [],
      }),
    )

    const result = await runPipeline(DEFAULT_RUNNER_URL, { pipeline: PIPELINE })

    expect(result.status).toBe('error')
    expect(result.error).toContain('Colunas inexistentes')
  })

  it('surfaces the server error body on a 500', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ detail: 'Cannot import spark_framework: No module named pyspark' }, 500),
    )

    const error = await runPipeline(DEFAULT_RUNNER_URL, { pipeline: PIPELINE }).catch(
      (caught: unknown) => caught,
    )

    expect(isRunnerError(error)).toBe(true)
    expect((error as RunnerError).kind).toBe('http')
    expect((error as RunnerError).status).toBe(500)
    expect((error as RunnerError).message).toBe(
      'Cannot import spark_framework: No module named pyspark',
    )
  })
})

describe('runner token', () => {
  it('sends the token header on run and validate, and omits it when unset', async () => {
    // A Response body can only be read once, so build a fresh one per call.
    fetchMock.mockImplementation(() => Promise.resolve(jsonResponse({ valid: true })))

    await validatePipeline(DEFAULT_RUNNER_URL, PIPELINE, undefined, 'secret-token')
    const withToken = fetchMock.mock.calls[0]?.[1] as RequestInit
    expect((withToken.headers as Record<string, string>)[RUNNER_TOKEN_HEADER]).toBe(
      'secret-token',
    )

    fetchMock.mockClear()
    await validatePipeline(DEFAULT_RUNNER_URL, PIPELINE)
    const withoutToken = fetchMock.mock.calls[0]?.[1] as RequestInit
    expect(RUNNER_TOKEN_HEADER in (withoutToken.headers as Record<string, string>)).toBe(false)
  })

  it('reports a 401 as an http error carrying the runner explanation', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ detail: "Missing or invalid 'x-sparquet-token' header." }, 401),
    )

    const error = await runPipeline(DEFAULT_RUNNER_URL, { pipeline: PIPELINE }).catch(
      (caught: unknown) => caught,
    )

    expect(isRunnerError(error)).toBe(true)
    expect((error as RunnerError).status).toBe(401)
    expect((error as RunnerError).message).toContain('x-sparquet-token')
  })

  it('reads auth_required from health', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ status: 'ok', version: '0.2.0', spark_available: true, auth_required: true }),
    )

    await expect(checkRunnerHealth()).resolves.toMatchObject({ authRequired: true })
  })
})
