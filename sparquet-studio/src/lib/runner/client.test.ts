import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  DEFAULT_RUNNER_URL,
  RunnerError,
  checkRunnerHealth,
  fetchCapabilities,
  isRunnerError,
  RUNNER_TOKEN_HEADER,
  runPipelineStream,
  runJob,
  validateJob,
  type PipelineStreamHandlers,
} from '@/lib/runner/client'
import type { PipelineSpec } from '@/types/pipeline'
import type { PipelineRunResult, PipelineStageResult, RunLogLine } from '@/types/studio'

const fetchMock = vi.fn()

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}

/** An SSE response built from raw text, optionally split across chunks. */
function sseResponse(chunks: string[], status = 200): Response {
  const encoder = new TextEncoder()
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk))
      controller.close()
    },
  })
  return new Response(stream, { status, headers: { 'content-type': 'text/event-stream' } })
}

function frame(event: string, data: unknown): string {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`
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

describe('validateJob', () => {
  it('forwards the pipeline and returns the parse error', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ valid: false, error: "KeyError: 'name'" }))

    const result = await validateJob(DEFAULT_RUNNER_URL, PIPELINE)

    expect(result).toEqual({ valid: false, error: "KeyError: 'name'" })
    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/validate`)
    expect(JSON.parse(String(init.body))).toEqual({ pipeline: PIPELINE })
  })
})

describe('runJob', () => {
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

    const result = await runJob(DEFAULT_RUNNER_URL, {
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

    const result = await runJob(DEFAULT_RUNNER_URL, { pipeline: PIPELINE })

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

    const result = await runJob(DEFAULT_RUNNER_URL, { pipeline: PIPELINE })

    expect(result.status).toBe('error')
    expect(result.error).toContain('Colunas inexistentes')
  })

  it('surfaces the server error body on a 500', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ detail: 'Cannot import sparquet: No module named pyspark' }, 500),
    )

    const error = await runJob(DEFAULT_RUNNER_URL, { pipeline: PIPELINE }).catch(
      (caught: unknown) => caught,
    )

    expect(isRunnerError(error)).toBe(true)
    expect((error as RunnerError).kind).toBe('http')
    expect((error as RunnerError).status).toBe(500)
    expect((error as RunnerError).message).toBe(
      'Cannot import sparquet: No module named pyspark',
    )
  })
})

describe('runPipelineStream', () => {
  const STAGES = [
    { id: 's1', name: 'Bronze', pipeline: PIPELINE },
    { id: 's2', name: 'Silver', pipeline: PIPELINE, params: { dt: '2026-01-01' } },
  ]

  /** Collects every callback so a test can assert the whole conversation. */
  function recorder() {
    const stageStarts: { index: number; id: string; name?: string }[] = []
    const logs: RunLogLine[] = []
    const stageResults: PipelineStageResult[] = []
    const errors: string[] = []
    let total: number | undefined
    let result: PipelineRunResult | undefined

    const handlers: PipelineStreamHandlers = {
      onStart: (value) => {
        total = value
      },
      onStageStart: (stage) => stageStarts.push(stage),
      onLog: (line) => logs.push(line),
      onStageResult: (stage) => stageResults.push(stage),
      onResult: (value) => {
        result = value
      },
      onError: (message) => errors.push(message),
    }

    return {
      handlers,
      stageStarts,
      logs,
      stageResults,
      errors,
      get total() {
        return total
      },
      get result() {
        return result
      },
    }
  }

  const stageResultPayload = (index: number, id: string, overrides: object = {}) => ({
    index,
    id,
    success: true,
    skipped: false,
    rows_read: 100,
    rows_written: 90,
    duration_ms: 1200,
    error: null,
    validations: [],
    output_metrics: [],
    ...overrides,
  })

  it('posts the stages in order and reports every event of a successful pipeline', async () => {
    const events = [
      frame('start', { pipeline: true, total: 2 }),
      frame('stage_start', { index: 0, id: 's1', name: 'Bronze' }),
      frame('log', {
        timestamp: '2026-08-17T10:00:00.000000+00:00',
        level: 'INFO',
        message: 'Leitura concluida',
        context: { linhas: 100 },
        source: 'pipeline',
        stage_id: 's1',
      }),
      frame('stage_result', stageResultPayload(0, 's1')),
      frame('stage_start', { index: 1, id: 's2', name: 'Silver' }),
      frame('stage_result', stageResultPayload(1, 's2', { rows_written: 88 })),
      frame('result', {
        success: true,
        duration_ms: 4300,
        stages: [
          stageResultPayload(0, 's1'),
          stageResultPayload(1, 's2', { rows_written: 88 }),
        ],
        preview: { columns: ['id'], rows: [[1]], truncated: false },
        error: null,
      }),
    ]
    // Deliberately split mid-frame: the parser must buffer partial frames.
    const joined = events.join('')
    const cut = Math.floor(joined.length / 3)
    fetchMock.mockResolvedValue(sseResponse([joined.slice(0, cut), joined.slice(cut)]))

    const spy = recorder()
    await runPipelineStream(
      DEFAULT_RUNNER_URL,
      { stages: STAGES, limit: 10, stopOnError: true },
      spy.handlers,
      undefined,
      'secret-token',
    )

    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/run/flow/stream`)
    expect(init.method).toBe('POST')
    const headers = init.headers as Record<string, string>
    expect(headers['content-type']).toBe('application/json')
    expect(headers[RUNNER_TOKEN_HEADER]).toBe('secret-token')
    expect(JSON.parse(String(init.body))).toEqual({
      stages: [
        { id: 's1', name: 'Bronze', pipeline: PIPELINE },
        { id: 's2', name: 'Silver', pipeline: PIPELINE, params: { dt: '2026-01-01' } },
      ],
      limit: 10,
      stop_on_error: true,
    })

    expect(spy.total).toBe(2)
    expect(spy.stageStarts).toEqual([
      { index: 0, id: 's1', name: 'Bronze' },
      { index: 1, id: 's2', name: 'Silver' },
    ])
    expect(spy.logs).toEqual([
      {
        ts: Date.parse('2026-08-17T10:00:00.000000+00:00'),
        level: 'info',
        message: 'Leitura concluida',
        source: 'pipeline',
        context: { linhas: 100 },
        stageId: 's1',
      },
    ])
    expect(spy.stageResults.map((stage) => [stage.id, stage.status])).toEqual([
      ['s1', 'success'],
      ['s2', 'success'],
    ])
    expect(spy.stageResults[0]).toMatchObject({
      index: 0,
      rowsRead: 100,
      rowsWritten: 90,
      durationMs: 1200,
    })
    expect(spy.errors).toEqual([])

    expect(spy.result).toMatchObject({
      status: 'success',
      durationMs: 4300,
      preview: { columns: ['id'], rows: [[1]], truncated: false },
    })
    expect(spy.result?.stages.map((stage) => stage.id)).toEqual(['s1', 's2'])
    expect(spy.result?.error).toBeUndefined()
  })

  it('maps a failing stage and leaves the stages after it unreported', async () => {
    const failure = stageResultPayload(1, 's2', {
      success: false,
      rows_written: 0,
      error: "AnalysisException: cannot resolve 'total'",
      validations: [{ type: 'not_null', passed: false, message: '3 nulls', failed_count: 3 }],
    })
    fetchMock.mockResolvedValue(
      sseResponse([
        frame('start', { pipeline: true, total: 2 }),
        frame('stage_result', stageResultPayload(0, 's1')),
        frame('stage_start', { index: 1, id: 's2' }),
        frame('stage_result', failure),
        frame('result', {
          success: false,
          duration_ms: 900,
          stages: [stageResultPayload(0, 's1'), failure],
          preview: null,
          error: 'Stage 2 failed',
        }),
      ]),
    )

    const spy = recorder()
    await runPipelineStream(DEFAULT_RUNNER_URL, { stages: STAGES }, spy.handlers)

    expect(spy.stageResults.map((stage) => stage.status)).toEqual(['success', 'error'])
    expect(spy.stageResults[1].error).toContain('cannot resolve')
    expect(spy.stageResults[1].validations).toEqual([
      { type: 'not_null', passed: false, message: '3 nulls', failedCount: 3 },
    ])
    expect(spy.result?.status).toBe('error')
    expect(spy.result?.error).toBe('Stage 2 failed')
    expect(spy.result?.preview).toBeUndefined()
    // Logs are streamed, never repeated inside the final payload.
    expect(spy.result?.logs).toEqual([])
  })

  it('reports a graceful stop_if_empty stage as skipped, not as a failure', async () => {
    fetchMock.mockResolvedValue(
      sseResponse([
        frame('stage_result', stageResultPayload(0, 's1', { skipped: true, rows_written: 0 })),
        frame('result', { success: true, duration_ms: 10, stages: [], preview: null }),
      ]),
    )

    const spy = recorder()
    await runPipelineStream(DEFAULT_RUNNER_URL, { stages: STAGES }, spy.handlers)

    expect(spy.stageResults[0].status).toBe('skipped')
    expect(spy.result?.status).toBe('success')
  })

  it('forwards a fatal error event and skips events it does not know', async () => {
    fetchMock.mockResolvedValue(
      sseResponse([
        frame('start', { pipeline: true, total: 1 }),
        frame('heartbeat', { at: 1 }),
        // No id: nothing on the canvas could be pinned to it, so it is dropped.
        frame('stage_start', { index: 0 }),
        frame('error', { error: 'Cannot import sparquet: No module named pyspark' }),
      ]),
    )

    const spy = recorder()
    await runPipelineStream(DEFAULT_RUNNER_URL, { stages: STAGES }, spy.handlers)

    expect(spy.stageStarts).toEqual([])
    expect(spy.errors).toEqual(['Cannot import sparquet: No module named pyspark'])
    expect(spy.result).toBeUndefined()
  })

  it('surfaces a refused run as an http RunnerError before any event', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ detail: "Missing or invalid 'x-sparquet-token' header." }, 401),
    )

    const spy = recorder()
    const error = await runPipelineStream(
      DEFAULT_RUNNER_URL,
      { stages: STAGES },
      spy.handlers,
    ).catch((caught: unknown) => caught)

    expect(isRunnerError(error)).toBe(true)
    expect((error as RunnerError).status).toBe(401)
    expect(spy.result).toBeUndefined()
  })

  it('explains a 404 as a runner without pipeline support', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ detail: 'Not Found' }, 404))

    const spy = recorder()
    const error = await runPipelineStream(
      DEFAULT_RUNNER_URL,
      { stages: STAGES },
      spy.handlers,
    ).catch((caught: unknown) => caught)

    expect((error as RunnerError).status).toBe(404)
    expect((error as RunnerError).message).toContain('/run/flow/stream')
  })

  it('reports an unreachable runner with the start command', async () => {
    fetchMock.mockRejectedValue(new TypeError('fetch failed'))

    const spy = recorder()
    const error = await runPipelineStream(
      DEFAULT_RUNNER_URL,
      { stages: STAGES },
      spy.handlers,
    ).catch((caught: unknown) => caught)

    expect((error as RunnerError).kind).toBe('unreachable')
    expect((error as RunnerError).message).toContain('uvicorn server.main:app --port 8787')
  })
})

describe('runner token', () => {
  it('sends the token header on run and validate, and omits it when unset', async () => {
    // A Response body can only be read once, so build a fresh one per call.
    fetchMock.mockImplementation(() => Promise.resolve(jsonResponse({ valid: true })))

    await validateJob(DEFAULT_RUNNER_URL, PIPELINE, undefined, 'secret-token')
    const withToken = fetchMock.mock.calls[0]?.[1] as RequestInit
    expect((withToken.headers as Record<string, string>)[RUNNER_TOKEN_HEADER]).toBe(
      'secret-token',
    )

    fetchMock.mockClear()
    await validateJob(DEFAULT_RUNNER_URL, PIPELINE)
    const withoutToken = fetchMock.mock.calls[0]?.[1] as RequestInit
    expect(RUNNER_TOKEN_HEADER in (withoutToken.headers as Record<string, string>)).toBe(false)
  })

  it('reports a 401 as an http error carrying the runner explanation', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({ detail: "Missing or invalid 'x-sparquet-token' header." }, 401),
    )

    const error = await runJob(DEFAULT_RUNNER_URL, { pipeline: PIPELINE }).catch(
      (caught: unknown) => caught,
    )

    expect(isRunnerError(error)).toBe(true)
    expect((error as RunnerError).status).toBe(401)
    expect((error as RunnerError).message).toContain('x-sparquet-token')
  })

  it('reads auth_required from health', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        status: 'ok',
        version: '0.2.0',
        spark_available: true,
        auth_required: true,
      }),
    )

    await expect(checkRunnerHealth()).resolves.toMatchObject({ authRequired: true })
  })
})
