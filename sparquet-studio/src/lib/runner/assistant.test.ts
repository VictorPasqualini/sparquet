import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  getAssistantInfo,
  getAssistUsage,
  streamAssistant,
  type AssistantStreamHandlers,
} from '@/lib/runner/assistant'
import { DEFAULT_RUNNER_URL, RUNNER_TOKEN_HEADER } from '@/lib/runner/client'

const fetchMock = vi.fn()

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}

/** An SSE body, written the way the runner writes it. */
function sseResponse(frames: { event: string; data: unknown }[]): Response {
  const text = frames
    .map((frame) => `event: ${frame.event}\ndata: ${JSON.stringify(frame.data)}\n\n`)
    .join('')
  return new Response(text, {
    status: 200,
    headers: { 'content-type': 'text/event-stream' },
  })
}

function lastCall(): [string, RequestInit] {
  const call = fetchMock.mock.calls.at(-1)
  if (!call) throw new Error('fetch was not called')
  return call as [string, RequestInit]
}

function collect(): AssistantStreamHandlers & {
  text: () => string
  tools: string[]
  usage: unknown
} {
  const chunks: string[] = []
  const tools: string[] = []
  const state = {
    text: () => chunks.join(''),
    tools,
    usage: undefined as unknown,
    onToken: (chunk: string) => chunks.push(chunk),
    onTool: (call: { name: string }) => tools.push(call.name),
    onUsage: (usage: unknown) => {
      state.usage = usage
    },
  }
  return state
}

beforeEach(() => {
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('getAssistantInfo', () => {
  it('maps what the runner can answer with', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        backend: 'ollama',
        available: true,
        local: true,
        model: 'qwen2.5-coder:7b',
        base_url: 'http://127.0.0.1:11434',
        models: ['qwen2.5-coder:7b', 'llama3.1:8b'],
        tools: ['list_formats', 'validate_config'],
        agent: '',
        version: '',
        hint: '',
        error: '',
      }),
    )

    const info = await getAssistantInfo(DEFAULT_RUNNER_URL, 'secret')

    expect(info.backend).toBe('ollama')
    expect(info.available).toBe(true)
    expect(info.local).toBe(true)
    expect(info.models).toEqual(['qwen2.5-coder:7b', 'llama3.1:8b'])
    expect(info.tools).toEqual(['list_formats', 'validate_config'])

    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/assistant`)
    expect((init.headers as Record<string, string>)[RUNNER_TOKEN_HEADER]).toBe('secret')
  })

  it('keeps the hint when the runner cannot answer, because it says what to do', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        backend: 'omnigent',
        available: false,
        local: true,
        error: 'Omnigent is not installed on this runner.',
        hint: '"pip install omnigent" (Python 3.12 or newer)',
      }),
    )

    const info = await getAssistantInfo()

    expect(info.available).toBe(false)
    expect(info.hint).toContain('pip install omnigent')
  })

  it('treats an older runner that says nothing as local rather than as billable', async () => {
    // A runner that does not answer the question has not answered "no", and
    // guessing "remote" would put a scary word next to a free setup.
    fetchMock.mockResolvedValue(jsonResponse({ backend: 'ollama', available: true }))

    expect((await getAssistantInfo()).local).toBe(true)
  })

  it('surfaces an unreachable runner as a runner error, not a parse failure', async () => {
    fetchMock.mockRejectedValue(new TypeError('Failed to fetch'))

    await expect(getAssistantInfo()).rejects.toThrow(/runner/i)
  })
})

describe('streamAssistant', () => {
  it('streams the text, the tools it ran and what the turn consumed', async () => {
    fetchMock.mockResolvedValue(
      sseResponse([
        { event: 'delta', data: { text: 'The ' } },
        { event: 'tool', data: { name: 'list_formats', args: {} } },
        { event: 'delta', data: { text: 'formats are…' } },
        {
          event: 'done',
          data: {
            usage: {
              model: 'qwen2.5-coder:7b',
              provider: 'ollama',
              local: true,
              inputTokens: 120,
              outputTokens: 40,
              toolCalls: 1,
              durationMs: 2500,
            },
          },
        },
      ]),
    )

    const handlers = collect()
    await streamAssistant(
      { messages: [{ role: 'user', content: 'which formats?' }] },
      DEFAULT_RUNNER_URL,
      'secret',
      handlers,
    )

    expect(handlers.text()).toBe('The formats are…')
    expect(handlers.tools).toEqual(['list_formats'])
    expect(handlers.usage).toMatchObject({ local: true, inputTokens: 120, toolCalls: 1 })
  })

  it('sends only the transcript, the model and what the question is about', async () => {
    fetchMock.mockResolvedValue(sseResponse([{ event: 'done', data: {} }]))

    await streamAssistant(
      {
        messages: [{ role: 'user', content: 'hi' }],
        model: 'llama3.1:8b',
        workflowId: 'w1',
      },
      DEFAULT_RUNNER_URL,
      'secret',
    )

    const [url, init] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/assistant/stream`)
    expect(JSON.parse(String(init.body))).toEqual({
      messages: [{ role: 'user', content: 'hi' }],
      model: 'llama3.1:8b',
      workflow_id: 'w1',
    })
  })

  it('throws what the error frame said, so a half-written answer explains itself', async () => {
    fetchMock.mockResolvedValue(
      sseResponse([
        { event: 'delta', data: { text: 'Let me check' } },
        { event: 'error', data: { message: 'Ollama is not answering.', hint: 'Start it.' } },
      ]),
    )

    const handlers = collect()
    await expect(
      streamAssistant({ messages: [{ role: 'user', content: 'q' }] }, undefined, '', handlers),
    ).rejects.toThrow('Ollama is not answering. Start it.')
    // What arrived before the failure is still the caller's to keep.
    expect(handlers.text()).toBe('Let me check')
  })

  it('skips frames it does not understand instead of failing on them', async () => {
    fetchMock.mockResolvedValue(
      sseResponse([
        { event: 'thinking', data: { text: 'hmm' } },
        { event: 'delta', data: { text: 'answer' } },
        { event: 'done', data: {} },
      ]),
    )

    const handlers = collect()
    await streamAssistant({ messages: [] }, undefined, '', handlers)

    expect(handlers.text()).toBe('answer')
  })

  it('reports an HTTP failure with what the runner said', async () => {
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({ detail: 'assistant:Ask is not granted to viewer.' }), {
        status: 403,
      }),
    )

    await expect(streamAssistant({ messages: [] })).rejects.toThrow(/assistant:Ask/)
  })
})

describe('getAssistUsage', () => {
  const SUMMARY = {
    period: '2026-09',
    scope: 't1',
    turns: 3,
    local_turns: 2,
    remote_turns: 1,
    input_tokens: 300,
    output_tokens: 120,
    tool_calls: 4,
    charged: 1,
    seconds: 7,
    recent: [
      {
        id: 'a1',
        period: '2026-09',
        backend: 'ollama',
        provider: 'ollama',
        model: 'qwen2.5-coder:7b',
        local: true,
        input_tokens: 100,
        output_tokens: 40,
        tool_calls: 2,
        duration_ms: 2500,
        amount: 0,
        created_at: '2026-09-16T10:00:00Z',
        actor: 'ana',
        workflow_id: null,
      },
    ],
  }

  it('reads the free turns and the charged ones apart', async () => {
    fetchMock.mockResolvedValue(jsonResponse(SUMMARY))

    const summary = await getAssistUsage(DEFAULT_RUNNER_URL, 'secret', { period: '2026-09' })

    expect(summary.turns).toBe(3)
    expect(summary.localTurns).toBe(2)
    expect(summary.remoteTurns).toBe(1)
    expect(summary.charged).toBe(1)
    expect(summary.recent[0]).toMatchObject({ model: 'qwen2.5-coder:7b', amount: 0, actor: 'ana' })

    const [url] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/credits/assist?period=2026-09`)
  })

  it('asks for one account only when told to', async () => {
    fetchMock.mockResolvedValue(jsonResponse(SUMMARY))

    await getAssistUsage(DEFAULT_RUNNER_URL, '', { accountId: 't2', limit: 5 })

    const [url] = lastCall()
    expect(url).toBe(`${DEFAULT_RUNNER_URL}/credits/assist?account_id=t2&limit=5`)
  })

  it('survives a runner that answers with less than it used to', async () => {
    fetchMock.mockResolvedValue(jsonResponse({ period: '2026-09' }))

    const summary = await getAssistUsage()

    expect(summary.turns).toBe(0)
    expect(summary.recent).toEqual([])
  })
})
