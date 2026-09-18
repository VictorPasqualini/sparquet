import { afterEach, describe, expect, it, vi } from 'vitest'

import { sendAiRequest } from '@/lib/ai/client'
import type { AiProviderId, AiSettings } from '@/types/ai'

const KEY = 'secret-key-1234'

interface Call {
  url: string
  headers: Record<string, string>
  body: string
}

function stubFetch(body: string, init?: ResponseInit): Call[] {
  const calls: Call[] = []
  vi.stubGlobal('fetch', (input: RequestInfo | URL, request?: RequestInit) => {
    const headers = new Headers(request?.headers)
    calls.push({
      url: String(input),
      headers: Object.fromEntries(headers.entries()),
      body: typeof request?.body === 'string' ? request.body : '',
    })
    return Promise.resolve(new Response(body, init))
  })
  return calls
}

const settingsFor = (provider: AiProviderId): AiSettings => ({
  provider,
  model: '',
  baseUrl: '',
  apiKey: KEY,
  temperature: 0.2,
  maxTokens: 1024,
  shareJobContext: false,
})

const ask = (provider: AiProviderId) =>
  sendAiRequest({
    settings: settingsFor(provider),
    system: 'system',
    messages: [{ role: 'user', content: 'hi' }],
  })

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('sendAiRequest', () => {
  it('sends the Google key as a header and never in the URL', async () => {
    const calls = stubFetch('data: {"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}\n\n')

    const response = await ask('google')

    expect(response.text).toBe('ok')
    expect(calls[0].url).toBe(
      'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse',
    )
    expect(calls[0].url).not.toContain(KEY)
    expect(calls[0].headers['x-goog-api-key']).toBe(KEY)
  })

  it('reports a 200 whose body is a JSON error envelope instead of an empty reply', async () => {
    stubFetch(JSON.stringify({ error: { message: 'context length exceeded' } }))

    await expect(ask('openai')).rejects.toThrow(/context length exceeded/)
  })

  it('keeps the answer when a gateway ignores stream and returns the whole completion', async () => {
    stubFetch(JSON.stringify({ choices: [{ message: { content: 'the answer' } }] }))

    await expect(ask('openai')).resolves.toMatchObject({ text: 'the answer' })
  })

  it('recovers a non-stream Anthropic message body', async () => {
    stubFetch(JSON.stringify({ content: [{ type: 'text', text: 'the answer' }] }))

    await expect(ask('anthropic')).resolves.toMatchObject({ text: 'the answer' })
  })

  it('reports a 200 that is not an event stream at all', async () => {
    stubFetch('<html>not an api</html>')

    await expect(ask('openai')).rejects.toThrow(/not an event stream/)
  })

  it('reports a streamed error that is a bare string', async () => {
    stubFetch('data: {"error":"quota exhausted"}\n\n')

    await expect(ask('openai')).rejects.toThrow(/quota exhausted/)
  })

  it('keeps an empty streamed reply as an empty success', async () => {
    stubFetch('data: {"choices":[{"delta":{}}]}\n\ndata: [DONE]\n\n')

    await expect(ask('openai')).resolves.toMatchObject({ text: '' })
  })

  it('never echoes the key back in a provider error', async () => {
    stubFetch(JSON.stringify({ error: { message: `bad key ${KEY}` } }), { status: 401 })

    await expect(ask('anthropic')).rejects.toThrow(/\[redacted\]/)
  })

  it('sends the caller prompt to the runner as instructions, never as the prompt', async () => {
    const calls = stubFetch('event: delta\ndata: {"text":"ok"}\n\nevent: done\ndata: {}\n\n')

    const response = await sendAiRequest({
      settings: settingsFor('runner'),
      system: 'answer with a JSON envelope',
      messages: [{ role: 'user', content: 'hi' }],
      runner: { baseUrl: 'http://127.0.0.1:8787', token: 'runner-token' },
    })

    expect(response.text).toBe('ok')
    expect(calls[0].url).toBe('http://127.0.0.1:8787/assistant/stream')
    // The runner's own prompt describes the tools that Python environment has,
    // which a browser cannot know — so the caller's rides on top of it, under its
    // own key, instead of replacing it.
    const body = JSON.parse(calls[0].body) as Record<string, unknown>
    expect(body.instructions).toBe('answer with a JSON envelope')
    expect(body.system).toBeUndefined()
    expect(JSON.stringify(calls[0].headers)).not.toContain(KEY)
    expect(calls[0].headers['x-sparquet-token']).toBe('runner-token')
  })

  it('answers whether the runner turn was free, which no other provider can', async () => {
    stubFetch('event: done\ndata: {"usage":{"local":true,"inputTokens":9,"outputTokens":2}}\n\n')

    const response = await sendAiRequest({
      settings: settingsFor('runner'),
      system: 'system',
      messages: [{ role: 'user', content: 'hi' }],
      runner: { baseUrl: 'http://127.0.0.1:8787', token: '' },
    })

    expect(response.local).toBe(true)
    expect(response.usage).toEqual({ inputTokens: 9, outputTokens: 2 })
  })

  it('reaches Ollama on its own address without asking for a key', async () => {
    const calls = stubFetch('data: {"choices":[{"delta":{"content":"ok"}}]}\n\ndata: [DONE]\n\n')

    const response = await sendAiRequest({
      settings: { ...settingsFor('ollama'), apiKey: '' },
      system: 'system',
      messages: [{ role: 'user', content: 'hi' }],
    })

    expect(response.text).toBe('ok')
    expect(calls[0].url).toBe('http://localhost:11434/v1/chat/completions')
  })
})
