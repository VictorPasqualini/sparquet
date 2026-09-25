import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { detectLocalAi, listOllamaModels, preferredOllamaModel } from '@/lib/ai/local'

const fetchMock = vi.fn()

const RUNNER = { baseUrl: 'http://127.0.0.1:8787', token: 'secret' }

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}

/** Answers per URL fragment; anything unmatched fails the way a dead port does. */
function routes(table: Record<string, Response | Error>): void {
  fetchMock.mockImplementation((input: RequestInfo | URL) => {
    const url = String(input)
    for (const [fragment, answer] of Object.entries(table)) {
      if (url.includes(fragment)) {
        return answer instanceof Error ? Promise.reject(answer) : Promise.resolve(answer)
      }
    }
    return Promise.reject(new TypeError('Failed to fetch'))
  })
}

const TAGS = {
  models: [{ name: 'llama3.1:8b' }, { name: 'qwen2.5-coder:7b' }],
}

beforeEach(() => {
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('preferredOllamaModel', () => {
  it('prefers a coder model over a bigger general one', () => {
    expect(preferredOllamaModel(['llama3.1:70b', 'qwen2.5-coder:7b'])).toBe('qwen2.5-coder:7b')
  })

  it('matches a quantized tag by prefix, because that is the same model', () => {
    expect(preferredOllamaModel(['qwen2.5-coder:7b-instruct-q4_K_M'])).toBe(
      'qwen2.5-coder:7b-instruct-q4_K_M',
    )
  })

  it('takes an unknown model rather than nothing', () => {
    expect(preferredOllamaModel(['mistral-small:24b'])).toBe('mistral-small:24b')
  })

  it('answers empty when nothing is pulled', () => {
    expect(preferredOllamaModel([])).toBe('')
  })
})

describe('listOllamaModels', () => {
  it('reads the names Ollama reports', async () => {
    routes({ '/api/tags': jsonResponse(TAGS) })

    expect(await listOllamaModels()).toEqual(['llama3.1:8b', 'qwen2.5-coder:7b'])
  })

  it('answers empty when Ollama is not there, instead of throwing', async () => {
    routes({})

    expect(await listOllamaModels()).toEqual([])
  })
})

describe('detectLocalAi', () => {
  it('prefers the runner, which is the only one that can call tools', async () => {
    routes({
      '/assistant': jsonResponse({
        backend: 'ollama',
        available: true,
        local: true,
        model: 'qwen2.5-coder:7b',
      }),
      '/api/tags': jsonResponse(TAGS),
    })

    const choice = await detectLocalAi(RUNNER)

    expect(choice?.settings.provider).toBe('runner')
    // The runner owns the model id; storing today's default would outlive it.
    expect(choice?.settings.model).toBe('')
    expect(choice?.reason).toContain('qwen2.5-coder:7b')
  })

  it('falls back to Ollama when no runner is listening', async () => {
    routes({ '/api/tags': jsonResponse(TAGS) })

    const choice = await detectLocalAi(RUNNER)

    expect(choice?.settings).toEqual({
      provider: 'ollama',
      model: 'qwen2.5-coder:7b',
      baseUrl: 'http://localhost:11434/v1',
    })
  })

  it('falls back to Ollama when the runner is up but has no model behind it', async () => {
    routes({
      '/assistant': jsonResponse({
        backend: 'ollama',
        available: false,
        local: true,
        error: 'Ollama is not answering.',
      }),
      '/api/tags': jsonResponse(TAGS),
    })

    expect((await detectLocalAi(RUNNER))?.settings.provider).toBe('ollama')
  })

  it('ignores a runner whose assistant is not local, so nothing silently bills', async () => {
    routes({
      '/assistant': jsonResponse({ backend: 'ollama', available: true, local: false }),
    })

    expect(await detectLocalAi(RUNNER)).toBeNull()
  })

  it('answers null when the machine has neither', async () => {
    routes({})

    expect(await detectLocalAi(RUNNER)).toBeNull()
  })

  it('stops at the first probe when the caller aborts', async () => {
    routes({ '/api/tags': jsonResponse(TAGS) })
    const controller = new AbortController()
    controller.abort()

    expect(await detectLocalAi(RUNNER, controller.signal)).toBeNull()
  })
})
