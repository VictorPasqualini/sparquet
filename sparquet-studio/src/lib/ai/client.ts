/**
 * One normalized call for every provider.
 *
 * The key travels from settings → request headers and nowhere else: it is never
 * logged, never stored and never included in a thrown message (provider error
 * bodies are redacted before they surface).
 */

import { getProviderInfo, resolveBaseUrl, resolveModel } from '@/lib/ai/providers'
import type { AiRequest, AiResponse, AiSettings } from '@/types/ai'

const ANTHROPIC_VERSION = '2023-06-01'

type Turn = { role: 'user' | 'assistant'; content: string }

/* ------------------------------------------------------------------ narrowing */

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

const asString = (value: unknown): string | undefined =>
  typeof value === 'string' ? value : undefined

const asNumber = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isFinite(value) ? value : undefined

function dig(value: unknown, ...path: (string | number)[]): unknown {
  let current: unknown = value
  for (const key of path) {
    if (typeof key === 'number') {
      if (!Array.isArray(current)) return undefined
      current = current[key]
      continue
    }
    if (!isRecord(current)) return undefined
    current = current[key]
  }
  return current
}

function parseJson(raw: string): unknown {
  try {
    return JSON.parse(raw) as unknown
  } catch {
    return undefined
  }
}

/* --------------------------------------------------------------------- errors */

function isAbortError(error: unknown): boolean {
  return (
    error instanceof Error && (error.name === 'AbortError' || error.name === 'TimeoutError')
  )
}

/** Providers sometimes echo the key back in an error body; strip it before display. */
function redact(text: string, apiKey: string): string {
  const key = apiKey.trim()
  if (key.length < 8) return text
  return text.split(key).join('[redacted]')
}

function statusHint(status: number): string {
  if (status === 401) return ' Check the API key in Settings — it was rejected.'
  if (status === 429) return ' Rate limit or quota reached — wait a moment, then retry.'
  return ''
}

/** Digs the human-readable message out of the error envelopes we can meet. */
function errorMessageOf(body: unknown): string | undefined {
  const candidates: unknown[] = [
    dig(body, 'error', 'message'),
    dig(body, 'error', 0, 'message'),
    dig(body, 0, 'error', 'message'),
    dig(body, 'error'),
    dig(body, 'message'),
    dig(body, 'detail'),
  ]
  for (const candidate of candidates) {
    const message = asString(candidate)
    if (message?.trim()) return message.trim()
  }
  return undefined
}

function providerMessage(body: string): string {
  const message = errorMessageOf(parseJson(body))
  if (message) return message
  const flat = body.replace(/\s+/g, ' ').trim()
  return flat.length > 300 ? `${flat.slice(0, 300)}…` : flat
}

async function responseError(response: Response, settings: AiSettings): Promise<Error> {
  let body = ''
  try {
    body = await response.text()
  } catch {
    body = ''
  }
  const label = getProviderInfo(settings.provider).label
  const detail = body ? redact(providerMessage(body), settings.apiKey) : response.statusText
  return new Error(
    `${label} request failed (HTTP ${response.status})${detail ? `: ${detail}` : ''}.${statusHint(response.status)}`,
  )
}

/* ------------------------------------------------------------------ SSE reader */

interface SseEvent {
  event: string
  data: string
}

function parseSseBlock(block: string): SseEvent | null {
  let event = ''
  const data: string[] = []
  for (const line of block.split('\n')) {
    if (!line || line.startsWith(':')) continue
    const separator = line.indexOf(':')
    const field = separator === -1 ? line : line.slice(0, separator)
    const value = separator === -1 ? '' : line.slice(separator + 1).replace(/^ /, '')
    if (field === 'event') event = value
    else if (field === 'data') data.push(value)
  }
  if (data.length === 0) return null
  return { event, data: data.join('\n') }
}

interface StreamRead {
  /** SSE events seen; zero means the 200 body was never an event stream. */
  events: number
  /** Body text, buffered only while no event has been seen. */
  raw: string
}

/** Enough of a non-stream body to recognize an error envelope or a whole reply. */
const RAW_LIMIT = 100_000

async function consumeSse(
  response: Response,
  onEvent: (event: SseEvent) => void,
): Promise<StreamRead> {
  const read: StreamRead = { events: 0, raw: '' }

  const emit = (event: SseEvent): void => {
    read.events += 1
    read.raw = ''
    onEvent(event)
  }

  const flush = (chunk: string, buffer: string): string => {
    if (read.events === 0 && read.raw.length < RAW_LIMIT) read.raw += chunk
    let rest = buffer + chunk
    let index = rest.indexOf('\n\n')
    while (index !== -1) {
      const parsed = parseSseBlock(rest.slice(0, index))
      if (parsed) emit(parsed)
      rest = rest.slice(index + 2)
      index = rest.indexOf('\n\n')
    }
    return rest
  }

  const stream = response.body
  if (!stream) {
    const text = (await response.text()).replace(/\r/g, '')
    const tail = flush(text, '')
    const parsed = parseSseBlock(tail)
    if (parsed) emit(parsed)
    return read
  }

  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  try {
    for (;;) {
      const { value, done } = await reader.read()
      if (done) break
      buffer = flush(decoder.decode(value, { stream: true }).replace(/\r/g, ''), buffer)
    }
    buffer = flush(decoder.decode().replace(/\r/g, ''), buffer)
    const parsed = parseSseBlock(buffer)
    if (parsed) emit(parsed)
  } finally {
    reader.releaseLock()
  }
  return read
}

/**
 * A 200 whose body is not an event stream: gateways answer plain JSON for quota
 * and context errors, and some ignore `stream: true` and return the whole reply.
 * Both used to resolve as an empty success.
 */
function recoverNonStream(
  read: StreamRead,
  settings: AiSettings,
  readWhole: (body: unknown) => string | undefined,
): string {
  const label = getProviderInfo(settings.provider).label
  const body = parseJson(read.raw)
  const failure = errorMessageOf(body)
  if (failure) throw new Error(`${label} failed: ${redact(failure, settings.apiKey)}`)
  const whole = readWhole(body)?.trim()
  if (whole) return whole
  const snippet = read.raw.replace(/\s+/g, ' ').trim().slice(0, 200)
  const detail = snippet ? `: ${redact(snippet, settings.apiKey)}` : ''
  throw new Error(`${label} returned a response that was not an event stream${detail}.`)
}

/** Anthropic `content` and Google `parts` both carry the whole reply as text chunks. */
function joinTextParts(parts: unknown): string | undefined {
  if (!Array.isArray(parts)) return undefined
  const text = parts.map((part) => asString(dig(part, 'text')) ?? '').join('')
  return text || undefined
}

/* ----------------------------------------------------------------- transport */

async function post(
  url: string,
  headers: Record<string, string>,
  body: unknown,
  request: AiRequest,
): Promise<Response> {
  const { settings, signal } = request
  let response: Response
  try {
    response = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...headers },
      body: JSON.stringify(body),
      signal,
    })
  } catch (error) {
    if (isAbortError(error)) throw error
    const label = getProviderInfo(settings.provider).label
    const reason = error instanceof Error ? error.message : String(error)
    throw new Error(
      `Could not reach ${label}: ${redact(reason, settings.apiKey)}. Check the base URL, your network, and whether the endpoint allows browser requests (CORS).`,
    )
  }
  if (!response.ok) throw await responseError(response, settings)
  return response
}

/* ------------------------------------------------------------------- message prep */

/**
 * Anthropic and Google both reject a conversation that opens on an assistant turn
 * or repeats a role, so every provider gets the same normalized transcript.
 */
function normalizeTurns(messages: Turn[]): Turn[] {
  const out: Turn[] = []
  for (const message of messages) {
    const content = message.content.trim()
    if (!content) continue
    if (out.length === 0 && message.role === 'assistant') continue
    const last = out[out.length - 1]
    if (last && last.role === message.role) {
      last.content = `${last.content}\n\n${content}`
      continue
    }
    out.push({ role: message.role, content })
  }
  return out
}

function requireTurns(messages: Turn[]): Turn[] {
  const turns = normalizeTurns(messages)
  if (turns.length === 0) throw new Error('Nothing to send — the request has no user message.')
  return turns
}

/* -------------------------------------------------------------------- anthropic */

async function sendAnthropic(request: AiRequest, baseUrl: string): Promise<AiResponse> {
  const { settings, onToken } = request
  const body: Record<string, unknown> = {
    model: resolveModel(settings),
    max_tokens: settings.maxTokens,
    temperature: settings.temperature,
    messages: requireTurns(request.messages),
    stream: true,
  }
  if (request.system.trim()) body.system = request.system

  const response = await post(
    `${baseUrl}/v1/messages`,
    {
      'x-api-key': settings.apiKey,
      'anthropic-version': ANTHROPIC_VERSION,
      // Without this opt-in header the Messages API refuses requests made from a page.
      'anthropic-dangerous-direct-browser-access': 'true',
    },
    body,
    request,
  )

  let text = ''
  let inputTokens: number | undefined
  let outputTokens: number | undefined
  let streamError: string | undefined

  const read = await consumeSse(response, ({ data }) => {
    const payload = parseJson(data)
    if (!isRecord(payload)) return
    const type = asString(payload.type)

    if (type === 'content_block_delta') {
      if (asString(dig(payload, 'delta', 'type')) !== 'text_delta') return
      const chunk = asString(dig(payload, 'delta', 'text'))
      if (!chunk) return
      text += chunk
      onToken?.(chunk)
      return
    }
    if (type === 'message_start') {
      inputTokens = asNumber(dig(payload, 'message', 'usage', 'input_tokens')) ?? inputTokens
      outputTokens = asNumber(dig(payload, 'message', 'usage', 'output_tokens')) ?? outputTokens
      return
    }
    if (type === 'message_delta') {
      outputTokens = asNumber(dig(payload, 'usage', 'output_tokens')) ?? outputTokens
      return
    }
    if (type === 'error') {
      streamError = asString(dig(payload, 'error', 'message')) ?? 'Unknown streaming error.'
    }
  })

  if (streamError)
    throw new Error(`Anthropic stream failed: ${redact(streamError, settings.apiKey)}`)
  if (!text && read.events === 0) {
    text = recoverNonStream(read, settings, (body) => joinTextParts(dig(body, 'content')))
    onToken?.(text)
  }
  return { text, usage: usageOf(inputTokens, outputTokens) }
}

/* ----------------------------------------------------------------------- openai */

/** o-series models reject `temperature` and rename the token budget. */
const isReasoningModel = (model: string): boolean => /^o\d/i.test(model)

async function sendOpenAi(
  request: AiRequest,
  baseUrl: string,
  official: boolean,
): Promise<AiResponse> {
  const { settings, onToken } = request
  const model = resolveModel(settings)
  const messages: { role: 'system' | 'user' | 'assistant'; content: string }[] = []
  if (request.system.trim()) messages.push({ role: 'system', content: request.system })
  messages.push(...requireTurns(request.messages))

  const body: Record<string, unknown> = { model, messages, stream: true }
  if (isReasoningModel(model)) {
    body.max_completion_tokens = settings.maxTokens
  } else {
    body.max_tokens = settings.maxTokens
    body.temperature = settings.temperature
  }
  // Self-hosted gateways often reject unknown fields, so only ask OpenAI for usage.
  if (official) body.stream_options = { include_usage: true }

  const headers: Record<string, string> = {}
  if (settings.apiKey.trim()) headers.authorization = `Bearer ${settings.apiKey.trim()}`

  const response = await post(`${baseUrl}/chat/completions`, headers, body, request)

  let text = ''
  let inputTokens: number | undefined
  let outputTokens: number | undefined
  let streamError: string | undefined

  const read = await consumeSse(response, ({ data }) => {
    if (data === '[DONE]') return
    const payload = parseJson(data)
    if (!isRecord(payload)) return

    // Gateways stream the error as an object or as a bare string.
    const failure = payload.error
    if (failure !== undefined && failure !== null) {
      streamError =
        asString(dig(failure, 'message')) ?? asString(failure) ?? 'Unknown streaming error.'
      return
    }
    const chunk = asString(dig(payload, 'choices', 0, 'delta', 'content'))
    if (chunk) {
      text += chunk
      onToken?.(chunk)
    }
    inputTokens = asNumber(dig(payload, 'usage', 'prompt_tokens')) ?? inputTokens
    outputTokens = asNumber(dig(payload, 'usage', 'completion_tokens')) ?? outputTokens
  })

  if (streamError) {
    const label = getProviderInfo(settings.provider).label
    throw new Error(`${label} stream failed: ${redact(streamError, settings.apiKey)}`)
  }
  if (!text && read.events === 0) {
    text = recoverNonStream(read, settings, (body) =>
      asString(dig(body, 'choices', 0, 'message', 'content')),
    )
    onToken?.(text)
  }
  return { text, usage: usageOf(inputTokens, outputTokens) }
}

/* ----------------------------------------------------------------------- google */

async function sendGoogle(request: AiRequest, baseUrl: string): Promise<AiResponse> {
  const { settings, onToken } = request
  const model = resolveModel(settings)
  const contents = requireTurns(request.messages).map((turn) => ({
    role: turn.role === 'assistant' ? 'model' : 'user',
    parts: [{ text: turn.content }],
  }))

  const body: Record<string, unknown> = {
    contents,
    generationConfig: {
      temperature: settings.temperature,
      maxOutputTokens: settings.maxTokens,
    },
  }
  if (request.system.trim()) body.systemInstruction = { parts: [{ text: request.system }] }

  // The key goes in the header, never the URL: request URLs land in the Resource
  // Timing buffer, HAR exports and proxy logs; headers do not.
  const url = `${baseUrl}/models/${encodeURIComponent(model)}:streamGenerateContent?alt=sse`
  const response = await post(url, { 'x-goog-api-key': settings.apiKey.trim() }, body, request)

  let text = ''
  let inputTokens: number | undefined
  let outputTokens: number | undefined
  let streamError: string | undefined

  const read = await consumeSse(response, ({ data }) => {
    const payload = parseJson(data)
    if (!isRecord(payload)) return

    const failure = payload.error
    if (failure !== undefined && failure !== null) {
      streamError =
        asString(dig(failure, 'message')) ?? asString(failure) ?? 'Unknown streaming error.'
      return
    }
    const parts = dig(payload, 'candidates', 0, 'content', 'parts')
    if (Array.isArray(parts)) {
      for (const part of parts) {
        const chunk = asString(dig(part, 'text'))
        if (!chunk) continue
        text += chunk
        onToken?.(chunk)
      }
    }
    inputTokens = asNumber(dig(payload, 'usageMetadata', 'promptTokenCount')) ?? inputTokens
    outputTokens =
      asNumber(dig(payload, 'usageMetadata', 'candidatesTokenCount')) ?? outputTokens
  })

  if (streamError)
    throw new Error(`Google stream failed: ${redact(streamError, settings.apiKey)}`)
  if (!text && read.events === 0) {
    text = recoverNonStream(read, settings, (body) =>
      joinTextParts(dig(body, 'candidates', 0, 'content', 'parts')),
    )
    onToken?.(text)
  }
  return { text, usage: usageOf(inputTokens, outputTokens) }
}

/* ------------------------------------------------------------------ entry point */

function usageOf(inputTokens?: number, outputTokens?: number): AiResponse['usage'] {
  if (inputTokens === undefined && outputTokens === undefined) return undefined
  return { inputTokens, outputTokens }
}

export async function sendAiRequest(request: AiRequest): Promise<AiResponse> {
  const { settings } = request
  const info = getProviderInfo(settings.provider)

  if (info.requiresKey && !settings.apiKey.trim()) {
    throw new Error(`Add your ${info.label} API key in Settings before asking the assistant.`)
  }
  if (!resolveModel(settings)) {
    throw new Error(`Choose a model for ${info.label} in Settings.`)
  }

  const baseUrl = resolveBaseUrl(settings)
  if (!baseUrl) {
    throw new Error(
      `Set the base URL for ${info.label} in Settings, for example http://localhost:11434/v1.`,
    )
  }

  request.signal?.throwIfAborted()

  switch (settings.provider) {
    case 'anthropic':
      return sendAnthropic(request, baseUrl)
    case 'openai':
      return sendOpenAi(request, baseUrl, true)
    case 'openai-compatible':
      return sendOpenAi(request, baseUrl, false)
    case 'google':
      return sendGoogle(request, baseUrl)
    default: {
      const unreachable: never = settings.provider
      throw new Error(`Unsupported provider: ${String(unreachable)}`)
    }
  }
}
