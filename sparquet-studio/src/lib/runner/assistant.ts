/**
 * Client for the runner's assistant (`/assistant`, `/assistant/stream`) and for
 * what it cost (`/credits/assist`).
 *
 * Its own module for the same reason `credits.ts` is one: asking a question,
 * running a Job and reading a bill fail in different ways, and a runner whose
 * Spark is broken can still answer a question about a config.
 *
 * The transcript sent up carries only user and assistant text. Tool calls and
 * their results are rebuilt by the runner on every turn and never travel from
 * here — a transcript this side could forge is a transcript that can tell the
 * model a configuration validated when it did not.
 */

import type {
  AssistantInfo,
  AssistantToolCall,
  AssistantUsage,
  AssistSummary,
  AssistTurn,
} from '@/types/assistant'

import {
  authHeaders,
  DEFAULT_RUNNER_URL,
  postEventStream,
  RunnerError,
  RUNNER_UNREACHABLE_MESSAGE,
} from './client'

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

const asString = (value: unknown, fallback = ''): string =>
  typeof value === 'string' ? value : fallback

const asNullableString = (value: unknown): string | null =>
  typeof value === 'string' ? value : null

const asNumber = (value: unknown, fallback = 0): number =>
  typeof value === 'number' && Number.isFinite(value) ? value : fallback

const asStringList = (value: unknown): string[] =>
  Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : []

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, '')
}

async function readErrorMessage(response: Response): Promise<string> {
  let body = ''
  try {
    body = await response.text()
  } catch {
    body = ''
  }
  try {
    const parsed: unknown = JSON.parse(body)
    if (isRecord(parsed) && typeof parsed.detail === 'string' && parsed.detail.length > 0) {
      return parsed.detail
    }
  } catch {
    /* not JSON — fall through to the status line */
  }
  return `Local runner error (HTTP ${response.status})`
}

async function getJson(
  baseUrl: string,
  path: string,
  token?: string,
  signal?: AbortSignal,
): Promise<unknown> {
  let response: Response
  try {
    response = await fetch(`${normalizeBaseUrl(baseUrl)}${path}`, {
      headers: authHeaders(token),
      signal,
    })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error
    throw new RunnerError(RUNNER_UNREACHABLE_MESSAGE, 'unreachable', undefined, error)
  }
  if (!response.ok) {
    throw new RunnerError(await readErrorMessage(response), 'http', response.status)
  }
  try {
    return (await response.json()) as unknown
  } catch (error) {
    throw new RunnerError(
      'The local runner returned a malformed response.',
      'malformed',
      response.status,
      error,
    )
  }
}

/* -------------------------------------------------------------------- reading */

function toInfo(value: unknown): AssistantInfo {
  const record = isRecord(value) ? value : {}
  return {
    backend: asString(record.backend, 'off'),
    available: record.available === true,
    // A runner that does not answer the question has not answered "no": an older
    // runner with no assistant at all is `available: false` anyway.
    local: record.local !== false,
    model: asString(record.model),
    baseUrl: asString(record.base_url),
    models: asStringList(record.models),
    tools: asStringList(record.tools),
    agent: asString(record.agent),
    version: asString(record.version),
    hint: asString(record.hint),
    error: asString(record.error),
  }
}

/** What this runner can answer with, or why it cannot. Needs no permission. */
export async function getAssistantInfo(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  signal?: AbortSignal,
): Promise<AssistantInfo> {
  return toInfo(await getJson(baseUrl, '/assistant', token, signal))
}

function toTurn(value: unknown): AssistTurn {
  const record = isRecord(value) ? value : {}
  return {
    id: asString(record.id),
    period: asString(record.period),
    backend: asString(record.backend),
    provider: asString(record.provider),
    model: asString(record.model),
    local: record.local !== false,
    inputTokens: asNumber(record.input_tokens),
    outputTokens: asNumber(record.output_tokens),
    toolCalls: asNumber(record.tool_calls),
    durationMs: asNumber(record.duration_ms),
    amount: asNumber(record.amount),
    createdAt: asString(record.created_at),
    actor: asNullableString(record.actor),
    workflowId: asNullableString(record.workflow_id),
  }
}

function toSummary(value: unknown): AssistSummary {
  const record = isRecord(value) ? value : {}
  const recent = Array.isArray(record.recent) ? record.recent : []
  return {
    period: asString(record.period),
    scope: asString(record.scope),
    turns: asNumber(record.turns),
    localTurns: asNumber(record.local_turns),
    remoteTurns: asNumber(record.remote_turns),
    inputTokens: asNumber(record.input_tokens),
    outputTokens: asNumber(record.output_tokens),
    toolCalls: asNumber(record.tool_calls),
    charged: asNumber(record.charged),
    seconds: asNumber(record.seconds),
    recent: recent.map(toTurn),
  }
}

/**
 * A month of assistant work. Scope follows every other bill: your own team
 * always, the whole runner with `credits:Read`.
 */
export async function getAssistUsage(
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  options: { period?: string; accountId?: string; limit?: number } = {},
  signal?: AbortSignal,
): Promise<AssistSummary> {
  const query = new URLSearchParams()
  if (options.period) query.set('period', options.period)
  if (options.accountId) query.set('account_id', options.accountId)
  if (options.limit !== undefined) query.set('limit', String(options.limit))
  const suffix = query.toString() ? `?${query.toString()}` : ''

  return toSummary(await getJson(baseUrl, `/credits/assist${suffix}`, token, signal))
}

/* ------------------------------------------------------------------ streaming */

export interface AssistantStreamHandlers {
  /** Text as it arrives. */
  onToken?: (chunk: string) => void
  /** The runner ran one of its tools, and what came back. */
  onTool?: (call: AssistantToolCall) => void
  /** The turn finished, with what it consumed. */
  onUsage?: (usage: AssistantUsage) => void
}

function toUsage(value: unknown): AssistantUsage | null {
  if (!isRecord(value)) return null
  return {
    model: asString(value.model),
    provider: asString(value.provider),
    local: value.local !== false,
    inputTokens: asNumber(value.inputTokens),
    outputTokens: asNumber(value.outputTokens),
    toolCalls: asNumber(value.toolCalls),
    durationMs: asNumber(value.durationMs),
  }
}

export interface AssistantRequest {
  messages: { role: 'user' | 'assistant'; content: string }[]
  /**
   * How the caller wants the answer shaped, appended to the runner's own
   * prompt. The canvas panel sends its catalog-built prompt here, which is what
   * keeps a proposal parseable when the provider is the runner; the assistant
   * screen sends nothing and lets the runner speak for itself.
   */
  instructions?: string
  /** Overrides the runner's default for this turn only. */
  model?: string
  /** What the question is about, so the cost lands on the right line of the bill. */
  workflowId?: string
}

/**
 * Asks one question and streams the answer.
 *
 * Resolves when the stream ends. An `error` frame becomes a thrown
 * `RunnerError`, because a half-written answer that stops for a reason the user
 * never sees is worse than no answer at all.
 */
export async function streamAssistant(
  request: AssistantRequest,
  baseUrl: string = DEFAULT_RUNNER_URL,
  token?: string,
  handlers: AssistantStreamHandlers = {},
  signal?: AbortSignal,
): Promise<void> {
  let failure: string | null = null

  await postEventStream(
    baseUrl,
    '/assistant/stream',
    {
      messages: request.messages,
      instructions: request.instructions,
      model: request.model,
      workflow_id: request.workflowId,
    },
    (frame) => {
      let payload: unknown
      try {
        payload = JSON.parse(frame.data) as unknown
      } catch {
        return
      }
      const record = isRecord(payload) ? payload : {}
      switch (frame.event) {
        case 'delta':
          handlers.onToken?.(asString(record.text))
          return
        case 'tool':
          handlers.onTool?.({
            name: asString(record.name),
            args: isRecord(record.args) ? record.args : undefined,
            result: record.result,
          })
          return
        case 'done': {
          const usage = toUsage(record.usage)
          if (usage) handlers.onUsage?.(usage)
          return
        }
        case 'error': {
          const hint = asString(record.hint)
          failure = `${asString(record.message, 'The assistant failed.')}${hint ? ` ${hint}` : ''}`
          return
        }
        default:
          // Forward compatibility: unknown events are simply skipped.
          return
      }
    },
    signal,
    token,
  )

  if (failure) throw new RunnerError(failure, 'http')
}
