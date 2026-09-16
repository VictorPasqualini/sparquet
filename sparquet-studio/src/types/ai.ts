/**
 * AI assistant contracts.
 *
 * Studio talks to model providers directly from the browser with a
 * user-supplied key (stored locally, never sent anywhere else), through a
 * self-hosted OpenAI-compatible endpoint, or through the local runner. Every
 * provider is normalized to the same request/response shape so the UI never
 * branches on vendor.
 *
 * `runner` is the odd one, and deliberately: the browser holds no key, the
 * model is whatever the runner was configured with, the assistant can call the
 * runner's tools, and the turn is metered. The other providers are a browser
 * talking to a vendor, and nothing downstream ever learns they happened.
 */

export const AI_PROVIDERS = [
  'runner',
  'ollama',
  'anthropic',
  'openai',
  'google',
  'openai-compatible',
] as const
export type AiProviderId = (typeof AI_PROVIDERS)[number]

export interface AiProviderInfo {
  id: AiProviderId
  label: string
  /** Where the user gets a key; shown in settings. */
  keyUrl: string
  /** Default model id used when the user has not chosen one. */
  defaultModel: string
  /** Curated model list; the field stays free-text so new models work instantly. */
  models: { id: string; label: string; hint?: string }[]
  /** Default base URL; editable for proxies and self-hosted gateways. */
  defaultBaseUrl: string
  /** True when requests are billed to the user's own key. */
  requiresKey: boolean
  docsNote?: string
}

export interface AiSettings {
  provider: AiProviderId
  model: string
  baseUrl: string
  apiKey: string
  temperature: number
  maxTokens: number
  /** Send the current job JSON with each request. */
  shareJobContext: boolean
}

export type AiIntent =
  | 'generate'
  | 'modify'
  | 'explain'
  | 'fix'
  | 'optimize'
  | 'document'
  | 'chat'

export interface AiMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  intent?: AiIntent
  createdAt: number
  /** Pipeline JSON proposed by the assistant, when the reply contained one. */
  proposal?: {
    pipeline: unknown
    summary: string
    applied: boolean
  }
  error?: string
  /** Token accounting reported by the provider, when available. */
  usage?: { inputTokens?: number; outputTokens?: number }
}

/** Where the local runner is, for the one provider that goes through it. */
export interface AiRunnerTarget {
  baseUrl: string
  token: string
  /** What the question is about, so the cost lands on the right line of the bill. */
  workflowId?: string
}

export interface AiRequest {
  settings: AiSettings
  system: string
  messages: { role: 'user' | 'assistant'; content: string }[]
  signal?: AbortSignal
  /** Streams partial text as it arrives. */
  onToken?: (chunk: string) => void
  /** Required by the `runner` provider and ignored by every other one. */
  runner?: AiRunnerTarget
  /** The `runner` provider reports the tools it called mid-answer. */
  onTool?: (call: { name: string; args?: Record<string, unknown>; result?: unknown }) => void
}

export interface AiResponse {
  text: string
  usage?: { inputTokens?: number; outputTokens?: number }
  /**
   * Set by the `runner` provider: the model answered on the runner's machine, so
   * the turn is recorded at a cost of zero. Absent for a browser-side provider,
   * where the question does not arise — the user's own key paid for it.
   */
  local?: boolean
}
