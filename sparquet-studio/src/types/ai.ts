/**
 * AI assistant contracts.
 *
 * Studio talks to model providers directly from the browser with a
 * user-supplied key (stored locally, never sent anywhere else), or through a
 * self-hosted OpenAI-compatible endpoint. Every provider is normalized to the
 * same request/response shape so the UI never branches on vendor.
 */

export const AI_PROVIDERS = ['anthropic', 'openai', 'google', 'openai-compatible'] as const
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
  /** Send the current workflow JSON with each request. */
  shareWorkflowContext: boolean
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

export interface AiRequest {
  settings: AiSettings
  system: string
  messages: { role: 'user' | 'assistant'; content: string }[]
  signal?: AbortSignal
  /** Streams partial text as it arrives. */
  onToken?: (chunk: string) => void
}

export interface AiResponse {
  text: string
  usage?: { inputTokens?: number; outputTokens?: number }
}
