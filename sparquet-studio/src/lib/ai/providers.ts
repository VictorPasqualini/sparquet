/**
 * Provider registry for the AI assistant.
 *
 * Model ids are free text everywhere in the app: this list is the curated menu,
 * not a whitelist, so a model released tomorrow works by typing its id.
 */

import { AI_PROVIDERS } from '@/types/ai'
import type { AiProviderId, AiProviderInfo, AiSettings } from '@/types/ai'

export const AI_PROVIDER_INFO: Record<AiProviderId, AiProviderInfo> = {
  anthropic: {
    id: 'anthropic',
    label: 'Anthropic',
    keyUrl: 'https://console.anthropic.com/settings/keys',
    defaultModel: 'claude-sonnet-4-5',
    models: [
      {
        id: 'claude-sonnet-4-5',
        label: 'Claude Sonnet 4.5',
        hint: 'Best default — strong pipeline authoring at a sane latency.',
      },
      {
        id: 'claude-opus-4-1',
        label: 'Claude Opus 4.1',
        hint: 'Deepest reasoning for large refactors. Slower and more expensive.',
      },
      {
        id: 'claude-haiku-4-5',
        label: 'Claude Haiku 4.5',
        hint: 'Fastest and cheapest — good for small edits and explanations.',
      },
    ],
    defaultBaseUrl: 'https://api.anthropic.com',
    requiresKey: true,
    docsNote:
      'Studio calls the Messages API straight from the browser and sends the opt-in header Anthropic requires for direct browser access.',
  },

  openai: {
    id: 'openai',
    label: 'OpenAI',
    keyUrl: 'https://platform.openai.com/api-keys',
    defaultModel: 'gpt-4.1',
    models: [
      {
        id: 'gpt-4.1',
        label: 'GPT-4.1',
        hint: 'Best default — long context and reliable JSON output.',
      },
      {
        id: 'gpt-4.1-mini',
        label: 'GPT-4.1 mini',
        hint: 'Faster and cheaper; fine for small edits.',
      },
      {
        id: 'o4-mini',
        label: 'o4-mini',
        hint: 'Reasoning model. Ignores temperature and bills thinking tokens.',
      },
    ],
    defaultBaseUrl: 'https://api.openai.com/v1',
    requiresKey: true,
    docsNote: 'Uses the Chat Completions API, so any compatible gateway works too.',
  },

  google: {
    id: 'google',
    label: 'Google Gemini',
    keyUrl: 'https://aistudio.google.com/apikey',
    defaultModel: 'gemini-2.5-pro',
    models: [
      {
        id: 'gemini-2.5-pro',
        label: 'Gemini 2.5 Pro',
        hint: 'Best default — very large context window.',
      },
      {
        id: 'gemini-2.5-flash',
        label: 'Gemini 2.5 Flash',
        hint: 'Faster and cheaper; good for iterative edits.',
      },
    ],
    defaultBaseUrl: 'https://generativelanguage.googleapis.com/v1beta',
    requiresKey: true,
    docsNote: 'Uses the Generative Language API with server-sent events.',
  },

  'openai-compatible': {
    id: 'openai-compatible',
    label: 'OpenAI-compatible',
    keyUrl: '',
    defaultModel: '',
    models: [],
    defaultBaseUrl: '',
    requiresKey: false,
    docsNote:
      'Any endpoint exposing POST /chat/completions — Ollama, vLLM, LM Studio, OpenRouter, Azure gateways. Set the base URL up to and including the version segment, e.g. http://localhost:11434/v1.',
  },
}

export const AI_PROVIDER_LIST: AiProviderInfo[] = AI_PROVIDERS.map((id) => AI_PROVIDER_INFO[id])

export function getProviderInfo(provider: AiProviderId): AiProviderInfo {
  return AI_PROVIDER_INFO[provider]
}

type EndpointSettings = Pick<AiSettings, 'provider' | 'baseUrl' | 'model'>

/** User value first, provider default as fallback, never a trailing slash. */
export function resolveBaseUrl(settings: EndpointSettings): string {
  const chosen = settings.baseUrl.trim() || AI_PROVIDER_INFO[settings.provider].defaultBaseUrl
  return chosen.replace(/\/+$/, '')
}

export function resolveModel(settings: EndpointSettings): string {
  return settings.model.trim() || AI_PROVIDER_INFO[settings.provider].defaultModel
}
