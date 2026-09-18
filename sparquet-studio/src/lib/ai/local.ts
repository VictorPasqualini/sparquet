/**
 * Finding a model that already runs on this machine.
 *
 * The Studio ships with no key, so its out-of-the-box provider has to be one
 * that needs none. Two of them can be true at once — the runner may be up with
 * Ollama behind it, or Ollama may be up with no runner at all — and which is
 * present is a fact about the machine, not a preference. So it is discovered
 * rather than configured: ask the runner, then ask Ollama, take the first that
 * answers.
 *
 * The runner is preferred when both are there. It is the only one that can call
 * tools, so the same model gives a better answer through it, and its turns are
 * recorded in Billing instead of vanishing.
 *
 * Nothing here ever selects a paid provider. A machine with no local model
 * keeps whatever is configured and the screen says what to install — silently
 * pointing somebody at a provider that will charge them is not a default, it is
 * a surprise.
 */

import { getAssistantInfo } from '@/lib/runner/assistant'
import type { AiSettings } from '@/types/ai'

/** Ollama's own address, and the `/v1` suffix the browser client needs. */
export const OLLAMA_NATIVE_URL = 'http://localhost:11434'
export const OLLAMA_OPENAI_URL = `${OLLAMA_NATIVE_URL}/v1`

/** How long a probe waits. A local port answers immediately or is not there. */
const PROBE_TIMEOUT_MS = 2500

/**
 * Models worth defaulting to, best first. Matched as a prefix, so
 * `qwen2.5-coder:7b-instruct-q4_K_M` counts as `qwen2.5-coder:7b`.
 */
const PREFERRED = ['qwen2.5-coder:7b', 'qwen2.5-coder', 'codellama', 'llama3.1', 'llama3']

export interface LocalAiChoice {
  /** What to store in settings. */
  settings: Pick<AiSettings, 'provider' | 'model' | 'baseUrl'>
  /** Said out loud in the UI, because a default nobody chose should explain itself. */
  reason: string
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/** Aborts on the caller's signal *or* on the timeout, whichever comes first. */
function withTimeout(signal?: AbortSignal): { signal: AbortSignal; done: () => void } {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS)
  const relay = () => controller.abort()
  signal?.addEventListener('abort', relay)
  return {
    signal: controller.signal,
    done: () => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', relay)
    },
  }
}

/** The models pulled on this machine, newest-first as Ollama reports them. */
export async function listOllamaModels(
  baseUrl: string = OLLAMA_NATIVE_URL,
  signal?: AbortSignal,
): Promise<string[]> {
  const probe = withTimeout(signal)
  try {
    const response = await fetch(`${baseUrl.replace(/\/+$/, '')}/api/tags`, {
      signal: probe.signal,
    })
    if (!response.ok) return []
    const body: unknown = await response.json()
    const models = isRecord(body) && Array.isArray(body.models) ? body.models : []
    return models
      .map((entry) => (isRecord(entry) && typeof entry.name === 'string' ? entry.name : ''))
      .filter((name) => name.length > 0)
  } catch {
    // Not running, blocked by CORS, not installed: all the same answer here.
    return []
  } finally {
    probe.done()
  }
}

/**
 * Which of the pulled models to use.
 *
 * A coder model that writes clean JSON beats a bigger general one for this
 * product, so the preference list wins over whatever is first — but an unknown
 * model is still better than none, so the list never excludes.
 */
export function preferredOllamaModel(models: string[]): string {
  for (const wanted of PREFERRED) {
    const hit = models.find((name) => name.startsWith(wanted))
    if (hit) return hit
  }
  return models[0] ?? ''
}

/**
 * The best local provider this machine can offer right now, or `null`.
 *
 * `null` is not a failure to report loudly: most machines have neither, and the
 * screens already say what to install when somebody actually asks a question.
 */
export async function detectLocalAi(
  runner: { baseUrl: string; token: string },
  signal?: AbortSignal,
): Promise<LocalAiChoice | null> {
  try {
    const info = await getAssistantInfo(runner.baseUrl, runner.token, signal)
    if (info.available && info.local) {
      return {
        // The runner picks the model; overriding it here would pin today's
        // default into settings and survive the operator changing it.
        settings: { provider: 'runner', model: '', baseUrl: '' },
        reason: `the local runner answers with ${info.model || 'its own model'}`,
      }
    }
  } catch {
    // No runner, or one too old to have an assistant. Ollama may still be there.
  }
  if (signal?.aborted) return null

  const models = await listOllamaModels(OLLAMA_NATIVE_URL, signal)
  const model = preferredOllamaModel(models)
  if (!model) return null
  return {
    settings: { provider: 'ollama', model, baseUrl: OLLAMA_OPENAI_URL },
    reason: `Ollama is running here with ${model}`,
  }
}
