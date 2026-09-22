/**
 * The runner's own assistant.
 *
 * Separate from `types/ai.ts` because the two answer different questions. That
 * file describes a model the browser calls with the user's key; this one
 * describes a model the *runner* calls, with the runner's tools, on the runner's
 * bill. The browser never learns the key, and the turn shows up on the billing
 * screen — which is the whole reason the seam exists.
 */

/** What `GET /assistant` answers: whether it can answer, and with what. */
export interface AssistantInfo {
  /** `ollama`, `omnigent`, or `off`. */
  backend: string
  available: boolean
  /** The model answers on the runner's own machine, so the turn costs nothing. */
  local: boolean
  model: string
  baseUrl: string
  /** Models this runner can already serve without pulling anything. */
  models: string[]
  /** Tools the assistant may call — the runner's, not the browser's. */
  tools: string[]
  version: string
  /** What to do about it, when `available` is false. Written by the runner. */
  hint: string
  error: string
}

/** One tool the runner ran mid-answer, surfaced so the user can see the work. */
export interface AssistantToolCall {
  name: string
  args?: Record<string, unknown>
  result?: unknown
}

/** What a finished turn consumed, as the runner measured it. */
export interface AssistantUsage {
  model: string
  provider: string
  local: boolean
  inputTokens: number
  outputTokens: number
  toolCalls: number
  durationMs: number
}

/** One assistant turn as the billing screen reads it back. */
export interface AssistTurn {
  id: string
  period: string
  backend: string
  provider: string
  model: string
  local: boolean
  inputTokens: number
  outputTokens: number
  toolCalls: number
  durationMs: number
  /** Credits taken. Zero for every local turn, which is the point. */
  amount: number
  createdAt: string
  actor: string | null
  workflowId: string | null
}

/**
 * A month of assistant work.
 *
 * `localTurns` and `remoteTurns` are reported apart rather than summed: "what is
 * the assistant costing us" and "how much is it being used" are different
 * questions, and a team running everything on its own hardware wants to watch
 * the second climb while the first stays at zero.
 */
export interface AssistSummary {
  period: string
  /** The account this covers, or `all` when it covers the whole runner. */
  scope: string
  turns: number
  localTurns: number
  remoteTurns: number
  inputTokens: number
  outputTokens: number
  toolCalls: number
  charged: number
  seconds: number
  recent: AssistTurn[]
}
