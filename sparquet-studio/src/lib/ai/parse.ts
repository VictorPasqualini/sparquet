/**
 * Pulls the proposed pipeline out of a model reply.
 *
 * Replies arrive in every shape a language model can produce — fenced blocks,
 * bare fences, JSON surrounded by prose, JSON with trailing commas, half a
 * stream. This module never throws: a failure is reported as `error` so the chat
 * can show the raw text instead of losing it.
 */

import type { AiIntent } from '@/types/ai'

export interface PipelineProposal {
  /** The parsed pipeline, or null when the reply carried none. */
  pipeline: unknown | null
  /** The prose around the JSON block. */
  summary: string
  error?: string
}

interface Span {
  start: number
  end: number
}

interface Candidate extends Span {
  /** Text handed to the JSON parser (fence markers already stripped). */
  body: string
}

type Outcome =
  | { status: 'object'; value: Record<string, unknown> }
  | { status: 'not-object' }
  | { status: 'invalid' }

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

const FENCE = /```([^\n]*)\n([\s\S]*?)(?:```|$)/g

function collectFences(text: string): Candidate[] {
  const out: Candidate[] = []
  FENCE.lastIndex = 0
  let match = FENCE.exec(text)
  while (match !== null) {
    out.push({
      start: match.index,
      end: match.index + match[0].length,
      body: match[2],
    })
    match = FENCE.exec(text)
  }
  return out
}

/** Balanced, depth-0 `{…}` regions, ignoring braces inside JSON strings. */
function collectRawObjects(text: string): Candidate[] {
  const out: Candidate[] = []
  let depth = 0
  let start = -1
  let inString = false
  let escaped = false

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index]
    if (inString) {
      if (escaped) escaped = false
      else if (char === '\\') escaped = true
      else if (char === '"') inString = false
      continue
    }
    if (char === '"') {
      // Quotes in the surrounding prose are not JSON strings.
      if (depth > 0) inString = true
      continue
    }
    if (char === '{') {
      if (depth === 0) start = index
      depth += 1
      continue
    }
    if (char === '}' && depth > 0) {
      depth -= 1
      if (depth === 0 && start !== -1) {
        out.push({ start, end: index + 1, body: text.slice(start, index + 1) })
        start = -1
      }
    }
  }
  return out
}

/** Strips JSON-with-comments and trailing commas without touching string contents. */
export function repairJson(raw: string): string {
  let out = ''
  let inString = false
  let escaped = false

  for (let index = 0; index < raw.length; index += 1) {
    const char = raw[index]

    if (inString) {
      out += char
      if (escaped) escaped = false
      else if (char === '\\') escaped = true
      else if (char === '"') inString = false
      continue
    }

    if (char === '"') {
      inString = true
      out += char
      continue
    }

    if (char === '/' && raw[index + 1] === '/') {
      while (index < raw.length && raw[index] !== '\n') index += 1
      out += '\n'
      continue
    }

    if (char === '/' && raw[index + 1] === '*') {
      index += 2
      while (index < raw.length && !(raw[index] === '*' && raw[index + 1] === '/')) index += 1
      index += 1
      continue
    }

    if (char === ',') {
      let ahead = index + 1
      while (ahead < raw.length) {
        const next = raw[ahead]
        if (next === ' ' || next === '\t' || next === '\n' || next === '\r') {
          ahead += 1
          continue
        }
        if (next === '/' && raw[ahead + 1] === '/') {
          while (ahead < raw.length && raw[ahead] !== '\n') ahead += 1
          continue
        }
        if (next === '/' && raw[ahead + 1] === '*') {
          ahead += 2
          while (ahead < raw.length && !(raw[ahead] === '*' && raw[ahead + 1] === '/'))
            ahead += 1
          ahead += 2
          continue
        }
        break
      }
      if (raw[ahead] === '}' || raw[ahead] === ']') continue
      out += char
      continue
    }

    out += char
  }

  return out
}

function parseCandidate(body: string): Outcome {
  const trimmed = body.trim()
  if (!trimmed) return { status: 'invalid' }

  for (const source of [trimmed, repairJson(trimmed)]) {
    try {
      const value: unknown = JSON.parse(source)
      return isRecord(value) ? { status: 'object', value } : { status: 'not-object' }
    } catch {
      continue
    }
  }
  return { status: 'invalid' }
}

/**
 * A reply can contain a small illustrative snippet next to the real answer, so the
 * closer an object is to a pipeline root the better; size only decides ties. `name`
 * is deliberately not a marker: `with_column` uses it too, so a long transformation
 * snippet would otherwise outrank a compact pipeline.
 */
const rank = (value: Record<string, unknown>): number => {
  const hasInput = 'input' in value
  const hasOutput = 'output' in value || 'outputs' in value
  if (hasInput && hasOutput) return 3
  if (hasInput || hasOutput) return 2
  return 'transformations' in value || 'validations' in value ? 1 : 0
}

/** Below this the object is a fragment, not something worth offering as "Apply". */
const ROOT_RANK = 2

function cleanProse(text: string): string {
  return text
    .split('\n')
    .map((line) => line.replace(/\s+$/, ''))
    .join('\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim()
}

function proseWithout(text: string, spans: Span[]): string {
  const ordered = [...spans].sort((a, b) => a.start - b.start)
  let out = ''
  let cursor = 0
  for (const span of ordered) {
    if (span.start < cursor) continue
    out += text.slice(cursor, span.start)
    cursor = span.end
  }
  out += text.slice(cursor)
  return cleanProse(out)
}

export function extractPipelineProposal(text: string): PipelineProposal {
  try {
    if (typeof text !== 'string' || !text.trim()) {
      return { pipeline: null, summary: '', error: 'The reply was empty.' }
    }

    const fences = collectFences(text)
    const inFence = (span: Span): boolean =>
      fences.some((fence) => span.start >= fence.start && span.end <= fence.end)
    const candidates = [...fences, ...collectRawObjects(text).filter((span) => !inFence(span))]

    let best: { candidate: Candidate; value: Record<string, unknown>; size: number } | null =
      null
    let sawJson = false

    for (const candidate of candidates) {
      const outcome = parseCandidate(candidate.body)
      if (outcome.status === 'not-object') {
        sawJson = true
        continue
      }
      if (outcome.status === 'invalid') continue

      sawJson = true
      const size = JSON.stringify(outcome.value).length
      const better =
        !best ||
        rank(outcome.value) > rank(best.value) ||
        (rank(outcome.value) === rank(best.value) && size > best.size)
      if (better) best = { candidate, value: outcome.value, size }
    }

    if (!best || rank(best.value) < ROOT_RANK) {
      const summary = proseWithout(text, fences)
      return {
        pipeline: null,
        summary: summary || cleanProse(text),
        error: sawJson
          ? 'The reply contained JSON that is not a pipeline object.'
          : 'The reply contained no pipeline JSON.',
      }
    }

    return {
      pipeline: best.value,
      summary: proseWithout(text, [...fences, best.candidate]),
    }
  } catch (error) {
    return {
      pipeline: null,
      summary: '',
      error: `Could not read the reply: ${error instanceof Error ? error.message : String(error)}`,
    }
  }
}

/**
 * Intents whose reply may carry a pipeline — mirrors PROPOSAL_INTENTS in prompt.ts.
 * explain/chat are asked for prose that quotes fragments, so scanning them for a
 * proposal turns a quoted snippet into a one-click canvas rebuild.
 */
const PROPOSAL_INTENTS: ReadonlySet<AiIntent> = new Set<AiIntent>([
  'generate',
  'modify',
  'fix',
  'optimize',
  'document',
])

export function extractProposalFor(intent: AiIntent, text: string): PipelineProposal {
  if (!PROPOSAL_INTENTS.has(intent)) return { pipeline: null, summary: '' }
  return extractPipelineProposal(text)
}
