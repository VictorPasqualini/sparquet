/**
 * The prompt is the assistant's only documentation.
 *
 * Nothing at run time checks that a model was told about a transformation, so a
 * catalog entry added without the prompt reading it is a capability the Studio
 * has and the assistant denies — and the failure looks like a bad model, not a
 * missing line. These tests hold the two claims the prompt makes out loud: that
 * the transformation list is exhaustive, and that what goes in the request is
 * only what the caller passed.
 */

import { describe, expect, it } from 'vitest'

import { FORMATS, TRANSFORMATIONS, VALIDATORS } from '@/catalog'
import { buildSystemPrompt, buildUserPrompt } from '@/lib/ai/prompt'
import type { AiIntent } from '@/types/ai'
import type { StudioNode, ValidationIssue } from '@/types/studio'

const SYSTEM = buildSystemPrompt()

describe('buildSystemPrompt', () => {
  it('names every transformation in the catalog, because it calls the list exhaustive', () => {
    const missing = TRANSFORMATIONS.filter((def) => !SYSTEM.includes(`- ${def.type} —`))
    expect(missing.map((def) => def.type)).toEqual([])
  })

  it('names every format and every validator', () => {
    expect(FORMATS.filter((def) => !SYSTEM.includes(`- ${def.id} (`)).map((def) => def.id)).toEqual(
      [],
    )
    expect(
      VALIDATORS.filter((def) => !SYSTEM.includes(`- ${def.type} —`)).map((def) => def.type),
    ).toEqual([])
  })

  it('sorts the formats by what they can actually do', () => {
    const readable = /Readable: (.+?)\. Writable: (.+?)\./.exec(SYSTEM)
    expect(readable).not.toBeNull()
    const [, read, write] = readable as RegExpExecArray

    for (const format of FORMATS) {
      expect(read.includes(format.id)).toBe(format.canRead)
      expect(write.includes(format.id)).toBe(format.canWrite)
    }
  })

  it('asks for the second source by its JSON name, which the canvas never shows', () => {
    // On the canvas the right-hand source is an edge; in JSON it is `input`, and
    // a model that only saw the canvas vocabulary writes a join that cannot run.
    for (const def of TRANSFORMATIONS.filter((entry) => entry.secondaryInput)) {
      const line = SYSTEM.split('\n').find((row) => row.startsWith(`- ${def.type} —`))
      expect(line, `no line for ${def.type}`).toBeDefined()
      expect(line).toContain('required: input')
    }
  })

  it('carries the fields a transformation cannot run without', () => {
    const rows = new Map(
      SYSTEM.split('\n')
        .filter((row) => row.startsWith('- '))
        .map((row) => [row.slice(2).split(' —')[0], row] as const),
    )
    for (const def of TRANSFORMATIONS) {
      const row = rows.get(def.type)
      for (const field of def.fields.filter((entry) => entry.required)) {
        expect(row, `${def.type} is missing ${field.key}`).toContain(field.key)
      }
    }
  })
})

/* ------------------------------------------------------------- user prompt */

const NODE: StudioNode = {
  id: 'n1',
  type: 'transform',
  position: { x: 0, y: 0 },
  data: { kind: 'transform', transform: 'filter', params: { condition: "status = 'A'" } },
} as StudioNode

const ISSUE: ValidationIssue = {
  id: 'i1',
  severity: 'error',
  message: 'The join has no right-hand source.',
  nodeId: 'n1',
}

describe('buildUserPrompt', () => {
  it('sends nothing about a job when the caller passed nothing', () => {
    const prompt = buildUserPrompt('chat', 'which formats can read Kafka?')

    expect(prompt).toContain('which formats can read Kafka?')
    expect(prompt).not.toContain('Current pipeline JSON')
    expect(prompt).not.toContain('Problems reported by the editor')
    expect(prompt).not.toContain('Selected node')
  })

  it('sends the pipeline, the issues and the selection when it has them', () => {
    const prompt = buildUserPrompt('modify', 'add a filter', {
      pipeline: { name: 'orders', input: { format: 'csv', path: '/in' } },
      issues: [ISSUE],
      selectedNode: NODE,
    })

    expect(prompt).toContain('"name": "orders"')
    expect(prompt).toContain('The join has no right-hand source.')
    expect(prompt).toContain('filter')
  })

  it('keeps an explicitly empty pipeline out, rather than sending "null"', () => {
    // Context off sets it to null; a literal `null` in the prompt reads as "the
    // job is empty", which is a different and wrong statement.
    const prompt = buildUserPrompt('modify', 'add a filter', {
      pipeline: null,
      issues: [],
      selectedNode: null,
    })

    expect(prompt).not.toContain('Current pipeline JSON')
    expect(prompt).not.toContain('null')
  })

  // `document` proposes too: it rewrites names and descriptions, which is an
  // edit to the JSON however little it changes what runs.
  const proposing: AiIntent[] = ['generate', 'modify', 'fix', 'optimize', 'document']
  const prose: AiIntent[] = ['explain', 'chat']

  it('demands one full JSON block for the intents whose answer is applied', () => {
    for (const intent of proposing) {
      const prompt = buildUserPrompt(intent, 'do it')
      expect(prompt, intent).toContain('FULL updated pipeline JSON')
      expect(prompt, intent).toContain('single ```json fenced block')
    }
  })

  it('asks the other intents not to answer with JSON at all', () => {
    for (const intent of prose) {
      const prompt = buildUserPrompt(intent, 'explain it')
      expect(prompt, intent).toContain('Answer in plain English')
      expect(prompt, intent).not.toContain('FULL updated pipeline JSON')
    }
  })
})
