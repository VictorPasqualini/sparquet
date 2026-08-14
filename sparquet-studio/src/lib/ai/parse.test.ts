import { describe, expect, it } from 'vitest'

import { extractPipelineProposal, extractProposalFor, repairJson } from '@/lib/ai/parse'

const asRecord = (value: unknown): Record<string, unknown> => {
  expect(typeof value).toBe('object')
  expect(value).not.toBeNull()
  return value as Record<string, unknown>
}

const PIPELINE = `{
  "name": "clientes",
  "input": { "format": "csv", "path": "/data/in" },
  "output": { "format": "parquet", "path": "/data/out" }
}`

describe('extractPipelineProposal', () => {
  it('reads a ```json fenced block and keeps the prose as the summary', () => {
    const reply = `Here is the pipeline you asked for.\n\n\`\`\`json\n${PIPELINE}\n\`\`\`\n\nIt reads the CSV and writes Parquet.`

    const result = extractPipelineProposal(reply)

    expect(result.error).toBeUndefined()
    expect(asRecord(result.pipeline).name).toBe('clientes')
    expect(result.summary).toBe(
      'Here is the pipeline you asked for.\n\nIt reads the CSV and writes Parquet.',
    )
  })

  it('reads a bare fenced block with no language tag', () => {
    const reply = `\`\`\`\n${PIPELINE}\n\`\`\``

    const result = extractPipelineProposal(reply)

    expect(result.error).toBeUndefined()
    expect(asRecord(result.pipeline).name).toBe('clientes')
    expect(result.summary).toBe('')
  })

  it('reads a raw JSON object surrounded by prose', () => {
    const reply = `Sure thing. ${PIPELINE} That pipeline copies the file.`

    const result = extractPipelineProposal(reply)

    expect(result.error).toBeUndefined()
    expect(asRecord(result.pipeline).input).toEqual({ format: 'csv', path: '/data/in' })
    expect(result.summary).toBe('Sure thing.  That pipeline copies the file.')
  })

  it('is not confused by braces inside prose or by {param} placeholders', () => {
    const reply = `Use {tipo_ativo} and {{cessoes}} here.\n\n\`\`\`json\n${PIPELINE}\n\`\`\``

    const result = extractPipelineProposal(reply)

    expect(result.error).toBeUndefined()
    expect(asRecord(result.pipeline).name).toBe('clientes')
    expect(result.summary).toBe('Use {tipo_ativo} and {{cessoes}} here.')
  })

  it('repairs trailing commas', () => {
    const reply = [
      '```json',
      '{',
      '  "name": "trailing",',
      '  "input": { "format": "csv", "path": "/in", },',
      '  "outputs": [',
      '    { "format": "parquet", "path": "/out" },',
      '  ],',
      '}',
      '```',
    ].join('\n')

    const result = extractPipelineProposal(reply)

    expect(result.error).toBeUndefined()
    expect(asRecord(result.pipeline).name).toBe('trailing')
    expect(asRecord(result.pipeline).outputs).toEqual([{ format: 'parquet', path: '/out' }])
  })

  it('repairs // and /* */ comments', () => {
    const reply = [
      '```json',
      '{',
      '  // the source table',
      '  "name": "commented",',
      '  /* block comment */',
      '  "input": { "format": "delta", "path": "db.t" }',
      '}',
      '```',
    ].join('\n')

    const result = extractPipelineProposal(reply)

    expect(result.error).toBeUndefined()
    expect(asRecord(result.pipeline).name).toBe('commented')
  })

  it('never treats a comma inside a string as a trailing comma', () => {
    const reply = `\`\`\`json\n{ "name": "a, b", "input": { "format": "csv", "path": "/x" } }\n\`\`\``

    const result = extractPipelineProposal(reply)

    expect(asRecord(result.pipeline).name).toBe('a, b')
    expect(repairJson('{ "a": "x,]" }')).toBe('{ "a": "x,]" }')
  })

  it('takes the largest valid object when several blocks are present', () => {
    const small = '{ "name": "small", "input": { "format": "csv", "path": "/a" } }'
    const reply = `First idea:\n\n\`\`\`json\n${small}\n\`\`\`\n\nBetter:\n\n\`\`\`json\n${PIPELINE}\n\`\`\``

    const result = extractPipelineProposal(reply)

    expect(asRecord(result.pipeline).name).toBe('clientes')
    expect(result.summary).toBe('First idea:\n\nBetter:')
  })

  it('prefers a pipeline-shaped object over a bigger unrelated one', () => {
    const noise = `{ "a": "${'x'.repeat(400)}" }`
    const reply = `\`\`\`json\n${noise}\n\`\`\`\n\n\`\`\`json\n${PIPELINE}\n\`\`\``

    const result = extractPipelineProposal(reply)

    expect(asRecord(result.pipeline).name).toBe('clientes')
  })

  it('accepts a still-open fence from a truncated stream', () => {
    const reply = `Working on it.\n\n\`\`\`json\n${PIPELINE}\n`

    const result = extractPipelineProposal(reply)

    expect(result.error).toBeUndefined()
    expect(asRecord(result.pipeline).name).toBe('clientes')
  })

  it('reports malformed JSON instead of throwing', () => {
    const reply = 'Here you go.\n\n```json\n{ "name": "broken", "input": { "format": }\n```'

    const result = extractPipelineProposal(reply)

    expect(result.pipeline).toBeNull()
    expect(result.error).toBeTruthy()
    expect(result.summary).toContain('Here you go.')
  })

  it('reports a reply with no JSON at all', () => {
    const result = extractPipelineProposal('Sparquet writes Kafka but never reads from it.')

    expect(result.pipeline).toBeNull()
    expect(result.error).toBe('The reply contained no pipeline JSON.')
    expect(result.summary).toBe('Sparquet writes Kafka but never reads from it.')
  })

  it('rejects a JSON block that is not an object', () => {
    const result = extractPipelineProposal('```json\n[1, 2, 3]\n```')

    expect(result.pipeline).toBeNull()
    expect(result.error).toBe('The reply contained JSON that is not a pipeline object.')
  })

  it('reports an empty reply', () => {
    expect(extractPipelineProposal('')).toEqual({
      pipeline: null,
      summary: '',
      error: 'The reply was empty.',
    })
    expect(extractPipelineProposal('   \n  ').error).toBe('The reply was empty.')
  })

  it('survives input that is not a string', () => {
    const result = extractPipelineProposal(undefined as unknown as string)

    expect(result.pipeline).toBeNull()
    expect(result.error).toBeTruthy()
  })

  it('keeps nested objects intact instead of picking one of them', () => {
    const reply = `\`\`\`json\n${PIPELINE}\n\`\`\``
    const pipeline = asRecord(extractPipelineProposal(reply).pipeline)

    expect(pipeline.output).toEqual({ format: 'parquet', path: '/data/out' })
  })

  it('prefers the pipeline root over a longer snippet that only carries "name"', () => {
    const snippet = `{ "type": "with_column", "name": "total", "expression": "${'a + '.repeat(100)}b" }`
    const small = '{ "name": "p", "input": { "format": "csv", "path": "/a" } }'
    const reply = `\`\`\`json\n${snippet}\n\`\`\`\n\n\`\`\`json\n${small}\n\`\`\``

    const result = extractPipelineProposal(reply)

    expect(asRecord(result.pipeline).name).toBe('p')
  })

  it('refuses a fragment that is not a pipeline root', () => {
    const reply = '```json\n{ "type": "filter", "condition": "status = 1" }\n```'

    const result = extractPipelineProposal(reply)

    expect(result.pipeline).toBeNull()
    expect(result.error).toBe('The reply contained JSON that is not a pipeline object.')
  })
})

describe('extractProposalFor', () => {
  const QUOTED = `Your input block is {"input": {"format": "delta", "path": "lastros.bronze"}} — it reads the table.`

  it('never proposes a pipeline for explain or chat, even when JSON is quoted', () => {
    for (const intent of ['explain', 'chat'] as const) {
      const result = extractProposalFor(intent, QUOTED)

      expect(result.pipeline).toBeNull()
      expect(result.error).toBeUndefined()
    }
  })

  it('still proposes a pipeline for the intents that ask for one', () => {
    for (const intent of ['generate', 'modify', 'fix', 'optimize', 'document'] as const) {
      const result = extractProposalFor(intent, `\`\`\`json\n${PIPELINE}\n\`\`\``)

      expect(asRecord(result.pipeline).name).toBe('clientes')
    }
  })
})
