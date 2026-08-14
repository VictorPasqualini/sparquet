import { describe, expect, it } from 'vitest'

import { getTransformation } from '@/catalog'
import type { FieldSpec } from '@/catalog/types'
import { buildSystemPrompt } from '@/lib/ai/prompt'
import { lintWorkflow } from '@/lib/validation/lint'
import { HANDLE } from '@/types/studio'
import type { StudioEdge, StudioNode, WorkflowSettings } from '@/types/studio'

const SETTINGS: WorkflowSettings = { pipelineName: 'test', description: '', spark: {} }

const link = (from: string, to: string): StudioEdge => ({
  id: `${from}->${to}`,
  source: from,
  target: to,
  sourceHandle: HANDLE.out,
  targetHandle: HANDLE.in,
})

/** source → the transformation under test → sink. */
const lintOne = (type: string, params: Record<string, unknown>): string[] => {
  const nodes: StudioNode[] = [
    {
      id: 'src',
      type: 'source',
      position: { x: 0, y: 0 },
      data: { kind: 'source', format: 'parquet', path: '/in', options: {} },
    },
    {
      id: 'node',
      type: 'transform',
      position: { x: 0, y: 0 },
      data: { kind: 'transform', transform: type, params },
    },
    {
      id: 'out',
      type: 'sink',
      position: { x: 0, y: 0 },
      data: {
        kind: 'sink',
        format: 'parquet',
        path: '/out',
        mode: 'overwrite',
        partitionBy: [],
        columns: null,
        options: {},
      },
    },
  ]
  const edges = [link('src', 'node'), link('node', 'out')]
  return lintWorkflow({ nodes, edges }, SETTINGS, [])
    .filter((issue) => issue.nodeId === 'node' && issue.severity === 'error')
    .map((issue) => issue.field ?? '')
}

const fieldsOf = (type: string): FieldSpec[] => getTransformation(type)?.fields ?? []

const fieldOf = (type: string, key: string): FieldSpec => {
  const field = fieldsOf(type).find((entry) => entry.key === key)
  if (!field) throw new Error(`${type} has no "${key}" field`)
  return field
}

const promptLine = (type: string): string => {
  const line = buildSystemPrompt()
    .split('\n')
    .find((entry) => entry.startsWith(`- ${type} —`))
  if (!line) throw new Error(`no prompt line for ${type}`)
  return line
}

describe('with_column precedence', () => {
  const columnField = fieldOf('with_column', 'column')
  const expressionField = fieldOf('with_column', 'expression')
  const mapField = fieldOf('with_column', 'columns')

  it('treats a present columns key as the multi-column form, empty map included', () => {
    const params = { column: 'total', expression: 'qtd * preco', columns: {} }
    expect(columnField.visibleWhen?.(params)).toBe(false)
    expect(expressionField.visibleWhen?.(params)).toBe(false)
    expect(mapField.validate?.({}, params)).toContain('empty columns map')
  })

  it('keeps the single-column form when columns is absent or null', () => {
    for (const params of [{ column: 'total' }, { column: 'total', columns: null }]) {
      expect(columnField.visibleWhen?.(params)).toBe(true)
      expect(mapField.validate?.(params.columns, params)).toBeNull()
    }
  })

  it('rejects a columns value that is not a map of expressions', () => {
    const params = { columns: ['total'] }
    expect(mapField.validate?.(params.columns, params)).toBeTruthy()
  })

  it('accepts a populated map', () => {
    const params = { columns: { base: 'valor', total: 'base * 1.1' } }
    expect(mapField.validate?.(params.columns, params)).toBeNull()
  })

  it('applies the same rule to the add_column alias', () => {
    const alias = fieldOf('add_column', 'columns')
    expect(alias.validate?.({}, { columns: {} })).toBeTruthy()
  })

  it('lints a node whose empty map silently disables the single-column keys', () => {
    expect(
      lintOne('with_column', { column: 'total', expression: 'qtd * preco', columns: {} }),
    ).toEqual(['columns'])
    expect(lintOne('with_column', { column: 'total', expression: 'qtd * preco' })).toEqual([])
  })
})

describe('debug nested chain', () => {
  it('exposes the key the engine reads', () => {
    expect(fieldOf('debug', 'transformations').type).toBe('json')
  })

  it('never advertises the join-only with_transformations key', () => {
    expect(getTransformation('debug')?.supportsSubPipeline).toBeFalsy()
    const line = promptLine('debug')
    expect(line).toContain('transformations')
    expect(line).not.toContain('with_transformations')
    expect(promptLine('join')).toContain('with_transformations')
  })

  it('rejects a preview chain that is not a list of transformation objects', () => {
    const field = fieldOf('debug', 'transformations')
    expect(field.validate?.({ type: 'filter' }, {})).toBeTruthy()
    expect(field.validate?.(['filter'], {})).toBeTruthy()
    expect(field.validate?.([{ type: 'filter', condition: 'a > 1' }], {})).toBeNull()
    expect(field.validate?.(undefined, {})).toBeNull()
  })
})

describe('join alias documentation', () => {
  it('scopes the l./r. restriction to the right-side chain', () => {
    const def = getTransformation('join')
    const prose = [...(def?.gotchas ?? []), fieldOf('join', 'on').docs ?? ''].join('\n')
    expect(prose).not.toMatch(/valid only in `on`/)
    expect(prose).not.toMatch(/exist only here/)
    expect(prose).toMatch(/right-side chain/)
  })
})
