import { describe, expect, it } from 'vitest'

import type { TransformNodeData } from '@/types/studio'

import { describeParams } from './TransformNode'

const withColumn = (params: Record<string, unknown>): TransformNodeData => ({
  kind: 'transform',
  transform: 'with_column',
  params,
})

describe('describeParams · with_column', () => {
  it('previews the columns map, which overrides the single-column form', () => {
    const preview = describeParams(
      withColumn({
        column: 'valor_liquido',
        expression: 'valor_total - valor_desconto',
        columns: { base: 'valor_total' },
      }),
    )
    expect(preview?.text).toBe('base = valor_total')
  })

  it('counts the entries when the map has several', () => {
    const preview = describeParams(
      withColumn({ column: 'valor_liquido', columns: { base: 'a', imposto: 'base * 0.15' } }),
    )
    expect(preview?.text).toBe('2 columns')
  })

  it('falls back to the single-column form when the map is empty', () => {
    const preview = describeParams(
      withColumn({ column: 'valor_liquido', expression: 'valor_total', columns: {} }),
    )
    expect(preview?.text).toBe('valor_liquido = valor_total')
  })
})
