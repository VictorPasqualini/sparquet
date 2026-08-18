import { describe, expect, it } from 'vitest'

import type { ValidationNodeData } from '@/types/studio'

import { describeRule } from './ValidationNode'

const rule = (validator: string, params: Record<string, unknown> = {}): ValidationNodeData => ({
  kind: 'validation',
  validator,
  params,
})

describe('describeRule', () => {
  it('lists the columns of a column rule', () => {
    expect(describeRule(rule('not_null', { columns: ['id', 'cpf'] }))).toBe('id, cpf')
    expect(describeRule(rule('unique', { columns: [] }))).toBeNull()
  })

  it('shows an open-ended range as an interval', () => {
    expect(describeRule(rule('range', { column: 'age', min: 0, max: 150 }))).toBe(
      'age in [0, 150]',
    )
    expect(describeRule(rule('range', { column: 'valor', min: 0 }))).toBe('valor in [0, ∞]')
    // A rule with no bound checks nothing, so only the column is worth showing.
    expect(describeRule(rule('range', { column: 'valor' }))).toBe('valor')
  })

  it('reads a SODA check as metric, scope and threshold', () => {
    expect(
      describeRule(rule('check', { metric: 'missing_percent', column: 'cpf', must_be: '< 1%' })),
    ).toBe('missing_percent(cpf) < 1%')
    expect(describeRule(rule('check', { metric: 'row_count', must_be: '> 0' }))).toBe(
      'row_count > 0',
    )
  })

  it('prefers the failed-rows query of a sql rule and keeps it on one line', () => {
    expect(
      describeRule(
        rule('sql', {
          query: 'SELECT COUNT(*) = 0 FROM _validation_df',
          failed_rows: '\nSELECT *\nFROM _validation_df\nWHERE valor < 0',
        }),
      ),
    ).toBe('SELECT *')
  })

  it('counts what a schema rule asserts', () => {
    expect(
      describeRule(
        rule('schema', { required_columns: ['id', 'valor'], column_types: { id: 'bigint' } }),
      ),
    ).toBe('2 required · 1 typed')
  })

  it('says nothing about an unknown validator', () => {
    expect(describeRule(rule('no_future_date', { column: 'dt' }))).toBeNull()
  })
})
