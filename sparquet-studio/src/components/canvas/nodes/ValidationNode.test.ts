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

  it('reads a metric rule as scope and threshold', () => {
    expect(describeRule(rule('missing_percent', { column: 'cpf', must_be: '< 1%' }))).toBe(
      'missing_percent(cpf) < 1%',
    )
    expect(describeRule(rule('duplicate_count', { columns: ['id', 'dt'], must_be: '= 0' }))).toBe(
      'duplicate_count(id, dt) = 0',
    )
    // An aggregate that measures no column is just the metric and its threshold.
    expect(describeRule(rule('avg', { column: 'valor', must_be: 'between 0 and 10' }))).toBe(
      'avg(valor) between 0 and 10',
    )
  })

  it('describes a multi-target rule by its targets, not by the shared defaults', () => {
    // The rule runs once per target, so the columns it measures are the targets'.
    expect(
      describeRule(
        rule('regex', {
          targets: [
            { column: 'cpf', pattern: '^[0-9]{11}$' },
            { column: 'cnpj', pattern: '^[0-9]{14}$' },
          ],
        }),
      ),
    ).toBe('2 targets · cpf, cnpj')
    expect(describeRule(rule('not_null', { targets: [{ columns: ['id'] }] }))).toBe('1 target · id')
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

  it('still says what an unknown validator measures', () => {
    // A custom rule registered with `fw.register_validator` imports as an unknown type;
    // the column it names is the only thing knowable, and it is worth showing.
    expect(describeRule(rule('no_future_date', { column: 'dt' }))).toBe('no_future_date(dt)')
  })
})
