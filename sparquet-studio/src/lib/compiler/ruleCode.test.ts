import { describe, expect, it } from 'vitest'

import { ruleCode } from './ruleCode'
import type { StudioNode } from '@/types/studio'

const rule = (validator: string, params: Record<string, unknown>): StudioNode =>
  ({
    id: 'n1',
    type: 'validation',
    position: { x: 0, y: 0 },
    data: { kind: 'validation', validator, params },
  }) as StudioNode

describe('ruleCode', () => {
  it('prefers a declared code, trimmed', () => {
    expect(ruleCode(rule('range', { column: 'age', min: 1, max: 99, code: ' AGE ' }))).toBe('AGE')
  })

  it('treats a blank declared code as absent', () => {
    expect(ruleCode(rule('not_null', { columns: ['email'], code: '  ' }))).toBe('not_null(email)')
  })

  /**
   * The same cases `sparquet_cola`'s own test pins. These strings land in the data,
   * so a difference between the two implementations is a real bug: a scoped
   * quarantine would silently match no rule.
   */
  it('derives the same expressions the library derives', () => {
    const cases: [StudioNode, string][] = [
      [rule('not_null', { columns: ['email'] }), 'not_null(email)'],
      [rule('not_null', { columns: ['id', 'cpf'] }), 'not_null(id,cpf)'],
      [rule('unique', { columns: ['id'] }), 'unique(id)'],
      [rule('unique', { columns: ['id', 'dt'] }), 'unique(id,dt)'],
      [rule('range', { column: 'age', min: 1, max: 99 }), 'range(age,1,99)'],
      [rule('range', { column: 'valor', min: 0 }), 'range(valor,0,*)'],
      [rule('range', { column: 'valor', max: 10 }), 'range(valor,*,10)'],
      [rule('regex', { column: 'email', pattern: '^.+@.+$' }), 'regex(email,^.+@.+$)'],
      [rule('check', { metric: 'missing_percent', column: 'cpf' }), 'missing_percent(cpf)'],
      [rule('check', { metric: 'invalid_count', columns: ['email'] }), 'invalid_count(email)'],
      [rule('check', { metric: 'row_count' }), 'row_count'],
      [rule('row_count', { min: 1 }), 'row_count'],
      [rule('schema', { required_columns: ['id'] }), 'schema'],
      [rule('sql', { query: 'SELECT true' }), 'sql'],
    ]
    for (const [node, expected] of cases) expect(ruleCode(node)).toBe(expected)
  })
})
