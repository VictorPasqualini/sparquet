import { describe, expect, it } from 'vitest'

import { ruleCode, ruleCodes } from './ruleCode'
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
      [rule('unique', { columns: ['id', 'dt'] }), 'unique(id,dt)'],
      [rule('range', { column: 'age', min: 1, max: 99 }), 'range(age,1,99)'],
      [rule('range', { column: 'valor', min: 0 }), 'range(valor,0,*)'],
      [rule('range', { column: 'valor', max: 10 }), 'range(valor,*,10)'],
      [rule('regex', { column: 'email', pattern: '^.+@.+$' }), 'regex(email,^.+@.+$)'],
      // Metrics are rule types: the code is the metric plus the column measured.
      [rule('missing_percent', { column: 'cpf', must_be: '< 1%' }), 'missing_percent(cpf)'],
      [rule('invalid_count', { columns: ['email'], must_be: '= 0' }), 'invalid_count(email)'],
      [rule('duplicate_count', { columns: ['id'], must_be: '= 0' }), 'duplicate_count(id)'],
      [rule('avg', { column: 'valor', must_be: '> 0' }), 'avg(valor)'],
      [rule('row_count', { min: 1 }), 'row_count'],
      [rule('freshness', { column: 'dt', must_be: '< 1d' }), 'freshness(dt)'],
      [rule('schema', { required_columns: ['id'] }), 'schema'],
      [rule('sql', { query: 'SELECT true' }), 'sql'],
    ]
    for (const [node, expected] of cases) expect(ruleCode(node)).toBe(expected)
  })
})

describe('ruleCodes', () => {
  it('answers with one code when the rule declares no targets', () => {
    expect(ruleCodes(rule('not_null', { column: 'email' }))).toEqual(['not_null(email)'])
  })

  /**
   * The user's own case: one entry, two documents, two different patterns. The library
   * expands it into two rules, so the node answers with two codes — scoping a
   * quarantine to it must scope to both.
   */
  it('derives one code per target, with the shared fields applied', () => {
    const node = rule('regex', {
      targets: [
        { column: 'document', pattern: '^[0-9]{11}$' },
        { column: 'document2', pattern: '^[0-9]{12}$' },
      ],
    })
    expect(ruleCodes(node)).toEqual(['regex(document,^[0-9]{11}$)', 'regex(document2,^[0-9]{12}$)'])
  })

  it('lets a target override a shared default and declare its own code', () => {
    const node = rule('range', {
      min: 0,
      targets: [{ column: 'valor', max: 100 }, { column: 'taxa', code: 'TAXA' }],
    })
    expect(ruleCodes(node)).toEqual(['range(valor,0,100)', 'TAXA'])
  })

  it('keeps a parent code visible instead of hiding a config the library refuses', () => {
    // `code` beside `targets` is an error in `expand_targets` — every expanded rule
    // would carry the same identifier. The linter reports it; swallowing it here
    // would make the canvas look fine.
    const node = rule('regex', { code: 'DOC', targets: [{ column: 'a' }, { column: 'b' }] })
    expect(ruleCodes(node)).toEqual(['DOC', 'DOC'])
  })

  it('falls back to the parent rule when a target is not an object', () => {
    // The library REFUSES a bare value as a target (it cannot tell which key it
    // fills). Studio cannot throw from a render path, and silently keeping the valid
    // targets would show a canvas that compiles to a config the framework rejects —
    // so the whole list is disregarded and the linter points at the field.
    const node = rule('not_null', { targets: ['cpf', { column: 'email' }] })
    expect(ruleCodes(node)).toEqual(['not_null()'])
  })

  it('falls back to the plain rule when targets is empty', () => {
    expect(ruleCodes(rule('not_null', { column: 'email', targets: [] }))).toEqual([
      'not_null(email)',
    ])
  })

  it('is the plural of ruleCode for a single-target rule', () => {
    const node = rule('regex', { targets: [{ column: 'cpf', pattern: '^[0-9]+$' }] })
    expect(ruleCode(node)).toBe(ruleCodes(node)[0])
  })
})
