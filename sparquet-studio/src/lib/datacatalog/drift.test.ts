import { describe, expect, it } from 'vitest'

import { compareSchema, normalizeType } from './drift'
import type { DatasetSchema } from './schema'

const schemaOf = (
  fields: DatasetSchema['fields'],
  confidence: DatasetSchema['confidence'] = 'complete',
): DatasetSchema => ({
  key: '/lake/silver/orders',
  fields,
  confidence,
  source: null,
})

describe('normalizeType', () => {
  it('folds the spellings Spark and a pipeline use for one type', () => {
    expect(normalizeType('BIGINT')).toBe('long')
    expect(normalizeType('integer')).toBe('int')
    expect(normalizeType('decimal(18, 2)')).toBe('decimal(18,2)')
  })

  it('keeps two genuinely different types apart', () => {
    expect(normalizeType('int')).not.toBe(normalizeType('long'))
    expect(normalizeType('decimal(18,2)')).not.toBe(normalizeType('decimal(10,0)'))
  })
})

describe('compareSchema', () => {
  it('calls a type written the other way a match', () => {
    const drift = compareSchema(schemaOf([{ name: 'id', type: 'long', origin: 'cast' }]), [
      { name: 'id', type: 'bigint', nullable: false },
    ])

    expect(drift.rows[0].status).toBe('match')
    expect(drift.drifted).toBe(false)
  })

  it('names a type that really changed', () => {
    const drift = compareSchema(
      schemaOf([{ name: 'amount', type: 'decimal(18,2)', origin: 'cast' }]),
      [{ name: 'amount', type: 'double', nullable: true }],
    )

    expect(drift.rows[0]).toMatchObject({ status: 'type', derived: 'decimal(18,2)', actual: 'double' })
    expect(drift.drifted).toBe(true)
  })

  it('reports a column the canvas states and the dataset lacks', () => {
    const drift = compareSchema(schemaOf([{ name: 'segment', type: null, origin: 'joined' }]), [
      { name: 'id', type: 'bigint', nullable: false },
    ])

    expect(drift.missing).toBe(1)
    expect(drift.extra).toBe(1)
    expect(drift.drifted).toBe(true)
  })

  it('does not call an extra column drift while the derived schema is partial', () => {
    const drift = compareSchema(
      schemaOf([{ name: 'id', type: 'long', origin: 'cast' }], 'partial'),
      [
        { name: 'id', type: 'bigint', nullable: false },
        { name: 'customer_id', type: 'string', nullable: true },
      ],
    )

    expect(drift.extra).toBe(1)
    expect(drift.drifted).toBe(false)
  })

  it('has nothing to compare when the canvas states no type', () => {
    const drift = compareSchema(schemaOf([{ name: 'id', type: null, origin: 'projected' }]), [
      { name: 'id', type: 'bigint', nullable: true },
    ])

    expect(drift.rows[0].status).toBe('unstated')
    expect(drift.drifted).toBe(false)
  })

  it('matches names the way Spark resolves them, ignoring case', () => {
    const drift = compareSchema(schemaOf([{ name: 'Amount', type: 'double', origin: 'cast' }]), [
      { name: 'amount', type: 'double', nullable: true },
    ])

    expect(drift.rows).toHaveLength(1)
    expect(drift.rows[0].status).toBe('match')
  })

  it('describes a dataset nothing was derived for as all extra', () => {
    const drift = compareSchema(null, [{ name: 'id', type: 'bigint', nullable: false }])

    expect(drift.rows.map((row) => row.status)).toEqual(['extra'])
    expect(drift.drifted).toBe(false)
  })
})
