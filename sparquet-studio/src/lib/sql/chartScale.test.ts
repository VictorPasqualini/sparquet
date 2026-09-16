import { describe, expect, it } from 'vitest'

import {
  axisLabel,
  chartable,
  isNumericType,
  labelOf,
  numberOf,
  ticksFor,
} from './chartScale'

const field = (name: string, type: string) => ({ name, type, nullable: true })

describe('isNumericType', () => {
  it('accepts the Spark type names a chart can plot', () => {
    for (const type of ['int', 'bigint', 'double', 'float', 'decimal(18,2)', 'SMALLINT']) {
      expect(isNumericType(type), type).toBe(true)
    }
  })

  it('refuses the ones it cannot', () => {
    for (const type of ['string', 'date', 'timestamp', 'boolean', 'array<int>', undefined]) {
      expect(isNumericType(type), String(type)).toBe(false)
    }
  })

  it('does not mistake a struct of numbers for a number', () => {
    expect(isNumericType('struct<total:double>')).toBe(false)
  })
})

describe('chartable', () => {
  it('is true when one column is a number', () => {
    expect(chartable([field('month', 'string'), field('orders', 'bigint')])).toBe(true)
  })

  it('is false for a result of strings, so the tab is never offered', () => {
    expect(chartable([field('month', 'string'), field('status', 'string')])).toBe(false)
  })

  it('is false for a result with no columns at all', () => {
    expect(chartable([])).toBe(false)
  })
})

describe('numberOf', () => {
  it('reads numbers, numeric strings and booleans', () => {
    expect(numberOf(42)).toBe(42)
    expect(numberOf('3.5')).toBe(3.5)
    expect(numberOf(true)).toBe(1)
    expect(numberOf(false)).toBe(0)
  })

  it('returns null for anything that is not a number', () => {
    expect(numberOf(null)).toBeNull()
    expect(numberOf(undefined)).toBeNull()
    expect(numberOf('')).toBeNull()
    expect(numberOf('n/a')).toBeNull()
    expect(numberOf(Number.NaN)).toBeNull()
    expect(numberOf(Number.POSITIVE_INFINITY)).toBeNull()
  })
})

describe('labelOf', () => {
  it('draws a null as one rather than as an empty gap', () => {
    expect(labelOf(null)).toBe('∅')
    expect(labelOf(undefined)).toBe('∅')
  })

  it('serialises a struct instead of printing [object Object]', () => {
    expect(labelOf({ region: 'south' })).toBe('{"region":"south"}')
  })
})

describe('axisLabel', () => {
  it('shortens big numbers so they fit the gutter', () => {
    expect(axisLabel(1_200_000)).toBe('1.2M')
    expect(axisLabel(840_000)).toBe('840k')
    expect(axisLabel(2_000_000_000)).toBe('2B')
  })

  it('leaves small numbers alone', () => {
    expect(axisLabel(0)).toBe('0')
    expect(axisLabel(7)).toBe('7')
    expect(axisLabel(0.5)).toBe('0.5')
  })

  it('keeps the sign of a negative tick', () => {
    expect(axisLabel(-1500)).toBe('-1.5k')
  })
})

describe('ticksFor', () => {
  it('lands on round numbers', () => {
    expect(ticksFor(97, 0)).toEqual([0, 25, 50, 75, 100])
  })

  it('always reaches zero, so a bar is drawn in proportion', () => {
    expect(ticksFor(120, 100)).toContain(0)
  })

  it('spans both sides when values go negative', () => {
    const ticks = ticksFor(20, -20)
    expect(ticks[0]).toBeLessThanOrEqual(-20)
    expect(ticks[ticks.length - 1]).toBeGreaterThanOrEqual(20)
    expect(ticks).toContain(0)
  })

  it('collapses to a single tick when every value is zero', () => {
    expect(ticksFor(0, 0)).toEqual([0])
  })

  it('does not leak floating point noise into a label', () => {
    for (const tick of ticksFor(1, 0)) {
      expect(String(tick).length).toBeLessThan(8)
    }
  })
})
