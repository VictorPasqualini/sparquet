import { describe, expect, it } from 'vitest'

import {
  changeFrom,
  currentPeriod,
  monthName,
  recentPeriods,
  shareOf,
  shortMonth,
} from '@/lib/billing'

/**
 * A period is UTC on both sides. Testing it from a fixed instant is the only way
 * to catch the classic bug: a browser west of Greenwich reading `getMonth()` and
 * asking the runner for the wrong month for the first hours of every month.
 */
describe('currentPeriod', () => {
  it('is the UTC month', () => {
    expect(currentPeriod(new Date('2026-08-29T12:00:00Z'))).toBe('2026-08')
  })

  it('does not fall back a month for a late-evening local time', () => {
    expect(currentPeriod(new Date('2026-09-01T00:30:00Z'))).toBe('2026-09')
  })
})

describe('recentPeriods', () => {
  it('walks back from the current month, newest first', () => {
    expect(recentPeriods(6, new Date('2026-02-15T00:00:00Z'))).toEqual([
      '2026-02',
      '2026-01',
      '2025-12',
      '2025-11',
      '2025-10',
      '2025-09',
    ])
  })

  it('always offers at least the current month', () => {
    expect(recentPeriods(0, new Date('2026-02-15T00:00:00Z'))).toEqual(['2026-02'])
  })
})

describe('monthName and shortMonth', () => {
  it('reads a period as a month', () => {
    expect(monthName('2026-08')).toMatch(/2026$/)
    expect(monthName('2026-08')).not.toBe('2026-08')
    expect(shortMonth('2026-08')).not.toContain('2026')
  })

  it('gives back anything it cannot read', () => {
    expect(monthName('nonsense')).toBe('nonsense')
    expect(monthName('2026-13')).toBe('2026-13')
    expect(shortMonth('')).toBe('')
  })
})

describe('shareOf', () => {
  it('is a percentage of the peak', () => {
    expect(shareOf(5, 20)).toBe(25)
    expect(shareOf(20, 20)).toBe(100)
  })

  it('is zero rather than infinite when there is no peak', () => {
    expect(shareOf(5, 0)).toBe(0)
    expect(shareOf(5, Number.NaN)).toBe(0)
  })

  it('never draws a bar past the end of its track', () => {
    expect(shareOf(50, 20)).toBe(100)
    expect(shareOf(-5, 20)).toBe(0)
  })
})

describe('changeFrom', () => {
  it('compares with the month before', () => {
    expect(changeFrom(100, 130)).toBe(30)
    expect(changeFrom(100, 40)).toBe(-60)
  })

  it('has no answer when there was nothing to grow from', () => {
    expect(changeFrom(0, 40)).toBeNull()
    expect(changeFrom(Number.NaN, 40)).toBeNull()
  })
})
