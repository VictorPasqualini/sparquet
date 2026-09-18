import { describe, expect, it } from 'vitest'

import { scheduleRank, sortSchedules, timeUntil } from './SchedulesPanel'
import type { ScheduleEntry } from '@/lib/runner/scheduling'

function entry(over: Partial<ScheduleEntry> = {}): ScheduleEntry {
  return {
    kind: 'job',
    id: 'j1',
    name: 'Daily sales',
    cron: '0 6 * * *',
    timezone: 'local',
    enabled: true,
    runAs: 'ana',
    workflowId: 'wf1',
    error: null,
    rule: '0 6 * * *',
    nextFire: '2026-09-14T09:00:00Z',
    lastFire: null,
    lastRunId: null,
    lastStatus: null,
    ...over,
  }
}

const NOW = Date.parse('2026-09-13T12:00:00Z')

describe('timeUntil', () => {
  it('counts forwards, which is the direction this column reads in', () => {
    expect(timeUntil(NOW + 30_000, NOW)).toBe('in under a minute')
    expect(timeUntil(NOW + 20 * 60_000, NOW)).toBe('in 20 min')
    expect(timeUntil(NOW + 5 * 3_600_000, NOW)).toBe('in 5 h')
  })

  it('says an overdue schedule is due rather than pretending it is ahead', () => {
    expect(timeUntil(NOW - 60_000, NOW)).toBe('due now')
  })

  it('gives a date once the hours stop meaning anything', () => {
    expect(timeUntil(Date.parse('2026-09-20T09:00:00Z'), NOW)).not.toMatch(/ h$/)
  })

  it('has nothing to say about a schedule with no next firing', () => {
    expect(timeUntil(0, NOW)).toBe('—')
  })
})

describe('sortSchedules', () => {
  it('puts what cannot fire first, then what is paused, then what fires soonest', () => {
    const broken = entry({ id: 'broken', error: 'not a schedule: @daily', nextFire: null })
    const paused = entry({ id: 'paused', enabled: false, nextFire: null })
    const soon = entry({ id: 'soon', nextFire: '2026-09-13T13:00:00Z' })
    const later = entry({ id: 'later', nextFire: '2026-09-14T09:00:00Z' })

    expect(sortSchedules([later, paused, soon, broken]).map((item) => item.id)).toEqual([
      'broken',
      'paused',
      'soon',
      'later',
    ])
  })

  it('falls back to the name so the order never depends on arrival', () => {
    const a = entry({ id: 'a', name: 'Alpha' })
    const b = entry({ id: 'b', name: 'Beta' })

    expect(sortSchedules([b, a]).map((item) => item.name)).toEqual(['Alpha', 'Beta'])
  })

  it('leaves the list it was given alone', () => {
    const list = [entry({ id: 'later', nextFire: '2026-09-14T09:00:00Z' }), entry({ id: 'broken', error: 'bad' })]

    sortSchedules(list)

    expect(list[0].id).toBe('later')
  })
})

describe('scheduleRank', () => {
  it('reads a broken schedule as more urgent than a paused one', () => {
    expect(scheduleRank(entry({ error: 'bad' }))).toBeLessThan(scheduleRank(entry({ enabled: false })))
  })
})
