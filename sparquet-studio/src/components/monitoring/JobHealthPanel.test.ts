/**
 * The two pure pieces of the health table: how a row is classified and how the
 * trend line is drawn. Both are read at a glance rather than measured, so the
 * cheap way to be wrong is to be plausible — a Job that has never run counted as
 * healthy, or a line drawn newest-first while everyone reads it oldest-first.
 */

import { describe, expect, it } from 'vitest'

import { sparklinePoints, verdictOf } from '@/components/monitoring/JobHealthPanel'
import type { JobHealth } from '@/lib/runner/monitoring'

const NOW = Date.parse('2026-09-13T12:00:00Z')
const DAY = 24 * 60 * 60 * 1000

function health(over: Partial<JobHealth> = {}): JobHealth {
  return {
    jobId: 'j1',
    name: 'Daily sales',
    workflowId: null,
    lastRunId: 'r1',
    lastStatus: 'success',
    lastStartedAt: new Date(NOW - 60_000).toISOString(),
    lastFinishedAt: new Date(NOW - 30_000).toISOString(),
    lastDurationMs: 30_000,
    lastRowsRead: 100,
    lastRowsWritten: 100,
    lastError: null,
    lastSuccessAt: new Date(NOW - 30_000).toISOString(),
    consecutiveFailures: 0,
    runs: 5,
    failures: 0,
    durations: [],
    volumes: [],
    ...over,
  }
}

describe('verdictOf', () => {
  it('calls a Job that has never run neither healthy nor failing', () => {
    expect(verdictOf(health({ lastStatus: null, lastStartedAt: null, runs: 0 }), NOW)).toBe('never')
  })

  it('reads a failure streak before anything else', () => {
    // Even if the run was minutes ago: "recent" is not "fine".
    expect(verdictOf(health({ consecutiveFailures: 1, lastStatus: 'failed' }), NOW)).toBe('failing')
  })

  it('calls a Job idle after a week of silence', () => {
    const quiet = health({ lastStartedAt: new Date(NOW - 8 * DAY).toISOString() })
    expect(verdictOf(quiet, NOW)).toBe('stale')
  })

  it('leaves a Job that ran six days ago alone', () => {
    const recent = health({ lastStartedAt: new Date(NOW - 6 * DAY).toISOString() })
    expect(verdictOf(recent, NOW)).toBe('healthy')
  })

  it('does not call a Job idle on an unparseable timestamp', () => {
    expect(verdictOf(health({ lastStartedAt: 'whenever' }), NOW)).toBe('healthy')
  })
})

describe('sparklinePoints', () => {
  it('draws nothing from nothing', () => {
    expect(sparklinePoints([])).toBe('')
  })

  it('draws a single run flat across the middle', () => {
    expect(sparklinePoints([1000], 64, 16)).toBe('0,8.0 64,8.0')
  })

  it('puts the oldest run on the left', () => {
    // The runner sends newest first; a trend is read left to right.
    expect(sparklinePoints([300, 100], 10, 10)).toBe('0.0,10.0 10.0,0.0')
  })

  it('scales to the run’s own range, not a shared one', () => {
    expect(sparklinePoints([30, 20, 10], 20, 10)).toBe('0.0,10.0 10.0,5.0 20.0,0.0')
  })

  it('keeps a flat series on screen instead of dividing by a zero range', () => {
    expect(sparklinePoints([5, 5, 5], 20, 10)).toBe('0.0,10.0 10.0,10.0 20.0,10.0')
  })
})
