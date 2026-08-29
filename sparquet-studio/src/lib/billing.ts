/**
 * Billing periods, as the screens need them.
 *
 * A period is `YYYY-MM` in UTC — the runner's own definition, so that two people
 * in two timezones agree on which month a run belongs to. Everything here is
 * about turning that id into something a person reads, and about producing the
 * list of months a screen offers.
 */

/** The period a moment falls in. UTC, like the runner's `current_period()`. */
export function currentPeriod(now: Date = new Date()): string {
  return periodOf(now.getUTCFullYear(), now.getUTCMonth() + 1)
}

/**
 * `count` periods ending in the current one.
 *
 * Newest first, which is the order a picker wants; the chart reverses it. The
 * runner's timeline returns the same months oldest first, so the two are the
 * same set read from opposite ends.
 */
export function recentPeriods(count = 6, now: Date = new Date()): string[] {
  const total = Math.max(1, Math.trunc(count))
  return Array.from({ length: total }, (_, back) => {
    const date = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - back, 1))
    return periodOf(date.getUTCFullYear(), date.getUTCMonth() + 1)
  })
}

/** `2026-08` as `August 2026`, so a period reads as a month and not as an id. */
export function monthName(period: string): string {
  const parts = monthParts(period)
  if (!parts) return period
  return `${label(parts.month, 'long')} ${parts.year}`
}

/** `2026-08` as `Aug`, for an axis where the year is already implied. */
export function shortMonth(period: string): string {
  const parts = monthParts(period)
  return parts ? label(parts.month, 'short') : period
}

/**
 * `value` as a percentage of `peak`, clamped to 0–100.
 *
 * Bars are drawn against the biggest row rather than against the total: the
 * question a ranked list answers is "which of these is the large one", and
 * shares of a total leave every bar invisible as soon as there are twenty rows.
 */
export function shareOf(value: number, peak: number): number {
  if (!Number.isFinite(value) || !Number.isFinite(peak) || peak <= 0) return 0
  return Math.max(0, Math.min(100, Math.round((value / peak) * 100)))
}

/**
 * How `value` compares with `before`, in percent, or null when there is nothing
 * to compare with.
 *
 * A month that follows a month of zero has no percentage — "up 100%" from
 * nothing would be a made-up number, and the screen says "first spending"
 * instead.
 */
export function changeFrom(before: number, value: number): number | null {
  if (!Number.isFinite(before) || !Number.isFinite(value) || before <= 0) return null
  return Math.round(((value - before) / before) * 100)
}

function periodOf(year: number, month: number): string {
  return `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}`
}

function monthParts(period: string): { year: string; month: number } | null {
  const [year, month] = String(period).split('-')
  const index = Number.parseInt(month ?? '', 10)
  if (!year || !Number.isFinite(index) || index < 1 || index > 12) return null
  return { year, month: index }
}

function label(month: number, style: 'long' | 'short'): string {
  return new Date(Date.UTC(2000, month - 1, 1)).toLocaleString(undefined, { month: style })
}
