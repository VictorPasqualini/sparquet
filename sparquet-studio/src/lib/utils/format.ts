/**
 * Display formatters shared by the library screens.
 *
 * Dependency-free on purpose: these run in render paths and in tests, so they
 * must never reach for the DOM, the store or a date library.
 */

const MINUTE = 60_000
const HOUR = 60 * MINUTE
const DAY = 24 * HOUR

/** Day and month, e.g. `12 Mar`. `en-GB` puts the day first without a comma. */
const DAY_MONTH = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short' })
const DAY_MONTH_YEAR = new Intl.DateTimeFormat('en-GB', {
  day: 'numeric',
  month: 'short',
  year: 'numeric',
})

/**
 * Compact, human timestamp: `just now`, `3 min ago`, `5 hours ago`, `yesterday`,
 * `4 days ago`, `12 Mar`, `12 Mar 2024`.
 */
export function relativeTime(ts: number): string {
  if (!Number.isFinite(ts) || ts <= 0) return 'never'

  const now = Date.now()
  const elapsed = now - ts

  // Clock skew can put a stored timestamp slightly ahead of the browser clock.
  if (elapsed < 45_000) return 'just now'
  if (elapsed < HOUR) return `${Math.max(1, Math.floor(elapsed / MINUTE))} min ago`

  const days = calendarDaysApart(ts, now)
  // Hours stay useful just after midnight, when the calendar day already flipped.
  if (days === 0 || elapsed < 12 * HOUR) {
    return `${plural(Math.max(1, Math.floor(elapsed / HOUR)), 'hour')} ago`
  }
  if (days === 1) return 'yesterday'
  if (days < 7) return `${plural(days, 'day')} ago`

  const date = new Date(ts)
  const sameYear = date.getFullYear() === new Date(now).getFullYear()
  return (sameYear ? DAY_MONTH : DAY_MONTH_YEAR).format(date)
}

/** `plural(1, 'node')` → `1 node`; `plural(3, 'node')` → `3 nodes`. */
export function plural(count: number, singular: string, pluralForm?: string): string {
  return `${count} ${count === 1 ? singular : (pluralForm ?? `${singular}s`)}`
}

/** Keeps both ends of a long value readable: `/data/bronze/…/clientes`. */
export function truncateMiddle(text: string, max = 40): string {
  if (text.length <= max) return text
  if (max <= 1) return '…'

  const head = Math.ceil((max - 1) / 2)
  const tail = max - 1 - head
  return `${text.slice(0, head)}…${tail > 0 ? text.slice(text.length - tail) : ''}`
}

/** Whole days between two instants, compared as calendar days (DST-safe). */
function calendarDaysApart(from: number, to: number): number {
  return Math.round((startOfDay(to) - startOfDay(from)) / DAY)
}

function startOfDay(ts: number): number {
  const date = new Date(ts)
  return new Date(date.getFullYear(), date.getMonth(), date.getDate()).getTime()
}
