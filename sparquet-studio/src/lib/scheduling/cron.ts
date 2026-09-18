/**
 * The cron dialect a schedule is written in, checked in the browser.
 *
 * The runner is the authority on when a schedule fires — it owns the clock and
 * the timezone database. What it cannot do is tell somebody typing an expression
 * that it is wrong *while they type it*, and a schedule saved with an unreadable
 * expression is a Job that silently never runs. So this mirrors the runner's
 * `scheduling.parse_cron` closely enough to answer one question: would the runner
 * accept this? It never answers "when does it fire next" — that stays on the
 * side that has the zone rules.
 *
 * Five fields, like the runner: minute, hour, day of month, month, day of week.
 * Six-field expressions are refused rather than interpreted, because reading
 * `0 0 6 * * *` as minute-zero-of-hour-zero would run a daily job every minute.
 */

const MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
const WEEKDAYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat']

const FIELD = /^[0-9a-z*/,-]+$/

interface FieldSpec {
  label: string
  low: number
  high: number
  names?: readonly string[]
}

const FIELDS: readonly FieldSpec[] = [
  { label: 'minute', low: 0, high: 59 },
  { label: 'hour', low: 0, high: 23 },
  { label: 'day of month', low: 1, high: 31 },
  { label: 'month', low: 1, high: 12, names: MONTHS },
  { label: 'day of week', low: 0, high: 7, names: WEEKDAYS },
]

/** The timezone value meaning "the clock of the machine running the runner". */
export const LOCAL_ZONE = 'local'

function resolveValue(token: string, spec: FieldSpec): number | null {
  const text = token.trim().toLowerCase()
  if (text.length === 0) return null
  if (spec.names) {
    const named = spec.names.indexOf(text)
    if (named >= 0) return spec.label === 'month' ? named + 1 : named
  }
  if (!/^\d+$/.test(text)) return null
  const value = Number(text)
  if (value < spec.low || value > spec.high) return null
  return value
}

function checkPart(part: string, spec: FieldSpec): string | null {
  const [range, step] = part.split('/', 2)
  if (part.split('/').length > 2) return `${spec.label}: "${part}" has more than one step.`
  if (step !== undefined) {
    if (!/^\d+$/.test(step) || Number(step) === 0) {
      return `${spec.label}: "${step}" is not a step. Use a positive number after "/".`
    }
  }
  if (range === '*') return null
  const bounds = range.split('-')
  if (bounds.length > 2) return `${spec.label}: "${range}" is not a range.`
  for (const bound of bounds) {
    if (resolveValue(bound, spec) === null) {
      return `${spec.label}: "${bound}" is not a value between ${spec.low} and ${spec.high}.`
    }
  }
  if (bounds.length === 2) {
    const from = resolveValue(bounds[0], spec) as number
    const to = resolveValue(bounds[1], spec) as number
    if (from > to) return `${spec.label}: the range "${range}" ends before it starts.`
  }
  return null
}

function checkField(text: string, spec: FieldSpec): string | null {
  const field = text.trim().toLowerCase()
  if (!FIELD.test(field)) return `${spec.label}: "${text}" has a character cron does not read.`
  for (const part of field.split(',')) {
    if (part.length === 0) return `${spec.label}: "${text}" has an empty item in its list.`
    const problem = checkPart(part, spec)
    if (problem) return problem
  }
  return null
}

/**
 * The reason the runner would refuse this expression, or null if it would take it.
 *
 * The message is meant to be shown as typed feedback, so it names the field it
 * is complaining about — "day of week" is much more useful than "invalid cron".
 */
export function validateCron(expression: string): string | null {
  const fields = expression.trim().toLowerCase().split(/\s+/).filter(Boolean)
  if (fields.length === 0) return 'Write a schedule, like "0 6 * * *" for every day at six.'
  if (expression.trim().startsWith('@')) {
    return 'Macros like "@daily" are not read here. Write the five fields: "0 0 * * *".'
  }
  if (fields.length !== 5) {
    return `A schedule has five fields — minute, hour, day of month, month, day of week. This has ${fields.length}.`
  }
  for (let index = 0; index < FIELDS.length; index += 1) {
    const problem = checkField(fields[index], FIELDS[index])
    if (problem) return problem
  }
  return null
}

function zoneSuffix(timezone?: string): string {
  const zone = (timezone ?? '').trim()
  if (zone.length === 0 || zone.toLowerCase() === LOCAL_ZONE) return ''
  return ` (${zone})`
}

function hourText(minute: string, hour: string): string | null {
  if (!/^\d+$/.test(minute) || !/^\d+$/.test(hour)) return null
  return `${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`
}

/**
 * The expression in words, for the surfaces that show a schedule without asking
 * the runner. Anything this does not recognise is shown as written — an
 * expression somebody typed is never less clear than a bad paraphrase of it.
 */
export function describeCron(expression: string, timezone?: string): string {
  const raw = expression.trim()
  const problem = validateCron(raw)
  if (problem) return raw.length > 0 ? raw : 'No schedule'
  const [minute, hour, day, month, weekday] = raw.toLowerCase().split(/\s+/)
  const suffix = zoneSuffix(timezone)
  const clock = hourText(minute, hour)

  if (minute === '*' && hour === '*' && day === '*' && month === '*' && weekday === '*') {
    return 'Every minute'
  }
  if (clock && day === '*' && month === '*' && weekday === '*') {
    return `Every day at ${clock}${suffix}`
  }
  if (clock && day === '*' && month === '*' && WEEKDAYS.includes(weekday)) {
    const name = weekday.charAt(0).toUpperCase() + weekday.slice(1)
    return `Every ${name} at ${clock}${suffix}`
  }
  if (clock && day === '*' && month === '*' && weekday === '1-5') {
    return `Every weekday at ${clock}${suffix}`
  }
  if (clock && month === '*' && weekday === '*' && /^\d+$/.test(day)) {
    return `On day ${day} of every month at ${clock}${suffix}`
  }
  if (hour === '*' && day === '*' && month === '*' && weekday === '*' && /^\d+$/.test(minute)) {
    return `Every hour at minute ${minute}${suffix}`
  }
  return `${raw}${suffix}`
}
