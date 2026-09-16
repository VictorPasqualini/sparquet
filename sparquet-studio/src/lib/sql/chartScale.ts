/**
 * The arithmetic behind the SQL editor's chart: which columns can be plotted,
 * what a cell is worth as a number, and where the gridlines go.
 *
 * Kept out of the component because it is the part that can be wrong without
 * looking wrong — an axis that skips zero, a tick sequence on values nobody
 * would have chosen, a column offered for plotting because its strings happen
 * to parse. None of that needs a DOM to pin down, so it is tested here.
 */

import type { RunnerSchemaField } from '@/lib/runner/client'

/** Spark's own type names, as the runner reports them (`decimal(18,2)`). */
const NUMERIC = /^(tinyint|smallint|int|integer|bigint|long|short|byte|float|double|decimal|numeric)/i

/**
 * Whether a column can carry a value.
 *
 * The declared type decides it, not the data: a `decimal` column of nulls is
 * still a number column, and offering to plot a string column because its first
 * rows happen to parse is how a chart ends up averaging postcodes.
 */
export function isNumericType(type: string | undefined): boolean {
  return type !== undefined && NUMERIC.test(type.trim())
}

/**
 * Whether a result holds anything a chart could plot.
 *
 * Asked before the chart tab is offered: a chart tab over a result of strings
 * is an invitation to an empty panel.
 */
export function chartable(fields: readonly RunnerSchemaField[]): boolean {
  return fields.some((field) => isNumericType(field.type))
}

/** A cell as a number, or null when it is not one — a null included. */
export function numberOf(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null
  if (typeof value === 'number') return Number.isFinite(value) ? value : null
  if (typeof value === 'boolean') return value ? 1 : 0
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

/** A cell as an axis label. A null is drawn as one rather than dropped. */
export function labelOf(value: unknown): string {
  if (value === null || value === undefined) return '∅'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function trimZeros(text: string): string {
  return text.includes('.') ? text.replace(/\.?0+$/, '') : text
}

/**
 * Axis numbers short enough to sit in a narrow gutter: `1.2M`, `840k`, `0.5`.
 *
 * Grouped digits are right for a table cell and wrong for an axis — `1,234,567`
 * can be wider than the plot it is labelling.
 */
export function axisLabel(value: number): string {
  const size = Math.abs(value)
  if (size === 0) return '0'
  if (size >= 1e9) return `${trimZeros((value / 1e9).toFixed(1))}B`
  if (size >= 1e6) return `${trimZeros((value / 1e6).toFixed(1))}M`
  if (size >= 1e3) return `${trimZeros((value / 1e3).toFixed(1))}k`
  if (Number.isInteger(value)) return String(value)
  return trimZeros(value.toPrecision(3))
}

/**
 * Gridline values on round numbers — the ones somebody would have chosen.
 *
 * Zero is always inside the range: a bar chart whose baseline is not zero is a
 * chart that lies about proportion, so the span is stretched to reach it rather
 * than cropped to the data.
 */
export function ticksFor(max: number, min: number): number[] {
  const top = max <= 0 ? 0 : max
  const bottom = min >= 0 ? 0 : min
  if (top === bottom) return [0]
  const raw = (top - bottom) / 4
  const magnitude = 10 ** Math.floor(Math.log10(raw))
  const step =
    [1, 2, 2.5, 5, 10].map((factor) => factor * magnitude).find((size) => size >= raw) ??
    magnitude * 10
  const out: number[] = []
  for (let value = Math.floor(bottom / step) * step; value <= top + step / 2; value += step) {
    // Floating point turns 0.1 + 0.2 into a tick label nobody wants to read.
    out.push(Number(value.toFixed(10)))
  }
  return out
}
