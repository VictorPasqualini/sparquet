/**
 * A chart over whatever the last query returned.
 *
 * A grid answers "what are the numbers"; a chart answers "what shape are they",
 * and that second question is the one a `GROUP BY month` is usually asked to
 * settle. Between reading forty rows and seeing forty bars there is no contest,
 * and the alternative today is exporting a CSV into a spreadsheet — which means
 * the answer leaves the tool that holds the data.
 *
 * Deliberately small. No chart library, no axis editor, no pivot: the result is
 * already shaped by SQL, and the SQL is right there to reshape it. What this
 * adds is a picture of the rows as they came back.
 *
 * Drawn as plain SVG for the same reason the run rail is plain divs — a
 * dependency that renders one bar chart is a dependency that has to be upgraded
 * forever. Colours come from the same tokens as everything else, so it follows
 * light and dark without a second palette.
 */

import { ChartColumn, ChartLine } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'

import { Field, Segmented, Select } from '@/components/ui'
import type { RunnerSchemaField } from '@/lib/runner/client'
import { axisLabel, isNumericType, labelOf, numberOf, ticksFor } from '@/lib/sql/chartScale'
import { cn } from '@/lib/utils/cn'
import { formatCount } from '@/lib/utils/format'

/**
 * Points drawn at most.
 *
 * Past this a bar is thinner than the gap beside it and the picture stops being
 * one. The cap is announced rather than silent: a chart of the first 200 of
 * 5,000 rows is a different claim from a chart of the result.
 */
const MAX_POINTS = 200

const PAD = { top: 12, right: 12, bottom: 26, left: 56 }
const HEIGHT = 240

export type ChartShape = 'bar' | 'line'

export interface ResultChartProps {
  columns: string[]
  rows: unknown[][]
  fields: RunnerSchemaField[]
}

export function ResultChart({ columns, rows, fields }: ResultChartProps) {
  const [shape, setShape] = useState<ChartShape>('bar')
  const [labelColumn, setLabelColumn] = useState('')
  const [valueColumn, setValueColumn] = useState('')

  const typeOf = useMemo(() => {
    const byName = new Map(fields.map((field) => [field.name, field.type]))
    return (name: string) => byName.get(name)
  }, [fields])

  const numericColumns = useMemo(
    () => columns.filter((name) => isNumericType(typeOf(name))),
    [columns, typeOf],
  )

  // Non-numeric columns first: the thing being grouped by — `month`, `status`,
  // `country` — is what a reader wants on the x axis, and it is almost never
  // the number that is also being plotted.
  const labelOptions = useMemo(
    () => [
      ...columns.filter((name) => !isNumericType(typeOf(name))),
      ...columns.filter((name) => isNumericType(typeOf(name))),
    ],
    [columns, typeOf],
  )

  useEffect(() => {
    setLabelColumn((current) =>
      current && labelOptions.includes(current) ? current : (labelOptions[0] ?? ''),
    )
  }, [labelOptions])

  useEffect(() => {
    setValueColumn((current) =>
      current && numericColumns.includes(current) ? current : (numericColumns[0] ?? ''),
    )
  }, [numericColumns])

  const points = useMemo(() => {
    if (!valueColumn) return []
    const valueIndex = columns.indexOf(valueColumn)
    const labelIndex = columns.indexOf(labelColumn)
    if (valueIndex < 0) return []
    return rows.slice(0, MAX_POINTS).map((row, index) => ({
      label: labelIndex >= 0 ? labelOf(row[labelIndex]) : `#${index + 1}`,
      value: numberOf(row[valueIndex]),
    }))
  }, [columns, labelColumn, rows, valueColumn])

  const drawn = useMemo(
    () => points.filter((point): point is { label: string; value: number } => point.value !== null),
    [points],
  )
  const skipped = points.length - drawn.length

  if (numericColumns.length === 0) {
    return (
      <p className="rounded-xl border border-dashed border-line px-3 py-8 text-center text-xs text-content-subtle">
        Nothing in this result is a number. A chart needs one numeric column — a{' '}
        <code className="font-mono">count(*)</code>, a <code className="font-mono">sum</code>, a
        value read from the table.
      </p>
    )
  }

  if (drawn.length === 0) {
    return (
      <p className="rounded-xl border border-dashed border-line px-3 py-8 text-center text-xs text-content-subtle">
        {points.length === 0
          ? 'The query returned no rows to plot.'
          : `Every value in ${valueColumn} is null.`}
      </p>
    )
  }

  const values = drawn.map((point) => point.value)
  const max = Math.max(...values, 0)
  const min = Math.min(...values, 0)
  const ticks = ticksFor(max, min)
  const top = Math.max(max, ticks[ticks.length - 1] ?? 0)
  const bottom = Math.min(min, ticks[0] ?? 0)
  const span = top - bottom || 1

  const width = Math.max(
    360,
    Math.min(1200, drawn.length * (shape === 'bar' ? 28 : 16) + PAD.left + PAD.right),
  )
  const plotWidth = width - PAD.left - PAD.right
  const plotHeight = HEIGHT - PAD.top - PAD.bottom
  const y = (value: number) => PAD.top + plotHeight - ((value - bottom) / span) * plotHeight
  const band = plotWidth / drawn.length
  const barWidth = Math.max(2, Math.min(28, band * 0.7))
  const centre = (index: number) => PAD.left + band * index + band / 2

  const path = drawn
    .map((point, index) => `${index === 0 ? 'M' : 'L'}${centre(index).toFixed(2)},${y(point.value).toFixed(2)}`)
    .join(' ')

  // Enough labels to orient, never enough to overlap: at most one per 56px.
  const labelEvery = Math.max(1, Math.ceil(drawn.length / Math.max(1, Math.floor(plotWidth / 56))))

  return (
    <div className="space-y-2.5">
      <div className="flex flex-wrap items-end gap-3">
        <Field label="Shape">
          <Segmented
            size="sm"
            ariaLabel="Chart shape"
            value={shape}
            onChange={setShape}
            options={[
              { value: 'bar', label: 'Bars', title: 'One bar per row — comparing categories' },
              { value: 'line', label: 'Line', title: 'A line across the rows — a trend in order' },
            ]}
          />
        </Field>

        <Field label="Label">
          <Select
            className="w-44"
            value={labelColumn}
            ariaLabel="Column to label points by"
            onValueChange={setLabelColumn}
            options={labelOptions.map((name) => ({ value: name, label: name }))}
          />
        </Field>

        <Field label="Value">
          <Select
            className="w-44"
            value={valueColumn}
            ariaLabel="Column to plot"
            onValueChange={setValueColumn}
            options={numericColumns.map((name) => ({ value: name, label: name }))}
          />
        </Field>

        <span className="flex items-center gap-1.5 pb-2 text-2xs text-content-subtle">
          {shape === 'bar' ? (
            <ChartColumn className="h-3 w-3" aria-hidden />
          ) : (
            <ChartLine className="h-3 w-3" aria-hidden />
          )}
          <span className="tabular-nums">{formatCount(drawn.length)}</span> plotted
          {rows.length > MAX_POINTS && <span>· first {MAX_POINTS} of {formatCount(rows.length)}</span>}
          {skipped > 0 && <span>· {skipped} null skipped</span>}
        </span>
      </div>

      <div className="overflow-x-auto rounded-xl border border-line bg-surface-sunken/40 p-2">
        <svg
          width={width}
          height={HEIGHT}
          viewBox={`0 0 ${width} ${HEIGHT}`}
          role="img"
          aria-label={`${shape === 'bar' ? 'Bar' : 'Line'} chart of ${valueColumn} by ${
            labelColumn || 'row number'
          }`}
          className="max-w-none"
        >
          {ticks.map((tick) => (
            <g key={tick}>
              <line
                x1={PAD.left}
                x2={width - PAD.right}
                y1={y(tick)}
                y2={y(tick)}
                strokeWidth={1}
                className={cn('stroke-line', tick === 0 && 'stroke-content-subtle')}
              />
              <text
                x={PAD.left - 8}
                y={y(tick) + 3}
                textAnchor="end"
                className="fill-content-subtle text-[9px] tabular-nums"
              >
                {axisLabel(tick)}
              </text>
            </g>
          ))}

          {shape === 'bar' ? (
            drawn.map((point, index) => {
              const zero = y(0)
              const target = y(point.value)
              return (
                <rect
                  key={`bar-${index}`}
                  x={centre(index) - barWidth / 2}
                  y={Math.min(zero, target)}
                  width={barWidth}
                  height={Math.max(1, Math.abs(zero - target))}
                  rx={2}
                  className="fill-brand-500"
                >
                  <title>{`${point.label}: ${point.value}`}</title>
                </rect>
              )
            })
          ) : (
            <>
              <path d={path} fill="none" strokeWidth={1.75} className="stroke-brand-500" />
              {drawn.map((point, index) => (
                <circle
                  key={`point-${index}`}
                  cx={centre(index)}
                  cy={y(point.value)}
                  r={drawn.length > 60 ? 1.5 : 2.5}
                  className="fill-brand-500"
                >
                  <title>{`${point.label}: ${point.value}`}</title>
                </circle>
              ))}
            </>
          )}

          {drawn.map((point, index) =>
            index % labelEvery === 0 ? (
              <text
                key={`label-${index}`}
                x={centre(index)}
                y={HEIGHT - 8}
                textAnchor="middle"
                className="fill-content-subtle text-[9px]"
              >
                {point.label.length > 12 ? `${point.label.slice(0, 11)}…` : point.label}
              </text>
            ) : null,
          )}
        </svg>
      </div>
    </div>
  )
}
