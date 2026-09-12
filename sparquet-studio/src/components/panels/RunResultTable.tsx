import { ArrowDown, ArrowUp, Check, Copy } from 'lucide-react'
import { useCallback, useMemo, useState, type ReactNode } from 'react'

import type { RunnerSchemaField } from '@/lib/runner/client'
import { cn } from '@/lib/utils/cn'
import { copyText } from '@/lib/utils/download'

/** Default ceiling on rendered rows — a preview is for sanity-checking, not browsing. */
const MAX_ROWS = 50

export interface RunResultTableProps {
  columns: string[]
  /** Row-major cells, aligned with `columns`. */
  rows: unknown[][]
  /** The runner already cut the result short. */
  truncated: boolean
  /** Rows to render. The SQL editor asks for more than a run preview does. */
  maxRows?: number
  /** What to say when the result is empty — a run and a query mean different things by it. */
  emptyMessage?: string
  /** Height of the scrolling area, as a Tailwind class. */
  heightClass?: string
  /**
   * The schema behind the columns. With it the header names each type and
   * numbers line up on the right; without it the table behaves as it always did.
   */
  fields?: RunnerSchemaField[]
  /** Let a header sort the rendered rows. Off for a run preview, on in the SQL editor. */
  sortable?: boolean
  /** Let a cell be clicked open to read the value that does not fit. */
  inspectable?: boolean
}

type Direction = 'asc' | 'desc'

interface Sort {
  column: number
  direction: Direction
}

interface Cell {
  row: number
  column: number
}

/** Spark's own type names, as the runner reports them (`decimal(18,2)`). */
const NUMERIC = /^(tinyint|smallint|int|integer|bigint|long|short|byte|float|double|decimal|numeric)/i

function isNumericType(type: string | undefined): boolean {
  return type !== undefined && NUMERIC.test(type.trim())
}

/**
 * Order two cells the way somebody reading a result expects.
 *
 * Nulls sink to the bottom in both directions, rather than counting as the
 * smallest value: they are missing data, and burying the actual extremes under
 * a page of blanks is what makes a sorted column useless. Numbers compare as
 * numbers, everything else by locale, and a structured value by its JSON —
 * arbitrary, but stable, which is what sorting really needs.
 */
function compare(left: unknown, right: unknown): number {
  const leftNull = left === null || left === undefined
  const rightNull = right === null || right === undefined
  if (leftNull || rightNull) return leftNull && rightNull ? 0 : leftNull ? 1 : -1
  if (typeof left === 'number' && typeof right === 'number') return left - right
  if (typeof left === 'boolean' && typeof right === 'boolean') {
    return Number(left) - Number(right)
  }
  const leftText = typeof left === 'object' ? JSON.stringify(left) : String(left)
  const rightText = typeof right === 'object' ? JSON.stringify(right) : String(right)
  return leftText.localeCompare(rightText, undefined, { numeric: true, sensitivity: 'base' })
}

export function RunResultTable({
  columns,
  rows,
  truncated,
  maxRows = MAX_ROWS,
  emptyMessage = 'The pipeline produced no rows.',
  heightClass = 'max-h-80',
  fields,
  sortable = false,
  inspectable = false,
}: RunResultTableProps) {
  const [sort, setSort] = useState<Sort | null>(null)
  const [selected, setSelected] = useState<Cell | null>(null)

  const typeOf = useCallback(
    (index: number): string | undefined => fields?.[index]?.type,
    [fields],
  )

  // The cap is applied before sorting, deliberately: the rows on screen stay the
  // rows the runner sent, reordered. Sorting first would silently turn "the
  // first 50 rows" into "the 50 largest", which is a different query.
  const visible = useMemo(() => rows.slice(0, maxRows), [maxRows, rows])

  const ordered = useMemo(() => {
    if (!sort) return visible
    const sign = sort.direction === 'asc' ? 1 : -1
    return [...visible].sort(
      (left, right) => sign * compare(left[sort.column], right[sort.column]),
    )
  }, [sort, visible])

  const toggleSort = useCallback((column: number) => {
    setSelected(null)
    setSort((current) => {
      if (!current || current.column !== column) return { column, direction: 'asc' }
      if (current.direction === 'asc') return { column, direction: 'desc' }
      return null
    })
  }, [])

  if (columns.length === 0) {
    return (
      <p className="rounded-xl border border-dashed border-line px-3 py-6 text-center text-xs text-content-subtle">
        The runner returned no columns to preview.
      </p>
    )
  }

  const clipped = truncated || rows.length > visible.length
  const selectedValue =
    selected && ordered[selected.row] ? ordered[selected.row][selected.column] : undefined

  return (
    <div className="overflow-hidden rounded-xl border border-line bg-surface">
      <div
        role="region"
        aria-label="Result preview"
        tabIndex={0}
        className={cn(heightClass, 'overflow-auto')}
      >
        <table className="w-full border-collapse text-left text-2xs">
          <thead>
            <tr className="sticky top-0 z-10 bg-surface-sunken">
              <th
                scope="col"
                className="w-10 border-b border-line px-2 py-1.5 text-right font-medium text-content-subtle"
              >
                #
              </th>
              {columns.map((column, index) => {
                const type = typeOf(index)
                const numeric = isNumericType(type)
                const active = sort?.column === index
                const label = (
                  <>
                    <span className="text-content-muted">{column}</span>
                    {type ? (
                      <span className="ml-1.5 font-normal text-content-subtle">{type}</span>
                    ) : null}
                    {active ? (
                      sort.direction === 'asc' ? (
                        <ArrowUp className="ml-1 inline h-3 w-3 text-brand-500" aria-hidden />
                      ) : (
                        <ArrowDown className="ml-1 inline h-3 w-3 text-brand-500" aria-hidden />
                      )
                    ) : null}
                  </>
                )
                return (
                  <th
                    key={column}
                    scope="col"
                    aria-sort={
                      active ? (sort.direction === 'asc' ? 'ascending' : 'descending') : undefined
                    }
                    className={cn(
                      'whitespace-nowrap border-b border-line font-medium',
                      numeric && 'text-right',
                      sortable ? 'p-0' : 'px-2.5 py-1.5',
                    )}
                  >
                    {sortable ? (
                      <button
                        type="button"
                        onClick={() => toggleSort(index)}
                        title={`Sort by ${column}`}
                        className={cn(
                          'w-full px-2.5 py-1.5 text-left hover:bg-surface-raised',
                          numeric && 'text-right',
                        )}
                      >
                        {label}
                      </button>
                    ) : (
                      label
                    )}
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody className="font-mono">
            {ordered.map((row, rowIndex) => (
              <tr
                key={rowIndex}
                className={cn(
                  'border-b border-line last:border-b-0',
                  rowIndex % 2 === 1 && 'bg-surface-sunken/50',
                )}
              >
                <td className="px-2 py-1 text-right font-sans text-content-subtle">
                  {rowIndex + 1}
                </td>
                {columns.map((column, cellIndex) => {
                  const numeric = isNumericType(typeOf(cellIndex))
                  const isSelected =
                    selected?.row === rowIndex && selected.column === cellIndex
                  return (
                    <td
                      key={column}
                      onClick={
                        inspectable
                          ? () =>
                              setSelected(
                                isSelected ? null : { row: rowIndex, column: cellIndex },
                              )
                          : undefined
                      }
                      className={cn(
                        'px-2.5 py-1 align-top text-content',
                        numeric && 'text-right tabular-nums',
                        inspectable && 'cursor-pointer',
                        isSelected && 'bg-brand-500/10 ring-1 ring-inset ring-brand-500/40',
                      )}
                    >
                      <span className="block max-w-[22rem] truncate whitespace-nowrap">
                        {renderCell(row[cellIndex])}
                      </span>
                    </td>
                  )
                })}
              </tr>
            ))}
            {ordered.length === 0 && (
              <tr>
                <td
                  colSpan={columns.length + 1}
                  className="px-2.5 py-6 text-center font-sans text-xs text-content-subtle"
                >
                  {emptyMessage}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {selected ? (
        <CellInspector
          column={columns[selected.column] ?? ''}
          type={typeOf(selected.column)}
          value={selectedValue}
          onClose={() => setSelected(null)}
        />
      ) : null}

      <p className="border-t border-line bg-surface-sunken px-2.5 py-1.5 text-2xs text-content-subtle">
        {clipped
          ? `Showing the first ${visible.length} of ${maxRows}+ rows — previews are capped.`
          : `${visible.length} ${visible.length === 1 ? 'row' : 'rows'}`}
        {sort
          ? ` · sorted by ${columns[sort.column]} ${sort.direction === 'asc' ? '↑' : '↓'}, within the rows shown`
          : ''}
      </p>
    </div>
  )
}

/**
 * The full value of one cell.
 *
 * A grid row has to stay one line tall to be scannable, so a long string or a
 * struct is truncated in place — and the one value somebody wants to read is
 * usually exactly the one that did not fit. This panel is where it fits: the
 * whole thing, JSON pretty-printed, and a copy button, because the next thing
 * done with an id or a message is pasting it somewhere else.
 */
function CellInspector({
  column,
  type,
  value,
  onClose,
}: {
  column: string
  type?: string
  value: unknown
  onClose: () => void
}) {
  const [copied, setCopied] = useState(false)
  const text =
    value === null || value === undefined
      ? 'null'
      : typeof value === 'object'
        ? JSON.stringify(value, null, 2)
        : String(value)

  return (
    <div className="border-t border-line bg-surface-sunken/60">
      <div className="flex items-center gap-2 px-2.5 py-1.5">
        <span className="truncate text-2xs font-medium text-content">{column}</span>
        {type ? <span className="text-2xs text-content-subtle">{type}</span> : null}
        <span className="ml-auto flex items-center gap-1">
          <button
            type="button"
            onClick={() => {
              void copyText(text).then((ok) => {
                if (!ok) return
                setCopied(true)
                globalThis.setTimeout(() => setCopied(false), 1500)
              })
            }}
            className="flex items-center gap-1 rounded px-1.5 py-0.5 text-2xs text-content-subtle hover:bg-surface-raised hover:text-content"
          >
            {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
            {copied ? 'Copied' : 'Copy'}
          </button>
          <button
            type="button"
            onClick={onClose}
            className="rounded px-1.5 py-0.5 text-2xs text-content-subtle hover:bg-surface-raised hover:text-content"
          >
            Close
          </button>
        </span>
      </div>
      <pre className="max-h-40 overflow-auto whitespace-pre-wrap break-words border-t border-line px-2.5 py-2 font-mono text-2xs leading-relaxed text-content">
        {text}
      </pre>
    </div>
  )
}

function renderCell(value: unknown): ReactNode {
  if (value === null || value === undefined) {
    return <span className="italic text-content-subtle">null</span>
  }
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}
