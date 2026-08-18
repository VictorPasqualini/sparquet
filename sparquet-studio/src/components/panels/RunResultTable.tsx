import type { ReactNode } from 'react'

import { cn } from '@/lib/utils/cn'

/** Hard ceiling on rendered rows — a preview is for sanity-checking, not browsing. */
const MAX_ROWS = 50

export interface RunResultTableProps {
  columns: string[]
  /** Row-major cells, aligned with `columns`. */
  rows: unknown[][]
  /** The runner already cut the result short. */
  truncated: boolean
}

export function RunResultTable({ columns, rows, truncated }: RunResultTableProps) {
  if (columns.length === 0) {
    return (
      <p className="rounded-xl border border-dashed border-line px-3 py-6 text-center text-xs text-content-subtle">
        The runner returned no columns to preview.
      </p>
    )
  }

  const visible = rows.slice(0, MAX_ROWS)
  const clipped = truncated || rows.length > visible.length

  return (
    <div className="overflow-hidden rounded-xl border border-line bg-surface">
      <div
        role="region"
        aria-label="Result preview"
        tabIndex={0}
        className="max-h-80 overflow-auto"
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
              {columns.map((column) => (
                <th
                  key={column}
                  scope="col"
                  className="whitespace-nowrap border-b border-line px-2.5 py-1.5 font-medium text-content-muted"
                >
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="font-mono">
            {visible.map((row, rowIndex) => (
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
                {columns.map((column, cellIndex) => (
                  <td key={column} className="px-2.5 py-1 align-top text-content">
                    <span className="block max-w-[22rem] truncate whitespace-nowrap">
                      {renderCell(row[cellIndex])}
                    </span>
                  </td>
                ))}
              </tr>
            ))}
            {visible.length === 0 && (
              <tr>
                <td
                  colSpan={columns.length + 1}
                  className="px-2.5 py-6 text-center font-sans text-xs text-content-subtle"
                >
                  The pipeline produced no rows.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <p className="border-t border-line bg-surface-sunken px-2.5 py-1.5 text-2xs text-content-subtle">
        {clipped
          ? `Showing the first ${visible.length} of ${MAX_ROWS}+ rows — previews are capped.`
          : `${visible.length} ${visible.length === 1 ? 'row' : 'rows'}`}
      </p>
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
