/**
 * Rows out of the Studio as CSV.
 *
 * RFC 4180, which is also the dialect the framework reads and writes: fields
 * that contain a comma, a quote or a newline are quoted, and a quote inside a
 * quoted field is doubled — never backslash-escaped. The two sides matching is
 * the point, since a result exported here is usually opened next to data the
 * framework produced.
 *
 * A value is rendered the way the grid renders it: `null` is an empty field
 * (a spreadsheet has no other notion of absent), and anything structured is
 * JSON, because a struct flattened by `String()` is `[object Object]`.
 */

const NEEDS_QUOTES = /[",\r\n]/

export function csvCell(value: unknown): string {
  if (value === null || value === undefined) return ''
  const text =
    typeof value === 'object' ? JSON.stringify(value) : typeof value === 'string' ? value : String(value)
  return NEEDS_QUOTES.test(text) ? `"${text.replaceAll('"', '""')}"` : text
}

/** A header line plus one line per row, `\r\n` terminated as the RFC asks. */
export function toCsv(columns: string[], rows: unknown[][]): string {
  const lines = [columns.map(csvCell).join(',')]
  for (const row of rows) lines.push(row.map(csvCell).join(','))
  return lines.join('\r\n')
}

/** A file name a person can find again: `orders-2026-09-12-1431.csv`. */
export function timestampedName(stem: string, extension: string): string {
  const now = new Date()
  const pad = (value: number) => String(value).padStart(2, '0')
  const slug =
    stem
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '') || 'export'
  return (
    `${slug}-${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}` +
    `-${pad(now.getHours())}${pad(now.getMinutes())}.${extension}`
  )
}
