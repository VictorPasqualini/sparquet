/**
 * The code that identifies a validation rule — what labels a row in the quarantine
 * annotation column, and what `validations.outputs.invalid.rules` refers to.
 *
 * ⚠️ This MIRRORS `BaseCheck.code()` / `derived_code()` in `sparquet_cola`
 * (`sparquet_cola/checks.py`). The library is the source of truth: it writes these
 * strings into the data. Studio only needs them to match a code back to the rule
 * node it came from, so the two renderings must agree exactly — `ruleCode.test.ts`
 * pins the same cases the library's own test pins. If a rendering changes there,
 * change it here in the same commit.
 */

import type { StudioNode } from '@/types/studio'

const args = (...parts: (string | number | null | undefined)[]): string =>
  parts.map((part) => (part === null || part === undefined ? '' : String(part))).join(',')

function columnsOf(params: Record<string, unknown>): string {
  const columns = params.columns
  if (Array.isArray(columns) && columns.length > 0) {
    return args(...columns.map((column) => (typeof column === 'string' ? column : String(column))))
  }
  const column = params.column
  return typeof column === 'string' ? column : column === undefined ? '' : String(column)
}

function num(value: unknown): string | number {
  // `*` marks an open side, so `range(age,1,*)` stays distinguishable from
  // `range(age,1,99)` — matching the library.
  if (value === null || value === undefined || value === '') return '*'
  return typeof value === 'number' ? value : String(value)
}

/** The rule's declared `code`, or the expression the library would derive. */
export function ruleCode(node: StudioNode): string {
  if (node.data.kind !== 'validation') return ''
  const params = (node.data.params ?? {}) as Record<string, unknown>
  const declared = params.code
  if (typeof declared === 'string' && declared.trim() !== '') return declared.trim()

  const type = node.data.validator ?? ''
  switch (type) {
    case 'not_null':
      return `not_null(${columnsOf(params)})`
    case 'unique':
      return `unique(${columnsOf(params)})`
    case 'range':
      return `range(${args(columnsOf(params), num(params.min), num(params.max))})`
    case 'regex':
      return `regex(${args(columnsOf(params), params.pattern as string)})`
    case 'check': {
      const metric = typeof params.metric === 'string' && params.metric ? params.metric : type
      const columns = columnsOf(params)
      return columns ? `${metric}(${columns})` : metric
    }
    default:
      // Aggregate checks never label a row, but they still answer with their type.
      return type
  }
}
