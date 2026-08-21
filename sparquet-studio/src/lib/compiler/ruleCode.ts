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

import { expandTargets } from './targets'

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

/** The declared `code`, or the expression the library would derive, for ONE rule. */
function codeOf(type: string, params: Record<string, unknown>): string {
  const declared = params.code
  if (typeof declared === 'string' && declared.trim() !== '') return declared.trim()
  switch (type) {
    case 'not_null':
      return `not_null(${columnsOf(params)})`
    case 'unique':
      return `unique(${columnsOf(params)})`
    case 'range':
      return `range(${args(columnsOf(params), num(params.min), num(params.max))})`
    case 'regex':
      return `regex(${args(columnsOf(params), params.pattern as string)})`
    default: {
      // Every metric is a rule type of its own, and its code is the metric plus the
      // column(s) it measured: `missing_percent(cpf)`, `duplicate_count(id)`. An
      // aggregate that measures no column is just its name — `row_count`. Rules with
      // no column at all (sql, schema) land here too and answer with their type,
      // matching the library's `derived_code()` default.
      const columns = columnsOf(params)
      return columns ? `${type}(${columns})` : type
    }
  }
}

/**
 * Every code a rule node answers to — one per target, or a single one when the rule
 * declares no `targets`.
 *
 * A `targets` rule is declaration sugar the library flattens into N independent rules
 * (`expand_targets` in `sparquet_cola/targets.py`): everything outside `targets` is a
 * shared default, each target overrides what it wants, and each expanded rule gets its
 * own result, its own report row and its own code. Studio has to agree on all of them,
 * because scoping the quarantine to this node means scoping to every code it produces —
 * emitting just the first would silently drop the other targets from the split.
 */
export function ruleCodes(node: StudioNode): string[] {
  if (node.data.kind !== 'validation') return []
  const type = node.data.validator ?? ''
  // `expandTargets` is the one place that mirrors the library's expansion — the same
  // reason the Python side exposes a single `expand_targets` for its two call sites.
  // A `code` declared beside `targets` survives into every expanded rule here: the
  // library refuses that config, the linter reports it, and hiding it would make the
  // canvas look fine.
  return expandTargets((node.data.params ?? {}) as Record<string, unknown>).map((params) =>
    codeOf(type, params),
  )
}

/**
 * The node's first code. A rule with no `targets` has exactly one, which is the common
 * case; prefer {@link ruleCodes} anywhere the full set matters.
 */
export function ruleCode(node: StudioNode): string {
  return ruleCodes(node)[0] ?? ''
}
