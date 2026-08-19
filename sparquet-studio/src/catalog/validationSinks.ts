/**
 * The three datasets the `validations` block writes on the side.
 *
 * They are part of the pipeline language, so their wording lives in the catalog
 * next to the transformations, formats and validators — one source for the palette
 * entries, the canvas nodes, the inspector, the linter and anything else that has
 * to explain them.
 *
 * The wording is deliberate and repeated everywhere these appear: the framework
 * calls `_write_validation_outputs(df)` and then `_write_outputs(df)` with the SAME
 * complete DataFrame, so a quarantine destination is a COPY taken on the side. It
 * never diverts a row away from the job's own destinations.
 */

import type { ValidationSinkRole } from '@/types/studio'

export interface ValidationSinkDef {
  role: ValidationSinkRole
  /** JSON path this destination compiles into. */
  jsonKey: 'validations.report' | 'validations.outputs.valid' | 'validations.outputs.invalid'
  /** Full name: the palette entry, the inspector title and the node title. */
  label: string
  /** One line under the node title. */
  subtitle: string
  /** What lands in it, in one sentence. Doubles as the palette tooltip. */
  summary: string
  /** Why it does not change what the main destinations receive. */
  caveat: string
  /** Format a freshly added node starts on — what the shipped examples use. */
  defaultFormat: string
  /** Extra search terms, so the palette finds it by intent as well as by name. */
  keywords: string[]
  icon: string
}

export const VALIDATION_SINKS: ValidationSinkDef[] = [
  {
    role: 'report',
    jsonKey: 'validations.report',
    label: 'Quality report',
    subtitle: 'quality destination — one row per rule',
    summary:
      'One row per rule: pipeline, rule_type, check_name, severity, passed, failed_count, metric_value, message, validated_at.',
    caveat:
      'Written only when the run reaches the outputs: in `fail` mode a violation aborts before it. It never touches the data the job writes.',
    defaultFormat: 'csv',
    keywords: ['report', 'quality', 'validation', 'audit', 'metrics', 'observability'],
    icon: 'ClipboardList',
  },
  {
    role: 'valid',
    jsonKey: 'validations.outputs.valid',
    label: 'Quarantine — valid rows',
    subtitle: 'quality destination — copy of the clean rows',
    summary:
      'A copy of the rows that break no row-level rule (not_null, unique, range, regex, and the missing/invalid checks).',
    caveat:
      'A copy, not a split: the job’s own destinations still receive every row, invalid ones included.',
    defaultFormat: 'delta',
    keywords: ['quarantine', 'valid', 'clean', 'quality', 'split', 'copy'],
    icon: 'ShieldCheck',
  },
  {
    role: 'invalid',
    jsonKey: 'validations.outputs.invalid',
    label: 'Quarantine — invalid rows',
    subtitle: 'quality destination — copy of the rejected rows',
    summary: 'A copy of the rows those same row-level rules rejected, kept apart for inspection.',
    caveat:
      'A copy, not a split: these rows are ALSO written to the job’s own destinations. Filter them out with a `filter` node if the main chain must not carry them.',
    defaultFormat: 'delta',
    keywords: ['quarantine', 'invalid', 'rejected', 'bad rows', 'quality', 'split', 'copy'],
    icon: 'ShieldAlert',
  },
]

const byRole = new Map(VALIDATION_SINKS.map((sink) => [sink.role, sink]))

export function getValidationSink(role: ValidationSinkRole): ValidationSinkDef {
  // Every role has an entry, and the map is built from the same union.
  return byRole.get(role) as ValidationSinkDef
}
