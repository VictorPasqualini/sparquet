/**
 * The three datasets the `validations` block writes on the side.
 *
 * They are part of the pipeline language, so their wording lives in the catalog
 * next to the transformations, formats and validators — one source for the canvas
 * handles, the inspector, the linter and anything else that has to explain them.
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
  /** Full name, used as the inspector title and the node badge. */
  label: string
  /** Two or three words, drawn beside the handle on the rule node. */
  handleLabel: string
  /** One line under the node title. */
  subtitle: string
  /** What lands in it, in one sentence. */
  summary: string
  /** Why it does not change what the main destinations receive. */
  caveat: string
  icon: string
}

export const VALIDATION_SINKS: ValidationSinkDef[] = [
  {
    role: 'report',
    jsonKey: 'validations.report',
    label: 'Quality report',
    handleLabel: 'report',
    subtitle: 'validation side output — one row per rule',
    summary:
      'One row per rule: pipeline, rule_type, check_name, severity, passed, failed_count, metric_value, message, validated_at.',
    caveat:
      'Written only when the run reaches the outputs: in `fail` mode a violation aborts before it. It never touches the data the job writes.',
    icon: 'ClipboardList',
  },
  {
    role: 'valid',
    jsonKey: 'validations.outputs.valid',
    label: 'Quarantine — valid rows',
    handleLabel: 'valid rows',
    subtitle: 'validation side output — copy of the clean rows',
    summary:
      'A copy of the rows that break no row-level rule (not_null, unique, range, regex, and the missing/invalid checks).',
    caveat:
      'A copy, not a split: the job’s own destinations still receive every row, invalid ones included.',
    icon: 'ShieldCheck',
  },
  {
    role: 'invalid',
    jsonKey: 'validations.outputs.invalid',
    label: 'Quarantine — invalid rows',
    handleLabel: 'invalid rows',
    subtitle: 'validation side output — copy of the rejected rows',
    summary: 'A copy of the rows those same row-level rules rejected, kept apart for inspection.',
    caveat:
      'A copy, not a split: these rows are ALSO written to the job’s own destinations. Filter them out with a `filter` node if the main chain must not carry them.',
    icon: 'ShieldAlert',
  },
]

const byRole = new Map(VALIDATION_SINKS.map((sink) => [sink.role, sink]))

export function getValidationSink(role: ValidationSinkRole): ValidationSinkDef {
  // Every role has an entry, and the map is built from the same union.
  return byRole.get(role) as ValidationSinkDef
}
