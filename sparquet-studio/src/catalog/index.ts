/**
 * Catalog entry point — every lookup the app needs, in one place.
 *
 * Data lives in sibling modules (transformations.core / transformations.advanced /
 * formats / validators) and is merged here so consumers never care where an entry
 * is defined.
 */

import { FORMATS } from './formats'
import { ADVANCED_TRANSFORMATIONS } from './transformations.advanced'
import { CORE_TRANSFORMATIONS } from './transformations.core'
import type { FormatDef, TransformationDef, ValidatorDef } from './types'
import { VALIDATION_SINKS, type ValidationSinkDef } from './validationSinks'
import { ON_FAILURE_OPTIONS, VALIDATORS } from './validators'

export * from './types'
export { getValidationSink, VALIDATION_SINKS } from './validationSinks'
export type { ValidationSinkDef } from './validationSinks'

export const TRANSFORMATIONS: TransformationDef[] = [
  ...CORE_TRANSFORMATIONS,
  ...ADVANCED_TRANSFORMATIONS,
]

export { FORMATS, ON_FAILURE_OPTIONS, VALIDATORS }

const transformationByType = new Map(TRANSFORMATIONS.map((t) => [t.type, t]))
const formatById = new Map(FORMATS.map((f) => [f.id, f]))
const validatorByType = new Map(VALIDATORS.map((v) => [v.type, v]))

export function getTransformation(type: string): TransformationDef | undefined {
  return transformationByType.get(type)
}

export function getFormat(id: string): FormatDef | undefined {
  return formatById.get(id.toLowerCase())
}

export function getValidator(type: string): ValidatorDef | undefined {
  return validatorByType.get(type)
}

/** Transformations offered in the palette. */
export const PALETTE_TRANSFORMATIONS: TransformationDef[] = TRANSFORMATIONS

export const READABLE_FORMATS: FormatDef[] = FORMATS.filter((f) => f.canRead)
export const WRITABLE_FORMATS: FormatDef[] = FORMATS.filter((f) => f.canWrite)

/** Case-insensitive fuzzy search across the palette. */
export function searchCatalog(query: string): {
  transformations: TransformationDef[]
  formats: FormatDef[]
  validators: ValidatorDef[]
  /** Quality destinations — the datasets the `validations` block writes. */
  validationSinks: ValidationSinkDef[]
} {
  const q = query.trim().toLowerCase()
  if (!q) {
    return {
      transformations: PALETTE_TRANSFORMATIONS,
      formats: FORMATS,
      validators: VALIDATORS,
      validationSinks: VALIDATION_SINKS,
    }
  }

  const matches = (haystack: (string | undefined)[]) =>
    haystack.some((value) => value?.toLowerCase().includes(q))

  return {
    transformations: PALETTE_TRANSFORMATIONS.filter((t) =>
      matches([t.type, t.label, t.summary, ...t.keywords]),
    ),
    formats: FORMATS.filter((f) => matches([f.id, f.label, f.summary])),
    validators: VALIDATORS.filter((v) => matches([v.type, v.label, v.summary, ...v.keywords])),
    validationSinks: VALIDATION_SINKS.filter((s) =>
      matches([s.role, s.label, s.summary, s.jsonKey, ...s.keywords]),
    ),
  }
}
