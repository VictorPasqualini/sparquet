/**
 * Discovery of `{param}` template variables inside a pipeline.
 *
 * The framework substitutes `{param}` in the RAW JSON text before parsing
 * (utils/template.py), so scanning the serialized document is exactly what the
 * runtime does. Runtime `{{var}}` placeholders are stripped first: their inner
 * braces would otherwise match as a param named after the variable.
 */

import { nanoid } from 'nanoid'

import type { ParamDefinition, ParamType } from '@/types/studio'

const RUNTIME_PLACEHOLDER = /\{\{\w+\}\}/g
/**
 * Names must start with a letter or underscore: a digit-only match would turn a
 * SQL/regex quantifier such as `^[0-9]{11}$` into a param called `11`, and
 * declaring it would make the framework rewrite the pattern at run time.
 */
const PARAM_PLACEHOLDER = /\{([A-Za-z_]\w*)\}/g

export function inferParams(pipeline: unknown): ParamDefinition[] {
  const source = JSON.stringify(pipeline ?? null).replace(RUNTIME_PLACEHOLDER, '')
  const keys = new Set<string>()
  for (const match of source.matchAll(PARAM_PLACEHOLDER)) {
    if (match[1]) keys.add(match[1])
  }

  return [...keys].map((key) => {
    const type = inferParamType(key, source)
    return { id: nanoid(8), key, type, value: seedParamValue(type) }
  })
}

/** `IN (...)` wins over the guard test: a list used as a guard is still a list. */
export function inferParamType(key: string, source: string): ParamType {
  if (new RegExp(`IN\\s*\\(\\s*\\{${key}\\}\\s*\\)`, 'i').test(source)) return 'list'
  if (new RegExp(`"skip_if_false"\\s*:\\s*"\\{${key}\\}"`).test(source)) return 'boolean'
  return 'string'
}

export function seedParamValue(type: ParamType): ParamDefinition['value'] {
  if (type === 'boolean') return true
  if (type === 'list') return []
  if (type === 'number') return 0
  return ''
}

/**
 * The `{key: value}` map the framework expects, built from the declared params.
 * Unnamed params are skipped: a blank key would substitute `{}` everywhere.
 */
export function paramValues(
  params: readonly ParamDefinition[],
): Record<string, ParamDefinition['value']> {
  const values: Record<string, ParamDefinition['value']> = {}
  for (const param of params) {
    if (param.key) values[param.key] = param.value
  }
  return values
}

/**
 * Keeps a param list in step with a pipeline: newly referenced keys are added
 * with a seed value, and values the user already typed are preserved.
 */
export function mergeParams(existing: ParamDefinition[], pipeline: unknown): ParamDefinition[] {
  const discovered = inferParams(pipeline)
  const byKey = new Map(existing.map((param) => [param.key, param]))
  const merged = discovered.map((param) => byKey.get(param.key) ?? param)
  // Keep declared-but-unused params: the user may be mid-edit.
  const seen = new Set(merged.map((param) => param.key))
  return [...merged, ...existing.filter((param) => !seen.has(param.key))]
}
