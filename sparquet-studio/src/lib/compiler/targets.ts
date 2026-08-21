/**
 * `targets` — one rule entry, N independent rules.
 *
 * A validation rule may carry a `targets` array: every key outside it is a shared
 * default, every key inside one target overrides it, and the framework flattens the
 * entry into one ordinary rule per target at parse time
 * (`sparquet_cola.targets.expand_targets`, called from `ValidationConfig.from_dict`).
 *
 * ⚠️ This MIRRORS that function. Studio never expands anything into the JSON — one
 * node stays one rule entry, `targets` and all, so the compiler remains an involution.
 * The expansion is needed only to reason about what the framework will do: how many
 * results a rule produces, which codes it answers to, and which fields are actually
 * filled in.
 *
 * Unlike the Python side this never throws: it runs inside render paths (node
 * previews, the compiler) where an exception would blank the canvas. Invalid shapes
 * fall back to the single parent rule and are reported instead by the `targets` field
 * in the validator catalog (`catalog/validators.ts`), whose `validate` mirrors the
 * library's refusals — that is the only place that can point the author at the
 * offending field, and it is what the linter surfaces via `checkFieldSpecs`.
 */

export const TARGETS_KEY = 'targets'

/** Parent keys the framework refuses next to `targets` — both would be duplicated. */
export const TARGETS_PARENT_FORBIDDEN = ['code', 'output'] as const

type Params = Record<string, unknown>

const isRecord = (value: unknown): value is Params =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

const withoutTargets = (params: Params): Params => {
  if (!(TARGETS_KEY in params)) return params
  const out: Params = {}
  for (const [key, value] of Object.entries(params)) {
    if (key === TARGETS_KEY) continue
    out[key] = value
  }
  return out
}

/** The `targets` array when the entry declares a usable one, else null. */
export function targetsOf(params: Params): Params[] | null {
  const raw = params[TARGETS_KEY]
  if (!Array.isArray(raw) || raw.length === 0) return null
  if (!raw.every(isRecord)) return null
  return raw
}

/**
 * The rules the framework will actually run for one rule entry.
 *
 * Without `targets` the params come back untouched, as a single element — so every
 * caller can treat one rule and a multi-target rule the same way. Idempotent: the
 * result never carries `targets`, so expanding twice changes nothing.
 */
export function expandTargets(params: Params): Params[] {
  const targets = targetsOf(params)
  if (!targets) return [withoutTargets(params)]
  const shared = withoutTargets(params)
  // A target's own keys win, and a nested `targets` is dropped rather than carried
  // into the result: the framework refuses it, and keeping it here would make this
  // function non-idempotent.
  return targets.map((target) => withoutTargets({ ...shared, ...target }))
}
