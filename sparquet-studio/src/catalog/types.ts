/**
 * Catalog contract.
 *
 * The catalog is the single description of the Sparquet pipeline language used by
 * the whole app: it drives the node palette, the inspector forms, client-side
 * linting, the docs drawer and the system prompt handed to the AI assistant.
 * Adding a transformation to the framework means adding one entry here.
 */

import type { WriteMode } from '@/types/pipeline'

/** Editor widget used for a JSON key. */
export type FieldType =
  /** Single-line free text. */
  | 'text'
  /** Multi-line free text. */
  | 'textarea'
  /** SQL expression (monospace + syntax hints). */
  | 'sql'
  /** Numeric input. */
  | 'number'
  /** Toggle. */
  | 'boolean'
  /** Single choice from `options`. */
  | 'select'
  /** Multiple choice from `options`. */
  | 'multi-select'
  /** Ordered list of strings (chips). */
  | 'string-list'
  /** Ordered list of SQL expressions, one per row. */
  | 'sql-list'
  /** Ordered map of string → string. */
  | 'key-value'
  /** Ordered map of name → SQL expression. */
  | 'expression-map'
  /** Arbitrary JSON edited in a code box (struct fields, options). */
  | 'json'
  /** Nested source config `{ format, path, options }` — rendered as a sub-form. */
  | 'source'

export interface FieldOption {
  value: string
  label: string
  hint?: string
}

export interface FieldSpec {
  /** JSON key written into the pipeline config. */
  key: string
  label: string
  type: FieldType
  required?: boolean
  placeholder?: string
  /** One-line hint shown under the control. */
  help?: string
  /** Longer markdown shown in the field's info popover. */
  docs?: string
  /** Value used when a node is created. `undefined` means "omit the key". */
  default?: unknown
  options?: FieldOption[]
  /** Rows for textarea/sql widgets. */
  rows?: number
  /** Fields in the `advanced` group start collapsed. */
  group?: 'main' | 'advanced'
  /** Hides the field unless the predicate matches the node's current params. */
  visibleWhen?: (params: Record<string, unknown>) => boolean
  /** Returns an error message, or null when the value is acceptable. */
  validate?: (value: unknown, params: Record<string, unknown>) => string | null
  /** Marks fields whose value commonly carries `{{runtime}}` placeholders. */
  supportsRuntimeVars?: boolean
}

export type NodeFamily =
  | 'source'
  | 'shape'
  | 'compute'
  | 'combine'
  | 'aggregate'
  | 'control'
  | 'inspect'
  | 'quality'
  | 'sink'

/** Accent token; maps to `--node-*` CSS variables. */
export type NodeAccent =
  | 'input'
  | 'transform'
  | 'combine'
  | 'control'
  | 'inspect'
  | 'validate'
  | 'output'

export interface CatalogExample {
  title: string
  json: string
}

export interface TransformationDef {
  /** Exact `type` string emitted in JSON. */
  type: string
  label: string
  family: NodeFamily
  accent: NodeAccent
  /** lucide-react icon name, resolved by the UI icon registry. */
  icon: string
  /** One line shown in the palette. */
  summary: string
  /** Markdown shown in the docs drawer. */
  description: string
  fields: FieldSpec[]
  /** Terms matched by palette search, beyond label/summary. */
  keywords: string[]
  /** Behaviors a user must know; surfaced in the inspector. */
  gotchas: string[]
  examples: CatalogExample[]
  /** `join` / `union`: a second incoming connection supplies `with`. */
  secondaryInput?: boolean
  /** `join`: right-hand side accepts its own transformation chain. */
  supportsSubPipeline?: boolean
  /** `collect`: publishes a `{{var}}` usable downstream. */
  emitsRuntimeVar?: boolean
  /** `stop_if_empty`: can halt the run. */
  canHalt?: boolean
  /** `debug`: never mutates the DataFrame. */
  sideEffectFree?: boolean
  /** `add_column`: parsed but never emitted for new nodes. */
  deprecatedAlias?: string
}

export interface FormatDef {
  /** Exact `format` string emitted in JSON. */
  id: string
  label: string
  icon: string
  canRead: boolean
  canWrite: boolean
  summary: string
  description: string
  /** Label for the `path` field — table name vs filesystem path vs topic. */
  pathLabel: string
  pathPlaceholder: string
  pathHelp?: string
  /** Write modes valid for this format, in menu order. */
  modes: WriteMode[]
  supportsPartitioning: boolean
  supportsMerge: boolean
  /** Options only meaningful when reading. */
  readOptions: FieldSpec[]
  /** Options only meaningful when writing. */
  writeOptions: FieldSpec[]
  gotchas: string[]
  examples: CatalogExample[]
}

export interface ValidatorDef {
  /** Exact `type` string emitted in the rules array. */
  type: string
  label: string
  icon: string
  summary: string
  description: string
  fields: FieldSpec[]
  keywords: string[]
  gotchas: string[]
  examples: CatalogExample[]
}

/** Builds the default params object for a set of fields. */
export function defaultsFor(fields: FieldSpec[]): Record<string, unknown> {
  const out: Record<string, unknown> = {}
  for (const field of fields) {
    if (field.default !== undefined) out[field.key] = structuredClone(field.default)
  }
  return out
}
