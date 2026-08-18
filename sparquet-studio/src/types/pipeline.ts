/**
 * TypeScript mirror of the Sparquet pipeline JSON schema.
 *
 * Source of truth is the Python framework:
 *   sparquet/core/config.py     — dataclasses and from_dict normalization
 *   sparquet/transform/engine.py — _BUILTIN_TRANSFORMATIONS registry
 *   sparquet/io/factory.py       — reader/writer registries
 *   sparquet/validation/engine.py — validator registry
 *
 * These types stay intentionally permissive (index signatures on transformation
 * and rule specs) because the framework forwards every unknown key into
 * `params`, and custom transformations/validators can be registered at runtime.
 */

/** Formats that can be read (ReaderFactory registry). */
export const READ_FORMATS = [
  'parquet',
  'csv',
  'delta',
  'iceberg',
  'txt',
  'view',
  'json',
  'orc',
  'avro',
  'xml',
  'binary',
  'hudi',
  'kafka',
  'postgresql',
  'mysql',
  'mariadb',
  'sqlserver',
  'oracle',
  'bigquery',
  'snowflake',
  'redshift',
  'mongodb',
  'documentdb',
  'dynamodb',
  'cassandra',
  'elasticsearch',
  'opensearch',
] as const

/** Formats that can be written (WriterFactory registry). */
export const WRITE_FORMATS = [
  'parquet',
  'csv',
  'delta',
  'iceberg',
  'txt',
  'view',
  'json',
  'orc',
  'avro',
  'xml',
  'hudi',
  'kafka',
  'postgresql',
  'mysql',
  'mariadb',
  'sqlserver',
  'oracle',
  'bigquery',
  'snowflake',
  'redshift',
  'mongodb',
  'documentdb',
  'dynamodb',
  'cassandra',
  'elasticsearch',
  'opensearch',
] as const

export type ReadFormat = (typeof READ_FORMATS)[number]
export type WriteFormat = (typeof WRITE_FORMATS)[number]
export type IoFormat = ReadFormat | WriteFormat

/** Write modes accepted by OutputConfig.mode. `merge` is Delta/Iceberg only. */
export const WRITE_MODES = ['overwrite', 'append', 'merge', 'ignore', 'error'] as const
export type WriteMode = (typeof WRITE_MODES)[number]

export const ON_FAILURE_MODES = ['fail', 'warn', 'skip'] as const
export type OnFailureMode = (typeof ON_FAILURE_MODES)[number]

/** Every transformation `type` string registered in the engine. */
export const TRANSFORMATION_TYPES = [
  'filter',
  'select',
  'drop',
  'rename',
  'cast',
  'with_column',
  'struct',
  'drop_duplicates',
  'distinct',
  'checkpoint',
  'stop_if_empty',
  'collect',
  'group_by',
  'sql',
  'fill_na',
  'sort',
  'join',
  'union',
  'debug',
] as const

export type TransformationType = (typeof TRANSFORMATION_TYPES)[number]

/** Every validation rule `type` string registered in the validation engine. */
export const VALIDATION_TYPES = [
  'not_null',
  'unique',
  'range',
  'regex',
  'row_count',
  'sql',
] as const

export type ValidationType = (typeof VALIDATION_TYPES)[number]

export interface SparkSettings {
  app_name?: string
  master?: string
  configs?: Record<string, string>
}

export interface InputSpec {
  format: string
  path: string
  options?: Record<string, unknown>
}

/** `{ "$include": "shared/filter.json" }` — expanded inline before parsing. */
export interface IncludeDirective {
  $include: string
}

export interface TransformationSpecBase {
  type: string
  /**
   * Guard evaluated after `{param}` template substitution:
   * empty string → skipped; boolean expression → skipped when false.
   */
  skip_if_false?: string
  [key: string]: unknown
}

export type TransformationSpec = TransformationSpecBase | IncludeDirective

export interface ValidationRuleSpec {
  type: string
  [key: string]: unknown
}

export interface ValidationsSpec {
  on_failure?: OnFailureMode
  rules?: ValidationRuleSpec[]
  /** Optional sink for the per-rule quality report. */
  report?: OutputSpec
  /** Optional row-routing (quarantine): keys `valid` / `invalid` → an output sink. */
  outputs?: Record<string, OutputSpec>
}

export interface OutputSpec {
  format: string
  path: string
  mode?: WriteMode | string
  partition_by?: string[]
  /** Per-destination column projection; omitted means "write every column". */
  columns?: string[]
  options?: Record<string, unknown>
  /** Per-destination transformations applied before projection and write. */
  transformations?: TransformationSpec[]
}

/**
 * A complete pipeline definition. `output` (single) and `outputs` (list) are both
 * accepted by PipelineConfig.from_dict; Studio always emits `outputs` when there
 * is more than one destination.
 */
export interface PipelineSpec {
  name: string
  description?: string
  spark?: SparkSettings
  input: InputSpec
  transformations?: TransformationSpec[]
  validations?: ValidationsSpec
  output?: OutputSpec
  outputs?: OutputSpec[]
}

export function isIncludeDirective(value: TransformationSpec): value is IncludeDirective {
  return typeof value === 'object' && value !== null && '$include' in value
}

export function isTransformationSpec(
  value: TransformationSpec,
): value is TransformationSpecBase {
  return !isIncludeDirective(value)
}

/** Normalizes `output` / `outputs` into a single list, mirroring from_dict. */
export function outputsOf(pipeline: PipelineSpec): OutputSpec[] {
  if (pipeline.outputs && pipeline.outputs.length > 0) return pipeline.outputs
  if (pipeline.output) return [pipeline.output]
  return []
}
