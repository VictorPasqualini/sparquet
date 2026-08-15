/**
 * Built-in validation rules — the exact six keys of `_BUILTIN_VALIDATORS`
 * (spark_framework/validation/engine.py): not_null, unique, range, regex,
 * row_count, custom_sql. Anything else must be registered at runtime.
 *
 * Validations report on the DataFrame after transformations and before any write;
 * they never modify it. Required params are read with `params["key"]` in Python, so
 * a missing one raises a raw KeyError at runtime — the forms here must enforce them.
 */

import type { FieldSpec, ValidatorDef } from '@/catalog/types'
import type { OnFailureMode } from '@/types/pipeline'

/** Hardcoded temp view the engine registers before running a `custom_sql` query. */
const VALIDATION_VIEW = '_validation_df'

const isBlank = (value: unknown): boolean =>
  value === undefined ||
  value === null ||
  (typeof value === 'string' && value.trim() === '') ||
  (Array.isArray(value) && value.length === 0)

const toList = (value: unknown): readonly unknown[] | null =>
  Array.isArray(value) ? (value as readonly unknown[]) : null

/** Number widgets can hand back strings, so bound checks parse before comparing. */
const toNumber = (value: unknown): number | null => {
  if (typeof value === 'number') return Number.isFinite(value) ? value : null
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value)
    return Number.isFinite(parsed) ? parsed : null
  }
  return null
}

const validateColumnList = (value: unknown): string | null => {
  const list = toList(value)
  if (!list || list.length === 0) return 'Add at least one column.'
  if (list.some((entry) => typeof entry !== 'string' || entry.trim() === ''))
    return 'Column names cannot be empty.'
  return null
}

const validateRequiredText = (message: string) => (value: unknown) =>
  typeof value === 'string' && value.trim() !== '' ? null : message

const columnsField = (help: string, docs: string): FieldSpec => ({
  key: 'columns',
  label: 'Columns',
  type: 'string-list',
  required: true,
  placeholder: 'id',
  help,
  docs,
  default: [],
  validate: validateColumnList,
})

const singleColumnField = (help: string): FieldSpec => ({
  key: 'column',
  label: 'Column',
  type: 'text',
  required: true,
  placeholder: 'valor',
  help,
  validate: validateRequiredText('Choose the column to check.'),
})

/** `range` with neither bound returns passed=true without reading the DataFrame. */
const requireAnyBound = (_value: unknown, params: Record<string, unknown>): string | null =>
  isBlank(params.min) && isBlank(params.max)
    ? 'Set a minimum, a maximum, or both — a rule with neither bound never checks anything.'
    : null

const validateMaxNotBelowMin =
  (fallbackMin: number | null) => (value: unknown, params: Record<string, unknown>) => {
    const max = toNumber(value)
    if (max === null) return null
    const min = toNumber(params.min) ?? fallbackMin
    return min !== null && max < min ? 'Maximum cannot be lower than the minimum.' : null
  }

export const VALIDATORS: ValidatorDef[] = [
  {
    type: 'not_null',
    label: 'Not null',
    icon: 'CircleSlash',
    summary: 'Fails when any listed column contains at least one NULL.',
    description: [
      'Counts NULLs in each listed column with its own `df.filter(col.isNull()).count()`.',
      'The rule fails as soon as one column has a single NULL.',
      '',
      'The message embeds a per-column dict of the offending counts, e.g.',
      "`Null values found in columns: {'id': 3, 'cpf': 1}` — only columns with a count",
      'greater than zero appear in it.',
    ].join('\n'),
    fields: [
      columnsField(
        'One Spark job per column — keep the list to the columns that matter.',
        [
          'Always a list, even for a single column: the framework iterates the value, so a',
          'bare string would be walked character by character and each character treated as',
          'a column name.',
          '',
          'Empty strings and NaN are **not** NULL and pass this rule.',
        ].join('\n'),
      ),
    ],
    keywords: ['null', 'missing', 'required', 'completeness', 'empty', 'mandatory', 'nulls'],
    gotchas: [
      'failed_count is the SUM of NULLs across every listed column, not the number of bad rows — a row NULL in two columns is counted twice.',
      "Only NULL counts: empty string '' and NaN pass.",
      'Runs one full count per column — N columns means N Spark jobs on the whole DataFrame.',
      'Omitting `columns` raises a Python KeyError that surfaces as PipelineResult.error, not as a validation failure.',
    ],
    examples: [
      {
        title: 'Business keys must always be present',
        json: '{ "type": "not_null", "columns": ["id_cessao", "numero_contrato"] }',
      },
      {
        title: 'Single column',
        json: '{ "type": "not_null", "columns": ["cpf"] }',
      },
    ],
  },
  {
    type: 'unique',
    label: 'Unique',
    icon: 'Fingerprint',
    summary: 'Fails when the combination of the listed columns is not unique.',
    description: [
      'Compares `df.count()` with `df.select(columns).distinct().count()`.',
      '',
      'Uniqueness is evaluated on the **tuple** of all listed columns, not per column:',
      '`["a", "b"]` validates the pair. Two separate rules are needed to require that',
      '`a` and `b` are each unique on their own.',
    ].join('\n'),
    fields: [
      columnsField(
        'Composite key — uniqueness is checked on all columns together.',
        [
          "Message format: `Found {n} duplicate rows for columns ['a', 'b']`.",
          '',
          "Spark's `distinct()` treats NULLs as equal, so many rows with a NULL key are",
          'reported as duplicates of each other.',
        ].join('\n'),
      ),
    ],
    keywords: [
      'duplicate',
      'duplicates',
      'primary key',
      'distinct',
      'composite key',
      'dedup',
      'uniqueness',
    ],
    gotchas: [
      'failed_count is the number of EXCESS rows (total - distinct), not the number of duplicated key groups: three rows sharing one key report 2.',
      'NULLs are treated as equal by distinct(), so rows with a NULL key are reported as duplicates.',
      'Costs two full scans of the DataFrame (count + distinct.count).',
      'To remove duplicates instead of reporting them, use the drop_duplicates transformation — validations never modify data.',
    ],
    examples: [
      {
        title: 'Composite key must be unique',
        json: '{ "type": "unique", "columns": ["id_cessao", "numero_contrato"] }',
      },
      {
        title: 'Surrogate key must be unique',
        json: '{ "type": "unique", "columns": ["id"] }',
      },
    ],
  },
  {
    type: 'range',
    label: 'Range',
    icon: 'Ruler',
    summary: 'Fails when a numeric column falls outside the inclusive [min, max] interval.',
    description: [
      'Counts rows matching `(col < min) OR (col > max)`, so both bounds are **inclusive**:',
      'a value exactly equal to min or max passes.',
      '',
      'Either bound may be omitted for an open-ended check, but with neither bound the',
      'validator returns `passed=true` without ever touching the DataFrame.',
    ].join('\n'),
    fields: [
      singleColumnField('Singular `column` here — not `columns` as in not_null/unique.'),
      {
        key: 'min',
        label: 'Minimum',
        type: 'number',
        placeholder: '0',
        help: 'Inclusive lower bound — a row fails only when the value is strictly lower.',
        validate: requireAnyBound,
      },
      {
        key: 'max',
        label: 'Maximum',
        type: 'number',
        placeholder: '150',
        help: 'Inclusive upper bound — a row fails only when the value is strictly higher.',
        docs: 'Leave empty for no upper bound. `null` in JSON is read the same as omitting the key.',
        validate: (value, params) => {
          const missing = requireAnyBound(value, params)
          if (missing) return missing
          return validateMaxNotBelowMin(null)(value, params)
        },
      },
    ],
    keywords: ['between', 'bounds', 'min', 'max', 'numeric', 'limit', 'boundary', 'outlier'],
    gotchas: [
      'NULLs PASS: the comparison yields NULL so the row is never matched — pair with a not_null rule to catch them.',
      'The key is `column` (singular); using `columns` raises a KeyError.',
      'Bounds are inclusive — a value equal to min or max is accepted.',
      'With both bounds omitted the rule is a silent no-op that always passes.',
      'The message renders a missing bound as None, e.g. `outside range [0, None]`.',
      'failed_count is the number of offending rows.',
    ],
    examples: [
      {
        title: 'Bounded on both sides',
        json: '{ "type": "range", "column": "idade", "min": 0, "max": 150 }',
      },
      {
        title: 'Lower bound only — no negative amounts',
        json: '{ "type": "range", "column": "valor", "min": 0 }',
      },
    ],
  },
  {
    type: 'regex',
    label: 'Regex',
    icon: 'Regex',
    summary: 'Fails when a string column does not match a regular expression.',
    description: [
      'Counts rows matching `~col.rlike(pattern) | col.isNull()`.',
      '',
      '`rlike` is a **partial** match: the pattern `abc` matches `xxabcxx`. Anchor the',
      'pattern with `^...$` when the whole value must match.',
      '',
      'NULLs are counted as failures here — the opposite of the range rule.',
    ].join('\n'),
    fields: [
      singleColumnField('Meant for string columns; other types are implicitly cast by Spark.'),
      {
        key: 'pattern',
        label: 'Pattern',
        type: 'text',
        required: true,
        placeholder: '^[0-9]{11}$',
        help: 'Java/Spark regex passed to rlike() — unanchored, so it matches anywhere in the value.',
        docs: [
          'Backslashes travel through JSON before reaching Spark, so they must be escaped:',
          'write `\\\\d` for a digit, `\\\\.` for a literal dot.',
          '',
          'Use `^...$` for a full-string match; without anchors `abc` also matches `xxabcxx`.',
        ].join('\n'),
        validate: validateRequiredText('Write the pattern the values must match.'),
      },
    ],
    keywords: ['pattern', 'rlike', 'format', 'match', 'string', 'email', 'cpf', 'regexp'],
    gotchas: [
      'NULLs COUNT AS FAILURES — the filter is `~rlike(pattern) OR isNull()`.',
      'rlike is a PARTIAL match: anchor with ^...$ to require the whole value to match.',
      'Backslashes must be JSON-escaped (\\\\d, not \\d).',
      'Non-string columns are implicitly cast by Spark, which can match unexpectedly.',
      'failed_count is the number of non-matching (or NULL) rows.',
    ],
    examples: [
      {
        title: 'Full-string CPF check',
        json: '{ "type": "regex", "column": "cpf", "pattern": "^[0-9]{11}$" }',
      },
      {
        title: 'Loose e-mail check (partial match)',
        json: '{ "type": "regex", "column": "email", "pattern": ".*@.*" }',
      },
    ],
  },
  {
    type: 'row_count',
    label: 'Row count',
    icon: 'Hash',
    summary: 'Fails when the total number of rows falls outside the expected interval.',
    description: [
      'Table-level rule — it takes no column at all. Fails when `count < min` or,',
      'when a maximum is given, `count > max`. Both bounds are inclusive.',
      '',
      '`{"type": "row_count", "min": 1}` is the idiomatic empty-DataFrame guard, but in',
      '`fail` mode it aborts the whole run. For a graceful stop use the `stop_if_empty`',
      'transformation instead.',
    ].join('\n'),
    fields: [
      {
        key: 'min',
        label: 'Minimum rows',
        type: 'number',
        default: 0,
        placeholder: '1',
        help: 'Inclusive lower bound. The engine defaults it to 0 when the key is absent.',
      },
      {
        key: 'max',
        label: 'Maximum rows',
        type: 'number',
        placeholder: '1000000',
        help: 'Inclusive upper bound. Leave empty for no upper bound.',
        validate: validateMaxNotBelowMin(0),
      },
    ],
    keywords: ['count', 'rows', 'volume', 'empty', 'size', 'cardinality', 'table'],
    gotchas: [
      'failed_count is HARDCODED to 1 on failure — it is never the number of missing or excess rows.',
      '`min` defaults to 0, so a rule with only `max` still prints "[0, ...]" in its message.',
      'A rule with neither min nor max always passes.',
      'Takes no column parameter — it always measures the whole DataFrame.',
      'Prefer the stop_if_empty transformation when an empty DataFrame should end the run gracefully instead of failing it.',
    ],
    examples: [
      {
        title: 'Guard against an empty load',
        json: '{ "type": "row_count", "min": 1 }',
      },
      {
        title: 'Expected volume window',
        json: '{ "type": "row_count", "min": 1000, "max": 1000000 }',
      },
    ],
  },
  {
    type: 'custom_sql',
    label: 'Custom SQL',
    icon: 'FileCode2',
    summary: `Runs a Spark SQL query over the ${VALIDATION_VIEW} view; passes when the result is true.`,
    description: [
      `The DataFrame is registered as the temp view \`${VALIDATION_VIEW}\` and the query runs`,
      'against it. Only row 0 / column 0 of the result is read and coerced with `bool()`.',
      '',
      '**Semantics are pass-when-true**: the query must express the invariant, not the',
      `violation — \`SELECT COUNT(*) = 0 FROM ${VALIDATION_VIEW} WHERE valor < 0\` passes`,
      'when no row is negative. A raw count inverts it, since `bool(0)` is false.',
    ].join('\n'),
    fields: [
      {
        key: 'query',
        label: 'Query',
        type: 'sql',
        required: true,
        rows: 4,
        placeholder: `SELECT COUNT(*) = 0 FROM ${VALIDATION_VIEW} WHERE valor < 0`,
        help: `Must read from ${VALIDATION_VIEW} and return a single boolean — true means the rule passes.`,
        docs: [
          `The view name is fixed and hardcoded as \`${VALIDATION_VIEW}\`; unlike the \`sql\``,
          'transformation there is no configurable `view_name`. Registering it also overwrites',
          `any session view already called \`${VALIDATION_VIEW}\`.`,
          '',
          'The query must return at least one row: an empty result raises an IndexError that',
          'surfaces as a pipeline error rather than as a validation failure.',
        ].join('\n'),
        validate: (value) => {
          if (typeof value !== 'string' || value.trim() === '')
            return 'Write a query that returns a single boolean.'
          return value.includes(VALIDATION_VIEW)
            ? null
            : `The query must read from the ${VALIDATION_VIEW} temp view.`
        },
      },
      {
        key: 'error_message',
        label: 'Error message',
        type: 'text',
        placeholder: 'Negative amounts found',
        help: 'Shown in the result and in the report when the query is falsy. Defaults to "Custom SQL validation failed".',
        group: 'advanced',
      },
    ],
    keywords: [
      'sql',
      'query',
      'custom',
      'expression',
      'assertion',
      'invariant',
      'business rule',
    ],
    gotchas: [
      `The temp view name is fixed: ${VALIDATION_VIEW}. There is no view_name option for this rule.`,
      'Pass-when-true: express the invariant (COUNT(*) = 0 FROM ... WHERE <bad>), never the violation.',
      'The result is coerced with bool(): 0, empty string and NULL are falsy; any other value is truthy.',
      'A query returning zero rows raises an IndexError that fails the pipeline instead of the rule.',
      'failed_count is HARDCODED to 1 on failure — it never reflects how many rows violated the condition.',
      `createOrReplaceTempView overwrites an existing session view named ${VALIDATION_VIEW}.`,
    ],
    examples: [
      {
        title: 'No negative amounts',
        json: `{ "type": "custom_sql", "query": "SELECT COUNT(*) = 0 FROM ${VALIDATION_VIEW} WHERE valor < 0", "error_message": "Negative amounts found" }`,
      },
      {
        title: 'Every contract belongs to a known assignment',
        json: `{ "type": "custom_sql", "query": "SELECT COUNT(*) = 0 FROM ${VALIDATION_VIEW} WHERE id_cessao IS NULL OR trim(id_cessao) = ''" }`,
      },
    ],
  },
]

/**
 * `warn` and `skip` are behaviorally identical: the engine only branches on
 * `on_failure == "fail"` (validation/engine.py). The hints must not promise otherwise.
 */
export const ON_FAILURE_OPTIONS: {
  value: OnFailureMode
  label: string
  hint: string
}[] = [
  {
    value: 'fail',
    label: 'Fail — abort the pipeline',
    hint: 'Default. Every rule still runs first, then a single error lists all failures and aborts: the validation report is NOT written and no output is written. The caller sees success=false with an empty validation_results list.',
  },
  {
    value: 'warn',
    label: 'Warn — log and continue',
    hint: 'Failures are logged as warnings and the run continues: the validation report IS written and every output is written. Identical to skip in the engine.',
  },
  {
    value: 'skip',
    label: 'Skip — ignore and continue',
    hint: 'Behaviorally identical to warn — failures are logged, the validation report IS written and every output is written. The engine only treats "fail" specially.',
  },
]
