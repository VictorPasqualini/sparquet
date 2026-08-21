/**
 * Built-in validation rules — the exact eight keys of `_BUILTIN_VALIDATORS`
 * (sparquet/validation/engine.py): not_null, unique, range, regex, row_count,
 * sql, plus the SODA-style `check` (metric + warn/fail threshold) and
 * `schema` (required/forbidden columns and types). Anything else is registered
 * at runtime.
 *
 * Validations report on the DataFrame after transformations and before any write;
 * they never modify it. Required params are read with `params["key"]` in Python, so
 * a missing one raises a raw KeyError at runtime — the forms here must enforce them.
 */

import type { FieldOption, FieldSpec, ValidatorDef } from '@/catalog/types'
import type { OnFailureMode } from '@/types/pipeline'

/** Hardcoded temp view the engine registers before running a `sql` query. */
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

// SODA-style `check` metric catalog. Column metrics need a `column` (or `columns`).
const CHECK_METRICS: FieldOption[] = [
  { value: 'row_count', label: 'row_count' },
  { value: 'distinct_count', label: 'distinct_count' },
  { value: 'missing_count', label: 'missing_count' },
  { value: 'missing_percent', label: 'missing_percent' },
  { value: 'duplicate_count', label: 'duplicate_count' },
  { value: 'duplicate_percent', label: 'duplicate_percent' },
  { value: 'invalid_count', label: 'invalid_count' },
  { value: 'invalid_percent', label: 'invalid_percent' },
  { value: 'min', label: 'min' },
  { value: 'max', label: 'max' },
  { value: 'avg', label: 'avg' },
  { value: 'sum', label: 'sum' },
  { value: 'stddev', label: 'stddev' },
  { value: 'freshness', label: 'freshness' },
]

const VALID_FORMATS: FieldOption[] = [
  'email',
  'uuid',
  'phone',
  'integer',
  'decimal',
  'number',
  'percentage',
  'date',
  'timestamp',
  'ip',
  'ipv4',
  'url',
  'boolean',
  'alphanumeric',
  'credit_card',
  'cpf',
  'cnpj',
].map((value) => ({ value, label: value }))

// Metrics scoped to a single column (need `column`); the rest use the whole df or `columns`.
const COLUMN_SCOPED_METRICS = new Set([
  'missing_count',
  'missing_percent',
  'invalid_count',
  'invalid_percent',
  'min',
  'max',
  'avg',
  'sum',
  'stddev',
  'freshness',
])

const requireColumnForMetric = (value: unknown, params: Record<string, unknown>): string | null => {
  const metric = typeof params.metric === 'string' ? params.metric : ''
  if (!COLUMN_SCOPED_METRICS.has(metric)) return null
  const hasColumn = typeof value === 'string' && value.trim() !== ''
  const hasColumns = Array.isArray(params.columns) && params.columns.length > 0
  return hasColumn || hasColumns ? null : `Metric "${metric}" needs a column.`
}

/**
 * Every rule accepts a `code`: the label that lands in the quarantine's annotation
 * column for each row the rule rejected. Declared once here rather than repeated in
 * fourteen definitions — it means the same thing everywhere.
 */
const codeField: FieldSpec = {
  key: 'code',
  label: 'Failure code',
  type: 'text',
  placeholder: 'AGE_RANGE',
  help: 'Written into the quarantine annotation column for every row this rule rejects.',
  docs: [
    'Omit it and the code becomes the rule expression itself — `range(age,1,99)`,',
    '`not_null(email)`, `unique(id)`. That is always readable, so a code is only worth',
    'declaring when you want a stable identifier your downstream owns: renaming a column',
    'or moving a bound changes the derived expression, and anything matching on the old',
    'string stops matching.',
    '',
    'It is also what `rules` on a quarantine destination refers to when you scope the',
    'split to a subset of the rules.',
  ].join('\n'),
  group: 'advanced',
}

const withCode = (validators: ValidatorDef[]): ValidatorDef[] =>
  validators.map((validator) => ({ ...validator, fields: [...validator.fields, codeField] }))

export const VALIDATORS: ValidatorDef[] = withCode([
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
    type: 'sql',
    label: 'SQL check',
    icon: 'FileCode2',
    summary: `Free-form SQL over the ${VALIDATION_VIEW} view: a boolean invariant (pass-when-true) OR a failed-rows query that returns the offending rows.`,
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
        label: 'Query (invariant)',
        type: 'sql',
        rows: 4,
        placeholder: `SELECT COUNT(*) = 0 FROM ${VALIDATION_VIEW} WHERE valor < 0`,
        help: `Pass-when-true: reads from ${VALIDATION_VIEW} and returns a single boolean. Leave empty if you use "Failed rows query" instead.`,
        docs: [
          `The view name is fixed and hardcoded as \`${VALIDATION_VIEW}\`; unlike the \`sql\``,
          'transformation there is no configurable `view_name`.',
          '',
          'The boolean query must return at least one row: an empty result raises an IndexError',
          'that surfaces as a pipeline error rather than as a validation failure.',
        ].join('\n'),
        validate: (value, params) => {
          const hasQuery = typeof value === 'string' && value.trim() !== ''
          const failed = params.failed_rows
          const hasFailed = typeof failed === 'string' && failed.trim() !== ''
          if (!hasQuery && !hasFailed)
            return 'Set a boolean query, or a "Failed rows query" below.'
          if (hasQuery && !value.includes(VALIDATION_VIEW))
            return `The query must read from the ${VALIDATION_VIEW} temp view.`
          return null
        },
      },
      {
        key: 'failed_rows',
        label: 'Failed rows query',
        type: 'sql',
        rows: 4,
        placeholder: `SELECT * FROM ${VALIDATION_VIEW} WHERE valor < 0`,
        help: `SODA-style "failed rows": returns the OFFENDING rows. The check fails if any come back, and failed_count is their count. Pair with "Failed rows output" to quarantine them.`,
        validate: (value, params) => {
          const hasFailed = typeof value === 'string' && value.trim() !== ''
          if (hasFailed && !value.includes(VALIDATION_VIEW))
            return `The failed-rows query must read from the ${VALIDATION_VIEW} temp view.`
          const query = params.query
          const hasQuery = typeof query === 'string' && query.trim() !== ''
          if (hasFailed && hasQuery)
            return 'Use a boolean query OR a failed-rows query, not both.'
          return null
        },
      },
      {
        key: 'error_message',
        label: 'Error message',
        type: 'text',
        placeholder: 'Negative amounts found',
        help: 'Shown in the result and report when the check fails. Defaults to "SQL validation failed".',
        group: 'advanced',
      },
      {
        key: 'output',
        label: 'Failed rows output',
        type: 'json',
        rows: 4,
        placeholder: '{ "format": "delta", "path": "dq.failed_rows", "mode": "overwrite" }',
        help: 'Optional sink for the failed rows: a full output config { format, path, mode, ... }. Only used together with a "Failed rows query".',
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
        json: `{ "type": "sql", "query": "SELECT COUNT(*) = 0 FROM ${VALIDATION_VIEW} WHERE valor < 0", "error_message": "Negative amounts found" }`,
      },
      {
        title: 'Every contract belongs to a known assignment',
        json: `{ "type": "sql", "query": "SELECT COUNT(*) = 0 FROM ${VALIDATION_VIEW} WHERE id_cessao IS NULL OR trim(id_cessao) = ''" }`,
      },
    ],
  },
  {
    type: 'check',
    label: 'Check (SODA)',
    icon: 'Sigma',
    summary: 'A metric compared to a warn/fail threshold — the big-data take on a SODA Core check.',
    description: [
      'Measures a metric over the DataFrame (or a column) and compares it to a threshold,',
      'with optional warn and fail levels — the framework equivalent of a SODA Core check.',
      '',
      '`must_be` is the PASS condition: `> 0`, `= 0`, `< 5%`, `between 10 and 100`, `< 1d`',
      '(freshness). A `warn` threshold downgrades a metric that passes `must_be` to a warning',
      'instead of a hard failure (severity "warn" does not abort under on_failure=fail).',
      '',
      'Metrics: row_count, distinct_count, missing_count/percent, duplicate_count/percent,',
      'invalid_count/percent, min, max, avg, sum, stddev, freshness. The invalid_* metrics',
      'read the valid_* configuration (valid_values, valid_format, valid_regex, valid_min/max).',
    ].join('\n'),
    fields: [
      {
        key: 'metric',
        label: 'Metric',
        type: 'select',
        required: true,
        options: CHECK_METRICS,
        help: 'What to measure. Column metrics (missing/invalid/min/max/avg/sum/stddev/freshness) need a column.',
        validate: (value) =>
          typeof value === 'string' && value.trim() !== '' ? null : 'Pick a metric to measure.',
      },
      {
        key: 'must_be',
        label: 'Must be (threshold)',
        type: 'text',
        required: true,
        placeholder: '< 5%',
        help: 'Pass condition: > < >= <= = != , or "between X and Y". Accepts % and durations (1d, 2h, 30m).',
        validate: validateRequiredText('Set a threshold, e.g. "> 0" or "< 5%".'),
      },
      {
        key: 'column',
        label: 'Column',
        type: 'text',
        placeholder: 'valor',
        help: 'Required for column metrics. Omit for row_count.',
        validate: requireColumnForMetric,
      },
      {
        key: 'columns',
        label: 'Columns',
        type: 'string-list',
        placeholder: 'id',
        help: 'For duplicate_count / distinct_count over a composite key.',
        group: 'advanced',
      },
      {
        key: 'warn',
        label: 'Warn threshold',
        type: 'text',
        placeholder: '= 0',
        help: 'Optional softer level: a metric that passes must_be but fails warn is reported as a warning.',
        group: 'advanced',
      },
      {
        key: 'name',
        label: 'Check name',
        type: 'text',
        placeholder: 'cpf completeness',
        help: 'Shown in the result and report — makes a failing check readable.',
        group: 'advanced',
      },
      {
        key: 'missing_values',
        label: 'Missing values',
        type: 'string-list',
        placeholder: 'N/A',
        help: 'Extra values treated as missing besides NULL (for missing_* metrics).',
        group: 'advanced',
      },
      {
        key: 'valid_format',
        label: 'Valid format',
        type: 'select',
        options: VALID_FORMATS,
        help: 'Named format for invalid_* metrics (email, uuid, cpf, cnpj, date…).',
        group: 'advanced',
      },
      {
        key: 'valid_values',
        label: 'Valid values',
        type: 'string-list',
        help: 'Accepted values for invalid_* metrics.',
        group: 'advanced',
      },
      {
        key: 'valid_regex',
        label: 'Valid regex',
        type: 'text',
        placeholder: '^[A-Z]{2}\\\\d{4}$',
        help: 'Custom regex for invalid_* metrics (overrides valid_format if both are set alongside it).',
        group: 'advanced',
      },
      { key: 'valid_min', label: 'Valid min', type: 'number', group: 'advanced' },
      { key: 'valid_max', label: 'Valid max', type: 'number', group: 'advanced' },
    ],
    keywords: [
      'soda',
      'check',
      'quality',
      'metric',
      'threshold',
      'missing',
      'duplicate',
      'invalid',
      'freshness',
      'warn',
      'completeness',
      'validity',
    ],
    gotchas: [
      'must_be is the PASS condition — "missing_percent < 5%" passes when the metric is below 5%.',
      'warn is optional; a metric passing must_be but failing warn is severity "warn" and does NOT abort under on_failure=fail.',
      'Column metrics need `column`; duplicate/distinct use the whole df or `columns`.',
      'invalid_* count present values that violate the valid_* config — a NULL is missing, not invalid.',
      'freshness is seconds since max(column); with no rows the age is infinite, so any "< X" fails.',
      'Percent metrics are 0–100 over the row count; the "%" in the threshold is cosmetic.',
    ],
    examples: [
      { title: 'Non-empty load', json: '{ "type": "check", "metric": "row_count", "must_be": "> 0" }' },
      {
        title: 'CPF completeness with a warn level',
        json: '{ "type": "check", "name": "cpf completeness", "metric": "missing_percent", "column": "cpf", "must_be": "< 1%", "warn": "= 0" }',
      },
      {
        title: 'No duplicate business keys',
        json: '{ "type": "check", "metric": "duplicate_count", "columns": ["id_cessao", "numero_contrato"], "must_be": "= 0" }',
      },
      {
        title: 'Valid e-mails',
        json: '{ "type": "check", "metric": "invalid_percent", "column": "email", "valid_format": "email", "must_be": "< 5%" }',
      },
      {
        title: 'Freshness under a day',
        json: '{ "type": "check", "metric": "freshness", "column": "atualizado_em", "must_be": "< 1d" }',
      },
    ],
  },
  {
    type: 'schema',
    label: 'Schema',
    icon: 'Columns3',
    summary: 'Asserts required/forbidden columns and column types (SODA-style schema check).',
    description: [
      'Structural check: asserts that columns exist, are absent, or carry the expected',
      'Spark type. Fails listing every problem found in a single message.',
      '',
      'Type matching is case-insensitive with aliases (long→bigint, integer→int, str→string);',
      'decimal matches on the base type, ignoring precision/scale.',
    ].join('\n'),
    fields: [
      {
        key: 'required_columns',
        label: 'Required columns',
        type: 'string-list',
        placeholder: 'id',
        help: 'Columns that must exist.',
      },
      {
        key: 'forbidden_columns',
        label: 'Forbidden columns',
        type: 'string-list',
        help: 'Columns that must NOT exist (e.g. leaked internal fields).',
        group: 'advanced',
      },
      {
        key: 'column_types',
        label: 'Column types',
        type: 'key-value',
        help: 'Map column → expected Spark type: int, bigint, string, double, timestamp, date, decimal(p,s). Aliases accepted.',
        group: 'advanced',
      },
    ],
    keywords: ['schema', 'columns', 'types', 'structure', 'contract', 'data contract', 'required'],
    gotchas: [
      'Type matching is case-insensitive with aliases; decimal matches the base type ignoring precision/scale.',
      'A required column that is also typed is reported once (as missing), not twice.',
      'failed_count is the number of schema problems found, not a row count.',
      'At least one of required_columns / forbidden_columns / column_types should be set — an empty schema check always passes.',
    ],
    examples: [
      {
        title: 'Required business keys',
        json: '{ "type": "schema", "required_columns": ["id_cessao", "numero_contrato"] }',
      },
      {
        title: 'Columns and types',
        json: '{ "type": "schema", "required_columns": ["id", "valor"], "column_types": { "id": "bigint", "valor": "double" } }',
      },
    ],
  },
])

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
