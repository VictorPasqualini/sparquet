/**
 * Core transformations — the row/column shaping vocabulary of the framework.
 *
 * Every field below mirrors a JSON key that `sparquet/transform/builtin.py`
 * actually reads. The framework performs NO schema validation: unknown keys are
 * absorbed into `params` and ignored, required keys blow up at runtime as a
 * KeyError inside `PipelineResult.error`. This catalog is the only validation layer,
 * so required-ness, defaults and `validate` here are load-bearing.
 */

import type { FieldSpec, TransformationDef } from '@/catalog/types'

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

const asStringList = (value: unknown): string[] =>
  Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : []

const requireList =
  (message: string) =>
  (value: unknown): string | null =>
    asStringList(value).length > 0 ? null : message

const requireMap =
  (message: string) =>
  (value: unknown): string | null =>
    isRecord(value) && Object.keys(value).length > 0 ? null : message

/**
 * `columns` (the expression-map form) wins over `column` + `expression` by KEY
 * PRESENCE, not by content: the engine only tests `params.get("columns") is not None`
 * (builtin.py:71-75), so an empty map still takes the multi-column branch, iterates
 * nothing and never reads the single-column keys. JSON `null` is the one value that
 * falls back to `column` / `expression`.
 */
const usesExpressionMap = (params: Record<string, unknown>): boolean =>
  params.columns !== undefined && params.columns !== null

const identifierPattern = /^[A-Za-z_][A-Za-z0-9_]*$/

interface PathNode {
  leaf: boolean
  children: Map<string, PathNode>
}

const emptyPathNode = (): PathNode => ({ leaf: false, children: new Map() })

/**
 * Mirrors `_expand_dotpaths` (builtin.py:135-159): dotted keys auto-nest, two maps on
 * the same path merge, everything else collides. Reproduced client-side because the
 * Python version only raises at apply time, after the job has already started.
 */
const structPathError = (
  fields: Record<string, unknown>,
  root: PathNode,
  prefix: string,
): string | null => {
  for (const [key, value] of Object.entries(fields)) {
    const path = prefix ? `${prefix}.${key}` : key
    const parts = key.split('.')
    let node = root
    for (let index = 0; index < parts.length - 1; index += 1) {
      const part = parts[index]
      if (!part) return `Field "${path}" has an empty path segment.`
      const next = node.children.get(part) ?? emptyPathNode()
      if (next.leaf) return `Path conflict in "${path}": "${part}" is already a leaf value.`
      node.children.set(part, next)
      node = next
    }

    const last = parts[parts.length - 1]
    if (!last) return `Field "${path}" has an empty path segment.`
    const existing = node.children.get(last)

    if (isRecord(value)) {
      if (existing?.leaf)
        return `Path conflict in "${path}": "${last}" is already a leaf value.`
      const branch = existing ?? emptyPathNode()
      node.children.set(last, branch)
      const nested = structPathError(value, branch, path)
      if (nested) return nested
      continue
    }

    if (typeof value !== 'string') return `Field "${path}" must be a SQL expression string.`
    if (!value.trim()) return `Field "${path}" has an empty expression.`
    if (existing) return `Duplicate struct field "${path}".`
    node.children.set(last, { leaf: true, children: new Map() })
  }

  return null
}

const parseJsonField = (value: unknown): Record<string, unknown> | string => {
  if (isRecord(value)) return value
  if (typeof value === 'string') {
    try {
      const parsed: unknown = JSON.parse(value)
      if (isRecord(parsed)) return parsed
      return 'Expected a JSON object.'
    } catch {
      return 'Invalid JSON.'
    }
  }
  return 'Expected a JSON object.'
}

/** Fields shared by the single-column and multi-column forms of `with_column`. */
const buildWithColumnFields = (): FieldSpec[] => [
  {
    key: 'column',
    label: 'Column name',
    type: 'text',
    placeholder: 'total_liquido',
    help: 'Name of the column to create. An existing column with the same name is replaced.',
    docs: [
      'Single-column form. The engine reads `params.get("column", params.get("name"))`,',
      'so the legacy `name` key still works but `column` wins when both are present.',
      '',
      'This field is ignored entirely as soon as the **Multiple columns** key is present,',
      'even when that map is empty.',
    ].join('\n'),
    visibleWhen: (params) => !usesExpressionMap(params),
    validate: (value, params) => {
      if (usesExpressionMap(params)) return null
      const single = typeof value === 'string' ? value.trim() : ''
      const legacy = typeof params.name === 'string' ? params.name.trim() : ''
      if (single || legacy) return null
      return 'Set a column name, or fill the multiple-columns map instead — the engine raises ValueError when both forms are empty.'
    },
  },
  {
    key: 'expression',
    label: 'SQL expression',
    type: 'sql',
    rows: 3,
    placeholder: 'quantidade * preco_unitario',
    help: 'Evaluated with F.expr against the columns available at this point.',
    supportsRuntimeVars: true,
    visibleWhen: (params) => !usesExpressionMap(params),
    validate: (value, params) => {
      if (usesExpressionMap(params)) return null
      const expression = typeof value === 'string' ? value.trim() : ''
      return expression ? null : 'The single-column form needs an expression.'
    },
  },
  {
    key: 'columns',
    label: 'Multiple columns (name → expression)',
    type: 'expression-map',
    group: 'advanced',
    help: 'Present at all — even empty — this map takes over: column, name and expression are ignored.',
    docs: [
      'Multi-column form: each entry becomes one `withColumn` call, **in key order**.',
      'Because they are applied sequentially, a later expression can reference a column',
      'defined earlier in the same block:',
      '',
      '```json',
      '{ "base": "valor * 1.0", "imposto": "base * 0.15", "total": "base + imposto" }',
      '```',
      '',
      'Precedence is absolute and decided by the key alone — whenever `columns` is present,',
      '`column` / `name` / `expression` are dead configuration and are never read. An empty',
      'map is therefore not a fallback: the node runs, creates nothing and reports no error.',
      'Key order is semantically load-bearing: reordering the map can break expressions that',
      'depend on earlier entries.',
    ].join('\n'),
    supportsRuntimeVars: true,
    validate: (value, params) => {
      if (!usesExpressionMap(params)) return null
      if (!isRecord(value)) return 'The columns map must be an object of name → SQL expression.'
      if (Object.keys(value).length === 0) {
        return 'An empty columns map still wins over column/expression and creates nothing. Add an entry, or remove the key to use the single-column form.'
      }
      return null
    },
  },
  {
    key: 'name',
    label: 'name (legacy alias of column)',
    type: 'text',
    group: 'advanced',
    help: 'Backward-compatible alias kept for imported configs. `column` wins when both are set.',
    visibleWhen: (params) => !usesExpressionMap(params) && params.name !== undefined,
  },
]

const WITH_COLUMN_KEYWORDS = [
  'derive',
  'compute',
  'expression',
  'calculated',
  'formula',
  'withColumn',
  'case when',
  'literal',
  'concat',
]

const WITH_COLUMN_GOTCHAS = [
  'Pick one form: the `columns` key wins as soon as it is present — an empty map creates nothing and still makes `column` / `expression` dead config.',
  'Keep the key order of the `columns` map — entries run top to bottom and later ones can reference earlier ones.',
  'Reusing an existing column name replaces that column instead of adding one.',
  'The expression is SQL, not a column name: quote literals ("\'PENDENTE\'") and backtick names that need escaping.',
  'Leaving both forms empty raises ValueError at apply time — the pipeline fails, it does not skip.',
]

export const CORE_TRANSFORMATIONS: TransformationDef[] = [
  {
    type: 'filter',
    label: 'Filter',
    family: 'shape',
    accent: 'transform',
    icon: 'Filter',
    summary: 'Keep only the rows matching a SQL boolean expression.',
    description: [
      'Runs `df.filter(condition)` with your string handed straight to Spark, so anything valid',
      'in a SQL `WHERE` clause works: comparisons, `AND` / `OR` / `NOT`, `IN`, `LIKE`, `BETWEEN`,',
      '`IS NULL` and function calls. The expression is resolved against the columns present at',
      'this point of the chain — including `ingestion_ts`, which the pipeline appends right after',
      'reading the input. Filter as early as possible: every later step (joins, structs, writes)',
      'then works over fewer rows. This is also the usual place to push a `{{runtime_var}}` list',
      'collected upstream, for predicate pushdown on Delta/Iceberg reads.',
    ].join(' '),
    fields: [
      {
        key: 'condition',
        label: 'Condition',
        type: 'sql',
        required: true,
        rows: 3,
        placeholder: "status = 'ATIVO' AND valor > 0",
        help: 'SQL boolean expression. Rows where it evaluates to true are kept.',
        docs: [
          'Passed verbatim to `df.filter()`. Nothing is checked against the real schema before',
          'the run, so a typo surfaces only as a Spark AnalysisException in `result.error`.',
          '',
          'NULL semantics are SQL semantics: `col <> 1` drops NULLs too. Use',
          '`(col <> 1 OR col IS NULL)` when you mean to keep them.',
        ].join('\n'),
        supportsRuntimeVars: true,
        validate: (value) =>
          typeof value === 'string' && value.trim() ? null : 'Write a boolean SQL expression.',
      },
    ],
    keywords: ['where', 'condition', 'predicate', 'restrict', 'rows', 'sql', 'subset'],
    gotchas: [
      'Check the column names yourself — a bad expression fails only at Spark analysis time, as result.error.',
      'Quote string literals with single quotes, never double quotes.',
      'Remember NULL semantics: `col <> 1` also removes NULL rows.',
      'An empty result is not an error — add a stop_if_empty node when the run should end there.',
      'Place it before joins and struct building so the heavy steps see fewer rows.',
    ],
    examples: [
      {
        title: 'Business filter',
        json: `{
  "type": "filter",
  "condition": "status = 'ATIVO' AND valor_total > 0"
}`,
      },
      {
        title: 'Pushdown of a runtime variable collected upstream',
        json: `{
  "type": "filter",
  "condition": "id_cessao IN ({{cessoes_pendentes}})"
}`,
      },
      {
        title: 'Conditional filter driven by a template param',
        json: `{
  "type": "filter",
  "skip_if_false": "{registradora}",
  "condition": "registradora = '{registradora}'"
}`,
      },
    ],
  },

  {
    type: 'select',
    label: 'Select',
    family: 'shape',
    accent: 'transform',
    icon: 'Columns3',
    summary: 'Project a list of columns or full SQL expressions, in order.',
    description: [
      'Rebuilds the DataFrame from the listed entries. **Every entry goes through `F.expr`**, never',
      '`F.col`, so a row can be a plain column name (`id`) or a complete SQL expression with an',
      'alias (`to_json(payload) AS value`, `CAST(id AS STRING) AS id_str`). The projection is',
      'destructive — anything not listed is dropped, including the auto-added `ingestion_ts` — and',
      'the list order becomes the output column order. Use it to lock the final shape before a',
      'write, or to reduce a wide DataFrame right after a join.',
    ].join(' '),
    fields: [
      {
        key: 'columns',
        label: 'Columns / expressions',
        type: 'sql-list',
        required: true,
        placeholder: 'to_json(payload) AS value',
        help: 'One entry per output column. Plain names or SQL expressions with an alias.',
        docs: [
          'Each item is parsed as SQL, so names that are not valid SQL identifiers (spaces, dots,',
          'reserved words) **must be backticked**: `` `data de emissao` ``. A plain string is not',
          'escaped for you.',
          '',
          'A dotted name such as `a.b` is read as field `b` of struct `a`, not as a column literally',
          'named `a.b` — backtick it when you mean the literal name.',
        ].join('\n'),
        supportsRuntimeVars: true,
        validate: requireList('Add at least one column or expression.'),
      },
    ],
    keywords: ['project', 'columns', 'expr', 'alias', 'keep', 'reorder', 'to_json', 'cast'],
    gotchas: [
      'Backtick any name with spaces, dots or reserved words — every entry is parsed as SQL.',
      'Write "a.b" only when you mean struct field b of a; backtick it for a literal dotted name.',
      'Anything you leave out is dropped, ingestion_ts included.',
      'The list order is the output column order.',
      'Alias every expression (AS nome) or Spark names the column after the expression text.',
    ],
    examples: [
      {
        title: 'Final shape before writing',
        json: `{
  "type": "select",
  "columns": ["id_cessao", "numero_contrato", "valor_total", "ingestion_ts"]
}`,
      },
      {
        title: 'Serialize a payload for Kafka',
        json: `{
  "type": "select",
  "columns": ["id_externo AS key", "to_json(payload) AS value"]
}`,
      },
      {
        title: 'Escaping a name that is not a valid identifier',
        json: `{
  "type": "select",
  "columns": ["id", "\`data de emissao\` AS data_emissao"]
}`,
      },
    ],
  },

  {
    type: 'drop',
    label: 'Drop columns',
    family: 'shape',
    accent: 'transform',
    icon: 'Trash2',
    summary: 'Remove the named columns and keep everything else.',
    description: [
      'Calls `df.drop(*columns)`. Names are matched literally — this step never parses SQL — and',
      'columns that do not exist are ignored silently by Spark. Reach for it when you want to peel',
      'off a handful of helper columns (join keys, temporary flags) and keep an unknown remainder;',
      'use `select` instead when you keep fewer columns than you remove, because `select` also',
      'fixes the output order.',
    ].join(' '),
    fields: [
      {
        key: 'columns',
        label: 'Columns to drop',
        type: 'string-list',
        required: true,
        placeholder: 'tmp_join_key',
        help: 'Plain column names. Non-existent names are ignored without error.',
        validate: requireList('Add at least one column to drop.'),
      },
    ],
    keywords: ['remove', 'delete', 'columns', 'prune', 'cleanup', 'discard'],
    gotchas: [
      'Typos are invisible: Spark ignores names that do not exist instead of failing.',
      'Prefer select when you keep fewer columns than you drop — it also sets the column order.',
      'Dropping a column used by a later expression fails downstream, not here.',
    ],
    examples: [
      {
        title: 'Remove helper columns after a join',
        json: `{
  "type": "drop",
  "columns": ["tmp_join_key", "debug_flag"]
}`,
      },
      {
        title: 'Drop the auto-added ingestion timestamp',
        json: `{
  "type": "drop",
  "columns": ["ingestion_ts"]
}`,
      },
    ],
  },

  {
    type: 'rename',
    label: 'Rename',
    family: 'shape',
    accent: 'transform',
    icon: 'PenLine',
    summary: 'Rename columns through an old → new mapping.',
    description: [
      'Applies one `withColumnRenamed(old, new)` per entry, **sequentially in key order**. Nothing',
      'else about the column changes: same type, same values, same position. A source column that',
      'does not exist is a silent no-op, so a typo leaves the original name in place. Use it to',
      'normalize incoming names to your target schema; use `with_column` when the value itself has',
      'to be computed.',
    ].join(' '),
    fields: [
      {
        key: 'mappings',
        label: 'Renames (old → new)',
        type: 'key-value',
        required: true,
        placeholder: 'id_old',
        help: 'Applied one by one, top to bottom — the order of the keys is part of the behavior.',
        docs: [
          'Because renames are sequential, chains are possible and order-sensitive: `a → b`',
          'followed by `b → c` ends with a single column named `c`. Reordering the map changes',
          'the result.',
          '',
          'A rename onto a name that already exists produces two columns with the same name, which',
          'makes every later reference ambiguous.',
        ].join('\n'),
        validate: requireMap('Add at least one old → new pair.'),
      },
    ],
    keywords: ['alias', 'columns', 'mapping', 'normalize', 'withColumnRenamed', 'names'],
    gotchas: [
      'Keep the key order — renames run sequentially, so a→b then b→c ends at c.',
      'A missing source column is a silent no-op; verify the spelling yourself.',
      'Do not rename onto an existing name — duplicated names make later references ambiguous.',
      'Rename cannot compute a value; use with_column for that.',
    ],
    examples: [
      {
        title: 'Normalize source names',
        json: `{
  "type": "rename",
  "mappings": {
    "id_old": "id_cessao",
    "nm_sacado": "nome_sacado"
  }
}`,
      },
      {
        title: 'Free a name before writing',
        json: `{
  "type": "rename",
  "mappings": {
    "value": "value_original"
  }
}`,
      },
    ],
  },

  {
    type: 'cast',
    label: 'Cast',
    family: 'shape',
    accent: 'transform',
    icon: 'Type',
    summary: 'Convert columns to other Spark data types.',
    description: [
      'Applies `withColumn(name, F.col(name).cast(type))` for each entry. Because it uses `F.col`',
      'and not `F.expr`, **each key must be an existing column name** — a typo raises an',
      'AnalysisException. Values are Spark type strings such as `string`, `int`, `bigint`,',
      '`decimal(18,2)`, `date`, `timestamp`, `boolean`, `array<string>`. Values that do not fit the',
      'target type become NULL rather than failing the run, so pair a risky cast with a `not_null`',
      'validation when the conversion matters.',
    ].join(' '),
    fields: [
      {
        key: 'columns',
        label: 'Casts (column → Spark type)',
        type: 'key-value',
        required: true,
        placeholder: 'valor_total',
        help: 'Keys are existing column names; values are Spark type strings.',
        docs: [
          'Accepted values are anything Spark can parse as a DataType:',
          '',
          '- `string`, `boolean`, `binary`',
          '- `int` / `bigint` / `smallint`, `float`, `double`, `decimal(18,2)`',
          '- `date`, `timestamp`',
          '- `array<string>`, `map<string,string>`, `struct<a:int,b:string>`',
          '',
          'String → date/timestamp casts follow the session format; use `to_date(col, "ddMMyyyy")`',
          'inside a `with_column` when the source layout is non-standard.',
        ].join('\n'),
        validate: requireMap('Add at least one column → type pair.'),
      },
    ],
    keywords: ['type', 'convert', 'schema', 'decimal', 'date', 'timestamp', 'int', 'string'],
    gotchas: [
      'Every key must be a real column — cast uses F.col(), so a typo raises AnalysisException.',
      'Values that do not fit become NULL instead of failing; validate afterwards when it matters.',
      'Use full Spark type strings: decimal(18,2), timestamp, array<string>.',
      'For non-standard date layouts use to_date/to_timestamp in a with_column instead.',
    ],
    examples: [
      {
        title: 'Normalize numeric and date types',
        json: `{
  "type": "cast",
  "columns": {
    "valor_total": "decimal(18,2)",
    "data_emissao": "date"
  }
}`,
      },
      {
        title: 'Force keys to string before a join',
        json: `{
  "type": "cast",
  "columns": {
    "id_cessao": "string",
    "numero_contrato": "string"
  }
}`,
      },
    ],
  },

  {
    type: 'with_column',
    label: 'With column',
    family: 'compute',
    accent: 'transform',
    icon: 'Variable',
    summary: 'Add or replace computed columns from SQL expressions.',
    description: [
      'Creates columns from SQL expressions evaluated with `F.expr`, in one of two mutually',
      'exclusive forms. The **single-column form** takes `column` (legacy alias: `name`) plus',
      '`expression`. The **multi-column form** takes a `columns` map of name → expression, applied',
      'in key order, so a later entry can reference a column created by an earlier one. As soon as',
      'the `columns` key is present it takes full precedence and the single-column keys are never',
      'read. Naming an existing column replaces it, which is the idiomatic way to clean a value in',
      'place.',
    ].join(' '),
    fields: buildWithColumnFields(),
    keywords: WITH_COLUMN_KEYWORDS,
    gotchas: WITH_COLUMN_GOTCHAS,
    examples: [
      {
        title: 'Single computed column',
        json: `{
  "type": "with_column",
  "column": "valor_liquido",
  "expression": "valor_total - valor_desconto"
}`,
      },
      {
        title: 'Several columns, later ones reusing earlier ones',
        json: `{
  "type": "with_column",
  "columns": {
    "base": "valor_total",
    "imposto": "base * 0.15",
    "total_com_imposto": "base + imposto"
  }
}`,
      },
      {
        title: 'Clean a value in place',
        json: `{
  "type": "with_column",
  "column": "documento",
  "expression": "lpad(regexp_replace(documento, '[^0-9]', ''), 14, '0')"
}`,
      },
    ],
  },

  {
    type: 'struct',
    label: 'Struct',
    family: 'compute',
    accent: 'transform',
    icon: 'Braces',
    summary: 'Build a nested struct column from a field → expression map.',
    description: [
      'Assembles one struct column from a map of fields, where a **string value is a SQL',
      'expression** and a **nested object value is a nested struct**. Keys may be dot-paths',
      '(`data.nc.issuerName`), which auto-nest into sub-structs, so a deep API payload can be',
      'declared as a flat, diff-friendly table instead of a pile of `named_struct` calls; both',
      'styles can be mixed in the same map. Field order follows the key order of the map and is',
      'preserved through dot-path expansion. Typical use is composing the JSON body of an event',
      'right before `to_json(payload)` in a `select` or in an output transformation.',
    ].join(' '),
    fields: [
      {
        key: 'column',
        label: 'Struct column name',
        type: 'text',
        required: true,
        placeholder: 'payload',
        help: 'Name of the struct column produced. The legacy key `name` is also accepted on read.',
        docs: [
          'The engine reads `params.get("column", params.get("name"))`, so imported configs using',
          '`name` still work — but new nodes should always emit `column`. When neither key is',
          'present the transformation raises',
          "`ValueError(\"struct requer 'column' (ou 'name').\")` at apply time.",
        ].join('\n'),
        validate: (value, params) => {
          const column = typeof value === 'string' ? value.trim() : ''
          const legacy = typeof params.name === 'string' ? params.name.trim() : ''
          return column || legacy ? null : 'Name the struct column.'
        },
      },
      {
        key: 'fields',
        label: 'Fields (name → expression)',
        type: 'json',
        required: true,
        rows: 10,
        default: {},
        help: 'String value = SQL expression. Object value = nested struct. Dotted keys auto-nest.',
        docs: [
          '```json',
          '{',
          '  "id_externo": "id_vert",',
          '  "data.nc.issuerName": "nome_sacado",',
          '  "data.nc.paymentMethod.indexCode": "lpad(codigo_indice, 4, \'0\')",',
          '  "data.nc.amount": { "value": "valor_total", "currency": "\'BRL\'" }',
          '}',
          '```',
          '',
          '**Ordering** — fields are written in key order and dot-path expansion merges common',
          'prefixes while preserving that order, so reordering the map reorders the struct.',
          '',
          '**Conflicts (ValueError at apply time)** — reusing a path segment that is already a leaf',
          "raises `struct: conflito no caminho '<key>': '<part>' já é um valor folha`, and a",
          "duplicated leaf key raises `struct: campo conflitante/duplicado '<key>'`. Two object",
          'values on the same path are merged instead of failing.',
          '',
          '**Leaf values are SQL, not column names** — literals must be quoted (`"\'BRL\'"`) and',
          'names that need escaping must be backticked. A field name containing a literal dot is',
          'impossible: every dot is treated as nesting.',
        ].join('\n'),
        supportsRuntimeVars: true,
        validate: (value) => {
          const parsed = parseJsonField(value)
          if (typeof parsed === 'string') return parsed
          if (Object.keys(parsed).length === 0) return 'Add at least one field.'
          return structPathError(parsed, emptyPathNode(), '')
        },
      },
    ],
    keywords: ['nested', 'payload', 'json', 'named_struct', 'object', 'dot path', 'event'],
    gotchas: [
      "Leaf values are SQL expressions — quote literals ('BRL') and backtick names needing escaping.",
      'Every dot in a key means nesting; a field whose name contains a literal dot is impossible.',
      'Keep the key order — it is the field order of the resulting struct.',
      'Do not reuse a path both as a leaf and as a parent, and do not repeat a leaf key: both raise ValueError at apply time.',
      'Two object values on the same path are merged, not rejected — check for accidental merges.',
      'The legacy key `name` is accepted on read, but always emit `column`.',
    ],
    examples: [
      {
        title: 'Flat dot-path payload',
        json: `{
  "type": "struct",
  "column": "payload",
  "fields": {
    "id_externo": "id_vert",
    "data.nc.issuerName": "nome_sacado",
    "data.nc.paymentMethod.indexCode": "lpad(codigo_indice, 4, '0')"
  }
}`,
      },
      {
        title: 'Dot-paths mixed with a nested object',
        json: `{
  "type": "struct",
  "column": "payload",
  "fields": {
    "header.tipo": "'REGISTRO'",
    "data.amount": {
      "value": "valor_total",
      "currency": "'BRL'"
    }
  }
}`,
      },
      {
        title: 'Serializing the struct right after building it',
        json: `{
  "type": "select",
  "columns": ["id_externo AS key", "to_json(payload) AS value"]
}`,
      },
    ],
  },

  {
    type: 'drop_duplicates',
    label: 'Drop duplicates',
    family: 'shape',
    accent: 'transform',
    icon: 'CopyMinus',
    summary: 'Deduplicate rows, optionally by a subset of columns.',
    description: [
      'Calls `df.dropDuplicates(columns)` when a subset is given, and plain `df.dropDuplicates()`',
      'otherwise — so **omitting the columns (or leaving the list empty) is exactly `distinct`**.',
      'With a subset, Spark keeps one arbitrary row per key: which one survives is not defined, so',
      'put a `sort` before it (or aggregate with `group_by`) when a specific row must win. Note',
      'that the pipeline appends `ingestion_ts` after the input read; it is constant within a run,',
      'so it does not break the all-columns form.',
    ].join(' '),
    fields: [
      {
        key: 'columns',
        label: 'Key columns (optional)',
        type: 'string-list',
        placeholder: 'id_cessao',
        help: 'Columns that define a duplicate. Leave empty to deduplicate over every column.',
        docs: [
          'The Python check is `if columns:`, so an **empty list behaves like an absent key** —',
          'it means "all columns", never "no columns".',
          '',
          'Plain column names only; this step does not parse SQL expressions.',
        ].join('\n'),
      },
    ],
    keywords: ['dedupe', 'duplicates', 'unique', 'distinct', 'subset', 'key'],
    gotchas: [
      'Leaving the list empty means all columns, not none — that is the same as distinct.',
      'Which row survives is arbitrary: sort first, or use group_by, when a specific row must win.',
      'Use plain column names — expressions are not parsed here.',
      'Dedupe on the natural key, not on the whole row, when the row carries volatile columns.',
    ],
    examples: [
      {
        title: 'One row per business key',
        json: `{
  "type": "drop_duplicates",
  "columns": ["id_cessao", "numero_contrato"]
}`,
      },
      {
        title: 'Deterministic winner: sort, then dedupe',
        json: `[
  { "type": "sort", "columns": ["id_cessao", "data_evento"], "ascending": [true, false] },
  { "type": "drop_duplicates", "columns": ["id_cessao"] }
]`,
      },
      {
        title: 'All columns (equivalent to distinct)',
        json: `{
  "type": "drop_duplicates"
}`,
      },
    ],
  },

  {
    type: 'distinct',
    label: 'Distinct',
    family: 'shape',
    accent: 'transform',
    icon: 'Fingerprint',
    summary: 'Remove duplicate rows using every column.',
    description: [
      'Calls `df.distinct()` — two rows are duplicates only when **all** their columns are equal.',
      'It reads no parameters at all: any extra key on the node is absorbed into params and',
      'ignored. Functionally identical to `drop_duplicates` with no subset; prefer `distinct` when',
      'you mean "the whole row", and `drop_duplicates` when you mean "one row per key". Watch out',
      'for volatile columns (event timestamps, generated ids): a single differing column is enough',
      'to keep both rows.',
    ].join(' '),
    fields: [],
    keywords: ['dedupe', 'duplicates', 'unique', 'all columns', 'drop_duplicates'],
    gotchas: [
      'Compares every column — one volatile column (a timestamp, a generated id) defeats it.',
      'Use drop_duplicates with a subset when only the business key should decide.',
      'Extra JSON keys on this node are silently ignored.',
      'It is a shuffle: place it after the filters that shrink the data, not before.',
    ],
    examples: [
      {
        title: 'Whole-row dedupe',
        json: `{
  "type": "distinct"
}`,
      },
      {
        title: 'Dedupe a projection instead of the wide row',
        json: `[
  { "type": "select", "columns": ["id_cessao", "registradora"] },
  { "type": "distinct" }
]`,
      },
    ],
  },

  {
    type: 'sort',
    label: 'Sort',
    family: 'shape',
    accent: 'transform',
    icon: 'ArrowUpDown',
    summary: 'Order rows by one or more columns.',
    description: [
      'Calls `df.orderBy(...)` with the columns resolved through `F.col`, so **plain column names',
      'only — no SQL expressions here** (unlike `select`). Direction is set by `ascending`, which',
      'accepts either a single boolean applied to every column or a list of booleans zipped',
      'positionally with the columns for per-column direction. A global sort is a full shuffle:',
      'use it to make a later `drop_duplicates` deterministic or to produce ordered files, not as',
      'cosmetic ordering in the middle of a chain.',
    ].join(' '),
    fields: [
      {
        key: 'columns',
        label: 'Sort columns',
        type: 'string-list',
        required: true,
        placeholder: 'data_evento',
        help: 'Plain column names, in priority order. Expressions are not parsed here.',
        validate: requireList('Add at least one column to sort by.'),
      },
      {
        key: 'ascending',
        label: 'Direction',
        type: 'json',
        default: true,
        rows: 2,
        help: 'true, false, or a list of booleans — one per column, in the same order.',
        docs: [
          '- `true` (default) — every column ascending.',
          '- `false` — every column descending.',
          '- `[true, false]` — per-column direction, zipped positionally with the columns.',
          '',
          'The per-column list is real behavior in the code even though it is missing from the',
          'framework README. Python `zip()` truncates silently: a list **shorter** than the columns',
          'drops the extra sort keys entirely, and a longer list ignores the extras — so keep the',
          'two lists the same length.',
        ].join('\n'),
        validate: (value, params) => {
          if (value === undefined || typeof value === 'boolean') return null
          if (!Array.isArray(value)) return 'Use true, false, or a list of booleans.'
          if (value.some((item) => typeof item !== 'boolean'))
            return 'Every direction must be true or false.'
          const columns = asStringList(params.columns).length
          if (columns > 0 && value.length !== columns)
            return `Give one direction per column: ${columns} column(s) but ${value.length} direction(s) — Spark zips the lists and silently drops the extras.`
          return null
        },
      },
    ],
    keywords: ['order by', 'orderBy', 'asc', 'desc', 'ranking', 'deterministic'],
    gotchas: [
      'Use plain column names — sort resolves them with F.col(), so expressions fail.',
      'Match the ascending list to the column list: zip() silently drops unmatched sort keys.',
      'Sorting is a full shuffle — do it once, as late as possible.',
      'Sort before drop_duplicates when a specific row must survive.',
      'NULLs sort first ascending and last descending; add a computed flag when you need otherwise.',
    ],
    examples: [
      {
        title: 'Per-column direction',
        json: `{
  "type": "sort",
  "columns": ["id_cessao", "data_evento"],
  "ascending": [true, false]
}`,
      },
      {
        title: 'All columns descending',
        json: `{
  "type": "sort",
  "columns": ["valor_total"],
  "ascending": false
}`,
      },
    ],
  },

  {
    type: 'fill_na',
    label: 'Fill nulls',
    family: 'compute',
    accent: 'transform',
    icon: 'Droplet',
    summary: 'Replace NULLs with a constant, or per column with a map.',
    description: [
      'Forwards to `df.fillna(value, subset=columns)`. The value can be a **scalar** — a number,',
      'string or boolean applied to every compatible column — or a **map of column → value**, which',
      'sets a different default per column. Spark only fills columns whose type matches the value,',
      'silently skipping the rest: a string fill never touches a numeric column. Use it for',
      'cleansing before a write or before arithmetic; use a `not_null` validation instead when the',
      'goal is to report missing data rather than hide it.',
    ].join(' '),
    fields: [
      {
        key: 'value',
        label: 'Fill value',
        type: 'json',
        required: true,
        rows: 4,
        placeholder: '0',
        help: 'A scalar (0, "N/A", false) for every column, or a map {"col": value} per column.',
        docs: [
          'Both forms are valid JSON in the same key:',
          '',
          '```json',
          '"value": 0',
          '"value": "N/A"',
          '"value": { "quantidade": 0, "nome_sacado": "N/A" }',
          '```',
          '',
          'Spark **ignores the column subset when the value is a map** — the map already names its',
          'own targets, so the two are mutually exclusive modes.',
          '',
          'Type matching is silent: a fill whose type does not match the column type does nothing,',
          'no warning. NULL is not a valid fill value.',
        ].join('\n'),
        validate: (value) => {
          if (value === undefined || value === null || value === '')
            return 'Set a scalar value or a column → value map.'
          if (Array.isArray(value)) return 'A list is not a fill value — use a scalar or a map.'
          if (isRecord(value)) {
            if (Object.keys(value).length === 0) return 'Add at least one column → value pair.'
            const invalid = Object.entries(value).find(
              ([, entry]) => entry === null || isRecord(entry) || Array.isArray(entry),
            )
            if (invalid) return `"${invalid[0]}" must map to a string, number or boolean.`
          }
          return null
        },
      },
      {
        key: 'columns',
        label: 'Restrict to columns (optional)',
        type: 'string-list',
        placeholder: 'quantidade',
        help: 'Subset for the scalar form. Omit to fill every column whose type matches.',
        visibleWhen: (params) => !isRecord(params.value),
      },
    ],
    keywords: ['null', 'na', 'default', 'coalesce', 'missing', 'cleansing', 'fillna'],
    gotchas: [
      'Pick one mode: with a map value, the column subset is ignored by Spark.',
      'Fills are type-matched silently — a string value never touches numeric columns.',
      'Prefer a not_null validation when missing data must be reported, not hidden.',
      'To fill with a computed value instead of a constant, use with_column + coalesce().',
    ],
    examples: [
      {
        title: 'Scalar fill on a subset',
        json: `{
  "type": "fill_na",
  "value": 0,
  "columns": ["quantidade", "valor_desconto"]
}`,
      },
      {
        title: 'Per-column defaults',
        json: `{
  "type": "fill_na",
  "value": {
    "quantidade": 0,
    "nome_sacado": "N/A"
  }
}`,
      },
    ],
  },

  {
    type: 'sql',
    label: 'SQL',
    family: 'compute',
    accent: 'transform',
    icon: 'Terminal',
    summary: 'Register the DataFrame as a temp view and run a full SQL query.',
    description: [
      'Registers the current DataFrame with `createOrReplaceTempView(view_name)` and replaces it',
      'with the result of `spark.sql(query)`. It is the escape hatch for anything the other nodes',
      'cannot express: window functions, `UNION ALL`, `CASE` cascades, self-joins, correlated',
      'subqueries. **The query must read from the view name** (default `_df`) to see the pipeline',
      'data — otherwise it quietly resolves against the catalog and the upstream chain is thrown',
      'away. The query runs on the whole session, so it can also join catalog tables and other',
      'temp views, and the resulting schema is entirely up to you.',
    ].join(' '),
    fields: [
      {
        key: 'query',
        label: 'Query',
        type: 'sql',
        required: true,
        rows: 6,
        placeholder: 'SELECT id, MIN(valor) AS valor FROM _df GROUP BY id',
        help: 'Run with spark.sql(). Read from the view name below to see the pipeline data.',
        docs: [
          'The result of this query becomes the new DataFrame — every column not selected is gone,',
          'including `ingestion_ts`.',
          '',
          'Runtime placeholders `{{var}}` are resolved before execution, which makes this a natural',
          'place for a pushdown list: `WHERE id_cessao IN ({{cessoes_pendentes}})`.',
        ].join('\n'),
        supportsRuntimeVars: true,
        validate: (value) =>
          typeof value === 'string' && value.trim() ? null : 'Write the SQL query to run.',
      },
      {
        key: 'view_name',
        label: 'Temp view name',
        type: 'text',
        default: '_df',
        group: 'advanced',
        placeholder: '_df',
        help: 'Name the current DataFrame is registered under before the query runs.',
        docs: [
          '`createOrReplaceTempView` **overwrites** any existing view with this name in the session,',
          'including views produced by the `view` output format — collisions are silent. Keep the',
          'default `_df` unless the query needs a meaningful alias.',
        ].join('\n'),
        validate: (value) => {
          if (value === undefined || value === '') return null
          if (typeof value !== 'string') return 'Use a plain identifier.'
          return identifierPattern.test(value)
            ? null
            : 'Use letters, digits and underscores, starting with a letter or underscore.'
        },
      },
    ],
    keywords: [
      'query',
      'spark.sql',
      'view',
      'window',
      'union',
      'subquery',
      'group by',
      'escape hatch',
    ],
    gotchas: [
      'Reference the view name in the FROM clause, or the query silently reads the catalog instead of your data.',
      'The query result replaces the DataFrame — unselected columns, ingestion_ts included, are gone.',
      'Renaming the view overwrites any session view with that name, including `view` outputs.',
      'Prefer the dedicated nodes when they fit: they stay readable and lintable in the canvas.',
      'Runtime {{vars}} are resolved here too — an undefined one is left literal and breaks the SQL.',
    ],
    examples: [
      {
        title: 'Aggregate through the default view',
        json: `{
  "type": "sql",
  "view_name": "_df",
  "query": "SELECT id_cessao, MIN(data_evento) AS primeiro_evento FROM _df GROUP BY id_cessao"
}`,
      },
      {
        title: 'Window function with a named view',
        json: `{
  "type": "sql",
  "view_name": "contratos",
  "query": "SELECT *, row_number() OVER (PARTITION BY id_cessao ORDER BY data_evento DESC) AS rn FROM contratos"
}`,
      },
      {
        title: 'Pushdown of a runtime variable',
        json: `{
  "type": "sql",
  "query": "SELECT * FROM _df WHERE id_cessao IN ({{cessoes_pendentes}})"
}`,
      },
    ],
  },
]
