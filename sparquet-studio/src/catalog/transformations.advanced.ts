/**
 * Advanced catalog entries: the nodes that are containers, control flow or
 * inspection rather than plain row/column work.
 *
 * `join` and `union` pull a second DataFrame from the canvas, `group_by` reshapes
 * the schema wholesale, `checkpoint` / `stop_if_empty` / `collect` steer execution,
 * `debug` observes without touching the data, and `$include` is not a runtime
 * transformation at all — it is a pre-parse directive.
 */

import type { FieldOption, FieldSpec, TransformationDef } from '@/catalog/types'

const lines = (...parts: string[]): string => parts.join('\n')

/**
 * The engine normalizes every debug action before matching it, so imported JSON
 * may legally say `printSchema` or `print-schema` (transform/builtin.py:490).
 */
const normalizeDebugAction = (raw: string): string =>
  raw.toLowerCase().replace(/-/g, '_').replace('printschema', 'print_schema')

const DEBUG_DEFAULT_ACTIONS = ['show', 'print_schema']

const debugHasAction = (params: Record<string, unknown>, action: string): boolean => {
  const raw = params.actions
  if (raw === undefined) return DEBUG_DEFAULT_ACTIONS.includes(action)
  if (!Array.isArray(raw)) return false
  return raw.some((item) => typeof item === 'string' && normalizeDebugAction(item) === action)
}

/**
 * `_VALID_JOIN_TYPES` (transform/builtin.py:378-385) — a closed whitelist, checked
 * after `.lower()`. Anything outside it raises at apply time, so the picker has to
 * carry the alias spellings too or imported configs would fail to round-trip.
 */
const JOIN_TYPE_OPTIONS: FieldOption[] = [
  {
    value: 'inner',
    label: 'inner',
    hint: 'Only rows that match on both sides.',
  },
  {
    value: 'left',
    label: 'left',
    hint: 'All left rows; right columns null when unmatched.',
  },
  {
    value: 'right',
    label: 'right',
    hint: 'All right rows; left columns null when unmatched.',
  },
  { value: 'full', label: 'full', hint: 'Every row from both sides.' },
  {
    value: 'cross',
    label: 'cross',
    hint: 'Cartesian product — ignores `on`, explodes row count.',
  },
  {
    value: 'leftsemi',
    label: 'leftsemi',
    hint: 'Filter: keeps left rows that match. Adds NO right columns.',
  },
  {
    value: 'leftanti',
    label: 'leftanti',
    hint: 'Filter: keeps left rows with no match. Adds NO right columns.',
  },
  { value: 'outer', label: 'outer', hint: 'Alias of full.' },
  { value: 'fullouter', label: 'fullouter', hint: 'Alias of full.' },
  { value: 'full_outer', label: 'full_outer', hint: 'Alias of full.' },
  { value: 'leftouter', label: 'leftouter', hint: 'Alias of left.' },
  { value: 'left_outer', label: 'left_outer', hint: 'Alias of left.' },
  { value: 'rightouter', label: 'rightouter', hint: 'Alias of right.' },
  { value: 'right_outer', label: 'right_outer', hint: 'Alias of right.' },
  { value: 'semi', label: 'semi', hint: 'Alias of leftsemi.' },
  { value: 'left_semi', label: 'left_semi', hint: 'Alias of leftsemi.' },
  { value: 'anti', label: 'anti', hint: 'Alias of leftanti.' },
  { value: 'left_anti', label: 'left_anti', hint: 'Alias of leftanti.' },
]

const joinOnField: FieldSpec = {
  key: 'on',
  label: 'Join keys',
  type: 'text',
  required: true,
  placeholder: 'id_cessao',
  help: 'A column name, a list of names, or a SQL expression — expressions must contain spaces.',
  docs: lines(
    'Three accepted shapes, and the framework tells them apart by looking for a space:',
    '',
    '| What you write | What Spark receives |',
    '| --- | --- |',
    '| `id_cessao` | a column name present on both sides |',
    '| `["id_cessao", "numero_contrato"]` | a list of shared column names |',
    '| `l.id_cessao = r.id AND l.dt = r.dt` | `F.expr(...)`, using the `l` / `r` aliases |',
    '',
    '**The space is the whole heuristic.** A string with no space is handed to Spark as a column *name*, so a compact expression like `l.id=r.id` — or even `upper(id)` — fails at analysis time with a confusing "column not found". Always write spaces around the operators.',
    '',
    '**The two forms are not equivalent downstream.** The list form produces Spark deduplicated join columns (one `id_cessao` in the result). The expression form keeps both sides, so later nodes that reference the bare name hit an ambiguity error — qualify them or drop one side.',
    '',
    '`l.` and `r.` are not available inside the right-side chain: it runs before the aliases are applied, so use bare column names there. Here in `on` — and in the nodes that follow the join, until a projection drops the qualifiers — they resolve normally.',
  ),
  validate: (value) => {
    if (Array.isArray(value)) {
      if (value.length === 0) return 'Add at least one join key.'
      return value.every((item) => typeof item === 'string' && item.trim() !== '')
        ? null
        : 'Every join key must be a non-empty column name.'
    }
    if (typeof value !== 'string' || value.trim() === '') {
      return 'A join key or SQL expression is required.'
    }
    const text = value.trim()
    const looksLikeExpression = /[=<>()]|\bAND\b|\bOR\b/i.test(text)
    if (looksLikeExpression && !text.includes(' ')) {
      return 'Spark only parses `on` as SQL when it contains a space — write "l.id = r.id".'
    }
    if (!looksLikeExpression && text.includes(' ')) {
      return 'A bare column name cannot contain spaces. Use the list form, or a full SQL expression.'
    }
    return null
  },
}

const joinHowField: FieldSpec = {
  key: 'how',
  label: 'Join type',
  type: 'select',
  default: 'inner',
  options: JOIN_TYPE_OPTIONS,
  help: 'Validated against a closed whitelist — an unlisted value fails the run.',
  docs: lines(
    'Lowercased and checked against 18 accepted spellings; anything else raises `ValueError` at apply time.',
    '',
    '`leftsemi` and `leftanti` are the two that surprise people: they are **filters**, not enrichments. The result has exactly the left schema, no right column is added, and no row is duplicated even when the right side matches many times. That makes them the idiomatic way to express "only cessions with an open status" (`leftsemi`) and "only cessions not registered yet" (`leftanti`).',
    '',
    'Every other type can multiply rows when the right side is not unique on the join key — close the right side chain with `distinct` or a `group_by` when in doubt.',
  ),
}

const joinBroadcastField: FieldSpec = {
  key: 'broadcast',
  label: 'Broadcast',
  type: 'select',
  options: [
    { value: '', label: 'auto (Spark decides)' },
    { value: 'true', label: 'right — broadcast the second source' },
    { value: 'left', label: 'left — broadcast the main side' },
  ],
  help: 'Map-side join: broadcast the small side to every executor and skip its shuffle. "right" broadcasts the joined source (a small dimension/lookup); "left" broadcasts the main chain.',
  docs: lines(
    'Adds a broadcast hint via F.broadcast(). Use it when the broadcast side fits comfortably in each executor (roughly a few hundred MB or less) — broadcasting a large side spills or OOMs.',
    '',
    'Left empty / "auto" leaves the choice to Spark (spark.sql.autoBroadcastJoinThreshold).',
  ),
}

const joinDef: TransformationDef = {
  type: 'join',
  label: 'Join',
  family: 'combine',
  accent: 'combine',
  icon: 'GitMerge',
  summary: 'Combines the chain with a second source, which can carry its own sub-pipeline.',
  description: lines(
    'Joins the main DataFrame with a second source read inline. The left side is aliased `l` and the right side `r`, but only *after* the right-side chain has run.',
    '',
    'Columns present on both sides are renamed on the right with a `_r` suffix, so a self join never produces two columns with the same name.',
    '',
    '**The right side is not a form field.** `input` (the source `{ format, path, options }`) is compiled from the node connected to the second input handle, and `with_transformations` from whatever chain feeds that handle. That chain accepts the full builtin vocabulary — `filter`, `select`, `distinct`, `group_by`, even another `join` — and it runs *before* the join, which is how you narrow a wide table instead of joining it whole.',
    '',
    'The canonical right-side chain in production configs is three nodes: push a `filter` down (usually `id IN ({{runtime_var}})`), `select` only the columns you need, then `distinct` so the join cannot fan out rows.',
    '',
    'The two fields below are the join itself: which keys to match on, and how to combine the rows.',
  ),
  fields: [joinOnField, joinHowField, joinBroadcastField],
  keywords: [
    'join',
    'merge',
    'lookup',
    'enrich',
    'leftsemi',
    'leftanti',
    'semi',
    'anti',
    'inner',
    'outer',
    'combine',
    'with_transformations',
    'second source',
  ],
  gotchas: [
    'The right side comes from the second input handle, not from a field. Disconnecting it leaves the node without `input`, which fails the run with a ValueError naming the missing key.',
    'A string `on` is only treated as SQL when it contains a space. `l.id=r.id` is read as a column name and fails at analysis time.',
    'The list form of `on` merges the join columns into one; the expression form does not, so the key itself is one of the names the join has to disambiguate.',
    'A name present on both sides is renamed on the RIGHT with a `_r` suffix (`nome` and `nome_r`; `_r2`, `_r3` if taken), so the result never carries two columns with the same name. The projection is built after the join, so an `on` written as SQL can still say `r.nome`. Rename inside the right-side chain when `_r` is not the name you want.',
    'The `l` / `r` aliases are applied only after the right-side chain has run, so bare column names are the only option inside that chain. They resolve in `on`, but NOT after a join that renamed a duplicate: that join ends in a projection, and downstream nodes see plain `nome` / `nome_r`.',
    '`leftsemi` / `leftanti` add no columns at all. Any downstream node expecting right-side columns after one of them will fail.',
    'The right-side chain runs on a nested engine that only knows the builtin types — transformations registered at runtime through `fw.register_transformation` are unavailable there.',
    'The nested chain shares the runtime-variable store with the outer chain: `{{vars}}` collected outside resolve inside, and a `collect` placed inside writes back to the outer scope.',
    '`skip_if_false` on a join prunes the whole subgraph — the nested chain and the source read included.',
    '`how` is validated against a closed whitelist. A typo is a hard failure, not a fallback to inner.',
  ],
  examples: [
    {
      title: 'leftsemi as a filter — keep only cessions with an open status',
      json: lines(
        '{',
        '  "type": "join",',
        '  "input": { "format": "delta", "path": "lastros.silver_cessoes_status" },',
        '  "with_transformations": [',
        '    { "type": "filter", "condition": "status IN (1, 10, 11)" },',
        '    { "type": "select", "columns": ["id_cessao"] },',
        '    { "type": "distinct" }',
        '  ],',
        '  "on": "id_cessao",',
        '  "how": "leftsemi"',
        '}',
      ),
    },
    {
      title: 'Left join with runtime predicate pushdown on a composite key',
      json: lines(
        '{',
        '  "type": "join",',
        '  "input": { "format": "delta", "path": "lastros.bronze_remessa" },',
        '  "with_transformations": [',
        '    { "type": "filter", "condition": "id_cessao IN ({{cessoes_pendentes}})" },',
        '    { "type": "select", "columns": ["id_cessao", "numero_contrato", "tipo_contrato"] }',
        '  ],',
        '  "on": ["id_cessao", "numero_contrato"],',
        '  "how": "left"',
        '}',
      ),
    },
    {
      title: 'Conditional leftanti — "not registered yet", only when the param asks for it',
      json: lines(
        '{',
        '  "type": "join",',
        '  "skip_if_false": "{processar_somente_cessoes_pendentes}",',
        '  "input": { "format": "delta", "path": "lastros.silver_registro_contratos" },',
        '  "with_transformations": [',
        '    { "type": "select", "columns": ["id_cessao"] },',
        '    { "type": "distinct" }',
        '  ],',
        '  "on": "id_cessao",',
        '  "how": "leftanti"',
        '}',
      ),
    },
  ],
  secondaryInput: true,
  supportsSubPipeline: true,
}

const unionDef: TransformationDef = {
  type: 'union',
  label: 'Union',
  family: 'combine',
  accent: 'combine',
  icon: 'Combine',
  summary: 'Appends the rows of a second source to the chain.',
  description: lines(
    'Stacks a second source under the current DataFrame. The source `{ format, path, options }` comes from the node connected to the second input handle, not from a field.',
    '',
    '**Unlike `join`, there is no right-side chain.** The second source is read and appended exactly as it lands — you cannot filter, select or rename it inline. When the shapes do not already agree, write the reshaping into a separate pipeline (or a `view`) and union that.',
    '',
    'The single option below decides *how* columns are matched, and it is the one thing worth getting right before running this node.',
  ),
  fields: [
    {
      key: 'allow_missing_columns',
      label: 'Match columns by name',
      type: 'boolean',
      default: false,
      help: 'Off = positional matching (df.union). On = by name, filling absentees with null.',
      docs: lines(
        '**Off (the default) is positional.** `df.union(other)` pairs column 1 with column 1, column 2 with column 2 and so on. Names are ignored entirely: if the two schemas have the same arity but a different order, the run succeeds and writes silently wrong data. There is no error to catch.',
        '',
        '**On switches to `unionByName(allowMissingColumns=True)`** — columns are paired by name and anything missing on either side is filled with `null`.',
        '',
        'One more trap specific to this framework: the main DataFrame carries an `ingestion_ts` column that the pipeline appends right after reading the input. The unioned source, read through this node, does **not** have it. A positional union of "the same table" therefore misaligns by one column. Turning this option on is the usual fix.',
      ),
    },
  ],
  keywords: ['union', 'append', 'concat', 'stack', 'unionByName', 'combine', 'rows'],
  gotchas: [
    'The default (false) matches columns by POSITION, not by name. A schema in a different order produces silently wrong data with no error.',
    'The main DataFrame carries the auto-added `ingestion_ts` column while the unioned source does not — a positional union between them is misaligned by one column.',
    'There is no `with_transformations` on union: the second source cannot be reshaped inline the way a join right side can.',
    'The second source comes from the second input handle; without it the node fails with a ValueError naming the missing `input`.',
    'With the option on, missing columns are filled with null rather than rejected — a typo in a column name shows up as an all-null column, not as an error.',
  ],
  examples: [
    {
      title: 'Append a history table by name (safe with differing schemas)',
      json: lines(
        '{',
        '  "type": "union",',
        '  "input": { "format": "delta", "path": "lastros.silver_cessao_historico" },',
        '  "allow_missing_columns": true',
        '}',
      ),
    },
    {
      title: 'Positional union of an incremental drop with an identical schema',
      json: lines(
        '{',
        '  "type": "union",',
        '  "input": { "format": "parquet", "path": "/data/incremental/pedidos" },',
        '  "allow_missing_columns": false',
        '}',
      ),
    },
  ],
  secondaryInput: true,
  supportsSubPipeline: false,
}

const groupByDef: TransformationDef = {
  type: 'group_by',
  label: 'Group by',
  family: 'aggregate',
  accent: 'transform',
  icon: 'Sigma',
  summary: 'Aggregates rows with full SQL expressions, each carrying its own alias.',
  description: lines(
    'Groups by the listed columns and reduces each group with SQL aggregate expressions. The output schema is exactly `by` + the aliases in `agg` (+ pivot columns) — **everything else is dropped**, so anything you still need after this node has to be carried through an aggregate such as `first(col) as col`.',
    '',
    '`agg` is a list of complete SQL expressions, each with its alias written inside the string. There is no separate alias key and no `{function, column}` form:',
    '',
    '```json',
    '"agg": ["sum(valor) as total", "count(*) as n"]',
    '```',
    '',
    'That looseness is the point — an entry can be any expression, not just one function call:',
    '',
    '- `sum(case when dt_vencimento <= dia then valor else 0 end) - sum(...) as saldo` — arithmetic across two conditional sums',
    '- `count(distinct struct(tipo_ativo, registradora)) > 1 as multi_ativos` — a boolean flag derived from an aggregate',
    '- `max_by(dt_compra, vl_dia) as dt_maior_fatura` — argmax, i.e. "the date of the largest invoice"',
    '- `first(tipo_contrato) as tipo_contrato` — collapse a group to one row while carrying a column along',
    '',
    '**The argmax idiom needs two nodes.** Aggregate to the finer grain first (`by: [entity, day]`), then group that result again at the coarser grain using `max_by` over the alias the first node produced. Keep the pair adjacent; reordering them changes the answer.',
    '',
    'The alias you write here is the only handle downstream nodes have on the column. Renaming it silently breaks every later `filter` / `select` string that mentions the old name — those are opaque SQL strings, so nothing links them structurally.',
  ),
  fields: [
    {
      key: 'by',
      label: 'Group by columns',
      type: 'string-list',
      required: true,
      placeholder: 'id_cessao',
      help: 'Plain column names. These survive into the result; every unlisted column is dropped.',
      validate: (value) => {
        if (!Array.isArray(value) || value.length === 0)
          return 'Add at least one grouping column.'
        return value.every((item) => typeof item === 'string' && item.trim() !== '')
          ? null
          : 'Every grouping column must be a non-empty name.'
      },
    },
    {
      key: 'agg',
      label: 'Aggregate expressions',
      type: 'sql-list',
      required: true,
      placeholder: 'sum(valor) as total',
      rows: 2,
      help: 'One complete SQL expression per row, each ending in `as <alias>`.',
      docs: lines(
        'Each row is passed straight to `F.expr()`, so any Spark SQL is accepted: `case when`, arithmetic between aggregates, `count(distinct struct(...))`, `max_by`, comparisons producing booleans.',
        '',
        'Write the alias inside the expression. Without one, Spark names the column after the expression itself (`count(1)`, `sum(valor)`), which downstream nodes can barely reference.',
      ),
      validate: (value) => {
        if (!Array.isArray(value) || value.length === 0) {
          return 'Add at least one aggregate expression.'
        }
        if (value.some((item) => typeof item !== 'string' || item.trim() === '')) {
          return 'Every entry must be a non-empty SQL expression.'
        }
        const unaliased = value.find(
          (item) => typeof item === 'string' && !/\sas\s/i.test(item),
        )
        if (typeof unaliased === 'string') {
          return `"${unaliased}" has no alias — write it inside the expression, e.g. "sum(valor) as total".`
        }
        return null
      },
    },
    {
      key: 'pivot',
      label: 'Pivot',
      type: 'json',
      group: 'advanced',
      placeholder: '{ "column": "mes", "values": ["jan", "fev"] }',
      help: 'A column name, or { column, values } to skip the extra scan.',
      docs: lines(
        'Two shapes are accepted:',
        '',
        '- `"mes"` — Spark scans the data once to discover the distinct values, then pivots.',
        '- `{ "column": "mes", "values": ["jan", "fev"] }` — you declare the values, so the extra scan is skipped and the output schema is deterministic.',
        '',
        'An **empty** `values` array is falsy in the framework check, so it silently degrades to the scanning form instead of producing zero pivot columns. Remove the key rather than leaving it empty.',
      ),
      validate: (value) => {
        if (value === undefined || value === null || value === '') return null
        if (typeof value === 'string') {
          return value.trim() === '' ? 'Enter a column name, or remove the pivot.' : null
        }
        if (typeof value !== 'object' || Array.isArray(value)) {
          return 'Pivot must be a column name, or an object { "column": "...", "values": [...] }.'
        }
        const record = value as Record<string, unknown>
        if (typeof record.column !== 'string' || record.column.trim() === '') {
          return 'The object form requires a non-empty "column".'
        }
        if ('values' in record) {
          if (!Array.isArray(record.values)) return '"values" must be a list of pivot values.'
          if (record.values.length === 0) {
            return 'An empty "values" list is ignored — remove the key to pivot with a scan instead.'
          }
        }
        return null
      },
    },
  ],
  keywords: [
    'group',
    'group_by',
    'aggregate',
    'agg',
    'sum',
    'count',
    'max_by',
    'first',
    'pivot',
    'rollup',
    'summarize',
  ],
  gotchas: [
    'The output schema is exactly `by` + the `agg` aliases (+ pivot columns). Every other column is dropped — carry the ones you need with `first(col) as col`.',
    '`agg` entries are opaque SQL strings and the alias must be inside the string. There is no separate alias key, and a map form is not supported.',
    'Entries are not validated against a function whitelist: a non-aggregate expression over an aggregate (`count(distinct struct(a, b)) > 1 as flag`) is legal and common.',
    'Renaming an alias breaks downstream `filter` / `select` strings that reference it, and nothing detects the break statically.',
    'The argmax idiom is two consecutive group_by nodes — the second consumes the first aliases. Reordering them changes the result.',
    'An empty `pivot.values` array silently falls back to the scanning pivot rather than producing no columns.',
    'A pivot without declared values costs an extra Spark scan over the data.',
  ],
  examples: [
    {
      title: 'Daily balance from two conditional sums',
      json: lines(
        '{',
        '  "type": "group_by",',
        '  "by": ["cnpj_entidade", "cnpj_fonte", "dia"],',
        '  "agg": [',
        '    "sum(case when dt_vencimento <= dia then valor_duplicata else 0 end) - sum(case when dt_pagamento <= dia then valor_duplicata else 0 end) as saldo"',
        '  ]',
        '}',
      ),
    },
    {
      title: 'Argmax pass — regroup the previous aggregate at a coarser grain',
      json: lines(
        '{',
        '  "type": "group_by",',
        '  "by": ["cnpj_entidade", "cnpj_fonte"],',
        '  "agg": [',
        '    "max(dt_compra) as dt_ultima_compra",',
        '    "max_by(vl_dia, dt_compra) as vl_ultima_compra",',
        '    "max_by(dt_compra, vl_dia) as dt_maior_fatura",',
        '    "max(vl_dia) as vl_maior_fatura"',
        '  ]',
        '}',
      ),
    },
    {
      title: 'Collapse to one row per contract and flag multi-asset cessions',
      json: lines(
        '{',
        '  "type": "group_by",',
        '  "by": ["id_cessao", "numero_contrato"],',
        '  "agg": [',
        '    "first(tipo_contrato) as tipo_contrato",',
        '    "count(distinct struct(tipo_ativo, registradora)) > 1 as multi_ativos"',
        '  ]',
        '}',
      ),
    },
    {
      title: 'Pivot with declared values (no discovery scan)',
      json: lines(
        '{',
        '  "type": "group_by",',
        '  "by": ["cnpj_entidade"],',
        '  "agg": ["sum(valor_duplicata) as total"],',
        '  "pivot": { "column": "mes", "values": ["jan", "fev", "mar"] }',
        '}',
      ),
    },
  ],
}

const checkpointDef: TransformationDef = {
  type: 'checkpoint',
  label: 'Checkpoint',
  family: 'control',
  accent: 'control',
  icon: 'Flag',
  summary: 'Materializes the DataFrame and truncates its logical plan.',
  description: lines(
    'A barrier, not a transformation: the data is unchanged, but Spark writes it out and forgets how it was computed. Everything after this node plans against a fresh, one-step lineage.',
    '',
    'Three placements earn their cost:',
    '',
    '1. **Before a `collect`** — the driver-side action then reads an already materialized DataFrame instead of recomputing the chain.',
    '2. **After a heavy chain of joins** — long lineages make the optimizer slow and can blow the plan up on re-evaluation.',
    '3. **As the last transformation before a fan-out to several outputs** — otherwise every destination recomputes the whole chain from scratch.',
    '',
    'In practice this node is almost always written bare; the two settings below rarely change.',
  ),
  fields: [
    {
      key: 'method',
      label: 'Method',
      type: 'select',
      default: 'localCheckpoint',
      options: [
        {
          value: 'localCheckpoint',
          label: 'localCheckpoint',
          hint: 'Executor-local disk. No setup, lost if an executor dies.',
        },
        {
          value: 'checkpoint',
          label: 'checkpoint',
          hint: 'Reliable storage. Requires sparkContext.setCheckpointDir.',
        },
      ],
      help: 'An invalid value does not fail — the node is silently skipped.',
      docs: lines(
        '`localCheckpoint` writes to executor-local disk: cheap, no configuration, but lost if the executor is lost.',
        '',
        '`checkpoint` writes to reliable storage and **requires `spark.sparkContext.setCheckpointDir(...)` to have been called** — the framework does not set it for you.',
        '',
        'The comparison is case-insensitive, so `LOCALCHECKPOINT` from an imported config still works. Studio emits the canonical casing.',
      ),
    },
    {
      key: 'eager',
      label: 'Materialize immediately',
      type: 'boolean',
      default: true,
      group: 'advanced',
      help: 'On (the default) runs a Spark action here instead of deferring it.',
    },
  ],
  keywords: [
    'checkpoint',
    'materialize',
    'cache',
    'persist',
    'lineage',
    'barrier',
    'localCheckpoint',
    'truncate plan',
  ],
  gotchas: [
    'An invalid `method` is NOT an error: the node is skipped, the DataFrame passes through untouched, and a warning is deferred to the very end of the run. The pipeline still reports success, so this is invisible unless you read the log tail.',
    'Deferred warnings never reach PipelineResult — Studio cannot surface them from the result object, only from the log stream.',
    '`checkpoint` (reliable) fails unless `sparkContext.setCheckpointDir` was set beforehand; `localCheckpoint` needs no setup.',
    'With `eager` on, this fires a Spark action right here — its position in the chain is a real cost decision, not a formality.',
    'It changes cost, never results: removing it cannot change the data, only how many times the lineage is recomputed.',
    'The same pipeline can legitimately contain several checkpoints — it is a phase marker, not a unique terminal node.',
  ],
  examples: [
    {
      title: 'Bare form, right before a collect (the usual shape)',
      json: lines('{ "type": "checkpoint" }'),
    },
    {
      title: 'Reliable checkpoint before a multi-output fan-out',
      json: lines(
        '{',
        '  "type": "checkpoint",',
        '  "method": "checkpoint",',
        '  "eager": true',
        '}',
      ),
    },
  ],
}

const stopIfEmptyDef: TransformationDef = {
  type: 'stop_if_empty',
  label: 'Stop if empty',
  family: 'control',
  accent: 'control',
  icon: 'OctagonX',
  summary: 'Ends the run gracefully when there is nothing left to process.',
  description: lines(
    'A guard, not a transformation. If the DataFrame is empty at this point the pipeline stops **successfully**: the remaining transformations do not run, validations do not run, and no output is written.',
    '',
    'The result comes back as `success = True`, `skipped = True`, `rows_written = 0` — which is what lets an orchestrator branch on `result.skipped` and skip the downstream stage entirely instead of treating an empty day as a failure.',
    '',
    '**Placement is the whole point.** Put it immediately after the filter that defines the working set and before the expensive work — checkpoints, joins, payload assembly. Placed later it still stops the run, but only after you have paid for everything above it.',
    '',
    'It costs one Spark action (`isEmpty`) wherever it sits.',
  ),
  fields: [
    {
      key: 'message',
      label: 'Stop reason',
      type: 'text',
      placeholder: 'DataFrame vazio — nada a processar',
      help: 'Logged when the run stops. Omit to use the framework default.',
      docs: 'Name the working set that came up empty ("no cessions for NOTA_COMERCIAL/B3"), not the fact that it was empty — the log line already says that, and this string is often the only clue about which of several parameterized runs stopped.',
    },
  ],
  keywords: [
    'stop',
    'stop_if_empty',
    'halt',
    'guard',
    'empty',
    'short circuit',
    'skip',
    'early exit',
    'terminator',
  ],
  gotchas: [
    'This is control flow: it raises PipelineStop. Everything downstream — remaining transformations, validations and every output — is skipped when it fires.',
    'The run is reported as successful (`success = True`, `skipped = True`), not as an error. Dashboards that only watch `success` will not notice that nothing was written.',
    'Placed inside an output chain, a join right side, or a debug preview, it still aborts the WHOLE pipeline. Worse, the result claims `skipped = True` even though earlier outputs may already have been written — a misleading state. Keep it in the main chain.',
    'It calls `df.isEmpty()`, a real Spark action, wherever it sits.',
    'Any node after it is unreachable when the DataFrame is empty — the path downstream is conditional, not guaranteed.',
  ],
  examples: [
    {
      title: 'After the flow filter, before the expensive payload work',
      json: lines(
        '{',
        '  "type": "stop_if_empty",',
        '  "message": "Sem cessoes para NOTA_COMERCIAL/B3 - registro nao iniciado"',
        '}',
      ),
    },
    {
      title: 'Bare form — the framework supplies the default reason',
      json: lines('{ "type": "stop_if_empty" }'),
    },
  ],
  canHalt: true,
}

const collectDef: TransformationDef = {
  type: 'collect',
  label: 'Collect to variable',
  family: 'control',
  accent: 'control',
  icon: 'Variable',
  summary: 'Pulls a column distinct values into a `{{variable}}` for later pushdown.',
  description: lines(
    'Runs `select(column).distinct().collect()` and stores the values under a runtime variable name. The DataFrame is returned untouched — this node exists purely for its side effect.',
    '',
    'The point is predicate pushdown. Once the working set is known, its keys can be injected as a literal list into the reads that come later, so Spark skips files instead of reading a large table in full:',
    '',
    '```json',
    '{ "type": "filter", "condition": "id_cessao IN ({{cessoes_pendentes}})" }',
    '```',
    '',
    'The variable is visible to every later node, including join right-side chains and per-output chains — an invisible edge between nodes that are not connected on the canvas. The store is cleared at the start of every run, so variables never leak between pipelines.',
    '',
    '**Order is load-bearing:** `checkpoint` → `collect` → consumer. A consumer that runs before the collect does not fail; the `{{name}}` is simply left as literal text and reaches Spark as broken SQL.',
  ),
  fields: [
    {
      key: 'column',
      label: 'Column',
      type: 'text',
      required: true,
      placeholder: 'id_cessao',
      help: 'Its distinct values are pulled to the driver. Keep the cardinality small.',
      validate: (value) =>
        typeof value === 'string' && value.trim() !== '' ? null : 'A column name is required.',
    },
    {
      key: 'as',
      label: 'Variable name',
      type: 'text',
      required: true,
      placeholder: 'cessoes_pendentes',
      help: 'Referenced downstream as {{name}}. Letters, digits and underscore only.',
      docs: lines(
        'The placeholder regex is `\\{\\{(\\w+)\\}\\}`, so a name containing a dot, a dash or spaces can never be referenced — the placeholder stays literal and no error is raised.',
        '',
        'A `{param}` of the same name no longer collides: the pre-parse template pass skips `{{name}}` entirely, so the two namespaces are independent. Older framework versions rewrote the runtime placeholder from `params` before the runtime layer saw it.',
      ),
      validate: (value) => {
        if (typeof value !== 'string' || value.trim() === '')
          return 'A variable name is required.'
        return /^\w+$/.test(value.trim())
          ? null
          : 'Only letters, digits and underscore — the {{name}} placeholder matches \\w+ and nothing else.'
      },
    },
  ],
  keywords: [
    'collect',
    'runtime variable',
    'pushdown',
    'predicate',
    'data skipping',
    'IN clause',
    'driver',
    'broadcast list',
  ],
  gotchas: [
    'This fires a driver-side action. Place it after a `checkpoint`, or the whole chain is recomputed to produce the list.',
    'Everything collected lands in driver memory — collecting a high-cardinality column is a straightforward way to kill the driver.',
    'An empty result renders as the literal `NULL`, so `IN ({{var}})` becomes `IN (NULL)` and matches nothing. That is usually correct, but it looks like a silent data loss.',
    'An unresolved `{{name}}` is left LITERAL — no error. A typo reaches Spark as invalid SQL, or resolves unexpectedly later inside a nested chain.',
    "Value formatting is decided by the FIRST element: a list of strings becomes `'a', 'b'`, a list of numbers becomes `1, 2`. A column with nulls, or mixed types, produces invalid SQL.",
    'Keep the variable name disjoint from `{param}` keys — the pre-parse template pass matches the inner braces of `{{name}}` and would destroy the placeholder.',
    'A collect placed inside a join right-side chain writes to the SAME shared store, so it is visible to later top-level nodes too.',
  ],
  examples: [
    {
      title: 'The full pushdown idiom: checkpoint, collect, then filter a later read',
      json: lines(
        '[',
        '  { "type": "checkpoint" },',
        '  { "type": "collect", "column": "id_cessao", "as": "cessoes_pendentes" },',
        '  {',
        '    "type": "join",',
        '    "input": { "format": "delta", "path": "lastros.bronze_remessa" },',
        '    "with_transformations": [',
        '      { "type": "filter", "condition": "id_cessao IN ({{cessoes_pendentes}})" },',
        '      { "type": "select", "columns": ["id_cessao", "numero_contrato"] }',
        '    ],',
        '    "on": ["id_cessao", "numero_contrato"],',
        '    "how": "left"',
        '  }',
        ']',
      ),
    },
    {
      title: 'Collected in the main chain, consumed inside an output branch',
      json: lines(
        '{',
        '  "type": "collect",',
        '  "column": "id_cessao",',
        '  "as": "cessoes_processadas"',
        '}',
      ),
    },
  ],
  emitsRuntimeVar: true,
}

const debugDef: TransformationDef = {
  type: 'debug',
  label: 'Debug',
  family: 'inspect',
  accent: 'inspect',
  icon: 'Bug',
  summary: 'Prints counts, schema or rows without touching the data.',
  description: lines(
    'An inspection probe. It runs the selected actions, prints them to stdout, and returns the **original** DataFrame — inserting or removing one can never change the pipeline result.',
    '',
    'It can also carry its own chain of transformations. Those apply to a throwaway copy used only for the printout: filter down to one troublesome key, select three columns, then look at them, all without affecting what flows downstream. The chain feeds the inspection view and is discarded afterwards.',
    '',
    'Output goes to stdout through plain `print()`, not through the structured JSON logger, so it will not show up in log aggregation — this is a node for interactive runs.',
  ),
  fields: [
    {
      key: 'label',
      label: 'Label',
      type: 'text',
      placeholder: 'after the contracts join',
      help: 'Shown in the printed separator. Worth setting when several debug nodes exist.',
    },
    {
      key: 'actions',
      label: 'Actions',
      type: 'multi-select',
      default: DEBUG_DEFAULT_ACTIONS,
      options: [
        {
          value: 'count',
          label: 'count',
          hint: 'Row count — a full Spark action.',
        },
        {
          value: 'print_schema',
          label: 'print_schema',
          hint: 'Column names and types. Cheap.',
        },
        { value: 'show', label: 'show', hint: 'Prints rows via df.show().' },
        { value: 'explain', label: 'explain', hint: 'Physical plan.' },
        {
          value: 'columns',
          label: 'columns',
          hint: 'Column names only. Cheap.',
        },
        { value: 'dtypes', label: 'dtypes', hint: 'Name/type pairs. Cheap.' },
      ],
      help: 'Run in the order listed. An unknown action prints a warning and is ignored.',
      docs: lines(
        '`count`, `show` and `explain` each trigger work on the inspected view — a debug node inside a hot path re-triggers computation every run.',
        '',
        '`show` deliberately uses `df.show()`. Databricks `display()` only renders when called directly from a notebook cell, so the framework never calls it.',
      ),
    },
    {
      key: 'show_rows',
      label: 'Rows to show',
      type: 'number',
      default: 20,
      visibleWhen: (params) => debugHasAction(params, 'show'),
    },
    {
      key: 'truncate',
      label: 'Truncate wide values',
      type: 'boolean',
      default: true,
      help: 'Off prints long payloads in full — useful for inspecting a JSON column.',
      visibleWhen: (params) => debugHasAction(params, 'show'),
    },
    {
      key: 'vertical',
      label: 'Vertical layout',
      type: 'boolean',
      default: false,
      help: 'One field per line — readable when the row is wider than the terminal.',
      visibleWhen: (params) => debugHasAction(params, 'show'),
    },
    {
      key: 'transformations',
      label: 'Preview chain',
      type: 'json',
      rows: 5,
      group: 'advanced',
      placeholder: '[{ "type": "filter", "condition": "id_cessao = \'C1\'" }]',
      help: 'Applied to a throwaway copy for this printout only. The key is `transformations` — `with_transformations` belongs to join and is ignored here.',
      docs: lines(
        'A JSON array of transformation objects, run on a copy of the DataFrame just before the actions print. The node still returns the original DataFrame, so nothing downstream can see this chain.',
        '',
        'Use it to narrow the printout: `filter` down to one troublesome key, `select` three columns, `group_by` to a count.',
        '',
        '`debug` reads this under the key `transformations`. The `with_transformations` key is read by `join` alone — spelled that way here it lands in `params`, is never read, and the printout silently covers the whole DataFrame.',
      ),
      validate: (value) => {
        if (value === undefined || value === null) return null
        if (!Array.isArray(value))
          return 'The preview chain must be a JSON array of transformations.'
        return value.every(
          (item) => typeof item === 'object' && item !== null && !Array.isArray(item),
        )
          ? null
          : 'Every entry must be a transformation object, e.g. { "type": "filter", "condition": "..." }.'
      },
    },
    {
      key: 'extended',
      label: 'Extended plan',
      type: 'boolean',
      default: false,
      group: 'advanced',
      help: 'Adds the parsed, analyzed and optimized plans to explain.',
      visibleWhen: (params) => debugHasAction(params, 'explain'),
    },
  ],
  keywords: [
    'debug',
    'inspect',
    'show',
    'print_schema',
    'explain',
    'count',
    'preview',
    'dtypes',
    'troubleshoot',
  ],
  gotchas: [
    'The node always returns the ORIGINAL DataFrame, even when its own transformations reshaped what was printed. Nothing downstream can observe it.',
    'The nested chain is read from `transformations`, NOT from `with_transformations` (a join-only key). Misspelled, it is absorbed into params, never runs, and the printout quietly covers everything.',
    'Its nested transformations affect the inspection view ONLY — they are applied to a throwaway copy and discarded.',
    'A `stop_if_empty` placed inside the nested chain still aborts the whole pipeline, despite this node being "read-only". So does a `collect`, which writes to the shared runtime store.',
    'The nested chain runs on an engine that only knows the builtin types — runtime-registered custom transformations are unavailable there.',
    '`count`, `show` and `explain` are Spark actions on the inspected view; a debug node in a hot path pays for them on every run.',
    'Output goes to stdout via print(), not to the structured logger — it will not appear in log aggregation.',
    'An unknown action prints a warning line and is otherwise ignored, so a typo never fails the run.',
  ],
  examples: [
    {
      title: 'Count and show a focused view of the working set',
      json: lines(
        '{',
        '  "type": "debug",',
        '  "label": "Cessoes a processar",',
        '  "actions": ["count", "show"],',
        '  "transformations": [',
        '    { "type": "select", "columns": ["id_operacao"] },',
        '    { "type": "distinct" }',
        '  ]',
        '}',
      ),
    },
    {
      title: 'Cheap checkpoint on the schema after a union',
      json: lines(
        '{',
        '  "type": "debug",',
        '  "label": "base unificada (sacado + cedente)",',
        '  "actions": ["count", "print_schema"]',
        '}',
      ),
    },
    {
      title: 'Inspect a wide payload untruncated, one field per line',
      json: lines(
        '{',
        '  "type": "debug",',
        '  "label": "payload final",',
        '  "actions": ["show"],',
        '  "show_rows": 2,',
        '  "truncate": false,',
        '  "vertical": true',
        '}',
      ),
    },
  ],
  sideEffectFree: true,
}

const includeDef: TransformationDef = {
  type: '$include',
  label: 'Include file',
  family: 'control',
  accent: 'control',
  icon: 'FileSymlink',
  summary: 'Splices transformations from another JSON file into this chain.',
  description: lines(
    '**Not a transformation.** `{ "$include": "shared/filter_tipo.json" }` is a directive: the file content is spliced into the transformations array *before* the JSON is parsed, so by the time the engine runs there is no include left — only the nodes it expanded into.',
    '',
    'The referenced file holds either a single transformation object or a list of them. A list is spliced in place, keeping the order of the nodes around it, which makes this the way to factor out a prelude that several pipelines repeat verbatim.',
    '',
    '`{param}` placeholders inside the included file are substituted with the same values as the main file — but only when the pipeline is run through `Sparquet.run` / `run_from_dict`. Loading a config directly through `Pipeline.from_file` expands the include and leaves the placeholders literal.',
    '',
    'Studio cannot read the referenced file, so it shows this node as an opaque marker: the nodes it expands into are invisible on the canvas and excluded from schema tracking, linting and preview.',
  ),
  fields: [
    {
      key: '$include',
      label: 'File path',
      type: 'text',
      required: true,
      placeholder: 'shared/filter_tipo.json',
      help: 'Relative to the folder of the pipeline JSON file.',
      docs: lines(
        'The path is resolved against the directory of the main pipeline file, with no existence check — a wrong path surfaces as a `FileNotFoundError` in the run result.',
        '',
        'It resolves against the process working directory instead when the config is executed as a dict (`run_from_dict`), which is why a relative path that works from one entry point can fail from the other.',
      ),
      validate: (value) => {
        if (typeof value !== 'string' || value.trim() === '') return 'A file path is required.'
        const path = value.trim()
        if (path.startsWith('/') || /^[A-Za-z]:[\\/]/.test(path)) {
          return 'Use a path relative to the pipeline JSON file, not an absolute one.'
        }
        return path.toLowerCase().endsWith('.json')
          ? null
          : 'The included file must be a .json file.'
      },
    },
  ],
  keywords: [
    'include',
    '$include',
    'import',
    'reuse',
    'shared',
    'partial',
    'snippet',
    'factor out',
    'directive',
  ],
  gotchas: [
    'Only the TOP-LEVEL transformations array is scanned. An include inside a join right-side chain, a debug chain or an output chain is never expanded and fails the run with a KeyError on `type`.',
    'Nested includes ARE resolved, and their paths are relative to the file that wrote them, not to the main pipeline — a folder of includes moves without rewriting the paths inside it. A cycle raises a ValueError naming the chain instead of recursing forever.',
    'Studio cannot preview the included content — the expanded nodes never appear on the canvas, and schema tracking, linting and preview all stop at this node.',
    '`{param}` substitution inside the included file only happens through `Sparquet.run` / `run_from_dict`. `Pipeline.from_file` expands the include but leaves placeholders literal.',
    'The path has no existence check; a wrong path surfaces as a FileNotFoundError in the run result.',
    'Base directory differs by entry point: the pipeline JSON folder for `run`, the process working directory for `run_from_dict`.',
  ],
  examples: [
    {
      title: 'Share the asset-type filter across every registration pipeline',
      json: lines('{ "$include": "shared/filtro_tipo_ativo.json" }'),
    },
    {
      title: 'The included file — a list is spliced in, keeping surrounding order',
      json: lines(
        '[',
        '  {',
        '    "type": "with_column",',
        '    "column": "cnpj_entidade",',
        '    "expression": "explode(array(cnpj_sacado, cnpj_cedente))"',
        '  },',
        '  { "type": "filter", "condition": "cnpj_entidade IS NOT NULL" }',
        ']',
      ),
    },
  ],
}

export const ADVANCED_TRANSFORMATIONS: TransformationDef[] = [
  joinDef,
  unionDef,
  groupByDef,
  checkpointDef,
  stopIfEmptyDef,
  collectDef,
  debugDef,
  includeDef,
]
