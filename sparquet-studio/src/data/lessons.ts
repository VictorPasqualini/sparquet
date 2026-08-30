/**
 * The in-app learning path.
 *
 * Every claim here is checked against the framework's behavior, not the README:
 * where the code and the docs disagree (kafka column defaults, `warn` vs `skip`,
 * `sort` accepting a list of booleans) the lessons describe the code.
 */

import type { JobTemplate } from '@/types/studio'

export interface LessonSection {
  heading: string
  /** Markdown. Rendered by the docs drawer. */
  body: string
  /** One short, actionable takeaway highlighted under the section. */
  tip?: string
  /** Pipeline JSON fragment illustrating the section. */
  code?: string
}

export interface Lesson {
  id: string
  title: string
  minutes: number
  level: JobTemplate['level']
  summary: string
  sections: LessonSection[]
  checklist: string[]
  /** Template opened by the "try it" action, when one matches the lesson. */
  templateId?: string
}

export const LESSONS: Lesson[] = [
  {
    id: 'first-pipeline',
    title: 'Your first pipeline in 5 minutes',
    minutes: 5,
    level: 'starter',
    summary:
      'Read a file, keep the rows you want, fix the types and write the result — the four moves every Sparquet pipeline is built from.',
    templateId: 'csv-to-parquet',
    sections: [
      {
        heading: 'A pipeline is four blocks',
        body: [
          'A Sparquet pipeline is a single JSON document with four parts: **one `input`**, an ordered list of **`transformations`**, an optional **`validations`** block, and **`output`** (or `outputs` for several destinations).',
          'There is no branching and no loop. The transformations array is a straight line: the DataFrame produced by one step is the input of the next.',
        ].join('\n\n'),
        code: `{
  "name": "csv_to_parquet",
  "input": { "format": "csv", "path": "/data/landing/orders" },
  "transformations": [
    { "type": "filter", "condition": "status = 'CONFIRMED'" }
  ],
  "output": { "format": "parquet", "path": "/data/curated/orders", "mode": "overwrite" }
}`,
        tip: '`name` is required and identifies the run in the logs and in the quality report.',
      },
      {
        heading: 'Pick a source',
        body: [
          'Six formats can be read: `parquet`, `csv`, `delta`, `iceberg`, `txt` and `view`. `kafka` is a **sink only** — it never appears on an input.',
          'CSV comes with defaults you get for free: `header=true`, `inferSchema=true` and `encoding=UTF-8`. Override them by supplying the exact same key in `options` — the merge is case-sensitive, so `Header` adds a second option instead of replacing the default.',
          'The `path` field means something different per format: a directory for `parquet`/`csv`/`txt`, a table name or a physical path for `delta`, a catalog identifier for `iceberg`, a temp-view name for `view`, and a topic for `kafka`.',
        ].join('\n\n'),
        code: `{
  "input": {
    "format": "csv",
    "path": "/data/landing/orders",
    "options": { "sep": ";", "header": "true", "inferSchema": "false" }
  }
}`,
        tip: 'Turning `inferSchema` off avoids an extra pass over the file and stops types drifting between runs.',
      },
      {
        heading: 'Shape the rows',
        body: [
          'Four transformations cover most first pipelines:',
          '- `filter` — a SQL boolean expression, evaluated against the current columns.\n- `select` — keeps the listed columns, in the listed order, and drops everything else.\n- `cast` — a map of column to Spark type.\n- `with_column` — adds or replaces one computed column.',
          'Order is load-bearing. Filtering on a column and then selecting it away works; doing it in the other order fails at analysis time.',
        ].join('\n\n'),
        code: `"transformations": [
  { "type": "filter", "condition": "status = 'CONFIRMED'" },
  { "type": "select", "columns": ["order_id", "customer_id", "country", "amount", "ordered_at"] },
  { "type": "cast", "columns": { "amount": "decimal(18,2)", "ordered_at": "timestamp" } },
  { "type": "with_column", "column": "order_date", "expression": "to_date(ordered_at)" }
]`,
      },
      {
        heading: 'Choose where it lands',
        body: [
          '`mode` is `overwrite` by default. `append` adds to the destination, and `merge` performs an upsert — but `merge` only exists for `delta` and `iceberg`; on Parquet or CSV it reaches Spark as an unknown save mode and fails.',
          '`partition_by` splits the written dataset by column value. It is honored by `parquet`, `csv`, `txt` and the non-merge paths of `delta`/`iceberg`, and silently ignored by `view` and `kafka`.',
          'Parquet and CSV always write a **directory of part files**, never a single file — `path` is a folder.',
        ].join('\n\n'),
        code: `"output": {
  "format": "parquet",
  "path": "/data/curated/orders",
  "mode": "overwrite",
  "partition_by": ["country"]
}`,
      },
      {
        heading: 'Run it and read the result',
        body: [
          'A run returns a result object instead of throwing: `success`, `rows_read`, `rows_written`, `validation_results`, `skipped` and `error`. Check the fields — an exception never reaches you.',
          'There are exactly three outcomes: a full run (`success` and not `skipped`), a graceful stop (`success` and `skipped`, produced by `stop_if_empty`), and a failure (`success` false, with the message in `error`).',
          'One column appears that you did not write: after reading the input, the framework adds `ingestion_ts` with the current timestamp. It flows into everything downstream unless a `select` or an output `columns` projection removes it.',
        ].join('\n\n'),
        tip: 'The graph is the source of truth. The JSON panel is generated from it, so edit nodes and let the JSON follow.',
      },
    ],
    checklist: [
      'Create a job from the "CSV to Parquet" template',
      'Change the input path and the filter condition to match your own data',
      'Open the JSON panel and find the four transformations you see on the canvas',
      'Add a `with_column` step and watch the JSON update',
      'Run the pipeline and read `rows_read` and `rows_written` in the result',
      'Spot the automatic `ingestion_ts` column in the output schema',
    ],
  },

  {
    id: 'thinking-in-nodes',
    title: 'Thinking in nodes: how a Sparquet graph maps to JSON',
    minutes: 8,
    level: 'starter',
    summary:
      "One node is one JSON object. Learn the mapping and you can read any pipeline — yours, a teammate's, or one the assistant wrote.",
    templateId: 'nested-payload-multi-output',
    sections: [
      {
        heading: 'One node, one object',
        body: [
          'Every transform node compiles to a single object with a `type` key. Everything else you fill in becomes a parameter of that transformation — there is no fixed schema, and the framework simply forwards unknown keys.',
          'That is why the editor matters: **the framework performs no schema validation**. A misspelled key is silently ignored and a missing required key raises a Python `KeyError` at runtime. Studio is the only layer that catches it before Spark does.',
          'Only two keys are special: `type`, which selects the transformation, and `skip_if_false`, the guard covered in the production lesson.',
        ].join('\n\n'),
        code: `{ "type": "with_column", "column": "order_date", "expression": "to_date(ordered_at)" }`,
      },
      {
        heading: 'The chain is a straight line',
        body: [
          'The transformations array runs top to bottom with no branching primitive. Two nodes bend that line:',
          '- `skip_if_false` on any node removes **that node** from the run.\n- `stop_if_empty` terminates the **whole pipeline** when the DataFrame is empty — remaining steps, validations and every output are skipped, and the run is still reported as a success.',
          'Anything you place after `stop_if_empty` is unreachable on an empty DataFrame, which is exactly the point: put it right after the filter that defines the working set, before the expensive joins.',
        ].join('\n\n'),
        code: `[
  { "type": "filter", "condition": "status = 'PENDING'" },
  { "type": "stop_if_empty", "message": "Nothing pending — nothing to do" },
  { "type": "join", "input": { "format": "delta", "path": "crm.dim_customer" }, "on": "customer_id" }
]`,
      },
      {
        heading: 'Sources, second inputs and sinks',
        body: [
          "A source node with no incoming edge is the pipeline `input`. A source wired into the **second handle** of a `join` or a `union` is not a separate read step — it compiles into that transformation's `with` block.",
          'Sink nodes compile into `outputs`. A single sink can be written as `output`; Studio emits the list form as soon as there is more than one.',
        ].join('\n\n'),
        code: `{
  "type": "join",
  "input": { "format": "delta", "path": "sales.dim_customer" },
  "on": ["customer_id"],
  "how": "left"
}`,
        tip: 'A `join` right side can carry its own chain of nodes — see the joining lesson.',
      },
      {
        heading: 'Fan-out happens at the write side',
        body: [
          'Every output starts from the **same** transformed DataFrame. Two knobs make them differ:',
          '- `columns` projects a subset for that destination only.\n- `transformations` reshapes the DataFrame for that destination only, before the projection and the write.',
          'Outputs are written sequentially and there is no transaction across them: if the second destination fails, the first one is already on disk and the run reports a failure.',
        ].join('\n\n'),
        code: `"outputs": [
  { "format": "parquet", "path": "/dw/full", "mode": "overwrite" },
  {
    "format": "csv",
    "path": "/export/report",
    "columns": ["order_id", "total"],
    "transformations": [
      { "type": "with_column", "column": "total", "expression": "cast(amount as double)" }
    ]
  }
]`,
        tip: 'Put a `checkpoint` as the last transformation when you have several outputs, otherwise each destination recomputes the whole lineage.',
      },
      {
        heading: 'What never reaches the JSON',
        body: [
          'Notes, node comments and labels are canvas metadata — they are not compiled. A **disabled** node stays visible but is omitted from the JSON entirely, which makes it the safest way to test "what if I remove this step".',
          'Use `debug` when you want to inspect without changing anything: it prints `count`, `print_schema`, `show`, `explain`, `columns` or `dtypes` and always returns the original DataFrame. It can even apply throwaway transformations for the inspection only.',
        ].join('\n\n'),
        code: `{
  "type": "debug",
  "label": "after join",
  "actions": ["count", "show"],
  "show_rows": 5,
  "transformations": [{ "type": "filter", "condition": "region = 'EU'" }]
}`,
        tip: 'Debug output goes to stdout, not to the structured JSON log — it will not show up in log aggregation.',
      },
      {
        heading: 'The round trip',
        body: [
          'The graph compiles to JSON, and JSON imports back into a graph. Paste a pipeline a colleague sent you into the JSON panel and the canvas rebuilds itself, layout included.',
          'Because the mapping is one-to-one, nothing is lost in translation: what you see on the canvas is exactly what Sparquet will execute.',
        ].join('\n\n'),
      },
    ],
    checklist: [
      'Open a template and match every node on the canvas to an object in the JSON panel',
      'Disable one transform node and confirm it disappears from the JSON',
      'Wire a second source into a join and see it become the `with` block',
      'Add a second sink and give it a different `columns` projection',
      'Insert a `debug` node and confirm the compiled JSON is unchanged downstream',
    ],
  },

  {
    id: 'transformations-that-matter',
    title: 'Transformations that matter: select vs with_column vs struct',
    minutes: 10,
    level: 'intermediate',
    summary:
      'Three steps do most of the work in a real pipeline. Knowing which one to reach for — and how each treats SQL — is most of the skill.',
    templateId: 'nested-payload-multi-output',
    sections: [
      {
        heading: 'select projects, and it is destructive',
        body: [
          'Every entry of `columns` is parsed as a **SQL expression**, never as a plain column name. That is what lets you write a full expression with an alias in the middle of a projection.',
          'Two consequences: a name that is not valid SQL (spaces, reserved words) has to be backtick-quoted, and a dotted name like `payload.total` is read as a struct field, not as a column literally named "payload.total".',
          '`select` drops everything not listed — including `ingestion_ts`. That is often what you want right before a `union`.',
        ].join('\n\n'),
        code: `{
  "type": "select",
  "columns": [
    "order_id",
    "to_json(payload) AS value",
    "CAST(amount AS DOUBLE) AS amount",
    "\`weird name\`"
  ]
}`,
      },
      {
        heading: 'with_column has two mutually exclusive forms',
        body: [
          'The single form takes `column` plus `expression`. The multi form takes a `columns` map of name to expression.',
          'When `columns` is present it **takes full precedence** — `column` and `expression` become dead config, silently ignored. Treat them as two modes, never mix them in one node.',
          'In the multi form, key order is semantic: columns are created in order, so a later expression can reference one defined earlier in the same block.',
        ].join('\n\n'),
        code: `{
  "type": "with_column",
  "columns": {
    "net_amount": "amount - coalesce(discount, 0)",
    "net_with_tax": "net_amount * 1.2"
  }
}`,
        tip: '`net_with_tax` can read `net_amount` because the map is applied in insertion order — reordering the keys breaks it.',
      },
      {
        heading: 'cast is not with_column',
        body: [
          '`cast` resolves its keys as **real columns**, not expressions: a missing column raises immediately. `with_column` goes through the SQL parser, so it can invent a column out of nothing.',
          'Cast failures follow Spark semantics: an unparseable value becomes `null`, not an error. If a bad cast must be visible, add a `not_null` validation on the casted column.',
        ].join('\n\n'),
        code: `{ "type": "cast", "columns": { "amount": "decimal(18,2)", "order_date": "date" } }`,
      },
      {
        heading: 'struct builds the nested shape',
        body: [
          '`struct` turns a map of field to expression into one nested column. It is the readable alternative to a wall of `named_struct(...)`.',
          'Keys can be **dot-paths**: `"issuer.name"` and `"issuer.document"` merge into one `issuer` sub-struct. You can mix dot-paths with nested maps in the same node, and field order follows key order.',
          'Two conflicts raise at run time: using a path segment that is already a leaf, and declaring the same leaf twice. Two nested maps on the same path are merged instead.',
          'Every leaf value is a SQL expression — so a constant must be quoted (`"\'ORDER_APPROVED\'"`), and a field name containing a literal dot is impossible because every dot means nesting.',
        ].join('\n\n'),
        code: `{
  "type": "struct",
  "column": "payload",
  "fields": {
    "external_id": "contract_id",
    "issuer.name": "issuer_name",
    "issuer.document": "lpad(cast(issuer_document as string), 14, '0')",
    "amounts": { "principal": "cast(principal_amount as double)", "total": "total_amount" }
  }
}`,
      },
      {
        heading: 'Reading the payload back',
        body: [
          'Once built, the struct is addressable with dots in any later expression: `payload.issuer.document`. Serialize the whole thing with `to_json(payload)` when a destination expects a string — that is exactly how the Kafka and audit outputs are fed.',
          'This is why per-output transformations exist: the same payload becomes a JSON string for messaging and a set of flat typed columns for the warehouse, without duplicating the pipeline.',
        ].join('\n\n'),
        code: `"transformations": [
  { "type": "with_column", "columns": {
      "issuer_document": "payload.issuer.document",
      "value": "to_json(payload)"
  } }
]`,
      },
      {
        heading: 'Cheap steps and expensive steps',
        body: [
          'Almost everything is lazy — Spark builds a plan and executes it once at the end. The exceptions matter for placement:',
          '- `stop_if_empty` runs `isEmpty()`\n- `collect` pulls distinct values to the driver\n- `checkpoint` with `eager` materializes the DataFrame\n- `debug` with `count`, `show` or `explain`\n- every non-empty `skip_if_false` costs one tiny Spark job',
          'The pipeline itself also counts rows twice per run: once after the read and once before writing.',
        ].join('\n\n'),
        tip: 'A `debug` node inside a hot path re-triggers computation every run — remove it before promoting the pipeline.',
      },
    ],
    checklist: [
      'Rewrite a chain of three `with_column` nodes as one multi-column node',
      'Use a full SQL expression with an alias inside a `select`',
      'Build a payload with `struct` using at least one dot-path key',
      'Read a nested field back with `payload.a.b` in a later step',
      'Serialize the payload with `to_json` inside an output transformation',
      'Remove every `debug` node before your first production run',
    ],
  },

  {
    id: 'joining-and-enriching',
    title: 'Joining and enriching data',
    minutes: 12,
    level: 'intermediate',
    summary:
      'How the right side of a join is read and shaped, why the join key is trickier than it looks, and how to push a key list down into a huge table.',
    templateId: 'join-runtime-pushdown',
    sections: [
      {
        heading: 'The right side is a read, not a node upstream',
        body: [
          'A `join` reads its own source from `with` — format, path and options, exactly like the pipeline input. Any readable format works.',
          '`with_transformations` is a nested chain applied to that right-hand DataFrame **before** the join. It accepts the full transformation catalog, including another join.',
          'What it does not accept is `$include`: includes are expanded only in the top-level transformations array.',
        ].join('\n\n'),
        code: `{
  "type": "join",
  "input": { "format": "delta", "path": "sales.bronze_customer_events" },
  "with_transformations": [
    { "type": "select", "columns": ["customer_id", "credit_score", "segment"] },
    { "type": "distinct" }
  ],
  "on": "customer_id",
  "how": "left"
}`,
      },
      {
        heading: 'l and r exist only in the ON clause',
        body: [
          'The left DataFrame is aliased `l` and the right one `r` — but the aliases are applied **after** `with_transformations` has run. Inside that nested chain the right side has no alias yet, so use bare column names there and keep `l.` / `r.` for the `on` expression.',
        ].join('\n\n'),
        code: `{
  "type": "join",
  "input": { "format": "delta", "path": "crm.dim_address" },
  "with_transformations": [{ "type": "select", "columns": ["document", "city", "state"] }],
  "on": "l.customer_document = r.document",
  "how": "left"
}`,
        tip: 'Qualify with `l.` and `r.` whenever both sides carry the same column name.',
      },
      {
        heading: 'How the join key is interpreted',
        body: [
          'The `on` value is read with a simple rule: **a string containing a space is treated as a SQL expression**; anything else is handed over as a column name, and a list is a list of column names.',
          "A list of names produces Spark's de-duplicated join columns — one `customer_id` in the result. An expression keeps both sides' columns, so downstream references can become ambiguous.",
          'The space rule bites on single-column expressions: `"upper(id)"` has no space, so it is treated as a column name and fails at analysis. Write `"upper(l.id) = upper(r.id)"` instead.',
          '`how` is validated against the full set of Spark spellings — `inner`, `cross`, `outer`, `full`, `fullouter`, `full_outer`, `left`, `leftouter`, `left_outer`, `right`, `rightouter`, `right_outer`, `semi`, `leftsemi`, `left_semi`, `anti`, `leftanti`, `left_anti`. Anything else raises.',
        ].join('\n\n'),
        code: `{ "on": ["customer_id", "order_id"], "how": "left" }`,
      },
      {
        heading: 'Runtime pushdown: collect and {{var}}',
        body: [
          'When the right side is huge and the left side is small, do not let Spark scan everything. Materialize the working set, collect its keys, and push a literal `IN (...)` into the read.',
          'Three steps, always in this order: `checkpoint` so the collect does not recompute the lineage, `collect` to publish the variable, then the placeholder inside the nested filter.',
          "`{{var}}` is resolved just before each transformation runs, and it is formatted for SQL: a list of strings becomes `'a', 'b'` with quotes escaped, a numeric list becomes `1, 2`, and an **empty list becomes `NULL`** so `IN (NULL)` correctly matches nothing.",
        ].join('\n\n'),
        code: `[
  { "type": "checkpoint" },
  { "type": "collect", "column": "customer_id", "as": "active_customers" },
  {
    "type": "join",
    "input": { "format": "delta", "path": "sales.bronze_customer_events" },
    "with_transformations": [
      { "type": "filter", "condition": "customer_id IN ({{active_customers}})" }
    ],
    "on": "customer_id",
    "how": "left"
  }
]`,
        tip: 'The runtime store is shared with nested chains and with per-output transformations, and it is cleared at the start of every run.',
      },
      {
        heading: 'Two traps with runtime variables',
        body: [
          'An **unresolved** `{{name}}` is left in place as literal text — no error. It reaches Spark as invalid SQL, so a typo shows up as a confusing parse failure rather than a missing-variable message.',
          'Keep parameter names and runtime variable names **disjoint**. The `{param}` template runs first over the raw text and its pattern also matches the inner braces of `{{name}}`; if a parameter shares the name, the runtime placeholder is destroyed before execution.',
          'Also beware nulls: the list type is decided by its first element, so a collected column containing nulls can render as invalid SQL.',
        ].join('\n\n'),
      },
      {
        heading: 'union is not a join',
        body: [
          '`union` appends rows from a second source. By default it matches columns **by position**, ignoring names — mismatched schemas silently produce wrong data.',
          'Set `allow_missing_columns` to true to match by name and fill the gaps with nulls. It is almost always the safer choice.',
          'There is no `with_transformations` on `union`, so shape the main DataFrame first. Remember the automatic `ingestion_ts` column: a positional union with a source that lacks it is the classic breakage, and a `select` before the union removes it.',
        ].join('\n\n'),
        code: `{
  "type": "union",
  "input": { "format": "csv", "path": "/data/landing/orders_us" },
  "allow_missing_columns": true
}`,
      },
    ],
    checklist: [
      'Join a second source and shape it with `with_transformations` first',
      'Write an `on` expression using `l.` and `r.` and confirm the space rule',
      'Chain `checkpoint` then `collect` then a `{{var}}` filter on the right side',
      'Check that your parameter names and runtime variable names never collide',
      'Set `allow_missing_columns` on every union unless you truly want positional matching',
      'Select away `ingestion_ts` before a union',
    ],
  },

  {
    id: 'data-quality',
    title: 'Data quality: validations, on_failure and the DQ report',
    minutes: 10,
    level: 'intermediate',
    summary:
      'Validations never change your data — they measure it. Learn the six rules, their inconsistent null handling, and when the report actually gets written.',
    templateId: 'ingestion-data-quality',
    sections: [
      {
        heading: 'Transform or validate?',
        body: [
          'The division is simple: **transformations change the data, validations report on it**.',
          '- Remove null rows before writing → `filter`\n- Know how many nulls arrived, without removing them → `not_null`\n- Drop duplicates → `drop_duplicates`\n- Fail the pipeline if duplicates exist → `unique`',
          'Validations run after all transformations and **before any output is written**, so a failing rule in the strict mode prevents every write.',
        ].join('\n\n'),
      },
      {
        heading: 'The six built-in rules',
        body: [
          '`not_null` and `unique` take **`columns`** (a list). `range` and `regex` take **`column`** (singular) — swapping them raises a raw `KeyError`, not a friendly message. `row_count` and `sql` take no column at all.',
          '`unique` is a composite check: `["a", "b"]` validates the pair, not each column separately. Use two rules if you need both.',
          '`range` bounds are inclusive and both are optional — but a rule with neither bound silently passes without touching the data.',
        ].join('\n\n'),
        code: `"validations": {
  "on_failure": "warn",
  "rules": [
    { "type": "not_null", "columns": ["id", "email"] },
    { "type": "unique", "columns": ["id"] },
    { "type": "range", "column": "age", "min": 0, "max": 150 },
    { "type": "regex", "column": "email", "pattern": "^[^@\\\\s]+@[^@\\\\s]+\\\\.[a-z]{2,}$" },
    { "type": "row_count", "min": 1 }
  ]
}`,
        tip: 'Regex patterns travel through JSON, so backslashes must be doubled — and `rlike` is a partial match, anchor with `^...$`.',
      },
      {
        heading: 'Nulls are treated differently by each rule',
        body: [
          'This is the single most surprising part of the validation engine:',
          '- `regex` counts **nulls as failures**.\n- `range` lets nulls **pass** — the comparison yields null, so the row is never matched.\n- `unique` treats multiple nulls as **duplicates**, because Spark considers nulls equal for `distinct`.\n- Only `not_null` actually targets nulls.',
          'Pair a `range` rule with a `not_null` rule on the same column when a missing value should also be a violation.',
        ].join('\n\n'),
      },
      {
        heading: 'on_failure decides what happens next',
        body: [
          "`fail` (the **default** when omitted) aborts the pipeline after all rules have run — no report, no outputs. The failure arrives as the result's `error` string, and `validation_results` comes back empty.",
          '`warn` and `skip` both continue to the report and the writes. They are two labels for the same behavior.',
          'The value is compared literally, so `FAIL` or a typo silently behaves like `warn`. Rules are never short-circuited either: all of them always run before the decision is made.',
        ].join('\n\n'),
        code: `{ "on_failure": "fail" }`,
        tip: 'Use `fail` to protect a destination, `warn` to build a quality history. You cannot have both in the same block.',
      },
      {
        heading: 'The quality report',
        body: [
          '`validations.report` is a normal output config, and it writes a fixed schema: `pipeline`, `rule_type`, `passed`, `failed_count`, `message`, `validated_at`.',
          'One row per rule, in declaration order, **including the rules that passed** (empty message, zero count). With zero rules it still writes an empty dataset.',
          'The catch: in `fail` mode with a violation, the abort happens first, so **no report is written for the failures you most want logged**. If the report must always exist, use `warn`.',
          '`columns` and `transformations` parse on the report block but are ignored — the report is written directly.',
        ].join('\n\n'),
        code: `"report": { "format": "delta", "path": "quality.validation_log", "mode": "append" }`,
      },
      {
        heading: 'sql for the rules that do not fit',
        body: [
          'The DataFrame is exposed under the fixed view name `_validation_df` — there is no configurable name here.',
          'The semantics are **pass when true**: write the invariant, not the violation. `SELECT count(*) = 0 FROM _validation_df WHERE amount < 0` is right; returning the raw count inverts the meaning, because zero bad rows would evaluate to false.',
          'Only the first cell of the first row is read, and the query can join catalog tables — which is how a pipeline compares its own output against the source it came from.',
        ].join('\n\n'),
        code: `{
  "type": "sql",
  "query": "SELECT count(*) = 0 FROM _validation_df WHERE open_amount <= 0",
  "error_message": "Customers staged with a non-positive open amount"
}`,
      },
      {
        heading: 'Read failed_count carefully',
        body: [
          '`failed_count` means something different per rule: summed nulls across columns for `not_null` (a row null in two columns counts twice), excess rows for `unique`, offending rows for `range` and `regex`, and a hardcoded `1` for `row_count` and `sql`.',
          'Never sum it across rule types and never present it as "bad rows".',
          'Every rule also costs its own Spark actions, so a long rule list is not free on a large DataFrame.',
        ].join('\n\n'),
      },
    ],
    checklist: [
      'Add `not_null` and `unique` rules on your key columns',
      'Decide consciously between `fail` and `warn` — the default is `fail`',
      'Point the report at an append destination to build a quality history',
      'Pair every `range` rule with a `not_null` rule on the same column',
      'Write one `sql` invariant against `_validation_df`',
      'Confirm the report is written by running once in `warn` mode',
    ],
  },

  {
    id: 'going-to-production',
    title: 'Going to production',
    minutes: 15,
    level: 'advanced',
    summary:
      'Parameterize one JSON for many runs, reuse fragments, control the cost of the plan, upsert safely, and understand what changes when the job runs on a cluster.',
    templateId: 'parameterized-pipeline',
    sections: [
      {
        heading: 'One pipeline, many runs: {param}',
        body: [
          'Parameters are substituted into the **raw JSON text before it is parsed**, so a placeholder can appear anywhere: a path, a SQL condition, a table name, an option value.',
          "Values are formatted for SQL by type: a string list becomes `'a', 'b'`, a numeric list becomes `1, 2`, `true` becomes `\"true\"`, and **`false` or an empty list becomes an empty string** — which is what drives the guard in the next section.",
          'A key that is not in the params map is left **literal**. Absent is not the same as false: the placeholder leaks into your SQL as text.',
          'Because substitution happens before parsing, a value containing a double quote or a backslash corrupts the JSON and the run fails with a parse error.',
        ].join('\n\n'),
        code: `{
  "input": { "format": "delta", "path": "sales.orders" },
  "transformations": [
    { "type": "filter", "condition": "region = '{region}'" },
    { "type": "filter", "skip_if_false": "{product_ids}", "condition": "product_id IN ({product_ids})" }
  ]
}`,
        tip: 'Parameters cannot be passed on the command line — a parameterized pipeline must be launched through the library API.',
      },
      {
        heading: 'skip_if_false: branching without branches',
        body: [
          'The guard has exactly three outcomes. Absent → the step runs. An **empty string** → the step is skipped. Anything else is evaluated by Spark as `SELECT (expr)` and the step is skipped only if the result is a genuine boolean `false`.',
          'It **fails open**: an expression that does not compile is swallowed and the step runs. A typo silently disables the guard instead of raising.',
          'The expression only sees literals — it cannot reference DataFrame columns, and `{{runtime}}` variables are not substituted inside it. It exists to branch on parameters.',
        ].join('\n\n'),
        code: `{
  "type": "struct",
  "skip_if_false": "'{load_mode}' in ('FULL', 'BACKFILL')",
  "column": "audit",
  "fields": { "mode": "'{load_mode}'", "at": "current_timestamp()" }
}`,
      },
      {
        heading: 'Reuse fragments with $include',
        body: [
          'An entry `{ "$include": "shared/region_filter.json" }` is replaced inline by the content of that file — a single transformation object or a list of them. The path is relative to the pipeline JSON.',
          'Two hard limits: it works **only in the top-level transformations array** (never inside `with_transformations`, `debug.transformations` or an output), and **nested includes are not expanded**. In both cases the raw directive survives and the run fails.',
          'Parameters are applied to the included file too, but only when the pipeline is launched with params through the library entry point.',
        ].join('\n\n'),
        code: `"transformations": [
  { "$include": "shared/region_filter.json" },
  { "type": "distinct" }
]`,
      },
      {
        heading: 'checkpoint: paying once for the plan',
        body: [
          'After heavy joins the logical plan grows and Spark recomputes it every time the DataFrame is consumed. `checkpoint` materializes the data and truncates the plan.',
          '`localCheckpoint` (the default) writes to executor-local disk. `checkpoint` writes to reliable storage and requires a checkpoint directory configured on the session.',
          'An invalid `method` does **not** raise: the step is ignored, the run still reports success, and the warning only appears at the very end of the log. Keep to the two valid values.',
          'Two placements pay for themselves: right before a `collect`, and as the last transformation when several outputs read the same DataFrame.',
        ].join('\n\n'),
        code: `{ "type": "checkpoint", "method": "localCheckpoint", "eager": true }`,
      },
      {
        heading: 'Merge writes',
        body: [
          'Upserts exist for `delta` and `iceberg` only, and both require the same two options: `on`, the whole match predicate over `T.` (target table) and `S.` (source DataFrame), and `actions`, the list of `WHEN ...` clauses. Nothing is generated for you — the plain upsert is `WHEN MATCHED THEN UPDATE SET *` plus `WHEN NOT MATCHED THEN INSERT *`.',
          'The clauses run in the order given, and the first one that matches wins. A delete on a CDC flag therefore goes **before** the update: `WHEN MATCHED AND S.op = \'D\' THEN DELETE`. To delete what the source no longer carries, add `WHEN NOT MATCHED BY SOURCE THEN DELETE` — correct only when the input is a complete snapshot.',
          'There is **no de-duplication of the source** — two source rows matching one target row raise a runtime error, so guard them with a `unique` validation or a `drop_duplicates`.',
          'On Delta, `UPDATE SET *` / `INSERT *` need source and target to carry the same columns: a CDC source with an extra `op` column fails to resolve it, and the clause has to list the target columns instead. Iceberg tolerates the extra column.',
          '`partition_by` is ignored on the merge path. And note the case sensitivity: Delta lower-cases the mode, Iceberg compares `merge` exactly — always emit lowercase.',
          'For Delta, `path` is a table name when it contains a dot and a physical path when it starts with a known scheme (`/`, `s3://`, `gs://`, `abfss://`, `wasbs://`, `hdfs://`, `dbfs:/`, `file:`). A dotted value under any other scheme is misread as a table name.',
        ].join('\n\n'),
        code: `"output": {
  "format": "delta",
  "path": "analytics.customer_summary",
  "mode": "merge",
  "options": {
    "on": "T.customer_id = S.customer_id AND T.updated_at < S.updated_at",
    "actions": [
      "WHEN MATCHED THEN UPDATE SET *",
      "WHEN NOT MATCHED THEN INSERT *"
    ]
  }
}`,
      },
      {
        heading: 'Running on Databricks, EMR and friends',
        body: [
          'The environment is detected automatically — Databricks, EMR, Dataproc, Synapse, or local — and it decides how the Spark session is obtained. On Databricks the active session is reused, so `app_name`, `master` and the `spark.configs` block are not applied.',
          "The session is a **process-level singleton**: once it exists, a later pipeline's `spark` block is effectively ignored. Configure the session where the job starts, not per pipeline.",
          '`master` is only honored locally; on a managed cluster it is ignored on purpose.',
          'A run returns its result instead of raising, so orchestrate on the fields: `success`, `skipped`, `rows_read`, `rows_written`, `validation_results`, `error`. Treat `skipped` as its own outcome — a graceful "there was nothing to do", not a failure.',
        ].join('\n\n'),
        code: `{
  "spark": {
    "app_name": "orders_nightly",
    "configs": { "spark.sql.shuffle.partitions": "200" }
  }
}`,
        tip: 'A pipeline can also start from a DataFrame you already have, in which case no `ingestion_ts` is added and `rows_read` stays at zero.',
      },
      {
        heading: 'The staging handoff pattern',
        body: [
          'Large migrations rarely fit in one JSON. The proven shape is **staging then commit**: several pipelines write to the same generic temp `view`, and one final pipeline reads that view, validates completeness with `sql`, and fans out to the real destinations.',
          'It keeps the per-case logic small and the write logic in exactly one place. Views are session-scoped, so every step must run in the same Spark session.',
        ].join('\n\n'),
        code: `"outputs": [
  { "format": "view", "path": "registration_staging", "options": { "cache": "true" } }
]`,
      },
    ],
    checklist: [
      'Replace every hardcoded environment value with a `{param}` placeholder',
      'Guard optional steps with `skip_if_false` and test both branches',
      'Move a repeated fragment into a shared file and `$include` it at the top level',
      'Add a `checkpoint` before `collect` and before a multi-output fan-out',
      'Make merge keys unique in the source before writing with `mode: merge`',
      'Configure the Spark session where the job starts, not in each pipeline',
      'Handle `skipped` separately from `success` in your orchestrator',
    ],
  },
]
