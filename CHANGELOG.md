# Changelog

All notable changes to the **framework** (`sparquet` on PyPI) are recorded here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Notes on the history below:

- The version has a single source, `__version__` in `sparquet/__init__.py`; see
  [docs/DEPLOY_PYPI.md](docs/DEPLOY_PYPI.md) for the release procedure.
- Git tags start at `v0.3.0`. The entries for `0.2.3` and earlier were reconstructed
  from the commits that bumped `__version__`, so their dates are commit dates.
- `0.4.0` has an entry but no link: it was never tagged and never reached PyPI, so
  its changes shipped inside `0.5.0`.
- Releases up to `0.2.3` were published under the old distribution name
  **`spark-framework`** (import `spark_framework`). `sparquet` is the name from
  `0.3.0` on.
- [Sparquet Studio](sparquet-studio/) and the data-quality engine
  [`sparquet-cola`](https://github.com/VictorPasqualini/sparquet-cola) carry their own
  version numbers. Studio changes appear here only when they touch the framework or
  the JSON contract.

## [0.11.0] — 2026-09-04

### Added

- **`repartition` transformation.** The write side had no lever at all: `partition_by` on
  the output decided which directories existed, and nothing decided how many tasks wrote
  into them. Files written are one per *(task, directory)* pair that holds rows, so 200
  shuffle partitions over 30 days of `dt` left up to 6,000 files behind. Repartitioning by
  the same key the output partitions by collapses that to one file per key value, because a
  single key value never splits across tasks — AQE merges neighbouring partitions but never
  separates one. Four parameters, none required on its own but at least one of the first two
  needed: `num_partitions`, `columns` (each entry goes through `F.expr`, so
  `pmod(hash(id), 64)` is as valid as a column name), `coalesce` (merge without a shuffle,
  reduces only) and `range` (`repartitionByRange`, split by value band instead of by hash).
  Every invalid combination raises with the reason rather than silently doing something
  else: `coalesce` with `columns` (no key to group by), `coalesce` with `range`, `coalesce`
  without a count, `range` without columns, a non-integer or non-positive count, and no
  parameters at all.

  ```json
  { "type": "repartition", "columns": ["dt"] }
  ```

- **Iceberg hidden partitioning: `partition_by` accepts partition transforms.**
  `bucket(16, id)`, `years(ts)`, `months(ts)`, `days(ts)` and `hours(ts)` alongside plain
  column names. A transform routes the write through `DataFrameWriterV2` (`writeTo`),
  because the V1 `partitionBy` only takes column names — so it requires a catalog table, and
  a physical path combined with a transform is refused with the alternative spelled out.
  This is the only form that keeps pruning: Iceberg stores the column-to-transform relation
  in the table metadata, so `WHERE id = 'X'` prunes the buckets on its own, whereas a bucket
  column materialized by hand with `pmod(hash(id), N)` is only pruned by a filter on that
  column. Spark's own `bucketBy` is not an alternative — it only works with `saveAsTable`
  (Hive bucketing), `save(path)` refuses it and Delta does not support it.

  ```json
  "partition_by": ["days(data_evento)", "bucket(16, id)"]
  ```

  The partition spec belongs to the table, not to the write: it is applied by
  `create` / `createOrReplace`, and an append into an existing table uses the spec the table
  already has.

### Changed

- **The JDBC reader validates the parallel-read options before Spark does.**
  `partitionColumn`/`lowerBound`/`upperBound`/`numPartitions` is an all-or-none quartet, and
  getting it wrong failed in two different ways, both bad: an incomplete `partitionColumn`
  raised a generic Spark message, while the three bounds **without** `partitionColumn` were
  ignored silently and the whole table came through one connection in one task. Now the
  first case raises a `ValueError` naming exactly what is missing, and the second emits a
  deferred warning saying the options are being ignored and why. `query` + `dbtable` and
  `query` + `partitionColumn` — both refused by Spark — are refused here first, the second
  one pointing at the way to write it (move the SELECT into `dbtable` as a subquery **with**
  an alias). Nothing that used to work stopped working: the pushdown options
  (`pushDownPredicate`, `pushDownAggregate`, `pushDownLimit`, `sessionInitStatement`,
  `queryTimeout`) always reached Spark through the passthrough and still do — they are now
  documented and offered by the Studio form.

- **`docs/PIPELINE_SCHEMA.md` gained a read and write strategy section.** The ordered
  levers for reading (partition path plus `basePath`, `filter` first for pruning and
  pushdown, `maxPartitionBytes`), the JDBC read, file-count arithmetic on write, hash
  bucketing, skew, and Iceberg hidden partitioning. It also records a decision: there will
  be no `input.partition_filter`. Catalyst produces the identical physical plan from a
  `filter` placed first — the predicate becomes `PartitionFilters` plus `PushedFilters`
  inside the `FileScan`, visible through `{ "type": "debug", "actions": ["explain"] }` — so
  the key would only be a second place to write the same condition, and a second place for
  it to drift.

### Fixed

- **The first load of an Iceberg merge now creates the table with `partition_by`.** A merge
  cannot run against a table that does not exist, so the first execution writes everything
  instead — and that write ignored `partition_by` entirely. The result was an unpartitioned
  table that no later run repartitions, since the Iceberg partition spec is fixed at
  creation. That first load now honors `partition_by`, transforms included, which makes it
  the one execution of a merge pipeline where the setting has any effect.

## [0.10.0] — 2026-08-30

### Changed

- **The aggregable validations are measured in a single Spark pass.** Every rule used to
  run its own action, and some ran several: `not_null` counted once **per column**,
  `unique` counted twice, and each percent metric counted the rows again for its
  denominator. On a local master that is where the validation stage spent its time, and the
  cost grew with the number of rules rather than with the data. `sparquet-cola` `0.4.0`
  lets a check declare its measurement as aggregation columns, and the engine merges every
  check of a run into one `df.agg(...)`. Same numbers, same messages, same order — pinned
  by a test that compares the block against rule-by-rule execution. Rules that cannot be
  expressed as an aggregation (`sql`, `schema`) still run on their own, and a third-party
  check that does not declare aggregations keeps working unchanged. Measured on 2M rows
  with 5 rules, the validation stage went from 4.53s to 2.39s.

- **`validations.cache` now defaults to `false`.** The default was `true` back when each
  rule was a separate pass over the DataFrame and caching traded N recomputations of the
  lineage for one. With the rules now measured in a single pass there is nothing left to
  amortize, and the cache only pays for materializing the data. Measured on 2M rows with
  5 rules: 6.39s without cache against 10.08s with it from parquet, and 7.86s against
  11.76s from CSV. Turn it on when the lineage itself is expensive **and** several actions
  follow it — `sql` rules, quarantines with different scopes, many outputs.

- **The validation report is built without spawning Python workers.** `createDataFrame`
  over the handful of driver-side rows parallelized them across `defaultParallelism`, so a
  5-row report started 16 Python worker processes and paid for them on every action. The
  rows are now literals over `spark.range(1)`, keeping the plan inside the JVM. On the same
  run the report stage went from 18.1s to 1.2s, and the whole pipeline from 29.7s to 12.0s.

- **Minimum `sparquet-cola` is now `0.4.0`**, for the single-pass measurement above.

### Removed

- **BREAKING: the declarative merge options are gone. `mode: merge` now requires `on` and
  `actions`.** Both forms shipped in `0.9.0`, the hand-written one on top of the generated
  one, and a pipeline could be written either way. Keeping both meant two code paths for one
  statement and, worse, two mental models of what a merge does — the generated form hid the
  clause order that decides whether a CDC delete works at all. The generated form was
  removed, so `on` and `actions` are the only way to write a merge, and both are required:
  neither has a default and a missing one raises a `ValueError` naming it before any Spark
  call. Every removed key is refused **by name**, with the clause that replaces it, so a
  pipeline written against the old form fails on config instead of running a merge that no
  longer means what it says:

  | Removed option | What replaces it |
  |---|---|
  | `merge_keys` | the whole predicate in `on`, e.g. `"T.id = S.id"` |
  | `merge_condition` | folded into `on`, which is the entire `ON` condition |
  | `delete_when` | a clause: `"WHEN MATCHED AND <condition> THEN DELETE"`, before the `UPDATE` |
  | `delete_not_matched_by_source` | a clause: `"WHEN NOT MATCHED BY SOURCE THEN DELETE"` |

  The plain upsert that used to be generated from `merge_keys` is written as:

  ```json
  "options": {
    "on": "T.id = S.id",
    "actions": [
      "WHEN MATCHED THEN UPDATE SET *",
      "WHEN NOT MATCHED THEN INSERT *"
    ]
  }
  ```

  `UPDATE SET *` / `INSERT *` are always fine on Iceberg, which tolerates an extra source
  column. On Delta they need both sides to carry the same columns: a CDC source with an `op`
  column the target lacks fails to resolve, and the clause has to list the target columns —
  which is what the framework used to do for you, and is now visible in the JSON.

  The Iceberg writer also validates the two options **before** its first-load shortcut (an
  empty target is written with `append` + `saveAsTable`, no MERGE at all), so a wrong merge
  config can no longer pass on the first run and fail on the second.

- **BREAKING: `with` is no longer read as the second source of `join`/`union`.** `0.9.0`
  renamed it to `input` and kept `with` working; it now raises a `ValueError` telling you to
  rename the key. The two names for one block were only a migration aid, and no project is
  running on the old one.

## [0.9.0] — 2026-08-30

### Added

- **`mode: merge` can now delete.** The generated `MERGE INTO` had only
  `WHEN MATCHED THEN UPDATE` and `WHEN NOT MATCHED THEN INSERT`, so the two ordinary CDC
  cases had to be handled with SQL written outside the framework. Two new
  `output.options`, shared by Delta and Iceberg:

  - `delete_when` — a SQL condition over the source, for the case where the source
    **carries** the deleted row with a flag on it. It becomes
    `WHEN MATCHED AND (<condition>) THEN DELETE`, emitted **before** the `UPDATE` clause,
    because in `MERGE INTO` the first matching clause wins.
  - `delete_not_matched_by_source` — `true`, or a SQL condition over the target, for the
    case where the source **no longer carries** the row. It becomes
    `WHEN NOT MATCHED BY SOURCE THEN DELETE`. This is only correct against a **complete
    snapshot** of the source: run against an incremental load it deletes everything that
    load did not repeat, so it stays off by default.

  ```json
  "output": {
    "format": "delta",
    "path": "/lake/pedidos",
    "mode": "merge",
    "options": { "merge_keys": ["id"], "delete_when": "S.op = 'D'" }
  }
  ```

- **The MERGE can be written by hand: `on` and `actions`.** The generated statement covers
  the ordinary upsert, but not updating only some columns, inserting a different set, or
  branching on more than one condition. Two `output.options`, shared by Delta and Iceberg:
  `on` replaces the `ON` predicate built from `merge_keys`/`merge_condition` — same shape as
  a join `on` written as SQL — and `actions` is the ordered list of `WHEN ...` clauses,
  emitted as written.

  ```json
  "options": {
    "on": "S.id = T.id AND S.loja = T.loja",
    "actions": [
      "WHEN MATCHED AND S.op = 'D' THEN DELETE",
      "WHEN MATCHED THEN UPDATE SET T.status = S.status",
      "WHEN NOT MATCHED THEN INSERT (id, loja, status) VALUES (S.id, S.loja, S.status)"
    ]
  }
  ```

  The two forms do not mix: with `actions` present, the generated `UPDATE`/`INSERT` and both
  delete options are dropped — that list is the whole MERGE body. Every clause must start
  with `WHEN`, and because the first matching clause of a group wins, an unconditional clause
  may only be the last of its group; a list that would leave a clause unreachable is refused
  with a message naming it, instead of reaching Spark as an analysis error. The assembly moved
  to `sparquet/io/merge.py`, shared by both writers.

- **`$include` is expanded recursively.** An included file may now carry `$include`
  directives of its own, and their paths are relative to **the file that wrote them**, not to
  the main JSON — a folder of shared transformations can be moved without rewriting the paths
  inside it. A cycle raises a `ValueError` naming the route it took
  (`a.json -> b.json -> a.json`) instead of exhausting the stack, and a chain deeper than 20
  levels is refused.

### Changed

- **The second source of `join` and `union` is now `input`, not `with`.** It is the same
  block as the pipeline's own `input` (`format`, `path`, `options`), and it now has the same
  name. `with` keeps working as the old name on both transformations, so no existing JSON
  breaks; the Studio compiler reads both and emits `input`.

- **A join no longer leaves two columns with the same name.** Joining sources that share
  column names — the normal case in a self join — produced a DataFrame with two `nome`
  columns, and the next transformation to mention `nome` died with `AMBIGUOUS_REFERENCE`
  three steps later, with nothing wrong in the JSON. Repeated names are now renamed on the
  **right** side with a `_r` suffix (`nome` and `nome_r`; `_r2`, `_r3` when taken), so the
  left side — the main chain — keeps the names it had. The projection is built after the
  join, so an `on` written as SQL can still refer to `r.nome`; because it drops the `l`/`r`
  aliases, they no longer resolve in the nodes after a join that renamed something. Nothing
  changes when no name repeats. Renaming inside `with_transformations` is still the way to
  choose a name other than `_r`.

- **A param named after a runtime variable no longer eats it.** `apply_template` matched the
  inner `{var}` of a runtime `{{var}}`, so a `params` key with the same name rewrote the
  reference before the `TransformationEngine` ever saw it — `{{tipo}}` became `{valor}` and
  the runtime variable silently disappeared. The pattern now excludes the doubled braces, and
  the two syntaxes are independent.

- **The Delta merge writes only the columns the target already has.** `UPDATE SET` and
  `INSERT` used to list every column of the incoming DataFrame, so a CDC source carrying
  a control column the target does not have (the `op` flag `delete_when` reads) failed
  with `DELTA_MERGE_UNRESOLVED_EXPRESSION`. Those columns are now left out of the write
  instead. Iceberg keeps `UPDATE SET *` / `INSERT *`, which tolerates the extra source
  column on its own.
- **Merge options are no longer forwarded to Spark on the Iceberg writer.** `merge_keys`,
  `merge_condition` and the two new delete options were passed through as writer options
  on non-merge writes — silently, but wrongly. Delta already stripped them.

## [0.8.0] — 2026-08-29

### Fixed

- **The `spark` block in the JSON now reaches session creation.** `Sparquet.__init__`
  used to build the SparkSession from the *constructor's* config, before `run` or
  `run_from_dict` had read the JSON. Since `SparkSession` is a per-process singleton,
  everything under `spark.configs` in the pipeline file was silently dropped — most
  visibly `spark.jars.packages`, which means **no JSON could ask for a connector's
  jar** (avro, delta, iceberg, kafka, the JDBC drivers). Through the CLI, where
  `Sparquet()` takes no arguments, the JSON was the only place those configs could
  come from at all. The session is now created on the first run, with the JSON config
  already parsed and the constructor's config layered on top, exactly as
  `_apply_spark_override` always intended. Code that needs the session before running
  a pipeline uses the new `Sparquet.spark` property.

- **`IcebergWriter` creates the table it writes to.** Writing went through
  `df.write.format("iceberg").save(path)`, and on Spark 4 that path requires the table
  to exist: pointing an output at a new catalog identifier failed with
  `[TABLE_OR_VIEW_NOT_FOUND]` instead of creating anything, so the first load of any
  Iceberg pipeline needed a `CREATE TABLE` issued by hand outside the framework. When
  `path` is a catalog identifier the writer now uses `saveAsTable`, which creates the
  table — honouring `partition_by` — and keeps working on the writes that follow; a
  physical path still goes through `save`. `mode: merge` against a table that does not
  exist yet writes the whole input instead of failing, which is the same state a merge
  into an empty table would leave. The path-vs-table rule moved to `is_table_name` in
  `sparquet/io/base.py`, shared with `DeltaWriter` instead of duplicated.

### Notes

- Both fixes change behaviour, not the JSON contract: a pipeline file that worked
  keeps working. The one difference to be aware of is that `Sparquet()` no longer
  starts a SparkSession by itself — the session appears on the first `run`, or on the
  first read of `Sparquet.spark`.


## [0.7.0] — 2026-08-29

### Added

- **Execution history outside the Studio** (`sparquet.observability`). The framework
  runs anywhere and depends on nothing, which is exactly why the runs that matter
  most — the nightly job on Databricks, the DAG on Airflow, a `sparquet.cli` call on
  a VM — left no trace in any history: monitoring existed only when Studio's runner
  was the one executing. Now the framework can report its own runs, and they read
  back like local ones: same steps, same logs, same screens.

  Off by default and free when off: without `SPARQUET_HISTORY_URL` (and without a
  sink registered in code) nothing is instantiated and `Pipeline.run` is unchanged.
  Turned on, the run is collected from the structured records the framework already
  emits and sent **once, at the end** — one request per execution, not one per step —
  as a single JSON document (`schema: "sparquet.run/1"`) over stdlib `urllib`, so no
  new dependency. Failures are reported too, since that is the run a reader wants.
  Sending never affects the pipeline: a dead receiver, a wrong token or a broken
  network is a `warning` in the log and an identical `PipelineResult`.

  ```bash
  export SPARQUET_HISTORY_URL="http://127.0.0.1:8765/runs/ingest"
  export SPARQUET_HISTORY_TOKEN="$SPARQUET_STUDIO_TOKEN"
  export SPARQUET_HISTORY_JOB_ID="j-orders"
  python -m sparquet run orders.json
  ```

  `SPARQUET_HISTORY_TIMEOUT`, `_WORKFLOW_ID`, `_PIPELINE_ID`, `_RUN_AS`, `_TAGS` and
  `_LOGS=off` complete the configuration; `Sparquet.register_history_sink(sink)`
  replaces the environment for anyone who already has somewhere to put telemetry.
  The receiving end is Studio's `POST /runs/ingest`, which marks such runs
  `launched="external"`. The token is a password: treat it as a credential, prefer
  `https://`, and do not publish the runner on a network to collect history — it
  executes arbitrary Spark and binds to `127.0.0.1` for that reason.

- `sparquet.utils.logger.add_sink` / `remove_sink`: subscribe to the structured
  records as dictionaries. Observability no longer has to re-instrument what the
  framework already logs.

### Changed

- **`rows_written` no longer costs an extra pass over the data.** Every output used
  to be counted with `output_df.count()` right before writing it. Spark is lazy, so
  that count was a full execution of the plan — read, parse, filter, transform —
  and then the write executed the same plan again. A pipeline with M destinations
  ran the work M+1 times to answer a question the write itself already answers:
  Spark instruments every write command with `BasicWriteJobStatsTracker`, which
  publishes `number of output rows` for the job that just wrote. `Pipeline` now reads
  that counter instead of computing its own (`sparquet/core/write_metrics.py`).

  The number is the same, and where it cannot be had it is not guessed. A format
  that publishes no write metric — JDBC, for instance — makes `measure()` answer
  `None`, meaning *don't know*, never zero, and the caller falls back to the explicit
  `count()`. `OutputMetrics.rows_from` says which of the two produced the number
  (`"write_metrics"` or `"count"`), so a wrong figure can never be mistaken for a
  measured one in a report, in history or in billing.

  **Benchmark** (`tests/bench_write_metrics.py`, 6,000,000 rows, CSV → Parquet,
  `local[4]`, minimum of several runs — the machine is noisy, so the minimum is the
  measure least contaminated by GC and neighbours):

  | destinations | write only (floor) | `write_metrics` | `count` (before) | gain |
  |---|---|---|---|---|
  | 1 | 3.66s | 5.61s | 6.87s | **1.27s — 18.4%** |
  | 2 | 8.05s | 8.79s | 12.50s | **3.71s — 29.7%** |

  The saving grows with the number of destinations, as expected: the old cost was
  one extra full execution *per output*. Both paths returned exactly the same row
  counts (5,567,015 and 11,134,030). The residual gap to the floor is
  `rows_read = df.count()` over the input, which still re-reads the source and is a
  separate optimisation.

## [0.6.0] — 2026-08-21

### Changed

- **Metrics are rule types; the `check` wrapper is gone.** `{"type": "missing_percent",
  "column": "cpf", "must_be": "< 1%"}` instead of `{"type": "check", "metric": ...}`.
  The wrapper's only job was to hold a `metric` field that is now the `type` itself, and
  a validation report row now names the metric instead of saying `check`. Removed
  outright, with no compatibility alias — migrating is mechanical: drop
  `"type": "check"` and promote `metric` to `type`.

  `not_null`, `unique`, `range`, `regex`, `sql` and `schema` stay, because their
  semantics are not the metrics': `regex` counts NULL as a violation while `invalid_*`
  treats NULL as *missing*, and `range` labels the ROW outside the interval while
  `min`/`max` describe the column and cannot point at a row. Folding either one in
  would have changed results silently.

- Requires **`sparquet-cola>=0.3.0`**.

### Added

- **`targets` on any rule: one entry, many independent rules.** Each target becomes a
  rule of its own — its own result, its own code, its own contribution to the
  quarantine — so a report says which column broke instead of giving one aggregate
  verdict:

  ```json
  { "type": "regex", "targets": [
      { "column": "cpf",  "pattern": "^[0-9]{11}$" },
      { "column": "cnpj", "pattern": "^[0-9]{14}$" } ] }
  ```

  Keys outside `targets` are shared defaults. Expansion happens at **config parse**,
  using the library's `expand_targets`, because the validation report pairs
  `validations.rules[i]` with `results[i]` by position — expanding later, or twice with
  two implementations, would attribute one rule's result to another rule's target.

- `examples/08_validacao_multi_alvo.json` — `targets` end to end, **runnable locally**
  (CSV in, CSV + JSON out, no cluster): three rule entries become seven report rows,
  the `range` shares `min` and lets each target set its own `max`, and the quarantine
  scope names a single target so the other one's violations stay out of it.

- `tests/test_examples.py` now checks the validation `type` of every shipped example,
  not just the transformations. That gap is exactly why a removed `check` survived in
  `examples/05_data_quality_soda.json` until it was looked for by hand. It also checks
  that every code in a `validations.outputs.*.rules` scope is one some rule actually
  produces: a typo there scopes the quarantine to **nothing**, and an empty dataset
  looks exactly like "no row violated".

### Fixed

- **CSV is written in the RFC 4180 dialect** (`escape='"'` by default, both ways):
  quotes inside a field come out **doubled** (`""`) instead of backslash-escaped
  (`\"`). Spark reads its own dialect back, but Python's `csv`, pandas and Excel do
  not — so the `validations.report`, whose `rule_params` column is quote-heavy JSON,
  arrived split mid-field in the very tools it is analysed in. Reader and writer
  changed together: fixing only the writer would leave the framework unable to read
  what it wrote. Files in the old dialect are still readable with
  `options: {"escape": "\\"}`, and `tests/test_csv_dialect_spark.py` pins all four
  directions.

- `not_null` and `unique` accept `column` (singular), not only `columns`. With
  `targets`, `{"column": "id"}` is the natural way to write a target — it is what
  `range` and `regex` use — and the derived code already rendered `not_null(id)` from
  it; only the execution read `params["columns"]` raw, so the pipeline died with a
  bare `KeyError: 'columns'` that named neither the rule nor the fix. Fixed in
  `sparquet-cola` 0.3.0.

## [0.5.0] — 2026-08-21

### Added

- **Step markers for the validations and for the datasets they write.** Every rule
  emits `Validation started`/`finished` (`scope="validation"`, with `index`/`total`),
  and the report and quarantine writes emit `Validation output started`/`written`
  (`scope="validation_sink"`, with `role`). Studio lights up each box as the run
  reaches it and shows the elapsed time per step once it finishes. Unlike a
  transformation — lazy, so it only builds a plan and reads near zero — a rule is a
  real action, so the time shown against it is time actually spent.
- **A quarantine row now says WHICH rule rejected it.** Every rule accepts a `code`
  (`{"type": "range", "column": "age", "min": 1, "max": 99, "code": "AGE_RANGE"}`) and,
  when it is omitted, the code IS the validation expression, rendered compactly and
  deterministically: `not_null(email)`, `unique(id,dt)`, `range(age,1,99)`
  (`*` marks an open bound: `range(amount,0,*)`), `regex(email,^.+@.+$)`,
  `missing_percent(cpf)`. The rendering lives in `sparquet-cola` (`BaseCheck.code()`),
  which knows each check's semantics.
- **`validations.outputs.invalid.annotate`** — the name of an `array<string>` column
  added to the quarantine with the codes of the rules that rejected each row, in rule
  order. It appears on the `invalid` sink **only**: on `valid` it would be empty by
  definition, the main outputs receive the whole DataFrame, and the report has one row
  per rule rather than per row. `annotate` anywhere else is a config error that says
  why. The column comes from the same predicates the split already computes, so it
  costs no extra pass over the data.
- **`validations.outputs.invalid.rules`** — a list of rule codes that scopes a
  quarantine sink to those rules alone. Absent means every row-level rule, exactly as
  before. This is what makes a per-severity quarantine possible: route the rows that
  break the critical rules to one table and keep the rest out of it. Sinks that share a
  scope share a single split, so the common case still runs one split per pipeline.
- A code in `rules` that no rule declares, and a `columns` projection that drops the
  `annotate` column, are both logged as warnings — each would otherwise write a
  quarantine that is silently empty or silently missing its reason.

### Changed

- **Validations no longer recompute the pipeline once per rule.** Each rule fires its
  own Spark action (`not_null` one per column, `unique` two), and without a cached
  DataFrame every one of them re-read the source and re-applied every transformation.
  The frame is now materialised before the rules run and released after the writes.
  Measured on 2M rows with 4 rules: **13.0s → 5.2s** for validations plus the write,
  cache warm-up included. Opt out with `"validations": { "cache": false }` when the
  frame is too large for the executor's memory and disk.
- **`rules` is refused on `outputs.valid`.** The scope belongs to the *split*, which is
  one operation: if `invalid` looked at one rule and `valid` at all of them, the two
  would stop partitioning the input and a row breaking only some other rule would land
  in neither. `valid` is the exact complement of the quarantine.
- **Requires `sparquet-cola>=0.2.0`** (was `>=0.1.0`): `BaseCheck.code()` and
  `Cola.split(annotate=…, only=…)` do not exist in 0.1.x.

## [0.4.0] — 2026-08-18

### Changed

- **Log messages and their context keys are now English**, so a library with English
  docs no longer prints Portuguese. `Pipeline iniciado` → `Pipeline started`,
  `Leitura concluida` → `Input read`, `Transformacao pulada` → `Transformation
  skipped`, `Pipeline concluido` → `Pipeline finished`, and the structured fields
  followed (`linhas` → `rows`, `formato` → `format`, `linhas_escritas` →
  `rows_written`, …). `PipelineResult.summary()` and the `sparquet` CLI help are
  English too.
  **Breaking for anything that parses the logs** — that is the whole reason this is a
  minor bump and not a patch. Sparquet Studio was updated in the same change.

- **The quality report is worth reading now.** It is written as a **single file**
  (`coalesce(1)`) — one rule is one row, so the default parallelism was producing a
  directory of part files that held nothing but a header — and it carries three new
  columns that answer the questions the old one could not: `target` (the column(s) the
  rule checked), `rule_params` (what was actually asserted — `min`/`max`, `pattern`,
  `metric`, `must_be`…) and `rows_read` (a denominator for `failed_count`). Before, a
  failed `range` told you nothing about which column or which bounds unless you went
  back to the pipeline JSON, and a *passing* rule recorded nothing at all.
  `rows_read` is deliberately the **read** count, not the validated one: counting the
  validated DataFrame would add a Spark action to every run, and with a `filter`
  upstream the two differ — hence the explicit name and no derived percentage.


- Studio: one node per validation rule, with the quality report and the two
  quarantine sinks as side outputs of a main chain that keeps every row; naming and
  interface passes over the canvas, palette and command palette.

### Fixed

- **A local run no longer crashes with `Python worker exited unexpectedly (crashed)`**
  when the interpreter on `PATH` differs from the one running the driver — a venv
  started by absolute path is enough to trigger it. With a `local*` master the session
  manager now pins `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to `sys.executable` via
  `setdefault`, so an explicit choice is never overridden. Scoped to a local master on
  purpose: on a cluster the executor runs on another host, where the driver's
  interpreter path may not exist, and the variable belongs to the platform.
  The symptom was misleading — pure JVM stages (CSV → Parquet) need no Python worker,
  so a job would run fine until the first stage that does, usually
  `validations.report`.

### Removed

- `main.py` at the repository root. It was a byte-for-byte duplicate of
  `sparquet/cli.py`, which is the real entry point (`[project.scripts]` installs it
  as the `sparquet` command; `python -m sparquet.cli` also works).


- The `website/` folder. The landing page and the full documentation moved to the
  separate [sparquet-web](https://github.com/VictorPasqualini/sparquet-web)
  repository, published at <https://sparquet.dev>.

### Added

- `tests/test_examples.py` — every `examples/*.json` must parse as a valid
  `PipelineConfig` and use only registered transformation types. Discovered, not
  listed, so a new example is covered the moment it lands.
- The CI test step now **discovers** `tests/**/test_*.py` instead of naming files, so
  a new test cannot silently stay out of coverage.


- Community files for the open-source release: `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, `SECURITY.md`, this changelog, issue templates and a pull
  request template.

## [0.3.1] — 2026-08-17

### Added

- **Step markers in the structured logs.** `Pipeline` and `TransformationEngine` emit
  `step=True` together with `scope` (`input` / `transformation` / `output`), `index`
  and `total`. Purely additive: `TransformationEngine.apply(..., top_level=False)`
  keeps the previous behaviour, so nested chains (`with_transformations`, per-output
  `transformations`) are unchanged. This is what feeds per-step status on the Studio
  canvas.

### Changed

- On the main (top-level) transformation chain, the log line `Applying transformation`
  became the pair `Transformation started` / `Transformation applied`. Nested chains
  still log `Applying transformation`. Anything that greps the logs for the old
  message needs updating.
- Studio vocabulary settled as **Workflow** (container) › **Job** (one pipeline JSON)
  › **Pipeline** (an ordered set of Jobs run in sequence). The framework API did not
  change: one JSON is still a *pipeline*, executed by the `Pipeline` class.
- Studio runner: `POST /run/flow/stream` runs several Jobs in one shared SparkSession,
  with per-stage status, logs and `stop_on_error`.

## [0.3.0] — 2026-08-16

First release under the name **`sparquet`**.

### Added

- **File formats**: `json`, `orc`, `avro`, `xml`, `hudi` (upsert via `hoodie.*`
  options) and `binary` (read-only, `binaryFile`).
- **Databases and warehouses**: JDBC (`postgresql`, `mysql`, `mariadb`, `sqlserver`,
  `oracle`), `bigquery`, `snowflake`, `redshift`.
- **NoSQL and search**: `mongodb`, `documentdb`, `dynamodb`, `cassandra` (also serves
  ScyllaDB, same connector), `elasticsearch`, and `opensearch` as a connector of its
  own with `opensearch.*` options.
- **Kafka batch read** (`kafka` was write-only before), including MSK via SASL/IAM.
- **SODA-style validations**: `check` (a metric compared to a warn/fail threshold,
  including `freshness` and named formats such as `email`, `cpf`, `cnpj`) and `schema`
  (required/forbidden columns and column types). `ValidationResult` gained `severity`,
  `metric_value` and `check_name`; the validation report carries them; `warn` severity
  never aborts the run.
- **`sql` validation, `failed_rows` mode** - a query that returns the offending rows
  instead of a boolean invariant, with an optional `output` to persist them.
- **Quarantine**: `validations.outputs` with `valid` / `invalid` sinks, routing rows
  aside from the main output.
- **`broadcast` on `join`** - map-side join without a shuffle.
- **`PipelineResult.output_metrics`** and a row count per destination (`rows_written`
  is their sum).
- **`input_view`** on `fw.run(...)`: registers and caches the input as a temp view, so
  a self-join or a `sql` step does not re-read the source. The `view` format gained
  `options.scope` (`session` / `global`).
- **CI/CD**: `.github/workflows/ci.yml` (unit tests on push/PR over Python 3.9, 3.11
  and 3.12) and `.github/workflows/publish.yml` (tests, build + `twine check`, then
  publish: PyPI on a published release, TestPyPI on a manual run), using PyPI Trusted
  Publishing (OIDC), so no API token lives in the repository.
- **Sparquet Studio** (`sparquet-studio/`): the visual editor for the pipeline JSON,
  with its own local FastAPI runner and a catalog that describes every transformation,
  format and validator.

### Changed

- **The data-quality engine is a separate package.** `sparquet_cola` moved out of this
  repository into [`sparquet-cola`](https://github.com/VictorPasqualini/sparquet-cola)
  and is declared as a dependency (`sparquet-cola>=0.1.0`, no upper cap: the library is
  kept backwards compatible). The JSON block is still `validations`, and
  `sparquet.validation.*` remain compatibility shims that re-export it under the
  historical names (`BaseValidator`, `ValidationResult`, `*Validator`).
- **Package and entry point renamed**: distribution `spark-framework` to `sparquet`,
  import `spark_framework` to `sparquet`, class `SparkFramework` to `Sparquet`, console
  script `spark-framework` to `sparquet`.
- **License**: MIT to **Apache 2.0**, with a `NOTICE` file.
- Validation type `custom_sql` renamed to `sql`.
- The Delta path-vs-table heuristic is scheme-agnostic: `s3a://`, `abfs://` and friends
  are treated as paths, not only `s3://`.

### Removed

- `input_view_scope` - the scope moved inside `input_view`, which now takes either a
  string (`"orders"`) or `{"name": "orders", "type": "session" | "global"}`.
- The `add_column` alias for `with_column`.
- `sparquet_cola/` from this repository and from the packaging: the wheel ships only
  `sparquet` and `sparquet.*`, and the engine comes from PyPI.

## [0.2.3] — 2026-07-06

Published as `spark-framework`.

### Added

- `debug` accepts its own `transformations`, applied to a throwaway DataFrame just for
  that inspection. The DataFrame of the pipeline is never affected.

## [0.2.1] — 2026-07-05

Published as `spark-framework`. (`0.2.2` was never released.)

### Added

- `checkpoint` (`localCheckpoint` / `checkpoint`, `eager`) to materialize a DataFrame
  and truncate its logical plan after heavy joins.
- `distinct`, and `stop_if_empty` for ending a run gracefully with `skipped=True`.
- `collect` plus runtime variables `{{var}}`: collect the distinct values of a column
  and push them into a later read as a literal `IN (...)`, for predicate pushdown and
  Delta data skipping.
- `struct` accepts dot-path keys (`"data.nc.issuerName"`), auto-nesting them.
- `group_by` with full SQL aggregation expressions and an optional `pivot`.
- `join` accepts `with_transformations` and the `l` / `r` aliases; merge writes use `T`
  (target) / `S` (source).
- Template parameters `{param}`, substituted in the raw JSON before parsing, with SQL
  formatting for lists and booleans, and `skip_if_false` to switch a whole step off.
  This replaced the earlier `filter_in` / `runtime_params` mechanism.
- `$include` in `transformations`, expanding a JSON fragment inline (one level).
- `fw.run(input_df=..., columns=...)`, to start from an existing DataFrame and inject
  literal columns.
- The `debug` transformation (`count`, `print_schema`, `show`, `explain`, `columns`,
  `dtypes`).
- `select` accepts full SQL expressions with aliases.
- A GitHub Actions workflow for publishing to PyPI, and
  [docs/DEPLOY_PYPI.md](docs/DEPLOY_PYPI.md).

### Fixed

- `debug` with `show` uses `df.show()` in every environment; the Databricks `display()`
  call was removed, since it only works when called from a notebook cell.

## [0.2.0] — 2026-05-10

Published as `spark-framework`.

### Added

- Delta Lake reader/writer (MERGE, time travel) and `txt`; Databricks support in the
  session manager.
- Kafka writer and the `view` reader/writer (Spark temp views).
- `with_column`.
- `fw.run()` orchestration as the library entry point.

## [0.1.0] — 2026-05-08

Published as `spark-framework`.

### Added

- First version of the framework: a JSON pipeline (`input`, `transformations`,
  `validations`, `output`), Parquet and Iceberg readers/writers, the transformation and
  validation engines, the extension registries (`register_*`), `PipelineResult`, the
  structured JSON logger and the CLI.

[Unreleased]: https://github.com/VictorPasqualini/sparquet/compare/v0.11.0...HEAD
[0.11.0]: https://github.com/VictorPasqualini/sparquet/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/VictorPasqualini/sparquet/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/VictorPasqualini/sparquet/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/VictorPasqualini/sparquet/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/VictorPasqualini/sparquet/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/VictorPasqualini/sparquet/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/VictorPasqualini/sparquet/compare/v0.3.1...v0.5.0
[0.3.1]: https://github.com/VictorPasqualini/sparquet/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/VictorPasqualini/sparquet/releases/tag/v0.3.0
