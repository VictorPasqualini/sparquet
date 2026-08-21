# Changelog

All notable changes to the **framework** (`sparquet` on PyPI) are recorded here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Notes on the history below:

- The version has a single source, `__version__` in `sparquet/__init__.py`; see
  [docs/DEPLOY_PYPI.md](docs/DEPLOY_PYPI.md) for the release procedure.
- Git tags start at `v0.3.0`. The entries for `0.2.3` and earlier were reconstructed
  from the commits that bumped `__version__`, so their dates are commit dates.
- Releases up to `0.2.3` were published under the old distribution name
  **`spark-framework`** (import `spark_framework`). `sparquet` is the name from
  `0.3.0` on.
- [Sparquet Studio](sparquet-studio/) and the data-quality engine
  [`sparquet-cola`](https://github.com/VictorPasqualini/sparquet-cola) carry their own
  version numbers. Studio changes appear here only when they touch the framework or
  the JSON contract.

## [Unreleased]

## [0.5.0] — 2026-08-20

### Added

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
- **`validations.outputs.{valid,invalid}.rules`** — a list of rule codes that scopes a
  quarantine sink to those rules alone. Absent means every row-level rule, exactly as
  before. This is what makes a per-severity quarantine possible: route the rows that
  break the critical rules to one table and keep the rest out of it. Sinks that share a
  scope share a single split, so the common case still runs one split per pipeline.
- A code in `rules` that no rule declares, and a `columns` projection that drops the
  `annotate` column, are both logged as warnings — each would otherwise write a
  quarantine that is silently empty or silently missing its reason.

### Changed

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

### Changed

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

### Added

- `tests/test_examples.py` — every `examples/*.json` must parse as a valid
  `PipelineConfig` and use only registered transformation types. Discovered, not
  listed, so a new example is covered the moment it lands.
- The CI test step now **discovers** `tests/**/test_*.py` instead of naming files, so
  a new test cannot silently stay out of coverage.

### Added

- Community files for the open-source release: `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, `SECURITY.md`, this changelog, issue templates and a pull
  request template.

### Changed

- Studio: one node per validation rule, with the quality report and the two
  quarantine sinks as side outputs of a main chain that keeps every row; naming and
  interface passes over the canvas, palette and command palette.

### Removed

- The `website/` folder. The landing page and the full documentation moved to the
  separate [sparquet-web](https://github.com/VictorPasqualini/sparquet-web)
  repository, published at <https://sparquet.dev>.

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

[Unreleased]: https://github.com/VictorPasqualini/sparquet/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/VictorPasqualini/sparquet/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/VictorPasqualini/sparquet/releases/tag/v0.3.0
