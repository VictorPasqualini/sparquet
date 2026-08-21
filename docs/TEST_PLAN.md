# Test plan — what is covered, what is not, and how to close the gap

Sparquet is a configuration language: a user writes JSON and the framework promises to
turn it into Spark work. Every reader, writer, transformation and validation is a
public surface, so "it worked once on my machine" is not coverage — the promise has to
be pinned by a test that fails when the behavior drifts.

This document is the inventory of that promise. The lists below are taken from the
**registries themselves** (`TransformationEngine._registry`, `ReaderFactory` /
`WriterFactory`, `Cola().available`), so a capability that exists in code but is
missing here means this file went stale — not that the capability is untested by
accident.

Status marks:

| Mark | Meaning |
|:---:|---|
| ✅ | pinned by a test that would fail if the behavior changed |
| ◐ | partially pinned — some property is asserted, the main one is not |
| ⬜ | no test at all: nothing would catch a regression |

Updated 2026-08-21, against `sparquet` 0.6.0 / `sparquet-cola` 0.3.0.

---

## 1. How the suites run today

Three layers, deliberately separated by what they need to run:

| Layer | Where | Needs | Command |
|---|---|---|---|
| **Pure** — config parsing, option building, pure helpers | `tests/**/test_*.py` | nothing but Python | `PYTHONPATH=. python tests/io/test_connectors.py` |
| **Local Spark** — real DataFrames, one `local[1]` session | `tests/*_spark.py` | Java + pyspark | `PYTHONPATH=. python tests/test_csv_dialect_spark.py` |
| **Studio** — compiler, catalog, linter, storage, canvas | `sparquet-studio/` | Node 18.18+ | `npm run typecheck && npm run test && npm run lint && npm run smoke` |

Everything at once, the way CI does it:

```bash
for f in $(find tests -name 'test_*.py' | sort); do PYTHONPATH=. python "$f" || echo "FAILED: $f"; done
```

Two rules make this work without a heavier harness:

- **Every file is a standalone script** (`unittest.main()` at the bottom), because CI
  runs each one directly. A file that only passes under pytest, or only when imported
  from the repository root, breaks the gate.
- **A file that needs Spark skips itself** when there is no Java, instead of failing —
  see the `setUpClass` in [`tests/test_csv_dialect_spark.py`](../tests/test_csv_dialect_spark.py).
  That is what lets the same suite run on a laptop with Spark and on a CI runner
  without it.

The data-quality engine lives in its own repository ([`sparquet-cola`](https://github.com/VictorPasqualini/sparquet-cola))
with the same two layers: `tests/test_cola_lib.py` (25 pure) and
`tests/test_split_spark.py` (14 with Spark).

---

## 2. The short version

| Axis | Surface | Pinned |
|---|---:|---:|
| Readers | 27 | 14 (option building only) |
| Writers | 26 | 14 (option building only) |
| Transformations | 19 | **0** |
| Validation rule types | 21 | 6 against real data |
| Pipeline orchestration | — | **0** |
| Studio | — | 351 tests + 19 smoke checks |

Two findings worth stating plainly, because they are the reason this document exists:

1. **No transformation has a behavioral test.** `tests/test_examples.py` asserts that
   every `type` in the shipped examples is a *known* type — nothing asserts that
   `struct` nests a dot-path, that `group_by` applies a pivot, or that `checkpoint`
   truncates the lineage. Nineteen transformations, zero assertions on the DataFrame
   that comes out.
2. **Fifteen metric rules have never run against a DataFrame.** `avg`, `min`, `max`,
   `sum`, `stddev`, `distinct_count`, `duplicate_*`, `missing_*`, `invalid_*` and
   `freshness` are covered for *code derivation* and *threshold parsing*, but the
   Spark suite only exercises `not_null`, `unique`, `range`, `regex` and `row_count`.
   A wrong aggregate expression would ship green.

Neither is a call to write two hundred tests. Both are a call to write the *first*
test in each family — the harness is the expensive part, and it already exists.

---

## 3. Sources and targets

The connector tests assert what a connector *asks Spark for*: the format string, the
options dict, and how `path` maps onto a table / collection / index / topic. They use
a fake builder instead of a real service, which is the right trade — the option
mapping is where the bugs are, and it is the part that can be checked in a second
without a cluster.

What that does **not** catch: a wrong Spark format name, a schema round-trip that
loses a type, a write mode that behaves differently than documented.

### File and table formats

| Format | Read | Write | Status | What is missing |
|---|:---:|:---:|:---:|---|
| `parquet` | ✓ | ✓ | ⬜ | round-trip: write then read, `partition_by`, all write modes |
| `csv` | ✓ | ✓ | ◐ | [dialect pinned](../tests/test_csv_dialect_spark.py) (RFC 4180 quoting, both ways). Missing: `sep`/`encoding`/`header` defaults, `inferSchema` off |
| `delta` | ✓ | ✓ | ◐ | [path-vs-table heuristic pinned](../tests/io/test_delta_path.py). Missing: `mode: merge` (keys + extra condition), time travel options |
| `iceberg` | ✓ | ✓ | ⬜ | `MERGE INTO`, catalog identifier handling |
| `txt` | ✓ | ✓ | ⬜ | single `value` column on read and write |
| `view` | ✓ | ✓ | ⬜ | session vs `global` scope, the auto-cache |
| `json` | ✓ | ✓ | ⬜ | JSON Lines default, `multiLine` |
| `orc` | ✓ | ✓ | ⬜ | round-trip |
| `avro` | ✓ | ✓ | ⬜ | format string and `avroSchema` (jar not needed to assert the builder call) |
| `xml` | ✓ | ✓ | ⬜ | `rowTag` required-ness |
| `binary` | ✓ | — | ⬜ | read-only refusal on write, the four columns it yields |
| `hudi` | ✓ | ✓ | ⬜ | `hoodie.*` options passthrough |

Local Spark covers `parquet`, `csv`, `json`, `orc`, `txt` and `view` with no extra
dependency — those six are the cheapest wins on this table.

### Databases, warehouses and streams

| Format | Status | Pinned by |
|---|:---:|---|
| `postgresql`, `mysql`, `mariadb`, `sqlserver`, `oracle` | ✅ | URL built from `host`+`database`, explicit `url` precedence, `query` over `dbtable`, write mode, missing-URL error |
| `bigquery` | ✅ | load path vs write table, `query` option |
| `snowflake` | ✅ | `dbtable` and format name |
| `redshift` | ✅ | `dbtable` (a missing `tempdir` is not asserted) |
| `mongodb`, `documentdb` | ✅ | collection from `path`, shared connector |
| `dynamodb` | ✅ | `tableName` |
| `cassandra` | ✅ | `keyspace.table` split, table-only + `keyspace` option |
| `elasticsearch` | ✅ | resource as load path |
| `opensearch` | ⬜ | **its own connector** (`opensearch.*` options), never asserted — the easiest gap here to close, next to the `elasticsearch` case it was split from |
| `kafka` | ✅ | `subscribe` from `path`, bootstrap alias, batch defaults, missing-bootstrap error |

An integration tier against containers (Postgres, MySQL, Mongo, Cassandra,
Elasticsearch, Kafka all have official images) is worth having eventually, but it
belongs behind an opt-in env var — never in the default `python tests/...` run, which
must stay a second long and offline.

---

## 4. Transformations

None of the nineteen has a behavioral test. The unit to test is small and pure enough
that one Spark session serves the whole file: build a DataFrame with
`createDataFrame`, run the transformation through `TransformationEngine`, assert on
`collect()` and on `df.columns`.

| Transformation | Priority | The property that needs pinning |
|---|:---:|---|
| `select` | high | plain names **and** SQL expressions with an alias (`to_json(payload) AS value`) |
| `with_column` | high | `column`+`expression`, the `columns` map, and that a later entry sees an earlier one |
| `struct` | high | dot-path auto-nesting, nested maps, and the two mixed |
| `join` | high | `on` as string / list / SQL with `l.`/`r.`, `broadcast`, `with_transformations` applied to the right side only |
| `group_by` | high | SQL agg expressions with aliases, `pivot` with and without an explicit value list |
| `sql` | high | `view_name` registration and that the view does not leak |
| `filter`, `drop`, `rename`, `cast` | medium | the obvious mapping, plus `cast` type aliases |
| `drop_duplicates`, `distinct` | medium | subset vs all columns |
| `fill_na`, `sort` | medium | `columns` subset, `ascending` |
| `collect` + `{{var}}` | high | the runtime store: list of strings quoted, numbers bare, **empty list → `NULL`**, visibility inside a join's `with_transformations`, and the reset between runs |
| `stop_if_empty` | high | `result.skipped is True`, `success is True`, nothing written, later steps not run |
| `checkpoint` | medium | `localCheckpoint` vs `checkpoint`, and that an invalid `method` is ignored **with a warning** rather than raising |
| `union` | medium | `allow_missing_columns` |
| `debug` | low | it returns the *original* df even when its own `transformations` filter it |
| `skip_if_false` | high | the three cases in the table in [CLAUDE.md](../CLAUDE.md): empty string skips, boolean expression skips when false, any other non-empty value runs |

---

## 5. Validations

The rule *engine* is `sparquet-cola` and is tested there. What belongs in this
repository is the **wiring**: how the config is parsed, how results reach the report,
and how rows reach the quarantine.

| Area | Status | Notes |
|---|:---:|---|
| Threshold DSL (`must_be` / `warn`) | ✅ | operators, `between`, `%` and duration suffixes, bare number, `None`, empty → raises |
| Severity (`pass`/`warn`/`fail`) | ✅ | a `warn` never aborts, even under `on_failure: "fail"` |
| Validity formats (`valid_format` names, type aliases) | ✅ | named regexes present, `schema` aliases match |
| Quarantine config (`rules`, `annotate`) | ✅ | parsed on `invalid`; refused on `valid`, on the report and on main outputs; codes trimmed; shapes validated |
| `targets` expansion | ✅ | pure expansion + idempotence in cola; [example 08 end to end](../examples/08_validacao_multi_alvo.json); every code in an example's scope must exist ([`test_examples.py`](../tests/test_examples.py)) |
| `not_null`, `unique`, `range`, `regex`, `row_count` | ✅ | real Spark in cola, including the `unique` window function and the annotated split |
| The 15 other metrics | ⬜ | **never executed against a DataFrame** — see §2 |
| `sql` rule: `query` mode | ◐ | parsed; the pass-when-true contract is not asserted |
| `sql` rule: `failed_rows` + per-rule `output` | ⬜ | that it writes exactly the offending rows |
| `on_failure` modes | ⬜ | `fail` aborts **before** any write and before the report; `warn`/`skip` continue and write |
| `validations.report` contents | ◐ | its CSV dialect is pinned; the columns (`target`, `rule_params`, `rows_read`, `severity`, `metric_value`) are not |
| Quarantine split at runtime | ◐ | pinned in cola (`split` + `annotate` + `only`); not through `Pipeline` |

First test to write here: one local-Spark file that runs a pipeline with one metric per
family (`missing_percent`, `duplicate_count`, `invalid_percent`, `avg`, `freshness`)
over a fixed 10-row CSV and asserts each `metric_value`. That single file closes the
biggest hole in the table.

---

## 6. Pipeline orchestration and the library API

Nothing in this section is pinned. It is the layer users touch first.

| Capability | The property that needs pinning |
|---|---|
| `apply_template` (`params`) | every row of the formatting table: `str`/`int`, `True` → `"true"`, `False` → `""`, list of strings → `'a', 'b'`, list of numbers, empty list → `""`, unmatched key stays literal |
| `resolve_includes` (`$include`) | single object and list, path relative to the main JSON, template applied before the parse |
| `fw.run(input_df=...)` | the `input` block is ignored and `result.output_df` is filled |
| `fw.run(columns=...)` | literal columns injected **before** the transformations |
| `fw.run(input_view=...)` | string form and `{"name","type"}`; a following `join`/`sql` sees the view without re-reading |
| `outputs` + `columns` | the projection is per destination and the main df is unchanged between writes |
| `outputs` + `transformations` | each destination starts from the same df; one destination's chain does not affect another's |
| `PipelineResult` | never raises: a failure lands in `.error`; `rows_read`/`rows_written`/`summary()` |
| `SparkContextManager` | environment detection (Databricks reuses the active session; local sets `PYSPARK_PYTHON` to `sys.executable`) |
| Structured logger | one JSON object per line, with the step markers the Studio run panel reads (`scope`, `index`, `total`, `step`) |

---

## 7. Studio

The Studio is the best-covered part of the project (351 unit tests, 19 smoke checks in
a real Chrome). What is pinned, and what is not:

| Area | Status | Notes |
|---|:---:|---|
| Compiler round-trip | ✅ | `compileGraph` ∘ `pipelineToGraph` over **every** conf in `examples/` — a new example is covered the moment it is added |
| Failure codes (`ruleCode`) | ✅ | the exact strings the library derives, including one per `target`; this is a cross-language contract and both sides are pinned |
| Linter | ✅ | 64 tests, including catalog field validation |
| Catalog | ✅ | every transformation and format entry has fields/defaults consistent with the schema |
| Storage migrations | ✅ | every hop, each one identity when there is nothing to upgrade, and idempotent |
| Runner client | ✅ | 27 tests over the HTTP/SSE contract |
| Pipelines (multi-Job) | ✅ | stage order from the links, cycles refused, unlinked stages last |
| AI proposal parsing | ✅ | 28 tests over the streaming client and the parser |
| Canvas / nodes / inspector | ◐ | node previews, handles and connection guards are pinned; the interactive canvas is covered by the smoke script rather than unit tests |
| The `targets` field UI | ⬜ | the JSON widget renders and its `validate` mirrors the library's refusals — no test asserts the inspector shows those messages |
| Runner service (`server/main.py`) | ⬜ | **no test at all**: token auth, the origin allow-list, the `403` on `/run`, the SSE event sequence, and `/run/flow/stream` per-stage status are only ever exercised by hand |

The runner service is the gap that matters most here — it is the only component that
executes arbitrary user configs, and its security posture (localhost binding, token,
origin allow-list) is exactly the kind of thing that breaks silently.

---

## 8. Conventions for a new test

- Name it `test_*.py`, put `unittest.main(verbosity=2)` at the bottom, and make sure
  `PYTHONPATH=. python tests/<file>.py` passes. CI runs each file that way.
- Needs Spark? Copy the `setUpClass` from
  [`tests/test_csv_dialect_spark.py`](../tests/test_csv_dialect_spark.py): build a
  `local[1]` session, probe it with a one-row `createDataFrame`, and raise
  `unittest.SkipTest` if Java is missing. Name the file `*_spark.py` so the intent is
  visible in the directory listing.
- Don't add a service dependency to the default run. An integration test that needs a
  container belongs behind an env var, skipped by default.
- A new connector goes into [`tests/io/test_connectors.py`](../tests/io/test_connectors.py)
  (format string, options built, `path` mapping). A new pure helper for validations
  goes into [`tests/validation/`](../tests/validation/).
- State the *why* in the docstring. A test whose name says what it does and whose
  docstring says which bug it prevents survives refactors; one that only asserts a
  value gets deleted by the next person.

---

## 9. Suggested order

Ranked by (risk it covers) ÷ (effort to write), highest first:

1. **Metrics against real data** — one local-Spark file, one metric per family. Fifteen
   implementations currently untested; a wrong aggregate ships green today.
2. **The six native file formats round-trip** — `parquet`, `csv`, `json`, `orc`, `txt`,
   `view`: write then read, assert schema and rows. No extra dependency.
3. **`on_failure` semantics** — that `fail` aborts before any write is a data-safety
   promise, and nothing checks it.
4. **The transformation families in the high-priority rows of §4** — `select`,
   `with_column`, `struct`, `join`, `group_by`, `sql`, plus `collect` + `{{var}}` and
   `stop_if_empty`.
5. **Template and `$include`** — pure, fast, and the formatting table in CLAUDE.md is a
   ready-made list of cases.
6. **Runner service** — FastAPI's `TestClient` covers auth, the origin allow-list and
   the SSE sequence without Spark (mock the pipeline run).
7. **`mode: merge`** for Delta and Iceberg — needs `delta-spark`, so it goes in a file
   that skips when the jar is absent.
8. **Container-backed integration** for the connectors, opt-in via env var.

Items 1–5 are all self-contained: each is one file, no harness work, and each one turns
a whole family of ⬜ into ✅.
