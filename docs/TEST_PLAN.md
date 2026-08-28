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

Updated 2026-08-27, against `sparquet` 0.6.0 / `sparquet-cola` 0.3.0.

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
| Readers | 27 | 14 (option building) + 6 read back what was written |
| Writers | 26 | 14 (option building) + 6 written then read back |
| Transformations | 19 | **0** |
| Validation rule types | 21 | 6 against real data |
| Pipeline orchestration | — | **0** |
| Studio | — | 455 tests + 19 smoke checks |
| Runner service (`server/`) | — | 180 `unittest` tests (history 48, auth 64, credits 42, run scope 16, workspace 10) |

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

What that does **not** catch: a wrong Spark format name, a write mode that behaves
differently than documented. The schema round-trip *is* now covered for the six
formats that need nothing but Spark — see
[`tests/test_formats_roundtrip_spark.py`](../tests/test_formats_roundtrip_spark.py).

### File and table formats

| Format | Read | Write | Status | What is missing |
|---|:---:|:---:|:---:|---|
| `parquet` | ✓ | ✓ | ◐ | [round-trip pinned](../tests/test_formats_roundtrip_spark.py): schema and rows survive, `partition_by` gives the column back. Missing: `append`/`error`/`ignore` write modes |
| `csv` | ✓ | ✓ | ◐ | [dialect pinned](../tests/test_csv_dialect_spark.py) (RFC 4180 quoting, both ways) and [round-trip pinned](../tests/test_formats_roundtrip_spark.py) with `header`. Missing: `sep`/`encoding` defaults, `inferSchema` off |
| `delta` | ✓ | ✓ | ◐ | [path-vs-table heuristic pinned](../tests/io/test_delta_path.py). Missing: `mode: merge` (keys + extra condition), time travel options |
| `iceberg` | ✓ | ✓ | ⬜ | `MERGE INTO`, catalog identifier handling |
| `txt` | ✓ | ✓ | ✅ | [round-trip pinned](../tests/test_formats_roundtrip_spark.py): single `value` column, lines back in the order written |
| `view` | ✓ | ✓ | ◐ | [round-trip pinned](../tests/test_formats_roundtrip_spark.py): session and `global` scope, the latter readable with and without the `global_temp.` prefix. Missing: the auto-cache being what stops a re-read |
| `json` | ✓ | ✓ | ◐ | [round-trip pinned](../tests/test_formats_roundtrip_spark.py): values and columns back whole, a null still null and not `""`. Missing: `multiLine` |
| `orc` | ✓ | ✓ | ✅ | [round-trip pinned](../tests/test_formats_roundtrip_spark.py): schema and rows identical |
| `avro` | ✓ | ✓ | ⬜ | format string and `avroSchema` (jar not needed to assert the builder call) |
| `xml` | ✓ | ✓ | ⬜ | `rowTag` required-ness |
| `binary` | ✓ | — | ⬜ | read-only refusal on write, the four columns it yields |
| `hudi` | ✓ | ✓ | ⬜ | `hoodie.*` options passthrough |

Local Spark covers `parquet`, `csv`, `json`, `orc`, `txt` and `view` with no extra
dependency, and those six now have a real write-then-read test — 11 cases in
[`tests/test_formats_roundtrip_spark.py`](../tests/test_formats_roundtrip_spark.py).
The fixture is built with `spark.sql(... VALUES ...)` rather than `createDataFrame`
on purpose: the second one starts a Python worker, and on a local master with a
mismatched `PYSPARK_PYTHON` the whole file would fail for a reason that has nothing
to do with file formats.

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
| `SPARK_HOME` alignment | [`tests/test_spark_home_alignment.py`](../tests/test_spark_home_alignment.py) — 10 tests: a stale `SPARK_HOME` declaring another version is rewritten to the imported pyspark and the divergence is reported; a home that agrees, one that declares no version, an unset one and any non-local master are left alone |
| Structured logger | one JSON object per line, with the step markers the Studio run panel reads (`scope`, `index`, `total`, `step`) |

---

## 7. Studio

The Studio is the best-covered part of the project (455 unit tests, 19 smoke checks in
a real Chrome, and 180 `unittest` tests on the runner: 48 on the history database, 64 on
identity, 42 on credits, 16 on the scope of the execution routes and 10 on the
workspace). What is pinned, and what is not:

| Area | Status | Notes |
|---|:---:|---|
| Compiler round-trip | ✅ | `compileGraph` ∘ `pipelineToGraph` over **every** conf in `examples/` — a new example is covered the moment it is added |
| Failure codes (`ruleCode`) | ✅ | the exact strings the library derives, including one per `target`; this is a cross-language contract and both sides are pinned |
| Linter | ✅ | 64 tests, including catalog field validation |
| Catalog | ✅ | every transformation and format entry has fields/defaults consistent with the schema |
| Storage migrations | ✅ | every hop, each one identity when there is nothing to upgrade, and idempotent |
| Runner client | ✅ | 36 tests over the HTTP/SSE contract, including the Studio ids forwarded for execution history, the `stage_skipped` event, `cancelRun` (the ids the `start` event carries, the `409` on a run that already ended) and a cancelled run read as cancelled rather than as a failure |
| Execution history | ✅ | server: 29 `unittest` tests over `ExecutionRepository`/`StepTracker`/`lineage_of` (success, mid-step failure, skip propagation, listing/ordering and its `job_id` filter across nested job runs, FK-enforced no-orphan-`StepRun`, the run log: arrival order, one `seq` sequence across batches, no leaking between job runs; and the run-detail fields: `run_as`/`launched` recorded and defaulted to `manual`, `lineage` stored and read back whole, the migration that adds all three to a DB written before they existed) · 7 of them on `lineage_of` itself, which derives inputs/outputs from the submitted config: join as a second input, the three quality sinks kept apart by role, list-valued inputs/outputs, the Kafka topic inside `options`, the JDBC `dbtable`, and anything unrecognisable read as no lineage; client: 18 tests over `lib/runner/history.ts`'s mapping, the 404→`null` contract, the `cancelled` status, `getJobRunLogs`'s paging by `seq`, the lineage JSON string parsed (and an unparseable one read as `null`), and `run_as`/`launched`/`lineage` all left `null` on a run recorded before they existed |
| History on the canvas | ✅ | 13 tests on `stepNodes.ts` (which box a persisted step belongs to, role-keyed `validation_sink` included, worst-status-wins, steps with no box surfaced) · 8 on `stageRuns.ts` (stages matched to job runs by `job_id`, never by position) · 12 on `runView.ts`, including the regression that guards the drill-down: a stage's pin request survives a load that is abandoned before it paints, so the reopened Job shows the execution the stage ran and not its own latest. `allowLatest` is pinned on both sides — off, opening a Job asks the runner nothing and paints nothing, but a run named by id is still honoured; this is what stops a Job or Pipeline from repainting its last execution just because it was opened |
| Catalog (what exists) | ✅ | 9 of the server tests cover the half of the database that holds the records rather than the runs: the foreign keys actually enforced, a Job attached to its Workflow, a Job saved before its Workflow still landing under it, `pipeline_stage` carrying the Job↔Pipeline relation in order, the same Job twice in one Pipeline (which is why the junction is keyed by stage), a re-save replacing the stages, a soft-deleted Workflow hiding its children, and a deleted Job whose executions stay readable |
| Workspace (the files) | ✅ | 10 `unittest` tests on `server/workspace.py`: the readable tree named after the Workflow, a Job file holding the **compiled** pipeline and not the Studio record, a record surviving a round trip, a rename moving the file instead of leaving a second copy, renaming a Workflow moving everything under it, the snapshot, delete removing both files, `.studio/meta.json` surviving a reopen, and the two refusals (unknown kind, an id that could escape the root) · client: 12 tests on `lib/storage/remote.ts` — reads served from the boot snapshot, meta keys mapped back, the runner being absent or too old read as "unavailable" rather than as an error, the token on every call, a refused write kept out of the cache, a Job compiled on the way out, meta routed to its own endpoint, and the migration backup deliberately never written to disk |
| Config version | ✅ | server: 10 tests on `history.config_version()` and the column it feeds — formatting and key order do not change the fingerprint, an edited step does, list order counts, the JSON reads back whole, two runs of an edited Job are told apart, an oversized configuration is still identified by its hash, a run recorded before the column reads as unknown rather than as wrong · client: 3 tests on `getJobRunConfig` (the version read back, the hash kept when the JSON was not, `404` → `null`) |
| Identity and permissions | ✅ | 64 `unittest` tests on `server/auth.py`: the policy evaluator (default deny, service-scoped wildcards, `deny` beating `allow` in either order, resource scoping that does not cross the kind, what each built-in role promises, garbage granting nothing), passwords (never stored as themselves, salted, an unreadable hash is not a way in, too-short refused), and the store (login, a wrong password and an unknown user failing identically, a disabled account, sessions unreadable in the database file, logout, expiry, disabling and password changes cutting live sessions, and the last-administrator guard on all three of demote/disable/delete) · 11 of them on password recovery: a code sets a new password, works exactly once, issuing a second one kills the first, an expired code and an unknown code both refused, a disabled account cannot be recovered into, the code is not stored as itself (asserted against the raw database bytes), redeeming ends the account's open sessions, a password below the minimum is refused **without** burning the code, no code is minted for a user that does not exist, and `find_user` is case-insensitive · 9 of them on **custom roles**: a role written in the interface grants what it says, a built-in name cannot be taken over, a built-in role refuses both edit and delete, a duplicate name is refused, editing a role changes what its holders may do without them logging in again, a role still held by a user *or by a team* cannot be deleted, a malformed statement is refused at write time rather than ignored at evaluation time, and a custom role survives a restart still flagged `custom` (which is what keeps the start-up rewrite of the built-ins from eating it) · 11 on **teams**: everybody lands in the default team, a team can be addressed by name, a duplicate name is refused, a team's roles are *added* to the ones a person holds, a personal `deny` still beats a team `allow`, moving somebody changes who pays from then on, deleting a team moves its members back into the default one, the default team cannot be deleted, a team's roles can be changed after the fact and take effect on the next request, and an unknown team is refused · client: 3 tests on `lib/auth/client.ts` (the minted code mapped, the redeem body, and the refusal surfaced as-is — it never says which half was wrong) |
| Execution credits | ✅ | 42 `unittest` tests on `server/credits.py`, in six groups. What a run costs: a `local` master is free, `yarn`/`spark://`/`k8s://` cost, `spark.remote` wins over a local master sitting next to it, a master nested in `configs` is found, a runner inside Databricks charges regardless of the master, and a malformed `spark` block does not crash the charge. **What a run is charged for**: one credit per successful write, so a Job writing three destinations costs three and **a run that failed before writing costs nothing**; the count comes from `output_metrics`, and a run with no writes at all writes no ledger row. **The free monthly allowance**: 40 writes a month are waived before any granted balance is touched, the period is `YYYY-MM` in UTC so it resets by itself, what is left does not accumulate into the next month, a run that spans the allowance pays only for the part above it, and while the runner only meters, the allowance is not burned either. Enforcement: a team that cannot pay for a single write is refused at admission with 402, a charge larger than the balance does not go negative but records a `shortfall`, and two teams never spend each other's balance. Accounts: created lazily on first sight, `SPARQUET_STUDIO_CREDITS_INITIAL` honoured, a rename keeps the same account, a negative grant takes credits back, a grant that would go below zero is refused, a grant of zero is refused. Ledger: order, the fields a run entry carries (`writes`, `free_amount`, `shortfall`, `job_run_id`, `pipeline_run_id`), `applied` distinguishing metered from charged. Principals: **the team**, not the person — moving somebody to another team changes who pays from then on and leaves the entries already written where they are — or the literal `token` account on a runner with no users · client: 9 tests on `lib/runner/credits.ts` — the balance and enforcement flags mapped, a metering-only runner read as one credit per write, the accounts list (and a malformed answer read as empty), the ledger's limit in the query string, a metered row kept distinguishable, the grant body including a null note, and `isOutOfCredits` true for 402 and for nothing else |
| Scope of the execution routes | ✅ | 16 `unittest` tests on the two helpers `/run`, `/run/stream` and `/run/flow/stream` authorize with (`server/test_run_scope.py`). Which resources a run names: the Job, the Pipeline and the Workflow it belongs to, every Job of a flow, and `*` for a Job that was never saved — that last one is the point, because an unsaved Job must not slip through under a resource nobody granted. And the decision itself: an `allow` on the Workflow covers the Job inside it, a `deny` on the Job is **not** widened away by a broader grant, a token-only runner runs everything, and a refusal says who was refused and with which roles. The check lives inside the handler rather than in the `requires(...)` dependency because the dependency runs before the body is read, and the body is where the target is |
| Error de-duplication | ✅ | 5 tests on `lib/runner/errorText.ts`: the same failure quoted at step, job and run level recognised as one, wrapping ignored, two different failures kept apart, and a missing message never treated as a match (which would silence the level that has one) |
| Pipelines (multi-Job) | ✅ | stage order from the links, cycles refused, unlinked stages last |
| AI proposal parsing | ✅ | 28 tests over the streaming client and the parser |
| Canvas / nodes / inspector | ◐ | node previews, handles and connection guards are pinned; the interactive canvas is covered by the smoke script rather than unit tests |
| Workspace tabs and the Runs browser | ⬜ | the Flow/JSON/Runs switch, the runs table and the run-detail dialog (its **Details** tab: job id, job run id, run as, launched, timestamps, duration, status, lineage) are React with no unit test. What they read is pinned — `runView.ts`, `history.ts`, `lineage_of` above — so the gap is the rendering, not the data |
| The `targets` field UI | ⬜ | the JSON widget renders and its `validate` mirrors the library's refusals — no test asserts the inspector shows those messages |
| Runner service (`server/main.py`) | ◐ | what a run authorizes is now pinned (see *Scope of the execution routes*), but there is still **no test over HTTP**: token auth, the origin allow-list, the SSE event sequence, the per-stage status of `/run/flow/stream`, and the `requires(...)` dependency that maps each route to an action (the evaluator behind it is pinned — see *Identity and permissions* — but no test asserts that `PUT /workspace` is the route demanding `workspace:Write`). The execution-history side-effects of these endpoints (`server/history.py`) are covered — see the row above. FastAPI's `TestClient` would cover all of it without Spark, and the only thing in the way is the dependency: starlette answers `RuntimeError: The starlette.testclient module requires the httpx2 package to be installed.`, and `httpx` is not in this environment — which is why `test_run_scope.py` calls the helpers directly instead |

The runner service is the gap that matters most here — it is the only component that
executes arbitrary user configs, and its security posture (localhost binding, token,
origin allow-list) is exactly the kind of thing that breaks silently.

---

## 8. Conventions for a new test

- Name it `test_*.py`, put `unittest.main(verbosity=2)` at the bottom, and make sure
  `PYTHONPATH=. python tests/<file>.py` passes. CI runs each file that way.
- Needs Spark? Copy the `setUpClass` from
  [`tests/test_csv_dialect_spark.py`](../tests/test_csv_dialect_spark.py): build a
  `local[1]` session, probe it, and raise `unittest.SkipTest` if Java is missing. A
  one-row `createDataFrame` is the strictest probe, because it also proves the Python
  worker starts; use `spark.sql("SELECT 1").count()` instead when the file stays on the
  JVM side, as
  [`tests/test_formats_roundtrip_spark.py`](../tests/test_formats_roundtrip_spark.py)
  does — otherwise the probe fails for a reason the file never depends on. Name the file `*_spark.py` so the intent is
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
2. ~~**The six native file formats round-trip**~~ — done:
   [`tests/test_formats_roundtrip_spark.py`](../tests/test_formats_roundtrip_spark.py).
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
