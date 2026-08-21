<div align="center">

# Sparquet

**Data engineering as JSON — with a canvas to design it and Spark to run it.**

A configuration-driven PySpark framework: describe a pipeline as a JSON document, run it anywhere Spark runs.
Then open the same document in **[Sparquet Studio](sparquet-studio/)**, a visual, AI-assisted editor for those pipelines.

[Two halves](#two-halves-of-one-idea) · [Install](#install) · [Pipeline in 30 seconds](#a-pipeline-in-30-seconds) · [What the language covers](#what-the-language-covers) · [Docs](#documentation) · [Contributing](#contributing)

![Sparquet Studio](sparquet-studio/docs/images/editor.png)

</div>

---

## Two halves of one idea

|  |  |
|---|---|
| **The framework** (`sparquet/`) | Reads a JSON pipeline and executes it on Spark: readers and writers for files (Parquet, Delta, Iceberg, CSV, JSON, ORC, Avro, XML, Hudi, text), temp views, Kafka, JDBC databases, warehouses and NoSQL stores; 19 transformations; a validation engine with a data-quality report and row-level quarantine; template parameters, runtime pushdown variables and reusable includes. Runs locally, on Databricks, EMR, Dataproc or Synapse — the session manager detects the environment. |
| **The studio** (`sparquet-studio/`) | A browser app that reads and writes exactly those documents on a node canvas, lints them as you type, generates them with an LLM of your choice, and executes them through a local runner. No account, no server, no telemetry. |

The JSON is the contract between them. Studio never invents syntax the framework does not support, and the framework never needs Studio to run.

## Install

### Framework

```bash
pip install sparquet
```

```python
from sparquet import Sparquet

fw = Sparquet(spark={"app_name": "MyJob", "master": "local[*]"})
result = fw.run("pipeline.json", params={"dt_ref": "2026-01-01"})
print(result.summary())
fw.stop()
```

Or from the CLI:

```bash
sparquet pipeline.json                  # console script installed with the package
python -m sparquet.cli pipeline.json    # same thing, from a checkout
```

> **Running locally on Windows?** Spark needs a JDK 17 (`JAVA_HOME`) and the Hadoop
> shims (`winutils.exe` + `hadoop.dll` in `%HADOOP_HOME%\bin`) before it can touch the
> filesystem. With a `local` master, Sparquet also pins `PYSPARK_PYTHON` to the
> interpreter running the job, so a `python` on `PATH` from a different install cannot
> crash the Spark workers — set the variable yourself to override it.

### Studio

```bash
cd sparquet-studio
npm install
npm run dev            # http://localhost:5273
```

Full instructions, including the AI setup and the local runner, are in the [Studio README](sparquet-studio/README.md).

## A pipeline in 30 seconds

```json
{
  "name": "customers_curated",
  "input": { "format": "csv", "path": "/data/landing/customers" },
  "transformations": [
    { "type": "filter", "condition": "status = 'active'" },
    { "type": "cast", "columns": { "created_at": "timestamp" } },
    { "type": "with_column", "column": "loaded_at", "expression": "current_timestamp()" },
    { "type": "drop_duplicates", "columns": ["id"] }
  ],
  "validations": {
    "on_failure": "warn",
    "rules": [
      { "type": "not_null", "columns": ["id"] },
      { "type": "unique", "columns": ["id"] }
    ],
    "report": { "format": "csv", "path": "/dq/customers", "mode": "append" }
  },
  "output": {
    "format": "parquet",
    "path": "/data/curated/customers",
    "mode": "overwrite",
    "partition_by": ["created_at"]
  }
}
```

That file runs as-is, and opens on the Studio canvas as nine connected nodes.

## What the language covers

**Transformations** — `filter` `select` `drop` `rename` `cast` `with_column` `struct` `drop_duplicates` `distinct` `sort` `fill_na` `sql` `group_by` `join` (with `broadcast` map-side hint) `union` `checkpoint` `stop_if_empty` `collect` `debug`

**IO** — files and tables: `parquet` `delta` `iceberg` `csv` `json` `orc` `avro` `xml` `hudi` `txt` `view` (`binary` is read-only); `kafka` batch read and write; relational via JDBC (`postgresql` `mysql` `mariadb` `sqlserver` `oracle`); warehouses (`bigquery` `snowflake` `redshift`); NoSQL/search (`mongodb` `documentdb` `dynamodb` `cassandra` `elasticsearch` `opensearch`). Delta and Iceberg support `MERGE` upserts and Delta time travel; `parquet` `csv` `json` `orc` `txt` `view` `binary` `delta` and `iceberg` are native, every other connector needs its driver JAR on the Spark classpath.

**Validations** — powered by **[`sparquet-cola`](https://github.com/VictorPasqualini/sparquet-cola)**, a standalone data-quality library (pyspark-only) that installs as a dependency (`pip install sparquet-cola`) and is usable on its own. Rules: `not_null` `unique` `range` `regex` `row_count` `sql` (boolean invariant OR `failed_rows` mode), plus SODA-style `check` (a metric vs a warn/fail threshold) and `schema` (columns and types). `fail` / `warn` / `skip` policies, an optional per-rule metrics report, and row-level **quarantine** (`validations.outputs`: split valid/invalid to their own sinks) — all written apart from the main output.

**Beyond the basics**

- **Template parameters** `{param}` substituted before parsing, with list and boolean formatting for SQL (`IN ('a','b')`), plus `skip_if_false` to switch whole steps on and off per run.
- **Runtime variables** `{{var}}` — `collect` a column into a variable and push it into a later read as a literal `IN (...)`, the declarative form of the `collect()` + `isin()` trick that makes Delta data skipping work.
- **Multiple destinations** with per-destination column projections and per-destination transformations, so one DataFrame can land as Parquet, as a Delta merge and as a Kafka topic in a single pass.
- **Includes** — `{ "$include": "shared/filters.json" }` to share fragments across pipelines.
- **Extensible** — register your own readers, writers, transformations and validators; Studio keeps unknown node types intact when it opens the file.

See [CLAUDE.md](CLAUDE.md) for the complete schema reference.

## Documentation

| Document | What it covers |
|---|---|
| [CLAUDE.md](CLAUDE.md) | Full JSON schema, API surface and conventions |
| [sparquet-studio/README.md](sparquet-studio/README.md) | Studio: install, first Job, AI setup, local runner, architecture |
| [sparquet-studio/server/README.md](sparquet-studio/server/README.md) | The local execution service, and why it must stay on `127.0.0.1` |
| [examples/](examples/) | Example pipelines, one per capability |
| [tests/case-of-success/ROADMAP_CASE_OF_SUCCESS.md](tests/case-of-success/ROADMAP_CASE_OF_SUCCESS.md) | A real migration: Spark jobs rewritten as declarative configs |
| [BACKLOG.md](BACKLOG.md) | Roadmap: what is planned, and what was deliberately left out |
| [CHANGELOG.md](CHANGELOG.md) | Release history |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Setup, tests, conventions, how to propose a change |
| [SECURITY.md](SECURITY.md) | Supported versions and how to report a vulnerability |
| [docs/DEPLOY_PYPI.md](docs/DEPLOY_PYPI.md) | Publishing the framework to PyPI |
| [sparquet.dev](https://sparquet.dev) | The public site: landing plus full documentation in English, Portuguese and Spanish, built from the separate [sparquet-web](https://github.com/VictorPasqualini/sparquet-web) repository |

The data-quality engine is a separate package and repository too:
[sparquet-cola](https://github.com/VictorPasqualini/sparquet-cola), which the framework
declares as a dependency and re-exports under `sparquet.validation.*`.

## Repository layout

```
sparquet/               the framework (readers, writers, transformations, validations)
sparquet-studio/        the visual editor (React + TypeScript) and its local runner
examples/               example pipelines, one per capability
tests/                  unit tests and a full real-world migration case
docs/                   documentation index and the PyPI release guide
CLAUDE.md               the JSON schema and API reference
BACKLOG.md              roadmap
```

## Contributing

Issues and pull requests are welcome, on either half — start with
[CONTRIBUTING.md](CONTRIBUTING.md) for setup, the test commands and the conventions that
matter (a new transformation, format or validator needs a Studio catalog entry and an
example pipeline; anything that changes the JSON schema starts as an issue). Everyone
taking part is expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

Apache 2.0 — see [LICENSE](LICENSE).
