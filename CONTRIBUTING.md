# Contributing to Sparquet

Sparquet is two halves of one idea: a JSON-driven PySpark framework (`sparquet/`) and
a visual editor for the same documents (`sparquet-studio/`). The JSON is the contract
between them, so most contributions touch both sides — a capability the framework
gains has to become a node Studio can offer.

Two things live outside this repository:

| What | Where | Why it matters here |
|---|---|---|
| The data-quality engine | [`sparquet-cola`](https://github.com/VictorPasqualini/sparquet-cola) (import `sparquet_cola`, a declared dependency) | Changes to validation *checks* belong there, not here. `sparquet/validation/*` are compatibility shims that re-export it. |
| The public documentation | [`sparquet-web`](https://github.com/VictorPasqualini/sparquet-web), published at <https://sparquet.dev> | Behaviour changes need a docs PR there, in **English, Portuguese and Spanish**. |

[CLAUDE.md](CLAUDE.md) is the in-repo reference for the JSON schema, the public API and
the conventions. Read it before proposing a change to the language, and update it in
the same pull request when the language changes.

## Proposing a change

- **Anything that changes the JSON schema: open an issue first.** A new transformation
  type, a new field, a renamed key, a changed default. The JSON is a published contract
  consumed by the framework, the Studio compiler, the linter and the docs site, so the
  shape is worth agreeing on before code exists.
- Bugs, connectors, docs fixes and tests: a pull request straight away is fine.
- Behaviour that only makes sense for one company or one dataset does not belong in the
  core. The extension points (`register_reader`, `register_writer`,
  `register_transformation`, `register_validator`) exist exactly for that.
- Keep pull requests to one subject. Commit messages follow the existing style:
  `feat:`, `fix:`, `docs:`, `chore:`, optionally scoped (`feat(studio): ...`).
- **Do not bump `__version__`** in a pull request. Releases are cut by a maintainer
  following [docs/DEPLOY_PYPI.md](docs/DEPLOY_PYPI.md); add your entry under
  `## [Unreleased]` in [CHANGELOG.md](CHANGELOG.md) instead.

## Setting up the framework

Requirements: **Python 3.9+** (CI runs 3.9, 3.11 and 3.12) and, to actually run Spark,
a **JDK 17** with `JAVA_HOME` pointing at it — not Java 8 or 11.

```bash
git clone https://github.com/VictorPasqualini/sparquet.git
cd sparquet
python -m venv .venv && source .venv/bin/activate   # Windows: .\.venv\Scripts\activate
pip install -e .                    # framework + pyspark + sparquet-cola
pip install -r requirements.txt     # the same runtime deps, without installing the package
pip install -e ".[delta]"           # only if you need OSS Delta outside Databricks
```

The pure unit tests need neither Java nor a cluster — `pip install -e .` is enough to
run them.

### Windows: the Hadoop shims

Spark needs the Hadoop native shims to touch the local filesystem on Windows. Without
them a local run hangs forever or dies with `NativeIO$Windows`, which looks like a
framework bug and is not:

1. Get `winutils.exe` and `hadoop.dll` for your Hadoop version.
2. Put them in `C:\hadoop\bin`.
3. Set `HADOOP_HOME=C:\hadoop` and add `%HADOOP_HOME%\bin` to `PATH`.

`sparquet-studio/run-runner.ps1` does this detection for the local runner and prints
what it found. WSL2 or Docker avoids the whole problem. More detail in the
[Studio README](sparquet-studio/README.md#local-runner).

## Running the tests

### Framework

The framework tests are plain `unittest` files with no pytest dependency. Most drive
fake builders and pure functions, so they run in a second; the few that need a real
SparkSession say so in their docstring and **skip themselves** when there is no Java.
Every `tests/**/test_*.py` is a standalone script, and
[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs each of them — so a file
that only passes under pytest, or only when imported from the repository root without
`PYTHONPATH`, breaks the gate. Run them the same way CI does before opening a pull
request:

```bash
PYTHONPATH=. python tests/io/test_connectors.py
PYTHONPATH=. python tests/io/test_delta_path.py
PYTHONPATH=. python tests/validation/test_soda_checks.py
# ...and any other tests/**/test_*.py you added
```

All of them at once, the way the CI step does it:

```bash
for f in $(find tests -name 'test_*.py' | sort); do
  PYTHONPATH=. python "$f" || echo "FAILED: $f"
done
```

PowerShell:

```powershell
$env:PYTHONPATH = "."
Get-ChildItem tests -Recurse -Filter test_*.py |
  ForEach-Object { python $_.FullName }
```

A new connector goes into `tests/io/test_connectors.py` (assert the Spark format
string, the options built and how `path` maps to a table/collection/index). A new pure
helper for validations goes into `tests/validation/test_soda_checks.py`. A new file is
picked up automatically as long as it is named `test_*.py` and runs as a script
(`unittest.main()` at the bottom).

[`docs/TEST_PLAN.md`](docs/TEST_PLAN.md) lists what is covered today and what is
still missing, connector by connector and transformation by transformation — a good
place to find a self-contained first contribution.

### Studio

Node 18.18+ (22 recommended):

```bash
cd sparquet-studio
npm install
npm run typecheck     # tsc --noEmit
npm run test          # vitest, includes the compiler round-trip over examples/
npm run lint          # eslint, zero warnings allowed
npm run smoke         # end-to-end in a real Chrome — run it when you touch the canvas
```

The local runner is optional and only powers the **Run** button:

```bash
pip install -r sparquet-studio/server/requirements.txt
cd sparquet-studio && uvicorn server.main:app --port 8787
```

It executes arbitrary Spark work with your permissions. Keep it on `127.0.0.1` and read
[`sparquet-studio/server/README.md`](sparquet-studio/server/README.md#security) before
changing anything about its token or `Origin` checks.

## Conventions that matter

**A new transformation, format or validator needs a catalog entry.** The catalog in
`sparquet-studio/src/catalog/` (`transformations.core.ts`,
`transformations.advanced.ts`, `formats.files.ts`, `formats.databases.ts`,
`validators.ts`) is the single source that feeds the node palette, the property forms,
the linter and the system prompt of the AI assistant. Without an entry, Studio cannot
offer your capability and cannot describe it — unknown types are still imported and
preserved on round-trip, but with no dedicated form. Include the field defaults and the
gotchas, the way the existing entries do.

**Every new capability gets an example pipeline** under [`examples/`](examples/), added
to [`examples/README.md`](examples/README.md) with one line on what it demonstrates.
The examples double as fixtures: the Studio compiler round-trips them in `npm run test`,
so an example that does not parse breaks the build.

**Documentation lives in `sparquet-web`.** The reference pages under
`src/content/docs/docs/reference/` of that repository describe real behaviour, in
English, Portuguese and Spanish. A pull request that changes behaviour should link to
the companion docs pull request.

**Validations report, transformations change.** Keep the separation: a rule that
silently drops rows belongs in `transformations`, not in `validations`.

**Keep readers and writers thin.** Build `.format(...).options(...)` and let
connector-specific knobs come through `options`. This repository does not vendor driver
JARs; a format that needs one says so in its module docstring and in the catalog entry.

**The engines are injectable and the factories are class-level registries.** Extensions
registered on a `Sparquet` instance affect every run in the process — do not add global
state beyond that.

**Errors surface in `PipelineResult`.** `PipelineResult` never raises; a failed run
returns `success=False` with `error` set. Logs are structured JSON via
`sparquet/utils/logger.py` — no bare `print`.

**Portuguese in the codebase.** Docstrings and comments are mixed Portuguese and
English today. New user-facing text (README, catalog copy, log messages, error
messages) should be English.

## Reporting bugs and vulnerabilities

Use the [issue templates](.github/ISSUE_TEMPLATE/). A bug report without the pipeline
JSON, the versions and the full traceback usually cannot be acted on.

Do **not** open a public issue for a security problem — see [SECURITY.md](SECURITY.md).

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md). Contributions
are licensed under [Apache 2.0](LICENSE), like the rest of the project.
