# Sparquet Studio — local runner

Optional HTTP bridge that executes a pipeline built in Studio against the real
`Sparquet`, and returns counters, validation results, a data preview and
the framework's structured logs.

Studio works entirely offline without it; the runner only powers the **Run**
button.

## Security

**This service executes arbitrary Spark jobs.** Every request carries a pipeline
definition with arbitrary SQL, arbitrary input paths and arbitrary output paths,
which are run with your user's permissions on your machine and on every data
store your machine can reach.

`POST /run` and `POST /validate` are therefore protected by two checks:

1. **A shared token.** Each request must carry the runner's token in the
   `X-Sparquet-Token` header. Requests without it get `401` and a body
   explaining where to find the token.
2. **An `Origin` allow-list.** A request whose `Origin` header is present and
   outside `SPARQUET_STUDIO_ORIGINS` gets `403` — an actual refusal, not just
   missing CORS response headers.

Both are needed. CORS alone protects nothing here: a cross-origin `POST` with no
custom header and no JSON content type is sent without a preflight, so the
browser only withholds the *response* from the attacker — the pipeline has
already run. Any page you visit while the runner is up could otherwise read
local files and write them to a bucket it controls. The custom header forces a
preflight, and the `Origin` check refuses the request outright.

`GET /health` and `GET /capabilities` stay open so Studio can detect the runner
and prompt for the token.

- Keep it bound to `127.0.0.1` (the default).
- Never put it behind a public address, a tunnel, or a reverse proxy.
- Only widen `SPARQUET_STUDIO_ORIGINS` to origins you control.
- Treat the token as a password: it is the only thing standing between a web
  page and arbitrary code execution on your machine.

### The token

On startup the runner prints a fresh token:

```
========================================================================
Sparquet Studio runner token (this session only):
    S3yhI-6191J6wu2xz7bCX9YpafB0GOLo
Send it as the 'x-sparquet-token' header on /run and /validate, or set
SPARQUET_STUDIO_TOKEN to keep the same token across restarts.
========================================================================
```

Paste it into Studio under **Settings → Local runner → Runner token**; the Run
panel also offers the same field in the card it shows the first time a run comes
back `401`, so getting unblocked never means leaving the panel. Studio keeps the
token with its other settings in the browser and sends it to nothing but the
runner URL.

To keep one token across restarts, set `SPARQUET_STUDIO_TOKEN` before starting;
the runner then prints only a note that it took the token from the environment.
Each process generates its own token, so `SPARQUET_STUDIO_TOKEN` is mandatory if
you ever start uvicorn with more than one worker (you should not: runs are
serialized per process).

From the shell:

```bash
curl -H "X-Sparquet-Token: $SPARQUET_STUDIO_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"pipeline": {"name": "demo"}, "dry_run": true}' \
     http://127.0.0.1:8787/run
```

## Install

```bash
pip install -r sparquet-studio/server/requirements.txt
```

`pyspark` and `sparquet` are expected to already be importable — either
because you run the service from the `sparquet-studio` directory (the module
inserts the repository root into `sys.path`), or because the framework is installed in the same
environment (`pip install -e .`). A working `JAVA_HOME` is required for Spark.

## Run

From the `sparquet-studio` directory (the module adds the repository root to
`sys.path`, so `sparquet` resolves without installing anything):

```bash
uvicorn server.main:app --port 8787
```

Windows PowerShell, same directory:

```powershell
uvicorn server.main:app --port 8787
```

`uvicorn` binds `127.0.0.1` by default. Running the module directly
(`python sparquet-studio/server/main.py`) uses the same host and port and honors
the environment variables below.

### Windows

Spark needs the Hadoop native shims to touch the local filesystem on Windows. A
`/run` that appears to hang forever, or that fails with `NativeIO$Windows`, is
almost always this and not the runner:

1. Download `winutils.exe` and `hadoop.dll` for your Hadoop version.
2. Put them in `C:\hadoop\bin`.
3. Set `HADOOP_HOME=C:\hadoop` and add `%HADOOP_HOME%\bin` to `PATH`.
4. Point `JAVA_HOME` at a JDK your PySpark supports (17 for Spark 3.5, 17+ for
   Spark 4) and restart the terminal.

`GET /health` reports `spark_available` from the import alone, so it can say
`ok` on a machine where a real run still cannot write files. WSL2 or Docker
avoids the whole problem.

The Spark session is created on the first `/run` request and reused for the
process lifetime. A global lock serializes runs: a second concurrent `/run`
returns `409` instead of corrupting the shared session, runtime-variable store
and deferred-warning buffer.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `SPARQUET_STUDIO_TOKEN` | a new random token per start | Shared secret required on `/run` and `/validate`. |
| `SPARQUET_STUDIO_ORIGINS` | `http://localhost:5273,http://127.0.0.1:5273` | Comma-separated origin allow-list, enforced for CORS **and** as a hard `403` on `/run` and `/validate`. |
| `SPARQUET_STUDIO_HOST` | `127.0.0.1` | Bind address when running `python server/main.py`. |
| `SPARQUET_STUDIO_PORT` | `8787` | Port when running `python server/main.py`. |
| `SPARQUET_FRAMEWORK_PATH` | repo root, inferred from this file | Directory containing the `sparquet` package. |

## Endpoints

### `GET /health`

```json
{
  "status": "ok",
  "version": "0.2.0",
  "spark_available": true,
  "framework_version": "0.2.3",
  "auth_required": true
}
```

`status` is `degraded` when pyspark or the framework cannot be found. This
endpoint never imports pyspark, so it stays fast, and it needs no token — it is
how Studio discovers the runner before it has one. `auth_required` is absent on
runners older than 0.2.0, which accepted unauthenticated `/run` calls.

### `POST /run`

Requires the `X-Sparquet-Token` header (`401` without it, `403` when `Origin` is
not allowed).

```json
{ "pipeline": { "name": "demo", "input": {}, "output": {} }, "params": {}, "limit": 50, "dry_run": false }
```

Executes `Sparquet.run_from_dict(pipeline, params=params)`. Response:

```json
{
  "success": true,
  "skipped": false,
  "pipeline_name": "demo",
  "rows_read": 120,
  "rows_written": 118,
  "duration_ms": 4210,
  "error": null,
  "validations": [{ "type": "not_null", "passed": true, "message": "", "failed_count": 0 }],
  "preview": { "columns": ["id"], "rows": [[1]], "truncated": false },
  "logs": [{ "timestamp": "...", "level": "INFO", "message": "Pipeline concluido", "context": {} }]
}
```

- `preview` comes from `PipelineResult.output_df` (`limit` rows, default 50,
  max 1000) with values converted to JSON-safe primitives; it is `null` when the
  run was skipped, failed, or the DataFrame could not be collected.
- `dry_run: true` parses the configuration and returns without touching Spark.
- `rows_written` is the main DataFrame count taken before the writes, so it does
  not necessarily match any single destination.

### `POST /validate`

Requires the `X-Sparquet-Token` header, same as `/run`.

```json
{ "pipeline": { "...": "..." }, "params": {} }
```

Applies `{param}` substitution and parses the config with
`PipelineConfig.from_dict` without executing anything → `{ "valid": true, "error": null }`.

### `GET /capabilities`

Live registries read from the engines and factories, so custom types registered
on the running framework instance show up:

```json
{ "transformations": ["cast", "..."], "readers": ["csv", "..."], "writers": ["csv", "..."], "validators": ["not_null", "..."] }
```

## Notes

`run_from_dict` resolves `$include` directives relative to the **process working
directory**, not to any file — use absolute include paths, or start the runner
from the directory your `$include` paths are relative to.
