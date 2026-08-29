# Sparquet Studio — local runner

Optional HTTP bridge that executes a Job built in Studio against the real
`Sparquet`, and returns counters, validation results, a data preview and
the framework's structured logs.

Studio works entirely offline without it; the runner only powers the **Run**
button.

> **Vocabulary.** A Studio **Job** is one pipeline JSON — exactly what the
> `pipeline` field of a request carries, and what the framework's `Pipeline` class
> executes. A Studio **Pipeline** is an ordered set of Jobs: it posts to
> `/run/flow/stream`, one `stages[]` entry per Job. The request and response
> fields below keep the framework's names.

## Security

**This service executes arbitrary Spark work.** Every request carries a pipeline
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

### Users and sessions

Out of the box the runner has no users, and the shared token above is the whole
of the authentication: whoever holds it can do everything. That is fine for one
person on one laptop, and it stays the default so that upgrading never locks
anybody out.

Create a user and the runner switches modes: from then on it wants a **session**
in addition to the token, and each request is authorized against the roles that
user holds. Create the first one on the machine the runner runs on:

```bash
python server/auth.py create-admin      # prompts for a username and password
python server/auth.py list-users
```

The same thing can be done from Studio (**Settings → Access & IAM → Add user**)
while the runner still has no users, because the token is the identity until the
first one exists.

Permissions follow the IAM shape: a statement is `{effect, actions, resources}`,
an action is `service:Verb` (`workspace:Write`, `run:Execute`, `iam:ManageUsers`),
a resource is `kind/id` (`job/j1`, `workflow/*`), `*` matches anywhere, **an
explicit `deny` wins over any `allow`**, and anything not allowed is denied. Four
roles ship with the runner:

| Role | May |
|---|---|
| `admin` | Everything, including who else has access. |
| `editor` | Build and run pipelines. Cannot manage users. |
| `operator` | Run what already exists and read the results. Cannot edit. |
| `viewer` | Read the library and the history. Changes nothing. |

`editor` holds `workspace:*`, so a couple of actions sit deliberately outside that
family: **`runner:Configure`** — moving the library to another directory decides
where this runner writes on its host, which is an administrator's call, not an
editor's. `admin` holds `*` and gets it for free.

Those four are rewritten on every start, so fixing a policy in code fixes it in
every installation — which is also why they cannot be edited. **Custom roles** are
written in the interface (**Settings → Access & IAM → Roles**) and are never
touched by an upgrade. A role still held by a user or a team cannot be deleted:
the holders would silently lose permissions and nothing on their screen would say
why. `GET /auth/policy` publishes the vocabulary of actions so the UI never
carries a stale copy of it.

### Teams

Everybody belongs to a **team**, and a team is two things at once:

- **Who pays.** Execution credits are charged to the team, not to the person, so a
  squad has one budget instead of one each. Moving somebody to another team changes
  who pays *from then on*; ledger entries already written stay with the team that
  paid at the time.
- **A second source of roles.** A team's roles are *added* to the ones its members
  hold personally. A team grants, it never takes away — and an explicit `deny` on
  either side still wins, because that is where restriction belongs.

Managed in **Settings → Access & IAM → Teams**. The default team cannot be
removed, and deleting a team moves its members back into it rather than leaving
them without one.
What the store guarantees, because each one is a way to get badly stuck or badly
exposed:

- Passwords are hashed with scrypt (PBKDF2-HMAC-SHA256 where scrypt is
  unavailable) and sessions are stored as SHA-256 hashes — a copy of the database
  file is not a set of live logins.
- Disabling an account or changing a password **ends the sessions already open**
  for it, so revoking access does not wait for the next login.
- The last enabled administrator cannot be demoted, disabled or deleted. Without
  that guard the only way back in is editing SQLite by hand.
- An unknown username costs the same time as a wrong password, so the endpoint
  does not answer "does this person exist?".

### Password recovery

There is no "email me a reset link", because the runner has no mail server and
should not grow one. Recovery is a **single-use code**, minted by somebody who
already has access to the machine or to `iam:ManageUsers`, and handed over out of
band:

```bash
python server/auth.py recovery-code ana   # prints a code, valid for 30 minutes
python server/auth.py reset-password ana  # or just set one directly
```

From Studio: **Settings → Access & IAM → Recovery code** on the person's row.
The code is shown once — the runner stores only its SHA-256 hash — and the person
spends it on the login screen under *I have a recovery code*.

Minting a code is, in effect, becoming that person, so the endpoint asks for **the
password of whoever is asking** on top of the session and `iam:ManageUsers`. A
stolen session or an unattended laptop is not enough. The password checked is the
caller's own, never the target's.

This adds no authority that did not already exist: whoever can run the CLI owns
the host and could edit the database anyway. What it buys is that the person
chooses their own password instead of being told one over chat.

The rules the store enforces:

- Issuing a code invalidates any earlier unused one for that user, so a lost code
  is fixed by minting another.
- A code works **once** and expires after `SPARQUET_STUDIO_RECOVERY_MINUTES`.
- Redeeming ends every session the account has open.
- The new password is validated *before* the code is burned — too short and you
  still have your code.
- A disabled account cannot be recovered into, and every refusal reads the same:
  the endpoint does not say whether the code was unknown, expired or already
  used.

Identity lives in its own SQLite file (`SPARQUET_STUDIO_AUTH_DB`), separate from
the execution history: they have different lifetimes, and a history database is
something you might copy around.

None of this makes the runner safe to expose. It is still bound to `127.0.0.1`,
and the token is still required on every call.

## Execution credits

**One credit per successful write**, charged **only when the run does not happen on
this machine**. Running Spark locally costs nothing; sending work to a cluster, to
Spark Connect or to a hosted runtime costs one credit for every destination the run
actually finished writing (`SPARQUET_STUDIO_CREDITS_PER_WRITE` if you want a write
to cost more).

Counting writes rather than Jobs is what makes **a failed run free**: the count
comes from `PipelineResult.output_metrics`, which gains an entry only once a writer
returns, so a run that died before writing has nothing to charge. A Job that writes
three destinations costs three.

Locality is read from the Job's own configuration, never from anything the caller
sends, so nobody can declare their own run free:

| In the Job | Counts as |
|---|---|
| `spark.master` (or `spark.configs["spark.master"]`) starting with `local` | local — free |
| `spark.remote` set to anything | remote — charged, even if a local master is also set |
| `yarn`, `spark://…`, `k8s://…`, or no master at all on a hosted runtime | remote — charged |
| the runner itself running on Databricks / EMR / Dataproc / Synapse | remote — every write is charged |

**Forty writes a month are free** (`SPARQUET_STUDIO_CREDITS_FREE_MONTHLY`), per
`YYYY-MM` period in UTC. The allowance is spent before any granted balance, resets
by itself when the month turns and does **not** accumulate — it is an allowance, not
a stock.

**Metering and enforcement are separate.** By default the ledger records every
remote write and blocks nothing, so turning a runner into a metered one never
suddenly stops anybody's work. Set `SPARQUET_STUDIO_CREDITS=on` and the balance
starts gating execution. That is why an account carries two numbers — `balance`
moves only under enforcement, `spent` always climbs — so switching enforcement on
starts from what was granted, not from accumulated debt. While only metering, the
free allowance is not burned either, and the entry says `applied: false`.

**Charged after the run, admitted before it.** The number of successful writes only
exists once the run is over, so that is when the debit happens. What can honestly be
checked up front is the minimum: under enforcement, a team that cannot pay for a
single write is refused with **HTTP 402** before Spark is started. A run that wrote
more than the account could cover does not go negative — the gap is recorded as
`shortfall` on the entry, and it is the *next* run that gets refused.

In a Pipeline each Job is charged as it finishes, so a flow that breaks at the
fourth Job has paid for what the first three wrote, and the ledger shows it line by
line with `job_run_id` and `pipeline_run_id`.

The account is the **team** (`credits:Read` to see other teams', `credits:Manage` to
grant), or the literal account `token` on a runner with no users. The ledger is
append-only and lives in its own SQLite file. Studio shows all of it under
**Settings → Billing**, and what a single run cost in the run detail.

```bash
# Give somebody credits without the UI:
curl -X POST http://127.0.0.1:8787/credits/<team-id>/grant \
  -H "x-sparquet-token: $SPARQUET_STUDIO_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"amount": 100, "note": "quarter budget"}'
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
| `SPARQUET_STUDIO_HISTORY_DB` | `server/data/execution_history.sqlite3` | SQLite file holding the execution history — the runs, their steps and their logs. |
| `SPARQUET_STUDIO_AUTH_DB` | `server/data/auth.sqlite3` | SQLite file holding users, teams, roles and sessions. Absent users means the runner stays in token-only mode. |
| `SPARQUET_STUDIO_SESSION_HOURS` | `12` | How long a session lasts before it has to be renewed by signing in again. |
| `SPARQUET_STUDIO_RECOVERY_MINUTES` | `30` | How long a password recovery code stays usable. |
| `SPARQUET_STUDIO_CREDITS` | unset (metering only) | `on`/`1`/`true`/`yes`/`enforce` makes balances actually gate execution. Anything else records without blocking. |
| `SPARQUET_STUDIO_CREDITS_PER_WRITE` | `1` | Credits one successful write to a non-local target costs. |
| `SPARQUET_STUDIO_CREDITS_FREE_MONTHLY` | `40` | Writes a team gets for free each calendar month (UTC). Does not accumulate. |
| `SPARQUET_STUDIO_CREDITS_INITIAL` | `0` | Balance an account is created with the first time it is seen. |
| `SPARQUET_STUDIO_CREDITS_DB` | `server/data/credits.sqlite3` | SQLite file holding accounts and the credit ledger. |
| `SPARQUET_STUDIO_WORKSPACE` | unset | Pins the library directory. Set it and the interface may not change it — a deployment that decides centrally decides centrally. Unset, the runner uses what was chosen in Settings, falling back to the per-user default. |
| `SPARQUET_HOME` | `%APPDATA%\Sparquet` on Windows, `$XDG_DATA_HOME/sparquet` (or `~/.local/share/sparquet`) elsewhere | Per-user data directory. Holds `studio.json` and, unless told otherwise, `workspace/` — the library. |
| `SPARQUET_STUDIO_HISTORY_PURGE` | on | `off`/`0`/`false`/`no` stops the runner from applying retention on its own. `POST /runs/purge` still works. |
| `SPARQUET_STUDIO_HISTORY_DETAIL_DAYS` | `30` | After this many days a run loses its logs, its steps and its stored JSON, and keeps its row. |
| `SPARQUET_STUDIO_HISTORY_MAX_DAYS` | `365` | After this many days the run row itself goes — but only with `SPARQUET_STUDIO_HISTORY_DELETE` on. |
| `SPARQUET_STUDIO_HISTORY_KEEP_RUNS` | `10` | The newest N executions of each Job and each Pipeline are never expired, however old they are. |
| `SPARQUET_STUDIO_HISTORY_DELETE` | unset (thin only) | `on`/`1`/`true`/`yes` lets the second stage delete run rows. Off, history thins but never shrinks in row count. |

## Endpoints

### `GET /health`

```json
{
  "status": "ok",
  "version": "0.2.0",
  "spark_available": true,
  "framework_version": "0.2.3",
  "auth_required": true,
  "login_required": false,
  "credits_enforced": false
}
```

`status` is `degraded` when pyspark or the framework cannot be found. This
endpoint never imports pyspark, so it stays fast, and it needs no token — it is
how Studio discovers the runner before it has one. `auth_required` is absent on
runners older than 0.2.0, which accepted unauthenticated `/run` calls.
`login_required` says whether this runner has users: `false` means the shared
token is the identity, `true` means a session is needed on top of it.
`credits_enforced` says whether a balance can refuse a run; `false` means credits
are being counted but nothing is blocked.

### `POST /run`

Requires the `X-Sparquet-Token` header (`401` without it, `403` when `Origin` is
not allowed).

```json
{
  "pipeline": { "name": "demo", "input": {}, "output": {} },
  "params": {},
  "limit": 50,
  "dry_run": false,
  "run_as": "victor",
  "launched": "manual",
  "tags": ["ad-hoc"]
}
```

`tags` is optional and **adds to** what the catalog already knows: the runner
looks up the tags of the Job, its Pipeline and its Workflow and bills the run
under all of them together, so a scheduled or scripted run is attributed without
the caller having to know anything. Same field on `POST /run/flow`.

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
  "logs": [{ "timestamp": "...", "level": "INFO", "message": "Pipeline finished", "context": {} }]
}
```

- `preview` comes from `PipelineResult.output_df` (`limit` rows, default 50,
  max 1000) with values converted to JSON-safe primitives; it is `null` when the
  run was skipped, failed, or the DataFrame could not be collected.
- `dry_run: true` parses the configuration and returns without touching Spark.
- `rows_written` is the main DataFrame count taken before the writes, so it does
  not necessarily match any single destination.
- `run_as` is the name the execution is recorded under. It is a **label, not a
  permission**: the runner authenticates a token, not a person, so a caller can
  claim any name. Omitted, the runner records its own OS account.
- `launched` is `manual`, `scheduled` or `api` — how the run was started.
  Anything the runner does not recognise is recorded as `api`, since a caller it
  cannot classify is by definition not a person clicking Run.
- The run is also recorded with its **lineage**: what the submitted configuration
  reads and writes (`input`/`inputs`, a `join`'s `with`, `output`/`outputs`, and
  the `validations` sinks), with `{param}` resolved. It comes from the JSON rather
  than from the run's own logs, so a run that dies on its first read still reports
  what it was going to touch.

### `POST /run/stream`

Same execution and same request body as `/run`, but as Server-Sent Events, so
Studio can paint per-step status and stream logs while Spark works.

Events: `start`, then `log`* (one per pipeline/stdout/JVM line, carrying `source`
and, for step markers, `context.index`/`context.step`), and a final `result` with
the same payload `/run` returns — or `error`.

```json
{ "pipeline_name": "demo", "timestamp": "...",
  "pipeline_run_id": "092fa5bd…", "job_run_id": "fc29e4d9…" }
```

The `start` event names the ids the run was persisted under: `pipeline_run_id`
addresses `/runs/{id}/cancel`, `job_run_id` addresses `/job-runs/{id}/logs`.

### `POST /run/flow/stream`

Runs several pipeline JSONs **in sequence** — a Studio **Pipeline**, where each Job
is one stage. Requires the token, same as `/run`. Server-Sent Events.

```json
{
  "stages": [
    { "id": "s1", "name": "bronze", "pipeline": { "...": "..." }, "params": {} },
    { "id": "s2", "name": "silver", "pipeline": { "...": "..." } }
  ],
  "limit": 50,
  "stop_on_error": true
}
```

Stages arrive already ordered and share one SparkSession, so a stage hands data to
the next through whatever it wrote — a path the next one reads, or a `view` output
registered as a temp view. No extra wiring is needed here.

Events: `start` (`{flow, total, pipeline_run_id}`), then per stage `stage_start` →
`log`* → `stage_result`, and a final `result`. A stage the stop or an earlier
failure kept from running arrives as `stage_cancelled` or `stage_skipped` instead.
The final `result`:

```json
{
  "success": true,
  "duration_ms": 36743,
  "stages": [
    { "index": 0, "id": "s1", "name": "bronze", "success": true,
      "rows_read": 12, "rows_written": 8, "duration_ms": 34197, "error": null,
      "validations": [], "output_metrics": [] }
  ],
  "preview": { "columns": ["country", "total"], "rows": [["BR", 655.99]] },
  "error": null
}
```

- Every `log` carries `stage_id`, so a line always traces back to the JSON that
  produced it (including `stdout` and JVM lines).
- `preview` is the **last** stage's output — the Pipeline's result.
- `stop_on_error: true` (default) stops at the first failing stage: later stages
  never start, and `error` names the stage that broke. `false` runs them all.
- `success` is true only when every stage ran and succeeded.
- The whole sequence takes the same single run lock as `/run`, so a second
  Pipeline (or a single run) while one is in progress gets `409`.

### `POST /runs/{run_id}/cancel`

Stops the run in flight. Two things happen: a flag makes the flow stop at the next
stage boundary, and `cancelAllJobs()` aborts whatever Spark is computing right
now — without it a long write would run to completion no matter what the flag
says. `run_id` is the `pipeline_run_id` the `start` event carried.

```json
{ "cancelled": true, "run_id": "092fa5bd…", "spark_jobs_cancelled": true }
```

`spark_jobs_cancelled` is false when nothing was computing on Spark yet: the run
still ends, but no JVM job had to be killed for it. `409` when `run_id` is not the
run this process is executing — a finished run has nothing to cancel, and
cancelling one run must never touch another.

A cancelled run is persisted with status `cancelled`: not a failure, not a skip.

### `GET /runs`

Past executions, most recent first. Filters: `workflow_id`, `pipeline_id`,
`job_id`, `limit` (default 20, max 200). `jobs`/`steps` come back **empty** here —
fetch `/runs/{id}` for the nested detail.

Each row carries `run_as` and `launched` alongside the status and the timings, so
a list of runs answers *who* and *how* without a second request. Both are `null`
on runs recorded before those columns existed. `pinned` says whether retention has
been told to keep this execution forever.

### `GET /runs/{run_id}`

One execution in full: every job it ran (or skipped) and every step of each, so
Studio can open a past run and jump straight to whichever step failed. `404` when
the runner no longer holds it.

Each `job_run` also carries `lineage`, a JSON **string** (like `step_run.details`)
holding `{"inputs": [...], "outputs": [...]}`, where each entry is
`{"role", "format", "address"}` plus `"mode"` on a write. `role` is `input`,
`join`, `output`, or `validation:report` / `validation:valid` / `validation:invalid`
for the quality sinks. It is `null` when the configuration named no dataset, or on
a run recorded before lineage existed.

### `POST /runs/{run_id}/pin`

Body `{"pinned": true}` keeps that execution forever: retention skips a pinned run
whatever its age. `{"pinned": false}` puts it back within reach. Answers
`{"run_id", "pinned"}`, `404` when the runner does not hold the run. Requires
`history:Pin`.

### `POST /runs/purge`

Applies the retention policy now, instead of waiting for the daily pass. Requires
`history:Purge`. `?dry_run=true` counts exactly what would go and touches nothing —
worth doing first, since the second stage deletes rows for good.

Retention runs in two stages, both driven by the environment variables above:

1. **Thin** — past `DETAIL_DAYS`, a run drops its log lines, its step rows and the
   copy of the JSON it ran. The run row survives with its status, its timings, its
   row counts and the `config_hash`, so success rates and durations still plot and
   two executions can still be compared by fingerprint.
2. **Delete** — past `MAX_DAYS`, and only with `HISTORY_DELETE` on, the row goes too.

Two things are never expired by either stage: a **pinned** run, and the newest
`KEEP_RUNS` executions of each Job and each Pipeline — a Job that runs once a
quarter must not open on an empty screen. The credit ledger is a separate database
and is never touched, so purging history never rewrites what was billed.

The answer reports `runs_thinned`, `runs_deleted`, `logs_deleted`, `steps_deleted`,
`configs_dropped`, `rows_removed`, `vacuumed` and the `policy` that was applied.
`VACUUM` only runs when enough came out to be worth rewriting the file; without it
SQLite keeps the freed pages.

### `POST /runs/ingest`

Records a run this runner never executed. Requires `history:Ingest`.

The framework runs anywhere and depends on nothing, which is exactly why the runs
that matter most — the nightly job on Databricks, the DAG on Airflow, a
`sparquet.cli` call on a VM — used to leave no trace in any history. Point the
framework at this endpoint and they land here like any other execution: same steps,
same logs, same screens.

On the machine that runs the pipeline:

```bash
export SPARQUET_HISTORY_URL="http://127.0.0.1:8765/runs/ingest"
export SPARQUET_HISTORY_TOKEN="$SPARQUET_STUDIO_TOKEN"
export SPARQUET_HISTORY_JOB_ID="j-orders"      # which Job in the library this is
export SPARQUET_HISTORY_RUN_AS="airflow"
python -m sparquet run orders.json
```

Nothing is sent unless `SPARQUET_HISTORY_URL` is set, and the framework sends
**once, at the end of the run** — one request per execution, not one per step. A
four-hour job appears when it finishes, with every step and every log line it
produced. Failures are reported too: that is the run a reader most wants. See
`sparquet/observability/history.py` for the full list of variables and for
registering a sink in code instead.

The body is the document that module produces (`{"schema": "sparquet.run/1", "run":
{...}, "records": [...]}`); anything else is refused with `400` rather than stored
as a run that says the wrong thing. Records are replayed through the same step
tracker and stored through the same log writer a local run uses, capped at 5000 per
submission. The answer is `{"pipeline_run_id", "job_run_id", "records",
"duration_ms"}`.

Two things differ from a local run, both on purpose:

- it is marked `launched: "external"`, so a reader can tell what this runner
  executed from what it was merely told about;
- timings come from the document, not from the clock here — otherwise the whole
  history of a nightly job would sit at the hour its report arrived.

An external run consumes **no credits** on this runner: the compute was not ours.

> **Security.** The token is a password — whoever holds it writes into this
> history. The runner binds to `127.0.0.1` because it executes arbitrary Spark;
> publishing it on a network to collect history exposes everything else it can do
> along with it. To receive runs from other machines, put a reverse proxy in front
> that accepts **only** this route, over TLS, and leave the runner closed.

### `GET /job-runs/{job_run_id}/logs`

What one job execution printed, in the order it printed it — the same lines the
run panel showed live, since every source funnels through the runner's event queue
and is persisted from there.

```json
{
  "job_run_id": "fc29e4d9…",
  "total": 34,
  "next_after": 3,
  "lines": [
    { "seq": 1, "timestamp": "...", "level": "INFO", "source": "pipeline",
      "message": "Pipeline started", "context": {} }
  ]
}
```

- Paged by `seq`, not by offset: lines are only ever appended, so `after` never
  re-reads or skips a line the way an offset does while a run is still going.
  `limit` defaults to 500, max 2000. `next_after` is null at the end.
- `source` is `pipeline` (the framework), `spark` (the JVM), `stdout` or `runner`.
- At most 3000 lines are stored per job execution; past that the runner records a
  single `WARNING` line naming how many it dropped, so a runaway job cannot grow
  the database without bound.

### `GET /job-runs/{job_run_id}/config`

The **version of the JSON that this execution ran** — not the Job as it is now.

```json
{
  "job_run_id": "fc29e4d9…",
  "config_hash": "sha256:9f2b…",
  "config": { "name": "vendas", "input": { "...": "..." } }
}
```

`config_hash` is SHA-256 over the canonical form of the configuration (keys
sorted, no whitespace) **after** `{param}` substitution, for the same reason
lineage resolves params: the same template run with different parameters did not
run the same thing. Two runs with the same hash ran the same JSON; two runs of
"the same Job" with different hashes did not.

`config` is the configuration itself, kept up to 512 KB. Past that only the hash
is stored and `config` comes back `null` — the question the hash answers is the
one that matters most, and a run listing must not carry megabytes. This is a
separate endpoint for the same reason: the configuration dwarfs the row that
describes the run. A run recorded before this existed reports both as `null`.

### `POST /validate`

Requires the `X-Sparquet-Token` header, same as `/run`.

```json
{ "pipeline": { "...": "..." }, "params": {} }
```

Applies `{param}` substitution and parses the config with
`PipelineConfig.from_dict` without executing anything → `{ "valid": true, "error": null }`.

### `GET /auth/status`

```json
{ "login_required": true, "principal": null }
```

Needs the token but no session — it is the call Studio makes before it can have
one. `principal` is filled in when the session header names a live session.

### `POST /auth/login`

```json
{ "username": "ana", "password": "..." }
```

→ `{ "token": "...", "expires_at": "2026-01-01T12:00:00+00:00", "user": { ... } }`,
or `401` for a wrong password, an unknown user or a disabled account — all three
answer the same way. Send the token back as the `X-Sparquet-Session` header (an
`Authorization: Bearer` header is accepted too) alongside `X-Sparquet-Token`.

### `POST /auth/logout` · `GET /auth/me`

`logout` ends the session in the header; `me` returns the principal behind it,
including the statements its roles grant, so Studio can grey out what would come
back `403`.

### `GET /auth/users` · `GET /auth/roles` · `GET /auth/teams`

Requires `iam:ReadUsers`. Users never include anything derived from the password;
a role says whether it is `custom`; a team carries its inherited roles and how many
members it has.

### `GET /auth/policy`

The vocabulary the interface builds a role out of: every action the runner
recognises, grouped by service, with what each one guards. Requires
`iam:ReadUsers`.

### `POST /auth/users` · `PATCH /auth/users/{id}` · `DELETE /auth/users/{id}`

Requires `iam:ManageUsers`. `POST` takes `{username, password, roles, team,
display_name}`; `PATCH` takes `roles`, `team` and/or `disabled`. `team` is an id or
a name; omitted, the person lands in the default team. Each refuses with `400` when
it would leave the runner with no enabled administrator.

### `POST /auth/roles` · `PATCH /auth/roles/{name}` · `DELETE /auth/roles/{name}`

Requires `iam:ManageRoles`. `POST` takes `{name, description, statements}` with
statements in the `{effect, actions, resources}` shape. Built-in roles refuse both
`PATCH` and `DELETE`, and a custom role still held by a user or a team refuses
`DELETE` with `400`.

### `POST /auth/teams` · `PATCH /auth/teams/{id}` · `DELETE /auth/teams/{id}`

Requires `iam:ManageTeams`. `POST` takes `{name, roles}`; `PATCH` takes `name`
and/or `roles`. Deleting moves the members into the default team, which itself
cannot be deleted.

### `POST /auth/users/{id}/password`

`{ "password": "...", "current_password": "..." }`. Changing your own password
requires `current_password`; an administrator resetting somebody else's does not.
Either way every session that password had opened stops working.

### `POST /auth/users/{id}/recovery`

Mints a single-use recovery code for that user and returns it once:

```json
{ "user_id": "u1", "username": "ana", "code": "…", "expires_at": "2026-08-28T12:30:00Z" }
```

Body: `{ "password": "..." }` — **the caller's own password**, a step-up on top of
the session and `iam:ManageUsers`, because minting a code is as good as becoming
that person. A wrong one answers `403`. Issuing invalidates any earlier unused code
for the same user. The runner keeps only the hash, so this response is the only
copy.

### `POST /auth/recover`

`{ "code": "...", "password": "..." }`. Needs the shared token but **no session**
— it is called from the login screen. Sets the password, burns the code and ends
every session that account had open. Every refusal returns the same message.

### `GET /credits/me`

```json
{ "account": { "id": "t1", "username": "platform", "balance": 7, "spent": 3,
               "period": "2026-08", "free_used": 9, "free_monthly": 40,
               "free_remaining": 31, "available": 38 },
  "enforced": false, "credits_per_write": 1, "free_monthly": 40,
  "usage": { "period": "2026-08", "writes": 12, "charged": 3, "waived": 9 } }
```

No permission needed — it is your own team's balance. `available` is the free
allowance left plus the granted balance; `usage` is this month, with `waived`
counting the writes the allowance covered.

### `GET /credits` · `GET /credits/{account_id}/ledger` · `POST /credits/{account_id}/grant`

Every account (`credits:Read`); one account's entries, newest first (your own
always, anybody else's with `credits:Read`); and adding credits, or taking them
back with a negative amount (`credits:Manage`). A ledger entry carries `applied:
false` when it was recorded on a runner that meters without enforcing, and says how
many `writes` it paid for, how much came out of the free allowance (`free_amount`)
and how much went unpaid (`shortfall`).

`GET /runs/{run_id}` returns the same figures per Job under `credits`, which is what
the run detail in Studio shows.

A run refused for lack of credits answers **402** with the message naming the
grant endpoint. Nothing is written to the history for it — it never started.

### `GET /credits/usage`

`?group_by=workflow|user|team|job|tag&period=YYYY-MM&account_id=...` — the month's
spending read along one dimension. Grouping by anything else answers **400**: the
value picks the query, so only those five are accepted.

```json
{ "period": "2026-08", "group_by": "workflow", "scope": "t1",
  "total": { "writes": 12, "charged": 3, "waived": 9, "runs": 5 },
  "overlapping": false,
  "groups": [{ "key": "w1", "label": "Vendas", "writes": 8, "charged": 2,
               "waived": 6, "runs": 3, "last_at": "2026-08-28T19:02:11Z" }] }
```

Your own team is always readable; `account_id` pointing at somebody else's, or
omitting the scope to read the whole runner, needs `credits:Read` and answers
**403** without it — rather than quietly answering about yourself.

The account is the **team**; `workflow` and `user` are ways of reading its
invoice, not payers. Workflow names are resolved at read time from the history
catalog, so renaming a workflow relabels every past month too. A row whose key is
`null` is reported as unattributed, never dropped — runs charged before the
attribution existed still add up to the total.

`group_by=tag` is the one dimension that does **not** partition the month, and
`overlapping` is `true` only for it: a run wearing `finance` and `nightly` is
counted in full under both, so the rows add up to more than `total`. `total` is
computed independently and always counts each entry once. The row with a `null`
key is the untagged spending, and it is omitted when there is none.

Tags are frozen on the ledger entry when the run is charged, taken from the Job,
its Pipeline and its Workflow together (`effective_tags`), plus anything the
caller passed as `tags` on `POST /run` or `POST /run/flow`. Retagging a Job
changes what it costs from the next run on and rewrites nothing already billed —
a closed month stays the month that was invoiced.

### `GET /credits/timeline`

`?months=6&account_id=...` — one row per month, oldest first, so a screen can
draw the series instead of a single total. `months` is clamped to 1–36. Months
with no spending are present as zeros rather than missing, because a gap would
change the shape of the chart. Same scope rule as `/credits/usage`.

```json
{ "scope": "t1",
  "periods": [{ "period": "2026-07", "writes": 0, "charged": 0, "waived": 0, "runs": 0 },
              { "period": "2026-08", "writes": 12, "charged": 3, "waived": 9, "runs": 5 }] }
```

### `GET /audit`

`?limit=&actor_id=&resource=&outcome=&action=&since=` — the trail of state-changing
requests the runner accepted or refused, newest first. Needs `iam:ReadAudit`.

```json
[{ "id": "a1", "at": "2026-08-29T14:03:11Z", "actor": "ana", "actor_id": "u1",
   "team": "platform", "roles": ["admin"], "action": "iam:CreateUser",
   "method": "POST", "path": "/auth/users", "resource": "u2",
   "outcome": "allowed", "status": 200, "detail": { "username": "bruno" },
   "ip": "127.0.0.1" }]
```

`action` accepts a `iam:*`-style prefix. A refused request is recorded with
`outcome: "denied"` and whatever identity it had — including none, which is
exactly the row worth reading. Bodies are never stored: `detail` holds only the
few named fields that say what changed.

### `GET /capabilities`

Live registries read from the engines and factories, so custom types registered
on the running framework instance show up:

```json
{ "transformations": ["cast", "..."], "readers": ["csv", "..."], "writers": ["csv", "..."], "validators": ["not_null", "..."] }
```

### `GET /workspace`

The whole Studio library in one read — how the editor loads on boot.

```json
{
  "root": "/repo/sparquet-workspace",
  "workflows": [{ "kind": "workflow", "id": "w1", "record": { "...": "..." }, "path": "vendas/workflow.json" }],
  "jobs": [{ "kind": "job", "id": "j1", "record": { "...": "..." }, "path": "vendas/jobs/ingestao.json" }],
  "pipelines": [],
  "meta": { "seeded": true, "version": 4 }
}
```

`record` is the Studio record; `path` is the reviewable file it was written to.

### `PUT /workspace/{kind}/{record_id}`

`kind` is `workflow`, `job` or `pipeline`. Body:

```json
{ "record": { "...": "..." }, "config": { "...": "..." } }
```

`config` is a Job's **compiled** Sparquet JSON — the client compiles it, because
the compiler is the client's and a second implementation here would drift. It is
what the reviewable file holds; `null` for the other kinds, and for a Job that
does not compile yet.

Each write produces two files: the reviewable one under
`<workflow-slug>/{jobs,pipelines}/<slug>.json`, and a sidecar in
`.studio/<kind>/<id>.json` holding the full record. Renaming moves the file
instead of leaving a stale copy next to the new one, and renaming a Workflow
moves everything under it. The same write mirrors the record into the catalog
tables below.

### `DELETE /workspace/{kind}/{record_id}`

Removes both files and soft-deletes the catalog row → `{ "deleted": true }`.
Soft, because its executions still point at it.

### `PUT` / `DELETE /workspace/meta/{key}`

Small values that belong to the library rather than to a record — which storage
version wrote it, whether the examples were seeded — kept in `.studio/meta.json`.
Body for `PUT`: `{ "value": <anything JSON> }`. They travel with the workspace so
a second checkout does not re-seed or re-migrate a library that is current.

### `GET /workspace/root`

Where the library is, and why it is there. Needs `workspace:Read`.

```json
{
  "root": "/home/ana/.local/share/sparquet/workspace",
  "source": "default",
  "default": "/home/ana/.local/share/sparquet/workspace",
  "settings_file": "/home/ana/.local/share/sparquet/studio.json",
  "writable": true,
  "inside_source_tree": false,
  "locked": false
}
```

`source` is the reason, strongest first: `env` (`SPARQUET_STUDIO_WORKSPACE` — then
`locked` is true and the interface may not change it), `settings` (chosen in the
interface), `legacy` (an older directory that already held a library, adopted so
nobody loses one), `default` (the per-user directory). Somebody who cannot find
their Jobs is almost always looking at a different directory than the runner is,
which is why the reason is returned and not only the path.

### `PUT /workspace/root`

Points the runner at another directory. Needs **`runner:Configure`**, not
`workspace:Write`: the built-in `editor` role holds `workspace:*`, and deciding
where the runner writes on its host is an administrator's call.

```json
{ "root": "/srv/sparquet/library" }
```

`null` or an empty string clears the choice and goes back to the default. The
answer is the same shape as `GET`. Refusals: `409` when the environment pinned
it, `400` for a relative path, for a directory that cannot be created or written,
and for **any path inside the runner's own source tree** — a checkout is code, it
gets pulled, reset and deleted, and a library in one is lost to the first `git
clean` or committed by accident long before that.

Changing the root **copies nothing**. The store is rebound and the runner starts
reading and writing the new place, which is what makes this the way to *adopt* a
directory that already holds a library. Moving files is the operator's job: a
half-finished copy with no way back is worse than a move nobody made.

All seven require the `X-Sparquet-Token` header.

## Where the library is stored

By default, in the per-user data directory — `%APPDATA%\Sparquet\workspace` on
Windows, `$XDG_DATA_HOME/sparquet/workspace` (usually
`~/.local/share/sparquet/workspace`) elsewhere. **Not** inside this checkout: a
checkout is code, and a library living in one is lost to the first `git clean`.
`PUT /workspace/root` points it anywhere else, and a `sparquet-workspace/` at the
repository root that already holds a `.studio/` is still adopted, so an existing
library keeps working — the runner logs a warning telling you to move it.

Whatever the directory, it has the same shape:

```
<library root>/
  vendas/
    workflow.json               the Workflow, readable
    jobs/ingestao.json          the COMPILED pipeline — runnable as-is
    pipelines/diario.json       the Pipeline and its stages
  .studio/
    workflow/w1.json            the full Studio records, by id
    job/j1.json
    pipeline/p1.json
    index.json                  id → reviewable path
    meta.json                   library-level bookkeeping
```

The point of the split: the top of the tree is what a person reviews in a pull
request, and `sparquet run vendas/jobs/ingestao.json` runs exactly the file they
read. `.studio/` is the editor's own state — canvas positions, parameters, the
things the framework has no use for. Both belong under version control — of the
library, which is the user's repository and not this one. Point a second machine
at the same directory (a checkout, a shared volume, a synced folder) and it opens
the same library.

Browser storage (IndexedDB, then localStorage) stays behind this as a fallback
for when the runner is not running. It is a cache, not the store.

## Execution history schema

SQLite, at `server/data/execution_history.sqlite3` (override with
`SPARQUET_STUDIO_HISTORY_DB`). Two halves in one file, with `PRAGMA
foreign_keys=ON` throughout: a **catalog** of what exists, and a **history** of
what ran.

```
workflow
  ├── job ─────────────┐            job.workflow_id  -> workflow.id
  └── pipeline         │            pipeline.workflow_id -> workflow.id
        └── pipeline_stage ─┘       which Jobs a Pipeline runs, in order

pipeline_run          one execution   -> workflow, pipeline, job
  └── job_run         one Job in it   -> pipeline_run, job
        ├── step_run  one input / transformation / validation / output
        └── run_log   the lines it printed, PK (job_run_id, seq)
```

**Job to Pipeline is many-to-many, and that is not a compromise.** In the Studio
a Job belongs to a Workflow, and a Pipeline is an ordered sequence of stages that
each point at a Job. The same Job can be a stage of several Pipelines, can appear
twice in one, and can be run on its own without belonging to any. So the relation
lives in `pipeline_stage (pipeline_id, stage_id, job_id, stage_index)`, keyed by
stage rather than by job. Every other edge above is a plain foreign key.

A Workflow, a Pipeline and a Job each carry **tags**, in `catalog_tag (kind,
record_id, tag)`. They are what Billing groups by, and they are inherited
downwards: a run's tags are its Job's, its Pipeline's and its Workflow's unioned,
most specific first. Tagging the Workflow is the cheap way to tag everything
inside it. A tag is trimmed, capped at 40 characters and deduplicated
case-insensitively, at most 20 per record — the same rules as `src/lib/tags.ts`
in the Studio, because a `Prod` stored apart from a `prod` would split a month's
spending for a reason invisible on the screen.

The catalog is written by `PUT /workspace/...`: saving a record in the editor
mirrors it here. Rows are never deleted, only marked with `deleted_at` — a run
whose Job has been removed is still a run that has to be readable, and deleting a
Workflow soft-deletes what belonged to it.

A run may still name an id the catalog has not seen (a script, a scheduler, an
older Studio). Those get a placeholder row, filled in on the next save. History
records what happened; it does not get to reject an execution because the catalog
was behind.

`pipeline_run` is the **execution**, not the Studio Pipeline: `kind` says whether
the user ran a single Job (`kind='job'`, one `job_run`) or a Pipeline
(`kind='pipeline'`, one `job_run` per stage, ordered by `stage_index`). A Job's
own runs are found through `job_run.job_id`, never by assuming `kind='job'`.

`job_run` also carries `name`, `lineage`, `config_hash` and `config` of its own:
they describe the execution as it happened, even if the Job has since been renamed
or rewritten. The hash is what tells two runs of "the same Job" apart, and what
matches a run against the file in git — see
`GET /job-runs/{job_run_id}/config` above.

The schema generation is in `PRAGMA user_version`. An older database is rebuilt
on open — SQLite cannot add a foreign key in place, so `pipeline_run` and
`job_run` are recreated with their keys and the rows copied, in one transaction.

## Notes

`run_from_dict` resolves `$include` directives relative to the **process working
directory**, not to any file — use absolute include paths, or start the runner
from the directory your `$include` paths are relative to.
