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

1. **A credential in a header of its own.** Either the runner's token in
   `X-Sparquet-Token`, or a live session in `X-Sparquet-Session` for a runner
   that has users. Requests carrying neither get `401` and a body explaining
   where to find the token. A session is accepted in the token's place because
   it buys the same thing — a custom header the browser will not attach
   cross-origin without a preflight — and it is the credential a logged-in
   person actually holds.
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

Once the runner has users, three more stop asking for the token: `GET
/auth/status`, `POST /auth/login` and `POST /auth/recover`. The token is typed
into Settings, Settings is behind the login, and the login demanding the token
is a closed loop — rotating the token would lock everybody out of the only
screen that can accept the new one. The `Origin` check still covers all three,
the password is the wall, and failed attempts are rate-limited
(`SPARQUET_STUDIO_LOGIN_ATTEMPTS`, default 10, per `SPARQUET_STUDIO_LOGIN_WINDOW`
seconds, default 300, counted per caller **and** per account). A runner with no
users still demands the token on all three: there is nothing else to ask for.

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

Create a user and the runner switches modes: from then on it wants a **session**,
and each request is authorized against the roles that user holds. The session
replaces the token rather than joining it — Studio keeps sending both, and either
one satisfies the guard. Create the first user on the machine the runner runs on:

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

### Default access

A record that nobody has written a rule about is **ungoverned**, and ungoverned
means anybody holding the matching action may use it. That is the right default
for a laptop and the wrong one for a shared runner, so a securable created
through this runner gets an owner at the moment it is created.

By default (`creator+team`) two records are written once, on creation:

- the creator becomes the **owner** — admin over it, and able to grant;
- their **team** gets `write`, so the people beside them can still edit it.

A secret is the one exception: the team gets `read`, not `write`. On a
credential `read` means *a run of theirs may use it* — which is what a teammate
needs — while `write` would let them repoint `pg-prod` at a different database.

It fires for a **Job, Pipeline, Workflow or saved query** the first time
`PUT /workspace/...` sees that id, for a **dataset** the first time an address
appears in the catalog, and for a **secret** when it is created.

### Saved queries as securables

A query saved from the SQL editor is a library file like a Job, and it is
governed like one: `query/<id>` takes grants and an owner, and the rules are
enforced in two places.

`GET`, `PUT` and `DELETE` on `/workspace/query/{id}` check `read`, `write` and
`admin` on the file. `POST /query` checks `read` on it when the request carries
`saved_query_id` — the SQL editor sends it whenever the buffer belongs to a
saved file, and sends nothing for an unnamed draft.

What a grant on a query never does is stand in for the tables. Every source of
every query is authorized as a dataset on its own, before any of them is opened,
so `read` on somebody's saved file is not a path to a table you are denied. The
statement can be pasted; the tables cannot be.

Jobs and Pipelines are deliberately left out of the document guard: they are
governed where they run, which is where reading a table actually happens.
Widening it to them is a change with its own consequences and is not made in
passing here.

Three conditions hold it back, and each one is deliberate:

- **A token-only caller claims nothing.** A shared runner token is not a person;
  naming it as an owner would govern a record against a principal that does not
  exist.
- **A record whose chain is already governed is left alone.** A Job saved into a
  Workflow that has rules inherits them, and writing an owner here would
  silently narrow the Workflow — the opposite of what a container is for.
- **The first catalog a library ever writes claims nothing.** That save is a
  browser syncing a catalog it already had, and treating a migrated catalog as a
  hundred fresh creations would hand one person every table in it.

Naming an owner does more than grant admin: it makes the record **governed**,
which closes it to everybody the rules do not mention. That is why the
conditions above matter more than the defaults do, and why
`SPARQUET_STUDIO_NEW_RESOURCE_DEFAULT=off` exists for a deployment that would
rather keep the older, open behaviour.

### Columns as securables

A column is a catalog object of its own, not a line in a table's description. It
carries a description, a classification and tags, it is stored inside the
dataset's catalog entry keyed by its lower-cased name, and it is addressed by
the rules as `<dataset key>#<column>` — `/lake/silver/orders#cpf`,
`main.silver.orders#cpf`.

The `#` is deliberate: a dataset address already nests with `/` and `.`, and a
column has to stay a column of `main.silver.orders` rather than becoming a
fourth level of the name.

What a rule on a column can do is **narrow**, never widen:

- the chain is the column, then every column, then the whole dataset chain —
  the table, the folders above it, and the tags on both;
- a `deny` on a column therefore beats an `allow` on the table, which is how
  "the whole table except the document number" gets said at all;
- an `allow` on a column cannot reopen a table that is denied above it, because
  a deny anywhere in the chain still wins.

A column carries its **own** tags and classification into that chain, as
`tag/pii` and `tag/classification:restricted`, alongside its table's. One rule
on `tag/classification:restricted` therefore governs restricted columns and
restricted tables alike, including the ones classified next month.

Two things a column deliberately is not:

- **ownable.** Ownership is admin that no deny can reach, and a column is handed
  over with its table. Assign the owner on the dataset.
- **automatic.** Only a column somebody described in the catalog is offered in
  the permissions screen. A rule on a column name nobody committed to would be a
  rule on a name the next schema change may take away.

The dataset's classification badge shows the **effective** one: a table is as
restricted as the most restricted column in it. A table marked `internal` whose
`cpf` column is `restricted` reads as `restricted` everywhere the badge is
shown, and the column's own row says which column raised it.

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
and every call still has to carry a credential — the token, or a session.

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

## The assistant

The runner answers questions about Sparquet itself, at `POST /assistant/stream`.
It exists because the useful answers are the ones a browser cannot give: which
formats *this* installation registered, and whether *this* config actually
validates. Studio's other providers (Anthropic, OpenAI, Gemini) talk to a vendor
from the browser with the user's own key and never learn any of that.

**Nothing leaves the machine by default.** The default backend is
[Ollama](https://ollama.com) on `http://127.0.0.1:11434`, which needs no key and
no egress. Pull a model once and the assistant works:

```bash
ollama pull qwen3:8b
```

Any pulled model is offered — `GET /assistant` lists what `/api/tags` reports.
`qwen3:8b` is the default because it fits in 8 GB of RAM *and* was the only
small model measured calling this runner's tools every time (see
[Omnigent](#omnigent) for the numbers). A machine with more room does better
with `qwen3:30b`.

`/api/chat` is used rather than Ollama's OpenAI-compatible `/v1` route, because
only the native one reports `prompt_eval_count` and `eval_count` while
streaming. Billing that cannot see tokens is billing that cannot answer "what
did this cost", which is the whole point of recording local turns at all.

### Tools

The assistant can call the runner, and the answers say which tools ran, because
"checked the installed formats" and "recalled them" are different claims about
how much to trust the reply:

| Tool | Answers with |
|---|---|
| `list_formats` | the read and write formats this installation registered, from the framework's own factory |
| `validate_config` | whether a pipeline JSON parses into a `PipelineConfig`, and the first error if not |

Tool calls and their results are rebuilt on the runner every turn and are never
accepted from the caller. A transcript the browser could write is a transcript
that can tell the model a configuration validated when it did not; `POST
/assistant/stream` therefore takes `user` and `assistant` text and nothing else.
A model that calls tools six times without answering is stopped and says so.

### Omnigent

`SPARQUET_STUDIO_ASSISTANT=omnigent` swaps the loop for
[Omnigent](https://pypi.org/project/omnigent/) (Apache-2.0), keeping the same
tools, the same streaming contract and the same metering. It is an optional
dependency of the runner and is imported lazily — absent, `GET /assistant`
answers `available: false` with the hint that installs it:

```bash
pip install omnigent      # Python 3.12+ only
```

That floor is why it is optional rather than required: the framework itself
supports 3.9, and a runner on 3.10 must keep working. It is not a fork in the
road, though — the framework is tested on 3.12 and 3.13 as well, so a single
interpreter carries both and `pip install sparquet omnigent` resolves without a
conflict.

Omnigent is pointed at the same Ollama by default, so choosing it changes the
agent loop without starting to spend money. Three things about the wiring follow
from what the package does rather than from what its own documentation
describes, and every one of them fails quietly when guessed wrong:

| Wiring | Why |
|---|---|
| `OpenAIAgentsSDKExecutor(api_key=…, base_url_override=…)` | `auth: {type: api_key, base_url}` is the *spec* syntax. The constructor takes plain keywords and rejects an `auth` keyword outright. |
| `use_responses=False` | It defaults to True, which is OpenAI's `/responses` endpoint. Ollama does not implement it, so the default breaks the backend this is pointed at by default. |
| Flat tool specs rather than OpenAI-shaped ones | Omnigent reads `name`, `description` and `parameters` off the top of each spec. In the nested shape it finds no name, and a spec with no name is skipped rather than refused — the symptom is an assistant answering from memory, not an error anybody sees. |
| `_tool_executor`, and a fresh `session_id` on every turn | Tool calls are dispatched through the attribute Omnigent's own runtime adapter assigns; without it every call comes back "no tool executor" and the model reasons on from a failure it cannot fix. The session key is what Omnigent replays history against, and our transcript already arrives whole from the browser — one shared key would show the model its own past twice, and on a runner with more than one user, somebody else's. |

**Token counts are whatever the endpoint volunteers.** The Agents SDK asks for
them (`stream_options: {"include_usage": true}`) only when it recognises the
base URL as OpenAI's own, Omnigent exposes no seam to set it, and a local server
does not volunteer usage unasked — so an Ollama turn is recorded with zero in and
zero out. Billing renders that as *No provider reported tokens*, not as a free
turn: the turn count and the duration are still real, and those are the numbers
that show an assistant moved in-house. The runner briefly patched the SDK's own
`ChatCmplHelpers.get_stream_options_param` to get the counts back; reaching into
a third-party class at import time, in a long-lived server, to improve a figure
nobody bills on was the wrong trade, and it is gone.

`server/test_omnigent_live.py` is what keeps that table honest. It skips when
the package is absent and, when it is there, drives the installed executor
through a real turn — tool call, tool result, streamed answer, usage — against a
stub that speaks the OpenAI streaming shape in place of model weights.

The prompt and the tools are this module's, in `server/assistant.py`. There is no
agent YAML: nothing here loads one, and a team that outgrows the built-in prompt
edits that file.

**Pick a model that calls tools.** The loop is only as good as the weights
behind it, and `tools` in `ollama show` is not evidence that they will be used.
A model that does not use them answers by *printing*
`{"name": "validate_config", "arguments": {…}}` as prose: nothing is dispatched,
the user reads a JSON blob, and the runner meters a turn with zero tool calls.
The same question — one that cannot be answered without `validate_config` —
through this runner's own prompt and tools:

| Model | Called the tool | Note |
|---|---|---|
| `qwen3:8b` | 5 of 5 | The default. Thinks first, so a turn is tens of seconds. |
| `llama3-groq-tool-use:8b` | 3 of 6 | Fine-tuned for tool use and still a coin flip. Fast when it works. |
| `llama3.1:8b` | 0 of 2 | Emits Llama's `{"name", "parameters"}` shape as text. |
| `qwen2.5-coder:7b` | 0 of 2 | Best at JSON of the four, and never calls anything. |

Not a property of the route or of the adapter: measured against Ollama's raw
`/v1/chat/completions` and its native `/api/chat`, streaming and not, at
temperature 0, the failures put the call in `content` and leave `tool_calls`
empty. If the assistant answers with JSON instead of running anything, change
the model, not the wiring.

### What a turn costs

**Every turn is recorded; a local turn is recorded at zero.** The credit ledger
writes no row when nothing was charged — it records movements of money — so
assistant turns are kept in their own `assist_usage` table instead, and a team
that moved its assistant onto its own hardware watches the turns climb while the
charge stays flat. That is the evidence the move worked, and a single
usage-and-cost figure would hide it.

A turn that went to a paid provider costs `SPARQUET_STUDIO_CREDITS_PER_ASSIST`
(default 1) and also writes a ledger row, under the same account, the same
enforcement switch and the same free monthly allowance as a run.

`assistant:Ask` is granted to the builtin `editor` and `operator` roles and not
to `viewer`: asking spends the runner's CPU, and a read-only account should not
be able to.

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
| `SPARQUET_STUDIO_CREDITS_PER_ASSIST` | `1` | Credits one assistant turn costs when it was **not** answered on this machine. A local turn is recorded at zero whatever this says. |
| `SPARQUET_STUDIO_ASSISTANT` | `ollama` | Which runtime answers questions. `omnigent` uses Omnigent instead; `off`/`none`/`disabled` turns the assistant off and makes the routes say so rather than never replying. |
| `SPARQUET_STUDIO_OLLAMA_URL` | `http://127.0.0.1:11434` | Where Ollama is. Both backends talk to it. |
| `SPARQUET_STUDIO_ASSISTANT_MODEL` | `qwen3:8b` | Model used when the caller names none. Free text — any pulled model works, but pick one that calls tools. |
| `SPARQUET_STUDIO_ASSISTANT_KEY` | unset | API key for the assistant endpoint, for the deployment that points `SPARQUET_STUDIO_OLLAMA_URL` at something that wants one. Ollama itself ignores it. |
| `SPARQUET_STUDIO_SECRET_KEY` | unset | Master key the `local` connection secrets are encrypted with. Without it the runner still boots and still serves `env` secrets — it refuses only to seal or open a `local` one. Changing it makes every existing `local` secret unreadable. |
| `SPARQUET_STUDIO_NEW_RESOURCE_DEFAULT` | `creator+team` | What a newly created Job, Pipeline, Workflow, saved query, dataset or secret is governed by. `creator+team` makes its author the owner and gives their team `write` (`read` on a secret). `creator` writes the ownership only. `off` leaves new records ungoverned. See **Default access**. |
| `SPARQUET_STUDIO_WORKSPACE` | unset | Pins the library directory. Set it and the interface may not change it — a deployment that decides centrally decides centrally. Unset, the runner uses what was chosen in Settings, falling back to the per-user default. |
| `SPARQUET_HOME` | `%APPDATA%\Sparquet` on Windows, `$XDG_DATA_HOME/sparquet` (or `~/.local/share/sparquet`) elsewhere | Per-user data directory. Holds `studio.json` and, unless told otherwise, `workspace/` — the library. |
| `SPARQUET_STUDIO_CREDITS_PROVIDER` | unset (SQLite) | `module:factory` building the credit ledger instead of the local one. See **Replaceable pieces**. |
| `SPARQUET_STUDIO_AUTH_PROVIDER` | unset (SQLite) | `module:factory` building the identity store instead of the local one. |
| `SPARQUET_STUDIO_WORKSPACE_PROVIDER` | unset (files on disk) | `module:factory` building the library store instead of the local one. |
| `SPARQUET_STUDIO_WARM_SPARK` | unset (cold) | `on`/`1`/`true`/`yes` builds the SparkSession at start-up instead of on the first request. See **A warm SparkSession**. |
| `SPARQUET_STUDIO_HISTORY_PURGE` | on | `off`/`0`/`false`/`no` stops the runner from applying retention on its own. `POST /runs/purge` still works. |
| `SPARQUET_STUDIO_HISTORY_DETAIL_DAYS` | `30` | After this many days a run loses its logs, its steps and its stored JSON, and keeps its row. |
| `SPARQUET_STUDIO_HISTORY_MAX_DAYS` | `365` | After this many days the run row itself goes — but only with `SPARQUET_STUDIO_HISTORY_DELETE` on. |
| `SPARQUET_STUDIO_HISTORY_KEEP_RUNS` | `10` | The newest N executions of each Job and each Pipeline are never expired, however old they are. |
| `SPARQUET_STUDIO_HISTORY_DELETE` | unset (thin only) | `on`/`1`/`true`/`yes` lets the second stage delete run rows. Off, history thins but never shrinks in row count. |
| `SPARQUET_STUDIO_MONITORS` | on | `off`/`0`/`false`/`no` stops the sweep thread. The rules stay, and `POST /monitors/evaluate` still evaluates them on demand. |
| `SPARQUET_STUDIO_MONITOR_INTERVAL` | `60` | Seconds between sweeps. Floored at 5 — a sweep is cheap, but not free. |
| `SPARQUET_STUDIO_MONITORS_DB` | `server/data/monitors.sqlite3` | SQLite file holding the alert rules, their current verdicts and the transitions they logged. |
| `SPARQUET_STUDIO_ALERT_WEBHOOK` | unset | URL the runner POSTs to when a rule starts or stops firing. Unset, alerts live only in the interface and in the log. |
| `SPARQUET_STUDIO_SCHEDULER` | on | `off`/`0`/`false`/`no` stops the sweep thread. The schedules stay in the library, and `POST /schedules/evaluate` still fires what is due. |
| `SPARQUET_STUDIO_SCHEDULER_INTERVAL` | `30` | Seconds between sweeps. Floored at 5. Cron resolution is one minute, so anything under 60 only buys punctuality. |
| `SPARQUET_STUDIO_SCHEDULER_GRACE` | `900` | How far back a sweep looks for an occurrence it missed, in seconds. `0` means a runner that was down never catches up at all. |

## A warm SparkSession

The first request that needs Spark pays for a JVM launch, for the connector jars
being resolved and for a session being configured — tens of seconds before any
work starts, all of it charged to whoever clicked Run first. With
`SPARQUET_STUDIO_WARM_SPARK=on` the runner does that at start-up instead, on a
daemon thread, so it overlaps with somebody opening the interface:

```bash
SPARQUET_STUDIO_WARM_SPARK=on uvicorn main:app --port 8787
```

**It warms with what the library declares, not with a plain session.** Connector
jars and SQL extensions take effect only when a session is *created*, so a plain
warm session would be rebuilt — JVM and all — by the first Delta query, and the
wait would come back exactly where it was meant to be gone. The runner reads the
`spark.configs` block of every saved Job and builds one session out of the union:
list-valued keys (`spark.jars.packages`, `spark.sql.extensions`, `spark.plugins`
and the other two) are merged, so a library with a Delta Job and an Iceberg Job
warms a session that opens both. A library that declares nothing warms a plain
session, which is the right answer for a Parquet-only runner: it downloads no jar
it has no use for.

It is off by default and that asymmetry with the purge thread is deliberate:
importing this module must not cost a JVM. The tests import it, and so does
anything that inspects the app. Turn it on for the process that serves the
Studio, where the session is going to be built anyway and the only question is
who waits for it.

A warm-up that fails is logged and swallowed — `Spark warm-up failed, the first
query will build the session` — because a runner that cannot build a session must
still start, and the request that needs one will fail with its own, much better
message. Adding a Job with a new connector after start-up is not a problem
either: `_ensure_framework` rebuilds the session when a request needs a
creation-time config the live one lacks, which is what it already did cold.

**What it costs.** A session that nobody uses still holds a JVM and its driver
heap for as long as the runner is up. On a laptop that is the price of not
waiting; on a shared box with several runners, it is worth leaving off.

## Monitoring and alerts

The run list answers "what happened this afternoon". It is ordered by time, so it
is structurally incapable of answering "which Job stopped running last Tuesday" —
a Job that has not run is not near the top of a list ordered by time, it is
nowhere in it. This is the other half: one row per Job whether or not it has ever
run, and rules that raise a hand without anybody looking.

**Health** (`GET /health/jobs`) is computed from the history, not stored
separately. For each Job in the catalog it reports the last run, the last
*successful* run, how many failures in a row, and the durations and row counts of
the recent successful runs — the same numbers a median rule compares against, so
a firing alert can be read against the shape that produced it.

**A rule** is a question asked of those facts, one of four:

| `kind` | Fires when | `threshold` is |
|---|---|---|
| `failed` | the last N runs all failed | N runs |
| `late` | no successful run for N minutes | minutes |
| `duration` | the last run took longer than the ceiling | milliseconds, or a multiple of the median |
| `volume` | the last run wrote fewer rows than the floor | rows, or a multiple of the median |

`baseline` picks which of the last two columns applies: `absolute` compares
against the number, `median` against the Job's own median times the number. The
asymmetry in the units is deliberate and is written on the form — a duration
threshold in minutes would round every sub-minute Job to zero, and a silence
threshold in milliseconds is nobody's mental model.

`job_id` names the Job the rule watches, or `*` for every Job in the library —
**including ones added later**, which is the point: a rule written once keeps
covering a library that grows.

`late` measures from the last **success**, not from the last run. A Job failing
every ten minutes is running constantly and is not fine.

**The sweep** runs on a daemon thread every `SPARQUET_STUDIO_MONITOR_INTERVAL`
seconds and is on by default — that asymmetry with the warm-up is deliberate too:
a sweep is one query against a SQLite file, and a rule nobody evaluates is not a
rule. It records the verdict per (rule, Job) and writes an event only on a
**transition**, so the log reads "this started firing / this cleared" rather than
one line a minute for as long as something is broken.

Changing a rule's question drops its recorded verdict, so a threshold edit clears
an alert that was answering the old question instead of leaving it on screen
asserting something nothing is checking. Deleting a rule takes its verdicts and
its events with it: an event says "this started firing" and nothing else — what it
was firing *about* lives in the rule, so an event whose rule is gone is a
timestamp nobody can read. The record worth keeping after the fact is the run,
and that is in the history.

**Webhook.** With `SPARQUET_STUDIO_ALERT_WEBHOOK` set, each transition is POSTed
as JSON — `monitor`, `name`, `kind`, `job_id`, `firing`, `reason`, `value`,
`baseline`, `run_id`, `at`, `rule`, and a ready-made `text` (`[FIRING] … — …`) so
a destination that shows one field shows a useful one — with `urllib` from the standard
library, on the sweep thread, with a short timeout. A webhook that fails is
logged and swallowed; an alerting path that can take the runner down with it is
worse than no alerting path.

**Prometheus.** `GET /metrics` renders the same facts in the text exposition
format, no dependency involved:

```
sparquet_job_last_run_success{job="j1",name="Daily sales"} 0
sparquet_job_consecutive_failures{job="j1",name="Daily sales"} 2
sparquet_monitor_firing{monitor="m1",kind="failed",job="j1"} 1
sparquet_monitors_firing 1
```

**Everything there is a gauge, including the counts.** `sparquet_job_runs_recorded`
is the number of runs *the history still holds*, and retention deletes rows — a
`_total` counter that goes down silently makes `rate()` wrong, and a wrong rate is
worse than an absent one. Scrape it for the state, and take run rates from the
history endpoints, which know what they purged.

Reading any of this needs `monitoring:Read` — which includes `/metrics`, so the
scraper needs a token like everything else. Creating, editing and deleting rules
needs `monitoring:Manage`. `POST /monitors/evaluate` deliberately needs only
`Read`: it is what somebody who has just fixed a Job presses to find out whether
the alert cleared, and that is not a change to anything.

## Schedules

A Job or a Pipeline can carry a `schedule` block, and the runner fires it without
anybody pressing anything.

**The schedule lives in the library record**, next to the Job it belongs to — not
in a table on the side:

```json
{ "id": "j1", "name": "Daily sales", "schedule": {
  "cron": "0 6 * * *", "timezone": "America/Sao_Paulo",
  "enabled": true, "runAs": "ana" } }
```

That placement is the whole design. The schedule is committed with the project, it
travels through git with the Job it schedules, a review shows it in the diff, and a
clone of the repository is already scheduled. Nothing has to be re-entered on the
machine that runs it, and there is no second store to keep in step with the first.

**The expression is a five-field cron** — minute, hour, day of month, month, day of
week — with `*`, lists (`0,30`), ranges (`9-17`), steps (`*/15`, `1-5/2`), month
names (`jan`..`dec`) and weekday names (`sun`..`sat`). Sunday is both `0` and `7`.
When the day-of-month and the day-of-week fields are both restricted the schedule
fires on either, which is what every other cron does. Six-field expressions are
refused rather than interpreted: reading `0 0 6 * * *` as minute-zero-of-hour-zero
would run a daily job every minute. Macros like `@daily` are refused for the same
reason — better a schedule that says it is invalid than one that quietly never runs.

**`timezone`** is an IANA name (`America/Sao_Paulo`), or `UTC`, or `local` — the
default — for the clock of the machine running the runner. The arithmetic runs in
that zone, so `0 6 * * *` stays at six in the morning across a daylight-saving
change instead of drifting to five or seven. An unknown zone name logs a warning
and falls back to local time, because a schedule that fires an hour off is a smaller
problem than one that never fires.

**A missed run is not queued.** If the runner was off between two and six, the six
o'clock occurrence runs when it comes back only if it is still inside
`SPARQUET_STUDIO_SCHEDULER_GRACE`; everything older is dropped. A night of missed
hourly runs collapses into one, never twelve. The runner keeps no memory of what it
fired across restarts on purpose — that memory would be a claim about a machine
that was not running.

**An overlapping run is skipped, not queued.** A schedule that comes due while the
previous execution still holds the run lock is reported with the 409 as its error
and its occurrence is spent. The next one runs normally.

**Identity.** `runAs` names an existing user, and the run carries that user's
permissions — the same check as if they had pressed Run. A schedule can never
outlive the access of whoever wrote it: disable the user and the schedule stops
firing. There is no service account here on purpose, since an identity that belongs
to nobody is an identity nobody notices still has access. On a runner with no users
configured the schedule runs as the token principal, like every other request.

**A scheduled Pipeline runs its stages in the order the canvas shows** — the same
topological sort, with the same tie-break on stage name, that the Studio uses to
draw the sequence. A scheduled Pipeline that ordered its stages differently from
the one the author saw on screen would be a different Pipeline. Stages in a cycle
are logged and left out, exactly as the interface reports them.

**What this is not.** The sweep belongs to one runner process. Two runners pointed
at the same workspace both fire, and nothing here coordinates them; that is the
job of a real scheduler, and the cloud backlog carries it. The honest limit of the
local scheduler is one machine, and the point of the grace window is to make its
behaviour after a restart predictable instead of pretending it never stopped.

Reading `GET /schedules` needs `workspace:Read` — a schedule is part of the library
record and anybody who can see the Job can see when it runs. `POST /schedules/evaluate`
needs `run:Execute`, because it starts executions.

## Connection secrets

A JDBC URL, the user and the password that go with it, a token — the things a Job
needs to open a source and must not carry inside its JSON. They live here, on the
runner, and a Job refers to one by writing `{secret:name/field}` wherever the
value would have gone:

```json
{ "format": "postgres", "path": "public.clientes",
  "options": { "url": "{secret:pg-prod/url}",
               "user": "{secret:pg-prod/user}",
               "password": "{secret:pg-prod/password}" } }
```

**The framework knows nothing about this.** `apply_template` matches
`(?<!\{)\{(\w+)\}(?!\})`, and `\w` covers neither `:` nor `/`, so the reference
is invisible to `{param}` substitution and to the `{{var}}` the transformation
engine resolves. The runner replaces it on the way to Spark and hands the
framework a finished document; nothing in `sparquet/` changed to make this work.

What is stored is the unresolved document, everywhere it matters: the run
history, the lineage, the config hash the credits are keyed by, and the JSON the
AI is shown. Only the copy given to Spark carries values.

### Where the material is

| Provider | What it is | When |
|---|---|---|
| `local` | Encrypted in the library's meta file with Fernet, the key from `SPARQUET_STUDIO_SECRET_KEY` and a per-store scrypt salt. Needs `cryptography`. | One team, one runner, a laptop. |
| `env` | A named environment variable this process reads. | Everything else — Vault, AWS/GCP/Azure secret managers, Kubernetes and CI all end by injecting a variable, and the runner only has to know the name. |

A third provider is one entry in `vault._RESOLVERS`: a callable taking the stored
binding and returning the value. Nothing else in the runner changes.

### Access

`secret` is a securable like a dataset or a Job, with an owner and grants over
it, and the levels mean something specific:

- **read** — a run of yours may *use* it. It never means "may be looked at": no
  endpoint returns a value at any level, to anybody, including the owner.
- **write** — rotate a field, retag it, delete it.
- **admin** — decide who else may use it.

Tags work as they do everywhere else, so a deny on `tag/pii` closes the
credential as well as the tables it reaches. A secret the caller may not reach is
absent from `GET /secrets` rather than shown greyed out: the name of a credential
is itself information.

Two things are deliberately sealed. `PUT /workspace/meta/secrets` is refused for
everyone — a blanket write would skip the encryption, the per-secret access check
and the keep-what-you-did-not-send behaviour of a rotation. And `GET /workspace`,
which is how Studio loads, strips the record out: `workspace:Read` is held by
anyone who may open the library, and the browser has no business holding the
ciphertext, the salt or the `env` bindings.

### Masking

A JDBC driver that cannot connect quotes the whole URL it tried, password
included. Every resolved value is therefore scrubbed out of the run response, out
of each SSE event **as it leaves the queue** — which is the same point the run
history is written from, so one pass covers the screen and the database — and out
of the error text of `/dataset/schema` and `/query`.

### `GET /secrets`

Every secret this caller may reach: `name`, `provider`, `description`, `tags`,
`fields` (names only), `binding` (for `env`, which variable each field reads),
`updated_at`, `updated_by`, and the caller's own `governed`/`level`/`owned`.
Needs `secrets:Read`.

### `PUT /secrets/{name}`

Creates or changes one. `values` is a patch over the fields — a value sets it,
`null` removes it, a field left out keeps what it had — which is what makes
rotating a single password possible from a browser that never read the others.
Needs `secrets:Write`, and `write` on the secret when it already exists. Creating
one makes the caller its owner.

```json
{ "provider": "env", "description": "Production Postgres", "tags": ["pii"],
  "values": { "url": "PG_URL", "user": "PG_USER", "password": "PG_PASSWORD" } }
```

### `DELETE /secrets/{name}` · `POST /secrets/{name}/check`

Deleting needs `write` on the secret. The check answers whether every field still
resolves — the master key is there, the variable is set — field by field, without
returning any of them. It is **not** a connection test: reaching the database
needs the driver on the classpath and a network route, and a failure there says
nothing about the secret.

## Replaceable pieces

Three things this runner does are **policy, not mechanism**: where credits are
kept, where identity is kept, and where the library is kept. The open runner
answers all three with SQLite and files on the operator's own disk, which is the
right answer for one team on one machine and the wrong one for a hosted service —
a tenant's ledger cannot live in a file another tenant's process can open, and
identity cannot be a password column when the customer arrives with an identity
provider of their own.

The wrong fix is a fork: it diverges on the first bug fixed on only one side, and
from then on every feature is written twice. So each of the three is a **slot** —
a `Protocol` in the module that owns it, the local class as the default, and an
environment variable naming a factory to use instead.

| Slot | Protocol | Local default | Variable |
|---|---|---|---|
| Credits | `credits.CreditLedger` | `credits.CreditStore` (SQLite) | `SPARQUET_STUDIO_CREDITS_PROVIDER` |
| Identity | `auth.IdentityStore` | `auth.AuthStore` (SQLite) | `SPARQUET_STUDIO_AUTH_PROVIDER` |
| Library | `workspace.WorkspaceStore` | `workspace.FileWorkspaceStore` (files) | `SPARQUET_STUDIO_WORKSPACE_PROVIDER` |

```bash
export SPARQUET_STUDIO_CREDITS_PROVIDER="sparquet_cloud.credits:build"
export SPARQUET_STUDIO_AUTH_PROVIDER="sparquet_cloud.identity:build"
export SPARQUET_STUDIO_WORKSPACE_PROVIDER="sparquet_cloud.library:build"
```

A factory takes no arguments and returns something satisfying the protocol.
Anything importable works: the alternative is a package on the `PYTHONPATH`, not
a patched copy of `main.py`. `GET /health` reports what was loaded.

**A configured provider that fails to load stops the process.** Logging the
failure and carrying on with the local default is exactly the wrong thing: a
hosted runner that quietly falls back to SQLite puts every tenant's ledger and
every tenant's identity in one file on one disk. That is a data isolation
failure, not a degraded mode, and it must never be something an operator has to
notice in a log.

What an implementation owes beyond the method signatures is written on each
protocol's docstring — that a reservation is settled or released and never left
open, that what was charged is frozen at charge time, that `has_users()` is the
switch between token-only and session mode, that `resolve_session()` is the whole
authorization surface. Those are the promises the endpoints are written against.

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
  "credits_enforced": false,
  "providers": { "credits": "local", "auth": "local", "workspace": "local" }
}
```

`status` is `degraded` when pyspark or the framework cannot be found. This
endpoint never imports pyspark, so it stays fast, and it needs no token — it is
how Studio discovers the runner before it has one. `auth_required` is absent on
runners older than 0.2.0, which accepted unauthenticated `/run` calls.
`login_required` says whether this runner has users: `false` means the shared
token is the identity, `true` means a session is needed on top of it.
`credits_enforced` says whether a balance can refuse a run; `false` means credits
are being counted but nothing is blocked. `providers` names which implementation
is answering each replaceable slot — `local` for the SQLite-and-files default,
otherwise the `module:factory` that was injected; an operator debugging a hosted
runner should not have to infer that from behaviour.

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

Step markers come from the framework's own log, and what they report differs by
kind: `input`, `validation` and `output` did touch data, but a `transformation`
marker means the operation was **added to the plan**, not that rows went through
it — Spark is lazy and nothing here forces an action to find out. So a
transformation whose error only the executor can find is reported as succeeded,
and the failure arrives on the next step that forces one. Reporting otherwise
would mean a `count()` after every transformation, re-reading the input once per
step.

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

A stage names **one** of two things: `pipeline`, the JSON to run inline, or
`path`, a `.json` in the library relative to its root:

```json
{ "id": "s3", "name": "gold", "path": "vendas/jobs/gold.json" }
```

The runner reads that file when the flow starts — nothing is imported and nothing
is cached, so an edit made outside the Studio takes effect on the next run, and
the file stays the source. Both together, or neither, is a `422` naming the stage.
A path that does not exist, is not JSON, or climbs out of the library root is a
`400`, raised **before** anything is charged, locked or executed: a Pipeline that
dies halfway with earlier stages already written is worse than one that never
started. Paths are always relative — an absolute one names a directory that
exists on exactly one machine.

A stage that names a `path` and no `job_id` belongs to the file itself: the runner
gives it the catalog identity `file:<relative path>` and files the execution under
that. The identity is derived from the path — normalised to forward slashes, no
leading slash — so two runs of the same file are two runs of the same object, and
a Windows client and a Linux one do not create two owners for it. The record is
created on the first authorized run, named after the `name` the JSON declares (the
file name if it declares none), and never overwritten afterwards: a name,
description or tags somebody put on it survive every later run. That is what makes
such a file visible in `GET /health/jobs`, alertable by a monitor rule, and
taggable for billing. It is deliberately *not* written into `job_id`, which is what
the IAM resource rules are matched against — a file that runs today under some
policy keeps running under it.

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

### `POST /query/validate`

Requires `catalog:Query`, same as `/query`.

```json
{ "sql": "select * form orders" }
```

→ `{ "checked": true, "ok": false, "message": "[PARSE_SYNTAX_ERROR] Syntax error at or near 'form'. SQLSTATE: 42601 (line 1, pos 9)", "line": 1, "column": 9, "reason": "" }`

The syntax check behind the SQL editor's markers. It hands the statement to
Spark's own parser — `sessionState().sqlParser().parsePlan` — and stops there:
the logical plan is built and thrown away, so nothing is read, no job starts and
**the tables do not have to exist**. That last part is what lets the editor
validate while a statement is still being typed, before its views are
registered.

The check comes from the parser that will execute the query on purpose. A
hand-written one in the browser would disagree with Spark about its own dialect
— lateral views, `QUALIFY`, backtick identifiers, interval literals — and an
editor that underlines valid SQL is worse than one that underlines nothing.

`checked` is the field to read first. It answers "was the parser actually
asked", and only when it is `true` does `ok` mean anything:

| `checked` | When | What the editor does |
|---|---|---|
| `false` | The buffer is empty or holds only comments; no SparkSession is up yet; this Spark exposes no parser | Marks nothing. `reason` says which of those it was. |
| `true`, `ok: true` | The parser accepted the statement | Clears its markers. Says nothing about whether the tables exist. |
| `true`, `ok: false` | The parser refused it | Underlines `line`/`column` with `message`. |

**It never builds a SparkSession.** A JVM launch is not something a keystroke
should pay for, so a runner with no live session answers `checked: false` rather
than starting one — the same statement is checked as soon as a query, a schema
read or a run has brought a session up.

`line` is 1-based and `column` is 0-based, exactly as Spark reports them, and
either can be `null` for an error that names no position. `message` is the first
paragraph of the parser's text: the `== SQL ==` dump under it is already on the
screen the marker is drawn on.

The read-only rule of `/query` is applied here too, before the parser sees the
statement, and reported the same way as a syntax error — `checked: true`,
`ok: false`, line 1 — rather than as a `400`. A `DELETE` typed into the editor
is something to underline, not a failed request.

### `GET /query/history` · `DELETE /query/history` · `POST /query/history/move`

All three require `catalog:Query`, same as `/query`.

```
GET /query/history?saved_query_id=q7a2&limit=50
GET /query/history?tab=t-9f1
```

→ `{ "runs": [ { "id": "…", "at": "2026-02-11T18:04:22Z", "sql": "SELECT …", "limit": 20, "elapsed_ms": 812, "rows": 20, "truncated": true, "error": null, "run_as": "ana" } ] }`

What this query has been run as, newest first. The runs are written by `/query`
itself as it executes, not posted by a client: the runner is the side that knows
how long a statement took, how many rows came back, whether the result was cut
and what the failure said. A failed run is recorded too, with the first line of
the message in `error` — including a refusal raised before Spark sees the
statement, such as a write or a denied table.

**Which history a run lands in is decided by the request.** `/query` takes two
optional fields for it, and they are read in this order:

| The request names | Filed under | Who can read it |
|---|---|---|
| `saved_query_id` | the saved query | everyone who holds `read` on that `query` securable |
| `tab` only | that tab, for that principal | that principal alone |
| neither | nothing — the query runs and is not recorded | — |

A saved query is a file two people can open, so its history is shared and every
run says who made it in `run_as`. A buffer nobody has saved has no file to share:
its key is `t:<principal>:<tab>`, built by the runner rather than filtered on the
way out, so another principal cannot address it even by guessing the tab id.

The third row is the useful one for anything that queries on somebody's behalf —
the catalog's row sample, say. It goes through `/query` like everything else and
names neither field, so reading a dataset's first rows does not appear in the
person's query history.

`GET` refuses a request that names neither with a `400`, and a `saved_query_id`
the caller cannot read with a `403` — the history of a closed query is as closed
as the query. `limit` caps the answer; the store keeps the 100 most recent runs
per query and drops the rest on insert.

`DELETE /query/history` takes the same two parameters and answers
`{ "removed": <n> }`.

```json
POST /query/history/move
{ "tab": "t-9f1", "saved_query_id": "q7a2" }
```

→ `{ "moved": 6 }`

Carries a draft's runs onto the file it was just saved as, which is what the
editor calls the first time a tab is saved. Without it, saving would drop
everything run to get there — and those are the runs that explain the query.

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

### `GET /assistant`

Needs a token and no permission: Studio asks this to decide whether to offer the
runner's assistant at all, and a **403** here would read as a broken runner
rather than as an unconfigured one.

```json
{ "backend": "ollama", "available": true, "local": true,
  "model": "qwen2.5-coder:7b", "base_url": "http://127.0.0.1:11434",
  "models": ["qwen2.5-coder:7b", "llama3.1:8b"],
  "tools": ["list_formats", "validate_config"],
  "version": "", "hint": "", "error": "" }
```

`available: false` always carries `error` — what is wrong — and `hint` — what to
do about it, such as `ollama pull qwen2.5-coder:7b` or `pip install omnigent`.
`local` says whether a turn is going to cost anything, which is what Studio puts
next to the model name.

### `POST /assistant/stream`

Requires `assistant:Ask`. Server-sent events, same framing as `/run/stream`.

```json
{ "messages": [{ "role": "user", "content": "which formats can I write?" }],
  "instructions": "Answer with a JSON envelope the canvas can apply.",
  "model": "qwen2.5-coder:7b",
  "workflow_id": "w1" }
```

Only `user` and `assistant` messages are accepted. `model` overrides the
runner's default for this turn; `workflow_id` is what the question is about, so
the cost lands on the right line of the bill — optional, because a question
asked from the assistant screen belongs to no Workflow and saying so is more
honest than guessing.

`instructions` is how the caller shapes the answer, and it is **appended** to the
runner's prompt rather than replacing it — appended after, and cut at 24,000
characters. The part it cannot drop is the part that lists the tools this
installation has, which is the only reason to ask the runner instead of a
vendor, and which a browser cannot write because it does not know what is
installed. The Studio's canvas panel is what this is for: its prompt is built
from the catalog that drives the forms and asks for a very specific JSON
envelope back, so without this, pointing the panel at the runner would quietly
turn it into a chat that cannot propose anything.

| Event | Payload |
|---|---|
| `delta` | `{"text": "…"}` — the answer as it arrives |
| `tool` | `{"name": "list_formats", "args": {}, "result": …}` — a tool ran |
| `done` | `{"usage": {"model": …, "provider": …, "local": true, "inputTokens": 120, "outputTokens": 40, "toolCalls": 1, "durationMs": 2500}}` |
| `error` | `{"message": "…", "hint": "…"}` — the turn stopped, and why |

The turn is metered whether it finished or failed, so a run of failures is
visible in billing instead of free.

### `GET /credits/assist`

`?period=YYYY-MM&account_id=…&limit=20` — a month of assistant work and the most
recent turns behind it. Same scope rule as `/credits/usage`: your own team
always, somebody else's or the whole runner with `credits:Read`.

```json
{ "period": "2026-09", "scope": "t1",
  "turns": 3, "local_turns": 2, "remote_turns": 1,
  "input_tokens": 300, "output_tokens": 120, "tool_calls": 4,
  "charged": 1, "seconds": 7,
  "recent": [{ "id": "a1", "period": "2026-09", "backend": "ollama",
               "provider": "ollama", "model": "qwen2.5-coder:7b", "local": true,
               "input_tokens": 100, "output_tokens": 40, "tool_calls": 2,
               "duration_ms": 2500, "amount": 0,
               "created_at": "2026-09-16T10:00:00Z",
               "actor": "ana", "workflow_id": null }] }
```

`turns` counts everything and `charged` counts only what money moved for, which
is the pair that answers "is this thing costing us anything". This reads
`assist_usage`, not the ledger — the ledger has no row for a free turn.

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

### `GET /health/jobs`

`?samples=20` — one row per Job in the catalog, computed from the history. Needs
`monitoring:Read`. See **Monitoring and alerts**.

```json
[{ "job_id": "j1", "name": "Daily sales", "workflow_id": "w1",
   "last_run_id": "r9", "last_status": "failed",
   "last_started_at": "2026-09-13T09:00:00Z", "last_finished_at": "2026-09-13T09:01:00Z",
   "last_duration_ms": 60000, "last_rows_read": 10, "last_rows_written": 0,
   "last_error": "Path does not exist", "last_success_at": "2026-09-12T09:00:00Z",
   "consecutive_failures": 2, "runs": 7, "failures": 2,
   "durations": [60000, 58000], "volumes": [1000, 1100] }]
```

`durations` and `volumes` are the recent **successful** runs, newest first — what
a median rule compares against and what the interface draws.

### `GET /monitors` · `POST /monitors`

The alert rules. Reading needs `monitoring:Read`; creating needs
`monitoring:Manage`. Body of a create, everything but `kind` optional:

```json
{ "kind": "failed", "job_id": "*", "threshold": 2, "baseline": "absolute",
  "window": 10, "enabled": true, "name": "Anything that fails twice" }
```

The answer is the stored rule plus `rule`, the sentence the runner writes for it —
`any Job: last 2 consecutive runs failed`. Every surface says it the same because
only one of them composes it. A `kind`, `baseline` or `threshold` that makes no
sense is a `400`.

### `PATCH /monitors/{id}` · `DELETE /monitors/{id}`

Both need `monitoring:Manage`. The patch changes only the fields given and drops
the rule's recorded verdict when the question changes; the delete takes the
verdicts and the transition log with it. Missing id is a `404`.

### `GET /monitors/status`

`?firing_only=` — what every rule is currently saying, one row per Job it
watches. Needs `monitoring:Read`.

```json
[{ "monitor_id": "m1", "job_id": "j1", "firing": true,
   "reason": "2 consecutive failed runs.", "since": "2026-09-13T09:01:00Z",
   "checked_at": "2026-09-13T09:02:00Z", "value": 2, "baseline": 2,
   "run_id": "r9", "kind": "failed", "name": "Anything that fails twice",
   "rule": "any Job: last 2 consecutive runs failed", "job_name": "Daily sales" }]
```

`value` is what was measured and `baseline` is what it was measured against — for
a median rule that is the computed ceiling, not the raw median, so the two numbers
can be read side by side.

### `GET /monitors/events`

`?limit=50&monitor_id=` — the transitions, newest first: when each alert started
and when it cleared. Needs `monitoring:Read`. Nothing is written while a rule goes
on saying the same thing.

### `POST /monitors/evaluate`

Runs the sweep now instead of waiting for the timer, and answers with what
changed. Needs `monitoring:Read` — see **Monitoring and alerts** for why.

```json
{ "checked": 12, "firing": 1, "transitions": [{ "id": "e3", "monitor_id": "m1",
  "job_id": "j1", "at": "2026-09-13T09:02:00Z", "firing": false,
  "reason": "Last run succeeded.", "value": 0, "baseline": 2, "run_id": "r10" }] }
```

### `GET /schedules`

Every schedule in the library, with what the runner intends to do next and what it
did last. Needs `workspace:Read`. `next_fire` is `null` when the schedule is paused
or its expression cannot be read, and `error` then says why.

```json
[{ "kind": "job", "id": "j1", "name": "Daily sales", "cron": "0 6 * * *",
   "timezone": "America/Sao_Paulo", "enabled": true, "run_as": "ana",
   "workflow_id": "w1", "error": null, "rule": "0 6 * * * (America/Sao_Paulo)",
   "next_fire": "2026-09-14T09:00:00Z", "last_fire": "2026-09-13T09:00:00Z",
   "last_run_id": "r42", "last_status": "success" }]
```

### `POST /schedules/evaluate`

Runs the sweep now instead of waiting for the timer, and answers with what it
fired. Needs `run:Execute`. `started` is false when the run did not begin, and
`error` carries the reason — a Job with no compiled file, a run already in flight,
a `runAs` that no longer exists.

```json
{ "checked": 4, "fired": 1, "fires": [{ "kind": "job", "id": "j1",
  "name": "Daily sales", "due_at": "2026-09-13T09:00:00Z", "started": true,
  "run_id": "r42", "error": null }] }
```

### `GET /metrics`

Prometheus text exposition of the health and the rules. Needs `monitoring:Read`,
so the scraper carries a token. Everything is a gauge — see **Monitoring and
alerts**.

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

The **first** write of an id also claims it under **Default access** above; later
writes of the same id do not, so editing somebody's Job is not a way to take it.

### `DELETE /workspace/{kind}/{record_id}`

Removes both files and soft-deletes the catalog row → `{ "deleted": true }`.
Soft, because its executions still point at it.

### `PUT` / `DELETE /workspace/meta/{key}`

Small values that belong to the library rather than to a record — which storage
version wrote it, whether the examples were seeded — kept in `.studio/meta.json`.
Body for `PUT`: `{ "value": <anything JSON> }`. They travel with the workspace so
a second checkout does not re-seed or re-migrate a library that is current.

`catalog` is the exception to "small": it holds every dataset annotation, and a
`PUT` of it claims the addresses it added that were not in the stored map — see
**Default access**. `secrets` is refused outright; it is written only through the
endpoints above.

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
nobody loses one), `default` (the per-user directory). A fifth value, `provider`,
means the deployment injected a store of its own (see **Replaceable pieces**):
there is no local directory, `root` is whatever that store calls itself, and
`locked` is true. Somebody who cannot find
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
answer is the same shape as `GET`. Refusals: `409` when the environment pinned it
or a store was injected, `400` for a relative path, for a directory that cannot be created or written,
and for **any path inside the runner's own source tree** — a checkout is code, it
gets pulled, reset and deleted, and a library in one is lost to the first `git
clean` or committed by accident long before that.

Changing the root **copies nothing**. The store is rebound and the runner starts
reading and writing the new place, which is what makes this the way to *adopt* a
directory that already holds a library. Moving files is the operator's job: a
half-finished copy with no way back is worse than a move nobody made.

### `GET /workspace/files`

Every runnable JSON in the library, for a stage that wants to point at a file
rather than at a Job. Needs **`workspace:Read`**.

```json
{
  "root": "/home/ana/.local/share/sparquet/workspace",
  "files": [
    { "path": "vendas/jobs/ingestao.json", "name": "ingestao",
      "size": 812, "modified": 1756400000,
      "owner_kind": "job", "owner_id": "j1" },
    { "path": "legado/limpeza.json", "name": "limpeza",
      "size": 240, "modified": 1756300000,
      "owner_kind": null, "owner_id": null }
  ]
}
```

Paths are relative to `root`, forward slashes on every platform. The editor's own
state (`.studio/`), anything hidden, and the half-written `.tmp-*.json` of a save
in flight are not listed: none of them is something to run.

`owner_kind`/`owner_id` name the Studio record the file is the artefact of, and
come from `.studio/index.json` rather than from the shape of the path — two
records whose names slugify the same, or one renamed since it was written, would
each give a wrong owner if the layout were re-derived, and this is the field that
decides whether a file may be deleted. **Both null is the interesting case**: a
file nobody here wrote, which has no canvas behind it and is the only kind
`DELETE` accepts.

### `GET /workspace/files/{path}`

The JSON at that path, **uncompiled** — exactly what is on disk:

```json
{ "path": "vendas/jobs/ingestao.json", "pipeline": { "name": "ingestao", "...": "..." } }
```

Needs `workspace:Read`. Same refusals as a staged path: `400` for a missing file,
for something that is not a JSON object, and for a path that leaves the root.

### `DELETE /workspace/files/{path}`

Removes one runnable JSON from the library directory. Needs
**`workspace:Delete`**. There is no undo and no trash — the file leaves the disk.

```json
{ "path": "legado/limpeza.json", "deleted": true }
```

`deleted: false` means the file was already gone. That is the end state asked
for, not an error, so it is a `200`: two clients racing to clean the same folder
should both succeed.

A file a Studio record owns is **refused** with `400`, naming the record to
delete instead. The readable file is half a record: removing only it would leave
the sidecar in `.studio/` pointing at nothing, and the record's next save would
write the file straight back. Delete the Job, Pipeline or Workflow and the file
goes with it.

```
400 "'vendas/jobs/ingestao.json' is the file of job j1. Delete the job itself —
     removing only the file would leave the record behind."
```

Same path refusals as the read: outside the root, inside `.studio/`, or hidden.
Emptied folders are pruned afterwards, because a folder here is only what the
paths have in common — one with nothing left in it is not a folder anyone kept.

This route is declared **before** `/workspace/{kind}/{record_id}` on purpose: a
file at the root of the library has two path segments and would otherwise be
matched as a record. A test pins the ordering so a reorder fails loudly.

All ten require the `X-Sparquet-Token` header.

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
