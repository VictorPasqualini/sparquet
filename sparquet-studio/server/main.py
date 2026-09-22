"""Sparquet Studio — local execution bridge.

Runs a pipeline described by an HTTP body through the real Sparquet and
returns counters, validations, a small data preview and the framework's own
structured logs.

SECURITY WARNING
================
Every request executes arbitrary Spark work: arbitrary SQL, arbitrary reads and
arbitrary writes on the machine (and on any warehouse this machine can reach).

`/run` and `/validate` therefore require a credential in a header of their own —
the shared secret in `X-Sparquet-Token`, printed on startup (or taken from
`SPARQUET_STUDIO_TOKEN`), or a live session — and reject any request whose
`Origin` is outside the allow-list. Without that, any web page the developer
happens to visit could drive this runner: CORS withholds the *response* from the
attacker but never stops the request from executing. `/health` stays open so
Studio can detect the runner and prompt for the token.

The three endpoints somebody locked out has to reach — `/auth/status`,
`/auth/login` and `/auth/recover` — ask for no token once the runner has users,
or the token would guard the only screen that can hand it back. The Origin check
still covers them, and `_LoginThrottle` caps how fast a password can be guessed.

This is still a single-developer tool: keep it bound to 127.0.0.1 and never expose
it to a network or the public internet.

Run it from the `sparquet-studio` directory:

    uvicorn server.main:app --port 8787

`sparquet` is imported like any other dependency — `server/requirements.txt`
pins the range in `compat.py`, and `/health` reports a version outside it. Two
escapes exist for a checkout that is not installed: a repository root above this
directory is added to sys.path when it holds a `sparquet/` package, and
`SPARQUET_FRAMEWORK_PATH` names one anywhere.
"""

from __future__ import annotations

import asyncio
import getpass
import importlib
import importlib.util
import json
import logging
import math
import os
import queue
import re
import secrets
import sys
import threading
import time
from contextlib import contextmanager, redirect_stdout
from datetime import date, datetime, timezone
from datetime import time as clock_time
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field

SERVICE_VERSION = "0.2.0"
FRAMEWORK_LOGGER = "sparquet"
DEFAULT_ORIGINS = ("http://localhost:5273", "http://127.0.0.1:5273")
DEFAULT_PREVIEW_LIMIT = 50
MAX_PREVIEW_LIMIT = 1000
TOKEN_HEADER = "x-sparquet-token"
#: A login, on top of the token. The token says "this request may reach the
#: runner at all"; this says who is making it. See `server/auth.py`.
SESSION_HEADER = "x-sparquet-session"

_VERSION_PATTERN = re.compile(r"""__version__\s*=\s*["']([^"']+)["']""")


# ---------------------------------------------------------------- bootstrap


_log = logging.getLogger("sparquet_studio.server")


def _framework_root() -> Path:
    override = os.getenv("SPARQUET_FRAMEWORK_PATH")
    if override:
        return Path(override).expanduser().resolve()
    # server/main.py -> sparquet-studio/server -> sparquet-studio -> repo root
    return Path(__file__).resolve().parents[2]


def _bootstrap_sys_path() -> None:
    root = _framework_root()
    if (root / "sparquet" / "__init__.py").exists() and str(root) not in sys.path:
        sys.path.insert(0, str(root))


def _pin_pyspark_python() -> None:
    """Force Spark's Python workers to be THIS interpreter.

    Without it Spark spawns whatever `python` the PATH resolves to. When that is a
    different build than the driver — easy to hit here, since the runner is started
    by absolute path from `.venv` while the PATH still points at the system Python —
    the worker dies with a bare "Python worker exited unexpectedly (crashed)".

    It only bites when a stage actually needs a worker, so a CSV-to-Parquet job runs
    fine and the crash shows up the first time a job writes the `validations.report`
    (built with `createDataFrame` from driver-side rows). That mismatch is a
    configuration problem, not a pipeline problem, so it should never reach the user.
    """
    for var in ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"):
        os.environ.setdefault(var, sys.executable)


_bootstrap_sys_path()
_pin_pyspark_python()


def _load_sibling_module(name: str) -> Any:
    """Loads a sibling module by path, not by package name — this module is started
    both as `uvicorn server.main:app` (from `sparquet-studio/`) and as
    `python server/main.py` (script mode), and only a path-based load is correct
    under both: a plain `import history` breaks under the first, a relative
    `from . import history` breaks under the second."""
    spec = importlib.util.spec_from_file_location(
        f"sparquet_studio_{name}", Path(__file__).resolve().parent / f"{name}.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # dataclasses (Python 3.14) resolves annotations via sys.modules[cls.__module__];
    # module_from_spec alone does not register it there.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


history = _load_sibling_module("history")
workspace = _load_sibling_module("workspace")
auth = _load_sibling_module("auth")
credits = _load_sibling_module("credits")
audit = _load_sibling_module("audit")
grants = _load_sibling_module("grants")
providers = _load_sibling_module("providers")
#: Named `vault` and not `secrets` on purpose: `secrets` is the stdlib module
#: imported above for token generation, and a sibling of that name would replace
#: it for every module the runner loads.
vault = _load_sibling_module("vault")
compat = _load_sibling_module("compat")
monitoring = _load_sibling_module("monitoring")
scheduling = _load_sibling_module("scheduling")
assistant = _load_sibling_module("assistant")


# ------------------------------------------------------------------ models


class RunRequest(BaseModel):
    pipeline: Dict[str, Any]
    params: Optional[Dict[str, Any]] = None
    limit: int = Field(default=DEFAULT_PREVIEW_LIMIT, ge=1, le=MAX_PREVIEW_LIMIT)
    dry_run: bool = False
    # Identify the Studio Job this run belongs to, so it is persisted linked to it
    # and shows up in that Job's execution history. Optional: an older Studio build
    # (or a bare API call) that omits these still runs fine, just unlinked.
    workflow_id: Optional[str] = None
    job_id: Optional[str] = None
    job_name: Optional[str] = None
    # Who this run is attributed to, and how it got started ("manual",
    # "scheduled", "api"). The runner authenticates a token, not a person, so
    # `run_as` is a claim: absent, the account the runner runs under is recorded.
    run_as: Optional[str] = None
    launched: Optional[str] = None
    #: Labels to bill this run under, on top of whatever the Job, its Pipeline and
    #: its Workflow already carry in the catalog. For a caller whose Job the
    #: Studio library has never seen: a script can tag its own runs.
    tags: Optional[List[str]] = None


class FlowStageRequest(BaseModel):
    """One JSON of a composed flow — a pipeline that is a stage of a larger job.

    A stage names its JSON one of two ways, and exactly one:

    * `pipeline` — the compiled config, sent inline. This is what a stage backed
      by a Studio Job does: the Job is the source, the Studio compiles it, and
      the file on disk is that same JSON written out.
    * `path` — a file **in the library**, relative to its root. The file is the
      source: nothing is imported, and it is read at the moment the stage runs,
      so an edit made outside the Studio takes effect on the next run. This is
      how a Pipeline points at a JSON another team owns, a script generated, or
      somebody wrote by hand.

    The path is relative on purpose. An absolute one would name a directory that
    exists on one machine, and a Pipeline that runs on the author's laptop and
    silently stops running anywhere else is worse than one that never ran.
    """

    id: str
    name: Optional[str] = None
    pipeline: Dict[str, Any] = Field(default_factory=dict)
    #: A `.json` in the library, relative to its root, read when the stage runs.
    path: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    job_id: Optional[str] = None
    #: Filled by `_resolve_staged_files` for a `path` stage no Job owns: the
    #: catalog identity of the file itself (`file:<relative path>`). Not part of
    #: what a client sends, and deliberately not `job_id` — see `_file_job_id`.
    file_job_id: Optional[str] = None


class RunFlowRequest(BaseModel):
    """Several pipelines, already in execution order, run one after another.

    The stages share one SparkSession, so a stage can hand data to the next either
    through storage (it writes, the next one reads the path) or through a temp view
    registered by a `view` output — both work without extra wiring here.
    """

    stages: List[FlowStageRequest]
    limit: int = Field(default=DEFAULT_PREVIEW_LIMIT, ge=1, le=MAX_PREVIEW_LIMIT)
    stop_on_error: bool = True
    workflow_id: Optional[str] = None
    pipeline_id: Optional[str] = None
    name: Optional[str] = None
    run_as: Optional[str] = None
    launched: Optional[str] = None
    #: Labels for the whole flow, added to what the catalog already says about the
    #: Pipeline, its Workflow and each stage's Job.
    tags: Optional[List[str]] = None


class ValidateRequest(BaseModel):
    pipeline: Dict[str, Any]
    params: Optional[Dict[str, Any]] = None


class DatasetSchemaRequest(BaseModel):
    """One dataset to open, in the same shape a Job's `input` block has."""

    format: str
    path: str
    options: Optional[Dict[str, Any]] = None
    #: Session config, as in a pipeline's `spark` block. `spark.jars.packages`
    #: and `spark.sql.extensions` are read when the SparkSession is CREATED and
    #: ignored afterwards, so asking for a connector the live session was not
    #: built with restarts that session — see `_ensure_framework`.
    spark: Optional[Dict[str, Any]] = None


class SchemaFieldOut(BaseModel):
    name: str
    #: Spark's own rendering of the type, nested types included, as in
    #: `array<struct<id:bigint,name:string>>`.
    type: str
    nullable: bool = True


class DatasetSchemaResponse(BaseModel):
    format: str
    path: str
    fields: List[SchemaFieldOut]
    read_at: str
    #: True when the SparkSession had to be rebuilt to load the connector this
    #: format needs. Worth showing: it is why the call took twenty seconds.
    session_restarted: bool = False


class QuerySource(BaseModel):
    """One dataset made visible to the SQL as a temp view named `alias`."""

    alias: str
    format: str
    path: str
    options: Optional[Dict[str, Any]] = None


class QueryRequest(BaseModel):
    sql: str
    sources: List[QuerySource] = Field(default_factory=list)
    limit: int = DEFAULT_PREVIEW_LIMIT
    #: Chosen by the caller so it can cancel a query that is still in flight —
    #: the response only arrives when the query is already over.
    query_id: Optional[str] = None
    #: Cancels the query by itself after this many seconds. None leaves it to
    #: run until it finishes or someone cancels it.
    timeout_seconds: Optional[int] = None
    #: As in `DatasetSchemaRequest`: a connector the live session lacks rebuilds it.
    spark: Optional[Dict[str, Any]] = None
    #: The library file this statement came from, when it came from one. Sent so
    #: the runner can check the grants on the saved query itself; an unsaved
    #: buffer has no id and is governed only by the tables it names.
    saved_query_id: Optional[str] = None
    #: The editor tab this ran from, which is what its history is keyed by while
    #: the buffer has no file yet. Omitted — by the catalog's row sample, say —
    #: the run is executed and not recorded: nothing would ever read it back.
    tab: Optional[str] = None


class QueryResponse(BaseModel):
    query_id: str
    columns: List[str]
    fields: List[SchemaFieldOut]
    rows: List[List[Any]]
    #: True when the query had more rows than `limit`, so the table can say so.
    truncated: bool
    elapsed_ms: int
    #: The SparkSession was rebuilt before this query, to load a connector it
    #: was missing. Explains an otherwise inexplicable first-query latency.
    session_restarted: bool = False


class QueryRunOut(BaseModel):
    id: str
    at: str
    sql: str
    limit: int
    elapsed_ms: int
    rows: int
    truncated: bool
    error: Optional[str] = None
    run_as: Optional[str] = None


class QueryHistoryResponse(BaseModel):
    runs: List[QueryRunOut] = Field(default_factory=list)


class MoveHistoryRequest(BaseModel):
    #: The scratch buffer the runs are under now.
    tab: str
    #: The file it has just been saved as, and the key they belong under from now on.
    saved_query_id: str


class ValidateQueryRequest(BaseModel):
    """A statement to parse without running it."""

    sql: str


class ValidateQueryResponse(BaseModel):
    """What the parser made of a statement.

    `checked` is the honest part: it is False when no SparkSession was alive to
    ask, and the editor then marks nothing rather than guessing in a dialect it
    does not implement. `ok` is only meaningful when `checked` is True.
    """

    checked: bool
    ok: bool = True
    message: str = ""
    #: 1-based, as the parser counts, and as an editor numbers its lines.
    line: Optional[int] = None
    #: 0-based, as the parser counts. Spark calls it `pos`.
    column: Optional[int] = None
    #: Why nothing was checked, when nothing was.
    reason: str = ""


class ValidationOut(BaseModel):
    type: str
    passed: bool
    message: str = ""
    failed_count: int = 0


class OutputMetricOut(BaseModel):
    format: str
    path: str
    mode: str = ""
    rows_written: int = 0


class PreviewOut(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    truncated: bool


class LogOut(BaseModel):
    timestamp: str
    level: str
    message: str
    context: Dict[str, Any] = Field(default_factory=dict)


class RunChargeOut(BaseModel):
    """What one execution cost its team.

    Sent back with the run and stored against the job execution, so the price
    appears where the work does instead of only in a billing screen: `writes` is
    what was actually written, `amount` what that cost, `free_amount` how much of
    it the monthly allowance covered, and `applied` whether a balance really moved
    (false on a runner that meters without enforcing).
    """

    amount: int
    writes: int
    applied: bool
    free_amount: int = 0
    #: Non-zero only when a run wrote more than its team could pay for. The work
    #: is done and recorded; the next run is the one that gets refused.
    shortfall: int = 0
    target: Optional[str] = None
    balance_after: Optional[int] = None


class RunResponse(BaseModel):
    success: bool
    skipped: bool = False
    # Stopped on request, not broken: the client paints `cancelled`, never `failed`.
    cancelled: bool = False
    pipeline_name: Optional[str] = None
    rows_read: int = 0
    rows_written: int = 0
    duration_ms: int = 0
    error: Optional[str] = None
    validations: List[ValidationOut] = Field(default_factory=list)
    output_metrics: List[OutputMetricOut] = Field(default_factory=list)
    preview: Optional[PreviewOut] = None
    logs: List[LogOut] = Field(default_factory=list)
    pipeline_run_id: Optional[str] = None
    job_run_id: Optional[str] = None
    #: What this run cost. Null for a local run, which is free, and for a run on a
    #: runner where crediting failed — never a reason to fail a run that worked.
    credits: Optional[RunChargeOut] = None


class ValidateResponse(BaseModel):
    valid: bool
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    version: str
    spark_available: bool
    framework_version: Optional[str] = None
    # Lets Studio tell this build apart from an older, unauthenticated runner.
    auth_required: bool = True
    # Whether this runner has users. False is the single-operator runner, where
    # the shared token is the whole of the authentication; true means Studio has
    # to log in before anything else will answer.
    login_required: bool = False
    # Whether a balance actually gates execution. False still meters: the ledger
    # records every remote Job either way. See `credits.py`.
    credits_enforced: bool = False
    # Which implementation is answering each replaceable slot — `"local"` for the
    # SQLite-and-files default, otherwise the `module:factory` that was injected.
    # An operator debugging a hosted runner should not have to infer this from
    # behaviour. See `providers.py`.
    providers: Dict[str, str] = Field(default_factory=dict)
    # Whether the installed framework falls inside the range this Studio was
    # built against, and what to do when it does not. True with no message is
    # the ordinary case, including a machine with no framework at all — that is
    # `spark_available`'s problem, not a version mismatch. See `compat.py`.
    framework_supported: bool = True
    framework_message: Optional[str] = None
    framework_requirement: str = ""


class CapabilitiesResponse(BaseModel):
    transformations: List[str]
    readers: List[str]
    writers: List[str]
    validators: List[str]


# ------------------------------------------------------------- json safety


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_safe(value: Any) -> Any:
    """Converts a Spark/Python value into something json.dumps can emit and
    JSON.parse can read back (NaN/Infinity are valid Python JSON but not JS)."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, clock_time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    as_dict = getattr(value, "asDict", None)
    if callable(as_dict):
        # pyspark Row subclasses tuple, so this must precede the sequence branch
        try:
            return _json_safe(as_dict(recursive=True))
        except TypeError:
            return _json_safe(as_dict())
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(item) for item in value]
    return str(value)


# ----------------------------------------------------------- log capturing


class _LogCollector(logging.Handler):
    """Collects the framework's structured JSON log lines emitted during a run."""

    def __init__(self, on_record: Optional[Callable[[Dict[str, Any]], None]] = None) -> None:
        super().__init__(level=logging.INFO)
        self.records: List[Dict[str, Any]] = []
        self._on_record = on_record

    def emit(self, record: logging.LogRecord) -> None:
        raw = record.getMessage()
        payload: Any = None
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            payload = None

        if isinstance(payload, dict):
            context = {
                key: _json_safe(item)
                for key, item in payload.items()
                if key not in ("timestamp", "level", "message")
            }
            self.append(
                str(payload.get("timestamp") or _now_iso()),
                str(payload.get("level") or record.levelname),
                str(payload.get("message") or ""),
                context,
            )
            return

        self.append(_now_iso(), record.levelname, raw, {})

    def append(
        self, timestamp: str, level: str, message: str, context: Dict[str, Any]
    ) -> None:
        record = {
            "timestamp": timestamp,
            "level": level,
            "message": message,
            "context": context,
        }
        self.records.append(record)
        if self._on_record is not None:
            self._on_record(record)


@contextmanager
def _capture_logs(
    on_record: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Iterator[_LogCollector]:
    collector = _LogCollector(on_record)
    log = logging.getLogger(FRAMEWORK_LOGGER)
    previous_level = log.level
    if log.getEffectiveLevel() > logging.INFO:
        log.setLevel(logging.INFO)
    log.addHandler(collector)
    try:
        yield collector
    finally:
        log.removeHandler(collector)
        log.setLevel(previous_level)


# ------------------------------------------------------ live streaming


class _StreamCollector(_LogCollector):
    """A `_LogCollector` that also pushes each record onto a live queue, so the
    SSE endpoint can forward pipeline logs as they are emitted (not only at the
    end). The `step=True` records (with `index`/`total`) drive the per-node status
    in Studio; everything else is a normal log line."""

    def __init__(
        self,
        events: "queue.Queue[Optional[Dict[str, Any]]]",
        on_record: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        super().__init__(on_record)
        self._events = events

    def append(
        self, timestamp: str, level: str, message: str, context: Dict[str, Any]
    ) -> None:
        super().append(timestamp, level, message, context)
        self._events.put(
            {
                "source": "pipeline",
                "timestamp": timestamp,
                "level": level,
                "message": message,
                "context": context,
            }
        )


def _spark_line_level(line: str) -> str:
    upper = line.upper()
    if " ERROR " in upper or "EXCEPTION" in upper or "ERROR:" in upper:
        return "ERROR"
    if " WARN " in upper or "WARNING" in upper:
        return "WARNING"
    return "INFO"


class _QueueWriter:
    """File-like sink that splits writes into lines and pushes each as a log event
    onto the stream queue. Used to capture stdout (the `debug` transformation's
    `print`/`df.show` output)."""

    def __init__(self, events: Any, source: str, level: str = "INFO") -> None:
        self._events = events
        self._source = source
        self._level = level
        self._buffer = ""

    def write(self, text: str) -> int:
        if not text:
            return 0
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._emit(line)
        return len(text)

    def flush(self) -> None:
        if self._buffer:
            self._emit(self._buffer)
            self._buffer = ""

    def _emit(self, line: str) -> None:
        level = _spark_line_level(line) if self._source == "spark" else self._level
        self._events.put(
            {
                "source": self._source,
                "timestamp": _now_iso(),
                "level": level,
                "message": line,
                "context": {},
            }
        )


@contextmanager
def _capture_streams(events: Any) -> Iterator[None]:
    """For the duration of a run, mirrors Python stdout (debug prints, `df.show`)
    and the JVM's own stderr (log4j lines — the *real* Spark/winutils error) onto
    the stream queue, then restores both. fd-level capture of file descriptor 2 is
    what surfaces the errors the JVM writes straight past Python's logging."""
    stdout_writer = _QueueWriter(events, "stdout", "DEBUG")

    r_fd, w_fd = os.pipe()
    saved_fd = os.dup(2)

    def _reader() -> None:
        try:
            with os.fdopen(r_fd, "r", errors="replace") as handle:
                for line in handle:
                    events.put(
                        {
                            "source": "spark",
                            "timestamp": _now_iso(),
                            "level": _spark_line_level(line),
                            "message": line.rstrip("\n"),
                            "context": {},
                        }
                    )
        except Exception:
            pass

    reader = threading.Thread(target=_reader, daemon=True)
    reader.start()
    os.dup2(w_fd, 2)
    try:
        with redirect_stdout(stdout_writer):
            yield
    finally:
        stdout_writer.flush()
        try:
            sys.stderr.flush()
        except Exception:
            pass
        os.dup2(saved_fd, 2)
        os.close(w_fd)
        os.close(saved_fd)
        reader.join(timeout=2)


def _sse(event: str, data: Dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


# ------------------------------------------------------------- log recording

# How many lines one job execution keeps. The JVM alone can print thousands per
# second, and this database sits on the user's laptop: past the ceiling the run
# records that it stopped recording, which beats growing without bound in silence.
MAX_STORED_LOG_LINES = 3000
# Lines per INSERT batch. Small enough that a run killed mid-flight still leaves
# most of its log behind, large enough not to write once per line.
_LOG_FLUSH_EVERY = 200


class _LogRecorder:
    """Persists a run's log lines, in batches, under the job execution they belong to.

    Fed from the SSE generator rather than from the worker thread: the queue there is
    the one funnel every source passes through — the framework's structured records,
    the JVM's stderr and stdout alike — so what history keeps is exactly what the
    user watched go by.
    """

    def __init__(self, repo: Any, job_run_id: Optional[str] = None) -> None:
        self._repo = repo
        self._job_run_id = job_run_id
        self._buffer: List[Dict[str, Any]] = []
        self._stored = 0
        self._dropped = 0

    def switch(self, job_run_id: str) -> None:
        """Files the lines from here on under another job execution — one stage of a
        flow handing over to the next. Each stage gets its own ceiling."""
        self.flush()
        self._job_run_id = job_run_id
        self._stored = 0
        self._dropped = 0

    def add(self, entry: Dict[str, Any]) -> None:
        if self._job_run_id is None:
            return
        if self._stored + len(self._buffer) >= MAX_STORED_LOG_LINES:
            self._dropped += 1
            return
        self._buffer.append(entry)
        if len(self._buffer) >= _LOG_FLUSH_EVERY:
            self.flush()

    def flush(self) -> None:
        if self._job_run_id is None:
            return
        pending = self._buffer
        self._buffer = []
        # `_dropped` only ever leaves zero once the ceiling was hit, so a flush that
        # sees it owes the reader one line saying the rest is missing.
        if self._dropped:
            pending = pending + [{
                "source": "runner",
                "timestamp": _now_iso(),
                "level": "WARNING",
                "message": (
                    f"{self._dropped} further log lines were produced but not "
                    f"recorded: history keeps the first {MAX_STORED_LOG_LINES} "
                    "lines of a run."
                ),
                "context": {},
            }]
            self._dropped = 0
        if not pending:
            return
        try:
            self._repo.append_logs(self._job_run_id, pending)
            self._stored += len(pending)
        except Exception:
            # A log line is never worth failing a run over.
            pass


# -------------------------------------------------------------- framework


_RUN_LOCK = threading.Lock()
_framework: Any = None


class _ActiveRun:
    """The run this process is executing, so `POST /runs/{id}/cancel` can reach it.

    `_RUN_LOCK` allows exactly one run at a time, so one slot is enough. The flag is
    read by the worker thread and written by the HTTP handler thread, hence the lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._run_id: Optional[str] = None
        self._cancelled = False

    def begin(self, run_id: str) -> None:
        with self._lock:
            self._run_id = run_id
            self._cancelled = False

    def end(self) -> None:
        with self._lock:
            self._run_id = None
            self._cancelled = False

    def request(self, run_id: str) -> bool:
        """Records a cancel request. False when `run_id` is not the run in flight."""
        with self._lock:
            if self._run_id != run_id:
                return False
            self._cancelled = True
            return True

    @property
    def cancelled(self) -> bool:
        with self._lock:
            return self._cancelled


_ACTIVE_RUN = _ActiveRun()


def _cancel_spark_jobs() -> bool:
    """Interrupts whatever Spark is computing right now.

    Python cannot kill a thread, so the flag alone would only take effect at the
    next stage boundary — a job blocked on a long `write` would keep going to the
    end. `cancelAllJobs()` makes the JVM abort the running stages, and the action
    raises inside the worker thread, which is what actually stops the run.

    Returns False when this process has no SparkSession to cancel (nothing has
    touched Spark yet) — not an error: the flag still ends the run.
    """
    module = sys.modules.get("sparquet.core.context")
    session = getattr(getattr(module, "SparkContextManager", None), "_session", None)
    if session is None:
        return False
    try:
        session.sparkContext.cancelAllJobs()
        return True
    except Exception:  # a session already stopped, a dead JVM — nothing left to kill
        return False

_HISTORY_DB_PATH = Path(
    os.getenv("SPARQUET_STUDIO_HISTORY_DB")
    or (Path(__file__).resolve().parent / "data" / "execution_history.sqlite3")
)
_history: Any = history.SQLiteExecutionRepository(_HISTORY_DB_PATH)

# The rules that watch that history, in their own file: they are configuration,
# and the runs they are about are purged on a schedule.
_monitors = monitoring.MonitorStore(
    Path(os.getenv("SPARQUET_STUDIO_MONITORS_DB"))
    if os.getenv("SPARQUET_STUDIO_MONITORS_DB")
    else None
)

# Set when a run finishes, so an alert about a failure does not wait for the next
# tick of the timer. The timer is still what answers "this Job did not run at
# all" — that one has no event to hang off, which is the whole reason it exists.
_MONITOR_WAKE = threading.Event()

# When each schedule last fired, in this process, and when this process started.
# Deliberately not persisted: it says what *this* runner has already done, and a
# second runner pointed at the same library is a different answer to the same
# question. The schedules themselves live in the library records, committed with
# the project — see `scheduling.py`.
_SCHEDULE_ANCHORS: Dict[str, datetime] = {}
_SCHEDULE_LOCK = threading.Lock()
_SCHEDULE_WAKE = threading.Event()
_SCHEDULER_STARTED_AT = datetime.now(timezone.utc)


def _finish_pipeline_run(*args: Any, **kwargs: Any) -> Any:
    """Closes a run in the history and nudges the monitor sweep.

    Every path that finishes a run goes through here rather than calling the
    repository directly: a rule that only notices on the next tick would report a
    failure a minute after the person who started it already saw it.
    """
    result = _history.finish_pipeline_run(*args, **kwargs)
    _MONITOR_WAKE.set()
    return result


#: How often the retention policy is applied on its own. Once a day is enough for
#: a rule expressed in days, and it never runs on the execution path — a purge
#: rewrites the database, which is not something to do while a run is streaming.
_PURGE_EVERY_SECONDS = 24 * 60 * 60


def _purge_history_periodically() -> None:
    """Applies the retention policy at start-up and once a day after that.

    Failures are logged and swallowed: a database that could not be trimmed is a
    disk-space problem, not a reason for the runner to stop serving.
    """
    while True:
        try:
            report = _history.purge(history.RetentionPolicy.from_env())
            if report.rows_removed or report.runs_thinned:
                _log.info(
                    "History purge: %s runs thinned, %s deleted, %s rows removed%s.",
                    report.runs_thinned, report.runs_deleted, report.rows_removed,
                    ", file rewritten" if report.vacuumed else "",
                )
        except Exception as exc:  # pragma: no cover - defensive
            _log.warning("History purge failed: %s", exc)
        time.sleep(_PURGE_EVERY_SECONDS)


# Where the library lives as files. The runner never writes inside its own source
# tree: a checkout is code, and a library kept in one is lost to the first
# `git clean` and committed by accident before that. So the default is the
# platform's per-user data directory, `SPARQUET_STUDIO_WORKSPACE` wins over
# everything for a deployment that decides centrally, and in between sits
# whatever somebody chose in the interface. A directory left over from the old
# default is adopted rather than abandoned — see `workspace.resolve_root`.
_LEGACY_WORKSPACE = _framework_root() / "sparquet-workspace"
_WORKSPACE_LOCATION = workspace.resolve_root(_LEGACY_WORKSPACE)
_WORKSPACE_ROOT = _WORKSPACE_LOCATION.root
# The third slot. The local store is real files on disk — the point of the
# product, since a Job is meant to be reviewable in a pull request — and a
# hosted deployment puts the same documents in object storage.
_workspace: workspace.WorkspaceStore = providers.load(
    "workspace", lambda: workspace.FileWorkspaceStore(_WORKSPACE_ROOT)
)

if _WORKSPACE_LOCATION.source == "legacy":
    _log.warning(
        "The library is still inside the source tree, at %s. It works, but a "
        "checkout is not a safe place for it: move the directory somewhere of "
        "your own and point SPARQUET_STUDIO_WORKSPACE at it, or choose the new "
        "location in Settings.",
        _WORKSPACE_ROOT,
    )


CANCELLED_ERROR = "Cancelled from Studio while it was running."


def _job_outcome_status(response: "RunResponse") -> str:
    if response.skipped:
        return history.SKIPPED
    return history.SUCCESS if response.success else history.FAILED


def _spark_available() -> bool:
    try:
        return importlib.util.find_spec("pyspark") is not None
    except (ImportError, ValueError):
        return False


def _framework_version() -> Optional[str]:
    module = sys.modules.get("sparquet")
    if module is not None:
        version = getattr(module, "__version__", None)
        return str(version) if version else None
    # Reading the source keeps /health from importing pyspark
    init_file = _framework_root() / "sparquet" / "__init__.py"
    try:
        match = _VERSION_PATTERN.search(init_file.read_text(encoding="utf-8"))
    except OSError:
        return None
    return match.group(1) if match else None


def _import(module_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except Exception as exc:  # ImportError, but pyspark can fail in other ways
        raise HTTPException(
            status_code=503,
            detail=(
                f"Cannot import '{module_name}': {exc}. Install the runner's "
                f"requirements into this environment (pip install -r "
                f"server/requirements.txt, which brings {compat.REQUIREMENT}) and "
                "pyspark for the Spark line you run — or point "
                "SPARQUET_FRAMEWORK_PATH at a checkout of the framework."
            ),
        ) from exc


def _get_framework() -> Any:
    """One Sparquet per process — the SparkSession is a process-global
    singleton, so recreating it per request would be both slow and useless."""
    global _framework
    if _framework is None:
        _framework = _import("sparquet").Sparquet()
    return _framework


#: Configs Spark only reads while the SparkSession is being BUILT. Setting one
#: on a live session is accepted and then ignored by the JVM, which is what made
#: "Delta works in a Job but not in the SQL editor" so hard to see: the first
#: request to reach this process fixed the session for every request after it,
#: connectors included.
_CREATION_ONLY = (
    "spark.jars",
    "spark.sql.extensions",
    "spark.sql.catalog.",
    "spark.serializer",
    "spark.kryo",
    "spark.plugins",
    "spark.driver.",
    "spark.executor.",
    "spark.hadoop.",
)

#: Creation-time configs whose value is a comma-separated LIST. A restart merges
#: them instead of replacing: rebuilding the session for Delta must not drop the
#: Iceberg packages an earlier request asked for.
_LIST_CONFIGS = (
    "spark.jars.packages",
    "spark.jars",
    "spark.jars.repositories",
    "spark.sql.extensions",
    "spark.plugins",
)

#: The connector coordinates the runner falls back to when nobody told it any.
#:
#: A Job carries its own `spark` block, and that is the right place for it. But
#: the SQL editor and the schema probe open datasets that no Job may mention yet
#: — somebody points at a Delta table on disk and asks what is in it — and with
#: no coordinate anywhere the read fails with `[DATA_SOURCE_NOT_FOUND] delta`,
#: which reads as "there is no such format".
#:
#: So the runner keeps a default per format, applied only when a request
#: actually asks for that format. A plain Parquet runner never downloads a jar
#: it has no use for, and a runner that has no network still starts.
#:
#: The pins are the ones the Spark line ships against. Override either with
#: `SPARQUET_STUDIO_DELTA_PACKAGE` / `SPARQUET_STUDIO_ICEBERG_PACKAGE`, or turn
#: the whole fallback off with `SPARQUET_STUDIO_NO_AUTO_CONNECTORS=1`.
_DELTA_PACKAGE = {4: "io.delta:delta-spark_2.13:4.3.1", 3: "io.delta:delta-spark_2.12:3.3.2"}
_ICEBERG_PACKAGE = {
    4: "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0",
    3: "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0",
}


def _spark_line() -> int:
    """The major version of the PySpark in this environment, 4 if unreadable."""
    try:
        return int(str(_import("pyspark").__version__).split(".")[0])
    except Exception:
        return 4


def _connector_configs(formats: Iterable[str]) -> Dict[str, str]:
    """The creation-time configs these formats need, as far as the runner knows.

    Empty for every format that needs nothing beyond what Spark ships — Parquet,
    CSV, JSON — and empty for all of them when the fallback is turned off.
    """
    if os.environ.get("SPARQUET_STUDIO_NO_AUTO_CONNECTORS", "").strip().lower() in ("1", "true", "yes"):
        return {}

    wanted = {str(name or "").strip().lower() for name in formats}
    line = _spark_line()
    configs: Dict[str, str] = {}

    def add(key: str, value: str) -> None:
        configs[key] = _merge_list(configs.get(key), value) if key in _LIST_CONFIGS else value

    if "delta" in wanted:
        add("spark.jars.packages", os.environ.get("SPARQUET_STUDIO_DELTA_PACKAGE")
            or _DELTA_PACKAGE.get(line, _DELTA_PACKAGE[4]))
        add("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        add("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    if "iceberg" in wanted:
        add("spark.jars.packages", os.environ.get("SPARQUET_STUDIO_ICEBERG_PACKAGE")
            or _ICEBERG_PACKAGE.get(line, _ICEBERG_PACKAGE[4]))
        add("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    return configs


def _formats_of(config: Any) -> List[str]:
    """Every format a submitted JSON reads or writes.

    Same reading as the lineage the run is recorded with, so a Job that reaches a
    Delta table through a join is counted like one that reads it directly.
    """
    raw = history.lineage_of(config)
    if not raw:
        return []
    try:
        lineage = json.loads(raw)
    except ValueError:
        return []
    out: List[str] = []
    for side in ("inputs", "outputs"):
        for entry in lineage.get(side) or []:
            fmt = entry.get("format")
            if isinstance(fmt, str) and fmt:
                out.append(fmt)
    return out


def _spark_for_formats(
    spark_settings: Optional[Dict[str, Any]], formats: Iterable[str]
) -> Optional[Dict[str, Any]]:
    """What the caller asked for, plus whatever these formats need to load at all.

    The caller wins on every key it set: a Job pinned to one Delta version must
    not be quietly moved to another because the runner has a newer default.
    """
    fallback = _connector_configs(formats)
    if not fallback:
        return spark_settings
    settings = dict(spark_settings or {})
    configs = dict(fallback)
    for key, value in _configs_of(spark_settings).items():
        configs[key] = _merge_list(fallback.get(key), value) if key in _LIST_CONFIGS else value
    settings["configs"] = configs
    return settings


#: Queries in flight. A session restart stops the JVM, so it may only happen
#: while nothing is running — `_RUN_LOCK` covers pipeline runs, this covers
#: `/query` and `/dataset/schema`, which deliberately do not take that lock.
_QUERY_COUNT = 0
_QUERY_COUNT_LOCK = threading.Lock()
_SESSION_LOCK = threading.RLock()


def _query_enter() -> None:
    """Marks a query as running, so a session restart refuses to interrupt it."""
    global _QUERY_COUNT
    with _QUERY_COUNT_LOCK:
        _QUERY_COUNT += 1


def _query_exit() -> None:
    global _QUERY_COUNT
    with _QUERY_COUNT_LOCK:
        _QUERY_COUNT = max(0, _QUERY_COUNT - 1)


def _creation_only(key: str) -> bool:
    return any(key == prefix or key.startswith(prefix) for prefix in _CREATION_ONLY)


def _merge_list(current: Optional[str], wanted: str) -> str:
    """Union of two comma-separated config values, order preserved."""
    out: List[str] = []
    for value in (current or "", wanted):
        for item in value.split(","):
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return ",".join(out)


def _configs_of(spark_settings: Optional[Dict[str, Any]]) -> Dict[str, str]:
    configs = (spark_settings or {}).get("configs") or {}
    if not isinstance(configs, dict):
        return {}
    return {str(key): str(value) for key, value in configs.items()}


def _session_gap(session: Any, wanted: Dict[str, str]) -> Dict[str, str]:
    """Creation-time configs the live session does not already satisfy."""
    try:
        conf = session.sparkContext.getConf()
    except Exception:
        return {}
    gap: Dict[str, str] = {}
    for key, value in wanted.items():
        if not _creation_only(key):
            continue
        current = conf.get(key, None)
        if current is None:
            gap[key] = value
        elif key in _LIST_CONFIGS:
            have = {item.strip() for item in str(current).split(",") if item.strip()}
            missing = [item.strip() for item in value.split(",") if item.strip() and item.strip() not in have]
            if missing:
                gap[key] = _merge_list(str(current), value)
        elif str(current) != value:
            gap[key] = value
    return gap


def _live_creation_configs(session: Any) -> Dict[str, str]:
    """What the live session was built with, so a restart keeps it."""
    try:
        pairs = session.sparkContext.getConf().getAll()
    except Exception:
        return {}
    return {str(key): str(value) for key, value in pairs if _creation_only(str(key))}


def _release_jvm() -> None:
    """Shut the py4j gateway down so the next session starts a NEW JVM.

    `SparkSession.stop()` stops the context and leaves the JVM running, and
    `spark.jars.packages` is resolved by SparkSubmit *while the JVM is starting*
    — never afterwards. So a session rebuilt over a live gateway comes back
    reporting the packages it was asked for and still cannot open the format:

        Py4JJavaError: An error occurred while calling o67.load

    with a `DATA_SOURCE_NOT_FOUND` or an EOFException from the Python data
    source lookup underneath, depending on the Spark line. Dropping the gateway
    is what makes the restart real; the next `getOrCreate` relaunches it with
    the merged submit arguments.

    Not done where the session belongs to someone else: on Databricks the
    cluster owns the JVM, and killing it is not this process's business.
    """
    context = _import("sparquet.core.context")
    try:
        if context.SparkContextManager.current_environment() == "databricks":
            return
    except Exception:  # an older framework without the accessor: assume ours
        pass

    from pyspark import SparkContext  # local import: the runner may have no Spark

    gateway = getattr(SparkContext, "_gateway", None)
    if gateway is not None:
        try:
            gateway.shutdown()
        except Exception:  # already gone, or the JVM died on its own
            pass
    SparkContext._gateway = None
    SparkContext._jvm = None


def _ensure_framework(
    spark_settings: Optional[Dict[str, Any]], *, run_lock_held: bool = False
) -> Tuple[Any, bool]:
    """The framework, with a SparkSession that can actually open what was asked.

    The session is a process-wide singleton and its connector configs are frozen
    at creation, so a caller asking for Delta after somebody else created a
    plain session used to get `[DATA_SOURCE_NOT_FOUND] delta` with nothing in the
    message pointing at the real cause. When the request needs a creation-time
    config the live session lacks, the session is rebuilt with the union of both
    — and refused, loudly, while anything is still running on it.

    Returns the framework and whether a restart happened.

    `run_lock_held` is for the run path, which already owns `_RUN_LOCK`: it is
    the thing the restart would otherwise wait for, and re-taking a lock you
    hold refuses your own run.
    """
    global _framework
    wanted = _configs_of(spark_settings)

    with _SESSION_LOCK:
        if _framework is None:
            _framework = _import("sparquet").Sparquet(spark=spark_settings or None)
            return _framework, False

        if not wanted:
            return _framework, False

        session = _framework.spark
        gap = _session_gap(session, wanted)
        if not gap:
            return _framework, False

        busy_query = _QUERY_COUNT > 0
        acquired = True if run_lock_held else _RUN_LOCK.acquire(blocking=False)
        if busy_query or not acquired:
            if acquired and not run_lock_held:
                _RUN_LOCK.release()
            raise HTTPException(
                status_code=409,
                detail=(
                    "This SparkSession was created without "
                    + ", ".join(sorted(gap))
                    + ", and those only take effect when the session is built. "
                    "Rebuilding it means restarting the JVM, which would kill the "
                    "run or query using it right now. Try again when it finishes."
                ),
            )

        try:
            merged = _live_creation_configs(session)
            for key, value in gap.items():
                merged[key] = _merge_list(merged.get(key), value) if key in _LIST_CONFIGS else value
            settings = dict(spark_settings or {})
            settings["configs"] = merged
            _import("sparquet.core.context").SparkContextManager.stop()
            _release_jvm()
            _framework = _import("sparquet").Sparquet(spark=settings)
            # Touch it here so a failure to build surfaces as this call, not as
            # an unrelated one two requests later.
            _framework.spark
        finally:
            if not run_lock_held:
                _RUN_LOCK.release()
        return _framework, True


def _warm_configs() -> Dict[str, str]:
    """The creation-time configs the saved Jobs between them ask for.

    Warming a plain session would be half a warm-up: connector jars and SQL
    extensions are read only when a session is built, so the first Delta query
    against a plain session rebuilds it — JVM and all — and the wait comes back
    exactly where it was meant to be gone. So the session is built with the union
    of what the library already declares, which is the closest thing the runner
    has to knowing what it will be asked for.

    A library that declares nothing warms a plain session, which is the right
    answer for a Parquet-only runner: it downloads no jar it has no use for.
    """
    try:
        jobs = _workspace.snapshot().jobs
    except Exception:  # pragma: no cover - a library that cannot be read warms plain
        return {}
    configs: Dict[str, str] = {}
    for document in jobs:
        record = document.record if isinstance(document.record, dict) else {}
        settings = record.get("settings")
        declared = ((settings or {}).get("spark") or {}).get("configs")
        if not isinstance(declared, dict):
            continue
        for key, value in declared.items():
            key, value = str(key), str(value)
            configs[key] = (
                _merge_list(configs.get(key), value) if key in _LIST_CONFIGS else value
            )
    return configs


def _warm_enabled(env: Optional[Dict[str, str]] = None) -> bool:
    """Whether this process builds the SparkSession before anybody asks for one."""
    source = env if env is not None else os.environ
    value = (source.get("SPARQUET_STUDIO_WARM_SPARK") or "").strip().lower()
    return value in ("1", "on", "true", "yes")


def _warm_spark() -> None:
    """Builds the SparkSession now, so the first person to ask does not wait.

    A cold first query pays for a JVM launch, the connector jars being resolved
    and a session being configured — tens of seconds, all of it before any work
    starts, and all of it charged to whoever happened to click Run first. Nothing
    here is new machinery: it is the same `_ensure_framework` every request goes
    through, called once at start-up on a daemon thread so it overlaps with the
    person opening the browser instead of with their first query.

    Failures are logged and swallowed. A warm-up is an optimisation, and a runner
    that cannot build a session at start-up must still start — the same request
    that would have built one will fail with its own, much better message.
    """
    started = time.perf_counter()
    try:
        configs = _warm_configs()
        framework, _ = _ensure_framework({"configs": configs} if configs else None)
        # `Sparquet` builds the session lazily, so touching it here is what
        # actually pays the cost on this thread rather than on the first request.
        framework.spark
        _log.info(
            "Spark warmed up in %s ms%s.",
            int((time.perf_counter() - started) * 1000),
            f" with {len(configs)} config(s) from the library" if configs else "",
        )
    except Exception as exc:  # pragma: no cover - defensive
        _log.warning("Spark warm-up failed, the first query will build the session: %s", exc)


def _apply_params(pipeline: Dict[str, Any], params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not params:
        return pipeline
    template = _import("sparquet.utils.template")
    rendered = template.apply_template(json.dumps(pipeline), params)
    parsed = json.loads(rendered)
    if not isinstance(parsed, dict):
        raise ValueError("Template substitution produced a non-object pipeline.")
    return parsed


def _run_as(principal: Any, claimed: Optional[str]) -> str:
    """Who the run is recorded against.

    An authenticated session wins over anything the body claims: on a runner with
    users, "who ran this" is a fact, and a caller must not be able to file their
    run under someone else's name. With no users the runner has one operator and
    no directory to check against, so the claim is taken as given, falling back to
    the OS account the runner process runs under.
    """
    if principal is not None and not getattr(principal, "token_only", False):
        return str(principal.username)[:120]
    claimed = (claimed or "").strip()
    if claimed:
        return claimed[:120]
    try:
        return getpass.getuser()
    except Exception:  # pragma: no cover - no account name on this platform
        return "unknown"


def _launched(claimed: Optional[str]) -> str:
    """How the run got started. Anything unrecognised is recorded as `api`: it
    reached the runner without Studio saying otherwise, which is what `api` means."""
    value = (claimed or "").strip().lower()
    if value in history.LAUNCH_KINDS:
        return value
    return history.MANUAL if not value else history.API


def _ensure_catalog(
    *, workflow_id: Optional[str], pipeline_id: Optional[str] = None,
    job_id: Optional[str] = None, name: Optional[str] = None,
    job_ids: Optional[List[Optional[str]]] = None,
) -> None:
    """Registers the ids a run is about, so the run's foreign keys resolve.

    A run is allowed to name a Job the workspace has never sent — a script, a
    scheduler, a Studio that has not synced. History records what happened; it does
    not reject an execution because the catalog was behind. A failure here is
    swallowed for the same reason: no run is worth losing over bookkeeping.
    """
    try:
        _history.ensure_run_targets(
            workflow_id=workflow_id, pipeline_id=pipeline_id, job_id=job_id, name=name,
        )
        for stage_job_id in job_ids or ():
            if stage_job_id:
                _history.ensure_run_targets(
                    workflow_id=workflow_id, pipeline_id=None, job_id=stage_job_id,
                )
    except Exception:  # pragma: no cover - bookkeeping must never fail a run
        pass


def _run_tags(
    *, workflow_id: Optional[str] = None, pipeline_id: Optional[str] = None,
    job_id: Optional[str] = None, extra: Optional[List[str]] = None,
) -> List[str]:
    """The labels this run is billed under.

    Read from the catalog at run time rather than taken from the request: the
    tags belong to the record, so what a run costs is attributed by what the
    library says today, not by what a client remembered to send. `extra` is for
    the caller the library has never heard of — a script tagging its own run.
    """
    tags = list(extra or [])
    try:
        tags += _history.effective_tags(
            workflow_id=workflow_id, pipeline_id=pipeline_id, job_id=job_id
        )
    except Exception:  # pragma: no cover - billing labels are not worth a run
        _log.warning("Could not read the tags for this run from the catalog.")
    return history.normalize_tags(tags)


def _lineage(pipeline: Dict[str, Any], params: Optional[Dict[str, Any]]) -> Optional[str]:
    """The datasets this JSON reads and writes, with `{param}` values resolved.

    Resolving matters: an unresolved `/data/{ano}/vendas` would file every run of
    the job under the same fictional path. A template that cannot be resolved is
    not worth failing a run over — the raw configuration is recorded instead.
    """
    try:
        return history.lineage_of(_apply_params(pipeline, params))
    except Exception:
        return history.lineage_of(pipeline)


def _config_version(
    pipeline: Dict[str, Any], params: Optional[Dict[str, Any]]
) -> Tuple[Optional[str], Optional[str]]:
    """The fingerprint of the JSON this run is about to execute, and its text.

    Fingerprinted after `{param}` substitution, for the same reason lineage is:
    what ran is the resolved configuration, and two runs of one template with
    different parameters did not execute the same thing. A template that cannot
    be resolved is recorded raw rather than not at all.
    """
    try:
        return history.config_version(_apply_params(pipeline, params))
    except Exception:
        return history.config_version(pipeline)


def _parse_config_error(
    pipeline: Dict[str, Any], params: Optional[Dict[str, Any]]
) -> Optional[str]:
    config_cls = _import("sparquet").PipelineConfig
    try:
        config_cls.from_dict(_apply_params(pipeline, params))
    except Exception as exc:
        return _describe(exc)
    return None


def _describe(exc: Exception) -> str:
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


#: What each format needs in the `spark` block to exist at all. The version is
#: deliberately left as a placeholder: the right coordinate depends on the Spark
#: line the runner is on (Scala 2.13 on Spark 4.x, 2.12 on 3.5), and printing a
#: wrong pin is worse than printing the shape of the right one.
_CONNECTOR_HINT: Dict[str, str] = {
    "delta": (
        "spark.jars.packages=io.delta:delta-spark_<scala>:<version> plus "
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension and "
        "spark.sql.catalog.spark_catalog="
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ),
    "iceberg": (
        "spark.jars.packages="
        "org.apache.iceberg:iceberg-spark-runtime-<spark>_<scala>:<version> plus "
        "spark.sql.extensions="
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    ),
    "hudi": "spark.jars.packages=org.apache.hudi:hudi-spark<spark>-bundle_<scala>:<version>",
    "kafka": "spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_<scala>:<version>",
    "bigquery": "spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_<scala>:<version>",
    "mongodb": "spark.jars.packages=org.mongodb.spark:mongo-spark-connector_<scala>:<version>",
    "cassandra": "spark.jars.packages=com.datastax.spark:spark-cassandra-connector_<scala>:<version>",
    "elasticsearch": "spark.jars.packages=org.elasticsearch:elasticsearch-spark-30_<scala>:<version>",
    "opensearch": "spark.jars.packages=org.opensearch.client:opensearch-spark-30_<scala>:<version>",
    "snowflake": "spark.jars.packages=net.snowflake:spark-snowflake_<scala>:<version>",
}


def _describe_io(exc: Exception, fmt: str) -> str:
    """`_describe`, plus what a missing connector actually means.

    Spark answers a format it cannot load with `[DATA_SOURCE_NOT_FOUND] Failed
    to find the data source: delta`, which reads as "there is no such thing" when
    it means "this session was built without the jar". The two are fixed very
    differently, so the message says which one this is.
    """
    text = _describe(exc)
    name = (fmt or "").strip().lower()
    marker = "DATA_SOURCE_NOT_FOUND" in text or "ClassNotFoundException" in text
    if not marker or name not in _CONNECTOR_HINT:
        return text
    return (
        f"{text}\n\nThe {name} connector is not on this SparkSession. Its jars and "
        "extensions are read only when the session is created, so declaring them "
        "afterwards has no effect. Put them in the `spark` block of a Job that "
        f"touches this dataset — {_CONNECTOR_HINT[name]} — and the runner rebuilds "
        "the session with them the next time nothing is running on it."
    )


def _build_preview(df: Any, limit: int, collector: _LogCollector) -> Optional[PreviewOut]:
    try:
        columns = [str(name) for name in df.columns]
        # limit + 1 tells us whether more rows exist without a second action
        rows = df.limit(limit + 1).collect()
    except Exception as exc:
        collector.append(
            _now_iso(), "WARNING", "Preview unavailable", {"error": _describe(exc)}
        )
        return None

    return PreviewOut(
        columns=columns,
        rows=[[_json_safe(value) for value in row] for row in rows[:limit]],
        truncated=len(rows) > limit,
    )


def _map_validations(results: Any) -> List[ValidationOut]:
    out: List[ValidationOut] = []
    for item in results or []:
        out.append(
            ValidationOut(
                type=str(getattr(item, "rule_type", "unknown")),
                passed=bool(getattr(item, "passed", False)),
                message=str(getattr(item, "message", "") or ""),
                failed_count=int(getattr(item, "failed_count", 0) or 0),
            )
        )
    return out


def _map_output_metrics(items: Any) -> List[OutputMetricOut]:
    out: List[OutputMetricOut] = []
    for item in items or []:
        out.append(
            OutputMetricOut(
                format=str(getattr(item, "format", "") or ""),
                path=str(getattr(item, "path", "") or ""),
                mode=str(getattr(item, "mode", "") or ""),
                rows_written=int(getattr(item, "rows_written", 0) or 0),
            )
        )
    return out


# ------------------------------------------------------------------- access


def _allowed_origins() -> List[str]:
    raw = os.getenv("SPARQUET_STUDIO_ORIGINS", "")
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or list(DEFAULT_ORIGINS)


def _load_token() -> tuple[str, bool]:
    configured = os.getenv("SPARQUET_STUDIO_TOKEN", "").strip()
    return (configured, True) if configured else (secrets.token_urlsafe(24), False)


AUTH_TOKEN, TOKEN_FROM_ENV = _load_token()

UNAUTHORIZED_HELP = (
    f"Missing or invalid '{TOKEN_HEADER}' header. This runner executes arbitrary "
    "Spark jobs, so /run and /validate require the token printed in the runner's "
    "terminal when it started. Paste that token into Studio (Settings -> Local "
    "runner) so every request carries it, or start the runner with "
    "SPARQUET_STUDIO_TOKEN=<value> to pin a token you already know."
)


def _announce_token() -> None:
    if TOKEN_FROM_ENV:
        print("Sparquet Studio runner: using the token from SPARQUET_STUDIO_TOKEN.")
        return
    print("=" * 72)
    print("Sparquet Studio runner token (this session only):")
    print(f"    {AUTH_TOKEN}")
    print(f"Send it as the '{TOKEN_HEADER}' header on /run and /validate, or set")
    print("SPARQUET_STUDIO_TOKEN to keep the same token across restarts.")
    print("=" * 72)


_announce_token()


def _check_origin(request: Request) -> None:
    """The half of the guard that is not about a secret at all.

    A browser will not send a request carrying a custom header cross-origin
    without asking first, and this refuses the asking. It runs on every guarded
    endpoint, whatever credential the caller goes on to present.
    """
    origin = request.headers.get("origin")
    if origin is not None and origin not in _allowed_origins():
        raise HTTPException(
            status_code=403,
            detail=(
                f"Origin '{origin}' is not allowed to use this runner. Set "
                "SPARQUET_STUDIO_ORIGINS if Studio is served from another origin."
            ),
        )


def _token_matches(request: Request) -> bool:
    return secrets.compare_digest(request.headers.get(TOKEN_HEADER, ""), AUTH_TOKEN)


# ------------------------------------------------------------------ identity

# Identity is a slot: the local backend is usernames and password hashes in
# SQLite, and a deployment that arrives with an identity provider names its own
# factory instead. See `providers.py` for why a failure here is fatal.
_auth: auth.IdentityStore = providers.load("auth", auth.AuthStore)

LOGIN_REQUIRED_HELP = (
    "This runner has users, so the shared token is no longer enough on its own. "
    f"Log in through Studio and send the session as the '{SESSION_HEADER}' header "
    "(or as `Authorization: Bearer <session>`)."
)
SESSION_EXPIRED_HELP = (
    "That session is not valid any more — it expired, it was logged out, or the "
    "account was disabled. Log in again."
)


def _session_token(request: Request) -> str:
    """The session, from either header. `Authorization: Bearer` is there because
    every HTTP client already knows it; the explicit header because the browser
    treats it like the runner token it travels with."""
    header = request.headers.get(SESSION_HEADER, "").strip()
    if header:
        return header
    authorization = request.headers.get("authorization", "").strip()
    scheme, _, value = authorization.partition(" ")
    return value.strip() if scheme.lower() == "bearer" else ""


def require_token(request: Request) -> None:
    """Blocks drive-by requests from any page the developer happens to visit.

    CORS cannot do this: for a request with no custom header and no JSON content
    type the browser skips the preflight, the app runs, and only the *response*
    is withheld from the attacker. A header the browser refuses to attach
    cross-origin without a preflight, plus a server-side Origin check, do.

    A live session is taken in the token's place, because it buys the very same
    thing: it also travels in a header of its own, so a browser will not attach
    it cross-origin without the preflight the Origin check refuses. Demanding
    both would mean that rotating the runner's token locks every user out of a
    screen whose only purpose is to let them back in — and once someone has
    logged in, the session is the credential they actually hold.
    """
    _check_origin(request)
    if _token_matches(request):
        return
    session = _session_token(request)
    if not session:
        raise HTTPException(status_code=401, detail=UNAUTHORIZED_HELP)
    principal = _auth.resolve_session(session)
    if principal is None:
        # Name the credential that went stale. Answering "no token" to somebody
        # holding an expired session sends them looking for the wrong thing.
        raise HTTPException(status_code=401, detail=SESSION_EXPIRED_HELP)
    request.state.principal = principal


def require_token_unless_users(request: Request) -> None:
    """The guard on the three endpoints a locked-out person has to reach.

    `/auth/status`, `/auth/login` and `/auth/recover` behind the shared token is
    a closed loop: the token is typed in Settings, and Settings is behind the
    login. So on a runner that has users these ask for no token — the login is
    the wall, and the Origin check still stands in front of it. With no users
    there is nothing else to ask for, and the token stays mandatory.
    """
    _check_origin(request)
    if _auth.has_users() or _token_matches(request):
        return
    raise HTTPException(status_code=401, detail=UNAUTHORIZED_HELP)


def current_principal(request: Request) -> Any:
    """Who is calling, after `require_token` has already vouched for the request.

    Two modes, and the difference is only whether any user exists. With none, the
    runner behaves as it always has: the shared token is the identity, with full
    rights — that runner has one operator, and upgrading must not lock them out.
    With users, a session is required for everything but logging in.
    """
    resolved = getattr(request.state, "principal", None)
    if resolved is not None:
        # `require_token` already looked this session up on the way in, and a
        # second lookup would only cost another read of the identity store.
        return resolved
    token = _session_token(request)
    if token:
        principal = _auth.resolve_session(token)
        if principal is None:
            raise HTTPException(status_code=401, detail=SESSION_EXPIRED_HELP)
        request.state.principal = principal
        return principal
    if _auth.has_users():
        raise HTTPException(status_code=401, detail=LOGIN_REQUIRED_HELP)
    request.state.principal = auth.TOKEN_PRINCIPAL
    return auth.TOKEN_PRINCIPAL


class _LoginThrottle:
    """A sliding window over failed credential attempts.

    `/auth/login` and `/auth/recover` stopped needing the shared token, so what
    used to gate a guesser is now only the password itself. This puts a ceiling
    on how fast one can be guessed. It counts failures only — someone who logs in
    is forgotten immediately, so a person who mistypes twice and then succeeds
    pays nothing.

    Deliberately in memory and per process: a restart clears it, which is the
    right trade for a single-developer runner, and there is no second process to
    keep in step with.
    """

    def __init__(self, limit: int, window: int) -> None:
        self._limit = max(1, limit)
        self._window = max(1, window)
        self._hits: Dict[str, List[float]] = {}
        self._lock = threading.Lock()

    def _fresh(self, key: str, now: float) -> List[float]:
        hits = [at for at in self._hits.get(key, []) if now - at < self._window]
        if hits:
            self._hits[key] = hits
        else:
            self._hits.pop(key, None)
        return hits

    def retry_after(self, keys: Iterable[str]) -> int:
        """Seconds to wait, or 0 to go ahead. The longest wait of any key wins."""
        now = time.time()
        wait = 0
        with self._lock:
            for key in keys:
                hits = self._fresh(key, now)
                if len(hits) >= self._limit:
                    wait = max(wait, int(hits[0] + self._window - now) + 1)
        return wait

    def record_failure(self, keys: Iterable[str]) -> None:
        now = time.time()
        with self._lock:
            for key in keys:
                self._hits.setdefault(key, []).append(now)

    def forget(self, keys: Iterable[str]) -> None:
        with self._lock:
            for key in keys:
                self._hits.pop(key, None)


def _int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, "").strip() or default))
    except ValueError:
        return default


_LOGIN_THROTTLE = _LoginThrottle(
    _int_env("SPARQUET_STUDIO_LOGIN_ATTEMPTS", 10),
    _int_env("SPARQUET_STUDIO_LOGIN_WINDOW", 300),
)


def _throttle_keys(request: Request, username: Optional[str] = None) -> List[str]:
    """Both axes, because either alone has a hole: counting only the caller lets
    a botnet spread the guessing across addresses, and counting only the account
    lets one caller sweep every account at full speed."""
    client = request.client.host if request.client else "unknown"
    keys = [f"ip:{client}"]
    if username and username.strip():
        keys.append(f"user:{username.strip().lower()}")
    return keys


def _refuse_if_throttled(keys: List[str]) -> None:
    wait = _LOGIN_THROTTLE.retry_after(keys)
    if wait:
        raise HTTPException(
            status_code=429,
            detail=(
                f"Too many failed attempts. Try again in {wait} seconds, or restart "
                "the runner if you are the operator and locked yourself out."
            ),
            headers={"Retry-After": str(wait)},
        )


def requires(action: str, resource: Any = "*") -> Callable[[Request], Any]:
    """Dependency for one action, optionally on one resource.

    `resource` may be a callable taking the request, for the endpoints whose
    target is in the path (`workspace/job/j1`). The run endpoints do not use this
    dependency at all: their target is in the body, which a dependency cannot read
    without consuming it, so they call `_authorize_run` once the body is parsed.
    """

    def dependency(request: Request) -> Any:
        require_token(request)
        principal = current_principal(request)
        target = resource(request) if callable(resource) else str(resource)
        if not principal.allows(action, target):
            raise HTTPException(
                status_code=403,
                detail=(
                    f"'{principal.username}' is not allowed to {action} on '{target}'. "
                    f"Roles held: {', '.join(principal.roles) or 'none'}."
                ),
            )
        return principal

    # Written on the closure so the guard can be read back off a route: the
    # question "which action protects this endpoint?" is worth being able to ask
    # of the app object rather than of the source.
    dependency.action = action  # type: ignore[attr-defined]
    dependency.resource = resource  # type: ignore[attr-defined]
    return dependency


# --------------------------------------------------------------------- credits

# Same slot mechanism as identity: SQLite here, Postgres and a payment gateway
# in a hosted deployment, the routes below unchanged either way.
_credits: credits.CreditLedger = providers.load("credits", credits.CreditStore)

# A hold belongs to a run in flight, and a run in flight belonged to the process
# that took it. This process is starting, so anything still open was left by a
# crash or a restart: give it back rather than let an account stay poorer for a
# run that never finished.
_credits.release_stale()

NO_CREDITS_STATUS = 402


def _admit_execution(
    principal: Any, pipeline: Dict[str, Any], *, job_name: Optional[str] = None
) -> Any:
    """Hold what the run declares it will cost, before Spark is started.

    The configuration says how many destinations it intends to write, and that
    much is reserved: a team that cannot cover its own declaration is refused here
    rather than halfway through a cluster hour. The hold is not a charge — what
    the run really cost is settled at the end and the rest comes back. Whether the
    target is local, and therefore free, is read from the configuration rather
    than from the request, so a caller cannot declare their own run free.
    """
    account_id, username = credits.account_for(principal)
    try:
        return _credits.reserve(
            account_id, credits.target_of(pipeline),
            credits.declared_writes(pipeline),
            username=username, job_name=job_name,
        )
    except credits.InsufficientCredits as error:
        raise HTTPException(status_code=NO_CREDITS_STATUS, detail=str(error)) from error
    except credits.CreditError as error:  # pragma: no cover - defensive
        raise HTTPException(status_code=400, detail=str(error)) from error


def _precheck_execution(principal: Any, pipeline: Dict[str, Any]) -> None:
    """Refuse a flow whose team has nothing available, without holding anything.

    A flow reserves per stage, as each one starts: holding for the whole flow up
    front would make a five-Job Pipeline unaffordable for a team that can pay for
    every one of them in turn. This is the cheap front-door check that keeps a
    flow with an empty account from starting a cluster at all.
    """
    account_id, username = credits.account_for(principal)
    try:
        _credits.precheck(account_id, credits.target_of(pipeline), username=username)
    except credits.InsufficientCredits as error:
        raise HTTPException(status_code=NO_CREDITS_STATUS, detail=str(error)) from error
    except credits.CreditError as error:  # pragma: no cover - defensive
        raise HTTPException(status_code=400, detail=str(error)) from error


def _release_reservation(reservation: Any) -> None:
    """Give a hold back when the run is over, whichever way it ended.

    Called from the `finally` that releases the run lock, so a run that raised, or
    was cancelled, or never wrote anything, does not leave credits promised to
    nobody. Releasing twice is a no-op, which is what makes it safe to also let
    `settle` release on the normal path.
    """
    if reservation is None:
        return
    try:
        _credits.release(reservation)
    except credits.CreditError as error:  # pragma: no cover - defensive
        _log.warning("Could not release the credit reservation: %s", error)


def _charge_execution(
    principal: Any, pipeline: Dict[str, Any], writes: int, *,
    job_name: Optional[str] = None, job_run_id: Optional[str] = None,
    pipeline_run_id: Optional[str] = None, reservation: Any = None,
    workflow_id: Optional[str] = None, tags: Optional[List[str]] = None,
) -> Any:
    """Charge one credit per destination the run actually wrote.

    Called **after** the execution, which is the only moment the number of
    successful writes exists: `RunResponse.output_metrics` carries one entry per
    completed write, because the framework appends to `PipelineResult.outputs`
    only once the writer returns. A run that failed before writing anything has
    none of them and therefore costs nothing — that is the whole of "errors do not
    spend a token", and it needs no cooperation from the framework.

    This never raises for lack of credit: the writes already happened. An account
    that could not cover them goes to zero with the gap recorded on the ledger
    entry, and it is the next `_admit_execution` that refuses.
    """
    account_id, username = credits.account_for(principal)
    try:
        return _credits.settle(
            reservation, credits.target_of(pipeline), writes, account_id=account_id,
            username=username, job_run_id=job_run_id,
            pipeline_run_id=pipeline_run_id, job_name=job_name,
            workflow_id=workflow_id, actor=credits.actor_for(principal),
            tags=tags,
        )
    except credits.CreditError as error:  # pragma: no cover - defensive
        _log.warning("Could not charge execution credits: %s", error)
        return None


def _charge_out(charge: Any) -> Optional[RunChargeOut]:
    """A `credits.Charge` as the API shape, or nothing when the run was free."""
    if charge is None or not getattr(charge, "charged", False):
        return None
    return RunChargeOut(
        amount=charge.amount, writes=charge.writes, applied=charge.applied,
        free_amount=charge.free_amount, shortfall=charge.shortfall,
        target=charge.target, balance_after=charge.balance_after,
    )


def _entry_charge_out(entry: Any) -> RunChargeOut:
    """A ledger row as the same shape, for a past run read back from history.

    The ledger stores what a charge did to the account, so the amount is negative
    there and positive here: this says what the run cost, not which way the
    balance moved.
    """
    return RunChargeOut(
        amount=-entry.amount, writes=entry.writes, applied=entry.applied,
        free_amount=entry.free_amount, shortfall=entry.shortfall,
        target=entry.target, balance_after=entry.balance_after,
    )


def _run_targets(
    workflow_id: Optional[str], pipeline_id: Optional[str], job_id: Optional[str]
) -> List[str]:
    """The resources a run can be authorized against: `workflow/w1`, `pipeline/p1`,
    `job/j1` — whichever of them the request actually named.

    A run belongs to all three at once, so a role may reasonably be written against
    any of them: "may run anything in this Workflow" and "may run this one Job" are
    both sensible grants. An unsaved Job from the editor names none of them and
    falls back to `*`, which is what every role scoped to everything already
    matches.
    """
    named = [
        f"{kind}/{value}" for kind, value in
        (("workflow", workflow_id), ("pipeline", pipeline_id), ("job", job_id))
        if value
    ]
    return named or ["*"]


def _authorize_run(
    principal: Any, action: str, *, workflow_id: Optional[str] = None,
    pipeline_id: Optional[str] = None, job_id: Optional[str] = None,
) -> None:
    """Authorize an execution once the body is parsed and the target is known.

    One allow among the run's identifiers is enough, but an explicit deny on any
    of them settles it — otherwise "may not run job/j1" could be walked around by
    also holding "may run everything in workflow/w1", and a deny that can be
    widened away is not a deny.
    """
    targets = _run_targets(workflow_id, pipeline_id, job_id)
    denied = next((target for target in targets if principal.denies(action, target)), None)
    if denied is None and any(principal.allows(action, target) for target in targets):
        return
    target = denied or ", ".join(targets)
    raise HTTPException(
        status_code=403,
        detail=(
            f"'{principal.username}' is not allowed to {action} on '{target}'. "
            f"Roles held: {', '.join(principal.roles) or 'none'}."
        ),
    )


def _may_run(
    principal: Any, action: str, *, workflow_id: Optional[str] = None,
    pipeline_id: Optional[str] = None, job_id: Optional[str] = None,
) -> bool:
    """`_authorize_run` as a question instead of an answer.

    Needed because a schedule is authorized for *two* accounts: the one saving it
    and the one it will run as. Refusing the save has to say which of the two was
    the problem, so the second check cannot be the one that raises.
    """
    targets = _run_targets(workflow_id, pipeline_id, job_id)
    if any(principal.denies(action, target) for target in targets):
        return False
    return any(principal.allows(action, target) for target in targets)


#: What makes two schedules the same schedule. Deliberately not the whole block:
#: the Studio also keeps presentation there, and a save that only moved a label
#: must not demand permission to run.
def _schedule_shape(schedule: Any) -> Optional[Tuple[Any, ...]]:
    if schedule is None:
        return None
    return (schedule.cron, schedule.timezone, schedule.enabled, schedule.run_as)


def _read_schedule(kind: str, record_id: str, record: Any) -> Optional[Any]:
    """The schedule of a record as the scheduler itself would read it, or `None`.

    Going through `scheduling.from_record` rather than reading the dict here is
    the point: the guard has to judge the schedule that would actually fire, not
    a second interpretation of the same keys that could drift from it.
    """
    if not isinstance(record, dict):
        return None
    try:
        return scheduling.from_record(kind, record, record_id)
    except Exception:  # an unreadable record is not a schedule
        return None


def _authorize_schedule_change(
    principal: Any, kind: str, record_id: str, before: Any, after: Any
) -> None:
    """A schedule is a run. Saving one needs the permission to start it.

    Without this, `workspace:Write` was enough to schedule anything: someone who
    may edit a Job but not run it could write `0 6 * * *` into it and have the
    scheduler run it for them every morning. Worse, the `run_as` field names the
    account the run is authorized as, so the same save could borrow an
    administrator's access — permission granted by typing a username.

    So two questions, and both of them only when the schedule actually changed:

    * may **you** run this thing? Anything else lets writing substitute for
      executing.
    * and if you named somebody else in `run_as`, may you act for that account
      (`iam:ManageUsers`), does it exist, and may **it** run this thing? The last
      one matters on its own: naming an account that cannot run the Job produces
      a schedule that fails every morning at six, silently, forever.

    Skipped entirely when the runner has no users — that is the single-operator
    case, where the shared token is the only identity there is and there is
    nobody to escalate to.
    """
    if kind not in scheduling.KINDS or not _auth.has_users():
        return
    old, new = _read_schedule(kind, record_id, before), _read_schedule(kind, record_id, after)
    if _schedule_shape(old) == _schedule_shape(new):
        return

    ids: Dict[str, Optional[str]] = {
        "workflow_id": (new or old).workflow_id if (new or old) else None,
        "pipeline_id": record_id if kind == scheduling.PIPELINE else None,
        "job_id": record_id if kind == scheduling.JOB else None,
    }
    # Removing or disabling a schedule is checked the same way. It changes what
    # runs, and "may stop the nightly load" is not a lesser permission.
    _authorize_run(principal, "run:Execute", **ids)

    run_as = ((new.run_as if new else "") or "").strip()
    if not run_as or run_as == principal.username:
        return
    if not principal.allows("iam:ManageUsers"):
        raise HTTPException(
            status_code=403,
            detail=(
                f"'{principal.username}' cannot schedule a run as '{run_as}'. "
                "Scheduling for another account needs iam:ManageUsers; leave the "
                "field empty to run as yourself."
            ),
        )
    owner = _auth.principal_for(run_as)
    if owner is None:
        raise HTTPException(
            status_code=400,
            detail=f"There is no account named '{run_as}' to run this schedule as.",
        )
    if not _may_run(owner, "run:Execute", **ids):
        raise HTTPException(
            status_code=403,
            detail=(
                f"'{run_as}' is not allowed to run {kind}/{record_id}, so a schedule "
                "running as that account would fail on every occurrence. Grant it "
                "run:Execute on the target, or name another account."
            ),
        )


# ------------------------------------------------------------------- grants


def _grants_now() -> List[Any]:
    """The dataset, Job and Pipeline rules as they stand on disk right now.

    Deliberately not cached: revoking access has to take effect when somebody
    saves it, not when the runner is next restarted, and re-reading a few
    kilobytes of JSON beside a query that is about to start a Spark job costs
    nothing worth measuring.
    """
    try:
        raw = _workspace.read_meta().get("grants")
    except Exception:  # a missing or unreadable meta file is simply no rules
        return []
    return grants.load(raw)


def _owners_now() -> List[Any]:
    """Who owns each dataset, Job, Pipeline and Workflow, as it stands on disk.

    Read beside the grants and never cached, for the same reason: a transfer of
    ownership has to take effect when it is saved.
    """
    try:
        raw = _workspace.read_meta().get("owners")
    except Exception:  # a missing or unreadable meta file is simply no owners
        return []
    return grants.load_owners(raw)


def _catalog_now() -> Dict[str, Any]:
    """The dataset annotations as they stand on disk right now.

    Same record the Studio writes from the catalog screen, and re-read for the
    same reason the grants are: a tag added this morning has to govern this
    afternoon's query, not the one after the next restart.
    """
    try:
        raw = _workspace.read_meta().get("catalog")
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _tags_of(address: str) -> List[tuple]:
    """The tag scopes one dataset carries, from its catalog entry.

    A tag is the one container a dataset has that is not in its address: the
    catalog says `pii`, and a rule written on `tag/pii` reaches every table that
    says so, including the ones described after the rule was written.
    """
    entry = _catalog_now().get(_dataset_id(address))
    if not isinstance(entry, dict):
        return []
    tags = entry.get("tags")
    return grants.tag_scopes(
        tags if isinstance(tags, list) else [],
        {
            "classification": entry.get("classification"),
            "domain": entry.get("domain"),
        },
    )


def _column_tags_of(address: str, column: str) -> List[tuple]:
    """The tag scopes one column carries, from the catalog entry of its dataset.

    Column annotations live inside the dataset's entry, keyed by the lower-cased
    column name, because a column has no address that survives its table. A
    column that was never described carries nothing of its own and is governed by
    its table alone.
    """
    entry = _catalog_now().get(_dataset_id(address))
    if not isinstance(entry, dict):
        return []
    columns = entry.get("columns")
    if not isinstance(columns, dict):
        return []
    annotation = columns.get((column or "").strip().lower())
    if not isinstance(annotation, dict):
        return []
    tags = annotation.get("tags")
    return grants.tag_scopes(
        tags if isinstance(tags, list) else [],
        {"classification": annotation.get("classification")},
    )


def _parents_of(resource: str, resource_id: str) -> List[tuple]:
    """The containers one securable inherits from — the Workflow a Job or a
    Pipeline lives in, and the tags the catalog gives a dataset.

    A dataset's path ancestors need nothing here: they are in the address
    itself, and `grants.scope_chain` derives them without asking anybody. Its
    tags are the opposite — they exist only in the catalog entry somebody typed.
    Mirrors `parentsOf` in `src/store/iam.ts`.
    """
    if resource == "dataset":
        return _tags_of(resource_id)
    if resource == "column":
        # Both sets of tags, the column's first: a column classified
        # `restricted` inside an `internal` table has to be reached by a rule on
        # `tag/classification:restricted`, and the table's own vocabulary still
        # applies to it. `grants.scope_chain` puts the dataset above the column
        # on its own; only the tags have to be looked up here.
        parsed = grants.parse_column_resource(resource_id)
        if parsed is None:
            return []
        key, column = parsed
        seen = set()
        out: List[tuple] = []
        for scope in _column_tags_of(key, column) + _tags_of(key):
            if scope[1] in seen:
                continue
            seen.add(scope[1])
            out.append(scope)
        return out
    if resource == "secret":
        # A credential carries the catalog's own vocabulary, so one rule on
        # `tag/pii` closes the table and the password that opens it together.
        secret = _secrets_now().items.get(vault.normalize(resource_id))
        return grants.tag_scopes(secret.tags if secret else [], {})
    if resource not in grants.CONTAINED_KINDS or not resource_id:
        return []
    try:
        doc = _workspace.read(resource, resource_id)
    except Exception:
        return []
    if doc is None:
        return []
    workflow_id = str((doc.record or {}).get("workflowId") or "").strip()
    return [("workflow", workflow_id)] if workflow_id else []


#: What a securable created through this runner is governed by, before anybody
#: writes a rule. `creator+team` — the default — makes the person who created it
#: the owner and gives their team `write` over it, so a new Job is administered
#: by its author and still editable by the people beside them. `creator` leaves
#: out the team grant, which suits a runner where teams are departments rather
#: than squads. `off` restores the older behaviour: a new record is ungoverned,
#: and whoever may run anything may run it.
_NEW_RESOURCE_DEFAULTS = ("creator+team", "creator", "off")

#: What the team grant is worth, per kind. A credential is the exception: a
#: teammate may *use* `pg-prod` without being able to repoint it at another
#: database, which is the difference between `read` and `write` on a secret.
_TEAM_LEVEL = {
    "job": "write",
    "pipeline": "write",
    "workflow": "write",
    "dataset": "write",
    "secret": "read",
    "query": "write",
}


def _new_resource_policy() -> str:
    value = os.getenv("SPARQUET_STUDIO_NEW_RESOURCE_DEFAULT", "").strip().lower()
    return value if value in _NEW_RESOURCE_DEFAULTS else "creator+team"


def _claim_new_resources(principal: Any, targets: Iterable[tuple]) -> None:
    """The default access brand-new securables get, written once, at creation.

    Three conditions, and all three matter.

    It only fires for a **real user**: a token-only runner has no principal to
    name, and inventing an owner there would govern a record that nobody could
    then be matched against.

    It only fires when nothing in the securable's chain governs it yet. A Job
    saved into a Workflow that already has rules inherits them, and writing an
    owner here would quietly override the Workflow with something narrower —
    the opposite of what a container is for. `governed` is exactly that
    question, which is why the decision carries it apart from the level.

    And it writes the whole batch in one pass. A catalog save arrives as a map
    and can name several new datasets at once; claiming them one at a time would
    rewrite the meta file once per dataset.
    """
    policy = _new_resource_policy()
    user_id = getattr(principal, "user_id", None)
    if policy == "off" or not user_id or getattr(principal, "token_only", False):
        return

    wanted: List[tuple] = []
    seen = set()
    for resource, resource_id in targets:
        resource_id = str(resource_id or "").strip()
        if not resource_id or resource not in grants.OWNABLE_KINDS:
            continue
        if (resource, resource_id) in seen:
            continue
        seen.add((resource, resource_id))
        wanted.append((resource, resource_id))
    if not wanted:
        return

    try:
        meta = _workspace.read_meta()
    except Exception:  # an unreadable meta file is not worth failing a save over
        return
    owner_records = meta.get("owners")
    owner_records = list(owner_records) if isinstance(owner_records, list) else []
    grant_records = meta.get("grants")
    grant_records = list(grant_records) if isinstance(grant_records, list) else []

    rules = grants.load(grant_records)
    owners = grants.load_owners(owner_records)
    identity = _identity_of(principal)
    team_id = getattr(principal, "team_id", None)
    with_team = policy == "creator+team" and bool(team_id)

    claimed = False
    for resource, resource_id in wanted:
        decision = grants.evaluate(
            rules, owners, resource, resource_id, identity,
            _parents_of(resource, resource_id),
        )
        if decision.governed:
            continue
        claimed = True
        owner_records.append(
            {
                "resource": resource,
                "resourceId": resource_id,
                "principalKind": "user",
                "principalId": str(user_id),
            }
        )
        if with_team:
            grant_records.append(
                {
                    "resource": resource,
                    "resourceId": resource_id,
                    "principalKind": "team",
                    "principalId": str(team_id),
                    "level": _TEAM_LEVEL.get(resource, "write"),
                    "effect": "allow",
                }
            )
    if not claimed:
        return
    _workspace.write_meta("owners", owner_records)
    if with_team:
        _workspace.write_meta("grants", grant_records)


def _claim_new_resource(principal: Any, resource: str, resource_id: str) -> None:
    """One securable, by the rules of `_claim_new_resources`."""
    _claim_new_resources(principal, [(resource, resource_id)])


def _identity_of(principal: Any) -> Any:
    """The principals one caller is, in the shape `grants` matches against."""
    return grants.Identity(
        user_id=getattr(principal, "user_id", None),
        username=getattr(principal, "username", None),
        team_id=getattr(principal, "team_id", None),
    )


def _dataset_id(address: str) -> str:
    """An address as the Studio catalog keys it: trailing slashes gone, case kept.

    Must agree with `datasetKey` in `src/lib/lineage/lineage.ts`, or a rule
    written against `/data/orders` would miss a Job that names `/data/orders/`.
    """
    trimmed = (address or "").strip()
    return trimmed.rstrip("/") or trimmed


def _authorize_resource(
    principal: Any,
    resource: str,
    resource_id: str,
    level: str,
    rules: Optional[List[Any]] = None,
) -> None:
    """Refuse when a grant closes this dataset, Job or Pipeline to this caller.

    Skipped on a token-only runner, for the reason `auth.py` gives the shared
    token the admin policy: no users exist there, so no rule can name anybody,
    and a `*` deny would lock the single operator out of their own machine.
    """
    if getattr(principal, "token_only", False):
        return
    scoped = _grants_now() if rules is None else rules
    owners = _owners_now()
    if not scoped and not owners:
        return
    if grants.allows(
        scoped, resource, resource_id, _identity_of(principal), level,
        owners=owners, parents=_parents_of(resource, resource_id),
    ):
        return
    raise HTTPException(
        status_code=403,
        detail=grants.refusal(
            resource, resource_id, level, getattr(principal, "username", "")
        ),
    )


# ------------------------------------------------------------------ secrets


def _secrets_now() -> Any:
    """The secret store as it stands on disk right now.

    Re-read rather than cached, like the grants beside it: a credential rotated
    at nine has to be the one used at ten, and the store is a few kilobytes.
    """
    try:
        raw = _workspace.read_meta().get("secrets")
    except Exception:  # an unreadable meta file is simply no secrets
        return vault.Store()
    return vault.load(raw)


def _write_secrets(store: Any) -> None:
    _workspace.write_meta("secrets", store.as_dict())


def _resolve_secrets(
    document: Dict[str, Any], principal: Any
) -> Tuple[Dict[str, Any], List[str]]:
    """A copy of a pipeline with its `{secret:...}` references filled in.

    Two things happen here and both matter. Access is checked per secret before
    anything is decrypted, so a refusal costs nothing and names the secret rather
    than the database behind it. And the values that were used come back with the
    document, because the caller has to mask them out of whatever it prints — a
    driver that cannot connect quotes the URL it tried, password included.

    `read` is the level asked for. On a secret that means "may be used by a run",
    not "may be looked at": no level returns a value to a person.
    """
    if not document:
        return document, []
    names = vault.names_in(document)
    if not names:
        return document, []

    for name in names:
        _authorize_resource(principal, "secret", name, "read")

    store = _secrets_now()
    try:
        rendered, used = vault.render(
            document, lambda name, field: vault.value_of(store, name, field)
        )
    except vault.SecretError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    _audit.record(
        actor=getattr(principal, "username", None) or "anonymous",
        actor_id=getattr(principal, "user_id", None),
        team_id=getattr(principal, "team_id", None),
        action="secrets:Use",
        method="POST",
        path="/secrets/use",
        resource=", ".join(f"secret/{name}" for name in names),
        outcome=audit.ALLOWED,
    )
    return rendered, used


#: Meta records that are not bookkeeping but policy, and the action each needs.
#: `workspace:Write` covers the rest of `meta/*`, and an editor holds it — which
#: must not be a way to rewrite the rules that restrain the editor.
_META_GUARDS = {"grants": "iam:ManageGrants", "owners": "iam:ManageGrants"}


#: Meta records the generic endpoint refuses outright, whoever is asking. The
#: secret store is written only through `/secrets`, which encrypts, checks access
#: per secret and keeps the existing material when one field is rotated. A blanket
#: PUT would do none of that, and would let a caller replace every credential on
#: the runner with one request.
_META_SEALED = {"secrets"}


def _guard_meta_key(principal: Any, key: str, value: Any = None) -> None:
    if str(key) in _META_SEALED:
        raise HTTPException(
            status_code=403,
            detail=(
                f"'{key}' is not writable here. Connection secrets are managed "
                "through /secrets, which encrypts the value and checks access to "
                "each secret on its own."
            ),
        )
    action = _META_GUARDS.get(str(key))
    if action is None or getattr(principal, "token_only", False):
        return
    if principal.allows(action, "*") and not principal.denies(action, "*"):
        return
    # Not a platform administrator — but possibly the owner of the securables
    # being changed, which in this model is the whole point: whoever owns a table
    # grants on that table without being handed the runner. So the change is
    # inspected rather than the caller, and it passes only if every securable it
    # touches is one this caller owns or holds admin on.
    if _owner_may_change_meta(principal, str(key), value):
        return
    raise HTTPException(
        status_code=403,
        detail=(
            f"'{principal.username}' is not allowed to {action}, and does not own "
            "every resource this change touches. Access rules are changed by an "
            "administrator, or by the owner of the resource itself."
        ),
    )


def _meta_entry_keys(key: str, value: Any) -> set:
    """The `(kind, id)` securables one stored `grants`/`owners` value names."""
    records: List[Any]
    if isinstance(value, dict):
        records = list(value.values())
    elif isinstance(value, list):
        records = list(value)
    else:
        records = []
    out = set()
    for item in records:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("resource") or "")
        ident = str(item.get("resourceId") or "").strip()
        if kind in grants.RESOURCE_KINDS and ident:
            out.add((kind, ident))
    return out


def _owner_may_change_meta(principal: Any, key: str, value: Any) -> bool:
    """Whether this caller owns everything a `grants`/`owners` write changes.

    Deleting the whole record is never an owner's call: it would drop rules over
    resources they have nothing to do with, so it stays with `iam:ManageGrants`.
    Transferring ownership away is not one either — an owner may hand out
    privileges on what they own, and giving away the object itself is a change
    only an administrator or the current owner of that exact object can make,
    which is what `may_administer` already answers.
    """
    if key not in ("grants", "owners") or value is None:
        return False
    try:
        current_raw = _workspace.read_meta().get(key)
    except Exception:
        return False

    before = _meta_entry_keys(key, current_raw)
    after = _meta_entry_keys(key, value)
    touched = before.symmetric_difference(after) or before.union(after)
    if not touched:
        return False

    rules = _grants_now()
    owners = _owners_now()
    identity = _identity_of(principal)
    for kind, ident in touched:
        if not grants.may_administer(
            rules, owners, kind, ident, identity, parents=_parents_of(kind, ident)
        ):
            return False
    return True


def _authorize_datasets(principal: Any, config: Any) -> None:
    """Check every dataset a submitted JSON reads and writes, before it runs.

    Reading a table through a Job is still reading it: a deny that only covered
    `/query` would be walked around by running a two-line pipeline that selects
    from the table and writes it somewhere the caller does own. The addresses
    come from `history.lineage_of`, which is the same extraction the catalog
    derives its dataset list from — so a rule written in the catalog names the
    same string this reads back.
    """
    if getattr(principal, "token_only", False):
        return
    raw = history.lineage_of(config)
    if not raw:
        return
    try:
        lineage = json.loads(raw)
    except ValueError:
        return
    rules = _grants_now()
    if not rules:
        return
    for side, level in (("inputs", "read"), ("outputs", "write")):
        for entry in lineage.get(side) or []:
            address = _dataset_id(str(entry.get("address") or ""))
            if address:
                _authorize_resource(principal, "dataset", address, level, rules)


def _authorize_document(principal: Any, kind: str, record_id: str, level: str) -> None:
    """Layer two on one library file, for the kinds whose edit path is the only
    place a rule on them could be enforced.

    A Job and a Pipeline are checked where it matters most for them — the run
    endpoints, which is where reading a table actually happens. A saved query has
    no such endpoint for its *file*: `/query` runs a statement, and the statement
    can be pasted. So the rule on the file is enforced where the file is touched,
    which is here. Other kinds are left to their own paths rather than widened in
    passing; doing that for Jobs is a change with its own consequences.
    """
    if kind != workspace.QUERY:
        return
    clean = str(record_id or "").strip()
    if clean:
        _authorize_resource(principal, "query", clean, level)


def _workspace_resource(request: Request) -> str:
    """`job/j1` — what a workspace call is actually touching, so a role can be
    scoped to one record without the endpoints changing."""
    kind = request.path_params.get("kind", "*")
    record_id = request.path_params.get("record_id", "*")
    return f"{kind}/{record_id}"


# --------------------------------------------------------------------- app


app = FastAPI(
    title="Sparquet Studio local runner",
    version=SERVICE_VERSION,
    description="Executes Sparquet pipelines locally. Never expose this publicly.",
)

_audit = audit.AuditStore()

#: Methods that change something. Reads are recorded only when they are refused —
#: a log of every GET is a log nobody reads.
_WRITING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


def audit_detail(request: Request, **fields: Any) -> None:
    """Let a handler say *what* it changed, in words the log can show.

    The middleware knows the route and the outcome but not the meaning: that a
    PATCH on `/auth/users/u3` demoted somebody is something only the handler
    knows. Never pass a password, a token or a pipeline body — see `audit.py`.
    """
    existing = getattr(request.state, "audit_detail", None) or {}
    existing.update(fields)
    request.state.audit_detail = existing


@app.middleware("http")
async def _audit_middleware(request: Request, call_next: Callable) -> Any:
    response = await call_next(request)
    path = request.url.path
    if audit.is_quiet(path):
        return response
    status = response.status_code
    writing = request.method.upper() in _WRITING_METHODS
    refused = status in (401, 403, 402)
    if not writing and not refused:
        return response

    principal = getattr(request.state, "principal", None)
    if principal is None:
        # Refused before any dependency resolved an identity: the request still
        # gets an entry, because an unauthenticated probe is the event most worth
        # having. Resolving the session here is a read, and a cheap one.
        token = _session_token(request)
        principal = _auth.resolve_session(token) if token else None

    if status >= 500:
        outcome = audit.FAILED
    elif refused:
        outcome = audit.DENIED
    else:
        outcome = audit.ALLOWED

    _audit.record(
        actor=getattr(principal, "username", None) or "anonymous",
        actor_id=getattr(principal, "user_id", None),
        team=getattr(principal, "team_name", None),
        team_id=getattr(principal, "team_id", None),
        roles=list(getattr(principal, "roles", []) or []),
        action=audit.action_for(request.method, path),
        method=request.method.upper(),
        path=path,
        resource=getattr(request.state, "audit_resource", None),
        outcome=outcome,
        status=status,
        detail=getattr(request.state, "audit_detail", None),
        ip=request.client.host if request.client else None,
    )
    return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins(),
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    available = _spark_available()
    version = _framework_version()
    compatibility = compat.check(version)
    return HealthResponse(
        status="ok" if available and version and compatibility.supported else "degraded",
        version=SERVICE_VERSION,
        spark_available=available,
        framework_version=version,
        login_required=_auth.has_users(),
        credits_enforced=credits.enforced(),
        providers=providers.describe(),
        framework_supported=compatibility.supported,
        framework_message=compatibility.message,
        framework_requirement=compatibility.requirement,
    )


@app.post(
    "/validate",
    response_model=ValidateResponse,
    dependencies=[Depends(requires("run:Validate"))],
)
def validate(body: ValidateRequest) -> ValidateResponse:
    error = _parse_config_error(body.pipeline, body.params)
    return ValidateResponse(valid=error is None, error=error)


@app.post(
    "/dataset/schema",
    response_model=DatasetSchemaResponse,
    dependencies=[Depends(requires("catalog:Inspect"))],
)
def dataset_schema(
    body: DatasetSchemaRequest, principal: Any = Depends(current_principal)
) -> DatasetSchemaResponse:
    """The schema a dataset really has, read from the storage itself.

    The catalog in Studio derives a schema from the canvas, which is a claim
    about what a Job writes. This opens the dataset and asks Spark, so the two
    can be compared and a drift can be named. Nothing is written, and no rows
    are collected: a reader is built and only `df.schema` is taken.
    """
    _authorize_resource(principal, "dataset", _dataset_id(body.path), "read")

    # Options are where a connection lives — the JDBC url, the user, the password
    # — so they carry `{secret:...}` here for the same reason a Job's do. The
    # values come back to be masked out of the error below: a driver that cannot
    # connect quotes the whole URL it tried.
    options, secret_values = _resolve_secrets(dict(body.options or {}), principal)

    framework, restarted = _ensure_framework(_spark_for_formats(body.spark, [body.format]))

    config_module = _import("sparquet.core.config")
    factory = _import("sparquet.io.factory")
    try:
        config = config_module.InputConfig.from_dict(
            {"format": body.format, "path": body.path, "options": options}
        )
        _query_enter()
        try:
            reader = factory.ReaderFactory.create(framework.spark, config)
            schema = reader.read().schema
        finally:
            _query_exit()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=vault.mask(_describe_io(exc, body.format), secret_values),
        ) from exc

    return DatasetSchemaResponse(
        format=config.format,
        path=config.path,
        fields=[
            SchemaFieldOut(
                name=field.name,
                type=field.dataType.simpleString(),
                nullable=bool(field.nullable),
            )
            for field in schema.fields
        ],
        read_at=_now_iso(),
        session_restarted=restarted,
    )


#: A temp view name Spark accepts and that cannot carry SQL of its own — the
#: alias is interpolated into the `FROM` clause, so it is never free text.
_ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")

#: The only statements the SQL editor may send. Everything else — INSERT, DROP,
#: CREATE, MSCK, a `SET` that reconfigures the session — is refused before Spark
#: sees it. This is a guard against a slip in the editor, not a sandbox: whoever
#: holds `run:Execute` can already write anything through a Job.
_READ_ONLY_HEADS = ("select", "with", "explain", "describe", "show")

#: How long a query may run before the runner cancels it on its own.
MAX_QUERY_TIMEOUT_SECONDS = 900


def _strip_sql_comments(sql: str) -> str:
    """Removes `--` and block comments, leaving quoted text alone.

    A comment is how a second statement hides from a naive prefix check, as in
    `select 1 --` followed by a newline and `; drop table t`, so the read-only
    guard has to look at the SQL with the comments already gone.
    """
    out: List[str] = []
    index, size = 0, len(sql)
    quote: Optional[str] = None
    while index < size:
        char = sql[index]
        if quote is not None:
            out.append(char)
            if char == "\\" and quote != "`":  # an escaped quote stays inside the string
                if index + 1 < size:
                    out.append(sql[index + 1])
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue
        if char in "'\"`":
            quote = char
            out.append(char)
            index += 1
            continue
        if sql.startswith("--", index):
            end = sql.find("\n", index)
            index = size if end < 0 else end
            continue
        if sql.startswith("/*", index):
            end = sql.find("*/", index + 2)
            index = size if end < 0 else end + 2
            out.append(" ")
            continue
        out.append(char)
        index += 1
    return "".join(out)


def _splits_statement(sql: str) -> bool:
    """True when a `;` separates two statements. A `;` inside a string literal
    or a quoted identifier separates nothing, so quotes are tracked here too."""
    quote: Optional[str] = None
    index, size = 0, len(sql)
    while index < size:
        char = sql[index]
        if quote is not None:
            if char == "\\" and quote != "`":
                index += 2
                continue
            if char == quote:
                quote = None
        elif char in "'\"`":
            quote = char
        elif char == ";":
            return True
        index += 1
    return False


def _read_only_sql(sql: str) -> str:
    """Returns the single read-only statement in `sql`, or raises 400."""
    stripped = _strip_sql_comments(sql).strip()
    while stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()
    if not stripped:
        raise HTTPException(status_code=400, detail="Write a query first.")
    if _splits_statement(stripped):
        raise HTTPException(
            status_code=400,
            detail="Send one statement at a time: this query has more than one.",
        )
    head = stripped.split(None, 1)[0].lower()
    if head not in _READ_ONLY_HEADS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"'{head.upper()}' is not allowed here: the SQL editor only reads. "
                f"Start the query with {', '.join(word.upper() for word in _READ_ONLY_HEADS)}."
            ),
        )
    return stripped


def _query_group(query_id: str) -> str:
    return f"studio-query-{query_id}"


def _check_query_id(query_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", query_id):
        raise HTTPException(status_code=400, detail=f"Invalid query_id: {query_id!r}")
    return query_id


#: Everything in a tab id or a username that is not this is dropped before it
#: becomes part of a history key — the key is an identifier, not free text.
_HISTORY_WORD = re.compile(r"[^A-Za-z0-9_.@-]")

#: A statement longer than this is a generated one, and the history is there to
#: be read. Kept generously long: a hand-written query with a long CASE is not
#: unusual, and truncating one would make "put it back in the editor" a lie.
MAX_HISTORY_SQL = 20000


def _history_key(principal: Any, saved_query_id: Optional[str], tab: Optional[str]) -> str:
    """Which history a run is filed under, or "" for one nobody will read back.

    A saved query is keyed by its file, so everyone who may open the query sees
    the same history — that is the whole point of keeping it here rather than in
    a browser. A buffer with no file is keyed by the tab *and the person*: a
    scratch statement is nobody else's business, and the key being principal-
    scoped is what makes that true, rather than a filter somebody has to
    remember to apply on the way out.
    """
    saved = (saved_query_id or "").strip()
    if saved:
        return f"q:{saved}"
    scratch = _HISTORY_WORD.sub("", tab or "")[:64]
    if not scratch:
        return ""
    return f"t:{_HISTORY_WORD.sub('_', _run_as(principal, None))[:64]}:{scratch}"


def _record_query_run(
    key: str, body: QueryRequest, principal: Any, *, elapsed_ms: int, rows: int,
    truncated: bool, error: Optional[str],
) -> None:
    """Files one run under `key`, and never fails the query it describes."""
    if not key:
        return
    try:
        _history.record_query_run(
            key,
            sql=(body.sql or "").strip()[:MAX_HISTORY_SQL],
            limit=max(1, min(int(body.limit or DEFAULT_PREVIEW_LIMIT), MAX_PREVIEW_LIMIT)),
            elapsed_ms=elapsed_ms,
            rows=rows,
            truncated=truncated,
            error=error,
            run_as=_run_as(principal, None),
        )
    except Exception:  # a history that cannot be written is not a failed query
        pass


def _first_line(text: str) -> str:
    """The sentence that says what went wrong, without the stack under it."""
    return (text or "").strip().split("\n", 1)[0][:2000]


@app.post(
    "/query",
    response_model=QueryResponse,
    dependencies=[Depends(requires("catalog:Query"))],
)
def query(body: QueryRequest, principal: Any = Depends(current_principal)) -> QueryResponse:
    """Runs a statement and records what happened to it.

    The recording is the runner's rather than the browser's because the runner
    is the side that knows: how long it took, how many rows came back, whether
    the cap cut them, and what the failure said. A history a teammate reads has
    to be a record of what happened, not of what a client reported — and it is
    the same reason the history of a saved query lives next to the file rather
    than in whichever browser happened to run it.

    A failed run is kept too, including one refused before Spark saw it: "what
    did I run that broke" is asked at least as often as the other question.
    """
    key = _history_key(principal, body.saved_query_id, body.tab)
    started = time.perf_counter()
    try:
        answer = _run_query(body, principal)
    except HTTPException as exc:
        _record_query_run(
            key, body, principal,
            elapsed_ms=int((time.perf_counter() - started) * 1000),
            rows=0, truncated=False, error=_first_line(str(exc.detail)),
        )
        raise
    _record_query_run(
        key, body, principal, elapsed_ms=answer.elapsed_ms, rows=len(answer.rows),
        truncated=answer.truncated, error=None,
    )
    return answer


def _run_query(body: QueryRequest, principal: Any) -> QueryResponse:
    """Runs one read-only SQL statement over datasets opened by the framework.

    Each source is opened through the same `ReaderFactory` a Job uses and
    registered as a temp view, so the SQL sees exactly what a pipeline reads —
    Delta and Iceberg tables included — without the query having to know how to
    reach the storage. The views are temporary and dropped at the end; nothing
    is written.
    """
    # The formats the query names decide what the session has to be able to open,
    # so a Delta table is readable here even when no Job has ever mentioned it.
    framework, restarted = _ensure_framework(
        _spark_for_formats(body.spark, [source.format for source in body.sources])
    )

    statement = _read_only_sql(body.sql)
    limit = max(1, min(int(body.limit or DEFAULT_PREVIEW_LIMIT), MAX_PREVIEW_LIMIT))
    query_id = _check_query_id(body.query_id or secrets.token_hex(8))

    seen: Dict[str, str] = {}
    for source in body.sources:
        if not _ALIAS_RE.match(source.alias):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid view name {source.alias!r}: use a letter or '_' followed "
                    "by letters, digits or '_'."
                ),
            )
        if source.alias in seen:
            raise HTTPException(
                status_code=400, detail=f"Two sources are both named {source.alias!r}."
            )
        seen[source.alias] = source.path

    # Each source is a table someone may be denied. Checked before any of them is
    # opened, so a refused query never touches storage at all.
    rules = _grants_now()
    for source in body.sources:
        _authorize_resource(
            principal, "dataset", _dataset_id(source.path), "read", rules
        )

    # A saved query is its own securable. Checked apart from the sources because
    # it answers a different question — who may open this statement — and it
    # never widens the other answer: the tables are authorized above either way,
    # so `read` on a query is not a way to reach a table through somebody else's
    # file.
    if body.saved_query_id:
        _authorize_resource(principal, "query", body.saved_query_id, "read", rules)

    # Same as a Job: the connection lives in the options, and a reference there is
    # resolved before any source is opened, so a refused secret costs no storage
    # access at all. What was used is kept to mask the errors further down.
    source_options: List[Dict[str, Any]] = []
    query_secret_values: List[str] = []
    for source in body.sources:
        resolved, used = _resolve_secrets(dict(source.options or {}), principal)
        source_options.append(resolved)
        query_secret_values.extend(used)

    spark = framework.spark
    config_module = _import("sparquet.core.config")
    factory = _import("sparquet.io.factory")

    registered: List[str] = []
    timer: Optional[threading.Timer] = None
    started = time.perf_counter()
    # Counted while it runs so a concurrent request cannot restart the session
    # under it: rebuilding stops the JVM, which would kill this query.
    _query_enter()
    try:
        for index, source in enumerate(body.sources):
            try:
                config = config_module.InputConfig.from_dict(
                    {
                        "format": source.format,
                        "path": source.path,
                        "options": source_options[index],
                    }
                )
                reader = factory.ReaderFactory.create(spark, config)
                reader.read().createOrReplaceTempView(source.alias)
            except Exception as exc:
                raise HTTPException(
                    status_code=400,
                    detail=vault.mask(
                        f"Cannot open {source.path!r} as {source.alias}: "
                        f"{_describe_io(exc, source.format)}",
                        query_secret_values,
                    ),
                ) from exc
            registered.append(source.alias)

        context = spark.sparkContext
        # The group is what makes the query cancellable: the HTTP response only
        # comes back once the query is over, so cancelling has to name it.
        context.setJobGroup(_query_group(query_id), statement[:200], True)
        if body.timeout_seconds:
            seconds = max(1, min(int(body.timeout_seconds), MAX_QUERY_TIMEOUT_SECONDS))
            timer = threading.Timer(
                seconds, lambda: context.cancelJobGroup(_query_group(query_id))
            )
            timer.daemon = True
            timer.start()

        try:
            frame = spark.sql(statement)
            # limit + 1 tells us whether more rows exist without a second action
            rows = frame.limit(limit + 1).collect()
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=400, detail=vault.mask(_describe(exc), query_secret_values)
            ) from exc

        return QueryResponse(
            query_id=query_id,
            columns=[str(name) for name in frame.columns],
            fields=[
                SchemaFieldOut(
                    name=field.name,
                    type=field.dataType.simpleString(),
                    nullable=bool(field.nullable),
                )
                for field in frame.schema.fields
            ],
            rows=[[_json_safe(value) for value in row] for row in rows[:limit]],
            truncated=len(rows) > limit,
            elapsed_ms=int((time.perf_counter() - started) * 1000),
            session_restarted=restarted,
        )
    finally:
        _query_exit()
        if timer is not None:
            timer.cancel()
        try:
            spark.sparkContext.clearJobGroup()
        except Exception:  # a dead JVM has nothing left to clear
            pass
        for alias in registered:
            try:
                spark.catalog.dropTempView(alias)
            except Exception:  # the view outliving the request is harmless
                pass


_PARSE_POSITION = re.compile(r"\(line (\d+), pos (\d+)\)")


def _parse_failure(exc: Exception) -> Tuple[str, Optional[int], Optional[int]]:
    """A parser error as a message and a place, as far as it names one.

    Spark reports the position inside the message — `(line 3, pos 12)` — on both
    the Python and the JVM side of the error, so the position is read out of the
    text rather than out of an attribute that differs between the two.
    """
    # The exception class name belongs in a log, not in a tooltip six pixels
    # under the word it is about — so the message alone, unless there is none.
    text = str(exc).strip() or _describe(exc)
    # The JVM traceback and the echoed SQL under a parse error are noise there
    # too: the first paragraph is the sentence that says what is wrong.
    head = text.split("\n\n", 1)[0].strip()
    found = _PARSE_POSITION.search(text)
    if not found:
        return head, None, None
    return head, int(found.group(1)), int(found.group(2))


@app.post(
    "/query/validate",
    response_model=ValidateQueryResponse,
    dependencies=[Depends(requires("catalog:Query"))],
)
def validate_query(body: ValidateQueryRequest) -> ValidateQueryResponse:
    """Parses a statement without running it, so the editor can mark syntax.

    The check has to come from the parser that will execute the query. A
    hand-written one in the browser would disagree with Spark about its own
    dialect — lateral views, `QUALIFY`, backtick identifiers, interval literals
    — and an editor that underlines valid SQL is worse than one that underlines
    nothing.

    `parsePlan` is the whole check: it builds the logical plan and stops there,
    so it never touches storage, never starts a job and never needs the tables
    to exist. That last part matters — the editor validates while the statement
    is being typed, long before its views are registered.

    A session is used only if one is already up. Building one costs a JVM
    launch, which is not something a keystroke should pay for, and the answer
    without one is "not checked" rather than a guess.
    """
    statement = (body.sql or "").strip()
    # A buffer holding only comments is a statement nobody has written yet, not
    # a broken one. Marking it would put a squiggle under a note to self.
    if not _strip_sql_comments(statement).strip(" ;\n\t\r"):
        return ValidateQueryResponse(checked=False, reason="Nothing to check.")

    framework = _framework
    if framework is None:
        return ValidateQueryResponse(
            checked=False,
            reason="No SparkSession is up yet — syntax is checked once one is.",
        )

    # The read-only rule is part of what the editor will refuse, so it is worth
    # saying before the run rather than after it.
    try:
        statement = _read_only_sql(statement)
    except HTTPException as exc:
        return ValidateQueryResponse(
            checked=True, ok=False, message=str(exc.detail), line=1, column=0
        )

    try:
        session = framework.spark
        parser = session._jsparkSession.sessionState().sqlParser()
    except Exception as exc:  # an old Spark, or a session that died under us
        return ValidateQueryResponse(
            checked=False, reason=f"This Spark exposes no parser to ask: {_describe(exc)}"
        )

    try:
        parser.parsePlan(statement)
    except Exception as exc:
        message, line, column = _parse_failure(exc)
        return ValidateQueryResponse(
            checked=True, ok=False, message=message, line=line, column=column
        )

    return ValidateQueryResponse(checked=True, ok=True)


def _history_scope(
    principal: Any, saved_query_id: Optional[str], tab: Optional[str]
) -> str:
    """The key a history call may touch, refusing the ones it may not.

    Reading the history of a saved query is reading the query: the statements
    are in it. So the same `read` that governs the file governs this, and a
    scratch key is reachable only by the person whose runs are under it —
    `_history_key` builds that in rather than filtering it out.
    """
    key = _history_key(principal, saved_query_id, tab)
    if not key:
        raise HTTPException(
            status_code=400, detail="Name a saved query or a tab to read the history of."
        )
    if (saved_query_id or "").strip():
        _authorize_resource(principal, "query", (saved_query_id or "").strip(), "read")
    return key


def _query_run_out(run: Any) -> QueryRunOut:
    return QueryRunOut(
        id=run.id, at=run.at, sql=run.sql, limit=run.limit, elapsed_ms=run.elapsed_ms,
        rows=run.rows, truncated=run.truncated, error=run.error, run_as=run.run_as,
    )


@app.get(
    "/query/history",
    response_model=QueryHistoryResponse,
    dependencies=[Depends(requires("catalog:Query"))],
)
def query_history(
    saved_query_id: Optional[str] = None,
    tab: Optional[str] = None,
    limit: int = 0,
    principal: Any = Depends(current_principal),
) -> QueryHistoryResponse:
    """What this query has been run as, newest first.

    Shared by the runner: two people who may open the same saved query read the
    same history, and each run says who made it. A buffer nobody has saved yet
    has no file to share, so its runs are the caller's own.
    """
    key = _history_scope(principal, saved_query_id, tab)
    runs = _history.list_query_runs(key, limit=limit)
    return QueryHistoryResponse(runs=[_query_run_out(run) for run in runs])


@app.delete("/query/history", dependencies=[Depends(requires("catalog:Query"))])
def clear_query_history(
    saved_query_id: Optional[str] = None,
    tab: Optional[str] = None,
    principal: Any = Depends(current_principal),
) -> Dict[str, Any]:
    """Forgets one query's runs. Shared history, shared clearing: it removes the
    runs of everyone who has run this query, which is what "clear" has to mean
    once the record stopped being one browser's."""
    key = _history_scope(principal, saved_query_id, tab)
    return {"removed": _history.clear_query_runs(key)}


@app.post("/query/history/move", dependencies=[Depends(requires("catalog:Query"))])
def move_query_history(
    body: MoveHistoryRequest, principal: Any = Depends(current_principal)
) -> Dict[str, Any]:
    """Carries a scratch buffer's runs onto the file it was just saved as.

    Saving is what turns a private history into a shared one, so both ends are
    checked: the scratch key is the caller's by construction, and the file has
    to be one they may read.
    """
    source = _history_scope(principal, None, body.tab)
    target = _history_scope(principal, body.saved_query_id, None)
    return {"moved": _history.move_query_runs(source, target)}


@app.post("/query/{query_id}/cancel", dependencies=[Depends(requires("catalog:Query"))])
def cancel_query(query_id: str) -> Dict[str, Any]:
    """Stops a query that is still running. Cancelling one that already finished
    (or never started) is not an error — there is nothing left to interrupt."""
    _check_query_id(query_id)
    module = sys.modules.get("sparquet.core.context")
    session = getattr(getattr(module, "SparkContextManager", None), "_session", None)
    if session is None:
        return {"cancelled": False, "reason": "No SparkSession is running."}
    try:
        session.sparkContext.cancelJobGroup(_query_group(query_id))
    except Exception as exc:
        return {"cancelled": False, "reason": _describe(exc)}
    return {"cancelled": True}


def _engine_registry(engine_attr: str, module_name: str, class_name: str) -> Dict[str, Any]:
    """Prefers the live engine of the running framework instance: transformation
    and validator registrations are instance-scoped, unlike reader/writer ones."""
    engine = getattr(_framework, engine_attr, None) if _framework is not None else None
    if engine is None:
        engine = getattr(_import(module_name), class_name)()
    registry = getattr(engine, "_registry", None)
    return registry if isinstance(registry, dict) else {}


@app.get("/capabilities", response_model=CapabilitiesResponse)
def capabilities() -> CapabilitiesResponse:
    factory = _import("sparquet.io.factory")
    return CapabilitiesResponse(
        transformations=sorted(
            _engine_registry(
                "_transform_engine", "sparquet.transform.engine", "TransformationEngine"
            )
        ),
        readers=sorted(factory.ReaderFactory._registry),
        writers=sorted(factory.WriterFactory._registry),
        validators=sorted(
            _engine_registry(
                "_validation_engine", "sparquet.validation.engine", "ValidationEngine"
            )
        ),
    )


@app.post("/run", response_model=RunResponse)
def run(body: RunRequest, principal: Any = Depends(current_principal)) -> RunResponse:
    # Authorized here rather than through `Depends(requires(...))`: the Job being
    # run is in the body, so this is the first point where the permission can be
    # about *this* Job instead of about running in general.
    _authorize_run(
        principal, "run:Execute", workflow_id=body.workflow_id, job_id=body.job_id,
    )
    # Then the second, narrower layer: the rules written in the catalog, which
    # name the Job itself and every dataset the JSON touches. `run:Execute` says
    # this person may run things here; these say *what*.
    if body.job_id:
        _authorize_resource(principal, "job", body.job_id, "write")
    _authorize_datasets(principal, body.pipeline)
    # And the credentials the JSON references, before anything is decrypted or
    # any lock is taken: `rendered` is what Spark receives, `body.pipeline` — the
    # one with `{secret:...}` still in it — is what history and the screens keep.
    rendered, secret_values = _resolve_secrets(body.pipeline, principal)
    started = time.perf_counter()
    pipeline_name = body.pipeline.get("name")
    name = str(pipeline_name) if isinstance(pipeline_name, str) else None

    if body.dry_run:
        error = _parse_config_error(body.pipeline, body.params)
        return RunResponse(
            success=error is None,
            pipeline_name=name,
            duration_ms=_elapsed_ms(started),
            error=error,
            logs=[
                LogOut(
                    timestamp=_now_iso(),
                    level="INFO",
                    message="Dry run: configuration parsed, nothing executed",
                    context={},
                )
            ],
        )

    # Runs share one SparkSession, one runtime-variable store and one global
    # deferred-warning buffer, so they must never overlap.
    if not _RUN_LOCK.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="A pipeline run is already in progress on this runner.",
        )

    # Before anything is recorded: a run that cannot pay for even one write did
    # not happen, and the lock has to go back or the runner stays busy over a
    # refusal. What it will actually cost is only known once it has run.
    try:
        reservation = _admit_execution(
            principal, body.pipeline, job_name=body.job_name or name
        )
    except HTTPException:
        _RUN_LOCK.release()
        raise

    _ensure_catalog(
        workflow_id=body.workflow_id, job_id=body.job_id, name=body.job_name or name,
    )
    pipeline_run_id = _history.create_pipeline_run(
        kind="job", workflow_id=body.workflow_id, pipeline_id=None,
        job_id=body.job_id, name=body.job_name or name,
        run_as=_run_as(principal, body.run_as), launched=_launched(body.launched),
    )
    config_hash, config_text = _config_version(body.pipeline, body.params)
    job_run_id = _history.create_job_run(
        pipeline_run_id, job_id=body.job_id, name=body.job_name or name, stage_index=0,
        lineage=_lineage(body.pipeline, body.params),
        config_hash=config_hash, config=config_text,
    )
    tracker = history.StepTracker(_history, job_run_id)
    _ACTIVE_RUN.begin(pipeline_run_id)
    try:
        with _capture_logs(tracker.handle) as collector:
            response = _execute_run(
                body, name, started, collector, rendered, secret_values
            )
    finally:
        cancelled = _ACTIVE_RUN.cancelled
        _ACTIVE_RUN.end()
        _RUN_LOCK.release()
        # The run is over however it ended: the hold goes back now, and what the
        # writes really cost is taken below.
        _release_reservation(reservation)

    # Only the framework's own records exist here: this endpoint captures no JVM
    # stderr and no stdout — those are streamed, and only `/run/stream` opens them.
    recorder = _LogRecorder(_history, job_run_id)
    for entry in collector.records:
        recorder.add({**entry, "source": "pipeline"})
    recorder.flush()

    status = history.CANCELLED if cancelled else _job_outcome_status(response)
    error = CANCELLED_ERROR if cancelled else response.error
    if cancelled:
        tracker.close(CANCELLED_ERROR, status=history.CANCELLED)
        response.cancelled = True
        response.error = error
    else:
        tracker.close(response.error if not response.success else None)
    _history.finish_job_run(
        job_run_id, status=status, duration_ms=response.duration_ms,
        error=error, rows_read=response.rows_read,
        rows_written=response.rows_written,
    )
    _finish_pipeline_run(
        pipeline_run_id, status=status, duration_ms=response.duration_ms,
        error=error,
    )
    response.pipeline_run_id = pipeline_run_id
    response.job_run_id = job_run_id
    response.credits = _charge_out(_charge_execution(
        principal, body.pipeline, len(response.output_metrics),
        job_name=body.job_name or name, job_run_id=job_run_id,
        pipeline_run_id=pipeline_run_id, reservation=reservation,
        workflow_id=body.workflow_id,
        tags=_run_tags(
            workflow_id=body.workflow_id, job_id=body.job_id, extra=body.tags
        ),
    ))
    return response


def _masked(response: RunResponse, values: Iterable[str]) -> RunResponse:
    """The response with every resolved secret blanked out of it.

    Applied to the whole payload rather than to `error` alone. A JDBC driver
    quotes the URL it failed to open, password and all, and that string arrives
    in the error, in a log line, and sometimes in a warning attached to a step —
    masking only the field where it is expected to appear is how it gets out.
    """
    secret_values = [value for value in values if value]
    if not secret_values:
        return response
    return RunResponse(**vault.scrub(response.model_dump(), secret_values))


def _scrubbed(entry: Dict[str, Any], values: Iterable[str]) -> Dict[str, Any]:
    """One event, on its way out of the run queue.

    Applied where the queue is drained rather than where the stream is written,
    because the same event goes two places: to the browser as SSE and into the
    run history. A driver that quotes the URL it could not open would otherwise
    put the password on the screen once and in the database for good.
    """
    return vault.scrub(entry, values) if values else entry


def _execute_run(
    body: RunRequest,
    name: Optional[str],
    started: float,
    collector: _LogCollector,
    rendered: Optional[Dict[str, Any]] = None,
    secret_values: Iterable[str] = (),
) -> RunResponse:
    """The body shared by `/run` and `/run/stream`: execute the pipeline and shape
    the response. The caller owns the run lock and the log capture.

    `rendered` is the pipeline with its `{secret:...}` references already
    replaced. It is a separate argument, and not a rewritten `body.pipeline`,
    because everything else the caller does with the body — history, lineage,
    the config hash, the credit charge — must keep the references."""
    document = body.pipeline if rendered is None else rendered
    # The Job's own `spark` block only reaches Spark if the session is built with
    # it, and the session outlives the run before this one. Writing Delta on a
    # runner whose session was created plain failed here for the same reason
    # reading it failed in the SQL editor.
    pipeline = document if isinstance(document, dict) else {}
    framework, _ = _ensure_framework(
        _spark_for_formats(pipeline.get("spark"), _formats_of(pipeline)),
        run_lock_held=True,
    )
    try:
        result = framework.run_from_dict(document, params=body.params or None)
    except Exception as exc:
        # Config loading (missing keys, bad $include) raises outside the
        # pipeline's own try block and never reaches PipelineResult.
        return _masked(
            RunResponse(
                success=False,
                pipeline_name=name,
                duration_ms=_elapsed_ms(started),
                error=_describe(exc),
                logs=[LogOut(**entry) for entry in collector.records],
            ),
            secret_values,
        )

    output_df = getattr(result, "output_df", None)
    preview = (
        _build_preview(output_df, body.limit, collector)
        if output_df is not None
        else None
    )

    return _masked(RunResponse(
        success=bool(result.success),
        skipped=bool(getattr(result, "skipped", False)),
        pipeline_name=str(getattr(result, "pipeline_name", None) or name or ""),
        rows_read=int(getattr(result, "rows_read", 0) or 0),
        rows_written=int(getattr(result, "rows_written", 0) or 0),
        duration_ms=_elapsed_ms(started),
        error=getattr(result, "error", None),
        validations=_map_validations(getattr(result, "validation_results", [])),
        output_metrics=_map_output_metrics(getattr(result, "output_metrics", [])),
        preview=preview,
        logs=[LogOut(**entry) for entry in collector.records],
    ), secret_values)


@app.post("/run/stream")
def run_stream(
    body: RunRequest, principal: Any = Depends(current_principal)
) -> StreamingResponse:
    """Same execution as `/run`, but as Server-Sent Events, so Studio can paint
    per-step status and stream logs while Spark works.

    Events: `log` (one per pipeline/stdout/JVM line, carrying `source` and, for
    step markers, `context.index`/`context.step`), then a final `result` with the
    same payload `/run` returns, or `error`.

    Note on laziness: Spark builds a plan, so most transformations report applied
    almost instantly; the wall-clock time shows up on the read, the validations and
    the write — the actions that really touch data.
    """
    _authorize_run(
        principal, "run:Execute", workflow_id=body.workflow_id, job_id=body.job_id,
    )
    # Then the second, narrower layer: the rules written in the catalog, which
    # name the Job itself and every dataset the JSON touches. `run:Execute` says
    # this person may run things here; these say *what*.
    if body.job_id:
        _authorize_resource(principal, "job", body.job_id, "write")
    _authorize_datasets(principal, body.pipeline)
    # And the credentials the JSON references, before anything is decrypted or
    # any lock is taken: `rendered` is what Spark receives, `body.pipeline` — the
    # one with `{secret:...}` still in it — is what history and the screens keep.
    rendered, secret_values = _resolve_secrets(body.pipeline, principal)
    started = time.perf_counter()
    pipeline_name = body.pipeline.get("name")
    name = str(pipeline_name) if isinstance(pipeline_name, str) else None

    if not _RUN_LOCK.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="A pipeline run is already in progress on this runner.",
        )

    try:
        reservation = _admit_execution(
            principal, body.pipeline, job_name=body.job_name or name
        )
    except HTTPException:
        _RUN_LOCK.release()
        raise

    _ensure_catalog(
        workflow_id=body.workflow_id, job_id=body.job_id, name=body.job_name or name,
    )
    pipeline_run_id = _history.create_pipeline_run(
        kind="job", workflow_id=body.workflow_id, pipeline_id=None,
        job_id=body.job_id, name=body.job_name or name,
        run_as=_run_as(principal, body.run_as), launched=_launched(body.launched),
    )
    config_hash, config_text = _config_version(body.pipeline, body.params)
    job_run_id = _history.create_job_run(
        pipeline_run_id, job_id=body.job_id, name=body.job_name or name, stage_index=0,
        lineage=_lineage(body.pipeline, body.params),
        config_hash=config_hash, config=config_text,
    )
    tracker = history.StepTracker(_history, job_run_id)
    _ACTIVE_RUN.begin(pipeline_run_id)

    events: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue()
    box: Dict[str, Any] = {}

    def _work() -> None:
        collector = _StreamCollector(events, tracker.handle)
        log = logging.getLogger(FRAMEWORK_LOGGER)
        previous_level = log.level
        if log.getEffectiveLevel() > logging.INFO:
            log.setLevel(logging.INFO)
        log.addHandler(collector)
        try:
            with _capture_streams(events):
                box["response"] = _execute_run(
                    body, name, started, collector, rendered, secret_values
                )
        except Exception as exc:  # pragma: no cover - defensive
            box["error"] = vault.mask(_describe(exc), secret_values)
        finally:
            log.removeHandler(collector)
            log.setLevel(previous_level)
            events.put(None)  # sentinel: work finished

    def _stream() -> Iterator[str]:
        worker = threading.Thread(target=_work, daemon=True)
        worker.start()
        recorder = _LogRecorder(_history, job_run_id)
        try:
            # The ids travel in the very first event: Studio needs them to address
            # `POST /runs/{id}/cancel` while the run is still going.
            yield _sse("start", {
                "pipeline_name": name, "timestamp": _now_iso(),
                "pipeline_run_id": pipeline_run_id, "job_run_id": job_run_id,
            })
            while True:
                entry = events.get()
                if entry is None:
                    break
                entry = _scrubbed(entry, secret_values)
                recorder.add(entry)
                yield _sse("log", entry)
            recorder.flush()
            worker.join(timeout=5)
            cancelled = _ACTIVE_RUN.cancelled
            if "response" in box:
                response: RunResponse = box["response"]
                status = history.CANCELLED if cancelled else _job_outcome_status(response)
                # Whatever Spark raised on the way out is the cancellation itself,
                # not a defect in the pipeline — say so plainly.
                error = CANCELLED_ERROR if cancelled else response.error
                if cancelled:
                    tracker.close(CANCELLED_ERROR, status=history.CANCELLED)
                else:
                    tracker.close(response.error if not response.success else None)
                _history.finish_job_run(
                    job_run_id, status=status, duration_ms=response.duration_ms,
                    error=error, rows_read=response.rows_read,
                    rows_written=response.rows_written,
                )
                _finish_pipeline_run(
                    pipeline_run_id, status=status, duration_ms=response.duration_ms,
                    error=error,
                )
                response.pipeline_run_id = pipeline_run_id
                response.job_run_id = job_run_id
                response.cancelled = cancelled
                if cancelled:
                    response.error = error
                response.credits = _charge_out(_charge_execution(
                    principal, body.pipeline, len(response.output_metrics),
                    job_name=body.job_name or name, job_run_id=job_run_id,
                    pipeline_run_id=pipeline_run_id, reservation=reservation,
                    workflow_id=body.workflow_id,
                    tags=_run_tags(
                        workflow_id=body.workflow_id, job_id=body.job_id,
                        extra=body.tags,
                    ),
                ))
                yield _sse("result", response.model_dump())
            else:
                error_message = (
                    CANCELLED_ERROR if cancelled
                    else box.get("error", "Run finished without a result")
                )
                status = history.CANCELLED if cancelled else history.FAILED
                tracker.close(error_message, status=status)
                duration_ms = _elapsed_ms(started)
                _history.finish_job_run(
                    job_run_id, status=status, duration_ms=duration_ms,
                    error=error_message, rows_read=None, rows_written=None,
                )
                _finish_pipeline_run(
                    pipeline_run_id, status=status, duration_ms=duration_ms,
                    error=error_message,
                )
                yield _sse(
                    "error",
                    {
                        "error": error_message,
                        "cancelled": cancelled,
                        "pipeline_run_id": pipeline_run_id,
                        "job_run_id": job_run_id,
                    },
                )
        finally:
            # A client that hangs up mid-run closes the generator here: whatever
            # was buffered still belongs to the history of that run.
            recorder.flush()
            _ACTIVE_RUN.end()
            _RUN_LOCK.release()
            _release_reservation(reservation)

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


#: What a catalog record for a bare file is called. A prefix rather than a bare
#: path so no Studio-generated id can ever collide with one, and so a glance at a
#: row says where the record came from.
FILE_JOB_PREFIX = "file:"


def _file_job_id(path: str) -> str:
    """The catalog identity of a `.json` that no Job owns.

    A stage can run a file by path — written by another team, generated by a
    script, kept in another repository. Those runs used to land in the history
    with no `job_id` at all, which was enough for the timeline and left the
    catalog with no owner: nobody to tag, nobody to hold, and — because §9.4
    health is per Job — no row in `GET /health/jobs` and no alert rule that could
    ever see them. So the file gets an identity of its own, derived from its path
    so that two runs of the same file are two runs of the same object.

    It stays out of `job_id` on the request on purpose. `job_id` is what the IAM
    resource rules are written against, and minting one here would start refusing
    flows that run today under a policy that never had this id to name.
    """
    return FILE_JOB_PREFIX + path.strip().replace("\\", "/").lstrip("/")


def _stage_job_id(stage: FlowStageRequest) -> Optional[str]:
    """Which catalog record this stage's execution belongs to.

    The Job when a Job owns the file, the file itself when nothing does.
    """
    return stage.job_id or stage.file_job_id


def _file_job_name(path: str, pipeline: Dict[str, Any]) -> str:
    """What the catalog should call a file that owns itself.

    The `name` the config declares first: that is what the file calls itself, and
    what somebody reading the catalog will recognise. The file name otherwise.
    """
    declared = pipeline.get("name") if isinstance(pipeline, dict) else None
    if isinstance(declared, str) and declared.strip():
        return declared.strip()
    return path.rsplit("/", 1)[-1]


def _register_file_jobs(
    stages: List[FlowStageRequest], workflow_id: Optional[str]
) -> None:
    """Gives every bare file in this flow its catalog record.

    Created, never overwritten — the same rule the rest of the catalog follows: a
    run knows an id, and whoever curated the record afterwards knows what it is
    called. So a second run of the same file does not undo the name, description
    or tags somebody put on it, and the first run is still enough for the record
    to exist with a name and a path.

    Called after the flow is authorized: a run nobody was allowed to start should
    not leave a record behind saying it was.
    """
    for stage in stages:
        if not stage.file_job_id:
            continue
        # The path the id was built from, not the raw one the client sent: the
        # record and its identity must agree on how the file is spelled.
        path = stage.file_job_id[len(FILE_JOB_PREFIX):]
        try:
            _history.ensure_run_targets(
                workflow_id=workflow_id, pipeline_id=None, job_id=stage.file_job_id,
                name=_file_job_name(path, stage.pipeline), path=path,
            )
        except Exception:  # pragma: no cover - bookkeeping must never fail a run
            _log.warning("Could not register %s in the catalog.", stage.file_job_id)


def _resolve_staged_files(stages: List[FlowStageRequest]) -> None:
    """Turns every `path` stage into an inline `pipeline`, in place.

    Reading happens here, once, for the whole flow: everything downstream —
    charging, lineage, the config stored with the run — is written against
    `stage.pipeline`, and a stage that read its file late would be a stage the
    history recorded as running something it never saw.
    """
    for index, stage in enumerate(stages, start=1):
        named = (stage.path or "").strip()
        if named and stage.pipeline:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Stage {index} names both a file and an inline pipeline. "
                    "It runs one or the other."
                ),
            )
        if not named:
            if not stage.pipeline:
                raise HTTPException(
                    status_code=422,
                    detail=f"Stage {index} has neither a pipeline nor a file to run.",
                )
            continue
        try:
            stage.pipeline = _workspace.read_file(named)
        except workspace.WorkspaceError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        # The file is its own catalog record when no Job owns it, so the runs of
        # it have somewhere to hang: an owner, tags, and a health row. Only the
        # identity here — the record itself is written once the flow is allowed
        # to run, by `_register_file_jobs`.
        if not stage.job_id:
            stage.file_job_id = _file_job_id(named)


@app.post("/run/flow/stream")
def run_flow_stream(
    body: RunFlowRequest, principal: Any = Depends(current_principal)
) -> StreamingResponse:
    """Runs several pipelines in sequence — a composed flow, where each JSON is one
    stage — streaming per-stage progress as Server-Sent Events.

    The stages arrive already ordered and share one SparkSession, so a stage hands
    data to the next through whatever it wrote: a path the next one reads, or a
    `view` output registered as a temp view.

    Events: `start`, then per stage `stage_start` → `log`* → `stage_result`, and a
    final `result` (or `error`). Every `log` carries `stage_id`, so a line can
    always be traced back to the JSON that produced it.

    Stage markers travel through the same queue as the logs, so a stage's lines can
    never be attributed to its neighbour: the queue is FIFO.
    """
    _authorize_run(
        principal, "run:Execute", workflow_id=body.workflow_id,
        pipeline_id=body.pipeline_id,
    )
    if body.pipeline_id:
        _authorize_resource(principal, "pipeline", body.pipeline_id, "write")
    started = time.perf_counter()

    if not body.stages:
        raise HTTPException(status_code=422, detail="A flow needs at least one stage.")

    # Before anything is charged, locked or started: a stage that points at a file
    # gets that file read now, so a missing or unparseable one is a 400 naming it
    # rather than a flow that dies halfway with earlier stages already written.
    _resolve_staged_files(body.stages)

    # Every stage is a Job with its own datasets, and a flow is not a way around
    # a rule on any of them. Checked once for the whole flow, before the first
    # stage starts: a flow that stops halfway has already written something.
    stage_rendered: List[Dict[str, Any]] = []
    flow_secret_values: List[str] = []
    for stage in body.stages:
        if stage.job_id:
            _authorize_resource(principal, "job", stage.job_id, "write")
        _authorize_datasets(principal, stage.pipeline)
        # The credentials too, and for the same reason the datasets are checked
        # here: a flow refused its secret at stage three must not have written
        # stages one and two first.
        stage_document, stage_used = _resolve_secrets(stage.pipeline, principal)
        stage_rendered.append(stage_document)
        flow_secret_values.extend(stage_used)

    # One check for the flow, against its first stage: a Pipeline whose team has
    # nothing available should not start at all. Nothing is held here — each stage
    # reserves what it declares as it starts, and settles as it finishes.
    _precheck_execution(principal, body.stages[0].pipeline)

    if not _RUN_LOCK.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="A pipeline run is already in progress on this runner.",
        )

    _register_file_jobs(body.stages, body.workflow_id)
    _ensure_catalog(
        workflow_id=body.workflow_id, pipeline_id=body.pipeline_id, name=body.name,
        job_ids=[_stage_job_id(stage) for stage in body.stages],
    )
    pipeline_run_id = _history.create_pipeline_run(
        kind="pipeline", workflow_id=body.workflow_id, pipeline_id=body.pipeline_id,
        job_id=None, name=body.name,
        run_as=_run_as(principal, body.run_as), launched=_launched(body.launched),
    )
    _ACTIVE_RUN.begin(pipeline_run_id)

    events: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue()
    box: Dict[str, Any] = {"stages": [], "error": None, "preview": None}
    # The reservation each stage opens as it starts. The streaming body releases
    # whatever is still open when the flow ends, so a stage that raised — or a
    # client that hung up between stages — never leaves credit held.
    holds: List[Any] = []

    def _cancel_remaining(from_index: int) -> None:
        """Every stage the cancel kept from running, recorded and announced."""
        for stage_index in range(from_index, len(body.stages)):
            pending = body.stages[stage_index]
            _history.skip_job_run(
                pipeline_run_id, job_id=_stage_job_id(pending), name=pending.name,
                stage_index=stage_index, status=history.CANCELLED,
            )
            events.put({"__stage_cancelled__": {
                "index": stage_index, "id": pending.id, "name": pending.name,
            }})

    def _work() -> None:
        log = logging.getLogger(FRAMEWORK_LOGGER)
        previous_level = log.level
        if log.getEffectiveLevel() > logging.INFO:
            log.setLevel(logging.INFO)
        try:
            for index, stage in enumerate(body.stages):
                # A cancel between two stages stops the flow here: the stage that
                # was running took the Spark cancellation, the rest never start.
                if _ACTIVE_RUN.cancelled:
                    box["error"] = CANCELLED_ERROR
                    _cancel_remaining(index)
                    break
                # A flow is charged stage by stage, each for the destinations it
                # actually wrote (below, once the stage is done). A stage that
                # cannot be paid for is not started: the flow stops here and the
                # rest are recorded as SKIPPED, so a flow that runs out of credit
                # at stage four has really run three and the ledger says so.
                try:
                    stage_hold = _admit_execution(
                        principal, stage.pipeline, job_name=stage.name
                    )
                    holds.append(stage_hold)
                except HTTPException as refusal:
                    box["error"] = str(refusal.detail)
                    _history.skip_job_run(
                        pipeline_run_id, job_id=_stage_job_id(stage), name=stage.name,
                        stage_index=index, status=history.FAILED,
                    )
                    for pending_index in range(index + 1, len(body.stages)):
                        pending = body.stages[pending_index]
                        _history.skip_job_run(
                            pipeline_run_id, job_id=_stage_job_id(pending), name=pending.name,
                            stage_index=pending_index, status=history.SKIPPED,
                        )
                    break
                stage_hash, stage_config = _config_version(stage.pipeline, stage.params)
                job_run_id = _history.create_job_run(
                    pipeline_run_id, job_id=_stage_job_id(stage), name=stage.name,
                    stage_index=index,
                    lineage=_lineage(stage.pipeline, stage.params),
                    config_hash=stage_hash, config=stage_config,
                )
                # The marker carries the job execution the next lines belong to, so
                # the generator can file this stage's logs under it.
                events.put({"__stage__": {"index": index, "id": stage.id,
                                          "name": stage.name,
                                          "job_run_id": job_run_id}})
                tracker = history.StepTracker(_history, job_run_id)
                # A fresh collector per stage keeps each stage's `logs` its own.
                collector = _StreamCollector(events, tracker.handle)
                log.addHandler(collector)
                stage_started = time.perf_counter()
                # Only the last stage's preview is kept: it is the flow's output.
                is_last = index == len(body.stages) - 1
                request = RunRequest(
                    pipeline=stage.pipeline,
                    params=stage.params,
                    limit=body.limit,
                )
                try:
                    with _capture_streams(events):
                        response = _execute_run(
                            request, stage.name, stage_started, collector,
                            stage_rendered[index], flow_secret_values,
                        )
                except Exception as exc:  # pragma: no cover - defensive
                    response = RunResponse(
                        success=False,
                        pipeline_name=stage.name,
                        duration_ms=_elapsed_ms(stage_started),
                        error=_describe(exc),
                    )
                finally:
                    log.removeHandler(collector)

                stage_cancelled = _ACTIVE_RUN.cancelled
                if stage_cancelled:
                    tracker.close(CANCELLED_ERROR, status=history.CANCELLED)
                    job_status = history.CANCELLED
                    stage_error: Optional[str] = CANCELLED_ERROR
                else:
                    tracker.close(response.error if not response.success else None)
                    job_status = _job_outcome_status(response)
                    stage_error = response.error
                _history.finish_job_run(
                    job_run_id, status=job_status, duration_ms=response.duration_ms,
                    error=stage_error, rows_read=response.rows_read,
                    rows_written=response.rows_written,
                )

                charge = _charge_out(_charge_execution(
                    principal, stage.pipeline, len(response.output_metrics),
                    job_name=stage.name, job_run_id=job_run_id,
                    pipeline_run_id=pipeline_run_id, reservation=stage_hold,
                    workflow_id=body.workflow_id,
                    tags=_run_tags(
                        workflow_id=body.workflow_id, pipeline_id=body.pipeline_id,
                        job_id=_stage_job_id(stage), extra=body.tags,
                    ),
                ))
                payload = {
                    "index": index,
                    "id": stage.id,
                    "name": stage.name,
                    "job_run_id": job_run_id,
                    "credits": charge.model_dump() if charge else None,
                    "success": response.success,
                    "skipped": response.skipped,
                    "cancelled": stage_cancelled,
                    "rows_read": response.rows_read,
                    "rows_written": response.rows_written,
                    "duration_ms": response.duration_ms,
                    "error": stage_error,
                    "validations": [v.model_dump() for v in response.validations],
                    "output_metrics": [m.model_dump() for m in response.output_metrics],
                }
                box["stages"].append(payload)
                if is_last and response.preview is not None:
                    box["preview"] = response.preview.model_dump()
                events.put({"__stage_result__": payload})

                if stage_cancelled:
                    box["error"] = CANCELLED_ERROR
                    _cancel_remaining(index + 1)
                    break

                if not response.success and body.stop_on_error:
                    box["error"] = (
                        f"Stage {index + 1} ({stage.name or stage.id}) failed: "
                        f"{response.error or 'unknown error'}"
                    )
                    # The remaining stages never run — persist and announce them as
                    # SKIPPED rather than leaving them stuck looking "pending" forever.
                    for skipped_index in range(index + 1, len(body.stages)):
                        skipped_stage = body.stages[skipped_index]
                        _history.skip_job_run(
                            pipeline_run_id, job_id=skipped_stage.job_id,
                            name=skipped_stage.name, stage_index=skipped_index,
                        )
                        events.put({"__stage_skipped__": {
                            "index": skipped_index, "id": skipped_stage.id,
                            "name": skipped_stage.name,
                        }})
                    break
        except Exception as exc:  # pragma: no cover - defensive
            box["error"] = vault.mask(_describe(exc), flow_secret_values)
        finally:
            log.setLevel(previous_level)
            events.put(None)  # sentinel: work finished

    def _stream() -> Iterator[str]:
        worker = threading.Thread(target=_work, daemon=True)
        worker.start()
        current: Optional[str] = None
        recorder = _LogRecorder(_history)
        try:
            yield _sse("start", {"flow": True, "total": len(body.stages),
                                 "timestamp": _now_iso(),
                                 "pipeline_run_id": pipeline_run_id})
            while True:
                entry = events.get()
                if entry is None:
                    break
                entry = _scrubbed(entry, flow_secret_values)
                marker = entry.get("__stage__")
                if marker is not None:
                    current = marker["id"]
                    recorder.switch(marker["job_run_id"])
                    yield _sse("stage_start", marker)
                    continue
                skipped = entry.get("__stage_skipped__")
                if skipped is not None:
                    yield _sse("stage_skipped", skipped)
                    continue
                stopped = entry.get("__stage_cancelled__")
                if stopped is not None:
                    yield _sse("stage_cancelled", stopped)
                    continue
                result = entry.get("__stage_result__")
                if result is not None:
                    # The stage is over: its lines are complete, so they go in now
                    # rather than waiting for the next stage to push them.
                    recorder.flush()
                    yield _sse("stage_result", result)
                    continue
                recorder.add(entry)
                yield _sse("log", {**entry, "stage_id": current})
            recorder.flush()
            worker.join(timeout=5)
            cancelled = _ACTIVE_RUN.cancelled
            stages = box["stages"]
            overall_success = (
                bool(stages) and all(s["success"] for s in stages)
                and len(stages) == len(body.stages)
            )
            if cancelled:
                status = history.CANCELLED
            elif overall_success:
                status = history.SUCCESS
            else:
                status = history.FAILED
            _finish_pipeline_run(
                pipeline_run_id, status=status,
                duration_ms=_elapsed_ms(started), error=box["error"],
            )
            yield _sse(
                "result",
                {
                    "id": pipeline_run_id,
                    "success": overall_success,
                    "cancelled": cancelled,
                    "duration_ms": _elapsed_ms(started),
                    "stages": stages,
                    "preview": box["preview"],
                    "error": box["error"],
                },
            )
        finally:
            recorder.flush()
            _ACTIVE_RUN.end()
            _RUN_LOCK.release()
            # A stage that raised, or a client that hung up between stages, leaves
            # its hold open. Releasing is idempotent, so the ones already settled
            # cost nothing here.
            for hold in holds:
                _release_reservation(hold)

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class StepRunOut(BaseModel):
    id: str
    job_run_id: str
    scope: str
    step_index: int
    type: str
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_ms: Optional[int] = None
    error_message: Optional[str] = None
    error_details: Optional[str] = None
    # Set on the quality datasets only ("report"/"valid"/"invalid"): those are
    # addressed by role, not by a position in a lane. See `history.StepTracker`.
    role: Optional[str] = None
    # What the framework reported about the step, as a JSON object: rows, path,
    # format, whether a rule passed. Studio shows it when a past run is reopened.
    details: Optional[str] = None


class JobRunOut(BaseModel):
    id: str
    pipeline_run_id: str
    job_id: Optional[str] = None
    name: Optional[str] = None
    stage_index: int
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None
    rows_read: Optional[int] = None
    rows_written: Optional[int] = None
    # `{"inputs": [...], "outputs": [...]}` as a JSON string — the datasets this
    # execution read and wrote, taken from the JSON that was submitted.
    lineage: Optional[str] = None
    # `sha256:<hex>` over the JSON that ran, so two executions of the same Job can
    # be told apart after the Job has been edited. The JSON itself is read with
    # `GET /job-runs/{id}/config`; it is too large to ship with every listing.
    config_hash: Optional[str] = None
    #: What this execution was charged, so the price is visible in the history
    #: next to the work it paid for. Null for a local run, which is free.
    credits: Optional[RunChargeOut] = None
    steps: List[StepRunOut] = Field(default_factory=list)


class PipelineRunOut(BaseModel):
    id: str
    kind: str
    workflow_id: Optional[str] = None
    pipeline_id: Optional[str] = None
    job_id: Optional[str] = None
    name: Optional[str] = None
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None
    # Who it ran as, and how it was started: "manual", "scheduled" or "api".
    run_as: Optional[str] = None
    launched: Optional[str] = None
    #: Kept forever: retention skips this run whatever its age.
    pinned: bool = False
    jobs: List[JobRunOut] = Field(default_factory=list)


def _step_run_out(step: Any) -> StepRunOut:
    return StepRunOut(**vars(step))


def _job_run_out(job: Any, charges: Optional[Dict[str, Any]] = None) -> JobRunOut:
    data = {key: value for key, value in vars(job).items() if key != "steps"}
    entry = (charges or {}).get(job.id)
    return JobRunOut(
        **data, credits=_entry_charge_out(entry) if entry else None,
        steps=[_step_run_out(step) for step in job.steps],
    )


def _pipeline_run_out(run: Any, charges: Optional[Dict[str, Any]] = None) -> PipelineRunOut:
    data = {key: value for key, value in vars(run).items() if key != "jobs"}
    return PipelineRunOut(
        **data, jobs=[_job_run_out(job, charges) for job in run.jobs]
    )


@app.get(
    "/runs",
    response_model=List[PipelineRunOut],
    dependencies=[Depends(requires("history:Read"))],
)
def list_runs(
    workflow_id: Optional[str] = None,
    pipeline_id: Optional[str] = None,
    job_id: Optional[str] = None,
    limit: int = 20,
) -> List[PipelineRunOut]:
    """Past executions of a Job or Pipeline, most recent first. `jobs`/`steps` come
    back empty here — fetch `/runs/{id}` for the full nested detail."""
    runs = _history.list_pipeline_runs(
        workflow_id=workflow_id, pipeline_id=pipeline_id, job_id=job_id,
        limit=min(max(limit, 1), 200),
    )
    return [_pipeline_run_out(run) for run in runs]


class RunGroupOut(BaseModel):
    key: Optional[str] = None
    label: str
    runs: int
    failed: int
    duration_ms_avg: Optional[int] = None
    duration_ms_total: int


class RunDayOut(BaseModel):
    day: str
    runs: int
    failed: int


class RunMetricsOut(BaseModel):
    period: str
    total: int
    succeeded: int
    failed: int
    other: int
    duration_ms_avg: Optional[int] = None
    duration_ms_p50: Optional[int] = None
    duration_ms_p95: Optional[int] = None
    duration_ms_total: int
    days: List[RunDayOut] = Field(default_factory=list)
    groups: List[RunGroupOut] = Field(default_factory=list)
    group_by: str


# Declared before `/runs/{run_id}`: routes match in the order they are added, and
# the other way round this path would be read as a run whose id is "metrics".
@app.get(
    "/runs/metrics",
    response_model=RunMetricsOut,
    dependencies=[Depends(requires("history:Read"))],
)
def run_metrics(
    period: Optional[str] = None,
    group_by: str = "pipeline",
    workflow_id: Optional[str] = None,
    limit: int = 20,
) -> RunMetricsOut:
    """How much this runner ran in one month, and how it went.

    The operational half of Billing. Credits count what was charged, which leaves
    out every local run and every run that failed before writing; this counts
    executions, so a month that cost nothing still has a shape. Same month, same
    breakdown dimensions, read from the execution history instead of the ledger.
    """
    metrics = _history.run_metrics(
        period=period, group_by=group_by, workflow_id=workflow_id,
        limit=min(max(limit, 1), 100),
    )
    return RunMetricsOut(
        **{
            key: value
            for key, value in vars(metrics).items()
            if key not in ("days", "groups")
        },
        days=[RunDayOut(**vars(day)) for day in metrics.days],
        groups=[RunGroupOut(**vars(group)) for group in metrics.groups],
    )


@app.get(
    "/runs/{run_id}",
    response_model=PipelineRunOut,
    dependencies=[Depends(requires("history:Read"))],
)
def get_run(run_id: str) -> PipelineRunOut:
    """One execution in full: every job it ran (or skipped) and every step of each,
    so Studio can open a past run and jump straight to whichever step failed."""
    run = _history.get_pipeline_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Execution not found.")
    # One lookup for the whole run rather than one per stage: a Pipeline with
    # twenty Jobs would otherwise be twenty queries for twenty small rows.
    charges = _credits.entries_for_job_runs([job.id for job in run.jobs])
    return _pipeline_run_out(run, charges)


class PinRequest(BaseModel):
    pinned: bool = True


class PinResponse(BaseModel):
    run_id: str
    pinned: bool


@app.post(
    "/runs/{run_id}/pin",
    response_model=PinResponse,
    dependencies=[Depends(requires("history:Pin"))],
)
def pin_run(run_id: str, body: PinRequest) -> PinResponse:
    """Keeps one execution forever. Retention expires history by age; this is how
    the run of an incident survives it."""
    if not _history.set_pinned(run_id, body.pinned):
        raise HTTPException(status_code=404, detail="Execution not found.")
    return PinResponse(run_id=run_id, pinned=body.pinned)


class PurgeResponse(BaseModel):
    dry_run: bool
    runs_thinned: int
    runs_deleted: int
    logs_deleted: int
    steps_deleted: int
    configs_dropped: int
    rows_removed: int
    vacuumed: bool
    policy: Dict[str, Any]


@app.post(
    "/runs/purge",
    response_model=PurgeResponse,
    dependencies=[Depends(requires("history:Purge"))],
)
def purge_runs(dry_run: bool = False) -> PurgeResponse:
    """Applies the retention policy now. `dry_run=true` answers with exactly what
    would go and touches nothing — worth doing first, since the second stage
    deletes rows for good."""
    policy = history.RetentionPolicy.from_env()
    report = _history.purge(policy, dry_run=dry_run)
    return PurgeResponse(**report.as_dict(), policy=vars(policy))


class RunIngestResponse(BaseModel):
    pipeline_run_id: str
    job_run_id: str
    records: int
    duration_ms: int


@app.post(
    "/runs/ingest",
    response_model=RunIngestResponse,
    dependencies=[Depends(requires("history:Ingest"))],
)
def ingest_run(document: Dict[str, Any] = Body(...)) -> RunIngestResponse:
    """Records a run that happened somewhere else.

    The framework runs anywhere and depends on nothing, which is exactly why the
    runs that matter most — the nightly job on Databricks, the DAG on Airflow —
    used to leave no trace here at all. With `SPARQUET_HISTORY_URL` pointed at this
    endpoint, the framework reports itself and those runs read back like any other:
    same steps, same logs, same screens. They are marked `launched="external"`, so
    a reader can always tell what this runner executed from what it merely heard
    about, and they consume no credits here — the compute was not ours.

    The document is what `sparquet.observability.history` produces; anything else
    is refused with 400 rather than stored as a run that says the wrong thing.
    """
    try:
        recorded = history.ingest_run(_history, document)
    except history.IngestError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return RunIngestResponse(**recorded)


# --------------------------------------------------------------- workspace
#
# The library as files on disk. The browser holds no authoritative copy: it reads
# this on load and writes back on every change, so what a user has is a directory
# they can diff, review and commit like any other source, and a second machine
# opening the same checkout sees the same library.


# ------------------------------------------------------- monitoring & alerts


class JobHealthOut(BaseModel):
    job_id: str
    name: Optional[str] = None
    workflow_id: Optional[str] = None
    last_run_id: Optional[str] = None
    last_status: Optional[str] = None
    last_started_at: Optional[str] = None
    last_finished_at: Optional[str] = None
    last_duration_ms: Optional[int] = None
    last_rows_read: Optional[int] = None
    last_rows_written: Optional[int] = None
    last_error: Optional[str] = None
    last_success_at: Optional[str] = None
    consecutive_failures: int = 0
    runs: int = 0
    failures: int = 0
    #: Past successful runs, newest first — the shape of the Job over time, which
    #: is what a sparkline draws and what a median rule compares against.
    durations: List[int] = Field(default_factory=list)
    volumes: List[int] = Field(default_factory=list)


class MonitorOut(BaseModel):
    id: str
    kind: str
    job_id: str
    threshold: float
    baseline: str
    window: int
    enabled: bool
    name: Optional[str] = None
    created_at: str
    updated_at: str
    #: The rule in words, built by the server so the interface and the webhook
    #: describe it the same way.
    rule: str


class MonitorIn(BaseModel):
    kind: str
    job_id: str = monitoring.ANY_JOB
    threshold: float = 1.0
    baseline: str = monitoring.ABSOLUTE
    window: int = monitoring.DEFAULT_WINDOW
    enabled: bool = True
    name: Optional[str] = None


class MonitorPatch(BaseModel):
    kind: Optional[str] = None
    job_id: Optional[str] = None
    threshold: Optional[float] = None
    baseline: Optional[str] = None
    window: Optional[int] = None
    enabled: Optional[bool] = None
    name: Optional[str] = None


class MonitorStateOut(BaseModel):
    monitor_id: str
    job_id: str
    firing: bool
    reason: str
    since: Optional[str] = None
    checked_at: Optional[str] = None
    value: Optional[float] = None
    baseline: Optional[float] = None
    run_id: Optional[str] = None
    #: Denormalised so the alert list reads without a second request.
    kind: Optional[str] = None
    rule: Optional[str] = None
    name: Optional[str] = None
    job_name: Optional[str] = None


class MonitorEventOut(BaseModel):
    id: str
    monitor_id: str
    job_id: str
    at: str
    firing: bool
    reason: str
    value: Optional[float] = None
    baseline: Optional[float] = None
    run_id: Optional[str] = None


class MonitorSweepOut(BaseModel):
    checked: int
    firing: int
    transitions: List[MonitorEventOut] = Field(default_factory=list)


def _job_facts(health: Any) -> Any:
    """`history.JobHealth` as `monitoring.JobFacts`.

    The one place the two modules meet. Written out field by field rather than
    passed as a dict so that a field added on one side and not the other fails
    here, where it is obvious, instead of silently evaluating a rule against a
    default.
    """
    return monitoring.JobFacts(
        job_id=health.job_id,
        name=health.name,
        workflow_id=health.workflow_id,
        last_run_id=health.last_run_id,
        last_status=health.last_status,
        last_started_at=health.last_started_at,
        last_finished_at=health.last_finished_at,
        last_duration_ms=health.last_duration_ms,
        last_rows_read=health.last_rows_read,
        last_rows_written=health.last_rows_written,
        last_error=health.last_error,
        last_success_at=health.last_success_at,
        consecutive_failures=health.consecutive_failures,
        runs=health.runs,
        failures=health.failures,
        durations=list(health.durations),
        volumes=list(health.volumes),
    )


def _monitor_out(monitor: Any) -> MonitorOut:
    return MonitorOut(**vars(monitor), rule=monitor.describe())


def _sweep_monitors() -> MonitorSweepOut:
    """Evaluates every rule once, against the health of every Job right now."""
    health = _history.job_health()
    facts = [_job_facts(record) for record in health]
    _monitors.forget([record.job_id for record in health])
    transitions = monitoring.sweep(_monitors, facts)
    firing = sum(1 for state in _monitors.states() if state.firing)
    return MonitorSweepOut(
        checked=len(facts),
        firing=firing,
        transitions=[MonitorEventOut(**vars(event)) for event in transitions],
    )


def _watch_monitors() -> None:
    """Runs the sweep on a timer, and again whenever a run has just finished.

    Failures are logged and swallowed, like the purge above: a rule that could not
    be evaluated is a gap in the alerting, not a reason for the runner to stop
    serving the people who are trying to fix whatever it was going to tell them
    about.
    """
    interval = monitoring.interval_seconds()
    while True:
        try:
            report = _sweep_monitors()
            for event in report.transitions:
                _log.info(
                    "Monitor %s %s for job %s: %s",
                    event.monitor_id,
                    "firing" if event.firing else "resolved",
                    event.job_id,
                    event.reason,
                )
        except Exception as exc:  # pragma: no cover - defensive
            _log.warning("Monitor sweep failed: %s", exc)
        _MONITOR_WAKE.wait(interval)
        _MONITOR_WAKE.clear()


@app.get(
    "/health/jobs",
    response_model=List[JobHealthOut],
    dependencies=[Depends(requires("monitoring:Read"))],
)
def job_health(samples: int = 20) -> List[JobHealthOut]:
    """Every Job in the library and what its own runs say about it.

    The answer to a question the run list cannot be asked: the run list is
    ordered by time, so a Job that stopped running a week ago is not near the
    top of it — it is nowhere in it.
    """
    return [JobHealthOut(**vars(record)) for record in _history.job_health(samples=samples)]


@app.get(
    "/monitors",
    response_model=List[MonitorOut],
    dependencies=[Depends(requires("monitoring:Read"))],
)
def list_monitors() -> List[MonitorOut]:
    return [_monitor_out(monitor) for monitor in _monitors.list()]


@app.post(
    "/monitors",
    response_model=MonitorOut,
    dependencies=[Depends(requires("monitoring:Manage"))],
)
def create_monitor(body: MonitorIn) -> MonitorOut:
    try:
        monitor = _monitors.create(
            body.kind, job_id=body.job_id, threshold=body.threshold,
            baseline=body.baseline, window=body.window, enabled=body.enabled,
            name=body.name,
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))
    return _monitor_out(monitor)


@app.patch(
    "/monitors/{monitor_id}",
    response_model=MonitorOut,
    dependencies=[Depends(requires("monitoring:Manage"))],
)
def update_monitor(monitor_id: str, body: MonitorPatch) -> MonitorOut:
    changes = {key: value for key, value in body.model_dump().items() if value is not None}
    try:
        monitor = _monitors.update(monitor_id, **changes)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))
    if monitor is None:
        raise HTTPException(status_code=404, detail="No such monitor.")
    return _monitor_out(monitor)


@app.delete(
    "/monitors/{monitor_id}",
    dependencies=[Depends(requires("monitoring:Manage"))],
)
def delete_monitor(monitor_id: str) -> Dict[str, bool]:
    if not _monitors.delete(monitor_id):
        raise HTTPException(status_code=404, detail="No such monitor.")
    return {"deleted": True}


@app.get(
    "/monitors/status",
    response_model=List[MonitorStateOut],
    dependencies=[Depends(requires("monitoring:Read"))],
)
def monitor_status(firing_only: bool = False) -> List[MonitorStateOut]:
    """What every rule is currently saying, per Job.

    A wildcard rule has one row per Job here — which is the point: "anything that
    failed" is one rule to write and a list of the Jobs it is true of to read.
    """
    rules = {monitor.id: monitor for monitor in _monitors.list()}
    names = {
        record.job_id: record.name for record in _history.job_health(samples=2)
    }
    out: List[MonitorStateOut] = []
    for state in _monitors.states():
        if firing_only and not state.firing:
            continue
        monitor = rules.get(state.monitor_id)
        out.append(
            MonitorStateOut(
                **vars(state),
                kind=monitor.kind if monitor else None,
                rule=monitor.describe() if monitor else None,
                name=monitor.name if monitor else None,
                job_name=names.get(state.job_id),
            )
        )
    return out


@app.get(
    "/monitors/events",
    response_model=List[MonitorEventOut],
    dependencies=[Depends(requires("monitoring:Read"))],
)
def monitor_events(limit: int = 50, monitor_id: Optional[str] = None) -> List[MonitorEventOut]:
    """The transitions, newest first: when each alert started and when it cleared."""
    return [
        MonitorEventOut(**vars(event))
        for event in _monitors.events(limit=limit, monitor_id=monitor_id)
    ]


@app.post(
    "/monitors/evaluate",
    response_model=MonitorSweepOut,
    dependencies=[Depends(requires("monitoring:Read"))],
)
def evaluate_monitors() -> MonitorSweepOut:
    """Runs the sweep now instead of waiting for the timer.

    Read rather than manage: asking a question of the history changes no rule,
    and somebody who has just fixed a Job wants the alert to clear without
    waiting a minute to find out whether it did.
    """
    return _sweep_monitors()


# ----------------------------------------------------------------- schedules


class ScheduleOut(BaseModel):
    """One scheduled Job or Pipeline, as the library states it plus what the
    runner knows: when it fires next and when it last did."""

    kind: str
    id: str
    name: str
    cron: str
    timezone: str
    enabled: bool
    run_as: str = ""
    workflow_id: Optional[str] = None
    #: Why this schedule cannot fire — an expression that does not parse, or an
    #: account that no longer exists. Null when there is nothing wrong with it.
    error: Optional[str] = None
    rule: str
    next_fire: Optional[str] = None
    last_fire: Optional[str] = None
    last_run_id: Optional[str] = None
    last_status: Optional[str] = None


class ScheduleFireOut(BaseModel):
    kind: str
    id: str
    name: str
    #: The occurrence this fire is for, which is not the same as when it started:
    #: a run held up by the one before it starts late and is still that occurrence.
    due_at: str
    started: bool = False
    run_id: Optional[str] = None
    error: Optional[str] = None


class ScheduleSweepOut(BaseModel):
    checked: int
    fired: int
    fires: List[ScheduleFireOut] = Field(default_factory=list)


def _library_schedules() -> Tuple[List[Any], Any]:
    """Every schedule in the library, with the snapshot they were read from.

    The snapshot comes back too because whatever fires needs the same read: the
    file a Job compiles to, the stages of a Pipeline, the names those stages sort
    by. Reading the library twice would leave a run using half of one version.
    """
    snapshot = _workspace.snapshot()
    triples = [(scheduling.JOB, doc.id, doc.record) for doc in snapshot.jobs]
    triples += [(scheduling.PIPELINE, doc.id, doc.record) for doc in snapshot.pipelines]
    return scheduling.read_schedules(triples), snapshot


def _schedule_document(snapshot: Any, kind: str, doc_id: str) -> Optional[Any]:
    documents = snapshot.jobs if kind == scheduling.JOB else snapshot.pipelines
    for doc in documents:
        if doc.id == doc_id:
            return doc
    return None


def _schedule_principal(schedule: Any) -> Optional[Any]:
    """Who a scheduled run is authorized as.

    The account that saved the schedule, assembled the way a login assembles it,
    so a schedule can never outlive the access of whoever wrote it: take the
    person's roles away and their schedules stop running with them. There is no
    service account here on purpose — an identity that belongs to nobody is an
    identity nobody notices still has access.

    With no users on this runner the shared token is the only identity there is,
    which is the single-operator case `Principal.token_only` exists for.
    """
    if not _auth.has_users():
        return auth.TOKEN_PRINCIPAL
    username = (schedule.run_as or "").strip()
    if not username:
        return None
    return _auth.principal_for(username)


def _drain_stream(response: Any) -> Optional[str]:
    """Consumes a streaming response to the end, with nobody watching it.

    `/run/flow/stream` is the only path that runs a Pipeline, and it is a stream
    because that is what the Studio needs. A schedule has no browser to show the
    events to, but it still has to consume every one of them: releasing the run
    lock, settling the credit holds and closing the run in the history all happen
    in the generator's `finally`, and a generator nobody finishes never gets there.

    The run id is picked out of the first event on the way past, so a scheduled
    Pipeline can be linked to its run the way a scheduled Job is.
    """
    iterator = getattr(response, "body_iterator", None)
    if iterator is None:
        return None
    found: List[str] = []

    async def _consume() -> None:
        async for chunk in iterator:
            if found:
                continue
            text = chunk.decode("utf-8", "replace") if isinstance(chunk, bytes) else str(chunk)
            match = re.search(r'"pipeline_run_id":\s*"([^"]+)"', text)
            if match:
                found.append(match.group(1))

    asyncio.run(_consume())
    return found[0] if found else None


def _fire_job(schedule: Any, snapshot: Any, principal: Any) -> RunResponse:
    """Runs a scheduled Job through `/run`, exactly as pressing run would.

    What executes is the compiled JSON in the library — the same file the
    framework runs and the same one code review saw — read at the moment it
    fires. Nothing is cached here: a Job edited this morning runs as edited.
    """
    doc = _schedule_document(snapshot, scheduling.JOB, schedule.id)
    if doc is None:
        raise HTTPException(status_code=404, detail="This Job is no longer in the library.")
    if not doc.path:
        raise HTTPException(
            status_code=409,
            detail="This Job has no compiled file in the library yet. Open and save it once.",
        )
    return run(
        RunRequest(
            pipeline=_workspace.read_file(doc.path),
            job_id=schedule.id,
            job_name=schedule.name,
            workflow_id=schedule.workflow_id,
            run_as=schedule.run_as or None,
            launched=history.SCHEDULED,
        ),
        principal,
    )


def _fire_pipeline(schedule: Any, snapshot: Any, principal: Any) -> Optional[str]:
    """Runs a scheduled Pipeline through `/run/flow/stream`, drained here.

    Every stage points at a file — the Job's compiled JSON, or the `.json` the
    stage names directly — so the Pipeline runs what the library holds rather
    than anything this process compiled itself.
    """
    doc = _schedule_document(snapshot, scheduling.PIPELINE, schedule.id)
    if doc is None:
        raise HTTPException(
            status_code=404, detail="This Pipeline is no longer in the library."
        )
    raw_stages = doc.record.get("stages")
    stages = [item for item in raw_stages if isinstance(item, dict)] if isinstance(raw_stages, list) else []
    if not stages:
        raise HTTPException(status_code=422, detail="This Pipeline has no stages.")
    links = doc.record.get("links") if isinstance(doc.record.get("links"), list) else []

    jobs = {job.id: job for job in snapshot.jobs}
    names: Dict[str, str] = {}
    for stage in stages:
        job = jobs.get(str(stage.get("jobId") or ""))
        path = str(stage.get("path") or "")
        label = str(job.record.get("name") or "") if job else (path.rsplit("/", 1)[-1] if path else "")
        names[str(stage.get("id") or "")] = label

    ordered, cyclic = scheduling.order_stages(stages, links, names)
    if cyclic:
        _log.warning(
            "Pipeline %s has stages on a cycle (%s); they run last.",
            schedule.name, ", ".join(cyclic),
        )

    flow_stages: List[FlowStageRequest] = []
    for stage in ordered:
        stage_id = str(stage.get("id") or "")
        job_id = str(stage.get("jobId") or "")
        path = str(stage.get("path") or "")
        if not path:
            job = jobs.get(job_id)
            if job is None or not job.path:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Stage {names.get(stage_id) or stage_id} points at a Job with no "
                        "compiled file in the library."
                    ),
                )
            path = job.path
        flow_stages.append(
            FlowStageRequest(
                id=stage_id,
                name=names.get(stage_id) or None,
                path=path,
                job_id=job_id or None,
            )
        )

    return _drain_stream(
        run_flow_stream(
            RunFlowRequest(
                stages=flow_stages,
                pipeline_id=schedule.id,
                name=schedule.name,
                workflow_id=schedule.workflow_id,
                run_as=schedule.run_as or None,
                launched=history.SCHEDULED,
            ),
            principal,
        )
    )


def _fire_schedule(schedule: Any, snapshot: Any, due: datetime) -> ScheduleFireOut:
    """One occurrence, started and reported. Never raises.

    A refusal is an outcome, not an exception: the runner already had a run in
    progress, the account is gone, the Job was deleted. Each of those is written
    down and the sweep carries on to the next schedule — one broken schedule must
    not stop the twelve that are fine.
    """
    report = ScheduleFireOut(
        kind=schedule.kind, id=schedule.id, name=schedule.name,
        due_at=due.isoformat().replace("+00:00", "Z"),
    )
    principal = _schedule_principal(schedule)
    if principal is None:
        report.error = (
            f"{schedule.run_as!r} is not an account on this runner."
            if schedule.run_as
            else "This schedule names no account to run as."
        )
        return report
    try:
        if schedule.kind == scheduling.JOB:
            response = _fire_job(schedule, snapshot, principal)
            report.started = True
            report.run_id = response.pipeline_run_id
            report.error = response.error
        else:
            report.run_id = _fire_pipeline(schedule, snapshot, principal)
            report.started = True
    except HTTPException as exc:
        # 409 is the overlap case and the common one: this runner shares a single
        # SparkSession, so a Job still running when its next occurrence comes
        # round means that occurrence is skipped rather than queued. Queueing
        # would turn a slow morning into a backlog nobody asked for.
        report.error = str(exc.detail)
    except Exception as exc:  # pragma: no cover - defensive
        report.error = _describe(exc)
    return report


def _sweep_schedules() -> ScheduleSweepOut:
    """Fires whatever is due, one at a time, in the order the library lists it."""
    now = datetime.now(timezone.utc)
    schedules, snapshot = _library_schedules()
    grace = scheduling.grace_seconds()
    fires: List[ScheduleFireOut] = []
    for schedule in schedules:
        key = f"{schedule.kind}:{schedule.id}"
        with _SCHEDULE_LOCK:
            anchor = _SCHEDULE_ANCHORS.get(key, _SCHEDULER_STARTED_AT)
        due = scheduling.due_at(
            schedule, anchor=anchor, now=now, grace_seconds=grace
        )
        if due is None:
            continue
        # Recorded as fired before it runs, and against `now` rather than the
        # occurrence: a run that takes an hour must not come back to a clock that
        # thinks its next two occurrences are still owed.
        with _SCHEDULE_LOCK:
            _SCHEDULE_ANCHORS[key] = datetime.now(timezone.utc)
        fires.append(_fire_schedule(schedule, snapshot, due))
    return ScheduleSweepOut(
        checked=len(schedules),
        fired=sum(1 for fire in fires if fire.started),
        fires=fires,
    )


def _watch_schedules() -> None:
    """The timer. Failures are logged and swallowed, like the monitor sweep."""
    interval = scheduling.interval_seconds()
    while True:
        try:
            report = _sweep_schedules()
            for fire in report.fires:
                _log.info(
                    "Schedule fired %s %s (due %s): %s",
                    fire.kind, fire.name, fire.due_at,
                    fire.error or fire.run_id or "started",
                )
        except Exception as exc:  # pragma: no cover - defensive
            _log.warning("Schedule sweep failed: %s", exc)
        _SCHEDULE_WAKE.wait(interval)
        _SCHEDULE_WAKE.clear()


def _last_scheduled_run(schedule: Any) -> Optional[Any]:
    """The most recent run this schedule started, for the screen.

    Read from the history rather than from the anchor above, because the anchor
    only knows about this process: a runner restarted an hour ago would otherwise
    show a Job that runs every morning as having never run.
    """
    selector = (
        {"job_id": schedule.id}
        if schedule.kind == scheduling.JOB
        else {"pipeline_id": schedule.id}
    )
    try:
        recent = _history.list_pipeline_runs(limit=20, **selector)
    except Exception:  # pragma: no cover - defensive
        return None
    for record in recent:
        if getattr(record, "launched", None) == history.SCHEDULED:
            return record
    return None


def _schedule_out(schedule: Any, now: datetime) -> ScheduleOut:
    upcoming = scheduling.next_fire(schedule, now) if schedule.runnable else None
    last = _last_scheduled_run(schedule)
    return ScheduleOut(
        kind=schedule.kind, id=schedule.id, name=schedule.name, cron=schedule.cron,
        timezone=schedule.timezone, enabled=schedule.enabled,
        run_as=schedule.run_as, workflow_id=schedule.workflow_id,
        error=schedule.error, rule=schedule.describe(),
        next_fire=upcoming.isoformat().replace("+00:00", "Z") if upcoming else None,
        last_fire=getattr(last, "started_at", None) if last else None,
        last_run_id=getattr(last, "id", None) if last else None,
        last_status=getattr(last, "status", None) if last else None,
    )


@app.get(
    "/schedules",
    response_model=List[ScheduleOut],
    dependencies=[Depends(requires("workspace:Read"))],
)
def list_schedules() -> List[ScheduleOut]:
    """Every schedule the library carries, with its next and last fire.

    Guarded by `workspace:Read` rather than an action of its own: a schedule is a
    field of a record, and whoever may read the record already reads it. A second
    permission over the same bytes would only be able to disagree with the first.
    """
    now = datetime.now(timezone.utc)
    schedules, _ = _library_schedules()
    return [_schedule_out(schedule, now) for schedule in schedules]


@app.post(
    "/schedules/evaluate",
    response_model=ScheduleSweepOut,
    dependencies=[Depends(requires("run:Execute"))],
)
def evaluate_schedules() -> ScheduleSweepOut:
    """Sweeps now instead of waiting for the timer.

    `run:Execute` and not a read: unlike the monitors' evaluate, this one starts
    runs. Each still runs as the account its own schedule names, so pressing it
    cannot be a way to run something as somebody else.
    """
    return _sweep_schedules()


# ------------------------------------------------------------------- metrics

#: Prometheus asks for this exact content type, down to the version.
_PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _escape_label(value: Optional[str]) -> str:
    """Backslash, quote and newline, in that order — the order matters."""
    text = "" if value is None else str(value)
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _labels(pairs: Dict[str, Optional[str]]) -> str:
    inner = ",".join(f'{key}="{_escape_label(value)}"' for key, value in pairs.items())
    return "{" + inner + "}"


def _epoch_seconds(value: Optional[str]) -> Optional[float]:
    moment = monitoring._parse_iso(value)
    return moment.timestamp() if moment else None


def _render_metrics(health: List[Any], states: List[Any], rules: Dict[str, Any]) -> str:
    """The exposition text, built from the same two reads the screens use.

    Everything here is a gauge, including the counts. A Prometheus counter has to
    be monotonic over the process's life, and these are read from a database whose
    old rows are purged on a schedule — a `_total` that goes down on purge day
    would make every `rate()` over it wrong. So the counts are named for what they
    are: how many runs are *on record* for this Job.
    """
    lines: List[str] = []

    def series(name: str, kind: str, help_text: str) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {kind}")

    series("sparquet_job_last_run_success", "gauge",
           "1 if the last recorded run of this Job succeeded, 0 otherwise.")
    for record in health:
        if record.last_status is None:
            continue
        labels = _labels({
            "job": record.name or record.job_id,
            "job_id": record.job_id,
            "workflow_id": record.workflow_id or "",
        })
        lines.append(
            f"sparquet_job_last_run_success{labels} "
            f"{1 if record.last_status == history.SUCCESS else 0}"
        )

    for name, kind, help_text, read in (
        ("sparquet_job_last_run_timestamp_seconds", "gauge",
         "When the last recorded run of this Job started, in epoch seconds.",
         lambda record: _epoch_seconds(record.last_started_at)),
        ("sparquet_job_last_success_timestamp_seconds", "gauge",
         "When this Job last succeeded, in epoch seconds. Missing means never.",
         lambda record: _epoch_seconds(record.last_success_at)),
        ("sparquet_job_last_run_duration_seconds", "gauge",
         "How long the last recorded run of this Job took.",
         lambda record: (record.last_duration_ms / 1000.0)
         if record.last_duration_ms is not None else None),
        ("sparquet_job_last_rows_written", "gauge",
         "Rows the last recorded run of this Job wrote.",
         lambda record: record.last_rows_written),
        ("sparquet_job_last_rows_read", "gauge",
         "Rows the last recorded run of this Job read.",
         lambda record: record.last_rows_read),
        ("sparquet_job_consecutive_failures", "gauge",
         "Failed runs since this Job last succeeded.",
         lambda record: record.consecutive_failures),
        ("sparquet_job_runs_recorded", "gauge",
         "Runs of this Job still on record, within the sampled window.",
         lambda record: record.runs),
        ("sparquet_job_failures_recorded", "gauge",
         "Failed runs of this Job still on record, within the sampled window.",
         lambda record: record.failures),
    ):
        series(name, kind, help_text)
        for record in health:
            value = read(record)
            if value is None:
                continue
            labels = _labels({
                "job": record.name or record.job_id,
                "job_id": record.job_id,
                "workflow_id": record.workflow_id or "",
            })
            lines.append(f"{name}{labels} {value}")

    series("sparquet_monitor_firing", "gauge",
           "1 while this rule is firing for this Job, 0 while it is not.")
    for state in states:
        monitor = rules.get(state.monitor_id)
        labels = _labels({
            "monitor": state.monitor_id,
            "monitor_name": (monitor.name if monitor else None) or "",
            "kind": monitor.kind if monitor else "",
            "job_id": state.job_id,
        })
        lines.append(f"sparquet_monitor_firing{labels} {1 if state.firing else 0}")

    series("sparquet_monitors_firing", "gauge",
           "How many (rule, Job) pairs are firing right now.")
    lines.append(f"sparquet_monitors_firing {sum(1 for state in states if state.firing)}")

    series("sparquet_runner_info", "gauge",
           "Always 1. The labels carry the runner's version.")
    lines.append(f'sparquet_runner_info{_labels({"version": SERVICE_VERSION})} 1')

    return "\n".join(lines) + "\n"


@app.get("/metrics", dependencies=[Depends(requires("monitoring:Read"))])
def metrics() -> Response:
    """The health of every Job in Prometheus exposition format.

    Text over HTTP and nothing else — no client library, no push gateway, no
    OpenTelemetry SDK. A scraper is a thing that reads a page, and the whole
    contract is four hundred bytes of documented text; taking a dependency to
    produce it would be taking a dependency, and its upgrades, to concatenate
    strings.

    Guarded by `monitoring:Read` like the screens, which for a scraper means the
    runner token in a bearer header — Prometheus has `authorization` in its scrape
    config for exactly this.
    """
    health = _history.job_health()
    rules = {monitor.id: monitor for monitor in _monitors.list()}
    return Response(
        content=_render_metrics(health, _monitors.states(), rules),
        media_type=_PROMETHEUS_CONTENT_TYPE,
    )


class WorkspaceDocumentOut(BaseModel):
    kind: str
    id: str
    record: Dict[str, Any]
    #: Relative path of the reviewable file, so the UI can tell the user what to commit.
    path: Optional[str] = None
    #: What the record's bytes hash to right now. Send it back on the next save and
    #: the runner refuses to overwrite a change made since. Empty means the record
    #: is not on disk yet. An older Studio that ignores it keeps the old behaviour:
    #: last write wins.
    revision: Optional[str] = None


class WorkspaceSnapshotOut(BaseModel):
    root: str
    workflows: List[WorkspaceDocumentOut] = Field(default_factory=list)
    jobs: List[WorkspaceDocumentOut] = Field(default_factory=list)
    pipelines: List[WorkspaceDocumentOut] = Field(default_factory=list)
    #: Saved SQL queries. An older Studio ignores the field; a newer one talking
    #: to an older runner gets an empty list, which is the same as "none saved".
    queries: List[WorkspaceDocumentOut] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)


class WorkspaceWriteRequest(BaseModel):
    #: The Studio record exactly as the editor holds it.
    record: Dict[str, Any]
    #: A Job's compiled Sparquet JSON. Sent so the readable file is the pipeline the
    #: framework runs, not an editor-shaped document nobody can execute.
    config: Optional[Dict[str, Any]] = None
    #: The revision this edit started from. Omit it and the save is unconditional,
    #: which is what every client did before this existed. Send it — `""` for a
    #: record believed to be new — and a save that would land on top of somebody
    #: else's is refused with 409 instead of quietly winning.
    revision: Optional[str] = None


class WorkspaceDeleteResponse(BaseModel):
    deleted: bool


def _workspace_doc_out(doc: Any) -> WorkspaceDocumentOut:
    return WorkspaceDocumentOut(
        kind=doc.kind, id=doc.id, record=doc.record, path=doc.path,
        revision=getattr(doc, "revision", None),
    )


def _mirror_catalog(doc: Any) -> None:
    """Copies what the workspace just wrote into the catalog tables.

    The files are the source of truth; the catalog is the queryable index that the
    run foreign keys point at. Failing to index must never fail a save — the next
    write, or a run of the record, puts it back.
    """
    record = doc.record if isinstance(doc.record, dict) else {}
    name = record.get("name")
    description = record.get("description")
    try:
        tags = history.normalize_tags(record.get("tags"))
        if doc.kind == workspace.WORKFLOW:
            _history.upsert_workflow(
                doc.id, name=name, description=description, path=doc.path, tags=tags
            )
        elif doc.kind == workspace.JOB:
            _history.upsert_job(
                doc.id, workflow_id=record.get("workflowId"), name=name,
                description=description, path=doc.path, tags=tags,
            )
        elif doc.kind == workspace.PIPELINE:
            stages = record.get("stages")
            _history.upsert_pipeline(
                doc.id, workflow_id=record.get("workflowId"), name=name,
                description=description, path=doc.path,
                stages=stages if isinstance(stages, list) else None, tags=tags,
            )
    except Exception:  # pragma: no cover - indexing is bookkeeping, not the save
        _log.warning("Could not index %s %s in the catalog.", doc.kind, doc.id)


def _public_meta(meta: Any) -> Dict[str, Any]:
    """The meta records a browser may load — which is every one but the secrets.

    `workspace:Read` is held by anyone who may open the library, and this is the
    read Studio starts with. The secret store belongs to a narrower audience and
    to no browser at all: `/secrets` answers with names, field names and tags,
    and the runner reads the material straight off disk when a run needs it.
    Sending the record here would put the ciphertext, the salt and every `env`
    binding into the browser of everybody who can list Jobs, which is the mirror
    the whole design refuses to keep.
    """
    if not isinstance(meta, dict):
        return {}
    return {key: value for key, value in meta.items() if key not in _META_SEALED}


@app.get(
    "/workspace",
    response_model=WorkspaceSnapshotOut,
    dependencies=[Depends(requires("workspace:Read"))],
)
def get_workspace() -> WorkspaceSnapshotOut:
    """The whole library in one read — how Studio loads on start."""
    try:
        snapshot = _workspace.snapshot()
    except workspace.WorkspaceError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return WorkspaceSnapshotOut(
        root=snapshot.root,
        workflows=[_workspace_doc_out(doc) for doc in snapshot.workflows],
        jobs=[_workspace_doc_out(doc) for doc in snapshot.jobs],
        pipelines=[_workspace_doc_out(doc) for doc in snapshot.pipelines],
        queries=[_workspace_doc_out(doc) for doc in snapshot.queries],
        meta=_public_meta(snapshot.meta),
    )


class WorkspaceRootOut(BaseModel):
    """Where the library is, and why it is there."""

    root: str
    #: `env`, `settings`, `legacy` or `default` — see `workspace.resolve_root` —
    #: or `provider`, when a deployment injected a store of its own and none of
    #: those apply because there is no local directory to name.
    source: str
    #: Where a library goes when nobody has said otherwise, so the interface can
    #: offer it back as the way to undo a choice.
    default: str
    #: The file a choice is remembered in.
    settings_file: str
    writable: bool
    #: True while the library is still sitting inside the runner's own source
    #: tree. It works, and it is not where it belongs.
    inside_source_tree: bool
    #: True when the deployment decided — `SPARQUET_STUDIO_WORKSPACE`, or an
    #: injected store — and nothing here may override it: a deployment that
    #: decides centrally decides centrally.
    locked: bool


class WorkspaceRootRequest(BaseModel):
    """`root: null` goes back to the default rather than choosing it explicitly,
    so a library that later moves with the platform follows it."""

    root: Optional[str] = None


def _workspace_root_out() -> WorkspaceRootOut:
    injected = providers.configured("workspace")
    if injected:
        # A store that is not a directory on this disk. There is no local path to
        # report and nothing here can move it, so the answer says where the
        # records actually are and that the choice was not made here.
        described = _workspace.describe()
        return WorkspaceRootOut(
            root=str(described.get("root") or described.get("kind") or injected),
            source="provider",
            default=str(workspace.default_root()),
            settings_file=str(workspace.settings_path()),
            writable=described.get("writable", True) is not False,
            inside_source_tree=False,
            locked=True,
        )
    root = Path(_WORKSPACE_ROOT)
    return WorkspaceRootOut(
        root=str(root),
        source=_WORKSPACE_LOCATION.source,
        default=str(workspace.default_root()),
        settings_file=str(workspace.settings_path()),
        writable=os.access(root, os.W_OK),
        inside_source_tree=_is_inside(root, _framework_root()),
        locked=bool(os.getenv("SPARQUET_STUDIO_WORKSPACE")),
    )


def _is_inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
    except (ValueError, OSError):
        return False
    return True


@app.get(
    "/workspace/root",
    response_model=WorkspaceRootOut,
    dependencies=[Depends(requires("workspace:Read"))],
)
def get_workspace_root() -> WorkspaceRootOut:
    """Where this runner keeps the JSON files.

    Somebody who cannot find their Jobs is nearly always looking at a different
    directory than the runner is, so the answer names the path *and* the reason
    it is that one.
    """
    return _workspace_root_out()


@app.put(
    "/workspace/root",
    response_model=WorkspaceRootOut,
    dependencies=[Depends(requires("runner:Configure"))],
)
def put_workspace_root(body: WorkspaceRootRequest) -> WorkspaceRootOut:
    """Moves the library to another directory, from now on and after a restart.

    Nothing is copied. The runner starts reading and writing the new place, which
    is what makes this the way to *adopt* a directory that already holds a
    library — a shared checkout, a synced folder, a mounted volume — rather than
    a way to relocate one. Moving the files is the operator's job, and doing it
    for them would mean a copy that half-fails somewhere with no way back.
    """
    global _WORKSPACE_LOCATION, _WORKSPACE_ROOT, _workspace

    injected = providers.configured("workspace")
    if injected:
        raise HTTPException(
            status_code=409,
            detail=(
                f"This runner stores the library through {injected}, not in a "
                "directory of its own. Where the records live is a decision of the "
                "deployment, not of this screen."
            ),
        )

    if os.getenv("SPARQUET_STUDIO_WORKSPACE"):
        raise HTTPException(
            status_code=409,
            detail=(
                "SPARQUET_STUDIO_WORKSPACE decides where the library lives on this "
                "runner. Change the variable and restart."
            ),
        )

    chosen = (body.root or "").strip()
    if chosen:
        target = Path(chosen).expanduser()
        if not target.is_absolute():
            raise HTTPException(
                status_code=400,
                detail="Give an absolute path: a relative one would depend on where the runner was started.",
            )
        # The one place it must not go. A checkout is code — pulled, reset and
        # deleted — and a library inside one is lost to the first `git clean`.
        if _is_inside(target, _framework_root()):
            raise HTTPException(
                status_code=400,
                detail=(
                    "That is inside the runner's own source tree. Choose a directory "
                    "of your own: a checkout gets reset and deleted, and it would take "
                    "the library with it."
                ),
            )
        try:
            target.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise HTTPException(
                status_code=400, detail=f"That directory cannot be created: {exc}"
            ) from exc
        if not os.access(target, os.W_OK):
            raise HTTPException(status_code=400, detail="That directory is not writable.")
        root, source = workspace.remember_root(target), "settings"
    else:
        workspace.write_setting("workspace", None)
        location = workspace.resolve_root(_LEGACY_WORKSPACE)
        root, source = location.root, location.source

    try:
        store = workspace.FileWorkspaceStore(root)
    except OSError as exc:
        raise HTTPException(status_code=400, detail=f"That directory cannot be used: {exc}") from exc

    _workspace = store
    _WORKSPACE_ROOT = store.root
    _WORKSPACE_LOCATION = workspace.Location(store.root, source)
    _log.info("The library is now read from %s (%s).", store.root, source)
    return _workspace_root_out()


class LibraryFileOut(BaseModel):
    """One runnable JSON in the library. `path` is always relative to its root."""

    path: str
    name: str
    size: int
    modified: float
    #: The Studio record whose artefact this file is, when there is one. A file
    #: with no owner is one the Studio did not write: it has no canvas to open and
    #: it is the only kind this API will delete.
    owner_kind: Optional[str] = None
    owner_id: Optional[str] = None


class LibraryFilesOut(BaseModel):
    root: str
    files: List[LibraryFileOut]


class LibraryFileContentOut(BaseModel):
    path: str
    #: The JSON as it is on disk, uncompiled and unmodified.
    pipeline: Dict[str, Any]


@app.get(
    "/workspace/files",
    response_model=LibraryFilesOut,
    dependencies=[Depends(requires("workspace:Read"))],
)
def list_library_files() -> LibraryFilesOut:
    """Every runnable JSON in the library, so a Pipeline stage can point at one.

    The whole tree, not only what the Studio wrote — a file another team owns, a
    script generated or somebody hand-wrote is exactly the case for this. The
    editor's own state under `.studio/` is not listed: it is not something to run.
    """
    return LibraryFilesOut(
        root=str(_WORKSPACE_ROOT),
        files=[LibraryFileOut(**item.to_json()) for item in _workspace.list_files()],
    )


@app.get(
    "/workspace/files/{path:path}",
    response_model=LibraryFileContentOut,
    dependencies=[Depends(requires("workspace:Read"))],
)
def read_library_file(path: str) -> LibraryFileContentOut:
    """The JSON at a relative path, as it is on disk.

    Studio reads it to show and lint what a file-backed stage would run. It is
    **not** cached: the file is the source of truth, and what is shown has to be
    what the next run will execute.
    """
    try:
        return LibraryFileContentOut(path=path, pipeline=_workspace.read_file(path))
    except workspace.WorkspaceError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


class LibraryFileDeletedOut(BaseModel):
    path: str
    #: False when the file was already gone — the same end state, not an error.
    deleted: bool


@app.delete(
    "/workspace/files/{path:path}",
    response_model=LibraryFileDeletedOut,
    dependencies=[Depends(requires("workspace:Delete"))],
)
def delete_library_file(path: str, request: Request) -> LibraryFileDeletedOut:
    """Removes a runnable JSON from the library directory. There is no undo.

    Only a file no Studio record owns — a conf left behind by a script, a copy
    somebody pasted in, a job the team stopped running. A Job's own file is half
    of a record and is refused here with the record to delete instead, because
    removing the file alone would leave the sidecar pointing at nothing and the
    next save would write it straight back.

    Declared before `/workspace/{kind}/{record_id}` on purpose: a file at the root
    of the library has two path segments and would otherwise be read as a record.
    """
    try:
        deleted = _workspace.delete_file(path)
    except workspace.WorkspaceError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except OSError as error:  # pragma: no cover - a locked or read-only library
        raise HTTPException(
            status_code=500, detail=f"Could not delete {path}: {error}"
        ) from error
    audit_detail(request, path=path, deleted=deleted)
    return LibraryFileDeletedOut(path=path, deleted=deleted)


def _annotation_keys(value: Any) -> set:
    """The dataset addresses a catalog map names."""
    if not isinstance(value, dict):
        return set()
    return {str(key).strip() for key in value if str(key).strip()}


def _catalog_keys(key: str) -> Optional[set]:
    """The dataset addresses the stored catalog names, or `None` if there is no
    stored catalog at all — which is what tells a first sync from a real edit."""
    if key != "catalog":
        return None
    try:
        meta = _workspace.read_meta()
    except Exception:
        return None
    stored = meta.get("catalog")
    return _annotation_keys(stored) if isinstance(stored, dict) else None


class WorkspaceMetaRequest(BaseModel):
    value: Any = None


@app.put(
    "/workspace/meta/{key}",
    response_model=Dict[str, Any],
    dependencies=[Depends(requires("workspace:Write", "meta/*"))],
)
def put_workspace_meta(
    key: str, body: WorkspaceMetaRequest, principal: Any = Depends(current_principal)
) -> Dict[str, Any]:
    """Library-level bookkeeping (storage version, seeded flag). Kept with the files
    rather than in the browser, so the answer to "was this library already
    migrated?" travels with the library."""
    _guard_meta_key(principal, key, body.value)
    before = _catalog_keys(key)
    _workspace.write_meta(key, body.value)
    if before is not None:
        # A dataset becomes a securable the moment somebody annotates it, so
        # "created" here means "appeared in this map and was not in the one
        # before". The first catalog a library ever writes is exempt: that save
        # is a browser syncing what it already had, and treating a whole
        # migrated catalog as a hundred fresh creations would hand one person
        # every table in it.
        after = _annotation_keys(body.value)
        _claim_new_resources(
            principal, [("dataset", name) for name in after - before]
        )
    return {"key": key, "value": body.value}


@app.delete(
    "/workspace/meta/{key}",
    response_model=Dict[str, Any],
    dependencies=[Depends(requires("workspace:Write", "meta/*"))],
)
def delete_workspace_meta(
    key: str, principal: Any = Depends(current_principal)
) -> Dict[str, Any]:
    _guard_meta_key(principal, key)
    _workspace.delete_meta(key)
    return {"key": key, "deleted": True}


@app.get(
    "/workspace/{kind}/{record_id}",
    response_model=WorkspaceDocumentOut,
    dependencies=[Depends(requires("workspace:Read", _workspace_resource))],
)
def get_workspace_document(
    kind: str, record_id: str, principal: Any = Depends(current_principal)
) -> WorkspaceDocumentOut:
    _authorize_document(principal, kind, record_id, "read")
    try:
        doc = _workspace.read(kind, record_id)
    except workspace.WorkspaceError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if doc is None:
        raise HTTPException(status_code=404, detail="Record not found in the workspace.")
    return _workspace_doc_out(doc)


@app.put(
    "/workspace/{kind}/{record_id}",
    response_model=WorkspaceDocumentOut,
    dependencies=[Depends(requires("workspace:Write", _workspace_resource))],
)
def put_workspace_document(
    kind: str,
    record_id: str,
    body: WorkspaceWriteRequest,
    principal: Any = Depends(current_principal),
) -> WorkspaceDocumentOut:
    """Saves one record. Writes the file first, then indexes it."""
    _authorize_document(principal, kind, record_id, "write")
    # Asked before the write, because afterwards every save looks like the first
    # one. Only a record that did not exist gets the default access: claiming an
    # old shared Job for whoever edited it next would be a transfer of ownership
    # dressed up as a save.
    try:
        current = _workspace.read(kind, record_id)
    except Exception:
        current = None
        existed = True
    else:
        existed = current is not None
    # A schedule saved here is a run somebody else's clock will start, so it is
    # authorized as one. Before the write, so a refused schedule leaves the file
    # exactly as it was.
    _authorize_schedule_change(
        principal, kind, record_id,
        current.record if current is not None else None,
        body.record,
    )
    try:
        doc = _workspace.write(
            workspace.Document(
                kind=kind, id=record_id, record=body.record, config=body.config
            ),
            expected_revision=body.revision,
        )
    except workspace.WorkspaceConflict as exc:
        # 409, not 400: nothing about the request is wrong. The library moved under
        # it — a second Studio on a shared or synced directory, a `git pull`, the
        # same file edited by hand. The current revision and record travel with the
        # refusal so the editor can show what arrived and let the person choose.
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(exc),
                "kind": kind,
                "id": record_id,
                "revision": exc.revision,
                "record": exc.record,
            },
        ) from exc
    except workspace.WorkspaceError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except OSError as exc:
        raise HTTPException(
            status_code=500, detail=f"Could not write to the workspace: {exc}"
        ) from exc
    if not existed:
        _claim_new_resource(principal, kind, record_id)
    _mirror_catalog(doc)
    return _workspace_doc_out(doc)


@app.delete(
    "/workspace/{kind}/{record_id}",
    response_model=WorkspaceDeleteResponse,
    dependencies=[Depends(requires("workspace:Delete", _workspace_resource))],
)
def delete_workspace_document(
    kind: str, record_id: str, principal: Any = Depends(current_principal)
) -> WorkspaceDeleteResponse:
    """Removes the files. The catalog row stays, marked deleted, because past runs
    point at it and a run that names a record nobody can look up is worse than a
    record marked gone."""
    _authorize_document(principal, kind, record_id, "admin")
    try:
        removed = _workspace.delete(kind, record_id)
    except workspace.WorkspaceError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        _history.soft_delete(kind, record_id)
    except Exception:  # pragma: no cover - see _mirror_catalog
        _log.warning("Could not mark %s %s deleted in the catalog.", kind, record_id)
    return WorkspaceDeleteResponse(deleted=removed)


class RunLogOut(BaseModel):
    seq: int
    timestamp: str
    level: str
    source: str
    message: str
    context: Dict[str, Any] = Field(default_factory=dict)


class RunLogsResponse(BaseModel):
    job_run_id: str
    lines: List[RunLogOut]
    # Total lines stored for this job execution, so the client can say how many it
    # is not showing.
    total: int
    # The `seq` to pass back as `after` to continue reading; None at the end.
    next_after: Optional[int] = None


@app.get(
    "/job-runs/{job_run_id}/logs",
    response_model=RunLogsResponse,
    dependencies=[Depends(requires("history:Read"))],
)
def get_job_run_logs(
    job_run_id: str, after: int = 0, limit: int = 500
) -> RunLogsResponse:
    """What one job execution printed, in the order it printed it.

    Paged by `seq` rather than by offset: lines are only ever appended, so `after`
    never re-reads or skips a line the way an offset does when a run is still going.
    """
    page = min(max(limit, 1), 2000)
    lines = _history.list_logs(job_run_id, after_seq=after, limit=page)
    out = [
        RunLogOut(
            seq=line.seq, timestamp=line.timestamp, level=line.level,
            source=line.source, message=line.message,
            context=_decode_context(line.context),
        )
        for line in lines
    ]
    return RunLogsResponse(
        job_run_id=job_run_id,
        lines=out,
        total=_history.count_logs(job_run_id),
        next_after=out[-1].seq if len(out) == page else None,
    )


class JobRunConfigResponse(BaseModel):
    job_run_id: str
    # `sha256:<hex>`, or None for a run recorded before this was kept.
    config_hash: Optional[str] = None
    # The JSON that ran. None when the run predates the column, or when the
    # configuration was over the size the history stores — `config_hash` still
    # identifies it, so two runs can be compared even then.
    config: Optional[Dict[str, Any]] = None


@app.get(
    "/job-runs/{job_run_id}/config",
    response_model=JobRunConfigResponse,
    dependencies=[Depends(requires("history:Read"))],
)
def get_job_run_config(job_run_id: str) -> JobRunConfigResponse:
    """The version of the JSON one execution ran.

    The history points at a Job, and a Job keeps being edited: this is what makes
    a past run reproducible, and what a reader compares against the file in git
    when a run that used to work stops working.
    """
    stored = _history.job_config(job_run_id)
    if stored is None:
        raise HTTPException(status_code=404, detail="Unknown job run.")
    return JobRunConfigResponse(
        job_run_id=job_run_id, config_hash=stored.config_hash, config=stored.config,
    )


def _decode_context(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


class CancelResponse(BaseModel):
    cancelled: bool
    run_id: str
    # False when nothing was computing on Spark yet: the run still ends, but no
    # JVM job had to be killed for it.
    spark_jobs_cancelled: bool = False


class LoginRequest(BaseModel):
    username: str
    password: str


class PrincipalOut(BaseModel):
    username: str
    display_name: Optional[str] = None
    user_id: Optional[str] = None
    roles: List[str] = Field(default_factory=list)
    # The policy statements behind those roles, so Studio can grey out what this
    # person cannot do instead of letting them find out from a 403.
    statements: List[Dict[str, Any]] = Field(default_factory=list)
    # True on a runner with no users: the shared token is the identity.
    token_only: bool = False
    # The team, which is both who pays for this person's runs and a second source
    # of roles: `roles` above are the ones held personally, `team_roles` the ones
    # that come with the team, and the statements are the union of both.
    team_id: Optional[str] = None
    team_name: Optional[str] = None
    team_roles: List[str] = Field(default_factory=list)


class SessionOut(BaseModel):
    token: str
    expires_at: str
    user: PrincipalOut


class AuthStatusOut(BaseModel):
    # Whether this runner has users at all.
    login_required: bool
    # Who the request is from, or None when it carries no session.
    principal: Optional[PrincipalOut] = None


class UserOut(BaseModel):
    id: str
    username: str
    display_name: Optional[str] = None
    roles: List[str] = Field(default_factory=list)
    disabled: bool = False
    created_at: Optional[str] = None
    last_login_at: Optional[str] = None
    team_id: Optional[str] = None
    team_name: Optional[str] = None


class TeamOut(BaseModel):
    """A group of people that shares one credit account and, optionally, roles."""

    id: str
    name: str
    roles: List[str] = Field(default_factory=list)
    members: int = 0
    created_at: Optional[str] = None


class CreateTeamRequest(BaseModel):
    name: str
    roles: List[str] = Field(default_factory=list)


class UpdateTeamRequest(BaseModel):
    name: Optional[str] = None
    roles: Optional[List[str]] = None


class MoveUserRequest(BaseModel):
    #: Id or name. Empty moves the person back to the default team.
    team: Optional[str] = None


class CreateRoleRequest(BaseModel):
    name: str
    description: str = ""
    statements: List[Dict[str, Any]] = Field(default_factory=list)


class UpdateRoleRequest(BaseModel):
    description: Optional[str] = None
    statements: Optional[List[Dict[str, Any]]] = None


class ActionOut(BaseModel):
    """One thing a policy can allow, with what it guards, for the role editor."""

    name: str
    description: str
    #: `run`, `workspace`, `iam`, `credits`, `history` — the half before the colon,
    #: so the editor can group by service instead of showing a flat list.
    service: str


class PolicyVocabularyOut(BaseModel):
    actions: List[ActionOut] = Field(default_factory=list)
    resource_kinds: List[ActionOut] = Field(default_factory=list)


class CreateUserRequest(BaseModel):
    username: str
    password: str
    roles: List[str] = Field(default_factory=list)
    display_name: Optional[str] = None
    #: Id or name of the team to put them in. Omitted means the default team.
    team: Optional[str] = None


class UpdateUserRequest(BaseModel):
    roles: Optional[List[str]] = None
    disabled: Optional[bool] = None
    team: Optional[str] = None


class PasswordRequest(BaseModel):
    password: str
    # Required when changing your own password, so a borrowed session cannot
    # quietly become a permanent one.
    current_password: Optional[str] = None


class RecoveryIssuedOut(BaseModel):
    """A recovery code, shown once. The runner keeps only its hash, so this
    response is the only copy that will ever exist."""

    user_id: str
    username: str
    code: str
    expires_at: str


class RecoverRequest(BaseModel):
    code: str
    password: str


class IssueRecoveryRequest(BaseModel):
    """The administrator's **own** password, re-entered to mint a code.

    Not the password of the person being recovered — they are by definition the
    one who cannot supply it. This is a step-up: a session left open on an
    unlocked laptop should not be enough to take over another account, and minting
    a recovery code is exactly that if nobody has to prove who is holding the
    keyboard.
    """

    password: str


class AccountOut(BaseModel):
    """One team's standing. The account id is the team id; `username` is its name.
    """

    id: str
    username: str
    balance: int
    #: Every credit a remote write ever cost this account, whether or not a
    #: balance was actually taken. See the metering-versus-enforcement split in
    #: credits.py.
    spent: int
    #: `YYYY-MM`. The free allowance below is scoped to it and refills on its own
    #: when the month turns.
    period: str = ""
    free_used: int = 0
    free_monthly: int = 0
    free_remaining: int = 0
    #: What could be spent right now: the rest of this month's allowance plus the
    #: granted balance, minus whatever runs in flight are holding.
    available: int = 0
    #: Reserved by runs that have not finished. Promised, not spent — a hold that
    #: nobody settles comes back at the next restart.
    held: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class UsageOut(BaseModel):
    """One month in three numbers. `waived` is what the free allowance covered —
    "you used 40 of your 40 free" and "you owe 40" are not the same sentence."""

    period: str
    writes: int
    charged: int
    waived: int


class CreditsOut(BaseModel):
    account: AccountOut
    enforced: bool
    credits_per_write: int
    free_monthly: int
    usage: UsageOut


class LedgerEntryOut(BaseModel):
    id: str
    account_id: str
    amount: int
    reason: str
    applied: bool
    balance_after: int
    created_at: str
    #: Successful writes this entry paid for. Zero on a grant.
    writes: int = 0
    free_amount: int = 0
    shortfall: int = 0
    period: Optional[str] = None
    job_run_id: Optional[str] = None
    pipeline_run_id: Optional[str] = None
    target: Optional[str] = None
    job_name: Optional[str] = None
    note: Optional[str] = None
    #: Where the spending happened and who caused it. The account still pays; these
    #: are what let one invoice be read by workflow and by person.
    workflow_id: Optional[str] = None
    actor: Optional[str] = None
    #: The labels the run carried when it was charged, frozen on the entry.
    tags: List[str] = Field(default_factory=list)


class UsageGroupOut(BaseModel):
    """One row of a bill: what a team, a person, a workflow or a job spent.

    `key` is null for spending that has no such dimension - a run started from a
    script belongs to no workflow, a Job nobody labelled has no tag. It is reported
    rather than dropped, so nothing is silently missing from the bill.

    By team, user, workflow or job the rows partition the month and add up to
    `total`. By tag they do not: a run wearing two labels is counted in full under
    each, which is what makes the question "what does finance cost me" answerable.
    """

    key: Optional[str] = None
    label: Optional[str] = None
    writes: int = 0
    charged: int = 0
    waived: int = 0
    runs: int = 0
    last_at: Optional[str] = None


class UsageBreakdownOut(BaseModel):
    period: str
    group_by: str
    #: Whether this is the whole runner or a single account. A caller without
    #: `credits:Read` only ever sees their own team.
    scope: str
    total: UsageGroupOut
    groups: List[UsageGroupOut] = Field(default_factory=list)
    #: True when a run can appear in more than one row, which is the case for
    #: tags and for nothing else. The rows then add up to more than `total`, and
    #: an interface that draws them as shares of a whole has to say so.
    overlapping: bool = False


class UsagePeriodOut(BaseModel):
    period: str
    writes: int = 0
    charged: int = 0
    waived: int = 0
    runs: int = 0


class UsageTimelineOut(BaseModel):
    scope: str
    periods: List[UsagePeriodOut] = Field(default_factory=list)


class GrantRequest(BaseModel):
    amount: int
    note: Optional[str] = None


class RoleOut(BaseModel):
    name: str
    description: str
    statements: List[Dict[str, Any]] = Field(default_factory=list)
    custom: bool = False


class AuditEventOut(BaseModel):
    id: str
    at: str
    actor: str
    action: str
    method: str
    path: str
    outcome: str
    actor_id: Optional[str] = None
    team: Optional[str] = None
    roles: List[str] = Field(default_factory=list)
    resource: Optional[str] = None
    status: Optional[int] = None
    detail: Optional[Dict[str, Any]] = None
    ip: Optional[str] = None


def _principal_out(principal: Any) -> PrincipalOut:
    return PrincipalOut(
        username=principal.username, display_name=principal.display_name,
        user_id=principal.user_id, roles=list(principal.roles),
        statements=list(principal.statements), token_only=principal.token_only,
        team_id=principal.team_id, team_name=principal.team_name,
        team_roles=list(principal.team_roles),
    )


def _user_out(user: Any) -> UserOut:
    return UserOut(
        id=user.id, username=user.username, display_name=user.display_name,
        roles=list(user.roles), disabled=user.disabled, created_at=user.created_at,
        last_login_at=user.last_login_at, team_id=user.team_id,
        team_name=user.team_name,
    )


def _team_out(team: Any) -> TeamOut:
    return TeamOut(
        id=team.id, name=team.name, roles=list(team.roles), members=team.members,
        created_at=team.created_at,
    )


def _role_out(role: Any) -> RoleOut:
    return RoleOut(
        name=role.name, description=role.description, statements=role.statements,
        custom=role.custom,
    )


def _account_out(account: Any) -> AccountOut:
    return AccountOut(
        id=account.id, username=account.username, balance=account.balance,
        spent=account.spent, period=account.period, free_used=account.free_used,
        free_monthly=account.free_monthly, free_remaining=account.free_remaining,
        available=account.available, held=account.held, created_at=account.created_at,
        updated_at=account.updated_at,
    )


def _catalog_names() -> Dict[str, str]:
    """Workflow ids to their names, for a bill that reads in words.

    The ledger stores the id and nothing else: a workflow that is renamed should be
    renamed on every invoice it ever appeared on, which a copy taken at charge time
    would not do. A workflow the catalog has never heard of keeps its id.
    """
    try:
        return {
            record.id: record.name
            for record in _history.list_catalog(include_deleted=True)
            if record.kind == "workflow" and record.name
        }
    except Exception:  # pragma: no cover - a bill is not worth failing over
        return {}


def _entry_out(entry: Any) -> LedgerEntryOut:
    return LedgerEntryOut(
        id=entry.id, account_id=entry.account_id, amount=entry.amount,
        reason=entry.reason, applied=entry.applied, balance_after=entry.balance_after,
        created_at=entry.created_at, writes=entry.writes,
        free_amount=entry.free_amount, shortfall=entry.shortfall, period=entry.period,
        job_run_id=entry.job_run_id, pipeline_run_id=entry.pipeline_run_id,
        target=entry.target, job_name=entry.job_name, note=entry.note,
        workflow_id=entry.workflow_id, actor=entry.actor,
        tags=list(getattr(entry, "tags", []) or []),
    )


@app.get(
    "/auth/status",
    response_model=AuthStatusOut,
    dependencies=[Depends(require_token_unless_users)],
)
def auth_status(request: Request) -> AuthStatusOut:
    """Whether a login is needed here, and who the caller already is.

    The one authenticated endpoint that answers without a session: Studio calls it
    on start to decide between showing the editor and showing a login form, and it
    cannot have a session yet at that point.
    """
    login_required = _auth.has_users()
    token = _session_token(request)
    principal = _auth.resolve_session(token) if token else None
    if principal is None and not login_required:
        principal = auth.TOKEN_PRINCIPAL
    return AuthStatusOut(
        login_required=login_required,
        principal=_principal_out(principal) if principal else None,
    )


@app.post(
    "/auth/login",
    response_model=SessionOut,
    dependencies=[Depends(require_token_unless_users)],
)
def auth_login(request: Request, body: LoginRequest) -> SessionOut:
    """Exchanges a username and password for a session.

    One message for every kind of failure — unknown user, wrong password, disabled
    account — because saying which one is a free answer to somebody guessing. And
    a ceiling on how many guesses fit in a window, because this endpoint is
    reachable without the shared token.
    """
    keys = _throttle_keys(request, body.username)
    _refuse_if_throttled(keys)
    session = _auth.login(body.username, body.password)
    if session is None:
        _LOGIN_THROTTLE.record_failure(keys)
        raise HTTPException(status_code=401, detail="Wrong username or password.")
    _LOGIN_THROTTLE.forget(keys)
    # Resolved rather than built from the user record: a principal is roles AND
    # the statements behind them, plus the team the roles are widened by. Studio
    # uses this answer to decide what to offer, so a login that reported only the
    # role names left an administrator looking at an empty policy — every control
    # greyed out with "your role does not allow ..." until the next reload asked
    # `/auth/status`, which always answered in full.
    principal = _auth.resolve_session(session.token)
    return SessionOut(
        token=session.token,
        expires_at=session.expires_at,
        user=_principal_out(principal) if principal else PrincipalOut(
            username=session.user.username, display_name=session.user.display_name,
            user_id=session.user.id, roles=list(session.user.roles),
        ),
    )


@app.post("/auth/logout", dependencies=[Depends(require_token)])
def auth_logout(request: Request) -> Dict[str, bool]:
    """Ends this session. Silent when there is none — logging out twice is not an
    error, and neither is logging out of a session that already expired."""
    token = _session_token(request)
    if token:
        _auth.logout(token)
    return {"logged_out": True}


@app.get("/auth/me", response_model=PrincipalOut)
def auth_me(principal: Any = Depends(current_principal)) -> PrincipalOut:
    return _principal_out(principal)


@app.get(
    "/auth/roles",
    response_model=List[RoleOut],
    dependencies=[Depends(requires("iam:ReadUsers"))],
)
def list_roles() -> List[RoleOut]:
    return [_role_out(role) for role in _auth.list_roles()]


@app.get(
    "/auth/policy",
    response_model=PolicyVocabularyOut,
    dependencies=[Depends(requires("iam:ReadUsers"))],
)
def policy_vocabulary() -> PolicyVocabularyOut:
    """Everything a policy statement may name: the actions and the resource kinds.

    The role editor is built from this rather than from a list copied into the
    client, so an action added to the runner shows up in the UI without a second
    change — and a client can never offer an action the server would reject.
    """
    return PolicyVocabularyOut(
        actions=[
            ActionOut(name=name, description=description, service=name.split(":")[0])
            for name, description in sorted(auth.ACTIONS.items())
        ],
        resource_kinds=[
            ActionOut(name=name, description=description, service=name)
            for name, description in sorted(auth.RESOURCE_KINDS.items())
        ],
    )


class AccessDecisionOut(BaseModel):
    resource: str
    resource_id: str
    #: Whether any rule or owner names this securable or a container of it.
    governed: bool
    #: read / write / admin, or null for "governed and nothing left".
    level: Optional[str] = None
    #: True when the level comes from owning it, or owning what contains it.
    owned: bool = False
    #: The `kind/id` the winning rule sits on — itself, or the ancestor it was
    #: inherited from. What the screen needs to send somebody to the right row.
    source: Optional[str] = None
    #: Who owns it, as recorded directly on it (not inherited).
    owner_kind: Optional[str] = None
    owner_id: Optional[str] = None
    #: Whether this caller may change the rules on it.
    may_administer: bool = False
    #: Every `kind/id` a rule could be written on to reach it, nearest first.
    chain: List[str] = Field(default_factory=list)


def _access_decision_out(
    principal: Any,
    kind: str,
    resource_id: str,
    rules: List[Any],
    owners: List[Any],
    token_only: bool = False,
) -> AccessDecisionOut:
    """Layer two's whole answer on one securable, in the shape the screens read.

    Shared by `/iam/access`, which asks it about the caller, and `/iam/simulate`,
    which asks it about somebody else. The two have to agree, and the only way
    to be sure they do is for there to be one of them.
    """
    identity = _identity_of(principal)
    parents = _parents_of(kind, resource_id)
    decision = grants.evaluate(rules, owners, kind, resource_id, identity, parents)
    owner = grants.owner_of(owners, kind, resource_id)
    return AccessDecisionOut(
        resource=kind,
        resource_id=resource_id,
        governed=decision.governed,
        # The shared token is the identity on a runner with no users, and
        # `_authorize_resource` never refuses it. Reporting anything narrower
        # here would grey out controls that in fact work.
        level="admin" if token_only else decision.level,
        owned=decision.owned,
        source=decision.source,
        owner_kind=owner.principal_kind if owner else None,
        owner_id=owner.principal_id if owner else None,
        may_administer=token_only or grants.may_administer(
            rules, owners, kind, resource_id, identity, parents
        ) or (
            principal.allows("iam:ManageGrants", "*")
            and not principal.denies("iam:ManageGrants", "*")
        ),
        chain=[f"{k}/{i}" for k, i in grants.scope_chain(kind, resource_id, parents)],
    )


class AccessQueryRequest(BaseModel):
    #: `[{"resource": "dataset", "resourceId": "/lake/silver/orders"}, ...]`
    resources: List[Dict[str, str]] = Field(default_factory=list)


@app.post("/iam/access", response_model=List[AccessDecisionOut])
def effective_access(
    body: AccessQueryRequest, principal: Any = Depends(current_principal)
) -> List[AccessDecisionOut]:
    """What this caller actually holds on each securable, and why.

    Studio can evaluate the same rules in the browser and does, to grey out a
    control rather than let somebody find out from a 403. This endpoint exists
    for the other half: inheritance needs the Workflow a Job belongs to, and
    that is a workspace record. Asking here means the explanation the screen
    shows is the one the runner will act on.
    """
    rules = _grants_now()
    owners = _owners_now()
    token_only = bool(getattr(principal, "token_only", False))

    out: List[AccessDecisionOut] = []
    for item in body.resources[:500]:
        kind = str(item.get("resource") or "")
        ident = str(item.get("resourceId") or "").strip()
        if kind not in grants.RESOURCE_KINDS or not ident:
            continue
        if kind == "dataset":
            ident = _dataset_id(ident)
        out.append(
            _access_decision_out(principal, kind, ident, rules, owners, token_only)
        )
    return out


class SecretOut(BaseModel):
    """One secret, as a screen may know it.

    There is no field here for a value and there is no endpoint that adds one.
    `fields` are names, and `binding` — the environment variables an `env` secret
    reads — is operational information: it says where the runner looks, never
    what it found.
    """

    name: str
    provider: str
    description: str = ""
    tags: List[str] = Field(default_factory=list)
    fields: List[str] = Field(default_factory=list)
    binding: Dict[str, str] = Field(default_factory=dict)
    updated_at: float = 0.0
    updated_by: str = ""
    #: Layer two, on this secret, for the caller — so the screen can grey out a
    #: rotate button instead of offering one the runner will refuse.
    governed: bool = False
    level: Optional[str] = None
    owned: bool = False


class SecretWriteRequest(BaseModel):
    provider: str = "local"
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    #: A patch over the fields: a value sets it, `null` removes it, and a field
    #: left out keeps what it had. Rotating a password means sending that one
    #: field — the caller could not resend the others, never having read them.
    values: Dict[str, Optional[str]] = Field(default_factory=dict)


class SecretCheckOut(BaseModel):
    name: str
    #: Field name to `"ok"` or to what went wrong reading it.
    fields: Dict[str, str] = Field(default_factory=dict)
    healthy: bool = False


def _secret_out(secret: Any, principal: Any, rules: List[Any], owners: List[Any]) -> SecretOut:
    token_only = bool(getattr(principal, "token_only", False))
    decision = grants.evaluate(
        rules, owners, "secret", secret.name, _identity_of(principal),
        grants.tag_scopes(secret.tags, {}),
    )
    redacted = secret.redacted()
    return SecretOut(
        name=redacted["name"],
        provider=redacted["provider"],
        description=redacted["description"],
        tags=redacted["tags"],
        fields=redacted["fields"],
        binding=redacted.get("binding") or {},
        updated_at=redacted["updated_at"],
        updated_by=redacted["updated_by"],
        governed=decision.governed,
        level="admin" if token_only else decision.level,
        owned=decision.owned,
    )


@app.get(
    "/secrets",
    response_model=List[SecretOut],
    dependencies=[Depends(requires("secrets:Read"))],
)
def list_secrets(principal: Any = Depends(current_principal)) -> List[SecretOut]:
    """Which connection secrets exist, and what this caller may do with each.

    A secret the caller cannot reach is left out rather than shown greyed: the
    name of a credential is itself information, and a deny on `secret/pg-prod`
    should not leave the name of the production database on the screen.
    """
    store = _secrets_now()
    rules = _grants_now()
    owners = _owners_now()
    out: List[SecretOut] = []
    for secret in sorted(store.items.values(), key=lambda item: item.name):
        entry = _secret_out(secret, principal, rules, owners)
        if entry.governed and not entry.level and not entry.owned:
            continue
        out.append(entry)
    return out


@app.put(
    "/secrets/{name}",
    response_model=SecretOut,
    dependencies=[Depends(requires("secrets:Write"))],
)
def put_secret(
    name: str, body: SecretWriteRequest, principal: Any = Depends(current_principal)
) -> SecretOut:
    """Create a secret, or change one — including rotating a single field."""
    try:
        clean = vault.check_name(name)
    except vault.SecretError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    store = _secrets_now()
    existed = clean in store.items
    if existed:
        _authorize_resource(principal, "secret", clean, "write")

    try:
        updated = vault.put(
            store,
            clean,
            provider=body.provider,
            values=body.values,
            description=body.description,
            tags=body.tags,
            actor=str(getattr(principal, "username", "") or ""),
        )
    except vault.SecretError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    _write_secrets(updated)
    if not existed:
        _claim_new_resource(principal, "secret", clean)
    return _secret_out(updated.items[clean], principal, _grants_now(), _owners_now())


@app.delete(
    "/secrets/{name}",
    response_model=Dict[str, Any],
    dependencies=[Depends(requires("secrets:Write"))],
)
def delete_secret(
    name: str, principal: Any = Depends(current_principal)
) -> Dict[str, Any]:
    clean = vault.normalize(name)
    _authorize_resource(principal, "secret", clean, "write")
    try:
        _write_secrets(vault.remove(_secrets_now(), clean))
    except vault.SecretError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    return {"name": clean, "deleted": True}


@app.post(
    "/secrets/{name}/check",
    response_model=SecretCheckOut,
    dependencies=[Depends(requires("secrets:Read"))],
)
def check_secret(
    name: str, principal: Any = Depends(current_principal)
) -> SecretCheckOut:
    """Whether every field still resolves — the master key, the variable, the file.

    Not a connection test: reaching the database needs its driver and a network
    route, and a failure there says nothing about the secret. This answers the
    part the store is responsible for, and answers it without the value leaving
    the runner.
    """
    clean = vault.normalize(name)
    _authorize_resource(principal, "secret", clean, "read")
    try:
        fields = vault.check(_secrets_now(), clean)
    except vault.SecretError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    return SecretCheckOut(
        name=clean,
        fields=fields,
        healthy=all(state == "ok" for state in fields.values()),
    )


#: The grant level the runner demands beside each action, so a simulation asks
#: layer two the question the endpoints actually ask it. Running a Job needs
#: `write` on the Job, not `read`: a run writes wherever its JSON points.
_ACTION_LEVEL: Dict[str, str] = {
    "run:Execute": "write",
    "run:Cancel": "write",
    "workspace:Write": "write",
    "workspace:Delete": "write",
    "iam:ManageGrants": "admin",
    "secrets:Write": "write",
}


def _policy_targets(resource: str, resource_id: str) -> List[str]:
    """The `kind/id` strings layer one is asked about for one securable.

    A Job is reached by a role written on the Job or on the Workflow that holds
    it — the same pair `_run_targets` builds for a real run. A dataset is reached
    by neither: policy actions are the platform's verbs, and which table they
    touch is layer two's question, so the action is checked against `*` alone.
    """
    if not resource or not resource_id or resource == "dataset":
        return ["*"]
    targets = [f"{resource}/{resource_id}"]
    targets += [
        f"{kind}/{ident}"
        for kind, ident in _parents_of(resource, resource_id)
        if kind == "workflow"
    ]
    return targets


class PolicyVerdictOut(BaseModel):
    action: str
    #: True when a statement allows the action on one of `targets` and none denies it.
    allowed: bool
    #: The target an explicit deny matched. It settles the verdict whatever else allows.
    denied_on: Optional[str] = None
    #: The target that carried the allow, so a screen can name the rule that answered.
    allowed_on: Optional[str] = None
    #: Everything the action was checked against, nearest first.
    targets: List[str] = Field(default_factory=list)


class SimulationRequest(BaseModel):
    #: Whose access to answer for — somebody else. The caller's own is `/iam/access`.
    username: str
    #: A platform action, for layer one. Optional: asking only about a securable
    #: is a fair question on its own ("what does Ana hold on this table?").
    action: Optional[str] = None
    #: A securable, for layer two. Also optional, for the mirror-image question.
    resource: str = ""
    resource_id: str = ""
    #: read / write / admin. Defaults to whatever the runner demands for `action`.
    level: Optional[str] = None


class SimulationOut(BaseModel):
    username: str
    #: False when no such user. The answer is then "nothing", which is more useful
    #: to show than a 404 in a screen whose whole job is answering questions.
    found: bool = False
    #: A disabled account is refused at login, whatever its roles say.
    disabled: bool = False
    display_name: Optional[str] = None
    team_id: Optional[str] = None
    team_name: Optional[str] = None
    roles: List[str] = Field(default_factory=list)
    #: The roles that come from the team rather than from the account itself,
    #: because "why can she do that?" is usually answered by this list.
    team_roles: List[str] = Field(default_factory=list)
    policy: Optional[PolicyVerdictOut] = None
    access: Optional[AccessDecisionOut] = None
    #: The level layer two was asked for, once `action` had been taken into account.
    level_asked: Optional[str] = None
    #: Both layers together: what would happen if this person tried it right now.
    allowed: bool = False
    #: Why, in the words the refusal itself would use.
    reason: str = ""


@app.post(
    "/iam/simulate",
    response_model=SimulationOut,
    dependencies=[Depends(requires("iam:ReadUsers"))],
)
def simulate_access(body: SimulationRequest) -> SimulationOut:
    """What somebody else would be allowed to do, and which layer decides it.

    Almost every refusal involves both layers, and they refuse for unrelated
    reasons: a missing action is a role to fix, a missing level is an owner to
    ask. Showing them side by side is the difference between an administrator
    knowing what to change and guessing.

    Answered by the same code the request itself would take — `principal_for`
    assembles the principal exactly as a login does, and layer two runs through
    `_access_decision_out` — so the simulation cannot drift from the runner it
    is describing.
    """
    username = (body.username or "").strip()
    principal = _auth.principal_for(username) if username else None
    if principal is None:
        return SimulationOut(
            username=username,
            reason=f"No user named '{username}' on this runner.",
        )

    user = _auth.find_user(username)
    disabled = bool(getattr(user, "disabled", False))
    out = SimulationOut(
        username=principal.username,
        found=True,
        disabled=disabled,
        display_name=principal.display_name,
        team_id=principal.team_id,
        team_name=principal.team_name,
        roles=list(principal.roles),
        team_roles=list(principal.team_roles),
    )

    kind = (body.resource or "").strip()
    resource_id = (body.resource_id or "").strip()
    if kind and kind not in grants.RESOURCE_KINDS:
        raise HTTPException(status_code=400, detail=f"Unknown resource kind '{kind}'.")
    if kind == "dataset":
        resource_id = _dataset_id(resource_id)

    action = (body.action or "").strip()
    if action:
        if action not in auth.ACTIONS:
            raise HTTPException(status_code=400, detail=f"Unknown action '{action}'.")
        targets = _policy_targets(kind, resource_id)
        # Same rule as `_authorize_run`: one allow among the targets is enough,
        # but a deny on any of them is final, or a deny could be widened away by
        # a broader grant written somewhere else.
        denied_on = next((t for t in targets if principal.denies(action, t)), None)
        allowed_on = next((t for t in targets if principal.allows(action, t)), None)
        out.policy = PolicyVerdictOut(
            action=action,
            allowed=denied_on is None and allowed_on is not None,
            denied_on=denied_on,
            allowed_on=None if denied_on else allowed_on,
            targets=targets,
        )

    if kind and resource_id:
        level = (body.level or _ACTION_LEVEL.get(action, "read")).strip()
        if level not in grants.LEVELS:
            raise HTTPException(status_code=400, detail=f"Unknown level '{level}'.")
        out.level_asked = level
        out.access = _access_decision_out(
            principal, kind, resource_id, _grants_now(), _owners_now()
        )

    policy_ok = out.policy is None or out.policy.allowed
    access_ok = True
    if out.access is not None and out.access.governed:
        held = out.access.level
        access_ok = held is not None and (
            grants.LEVEL_RANK[held] >= grants.LEVEL_RANK[out.level_asked or "read"]
        )
    out.allowed = policy_ok and access_ok and not disabled

    if disabled:
        out.reason = f"'{out.username}' is disabled and cannot sign in at all."
    elif not policy_ok and out.policy is not None:
        held_roles = ", ".join(principal.roles) or "none"
        where = out.policy.denied_on
        out.reason = (
            f"Denied by a role on '{where}': no role may {action} there. "
            f"Roles held: {held_roles}."
            if where
            else f"No role held allows {action}. Roles held: {held_roles}."
        )
    elif not access_ok and out.access is not None:
        out.reason = grants.refusal(
            kind, resource_id, out.level_asked or "read", out.username
        )
    elif out.access is not None and not out.access.governed:
        out.reason = (
            f"Allowed. Nothing governs '{kind}/{resource_id}', "
            "so layer one decides on its own."
        )
    elif out.access is not None:
        via = f" via {out.access.source}" if out.access.source else ""
        holds = "owns it" if out.access.owned else f"holds {out.access.level}"
        out.reason = f"Allowed. {out.username} {holds} on '{kind}/{resource_id}'{via}."
    else:
        out.reason = (
            f"Allowed. A role held permits {action}." if action else "Nothing asked."
        )

    return out


@app.post(
    "/auth/roles",
    response_model=RoleOut,
    dependencies=[Depends(requires("iam:ManageRoles"))],
)
def create_role(body: CreateRoleRequest) -> RoleOut:
    """A role written here, rather than shipped with the runner.

    The built-in names are refused: the shipped roles are rewritten on every start
    so that fixing a policy in code fixes it on every installation, and an edit
    made here would be silently lost on the next restart.
    """
    try:
        role = _auth.create_role(body.name, body.description, body.statements)
    except auth.AuthError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return _role_out(role)


@app.patch(
    "/auth/roles/{name}",
    response_model=RoleOut,
    dependencies=[Depends(requires("iam:ManageRoles"))],
)
def update_role(name: str, body: UpdateRoleRequest) -> RoleOut:
    try:
        role = _auth.update_role(
            name, description=body.description, statements=body.statements
        )
    except auth.AuthError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return _role_out(role)


@app.delete(
    "/auth/roles/{name}",
    dependencies=[Depends(requires("iam:ManageRoles"))],
)
def delete_role(name: str) -> Dict[str, bool]:
    """Removes a custom role. Refused while anyone still holds it: deleting a role
    out from under a user would quietly change what they can do, and the operator
    should decide what those people get instead."""
    try:
        _auth.delete_role(name)
    except auth.AuthError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return {"deleted": True}


@app.get(
    "/auth/teams",
    response_model=List[TeamOut],
    dependencies=[Depends(requires("iam:ReadUsers"))],
)
def list_teams() -> List[TeamOut]:
    return [_team_out(team) for team in _auth.list_teams()]


@app.post(
    "/auth/teams",
    response_model=TeamOut,
    dependencies=[Depends(requires("iam:ManageTeams"))],
)
def create_team(body: CreateTeamRequest) -> TeamOut:
    """A team is a billing account and a way of granting roles to a group at once.

    Roles given here are added to whatever each member holds personally; a team
    never takes anything away, because a grant that can also revoke makes "why can
    this person not do X" an unanswerable question.
    """
    try:
        team = _auth.create_team(body.name, roles=body.roles)
    except auth.AuthError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return _team_out(team)


@app.patch(
    "/auth/teams/{team_id}",
    response_model=TeamOut,
    dependencies=[Depends(requires("iam:ManageTeams"))],
)
def update_team(team_id: str, body: UpdateTeamRequest) -> TeamOut:
    try:
        team = _auth.update_team(team_id, name=body.name, roles=body.roles)
    except auth.AuthError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return _team_out(team)


@app.delete(
    "/auth/teams/{team_id}",
    dependencies=[Depends(requires("iam:ManageTeams"))],
)
def delete_team(team_id: str) -> Dict[str, bool]:
    """Removes a team; its members move to the default one rather than being left
    without an account to charge. The default team itself cannot go."""
    try:
        _auth.delete_team(team_id)
    except auth.AuthError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return {"deleted": True}


@app.get(
    "/auth/users",
    response_model=List[UserOut],
    dependencies=[Depends(requires("iam:ReadUsers"))],
)
def list_users() -> List[UserOut]:
    return [_user_out(user) for user in _auth.list_users()]


@app.post(
    "/auth/users",
    response_model=UserOut,
    dependencies=[Depends(requires("iam:ManageUsers"))],
)
def create_user(request: Request, body: CreateUserRequest) -> UserOut:
    """Creates a user.

    The first one is the moment this runner stops being token-only: until it
    exists the shared token authorizes everything, including this call, which is
    how an operator bootstraps themselves an account without a second channel.
    Give that first user the `admin` role — nothing else can create the next one.
    """
    try:
        user = _auth.create_user(
            body.username, body.password, roles=body.roles,
            display_name=body.display_name, team=body.team,
        )
    except auth.AuthError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    audit_detail(
        request, created=user.username, roles=list(user.roles), team=user.team_name
    )
    request.state.audit_resource = f"user/{user.id}"
    return _user_out(user)


@app.patch(
    "/auth/users/{user_id}",
    response_model=UserOut,
    dependencies=[Depends(requires("iam:ManageUsers"))],
)
def update_user(request: Request, user_id: str, body: UpdateUserRequest) -> UserOut:
    before = _auth.get_user(user_id)
    try:
        if body.roles is not None:
            _auth.set_roles(user_id, body.roles)
        if body.disabled is not None:
            _auth.set_disabled(user_id, body.disabled)
        if body.team is not None:
            _auth.set_user_team(user_id, body.team)
    except auth.AuthError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    user = _auth.get_user(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="No such user.")
    request.state.audit_resource = f"user/{user_id}"
    changed: Dict[str, Any] = {"user": user.username}
    if body.roles is not None:
        changed["roles"] = {
            "from": list(getattr(before, "roles", []) or []), "to": list(user.roles)
        }
    if body.disabled is not None:
        changed["disabled"] = bool(body.disabled)
    if body.team is not None:
        changed["team"] = {
            "from": getattr(before, "team_name", None), "to": user.team_name
        }
    audit_detail(request, **changed)
    return _user_out(user)


@app.post("/auth/users/{user_id}/password")
def set_password(
    user_id: str, body: PasswordRequest, principal: Any = Depends(current_principal)
) -> Dict[str, bool]:
    """Sets a password: your own, or anyone's with `iam:ManageUsers`.

    Changing your own requires the current one. An administrator resetting someone
    else's does not have it — that is the point of a reset — which is why the two
    paths are told apart here rather than merged.
    """
    own = principal.user_id is not None and principal.user_id == user_id
    if not own and not principal.allows("iam:ManageUsers"):
        raise HTTPException(
            status_code=403, detail="Only an administrator can change another user's password.",
        )
    if own:
        if not body.current_password or not _auth.verify_credentials(
            principal.username, body.current_password
        ):
            raise HTTPException(status_code=403, detail="The current password is wrong.")
    try:
        _auth.set_password(user_id, body.password)
    except auth.AuthError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return {"changed": True}


@app.post(
    "/auth/users/{user_id}/recovery",
    response_model=RecoveryIssuedOut,
    dependencies=[Depends(requires("iam:ManageUsers"))],
)
def issue_recovery(
    user_id: str, body: IssueRecoveryRequest,
    principal: Any = Depends(current_principal),
) -> RecoveryIssuedOut:
    """Mints a single-use code the person can trade for a password of their own.

    An administrator could simply set the password instead; this exists so they
    do not have to know it. The code is handed over out of band — chat, phone,
    in person — and it is short-lived because that trip is all it has to survive.

    The administrator re-enters **their own** password to do it. Not the password
    of the person being recovered: that person is by definition the one who cannot
    supply it. This is a step-up, and it is here because minting a recovery code
    is a way to take over an account — an unattended session with an open Studio
    should not be enough. A runner with no users has no password to ask for; there
    the shared token is the identity, and whoever holds it owns the host anyway.
    """
    if principal.user_id is not None and not _auth.verify_credentials(
        principal.username, body.password
    ):
        raise HTTPException(status_code=403, detail="Your password is wrong.")
    user = _auth.get_user(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="No such user.")
    try:
        code, expires_at = _auth.issue_recovery(user_id, issued_by=principal.username)
    except auth.AuthError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return RecoveryIssuedOut(
        user_id=user.id, username=user.username, code=code, expires_at=expires_at
    )


@app.post("/auth/recover", dependencies=[Depends(require_token_unless_users)])
def recover_password(request: Request, body: RecoverRequest) -> Dict[str, bool]:
    """Trades a recovery code for a new password. No session required — the
    caller is by definition locked out — and on a runner with users no shared
    token either, since somebody locked out of Settings cannot read one.

    Every failure reads the same, deliberately: unknown code, expired code, code
    already used, account disabled. A specific answer would turn this into an
    oracle for somebody guessing. The rate limit is what keeps the guessing slow;
    it counts against the caller only, since the code names no account until it
    is redeemed.
    """
    keys = _throttle_keys(request)
    _refuse_if_throttled(keys)
    try:
        _auth.redeem_recovery(body.code, body.password)
    except auth.AuthError as error:
        _LOGIN_THROTTLE.record_failure(keys)
        raise HTTPException(status_code=400, detail=str(error)) from error
    _LOGIN_THROTTLE.forget(keys)
    return {"changed": True}


@app.delete(
    "/auth/users/{user_id}",
    dependencies=[Depends(requires("iam:ManageUsers"))],
)
def delete_user(user_id: str) -> Dict[str, bool]:
    """Removes access. Hard delete, unlike the catalog: a user is not a record
    past runs point at — the runs keep the name they were run under, as text."""
    try:
        _auth.delete_user(user_id)
    except auth.AuthError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return {"deleted": True}


# --------------------------------------------------------------------- credits


@app.get("/audit", response_model=List[AuditEventOut])
def list_audit(
    limit: int = 100,
    actor_id: Optional[str] = None,
    resource: Optional[str] = None,
    outcome: Optional[str] = None,
    action: Optional[str] = None,
    since: Optional[str] = None,
    _: Any = Depends(requires("iam:ReadAudit")),
) -> List[AuditEventOut]:
    """The audit log, newest first.

    `action` accepts a service wildcard (`iam:*`), which is how the interface asks
    for "everything that touched access" without knowing every verb.
    """
    events = _audit.list(
        limit=limit, actor_id=actor_id, resource=resource,
        outcome=outcome, action=action, since=since,
    )
    return [
        AuditEventOut(
            id=event.id, at=event.at, actor=event.actor, actor_id=event.actor_id,
            team=event.team, roles=event.roles, action=event.action,
            method=event.method, path=event.path, resource=event.resource,
            outcome=event.outcome, status=event.status, detail=event.detail,
            ip=event.ip,
        )
        for event in events
    ]


@app.get("/credits/me", response_model=CreditsOut)
def my_credits(principal: Any = Depends(current_principal)) -> CreditsOut:
    """Your own balance. No permission needed: knowing what you may spend is part
    of being able to spend it, and refusing to say would only produce runs that
    fail at admission for a reason nobody could look up."""
    account_id, username = credits.account_for(principal)
    return CreditsOut(
        account=_account_out(_credits.account(account_id, username)),
        enforced=credits.enforced(),
        credits_per_write=credits.credits_per_write(),
        free_monthly=credits.free_monthly(),
        usage=UsageOut(**_credits.usage(account_id)),
    )


@app.get("/credits/usage", response_model=UsageBreakdownOut)
def credit_usage(
    group_by: str = "workflow",
    period: Optional[str] = None,
    account_id: Optional[str] = None,
    principal: Any = Depends(current_principal),
) -> UsageBreakdownOut:
    """A month of spending, grouped by team, user, workflow or job.

    Scope follows the same rule as the ledger: your own team is always readable,
    the whole runner needs `credits:Read`. A caller without it asking for another
    account is not told a different total - it is refused, because a bill that
    quietly answers about somebody else is worse than one that answers nothing.
    """
    own, _ = credits.account_for(principal)
    everyone = principal is not None and principal.allows("credits:Read")
    if account_id and account_id != own and not everyone:
        raise HTTPException(
            status_code=403,
            detail="Reading another account's spending needs credits:Read.",
        )
    scoped = account_id or (None if everyone else own)
    try:
        groups = _credits.breakdown(
            group_by=group_by, period=period, account_id=scoped
        )
    except credits.CreditError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    names = _catalog_names() if group_by == "workflow" else {}
    rows = [
        UsageGroupOut(**{**row, "label": names.get(str(row["key"]), row["label"])})
        if row["key"] else UsageGroupOut(**row)
        for row in groups
    ]
    # Counted from the entries rather than summed from the rows: by tag a run
    # appears under each of its labels, and a total that added those up would
    # tell the reader the month cost more than it did.
    total = _credits.totals(period=period, account_id=scoped)
    return UsageBreakdownOut(
        period=period or credits.current_period(),
        group_by=group_by,
        scope="all" if scoped is None else scoped,
        total=UsageGroupOut(key=None, label="Total", **total),
        groups=rows,
        overlapping=group_by == "tag",
    )


@app.get("/credits/timeline", response_model=UsageTimelineOut)
def credit_timeline(
    months: int = 6,
    account_id: Optional[str] = None,
    principal: Any = Depends(current_principal),
) -> UsageTimelineOut:
    """Spending month by month, oldest first.

    One month says how much; the series says whether that is normal, which is the
    question somebody looking at a bill actually has. Scope follows the same rule
    as the rest of billing: your own team always, the whole runner with
    `credits:Read`.
    """
    own, _ = credits.account_for(principal)
    everyone = principal is not None and principal.allows("credits:Read")
    if account_id and account_id != own and not everyone:
        raise HTTPException(
            status_code=403,
            detail="Reading another account's spending needs credits:Read.",
        )
    scoped = account_id or (None if everyone else own)
    return UsageTimelineOut(
        scope="all" if scoped is None else scoped,
        periods=[
            UsagePeriodOut(**row)
            for row in _credits.usage_timeline(months=months, account_id=scoped)
        ],
    )


@app.get(
    "/credits",
    response_model=List[AccountOut],
    dependencies=[Depends(requires("credits:Read"))],
)
def list_credit_accounts() -> List[AccountOut]:
    return [_account_out(account) for account in _credits.list_accounts()]


@app.get(
    "/credits/{account_id}/ledger",
    response_model=List[LedgerEntryOut],
)
def credit_ledger(
    account_id: str, limit: int = 100, principal: Any = Depends(current_principal)
) -> List[LedgerEntryOut]:
    """What an account was charged, newest first. Your own is always readable;
    anyone else's needs `credits:Read`."""
    own, _ = credits.account_for(principal)
    if account_id != own and not principal.allows("credits:Read"):
        raise HTTPException(
            status_code=403,
            detail="Reading another account's credit ledger needs credits:Read.",
        )
    return [_entry_out(entry) for entry in _credits.ledger(account_id, limit=limit)]


@app.post(
    "/credits/{account_id}/grant",
    response_model=AccountOut,
    dependencies=[Depends(requires("credits:Manage"))],
)
def grant_credits(account_id: str, body: GrantRequest) -> AccountOut:
    """Adds credits, or takes them back with a negative amount. Both are the same
    operation on purpose: every movement of an account is then one table to read."""
    try:
        account = _credits.grant(account_id, body.amount, note=body.note)
    except credits.CreditError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return _account_out(account)


@app.post(
    "/runs/{run_id}/cancel",
    response_model=CancelResponse,
    dependencies=[Depends(requires("run:Cancel"))],
)
def cancel_run(run_id: str) -> CancelResponse:
    """Stops the run in flight.

    Two things happen: the flag makes the flow stop at the next stage boundary, and
    `cancelAllJobs()` aborts whatever Spark is computing right now — without it a
    long write would run to completion no matter what the flag says.

    409 when `run_id` is not the run this process is executing: a finished run has
    nothing to cancel, and cancelling one run must never touch another.
    """
    if not _ACTIVE_RUN.request(run_id):
        raise HTTPException(
            status_code=409,
            detail="This execution is not running on this runner any more.",
        )
    return CancelResponse(
        cancelled=True, run_id=run_id, spark_jobs_cancelled=_cancel_spark_jobs()
    )


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


# Started last, once `_log` and the whole module exist: the first purge runs
# immediately, on a daemon thread, so it never delays a request or survives a

# ------------------------------------------------------- the assistant


class AssistantTurnIn(BaseModel):
    """One message of the transcript the browser is holding.

    Only `user` and `assistant` are accepted; tool calls and results are the
    runner's business and are rebuilt on every turn rather than trusted from the
    client — a transcript that can name a tool result is a transcript that can
    forge one.
    """

    role: str
    content: str


class AssistRequest(BaseModel):
    messages: List[AssistantTurnIn] = Field(default_factory=list)
    #: Overrides the runner's default for this turn only. Free text, like every
    #: model id in this product.
    model: Optional[str] = None
    #: What the question is about, so the cost lands on the right line of the
    #: bill. Optional, because a question asked from the assistant screen belongs
    #: to no Workflow and saying so is more honest than guessing.
    workflow_id: Optional[str] = None
    #: Extra guidance appended to the runner's own prompt — how the caller wants
    #: the answer shaped, which the canvas panel uses to ask for the JSON
    #: envelope it knows how to apply. It never replaces the prompt.
    instructions: Optional[str] = None


class AssistantInfo(BaseModel):
    """What `GET /assistant` answers: whether it can answer, and with what."""

    backend: str
    available: bool
    local: bool
    model: str = ""
    base_url: str = ""
    models: List[str] = Field(default_factory=list)
    tools: List[str] = Field(default_factory=list)
    version: str = ""
    hint: str = ""
    error: str = ""


class AssistTurnOut(BaseModel):
    id: str
    period: str
    backend: str
    provider: str
    model: str
    local: bool
    input_tokens: int
    output_tokens: int
    tool_calls: int
    duration_ms: int
    amount: int
    created_at: str
    actor: Optional[str] = None
    workflow_id: Optional[str] = None


class AssistSummaryOut(BaseModel):
    period: str
    scope: str
    turns: int
    local_turns: int
    remote_turns: int
    input_tokens: int
    output_tokens: int
    tool_calls: int
    charged: int
    seconds: int
    recent: List[AssistTurnOut] = Field(default_factory=list)


def _assistant_info(detail: Dict[str, Any]) -> AssistantInfo:
    return AssistantInfo(
        backend=str(detail.get("backend") or ""),
        available=bool(detail.get("available")),
        local=bool(detail.get("local", True)),
        model=str(detail.get("model") or ""),
        base_url=str(detail.get("baseUrl") or ""),
        models=[str(name) for name in detail.get("models") or []],
        tools=[str(name) for name in detail.get("tools") or []],
        version=str(detail.get("version") or ""),
        hint=str(detail.get("hint") or ""),
        error=str(detail.get("error") or ""),
    )


@app.get("/assistant", response_model=AssistantInfo)
def assistant_info() -> AssistantInfo:
    """Which runtime answers here, and whether it can right now.

    No permission needed, and deliberately: the Studio asks this on every load to
    decide whether to offer the runner's assistant at all, and a 403 would be
    indistinguishable from a runner that has none.
    """
    try:
        return _assistant_info(assistant.describe())
    except assistant.AssistantUnavailable as error:
        return AssistantInfo(
            backend=assistant.backend_name(), available=False, local=True,
            error=str(error), hint=getattr(error, "hint", ""),
        )


@app.post(
    "/assistant/stream",
    dependencies=[Depends(requires("assistant:Ask"))],
)
def assistant_stream(
    body: AssistRequest, principal: Any = Depends(current_principal)
) -> StreamingResponse:
    """One turn, as Server-Sent Events.

    Events: `delta` with the text as it arrives, `tool` when the assistant called
    one of this runner's tools and what it answered, then a final `done` carrying
    what the turn consumed — or `error`.

    The turn is metered when it ends, not when it starts, because what it cost is
    not known until the model stops. A client that hangs up mid-answer still pays
    for the work, which is why the recording happens in `finally` and not after
    the last yield.
    """
    try:
        backend = assistant.build()
    except assistant.AssistantUnavailable as error:
        raise HTTPException(
            status_code=503,
            detail=f"{error} {getattr(error, 'hint', '')}".strip(),
        ) from error

    turns = assistant.turns_of(body.messages)
    if not turns:
        raise HTTPException(status_code=400, detail="There is nothing to answer.")

    account_id, username = credits.account_for(principal)
    actor = credits.actor_for(principal)

    def _stream() -> Iterator[str]:
        usage: Optional[Any] = None
        try:
            system = assistant.prompt_with(body.instructions or "")
            for event in backend.stream(turns, model=body.model, system=system):
                if event.kind == "done":
                    usage = event.usage
                yield _sse(event.kind, event.payload())
        except assistant.AssistantUnavailable as error:
            yield _sse("error", {"message": str(error), "hint": getattr(error, "hint", "")})
        except Exception as error:  # pragma: no cover - defensive
            yield _sse("error", {"message": f"{type(error).__name__}: {error}"})
        finally:
            if usage is not None:
                try:
                    _credits.record_assist(
                        account_id, backend=backend.id, provider=usage.provider,
                        model=usage.model, local=usage.local,
                        input_tokens=usage.input_tokens,
                        output_tokens=usage.output_tokens,
                        duration_ms=usage.duration_ms, tool_calls=usage.tool_calls,
                        username=username, actor=actor, workflow_id=body.workflow_id,
                    )
                except Exception:  # pragma: no cover - metering must not break a turn
                    _log.exception("Could not record the assistant turn")

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/credits/assist", response_model=AssistSummaryOut)
def assist_usage(
    period: Optional[str] = None,
    account_id: Optional[str] = None,
    limit: int = 20,
    principal: Any = Depends(current_principal),
) -> AssistSummaryOut:
    """A month of assistant work, and the most recent turns behind it.

    Reported next to the rest of billing and not inside the assistant screen,
    because the question it answers — is this thing costing us anything — is a
    billing question. Scope follows the same rule as every other bill: your own
    team always, the whole runner with `credits:Read`.
    """
    own, _ = credits.account_for(principal)
    everyone = principal is not None and principal.allows("credits:Read")
    if account_id and account_id != own and not everyone:
        raise HTTPException(
            status_code=403,
            detail="Reading another account's spending needs credits:Read.",
        )
    scoped = account_id or (None if everyone else own)
    summary = _credits.assist_summary(account_id=scoped, period=period)
    recent = _credits.assist_turns(
        account_id=scoped, limit=limit, period=period or credits.current_period()
    )
    return AssistSummaryOut(
        period=summary.period, scope=summary.account_id, turns=summary.turns,
        local_turns=summary.local_turns, remote_turns=summary.remote_turns,
        input_tokens=summary.input_tokens, output_tokens=summary.output_tokens,
        tool_calls=summary.tool_calls, charged=summary.charged,
        seconds=summary.seconds,
        recent=[
            AssistTurnOut(
                id=turn.id, period=turn.period, backend=turn.backend,
                provider=turn.provider, model=turn.model, local=turn.local,
                input_tokens=turn.input_tokens, output_tokens=turn.output_tokens,
                tool_calls=turn.tool_calls, duration_ms=turn.duration_ms,
                amount=turn.amount, created_at=turn.created_at, actor=turn.actor,
                workflow_id=turn.workflow_id,
            )
            for turn in recent
        ],
    )



# shutdown. `SPARQUET_STUDIO_HISTORY_PURGE=off` leaves the database untouched.
if history.RetentionPolicy.enabled():
    threading.Thread(
        target=_purge_history_periodically, name="history-purge", daemon=True
    ).start()


# On unless turned off, like the purge and unlike the warm-up: a sweep is one
# query against a SQLite file, and a rule nobody evaluates is not a rule.
# `SPARQUET_STUDIO_MONITORS=off` stops the timer and leaves the rules in place.
if monitoring.sweeping_enabled():
    threading.Thread(target=_watch_monitors, name="monitor-sweep", daemon=True).start()


# On unless turned off, for the same reason: a schedule honoured only while
# somebody has the Studio open is not a schedule. `SPARQUET_STUDIO_SCHEDULER=off`
# stops the timer and leaves the records alone, which is how a second runner
# pointed at the same library avoids both of them firing the same Job.
if scheduling.scheduling_enabled():
    threading.Thread(target=_watch_schedules, name="schedule-sweep", daemon=True).start()


# Off unless asked for, unlike the purge above, and the asymmetry is deliberate:
# importing this module must not cost a JVM. The tests import it, and so does
# anything that inspects the app — a warm-up on import would turn every one of
# those into a Spark start-up. `SPARQUET_STUDIO_WARM_SPARK=on` is for the process
# that serves the Studio, where the session is going to be built anyway and the
# only question is who waits for it.
if _warm_enabled():
    threading.Thread(target=_warm_spark, name="spark-warm", daemon=True).start()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.getenv("SPARQUET_STUDIO_HOST", "127.0.0.1"),
        port=int(os.getenv("SPARQUET_STUDIO_PORT", "8787")),
    )
