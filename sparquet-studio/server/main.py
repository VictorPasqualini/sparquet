"""Sparquet Studio — local execution bridge.

Runs a pipeline described by an HTTP body through the real Sparquet and
returns counters, validations, a small data preview and the framework's own
structured logs.

SECURITY WARNING
================
Every request executes arbitrary Spark work: arbitrary SQL, arbitrary reads and
arbitrary writes on the machine (and on any warehouse this machine can reach).

`/run` and `/validate` therefore require a shared secret in the `X-Sparquet-Token`
header, printed on startup (or taken from `SPARQUET_STUDIO_TOKEN`), and reject any
request whose `Origin` is outside the allow-list. Without that, any web page the
developer happens to visit could drive this runner: CORS withholds the *response*
from the attacker but never stops the request from executing. `/health` stays open
so Studio can detect the runner and prompt for the token.

This is still a single-developer tool: keep it bound to 127.0.0.1 and never expose
it to a network or the public internet.

Run it from the `sparquet-studio` directory; this module inserts the repository
root into sys.path so `sparquet` is importable:

    uvicorn server.main:app --port 8787
"""

from __future__ import annotations

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
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
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
providers = _load_sibling_module("providers")


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
                f"Cannot import '{module_name}': {exc}. Start the runner from the "
                "sparquet-studio directory inside the Sparquet repository, or "
                "install pyspark and the sparquet framework in this environment."
            ),
        ) from exc


def _get_framework() -> Any:
    """One Sparquet per process — the SparkSession is a process-global
    singleton, so recreating it per request would be both slow and useless."""
    global _framework
    if _framework is None:
        _framework = _import("sparquet").Sparquet()
    return _framework


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


def require_token(request: Request) -> None:
    """Blocks drive-by requests from any page the developer happens to visit.

    CORS cannot do this: for a request with no custom header and no JSON content
    type the browser skips the preflight, the app runs, and only the *response*
    is withheld from the attacker. A header the browser refuses to attach
    cross-origin without a preflight, plus a server-side Origin check, do.
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

    if not secrets.compare_digest(request.headers.get(TOKEN_HEADER, ""), AUTH_TOKEN):
        raise HTTPException(status_code=401, detail=UNAUTHORIZED_HELP)


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


def current_principal(request: Request) -> Any:
    """Who is calling, after `require_token` has already vouched for the request.

    Two modes, and the difference is only whether any user exists. With none, the
    runner behaves as it always has: the shared token is the identity, with full
    rights — that runner has one operator, and upgrading must not lock them out.
    With users, a session is required for everything but logging in.
    """
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
    return HealthResponse(
        status="ok" if available and version else "degraded",
        version=SERVICE_VERSION,
        spark_available=available,
        framework_version=version,
        login_required=_auth.has_users(),
        credits_enforced=credits.enforced(),
        providers=providers.describe(),
    )


@app.post(
    "/validate",
    response_model=ValidateResponse,
    dependencies=[Depends(requires("run:Validate"))],
)
def validate(body: ValidateRequest) -> ValidateResponse:
    error = _parse_config_error(body.pipeline, body.params)
    return ValidateResponse(valid=error is None, error=error)


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
            response = _execute_run(body, name, started, collector)
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
    _history.finish_pipeline_run(
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


def _execute_run(
    body: RunRequest, name: Optional[str], started: float, collector: _LogCollector
) -> RunResponse:
    """The body shared by `/run` and `/run/stream`: execute the pipeline and shape
    the response. The caller owns the run lock and the log capture."""
    framework = _get_framework()
    try:
        result = framework.run_from_dict(body.pipeline, params=body.params or None)
    except Exception as exc:
        # Config loading (missing keys, bad $include) raises outside the
        # pipeline's own try block and never reaches PipelineResult.
        return RunResponse(
            success=False,
            pipeline_name=name,
            duration_ms=_elapsed_ms(started),
            error=_describe(exc),
            logs=[LogOut(**entry) for entry in collector.records],
        )

    output_df = getattr(result, "output_df", None)
    preview = (
        _build_preview(output_df, body.limit, collector)
        if output_df is not None
        else None
    )

    return RunResponse(
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
    )


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
                box["response"] = _execute_run(body, name, started, collector)
        except Exception as exc:  # pragma: no cover - defensive
            box["error"] = _describe(exc)
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
                _history.finish_pipeline_run(
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
                _history.finish_pipeline_run(
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
    started = time.perf_counter()

    if not body.stages:
        raise HTTPException(status_code=422, detail="A flow needs at least one stage.")

    # Before anything is charged, locked or started: a stage that points at a file
    # gets that file read now, so a missing or unparseable one is a 400 naming it
    # rather than a flow that dies halfway with earlier stages already written.
    _resolve_staged_files(body.stages)

    # One check for the flow, against its first stage: a Pipeline whose team has
    # nothing available should not start at all. Nothing is held here — each stage
    # reserves what it declares as it starts, and settles as it finishes.
    _precheck_execution(principal, body.stages[0].pipeline)

    if not _RUN_LOCK.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="A pipeline run is already in progress on this runner.",
        )

    _ensure_catalog(
        workflow_id=body.workflow_id, pipeline_id=body.pipeline_id, name=body.name,
        job_ids=[stage.job_id for stage in body.stages],
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
                pipeline_run_id, job_id=pending.job_id, name=pending.name,
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
                        pipeline_run_id, job_id=stage.job_id, name=stage.name,
                        stage_index=index, status=history.FAILED,
                    )
                    for pending_index in range(index + 1, len(body.stages)):
                        pending = body.stages[pending_index]
                        _history.skip_job_run(
                            pipeline_run_id, job_id=pending.job_id, name=pending.name,
                            stage_index=pending_index, status=history.SKIPPED,
                        )
                    break
                stage_hash, stage_config = _config_version(stage.pipeline, stage.params)
                job_run_id = _history.create_job_run(
                    pipeline_run_id, job_id=stage.job_id, name=stage.name,
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
                            request, stage.name, stage_started, collector
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
                        job_id=stage.job_id, extra=body.tags,
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
            box["error"] = _describe(exc)
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
            _history.finish_pipeline_run(
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


class WorkspaceDocumentOut(BaseModel):
    kind: str
    id: str
    record: Dict[str, Any]
    #: Relative path of the reviewable file, so the UI can tell the user what to commit.
    path: Optional[str] = None


class WorkspaceSnapshotOut(BaseModel):
    root: str
    workflows: List[WorkspaceDocumentOut] = Field(default_factory=list)
    jobs: List[WorkspaceDocumentOut] = Field(default_factory=list)
    pipelines: List[WorkspaceDocumentOut] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)


class WorkspaceWriteRequest(BaseModel):
    #: The Studio record exactly as the editor holds it.
    record: Dict[str, Any]
    #: A Job's compiled Sparquet JSON. Sent so the readable file is the pipeline the
    #: framework runs, not an editor-shaped document nobody can execute.
    config: Optional[Dict[str, Any]] = None


class WorkspaceDeleteResponse(BaseModel):
    deleted: bool


def _workspace_doc_out(doc: Any) -> WorkspaceDocumentOut:
    return WorkspaceDocumentOut(kind=doc.kind, id=doc.id, record=doc.record, path=doc.path)


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
        meta=snapshot.meta,
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


class WorkspaceMetaRequest(BaseModel):
    value: Any = None


@app.put(
    "/workspace/meta/{key}",
    response_model=Dict[str, Any],
    dependencies=[Depends(requires("workspace:Write", "meta/*"))],
)
def put_workspace_meta(key: str, body: WorkspaceMetaRequest) -> Dict[str, Any]:
    """Library-level bookkeeping (storage version, seeded flag). Kept with the files
    rather than in the browser, so the answer to "was this library already
    migrated?" travels with the library."""
    _workspace.write_meta(key, body.value)
    return {"key": key, "value": body.value}


@app.delete(
    "/workspace/meta/{key}",
    response_model=Dict[str, Any],
    dependencies=[Depends(requires("workspace:Write", "meta/*"))],
)
def delete_workspace_meta(key: str) -> Dict[str, Any]:
    _workspace.delete_meta(key)
    return {"key": key, "deleted": True}


@app.get(
    "/workspace/{kind}/{record_id}",
    response_model=WorkspaceDocumentOut,
    dependencies=[Depends(requires("workspace:Read", _workspace_resource))],
)
def get_workspace_document(kind: str, record_id: str) -> WorkspaceDocumentOut:
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
    kind: str, record_id: str, body: WorkspaceWriteRequest
) -> WorkspaceDocumentOut:
    """Saves one record. Writes the file first, then indexes it."""
    try:
        doc = _workspace.write(
            workspace.Document(
                kind=kind, id=record_id, record=body.record, config=body.config
            )
        )
    except workspace.WorkspaceError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except OSError as exc:
        raise HTTPException(
            status_code=500, detail=f"Could not write to the workspace: {exc}"
        ) from exc
    _mirror_catalog(doc)
    return _workspace_doc_out(doc)


@app.delete(
    "/workspace/{kind}/{record_id}",
    response_model=WorkspaceDeleteResponse,
    dependencies=[Depends(requires("workspace:Delete", _workspace_resource))],
)
def delete_workspace_document(kind: str, record_id: str) -> WorkspaceDeleteResponse:
    """Removes the files. The catalog row stays, marked deleted, because past runs
    point at it and a run that names a record nobody can look up is worse than a
    record marked gone."""
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


@app.get("/auth/status", response_model=AuthStatusOut, dependencies=[Depends(require_token)])
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


@app.post("/auth/login", response_model=SessionOut, dependencies=[Depends(require_token)])
def auth_login(body: LoginRequest) -> SessionOut:
    """Exchanges a username and password for a session.

    One message for every kind of failure — unknown user, wrong password, disabled
    account — because saying which one is a free answer to somebody guessing.
    """
    session = _auth.login(body.username, body.password)
    if session is None:
        raise HTTPException(status_code=401, detail="Wrong username or password.")
    return SessionOut(
        token=session.token,
        expires_at=session.expires_at,
        user=PrincipalOut(
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


@app.post("/auth/recover", dependencies=[Depends(require_token)])
def recover_password(body: RecoverRequest) -> Dict[str, bool]:
    """Trades a recovery code for a new password. No session required — the
    caller is by definition locked out — but the shared token still is, because
    this endpoint is on the same runner as everything else.

    Every failure reads the same, deliberately: unknown code, expired code, code
    already used, account disabled. A specific answer would make this an oracle
    for someone holding the token and guessing.
    """
    try:
        _auth.redeem_recovery(body.code, body.password)
    except auth.AuthError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
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
# shutdown. `SPARQUET_STUDIO_HISTORY_PURGE=off` leaves the database untouched.
if history.RetentionPolicy.enabled():
    threading.Thread(
        target=_purge_history_periodically, name="history-purge", daemon=True
    ).start()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.getenv("SPARQUET_STUDIO_HOST", "127.0.0.1"),
        port=int(os.getenv("SPARQUET_STUDIO_PORT", "8787")),
    )
