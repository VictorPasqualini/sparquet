"""Studio's catalog and execution history: what exists, and what it did.

Two halves of one database, joined by real foreign keys.

**Catalog** — the records themselves, mirrored from the file workspace
(`workspace.py`): `workflow` owns `pipeline` and `job`; `pipeline_stage` is the
junction that says which Jobs a Pipeline runs, and in what order. A Job belongs to
a Workflow, not to a Pipeline: it can be a stage of several Pipelines, appear twice
in the same one, or run entirely on its own — so a `job.pipeline_id` column would
have to lie about at least one of those. `pipeline_stage` carries that relationship
where it actually lives.

**History** — what happened: `pipeline_run` -> `job_run` -> `step_run`, plus the
`run_log` lines each job execution printed.

Vocabulary note (see repo CLAUDE.md): a Studio "Pipeline" is an ordered sequence of
Jobs; a Studio "Job" is one framework pipeline JSON. A `pipeline_run` row here is the
top-level execution instance either way — a solo Job run (`kind="job"`, exactly one
`job_run`) or a multi-Job Pipeline run (`kind="pipeline"`, one `job_run` per stage). A
`step_run` is one step inside a Job's own execution (input / transform / validation /
validation_sink / output), keyed by the framework's own `step=True` log markers.

Deletes in the catalog are SOFT (`deleted_at`). A Job that is removed from the
Studio still has runs in the history, and those runs still point at it: hard-deleting
the row would either orphan them or force the run's own `job_id` to null, and both
lose the answer to "what ran here in March".

Storage: SQLite (stdlib `sqlite3`), one file under `server/data/`. Kept behind
`ExecutionRepository` so a future `PostgresExecutionRepository` or
`CloudExecutionRepository` can replace `SQLiteExecutionRepository` without the runner
(`main.py`) changing at all.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple

# --------------------------------------------------------------------- status

PENDING = "pending"
RUNNING = "running"
SUCCESS = "success"
FAILED = "failed"
SKIPPED = "skipped"
# Stopped on purpose: the user asked for it while it was running. Distinct from
# FAILED — nothing is wrong with the pipeline — and from SKIPPED, which is the
# framework's own graceful ending (`stop_if_empty`) or a stage that never started
# because an earlier one failed.
CANCELLED = "cancelled"

# ------------------------------------------------------------------ launched by
# How a run got started. "manual" is a person pressing Run in Studio, "scheduled"
# a timer or orchestrator, "api" a direct call to the runner. Kept as plain
# strings, not an enum, so a caller that invents a fifth kind is recorded rather
# than rejected — history describes what happened, it does not police it.
MANUAL = "manual"
SCHEDULED = "scheduled"
API = "api"
# The framework itself, reporting a run that happened somewhere else entirely —
# `sparquet.cli`, Airflow, Databricks — and that this runner never executed. See
# `ingest_run`.
EXTERNAL = "external"
LAUNCH_KINDS = (MANUAL, SCHEDULED, API, EXTERNAL)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex


# --------------------------------------------------------------------------- rows


@dataclass
class StepRun:
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
    # The quality datasets are addressed by ROLE ("report"/"valid"/"invalid"), not
    # by a position: they take no connection, so there is no lane to count in.
    # `step_index` still carries arrival order there, purely so the list keeps a
    # stable order — the identity is the role.
    role: Optional[str] = None
    # Everything the framework reported about the step beyond its status, as JSON:
    # rows, path, format, and for a rule whether it passed. What makes a box's
    # state readable when the run is opened again from history.
    details: Optional[str] = None


@dataclass
class RunLogLine:
    """One line of what a job execution printed, kept so a past run can be read back
    exactly as it was watched live."""

    job_run_id: str
    seq: int
    timestamp: str
    level: str
    # "pipeline" (the framework's own structured records), "spark" (the JVM's
    # stderr) or "stdout" (a `debug` transformation's prints).
    source: str
    message: str
    # The structured record's own fields, as a JSON object, or None for a plain line.
    context: Optional[str] = None


@dataclass
class JobRun:
    id: str
    pipeline_run_id: str
    job_id: Optional[str]
    name: Optional[str]
    stage_index: int
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None
    rows_read: Optional[int] = None
    rows_written: Optional[int] = None
    # What this execution read and what it wrote, as a JSON object
    # (`{"inputs": [...], "outputs": [...]}`), taken from the JSON that was
    # submitted — so lineage survives even a run that failed before writing.
    lineage: Optional[str] = None
    # Fingerprint of the JSON that actually ran, as `sha256:<hex>`. The Job it
    # points at keeps changing; this does not, so two runs of "the same Job" can
    # be told apart, and a run can be matched against the file in git. The
    # configuration itself is stored beside it and read back with `job_config`.
    config_hash: Optional[str] = None
    steps: List[StepRun] = field(default_factory=list)


@dataclass
class JobConfig:
    """The version of the JSON one execution ran."""

    job_run_id: str
    #: `sha256:<hex>`, or None for a run recorded before this was kept.
    config_hash: Optional[str] = None
    #: The configuration itself, or None when it predates the column or was over
    #: `MAX_STORED_CONFIG_BYTES`.
    config: Optional[Dict[str, Any]] = None


@dataclass
class PipelineRun:
    id: str
    kind: str  # "job" | "pipeline"
    workflow_id: Optional[str]
    pipeline_id: Optional[str]
    job_id: Optional[str]
    name: Optional[str]
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None
    # Who asked for it, and how it got started ("manual" | "scheduled" | "api").
    # The runner has no user directory: `run_as` is whatever the caller claims,
    # falling back to the account the runner process itself runs under.
    run_as: Optional[str] = None
    launched: Optional[str] = None
    #: Marked by a person as worth keeping. Retention never touches a pinned run
    #: — "this is the execution of the incident" has to outlive any age rule.
    pinned: bool = False
    jobs: List[JobRun] = field(default_factory=list)


@dataclass
class CatalogRecord:
    """One row of the catalog: a Workflow, a Pipeline or a Job.

    The same shape for all three because they differ only in what they point at —
    a Workflow points at nothing, the other two at their Workflow. `stages` is
    filled for a Pipeline only, from the `pipeline_stage` junction.
    """

    id: str
    kind: str  # "workflow" | "pipeline" | "job"
    workflow_id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    #: Where the readable file lives in the workspace, relative to its root.
    path: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    #: Set when the record was removed from the Studio. The row itself stays, so
    #: the runs that name it keep pointing at something that exists.
    deleted_at: Optional[str] = None
    stages: List[Dict[str, Any]] = field(default_factory=list)
    #: Free labels the user put on this record, to slice history and spending by
    #: something the organisation cares about — a cost centre, a domain, an
    #: environment. They belong to the record, not to the run.
    tags: List[str] = field(default_factory=list)


# --------------------------------------------------------------------- retention


#: How many ids go into one `IN (...)` clause. SQLite's parameter limit is far
#: higher, but a purge of a year's runs should not build a query megabytes long.
_CHUNK = 400


def _chunks(items: List[str], size: int = _CHUNK):
    for start in range(0, len(items), size):
        yield items[start:start + size]


@dataclass(frozen=True)
class RetentionPolicy:
    """What the history keeps, and for how long.

    The database grows very unevenly, which is why expiry happens in two stages
    rather than one. Almost all the bytes are `run_log` (one row per logged line;
    the 3000-line ceiling is *per execution*, not for the file) and
    `job_run.config` (the whole JSON that ran, per execution). The execution row
    itself — status, timings, rows read and written, lineage, who ran it — is
    about 200 bytes and is exactly what a time series needs.

    So:

    - after `detail_days`, a run is **thinned**: its log lines, its step rows and
      its stored configuration go, and the run stays, still reporting what it did
      and still identified by `config_hash`.
    - after `max_days`, the run row itself goes — but only when `delete` is on.
      Losing the fact that something ran is the operator's call, not a default.

    Two guards sit above the ages: a run marked `pinned` is never touched, and the
    newest `keep_runs` executions of every Job and Pipeline are always kept, however
    old, so a monthly Job does not open on an empty screen.

    The credit ledger is never touched by any of this. It lives in its own
    database file and references `job_run_id`: billing is a financial record, so
    the detail of an execution may expire while the line that charged for it
    stays.
    """

    detail_days: int = 30
    max_days: int = 365
    keep_runs: int = 10
    #: Delete the run rows themselves once they pass `max_days`. Off by default:
    #: thinning is reversible in its consequences (you lose detail), deleting is not.
    delete: bool = False
    #: Rows that have to come out before the file is rewritten. VACUUM copies the
    #: whole database, so it is not worth doing for a handful of log lines.
    vacuum_after: int = 1000

    @classmethod
    def from_env(cls, env: Optional[Dict[str, str]] = None) -> "RetentionPolicy":
        """`SPARQUET_STUDIO_HISTORY_*`: `PURGE` (on|off, default on), `DETAIL_DAYS`,
        `MAX_DAYS`, `KEEP_RUNS`, `DELETE` (on|off, default off). A value that does
        not parse falls back to the default rather than stopping the runner from
        starting."""
        source = env if env is not None else os.environ

        def number(name: str, fallback: int) -> int:
            try:
                value = int(str(source.get(f"SPARQUET_STUDIO_HISTORY_{name}", "")).strip())
            except ValueError:
                return fallback
            return value if value >= 0 else fallback

        return cls(
            detail_days=number("DETAIL_DAYS", cls.detail_days),
            max_days=number("MAX_DAYS", cls.max_days),
            keep_runs=number("KEEP_RUNS", cls.keep_runs),
            delete=_flag(source.get("SPARQUET_STUDIO_HISTORY_DELETE"), False),
        )

    @staticmethod
    def enabled(env: Optional[Dict[str, str]] = None) -> bool:
        """Whether the runner runs the purge on its own at all. `off` leaves the
        database exactly as it is; `POST /runs/purge` still works by hand."""
        source = env if env is not None else os.environ
        return _flag(source.get("SPARQUET_STUDIO_HISTORY_PURGE"), True)


def _flag(value: Optional[str], fallback: bool) -> bool:
    if value is None or not str(value).strip():
        return fallback
    return str(value).strip().lower() in ("1", "on", "true", "yes")


@dataclass
class PurgeReport:
    """What a purge did, or would do when `dry_run`."""

    dry_run: bool = False
    #: Runs that kept their row and lost their detail.
    runs_thinned: int = 0
    #: Runs whose row went too.
    runs_deleted: int = 0
    logs_deleted: int = 0
    steps_deleted: int = 0
    configs_dropped: int = 0
    vacuumed: bool = False

    @property
    def rows_removed(self) -> int:
        return self.logs_deleted + self.steps_deleted + self.runs_deleted

    def as_dict(self) -> Dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "runs_thinned": self.runs_thinned,
            "runs_deleted": self.runs_deleted,
            "logs_deleted": self.logs_deleted,
            "steps_deleted": self.steps_deleted,
            "configs_dropped": self.configs_dropped,
            "rows_removed": self.rows_removed,
            "vacuumed": self.vacuumed,
        }


# --------------------------------------------------------------------- repository


class ExecutionRepository(Protocol):
    # ---- catalog: what exists
    def upsert_workflow(
        self, workflow_id: str, *, name: Optional[str] = None,
        description: Optional[str] = None, path: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> None: ...

    def upsert_job(
        self, job_id: str, *, workflow_id: Optional[str] = None, name: Optional[str] = None,
        description: Optional[str] = None, path: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> None: ...

    def upsert_pipeline(
        self, pipeline_id: str, *, workflow_id: Optional[str] = None,
        name: Optional[str] = None, description: Optional[str] = None,
        path: Optional[str] = None, stages: Optional[List[Dict[str, Any]]] = None,
        tags: Optional[List[str]] = None,
    ) -> None: ...

    def tags_for(self, kind: str, record_id: str) -> List[str]: ...

    def effective_tags(
        self, *, workflow_id: Optional[str] = None, pipeline_id: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> List[str]: ...

    def list_tags(self) -> List[Dict[str, Any]]: ...

    def soft_delete(self, kind: str, record_id: str) -> None: ...

    def list_catalog(self, *, include_deleted: bool = False) -> List[CatalogRecord]: ...

    def ensure_run_targets(
        self, *, workflow_id: Optional[str], pipeline_id: Optional[str],
        job_id: Optional[str], name: Optional[str] = None,
    ) -> None: ...

    # ---- history: what happened
    def create_pipeline_run(
        self, *, kind: str, workflow_id: Optional[str], pipeline_id: Optional[str],
        job_id: Optional[str], name: Optional[str], run_as: Optional[str] = None,
        launched: str = MANUAL, started_at: Optional[str] = None,
    ) -> str: ...

    def finish_pipeline_run(
        self, run_id: str, *, status: str, duration_ms: int, error: Optional[str],
        finished_at: Optional[str] = None,
    ) -> None: ...

    def create_job_run(
        self, pipeline_run_id: str, *, job_id: Optional[str], name: Optional[str],
        stage_index: int, lineage: Optional[str] = None,
        config_hash: Optional[str] = None, config: Optional[str] = None,
        started_at: Optional[str] = None,
    ) -> str: ...

    def job_config(self, job_run_id: str) -> Optional[JobConfig]: ...

    def finish_job_run(
        self, job_run_id: str, *, status: str, duration_ms: int,
        error: Optional[str], rows_read: Optional[int], rows_written: Optional[int],
        finished_at: Optional[str] = None,
    ) -> None: ...

    def skip_job_run(
        self, pipeline_run_id: str, *, job_id: Optional[str], name: Optional[str],
        stage_index: int, status: str = SKIPPED,
    ) -> str: ...

    def create_step_run(
        self, job_run_id: str, scope: str, step_index: int, type_: str, *,
        status: str, timestamp: str, role: Optional[str] = None,
        details: Optional[str] = None,
    ) -> str: ...

    def finish_step_run(
        self, step_id: str, *, status: str, timestamp: str,
        error_message: Optional[str], error_details: Optional[str],
        details: Optional[str] = None,
    ) -> None: ...

    def append_logs(self, job_run_id: str, lines: List[Dict[str, Any]]) -> int: ...

    def list_logs(
        self, job_run_id: str, *, after_seq: int = 0, limit: int = 500,
    ) -> List[RunLogLine]: ...

    def count_logs(self, job_run_id: str) -> int: ...

    def list_pipeline_runs(
        self, *, workflow_id: Optional[str] = None, pipeline_id: Optional[str] = None,
        job_id: Optional[str] = None, limit: int = 20,
    ) -> List[PipelineRun]: ...

    def get_pipeline_run(self, run_id: str) -> Optional[PipelineRun]: ...

    def set_pinned(self, run_id: str, pinned: bool) -> bool: ...

    def purge(
        self, policy: "RetentionPolicy", *, dry_run: bool = False,
        now: Optional[datetime] = None,
    ) -> "PurgeReport": ...


#: A tag is a label a human types, so it is bounded here rather than trusted.
MAX_TAGS = 20
MAX_TAG_LENGTH = 40


def normalize_tags(values: Any) -> List[str]:
    """The tags as they will be stored: trimmed, deduplicated, bounded.

    Deduplication is case-insensitive while the stored form keeps the case the
    user typed. `Prod` and `prod` are one tag — two would split a bill in half
    for a reason nobody would ever guess from the screen — but the label is
    still shown the way it was written.
    """
    if not isinstance(values, (list, tuple, set)):
        return []
    out: List[str] = []
    seen = set()
    for value in values:
        if not isinstance(value, str):
            continue
        tag = value.strip()[:MAX_TAG_LENGTH].strip()
        if not tag:
            continue
        key = tag.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(tag)
        if len(out) >= MAX_TAGS:
            break
    return out


_CATALOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflow (
  id TEXT PRIMARY KEY,
  name TEXT,
  description TEXT,
  path TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS pipeline (
  id TEXT PRIMARY KEY,
  workflow_id TEXT REFERENCES workflow(id),
  name TEXT,
  description TEXT,
  path TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  deleted_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipeline_workflow ON pipeline(workflow_id);

CREATE TABLE IF NOT EXISTS job (
  id TEXT PRIMARY KEY,
  workflow_id TEXT REFERENCES workflow(id),
  name TEXT,
  description TEXT,
  path TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  deleted_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_job_workflow ON job(workflow_id);

/* Which Jobs a Pipeline runs, and in what order. The junction exists because the
   relationship is many-to-many: one Job can be a stage of several Pipelines, and
   the same Job can appear twice in one Pipeline (hence `stage_id` in the key, not
   `job_id`). Stages are rewritten wholesale on every save, so they cascade. */
CREATE TABLE IF NOT EXISTS pipeline_stage (
  pipeline_id TEXT NOT NULL REFERENCES pipeline(id) ON DELETE CASCADE,
  stage_id TEXT NOT NULL,
  job_id TEXT NOT NULL REFERENCES job(id),
  stage_index INTEGER NOT NULL,
  PRIMARY KEY (pipeline_id, stage_id)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_stage_job ON pipeline_stage(job_id);

/* Tags of a catalog record. One table for the three kinds because a tag is the
   same thing on all of them, and `kind` in the key is what keeps a Job and a
   Pipeline that happen to share an id apart. Rewritten wholesale on save, like
   stages: the record's tags are what the last save said, never a merge. */
CREATE TABLE IF NOT EXISTS catalog_tag (
  kind TEXT NOT NULL,
  record_id TEXT NOT NULL,
  tag TEXT NOT NULL,
  PRIMARY KEY (kind, record_id, tag)
);
CREATE INDEX IF NOT EXISTS idx_catalog_tag_tag ON catalog_tag(tag);
"""

_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_run (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  workflow_id TEXT REFERENCES workflow(id),
  pipeline_id TEXT REFERENCES pipeline(id),
  job_id TEXT REFERENCES job(id),
  name TEXT,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  duration_ms INTEGER,
  error TEXT,
  run_as TEXT,
  launched TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_pipeline
  ON pipeline_run(pipeline_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_job
  ON pipeline_run(job_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_workflow
  ON pipeline_run(workflow_id, started_at DESC);

CREATE TABLE IF NOT EXISTS job_run (
  id TEXT PRIMARY KEY,
  pipeline_run_id TEXT NOT NULL REFERENCES pipeline_run(id),
  job_id TEXT REFERENCES job(id),
  name TEXT,
  stage_index INTEGER NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  duration_ms INTEGER,
  error TEXT,
  rows_read INTEGER,
  rows_written INTEGER,
  lineage TEXT,
  config_hash TEXT,
  config TEXT
);
CREATE INDEX IF NOT EXISTS idx_job_run_pipeline_run
  ON job_run(pipeline_run_id, stage_index);

CREATE TABLE IF NOT EXISTS step_run (
  id TEXT PRIMARY KEY,
  job_run_id TEXT NOT NULL REFERENCES job_run(id),
  scope TEXT NOT NULL,
  step_index INTEGER NOT NULL,
  type TEXT,
  status TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  duration_ms INTEGER,
  error_message TEXT,
  error_details TEXT,
  role TEXT,
  details TEXT
);
CREATE INDEX IF NOT EXISTS idx_step_run_job_run
  ON step_run(job_run_id, scope, step_index);

CREATE TABLE IF NOT EXISTS run_log (
  job_run_id TEXT NOT NULL REFERENCES job_run(id),
  seq INTEGER NOT NULL,
  timestamp TEXT NOT NULL,
  level TEXT NOT NULL,
  source TEXT NOT NULL,
  message TEXT NOT NULL,
  context TEXT,
  PRIMARY KEY (job_run_id, seq)
);
"""


"""Columns added after the first release, per table. Every one of them must be
nullable: `ALTER TABLE ... ADD COLUMN` fills the existing rows with NULL, and a
run recorded before the column existed genuinely has nothing to put there."""
_LATER_COLUMNS = {
    "step_run": ("role", "details"),
    "pipeline_run": ("run_as", "launched", "pinned"),
    "job_run": ("lineage", "config_hash", "config"),
}


def _add_missing_columns(conn: sqlite3.Connection) -> None:
    """Brings an older database up to the current shape.

    `CREATE TABLE IF NOT EXISTS` is a no-op on a database that already has the
    table, so a column added later never reaches it — which is why `step_index`
    stayed NOT NULL and carries arrival order for the role-keyed steps instead of
    NULL.
    """
    for table, columns in _LATER_COLUMNS.items():
        existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
        for column in columns:
            if column not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} TEXT")


# Schema generations, tracked in `PRAGMA user_version` — the one marker SQLite
# maintains for free. Generation 1 is the catalog: `workflow`/`pipeline`/`job` and
# the foreign keys from the run tables to them.
_SCHEMA_VERSION = 1

_PIPELINE_RUN_COLUMNS = (
    "id, kind, workflow_id, pipeline_id, job_id, name, status, started_at, "
    "finished_at, duration_ms, error, run_as, launched"
)
_JOB_RUN_COLUMNS = (
    "id, pipeline_run_id, job_id, name, stage_index, status, started_at, "
    "finished_at, duration_ms, error, rows_read, rows_written, lineage"
)


def _runs_are_linked(conn: sqlite3.Connection) -> bool:
    """Whether `pipeline_run` already declares its foreign keys into the catalog."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='pipeline_run'"
    ).fetchone()
    return bool(row) and "REFERENCES job(id)" in (row["sql"] or "")


def _backfill_catalog(conn: sqlite3.Connection) -> None:
    """Gives every id the existing history mentions a catalog row to point at.

    A database written before the catalog existed has runs naming workflows, jobs
    and pipelines that no table holds. Adding the foreign key without these rows
    would refuse the copy. They are placeholders on purpose — no name, no
    description — and the Studio's first workspace sync fills them in for whatever
    still exists.
    """
    stamp = _now_iso()
    conn.execute(
        "INSERT OR IGNORE INTO workflow (id, created_at, updated_at) "
        "SELECT DISTINCT workflow_id, ?, ? FROM pipeline_run WHERE workflow_id IS NOT NULL",
        (stamp, stamp),
    )
    conn.execute(
        "INSERT OR IGNORE INTO job (id, created_at, updated_at) "
        "SELECT DISTINCT job_id, ?, ? FROM pipeline_run WHERE job_id IS NOT NULL",
        (stamp, stamp),
    )
    conn.execute(
        "INSERT OR IGNORE INTO job (id, created_at, updated_at) "
        "SELECT DISTINCT job_id, ?, ? FROM job_run WHERE job_id IS NOT NULL",
        (stamp, stamp),
    )
    conn.execute(
        "INSERT OR IGNORE INTO pipeline (id, created_at, updated_at) "
        "SELECT DISTINCT pipeline_id, ?, ? FROM pipeline_run WHERE pipeline_id IS NOT NULL",
        (stamp, stamp),
    )


def _link_runs_to_catalog(conn: sqlite3.Connection) -> None:
    """Rebuilds `pipeline_run` and `job_run` with foreign keys into the catalog.

    SQLite cannot add a foreign key to a table that already exists, so this is the
    documented rebuild: create the new shape, copy, drop, rename. Both tables go in
    one transaction because `job_run` references `pipeline_run` — leaving one
    rebuilt and the other not would be a broken database, not a half-migrated one.
    """
    _backfill_catalog(conn)
    # `started_at` is NOT NULL in the current shape but was nullable in the first
    # one, and a row that never got a start time would fail the copy — a database
    # that refuses to open over one unfinished run from months ago.
    _pipeline_run_select = _PIPELINE_RUN_COLUMNS.replace(
        "started_at",
        f"COALESCE(started_at, finished_at, '{_now_iso()}') AS started_at",
        1,
    )
    conn.executescript(
        f"""
        CREATE TABLE pipeline_run__new (
          id TEXT PRIMARY KEY,
          kind TEXT NOT NULL,
          workflow_id TEXT REFERENCES workflow(id),
          pipeline_id TEXT REFERENCES pipeline(id),
          job_id TEXT REFERENCES job(id),
          name TEXT,
          status TEXT NOT NULL,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          duration_ms INTEGER,
          error TEXT,
          run_as TEXT,
          launched TEXT
        );
        INSERT INTO pipeline_run__new ({_PIPELINE_RUN_COLUMNS})
          SELECT {_pipeline_run_select} FROM pipeline_run;

        CREATE TABLE job_run__new (
          id TEXT PRIMARY KEY,
          pipeline_run_id TEXT NOT NULL REFERENCES pipeline_run(id),
          job_id TEXT REFERENCES job(id),
          name TEXT,
          stage_index INTEGER NOT NULL,
          status TEXT NOT NULL,
          started_at TEXT,
          finished_at TEXT,
          duration_ms INTEGER,
          error TEXT,
          rows_read INTEGER,
          rows_written INTEGER,
          lineage TEXT
        );
        INSERT INTO job_run__new ({_JOB_RUN_COLUMNS})
          SELECT {_JOB_RUN_COLUMNS} FROM job_run;

        DROP TABLE job_run;
        DROP TABLE pipeline_run;
        ALTER TABLE pipeline_run__new RENAME TO pipeline_run;
        ALTER TABLE job_run__new RENAME TO job_run;

        CREATE INDEX IF NOT EXISTS idx_pipeline_run_pipeline
          ON pipeline_run(pipeline_id, started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_pipeline_run_job
          ON pipeline_run(job_id, started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_pipeline_run_workflow
          ON pipeline_run(workflow_id, started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_job_run_pipeline_run
          ON job_run(pipeline_run_id, stage_index);
        """
    )


class SQLiteExecutionRepository:
    """Local `ExecutionRepository`. One short-lived connection per call, guarded by
    a process-wide lock — the write volume here (a handful of rows per run) makes
    that far simpler than pooling, and it sidesteps `database is locked` errors from
    the runner's own log-streaming thread writing while `GET /runs` reads."""

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, closing(self._connect()) as conn:
            # Enforcement stays off for the whole bootstrap: the rebuild below drops
            # tables that other tables reference, which SQLite would otherwise refuse.
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.executescript(_CATALOG_SCHEMA)
            conn.executescript(_SCHEMA)
            _add_missing_columns(conn)
            if int(conn.execute("PRAGMA user_version").fetchone()[0]) < _SCHEMA_VERSION:
                if not _runs_are_linked(conn):
                    _link_runs_to_catalog(conn)
                conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                # The rebuild recreates `job_run` from a fixed column list, so any
                # column added after that list was written is gone again. Cheap to
                # put back, and it only happens once per database.
                _add_missing_columns(conn)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, timeout=5)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    # ---- catalog ---------------------------------------------------------

    def upsert_workflow(
        self, workflow_id: str, *, name: Optional[str] = None,
        description: Optional[str] = None, path: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        with self._lock, closing(self._connect()) as conn:
            self._upsert(conn, "workflow", workflow_id, None, name, description, path)
            if tags is not None:
                self._set_tags(conn, "workflow", workflow_id, tags)
            conn.commit()

    def upsert_job(
        self, job_id: str, *, workflow_id: Optional[str] = None, name: Optional[str] = None,
        description: Optional[str] = None, path: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        with self._lock, closing(self._connect()) as conn:
            self._ensure_workflow(conn, workflow_id)
            self._upsert(conn, "job", job_id, workflow_id, name, description, path)
            if tags is not None:
                self._set_tags(conn, "job", job_id, tags)
            conn.commit()

    def upsert_pipeline(
        self, pipeline_id: str, *, workflow_id: Optional[str] = None,
        name: Optional[str] = None, description: Optional[str] = None,
        path: Optional[str] = None, stages: Optional[List[Dict[str, Any]]] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        with self._lock, closing(self._connect()) as conn:
            self._ensure_workflow(conn, workflow_id)
            self._upsert(conn, "pipeline", pipeline_id, workflow_id, name, description, path)
            if stages is not None:
                self._set_stages(conn, pipeline_id, stages)
            if tags is not None:
                self._set_tags(conn, "pipeline", pipeline_id, tags)
            conn.commit()

    def tags_for(self, kind: str, record_id: str) -> List[str]:
        if kind not in ("workflow", "pipeline", "job"):
            raise ValueError(f"Unknown catalog kind: {kind!r}")
        with self._lock, closing(self._connect()) as conn:
            return self._tags_for(conn, kind, record_id)

    def effective_tags(
        self, *, workflow_id: Optional[str] = None, pipeline_id: Optional[str] = None,
        job_id: Optional[str] = None,
    ) -> List[str]:
        """Every tag that applies to a run: the Job's, plus what it inherits.

        A tag on a Workflow applies to everything inside it — that is the point of
        putting it there, and having to repeat "cost-centre: marketing" on forty
        Jobs would guarantee that one of them ends up missing it and quietly
        untagged on the invoice. The Job's own tags come first, since they are the
        most specific.
        """
        out: List[str] = []
        seen = set()
        with self._lock, closing(self._connect()) as conn:
            for kind, record_id in (
                ("job", job_id), ("pipeline", pipeline_id), ("workflow", workflow_id)
            ):
                if not record_id:
                    continue
                for tag in self._tags_for(conn, kind, record_id):
                    key = tag.casefold()
                    if key not in seen:
                        seen.add(key)
                        out.append(tag)
        return out[:MAX_TAGS]

    def list_tags(self) -> List[Dict[str, Any]]:
        """Every tag in use, with how many records carry it. For a picker."""
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT tag, COUNT(*) AS records FROM catalog_tag "
                "GROUP BY tag ORDER BY records DESC, tag"
            ).fetchall()
        return [{"tag": row["tag"], "records": int(row["records"])} for row in rows]

    def soft_delete(self, kind: str, record_id: str) -> None:
        """Marks a catalog record removed without dropping the row its runs point at."""
        if kind not in ("workflow", "pipeline", "job"):
            raise ValueError(f"Unknown catalog kind: {kind!r}")
        stamp = _now_iso()
        with self._lock, closing(self._connect()) as conn:
            conn.execute(
                f"UPDATE {kind} SET deleted_at=?, updated_at=? WHERE id=?",
                (stamp, stamp, record_id),
            )
            if kind == "workflow":
                # Removing a Workflow removes what belongs to it. Soft, like the
                # parent — the executions of those Jobs are still worth reading.
                for child in ("pipeline", "job"):
                    conn.execute(
                        f"UPDATE {child} SET deleted_at=?, updated_at=? "
                        "WHERE workflow_id=? AND deleted_at IS NULL",
                        (stamp, stamp, record_id),
                    )
            conn.commit()

    def list_catalog(self, *, include_deleted: bool = False) -> List[CatalogRecord]:
        """Every record the catalog knows, Workflows first, then Pipelines, then Jobs."""
        where = "" if include_deleted else " WHERE deleted_at IS NULL"
        records: List[CatalogRecord] = []
        with self._lock, closing(self._connect()) as conn:
            for kind in ("workflow", "pipeline", "job"):
                rows = conn.execute(f"SELECT * FROM {kind}{where} ORDER BY id").fetchall()
                for row in rows:
                    records.append(self._row_to_catalog(row, kind))
            stages = conn.execute(
                "SELECT * FROM pipeline_stage ORDER BY pipeline_id, stage_index"
            ).fetchall()
            tag_rows = conn.execute(
                "SELECT kind, record_id, tag FROM catalog_tag ORDER BY kind, record_id, tag"
            ).fetchall()
        by_pipeline: Dict[str, List[Dict[str, Any]]] = {}
        for row in stages:
            by_pipeline.setdefault(row["pipeline_id"], []).append(
                {"stage_id": row["stage_id"], "job_id": row["job_id"],
                 "stage_index": row["stage_index"]}
            )
        by_record: Dict[tuple, List[str]] = {}
        for row in tag_rows:
            by_record.setdefault((row["kind"], row["record_id"]), []).append(row["tag"])
        for record in records:
            if record.kind == "pipeline":
                record.stages = by_pipeline.get(record.id, [])
            record.tags = by_record.get((record.kind, record.id), [])
        return records

    def ensure_run_targets(
        self, *, workflow_id: Optional[str], pipeline_id: Optional[str],
        job_id: Optional[str], name: Optional[str] = None,
    ) -> None:
        """Makes the ids a run is about exist in the catalog, ahead of the run.

        `create_pipeline_run` does this for itself, so this is for callers that want
        the catalog to name the Job before the first execution of it lands — the run
        panel, which knows the Studio name while the runner only knows the id.
        """
        with self._lock, closing(self._connect()) as conn:
            self._ensure_workflow(conn, workflow_id)
            if job_id:
                self._ensure(conn, "job", job_id, workflow_id, name)
            if pipeline_id:
                self._ensure(conn, "pipeline", pipeline_id, workflow_id, name)
            conn.commit()

    # ---- catalog internals -----------------------------------------------

    @staticmethod
    def _upsert(
        conn: sqlite3.Connection, table: str, record_id: str, workflow_id: Optional[str],
        name: Optional[str], description: Optional[str], path: Optional[str],
    ) -> None:
        """Writes a record the Studio owns. Reviving a soft-deleted id is deliberate:
        the same record came back, and its history belongs to it."""
        stamp = _now_iso()
        columns = "id, name, description, path, created_at, updated_at, deleted_at"
        values: tuple = (record_id, name, description, path, stamp, stamp, None)
        if table != "workflow":
            columns = "id, workflow_id, name, description, path, created_at, updated_at, deleted_at"
            values = (record_id, workflow_id, name, description, path, stamp, stamp, None)
        placeholders = ", ".join("?" * len(values))
        updates = [
            "name=excluded.name", "description=excluded.description",
            "path=excluded.path", "updated_at=excluded.updated_at", "deleted_at=NULL",
        ]
        if table != "workflow":
            updates.insert(0, "workflow_id=excluded.workflow_id")
        conn.execute(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
            f"ON CONFLICT(id) DO UPDATE SET {', '.join(updates)}",
            values,
        )

    @staticmethod
    def _ensure(
        conn: sqlite3.Connection, table: str, record_id: str,
        workflow_id: Optional[str] = None, name: Optional[str] = None,
    ) -> None:
        """Creates a placeholder row if the id is unknown. Never overwrites a real
        one: a run knows an id, the workspace knows what it is called."""
        stamp = _now_iso()
        if table == "workflow":
            conn.execute(
                "INSERT OR IGNORE INTO workflow (id, name, created_at, updated_at) "
                "VALUES (?, ?, ?, ?)",
                (record_id, name, stamp, stamp),
            )
            return
        conn.execute(
            f"INSERT OR IGNORE INTO {table} (id, workflow_id, name, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (record_id, workflow_id, name, stamp, stamp),
        )

    def _ensure_workflow(self, conn: sqlite3.Connection, workflow_id: Optional[str]) -> None:
        if workflow_id:
            self._ensure(conn, "workflow", workflow_id)

    def _set_stages(
        self, conn: sqlite3.Connection, pipeline_id: str, stages: List[Dict[str, Any]],
    ) -> None:
        """Rewrites a Pipeline's stages wholesale — the order is the record, and a
        diff of it would be more code than a replace."""
        conn.execute("DELETE FROM pipeline_stage WHERE pipeline_id=?", (pipeline_id,))
        for index, stage in enumerate(stages):
            job_id = stage.get("job_id") or stage.get("jobId")
            stage_id = stage.get("stage_id") or stage.get("id")
            if not job_id or not stage_id:
                continue
            # A stage can name a Job the workspace has not sent yet (a broken
            # reference the canvas already reports); the placeholder keeps the
            # junction writable so the rest of the Pipeline is still recorded.
            self._ensure(conn, "job", str(job_id))
            conn.execute(
                "INSERT OR REPLACE INTO pipeline_stage "
                "(pipeline_id, stage_id, job_id, stage_index) VALUES (?, ?, ?, ?)",
                (pipeline_id, str(stage_id), str(job_id), index),
            )

    @staticmethod
    def _set_tags(
        conn: sqlite3.Connection, kind: str, record_id: str, tags: Any
    ) -> None:
        conn.execute(
            "DELETE FROM catalog_tag WHERE kind = ? AND record_id = ?", (kind, record_id)
        )
        for tag in normalize_tags(tags):
            conn.execute(
                "INSERT OR REPLACE INTO catalog_tag (kind, record_id, tag) "
                "VALUES (?, ?, ?)",
                (kind, record_id, tag),
            )

    @staticmethod
    def _tags_for(conn: sqlite3.Connection, kind: str, record_id: str) -> List[str]:
        rows = conn.execute(
            "SELECT tag FROM catalog_tag WHERE kind = ? AND record_id = ? ORDER BY tag",
            (kind, record_id),
        ).fetchall()
        return [str(row["tag"]) for row in rows]

    @staticmethod
    def _row_to_catalog(row: sqlite3.Row, kind: str) -> CatalogRecord:
        keys = row.keys()
        return CatalogRecord(
            id=row["id"], kind=kind,
            workflow_id=row["workflow_id"] if "workflow_id" in keys else None,
            name=row["name"], description=row["description"],
            path=row["path"] if "path" in keys else None,
            created_at=row["created_at"], updated_at=row["updated_at"],
            deleted_at=row["deleted_at"],
        )

    # ---- pipeline_run --------------------------------------------------

    def create_pipeline_run(
        self, *, kind: str, workflow_id: Optional[str], pipeline_id: Optional[str],
        job_id: Optional[str], name: Optional[str], run_as: Optional[str] = None,
        launched: str = MANUAL, started_at: Optional[str] = None,
    ) -> str:
        run_id = _new_id()
        with self._lock, closing(self._connect()) as conn:
            # The catalog rows the foreign keys point at, created here rather than
            # left to the caller: history records what happened, and refusing an
            # execution because the catalog was behind would lose it entirely.
            self._ensure_workflow(conn, workflow_id)
            if job_id:
                self._ensure(conn, "job", job_id, workflow_id, name)
            if pipeline_id:
                self._ensure(conn, "pipeline", pipeline_id, workflow_id, name)
            conn.execute(
                "INSERT INTO pipeline_run "
                "(id, kind, workflow_id, pipeline_id, job_id, name, status, started_at, "
                "run_as, launched) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, kind, workflow_id, pipeline_id, job_id, name, RUNNING,
                 started_at or _now_iso(), run_as, launched),
            )
            conn.commit()
        return run_id

    def finish_pipeline_run(
        self, run_id: str, *, status: str, duration_ms: int, error: Optional[str],
        finished_at: Optional[str] = None,
    ) -> None:
        with self._lock, closing(self._connect()) as conn:
            conn.execute(
                "UPDATE pipeline_run SET status=?, finished_at=?, duration_ms=?, error=? "
                "WHERE id=?",
                (status, finished_at or _now_iso(), duration_ms, error, run_id),
            )
            conn.commit()

    # ---- job_run ---------------------------------------------------------

    def create_job_run(
        self, pipeline_run_id: str, *, job_id: Optional[str], name: Optional[str],
        stage_index: int, lineage: Optional[str] = None,
        config_hash: Optional[str] = None, config: Optional[str] = None,
        started_at: Optional[str] = None,
    ) -> str:
        job_run_id = _new_id()
        with self._lock, closing(self._connect()) as conn:
            # `job_run.job_id` points at the catalog too, and a stage can name a
            # Job the workspace never sent. The placeholder keeps the execution
            # recordable; the name arrives with the next save.
            if job_id:
                self._ensure(conn, "job", job_id, None, name)
            conn.execute(
                "INSERT INTO job_run "
                "(id, pipeline_run_id, job_id, name, stage_index, status, started_at, "
                "lineage, config_hash, config) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (job_run_id, pipeline_run_id, job_id, name, stage_index, RUNNING,
                 started_at or _now_iso(), lineage, config_hash, config),
            )
            conn.commit()
        return job_run_id

    def finish_job_run(
        self, job_run_id: str, *, status: str, duration_ms: int,
        error: Optional[str], rows_read: Optional[int], rows_written: Optional[int],
        finished_at: Optional[str] = None,
    ) -> None:
        with self._lock, closing(self._connect()) as conn:
            conn.execute(
                "UPDATE job_run SET status=?, finished_at=?, duration_ms=?, error=?, "
                "rows_read=?, rows_written=? WHERE id=?",
                (status, finished_at or _now_iso(), duration_ms, error, rows_read,
                 rows_written, job_run_id),
            )
            conn.commit()

    def skip_job_run(
        self, pipeline_run_id: str, *, job_id: Optional[str], name: Optional[str],
        stage_index: int, status: str = SKIPPED,
    ) -> str:
        """A stage that never ran. SKIPPED when an earlier stage failed, CANCELLED
        when the user stopped the run — the row exists either way so the stage does
        not sit at "pending" forever."""
        job_run_id = _new_id()
        with self._lock, closing(self._connect()) as conn:
            # `job_run.job_id` points at the catalog too, and a stage can name a
            # Job the workspace never sent. The placeholder keeps the execution
            # recordable; the name arrives with the next save.
            if job_id:
                self._ensure(conn, "job", job_id, None, name)
            conn.execute(
                "INSERT INTO job_run "
                "(id, pipeline_run_id, job_id, name, stage_index, status) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (job_run_id, pipeline_run_id, job_id, name, stage_index, status),
            )
            conn.commit()
        return job_run_id

    def job_config(self, job_run_id: str) -> Optional[JobConfig]:
        """The JSON this execution ran, as it was submitted.

        Kept out of the run listings on purpose: a configuration is orders of
        magnitude larger than the row that describes the run, and a reader wants
        it for one execution at a time. None when there is no such execution;
        a `JobConfig` whose `config` is None when the run predates this column or
        the configuration was too large to keep — `config_hash` still identifies
        it in that case.
        """
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT id, config_hash, config FROM job_run WHERE id = ?",
                (job_run_id,),
            ).fetchone()
        if not row:
            return None
        parsed: Optional[Dict[str, Any]] = None
        if row["config"]:
            try:
                loaded = json.loads(row["config"])
            except json.JSONDecodeError:  # pragma: no cover - written by json.dumps
                loaded = None
            parsed = loaded if isinstance(loaded, dict) else None
        return JobConfig(
            job_run_id=job_run_id, config_hash=row["config_hash"], config=parsed
        )

    # ---- step_run ----------------------------------------------------------

    def create_step_run(
        self, job_run_id: str, scope: str, step_index: int, type_: str, *,
        status: str, timestamp: str, role: Optional[str] = None,
        details: Optional[str] = None,
    ) -> str:
        step_id = _new_id()
        finished_at = timestamp if status == SKIPPED else None
        duration_ms = 0 if status == SKIPPED else None
        with self._lock, closing(self._connect()) as conn:
            conn.execute(
                "INSERT INTO step_run "
                "(id, job_run_id, scope, step_index, type, status, started_at, "
                "finished_at, duration_ms, role, details) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (step_id, job_run_id, scope, step_index, type_, status, timestamp,
                 finished_at, duration_ms, role, details),
            )
            conn.commit()
        return step_id

    def finish_step_run(
        self, step_id: str, *, status: str, timestamp: str,
        error_message: Optional[str], error_details: Optional[str],
        details: Optional[str] = None,
    ) -> None:
        with self._lock, closing(self._connect()) as conn:
            conn.execute(
                "UPDATE step_run SET status=?, finished_at=?, "
                "duration_ms=CAST((julianday(?) - julianday(started_at)) * 86400000 AS INTEGER), "
                "error_message=?, error_details=?, "
                # A step that failed mid-flight reports nothing on the way out, so
                # whatever the opening marker already told us is kept.
                "details=COALESCE(?, details) WHERE id=?",
                (status, timestamp, timestamp, error_message, error_details, details,
                 step_id),
            )
            conn.commit()

    # ---- run_log -------------------------------------------------------------

    def append_logs(self, job_run_id: str, lines: List[Dict[str, Any]]) -> int:
        """Appends log lines to a job execution; returns the last `seq` written.

        The sequence continues from whatever is already stored, so a run flushing in
        batches still reads back in one strict order — timestamps cannot do that job
        alone, since a burst of lines shares the same millisecond.
        """
        if not lines:
            return 0
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS last FROM run_log WHERE job_run_id = ?",
                (job_run_id,),
            ).fetchone()
            seq = int(row["last"])
            payload = []
            for line in lines:
                seq += 1
                context = line.get("context")
                payload.append((
                    job_run_id, seq,
                    str(line.get("timestamp") or _now_iso()),
                    str(line.get("level") or "INFO"),
                    str(line.get("source") or "pipeline"),
                    str(line.get("message") or ""),
                    json.dumps(context) if context else None,
                ))
            conn.executemany(
                "INSERT INTO run_log "
                "(job_run_id, seq, timestamp, level, source, message, context) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                payload,
            )
            conn.commit()
        return seq

    def list_logs(
        self, job_run_id: str, *, after_seq: int = 0, limit: int = 500,
    ) -> List[RunLogLine]:
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM run_log WHERE job_run_id = ? AND seq > ? "
                "ORDER BY seq LIMIT ?",
                (job_run_id, after_seq, limit),
            ).fetchall()
        return [
            RunLogLine(
                job_run_id=row["job_run_id"], seq=row["seq"], timestamp=row["timestamp"],
                level=row["level"], source=row["source"], message=row["message"],
                context=row["context"],
            )
            for row in rows
        ]

    def count_logs(self, job_run_id: str) -> int:
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS total FROM run_log WHERE job_run_id = ?",
                (job_run_id,),
            ).fetchone()
        return int(row["total"])

    # ---- queries -------------------------------------------------------------

    def list_pipeline_runs(
        self, *, workflow_id: Optional[str] = None, pipeline_id: Optional[str] = None,
        job_id: Optional[str] = None, limit: int = 20,
    ) -> List[PipelineRun]:
        clauses: List[str] = []
        params: List[Any] = []
        if workflow_id is not None:
            clauses.append("workflow_id = ?")
            params.append(workflow_id)
        if pipeline_id is not None:
            clauses.append("pipeline_id = ?")
            params.append(pipeline_id)
        if job_id is not None:
            # A Job runs on its own (pipeline_run.job_id) or as one stage of a
            # Studio Pipeline, where the id lives on the nested job_run and the
            # pipeline_run names the Pipeline instead. Both are executions of this
            # Job and both belong in its history — matching only the first left a
            # Job that is always run through a Pipeline showing no history at all.
            clauses.append(
                "(job_id = ? OR EXISTS (SELECT 1 FROM job_run "
                "WHERE job_run.pipeline_run_id = pipeline_run.id "
                "AND job_run.job_id = ?))"
            )
            params.extend((job_id, job_id))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(
                f"SELECT * FROM pipeline_run {where} ORDER BY started_at DESC LIMIT ?",
                (*params, limit),
            ).fetchall()
        return [self._row_to_pipeline_run(row) for row in rows]

    def get_pipeline_run(self, run_id: str) -> Optional[PipelineRun]:
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM pipeline_run WHERE id = ?", (run_id,)
            ).fetchone()
            if row is None:
                return None
            run = self._row_to_pipeline_run(row)
            job_rows = conn.execute(
                "SELECT * FROM job_run WHERE pipeline_run_id = ? ORDER BY stage_index",
                (run_id,),
            ).fetchall()
            jobs = [self._row_to_job_run(job_row) for job_row in job_rows]
            for job in jobs:
                step_rows = conn.execute(
                    # Execution order, not alphabetical order by scope: a reader
                    # follows the run from the read to the writes, and `started_at`
                    # is the only field that knows that sequence. Scope and index
                    # break ties within the same instant.
                    "SELECT * FROM step_run WHERE job_run_id = ? "
                    "ORDER BY COALESCE(started_at, ''), scope, step_index",
                    (job.id,),
                ).fetchall()
                job.steps = [self._row_to_step_run(step_row) for step_row in step_rows]
            run.jobs = jobs
            return run

    # ---- retention -----------------------------------------------------------

    def set_pinned(self, run_id: str, pinned: bool) -> bool:
        """Marks a run as never-purge, or unmarks it. False when there is no such run."""
        with self._lock, closing(self._connect()) as conn:
            cursor = conn.execute(
                "UPDATE pipeline_run SET pinned = ? WHERE id = ?",
                (1 if pinned else 0, run_id),
            )
            conn.commit()
        return cursor.rowcount > 0

    def purge(
        self, policy: "RetentionPolicy", *, dry_run: bool = False,
        now: Optional[datetime] = None,
    ) -> "PurgeReport":
        """Applies the retention policy. See `RetentionPolicy` for the rules.

        `dry_run` counts exactly what would go and touches nothing, which is what
        `POST /runs/purge?dry_run=true` answers with.
        """
        moment = now or datetime.now(timezone.utc)
        thin_before = (moment - timedelta(days=policy.detail_days)).isoformat()
        delete_before = (moment - timedelta(days=policy.max_days)).isoformat()

        with self._lock, closing(self._connect()) as conn:
            protected = self._protected_runs(conn, policy.keep_runs)
            thin_ids = self._runs_older_than(conn, thin_before, protected)
            delete_ids = (
                self._runs_older_than(conn, delete_before, protected)
                if policy.delete else []
            )
            # A run that is being deleted outright is not also "thinned": counting
            # it twice would make the report claim more work than was done.
            thin_ids = [run_id for run_id in thin_ids if run_id not in set(delete_ids)]

            logs = self._count_children(conn, "run_log", thin_ids + delete_ids)
            steps = self._count_children(conn, "step_run", thin_ids + delete_ids)
            configs = self._count_configs(conn, thin_ids)
            report = PurgeReport(
                dry_run=dry_run, runs_thinned=len(thin_ids),
                runs_deleted=len(delete_ids), logs_deleted=logs,
                steps_deleted=steps, configs_dropped=configs,
            )
            if dry_run or not (thin_ids or delete_ids):
                return report

            for chunk in _chunks(thin_ids + delete_ids):
                marks = ",".join("?" * len(chunk))
                conn.execute(
                    f"DELETE FROM run_log WHERE job_run_id IN "
                    f"(SELECT id FROM job_run WHERE pipeline_run_id IN ({marks}))",
                    chunk,
                )
                conn.execute(
                    f"DELETE FROM step_run WHERE job_run_id IN "
                    f"(SELECT id FROM job_run WHERE pipeline_run_id IN ({marks}))",
                    chunk,
                )
            for chunk in _chunks(thin_ids):
                marks = ",".join("?" * len(chunk))
                # The fingerprint stays: it is what tells two runs of an edited Job
                # apart, and it costs 71 bytes against the whole JSON.
                conn.execute(
                    f"UPDATE job_run SET config = NULL WHERE config IS NOT NULL "
                    f"AND pipeline_run_id IN ({marks})",
                    chunk,
                )
            for chunk in _chunks(delete_ids):
                marks = ",".join("?" * len(chunk))
                conn.execute(f"DELETE FROM job_run WHERE pipeline_run_id IN ({marks})", chunk)
                conn.execute(f"DELETE FROM pipeline_run WHERE id IN ({marks})", chunk)
            conn.commit()

            # SQLite does not hand freed pages back to the filesystem on its own:
            # without this the file stops growing but never shrinks. It rewrites
            # the whole database, so it runs only when enough came out to be worth
            # it — and never inside a transaction, hence the commit above.
            if report.rows_removed >= policy.vacuum_after:
                conn.execute("VACUUM")
                report.vacuumed = True
        return report

    @staticmethod
    def _protected_runs(conn: sqlite3.Connection, keep_runs: int) -> set:
        """The runs no age rule may touch: the pinned ones, and the newest
        `keep_runs` of every Job and every Pipeline.

        The second half is what keeps a monthly Job from opening on an empty
        screen: by any age rule alone, a Job that runs twelve times a year loses
        its whole history and looks like it never ran.
        """
        pinned = {
            row["id"] for row in conn.execute(
                "SELECT id FROM pipeline_run WHERE pinned = 1"
            )
        }
        if keep_runs <= 0:
            return pinned
        recent = {
            row["id"] for row in conn.execute(
                """
                SELECT id FROM (
                  SELECT id, ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(pipeline_id, job_id, '')
                    ORDER BY started_at DESC, id DESC
                  ) AS position
                  FROM pipeline_run
                ) WHERE position <= ?
                """,
                (keep_runs,),
            )
        }
        return pinned | recent

    @staticmethod
    def _runs_older_than(
        conn: sqlite3.Connection, before: str, protected: set
    ) -> List[str]:
        rows = conn.execute(
            "SELECT id FROM pipeline_run WHERE started_at IS NOT NULL "
            "AND started_at < ? ORDER BY started_at",
            (before,),
        ).fetchall()
        return [row["id"] for row in rows if row["id"] not in protected]

    @staticmethod
    def _count_children(
        conn: sqlite3.Connection, table: str, run_ids: List[str]
    ) -> int:
        total = 0
        for chunk in _chunks(run_ids):
            marks = ",".join("?" * len(chunk))
            row = conn.execute(
                f"SELECT COUNT(*) AS total FROM {table} WHERE job_run_id IN "
                f"(SELECT id FROM job_run WHERE pipeline_run_id IN ({marks}))",
                chunk,
            ).fetchone()
            total += int(row["total"])
        return total

    @staticmethod
    def _count_configs(conn: sqlite3.Connection, run_ids: List[str]) -> int:
        total = 0
        for chunk in _chunks(run_ids):
            marks = ",".join("?" * len(chunk))
            row = conn.execute(
                f"SELECT COUNT(*) AS total FROM job_run WHERE config IS NOT NULL "
                f"AND pipeline_run_id IN ({marks})",
                chunk,
            ).fetchone()
            total += int(row["total"])
        return total

    @staticmethod
    def _row_to_pipeline_run(row: sqlite3.Row) -> PipelineRun:
        return PipelineRun(
            id=row["id"], kind=row["kind"], workflow_id=row["workflow_id"],
            pipeline_id=row["pipeline_id"], job_id=row["job_id"], name=row["name"],
            status=row["status"], started_at=row["started_at"],
            finished_at=row["finished_at"], duration_ms=row["duration_ms"],
            error=row["error"], run_as=row["run_as"], launched=row["launched"],
            pinned=bool(_column(row, "pinned")),
        )

    @staticmethod
    def _row_to_job_run(row: sqlite3.Row) -> JobRun:
        return JobRun(
            id=row["id"], pipeline_run_id=row["pipeline_run_id"], job_id=row["job_id"],
            name=row["name"], stage_index=row["stage_index"], status=row["status"],
            started_at=row["started_at"], finished_at=row["finished_at"],
            duration_ms=row["duration_ms"], error=row["error"],
            rows_read=row["rows_read"], rows_written=row["rows_written"],
            lineage=row["lineage"], config_hash=_column(row, "config_hash"),
        )

    @staticmethod
    def _row_to_step_run(row: sqlite3.Row) -> StepRun:
        return StepRun(
            id=row["id"], job_run_id=row["job_run_id"], scope=row["scope"],
            step_index=row["step_index"], type=row["type"], status=row["status"],
            started_at=row["started_at"], finished_at=row["finished_at"],
            duration_ms=row["duration_ms"], error_message=row["error_message"],
            error_details=row["error_details"], role=row["role"],
            details=row["details"],
        )


# ------------------------------------------------------------------------ lineage


"""Where a dataset lives, in the order the formats prefer to say it. A reader,
writer or sink names exactly one of these; Kafka hides its topic in `options`,
which is why the lookup falls through to there."""
_ADDRESS_KEYS = ("path", "table", "view", "view_name", "topic", "dbtable", "url", "uri")


def _dataset(entry: Any, role: str) -> Optional[Dict[str, Any]]:
    """One side of the lineage — format, where it points, and (for a write) the mode."""
    if not isinstance(entry, dict):
        return None
    fmt = entry.get("format")
    options = entry.get("options")
    sources: List[Dict[str, Any]] = [entry]
    if isinstance(options, dict):
        sources.append(options)
    address: Optional[str] = None
    for source in sources:
        for key in _ADDRESS_KEYS:
            value = source.get(key)
            if isinstance(value, str) and value:
                address = value
                break
        if address is not None:
            break
    if address is None and not isinstance(fmt, str):
        return None
    dataset: Dict[str, Any] = {
        "role": role,
        "format": str(fmt) if isinstance(fmt, str) else None,
        "address": address,
    }
    mode = entry.get("mode")
    if isinstance(mode, str) and mode:
        dataset["mode"] = mode
    return dataset


def _collect(target: List[Dict[str, Any]], entry: Any, role: str) -> None:
    """Adds `entry` — one dataset or a list of them — to `target`."""
    entries = entry if isinstance(entry, list) else [entry]
    for item in entries:
        dataset = _dataset(item, role)
        if dataset is not None:
            target.append(dataset)


#: A configuration larger than this is fingerprinted but not stored. Half a
#: megabyte is far beyond any hand-written pipeline; something that big is
#: generated, and keeping a copy per execution would grow the database faster
#: than the history it exists to hold.
MAX_STORED_CONFIG_BYTES = 512 * 1024


def _column(row: sqlite3.Row, name: str) -> Optional[Any]:
    """A column that may not exist yet on this database."""
    return row[name] if name in row.keys() else None


def config_version(config: Any) -> Tuple[Optional[str], Optional[str]]:
    """`(fingerprint, text)` for the JSON about to run.

    The history points at a Job, and a Job is edited: without this, two runs a
    month apart are indistinguishable even though they executed different JSON.
    The fingerprint is a sha256 over a canonical rendering — keys sorted,
    whitespace fixed — so it depends on what the configuration *says*, not on how
    it happened to be formatted, and the same JSON always fingerprints the same
    way in the Studio, in git and here.

    The text is the same canonical rendering, or None when it is over
    `MAX_STORED_CONFIG_BYTES`; the fingerprint is returned either way, because
    knowing that two runs used the same configuration is most of the value.
    """
    if not isinstance(config, dict):
        return None, None
    try:
        canonical = json.dumps(config, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):  # pragma: no cover - config arrives from JSON
        return None, None
    encoded = canonical.encode("utf-8")
    digest = "sha256:" + hashlib.sha256(encoded).hexdigest()
    return digest, canonical if len(encoded) <= MAX_STORED_CONFIG_BYTES else None


def lineage_of(config: Any) -> Optional[str]:
    """What a Job reads and what it writes, taken from the JSON submitted to run.

    Read off the configuration rather than off the run's own log records on
    purpose: a run that dies on the first read still says what it was going to
    touch, and that is exactly the run whose lineage a reader wants. Returns the
    JSON to persist, or None when the configuration names no dataset at all.

    A join reads a second dataset, so it belongs on the input side; the quality
    sinks (`validations.report`, `validations.outputs.*`) are writes, and keep
    the role that says which one, since they are targets a reader looks for by
    name.
    """
    if not isinstance(config, dict):
        return None

    inputs: List[Dict[str, Any]] = []
    outputs: List[Dict[str, Any]] = []

    _collect(inputs, config.get("input"), "input")
    _collect(inputs, config.get("inputs"), "input")
    transformations = config.get("transformations")
    if isinstance(transformations, list):
        for step in transformations:
            if isinstance(step, dict) and step.get("type") == "join":
                _collect(inputs, step.get("with"), "join")

    _collect(outputs, config.get("output"), "output")
    _collect(outputs, config.get("outputs"), "output")
    validations = config.get("validations")
    if isinstance(validations, dict):
        _collect(outputs, validations.get("report"), "validation:report")
        sinks = validations.get("outputs")
        if isinstance(sinks, dict):
            for name in ("valid", "invalid"):
                _collect(outputs, sinks.get(name), f"validation:{name}")

    if not inputs and not outputs:
        return None
    return json.dumps({"inputs": inputs, "outputs": outputs})


# --------------------------------------------------------------------- step tracker


"""Context keys that address the step rather than describe it — everything else in
the record is state worth keeping (rows, path, format, whether a rule passed)."""
_CONTROL_KEYS = ("step", "index", "total", "scope", "type", "rule", "role")


class StepTracker:
    """Turns a Job's `step=True` log records (emitted by the framework itself —
    `transform/engine.py`, `validation/engine.py`, `core/pipeline.py`) into
    persisted `step_run` rows, live, as the run happens.

    Each step logs a "started" record then either an "applied"/"finished" record
    (it ran to completion) or nothing more — the exception that fails the job
    interrupts execution mid-step, so `close()` flips whatever is still open into
    FAILED with the job's own error message.

    A marker is addressed in ONE of two ways, never both, and both are tracked:

    - by `index` inside its `scope`'s lane — `input`, `transformation`,
      `output`, `validation`;
    - by `role` — the quality datasets (`scope="validation_sink"`, role
      `report`/`valid`/`invalid`), which take no connection on the canvas and so
      sit in no lane with a position to count. Dropping those (an earlier version
      required an `index`) is what left a job that failed writing its validation
      report with no failed step to point at.
    """

    def __init__(self, repo: ExecutionRepository, job_run_id: str) -> None:
        self._repo = repo
        self._job_run_id = job_run_id
        self._open: Dict[tuple, str] = {}
        # What the opening marker reported, kept until the step closes: a step that
        # dies mid-flight would otherwise persist nothing about itself at all.
        self._details: Dict[tuple, Dict[str, Any]] = {}
        # Arrival order per scope for the role-keyed steps, so the list of a job's
        # steps has a stable order even where no index exists.
        self._lane: Dict[str, int] = {}

    def handle(self, record: Dict[str, Any]) -> None:
        context = record.get("context") or {}
        if not context.get("step"):
            return
        index = context.get("index")
        role = context.get("role")
        if index is None and role is None:
            return
        # Same default the Studio client uses for a marker with no scope, so the
        # persisted row and the live event name the same lane.
        scope = str(context.get("scope") or "transformation")
        role = str(role) if role is not None else None
        key = (scope, index if index is not None else f"role:{role}")
        message = str(record.get("message") or "").lower()
        step_type = str(context.get("type") or context.get("rule") or "")
        timestamp = str(record.get("timestamp") or _now_iso())
        details = {k: v for k, v in context.items() if k not in _CONTROL_KEYS}

        if "skipped" in message:
            self._repo.create_step_run(
                self._job_run_id, scope, self._position(key, index), step_type,
                status=SKIPPED, timestamp=timestamp, role=role,
                details=json.dumps(details) if details else None,
            )
            return

        if message.endswith("started"):
            step_id = self._repo.create_step_run(
                self._job_run_id, scope, self._position(key, index), step_type,
                status=RUNNING, timestamp=timestamp, role=role,
                details=json.dumps(details) if details else None,
            )
            self._open[key] = step_id
            self._details[key] = details
            return

        # "applied" / "written" / "read" / "finished" — the step that opened this
        # (scope, index-or-role) is done.
        step_id = self._open.pop(key, None)
        if step_id is not None:
            merged = {**self._details.pop(key, {}), **details}
            self._repo.finish_step_run(
                step_id, status=SUCCESS, timestamp=timestamp,
                error_message=None,
                error_details=None,
                details=json.dumps(merged) if merged else None,
            )

    def _position(self, key: tuple, index: Optional[Any]) -> int:
        """Where the step sits in its scope's list. The index when there is one;
        otherwise arrival order, since a role has no position of its own."""
        if index is not None:
            return int(index)
        position = self._lane.get(key[0], 0)
        self._lane[key[0]] = position + 1
        return position

    def close(self, error_message: Optional[str], *, status: str = FAILED) -> None:
        """Closes whatever is still open. FAILED by default — the exception that
        ended the job interrupted the step mid-flight — or CANCELLED when the run
        was stopped on purpose, so the box says stopped instead of broken."""
        timestamp = _now_iso()
        for key, step_id in self._open.items():
            details = self._details.get(key)
            self._repo.finish_step_run(
                step_id, status=status, timestamp=timestamp,
                error_message=error_message, error_details=None,
                details=json.dumps(details) if details else None,
            )
        self._open.clear()
        self._details.clear()


# ------------------------------------------------------------------- ingestion


#: Document format this runner knows how to read. The framework stamps it on every
#: submission; a future format bumps the number and is refused here rather than
#: half-read into a run that says the wrong thing.
INGEST_SCHEMA = "sparquet.run/1"

#: Ceiling on the records replayed from one submission. The framework caps what it
#: sends, but the endpoint is reachable by anything holding a token, and one POST
#: must not be able to write an unbounded number of rows.
MAX_INGEST_RECORDS = 5000

_INGEST_STATUS = {
    "success": SUCCESS,
    "failed": FAILED,
    "skipped": SKIPPED,
    "cancelled": CANCELLED,
}


class IngestError(ValueError):
    """The submitted document cannot be recorded as a run."""


def _duration_ms(started_at: Optional[str], finished_at: Optional[str]) -> int:
    """How long the run took, from its own clock — not from ours.

    The run happened on another machine, possibly hours ago; measuring from the
    moment it arrived here would record the delay of the report, not the work.
    """
    if not started_at or not finished_at:
        return 0
    try:
        start = datetime.fromisoformat(started_at)
        end = datetime.fromisoformat(finished_at)
    except (TypeError, ValueError):
        return 0
    return max(0, int((end - start).total_seconds() * 1000))


def _ingest_int(value: Any) -> Optional[int]:
    """A row count, or None. The payload comes from another process: a string
    where a number belongs must not become a row count nobody can compare."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ingest_records(document: Dict[str, Any]) -> List[Dict[str, Any]]:
    records = document.get("records")
    if not isinstance(records, list):
        return []
    return [r for r in records[:MAX_INGEST_RECORDS] if isinstance(r, dict)]


def ingest_run(repo: ExecutionRepository, document: Dict[str, Any]) -> Dict[str, Any]:
    """Records a run this runner did not execute.

    The framework can report its own runs (`sparquet.observability.history`), so a
    pipeline running under `sparquet.cli`, Airflow, Databricks or EMR lands in the
    same history as one launched from the Studio — which is where monitoring is
    actually wanted, and where it did not exist.

    The submission carries the very records the framework already emits, so they
    are replayed through the same `StepTracker` and stored through the same
    `append_logs` that a local run uses. One code path, one shape: an external run
    and a local one read back identically, and a change to how steps are derived
    cannot drift between the two.

    Timestamps come from the document, not from the clock here: the run is being
    reported after the fact, and recording the arrival time would put the whole
    history of a nightly job at the hour its report reached the runner.

    Raises `IngestError` when the document is not a run this runner can read.
    """
    if not isinstance(document, dict):
        raise IngestError("The submitted document is not an object")
    schema = str(document.get("schema") or "")
    if schema != INGEST_SCHEMA:
        raise IngestError(
            f"Unsupported document schema {schema!r}; this runner reads {INGEST_SCHEMA!r}"
        )
    run = document.get("run")
    if not isinstance(run, dict):
        raise IngestError("The submitted document carries no run")

    name = run.get("name")
    started_at = str(run.get("started_at") or _now_iso())
    finished_at = str(run.get("finished_at") or _now_iso())
    # An unknown status is stored as it came: history describes what happened, it
    # does not police it — the same rule the launch kinds follow.
    raw_status = str(run.get("status") or FAILED)
    status = _INGEST_STATUS.get(raw_status, raw_status)
    error = run.get("error")

    pipeline_run_id = repo.create_pipeline_run(
        kind="job",
        workflow_id=run.get("workflow_id"),
        pipeline_id=run.get("pipeline_id"),
        job_id=run.get("job_id"),
        name=name,
        run_as=run.get("run_as"),
        launched=EXTERNAL,
        started_at=started_at,
    )
    job_run_id = repo.create_job_run(
        pipeline_run_id,
        job_id=run.get("job_id"),
        name=name,
        stage_index=0,
        started_at=started_at,
    )

    records = _ingest_records(document)
    tracker = StepTracker(repo, job_run_id)
    for record in records:
        tracker.handle(record)
    # Whatever the framework did not close, the run's own ending closes — the same
    # rule a local run follows, and the reason a job that died mid-write still has
    # a failed step to point at instead of one stuck at "running".
    tracker.close(
        str(error) if error else None,
        status=status if status in (FAILED, CANCELLED, SKIPPED) else SUCCESS,
    )

    stored = 0
    if records:
        stored = len(records)
        repo.append_logs(
            job_run_id,
            [{**record, "source": "pipeline"} for record in records],
        )

    duration_ms = _duration_ms(started_at, finished_at)
    repo.finish_job_run(
        job_run_id,
        status=status,
        duration_ms=duration_ms,
        error=str(error) if error else None,
        rows_read=_ingest_int(run.get("rows_read")),
        rows_written=_ingest_int(run.get("rows_written")),
        finished_at=finished_at,
    )
    repo.finish_pipeline_run(
        pipeline_run_id,
        status=status,
        duration_ms=duration_ms,
        error=str(error) if error else None,
        finished_at=finished_at,
    )
    return {
        "pipeline_run_id": pipeline_run_id,
        "job_run_id": job_run_id,
        "records": stored,
        "duration_ms": duration_ms,
    }
