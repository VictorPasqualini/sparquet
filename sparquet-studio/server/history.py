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
import sqlite3
import threading
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
LAUNCH_KINDS = (MANUAL, SCHEDULED, API)


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


# --------------------------------------------------------------------- repository


class ExecutionRepository(Protocol):
    # ---- catalog: what exists
    def upsert_workflow(
        self, workflow_id: str, *, name: Optional[str] = None,
        description: Optional[str] = None, path: Optional[str] = None,
    ) -> None: ...

    def upsert_job(
        self, job_id: str, *, workflow_id: Optional[str] = None, name: Optional[str] = None,
        description: Optional[str] = None, path: Optional[str] = None,
    ) -> None: ...

    def upsert_pipeline(
        self, pipeline_id: str, *, workflow_id: Optional[str] = None,
        name: Optional[str] = None, description: Optional[str] = None,
        path: Optional[str] = None, stages: Optional[List[Dict[str, Any]]] = None,
    ) -> None: ...

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
        launched: str = MANUAL,
    ) -> str: ...

    def finish_pipeline_run(
        self, run_id: str, *, status: str, duration_ms: int, error: Optional[str],
    ) -> None: ...

    def create_job_run(
        self, pipeline_run_id: str, *, job_id: Optional[str], name: Optional[str],
        stage_index: int, lineage: Optional[str] = None,
        config_hash: Optional[str] = None, config: Optional[str] = None,
    ) -> str: ...

    def job_config(self, job_run_id: str) -> Optional[JobConfig]: ...

    def finish_job_run(
        self, job_run_id: str, *, status: str, duration_ms: int,
        error: Optional[str], rows_read: Optional[int], rows_written: Optional[int],
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
    "pipeline_run": ("run_as", "launched"),
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
    ) -> None:
        with self._lock, closing(self._connect()) as conn:
            self._upsert(conn, "workflow", workflow_id, None, name, description, path)
            conn.commit()

    def upsert_job(
        self, job_id: str, *, workflow_id: Optional[str] = None, name: Optional[str] = None,
        description: Optional[str] = None, path: Optional[str] = None,
    ) -> None:
        with self._lock, closing(self._connect()) as conn:
            self._ensure_workflow(conn, workflow_id)
            self._upsert(conn, "job", job_id, workflow_id, name, description, path)
            conn.commit()

    def upsert_pipeline(
        self, pipeline_id: str, *, workflow_id: Optional[str] = None,
        name: Optional[str] = None, description: Optional[str] = None,
        path: Optional[str] = None, stages: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        with self._lock, closing(self._connect()) as conn:
            self._ensure_workflow(conn, workflow_id)
            self._upsert(conn, "pipeline", pipeline_id, workflow_id, name, description, path)
            if stages is not None:
                self._set_stages(conn, pipeline_id, stages)
            conn.commit()

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
        by_pipeline: Dict[str, List[Dict[str, Any]]] = {}
        for row in stages:
            by_pipeline.setdefault(row["pipeline_id"], []).append(
                {"stage_id": row["stage_id"], "job_id": row["job_id"],
                 "stage_index": row["stage_index"]}
            )
        for record in records:
            if record.kind == "pipeline":
                record.stages = by_pipeline.get(record.id, [])
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
        launched: str = MANUAL,
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
                 _now_iso(), run_as, launched),
            )
            conn.commit()
        return run_id

    def finish_pipeline_run(
        self, run_id: str, *, status: str, duration_ms: int, error: Optional[str],
    ) -> None:
        with self._lock, closing(self._connect()) as conn:
            conn.execute(
                "UPDATE pipeline_run SET status=?, finished_at=?, duration_ms=?, error=? "
                "WHERE id=?",
                (status, _now_iso(), duration_ms, error, run_id),
            )
            conn.commit()

    # ---- job_run ---------------------------------------------------------

    def create_job_run(
        self, pipeline_run_id: str, *, job_id: Optional[str], name: Optional[str],
        stage_index: int, lineage: Optional[str] = None,
        config_hash: Optional[str] = None, config: Optional[str] = None,
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
                 _now_iso(), lineage, config_hash, config),
            )
            conn.commit()
        return job_run_id

    def finish_job_run(
        self, job_run_id: str, *, status: str, duration_ms: int,
        error: Optional[str], rows_read: Optional[int], rows_written: Optional[int],
    ) -> None:
        with self._lock, closing(self._connect()) as conn:
            conn.execute(
                "UPDATE job_run SET status=?, finished_at=?, duration_ms=?, error=?, "
                "rows_read=?, rows_written=? WHERE id=?",
                (status, _now_iso(), duration_ms, error, rows_read, rows_written, job_run_id),
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

    @staticmethod
    def _row_to_pipeline_run(row: sqlite3.Row) -> PipelineRun:
        return PipelineRun(
            id=row["id"], kind=row["kind"], workflow_id=row["workflow_id"],
            pipeline_id=row["pipeline_id"], job_id=row["job_id"], name=row["name"],
            status=row["status"], started_at=row["started_at"],
            finished_at=row["finished_at"], duration_ms=row["duration_ms"],
            error=row["error"], run_as=row["run_as"], launched=row["launched"],
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
