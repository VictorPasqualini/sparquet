"""Audit log: who changed what, when, and whether the runner let them.

Separate from every other store on purpose, and for a different reason than the
others. Execution history answers "what did this Job do"; the audit log answers
"who touched this installation". They are read by different people, kept for
different lengths of time, and the second one must survive the first being
purged — so it gets its own file (`server/data/audit.sqlite3`, override with
`SPARQUET_STUDIO_AUDIT_DB`).

Two properties matter more than features here:

* **Append-only.** There is no update and no delete in this module's API. An
  audit log an administrator can edit is not evidence of anything.
* **It records the refusals too.** A log with only the successful changes hides
  exactly the interesting case: somebody probing what they are not allowed to
  do. Every `401` and `403` is recorded, on any method.

What is deliberately *not* recorded: passwords, recovery codes, session tokens
and pipeline bodies. The log says a password was changed, never to what; it says
a Job was saved, never the SQL inside it. A log that copies secrets turns into a
second place to steal them from.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def default_db_path() -> Path:
    configured = os.getenv("SPARQUET_STUDIO_AUDIT_DB", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parent / "data" / "audit.sqlite3"


_SCHEMA = """
CREATE TABLE IF NOT EXISTS audit (
  id TEXT PRIMARY KEY,
  at TEXT NOT NULL,
  actor TEXT NOT NULL,
  actor_id TEXT,
  team TEXT,
  team_id TEXT,
  roles TEXT,
  action TEXT NOT NULL,
  method TEXT NOT NULL,
  path TEXT NOT NULL,
  resource TEXT,
  outcome TEXT NOT NULL,
  status INTEGER,
  detail TEXT,
  ip TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_at ON audit(at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit(actor_id, at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_resource ON audit(resource, at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_outcome ON audit(outcome, at DESC);
"""

#: Outcomes an entry can carry. `denied` is a refusal by policy or by a missing
#: session; `failed` is the runner itself answering 5xx.
ALLOWED = "allowed"
DENIED = "denied"
FAILED = "failed"


@dataclass
class Event:
    """One thing that happened, as it will be read back."""

    id: str
    at: str
    actor: str
    action: str
    method: str
    path: str
    outcome: str
    actor_id: Optional[str] = None
    team: Optional[str] = None
    team_id: Optional[str] = None
    roles: List[str] = field(default_factory=list)
    resource: Optional[str] = None
    status: Optional[int] = None
    detail: Optional[Dict[str, Any]] = None
    ip: Optional[str] = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class AuditStore:
    """The log, in SQLite. Safe to share across threads."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.path = Path(db_path) if db_path else default_db_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with closing(self._connect()) as conn:
            conn.executescript(_SCHEMA)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    # ---- writing ---------------------------------------------------------

    def record(
        self,
        *,
        actor: str,
        action: str,
        method: str,
        path: str,
        outcome: str,
        actor_id: Optional[str] = None,
        team: Optional[str] = None,
        team_id: Optional[str] = None,
        roles: Optional[List[str]] = None,
        resource: Optional[str] = None,
        status: Optional[int] = None,
        detail: Optional[Dict[str, Any]] = None,
        ip: Optional[str] = None,
    ) -> str:
        """Append one event and return its id.

        Never raises on a full disk or a locked database: an audit write that can
        take the request down with it would make the log a liability rather than a
        safeguard. A failure to record is swallowed here and shows up as a gap.
        """
        event_id = uuid.uuid4().hex
        try:
            with self._lock, closing(self._connect()) as conn:
                conn.execute(
                    "INSERT INTO audit (id, at, actor, actor_id, team, team_id, roles, "
                    "action, method, path, resource, outcome, status, detail, ip) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event_id,
                        _now_iso(),
                        actor,
                        actor_id,
                        team,
                        team_id,
                        json.dumps(roles or []),
                        action,
                        method,
                        path,
                        resource,
                        outcome,
                        status,
                        json.dumps(detail) if detail else None,
                        ip,
                    ),
                )
                conn.commit()
        except sqlite3.Error:
            return ""
        return event_id

    # ---- reading ---------------------------------------------------------

    def list(
        self,
        *,
        limit: int = 100,
        actor_id: Optional[str] = None,
        resource: Optional[str] = None,
        outcome: Optional[str] = None,
        action: Optional[str] = None,
        since: Optional[str] = None,
    ) -> List[Event]:
        """The newest events first, narrowed by whichever filters are given."""
        limit = max(1, min(int(limit), 1000))
        clauses: List[str] = []
        params: List[Any] = []
        if actor_id:
            clauses.append("actor_id = ?")
            params.append(actor_id)
        if resource:
            clauses.append("resource = ?")
            params.append(resource)
        if outcome:
            clauses.append("outcome = ?")
            params.append(outcome)
        if action:
            # A prefix ending in `:*` reads as the whole service, the way a policy
            # statement does — asking for "everything IAM did" is the common case.
            if action.endswith(":*"):
                clauses.append("action LIKE ?")
                params.append(action[:-1] + "%")
            else:
                clauses.append("action = ?")
                params.append(action)
        if since:
            clauses.append("at >= ?")
            params.append(since)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        with closing(self._connect()) as conn:
            rows = conn.execute(
                f"SELECT * FROM audit {where} ORDER BY at DESC, rowid DESC LIMIT ?",
                params,
            ).fetchall()
        return [self._event_of(row) for row in rows]

    def count(self) -> int:
        with closing(self._connect()) as conn:
            return int(conn.execute("SELECT COUNT(*) FROM audit").fetchone()[0])

    @staticmethod
    def _event_of(row: sqlite3.Row) -> Event:
        try:
            roles = json.loads(row["roles"] or "[]")
        except (ValueError, TypeError):
            roles = []
        detail = None
        if row["detail"]:
            try:
                detail = json.loads(row["detail"])
            except ValueError:
                detail = {"raw": row["detail"]}
        return Event(
            id=row["id"],
            at=row["at"],
            actor=row["actor"],
            actor_id=row["actor_id"],
            team=row["team"],
            team_id=row["team_id"],
            roles=roles if isinstance(roles, list) else [],
            action=row["action"],
            method=row["method"],
            path=row["path"],
            resource=row["resource"],
            outcome=row["outcome"],
            status=row["status"],
            detail=detail,
            ip=row["ip"],
        )


#: Paths that are never worth an entry: health probes and the polling the UI does
#: while a run is on screen. They are reads, they happen constantly, and letting
#: them in would bury the events somebody actually needs to find.
_QUIET_PREFIXES = ("/health", "/capabilities", "/docs", "/openapi", "/favicon")


def is_quiet(path: str) -> bool:
    return path.startswith(_QUIET_PREFIXES)


def action_for(method: str, path: str) -> str:
    """The action to file a request under, derived from where it landed.

    The runner already declares an action per route through `requires(...)`, but
    a request refused before reaching the route never gets that far — and those
    are exactly the ones worth recording. Deriving from the path keeps the log
    complete, at the cost of being coarser than the policy itself.
    """
    parts = [p for p in path.split("/") if p]
    head = parts[0] if parts else ""
    verb = {
        "GET": "Read",
        "HEAD": "Read",
        "POST": "Write",
        "PUT": "Write",
        "PATCH": "Write",
        "DELETE": "Delete",
    }.get(method.upper(), method.upper())
    service = {
        "auth": "iam",
        "credits": "credits",
        "workspace": "workspace",
        "run": "run",
        "runs": "history",
        "job-runs": "history",
        "audit": "audit",
    }.get(head, head or "http")
    if service == "run":
        return "run:Execute"
    return f"{service}:{verb}"
