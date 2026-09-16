"""Monitors: the rules that turn execution history into an alert.

The history already knows everything an alert needs — when each Job ran, how long
it took, how many rows it wrote, whether it failed. What it does not do is *tell
anybody*, and the gap that leaves is not the failed run: a failed run is visible
the moment somebody opens the Studio. The gap is the run that never started, the
one that finished in four seconds because the source was empty, and the one that
took an hour when it always takes five minutes. Nobody opens a screen to look for
those, because there is nothing to look at.

So a monitor is a question asked *of the history*, on a timer, by the runner:

* **failed** — the last run of this Job failed (or the last N did).
* **late** — no successful run finished in the last N minutes. The only one of the
  four that fires when nothing happened, which is why it needs the timer rather
  than only the end of a run.
* **duration** — the last run took longer than a ceiling, or longer than a
  multiple of what this Job usually takes.
* **volume** — the last run wrote fewer rows than a floor, or than a fraction of
  what this Job usually writes.

Two of those take an absolute number and two take a comparison against the Job's
own past, so the threshold is a pair: a number and a `baseline` saying how to read
it (`absolute` or `median`). A median over the Job's own recent runs is what makes
a rule worth writing once — `duration > 2x median` is true of every Job in the
library and needs no tuning per Job, while `duration > 1800000ms` has to be set,
and re-set, for each one.

Evaluation is a pure function of a rule and the Job's facts (`evaluate`), which is
what lets it be tested without a database, a clock or a Spark session. Everything
stateful lives in `MonitorStore`: the rules, the current state of each, and the
transitions. The state matters as much as the verdict — an alert that notifies on
every check is an alert somebody mutes on the second day, so a notification is
sent when a monitor *changes*: ok to firing, and firing back to ok.

Storage is its own SQLite file (`server/data/monitors.sqlite3`, override with
`SPARQUET_STUDIO_MONITORS_DB`), for the same reason the audit log has one: these
are configuration, and the execution history they read is purged on a schedule.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import urllib.error
import urllib.request
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

_log = logging.getLogger("sparquet.studio.monitoring")

# ------------------------------------------------------------------ vocabulary

FAILED = "failed"
LATE = "late"
DURATION = "duration"
VOLUME = "volume"
KINDS = (FAILED, LATE, DURATION, VOLUME)

#: How the threshold reads. `absolute` is the number itself — milliseconds for
#: `duration`, rows for `volume`; `median` is a multiple of the median of the
#: Job's own recent runs.
ABSOLUTE = "absolute"
MEDIAN = "median"
BASELINES = (ABSOLUTE, MEDIAN)

#: Every Job, including the ones added after the rule was written. The point of
#: the wildcard: "tell me when anything fails" is the first rule anybody wants,
#: and a per-Job version of it is a rule somebody has to remember to add.
ANY_JOB = "*"

#: How many past runs a median is taken over when the rule does not say.
DEFAULT_WINDOW = 10


def default_db_path() -> Path:
    configured = os.getenv("SPARQUET_STUDIO_MONITORS_DB", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parent / "data" / "monitors.sqlite3"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Reads a timestamp the history wrote, whatever shape it wrote it in.

    The history has two writers — this runner and `sparquet.observability`, which
    reports runs executed elsewhere — and they do not agree on the spelling of
    UTC. A timestamp that cannot be read is `None` rather than an exception: a
    monitor is not the place to discover that one row is malformed.
    """
    if not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def median(values: Sequence[float]) -> Optional[float]:
    """The middle of the samples, or `None` when there are none.

    The median rather than the mean, and this is the whole reason the baseline is
    worth having: one run that took twenty minutes because a cluster was busy
    moves a mean enough to hide the next one, and moves a median not at all.
    """
    ordered = sorted(values)
    if not ordered:
        return None
    middle = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return float(ordered[middle])
    return (float(ordered[middle - 1]) + float(ordered[middle])) / 2.0


# ----------------------------------------------------------------- the records


@dataclass
class Monitor:
    """One rule. `job_id` is a Job, or `*` for every Job in the library."""

    id: str
    kind: str
    job_id: str = ANY_JOB
    threshold: float = 1.0
    baseline: str = ABSOLUTE
    window: int = DEFAULT_WINDOW
    enabled: bool = True
    name: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""

    def describe(self) -> str:
        """The rule in words, for the notification and for the interface."""
        where = "any Job" if self.job_id == ANY_JOB else f"Job {self.job_id}"
        if self.kind == FAILED:
            times = int(self.threshold)
            runs = "run" if times == 1 else "consecutive runs"
            return f"{where}: last {times if times > 1 else ''} {runs} failed".replace("  ", " ")
        if self.kind == LATE:
            return f"{where}: no successful run in {int(self.threshold)} minutes"
        if self.kind == DURATION:
            if self.baseline == MEDIAN:
                return f"{where}: last run took more than {self.threshold:g}x its median"
            return f"{where}: last run took more than {int(self.threshold)} ms"
        if self.kind == VOLUME:
            if self.baseline == MEDIAN:
                return f"{where}: last run wrote less than {self.threshold:g}x its median"
            return f"{where}: last run wrote fewer than {int(self.threshold)} rows"
        return f"{where}: {self.kind}"


@dataclass
class JobFacts:
    """What the history knows about one Job, as a monitor needs to read it.

    Built by `history.job_health`. Kept as a plain record rather than a database
    cursor so that `evaluate` can be handed a made-up one in a test.
    """

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
    #: Successful runs before the last one, newest first. The run being judged is
    #: never part of the baseline it is judged against.
    durations: List[int] = field(default_factory=list)
    volumes: List[int] = field(default_factory=list)


@dataclass
class Verdict:
    """The answer for one rule on one Job."""

    firing: bool
    reason: str
    value: Optional[float] = None
    baseline: Optional[float] = None
    run_id: Optional[str] = None


@dataclass
class MonitorState:
    """What a rule was last seen doing, per Job it watches."""

    monitor_id: str
    job_id: str
    firing: bool
    reason: str
    since: Optional[str] = None
    checked_at: Optional[str] = None
    value: Optional[float] = None
    baseline: Optional[float] = None
    run_id: Optional[str] = None


@dataclass
class MonitorEvent:
    """A transition. Only transitions are recorded — see the module docstring."""

    id: str
    monitor_id: str
    job_id: str
    at: str
    firing: bool
    reason: str
    value: Optional[float] = None
    baseline: Optional[float] = None
    run_id: Optional[str] = None


# ------------------------------------------------------------------ evaluation

#: Statuses `history` uses. Imported by value rather than from `history` to keep
#: this module loadable on its own, which is what makes it testable on its own.
_SUCCESS = "success"
_FAILED = "failed"


def evaluate(monitor: Monitor, facts: JobFacts, now: Optional[datetime] = None) -> Verdict:
    """Does this rule fire for this Job, right now?

    Pure: same rule and same facts, same answer. `now` is passed in rather than
    read, because `late` is the one rule whose answer depends on the clock and a
    test that has to wait ten minutes is a test nobody runs.

    A Job with no runs at all never fires. That is deliberate for all four kinds,
    `late` included: a Job created this morning and not yet run is not an
    incident, and a monitor that treats it as one fires for every new Job in the
    library before anybody has finished writing it.
    """
    if monitor.kind == FAILED:
        return _evaluate_failed(monitor, facts)
    if monitor.kind == LATE:
        return _evaluate_late(monitor, facts, now or datetime.now(timezone.utc))
    if monitor.kind == DURATION:
        return _evaluate_duration(monitor, facts)
    if monitor.kind == VOLUME:
        return _evaluate_volume(monitor, facts)
    return Verdict(firing=False, reason=f"Unknown monitor kind '{monitor.kind}'.")


def _evaluate_failed(monitor: Monitor, facts: JobFacts) -> Verdict:
    needed = max(1, int(monitor.threshold or 1))
    seen = facts.consecutive_failures
    if seen >= needed:
        runs = "run" if seen == 1 else "runs"
        return Verdict(
            firing=True,
            reason=f"{seen} consecutive failed {runs}. Last error: {_short(facts.last_error)}",
            value=float(seen),
            baseline=float(needed),
            run_id=facts.last_run_id,
        )
    return Verdict(
        firing=False,
        reason="The last run did not fail." if seen == 0 else f"{seen} of {needed} failures.",
        value=float(seen),
        baseline=float(needed),
        run_id=facts.last_run_id,
    )


def _evaluate_late(monitor: Monitor, facts: JobFacts, now: datetime) -> Verdict:
    limit = max(1.0, float(monitor.threshold or 1))
    last = _parse_iso(facts.last_success_at)
    if last is None:
        # Never succeeded. If it never ran either, there is nothing to be late
        # for; if it ran and failed, that is what the `failed` rule is for, and
        # reporting it twice under two names makes both worth less.
        return Verdict(
            firing=False,
            reason="No successful run on record yet.",
            baseline=limit,
            run_id=facts.last_run_id,
        )
    minutes = (now - last).total_seconds() / 60.0
    if minutes > limit:
        return Verdict(
            firing=True,
            reason=(
                f"No successful run for {int(minutes)} minutes; "
                f"the rule allows {int(limit)}."
            ),
            value=round(minutes, 1),
            baseline=limit,
            run_id=facts.last_run_id,
        )
    return Verdict(
        firing=False,
        reason=f"Last success {int(minutes)} minutes ago.",
        value=round(minutes, 1),
        baseline=limit,
        run_id=facts.last_run_id,
    )


def _evaluate_duration(monitor: Monitor, facts: JobFacts) -> Verdict:
    if facts.last_status != _SUCCESS or facts.last_duration_ms is None:
        # Judging the duration of a failed run says nothing: it stopped early or
        # hung, and either way `failed` already fired.
        return Verdict(firing=False, reason="No successful run to measure.", run_id=facts.last_run_id)
    took = float(facts.last_duration_ms)
    ceiling = _ceiling(monitor, facts.durations)
    if ceiling is None:
        return Verdict(
            firing=False,
            reason="Not enough past runs to compare against yet.",
            value=took,
            run_id=facts.last_run_id,
        )
    if took > ceiling:
        return Verdict(
            firing=True,
            reason=f"Last run took {int(took)} ms, over the {int(ceiling)} ms this rule allows.",
            value=took,
            baseline=ceiling,
            run_id=facts.last_run_id,
        )
    return Verdict(
        firing=False,
        reason=f"Last run took {int(took)} ms.",
        value=took,
        baseline=ceiling,
        run_id=facts.last_run_id,
    )


def _evaluate_volume(monitor: Monitor, facts: JobFacts) -> Verdict:
    if facts.last_status != _SUCCESS or facts.last_rows_written is None:
        return Verdict(firing=False, reason="No successful run to measure.", run_id=facts.last_run_id)
    wrote = float(facts.last_rows_written)
    floor = _ceiling(monitor, facts.volumes)
    if floor is None:
        return Verdict(
            firing=False,
            reason="Not enough past runs to compare against yet.",
            value=wrote,
            run_id=facts.last_run_id,
        )
    if wrote < floor:
        return Verdict(
            firing=True,
            reason=f"Last run wrote {int(wrote)} rows, under the {int(floor)} this rule expects.",
            value=wrote,
            baseline=floor,
            run_id=facts.last_run_id,
        )
    return Verdict(
        firing=False,
        reason=f"Last run wrote {int(wrote)} rows.",
        value=wrote,
        baseline=floor,
        run_id=facts.last_run_id,
    )


def _ceiling(monitor: Monitor, samples: Sequence[int]) -> Optional[float]:
    """The number the last run is compared against, whichever way the rule reads.

    `None` means "do not judge": an absolute rule with no threshold is not a rule,
    and a median rule on a Job with no history has nothing to be typical of. Both
    answer that they cannot tell rather than guessing, because a monitor that
    fires on its first day is a monitor that gets turned off on its second.
    """
    if monitor.baseline == MEDIAN:
        window = max(1, int(monitor.window or DEFAULT_WINDOW))
        middle = median([float(value) for value in samples[:window]])
        if middle is None:
            return None
        return middle * float(monitor.threshold or 1)
    threshold = float(monitor.threshold or 0)
    return threshold if threshold > 0 else None


def _short(text: Optional[str], limit: int = 160) -> str:
    if not text:
        return "(none)"
    flat = " ".join(str(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


# --------------------------------------------------------------- notification


def webhook_url(env: Optional[Dict[str, str]] = None) -> str:
    source = env if env is not None else os.environ
    return (source.get("SPARQUET_STUDIO_ALERT_WEBHOOK") or "").strip()


def notify(monitor: Monitor, state: MonitorState, *, url: str = "", timeout: float = 5.0) -> bool:
    """Posts one transition to the configured webhook.

    JSON to a URL, which is what Slack, Teams, Opsgenie and a three-line script
    all accept — deliberately not an integration with any of them, because an
    integration is a dependency and a credential per destination.

    Never raises. An alert channel that can take the runner down with it is worse
    than no alert channel: the moment it matters is an incident, and an incident
    is exactly when the other end is unreachable.
    """
    target = url or webhook_url()
    if not target:
        return False
    payload = {
        "monitor": monitor.id,
        "name": monitor.name or monitor.describe(),
        "kind": monitor.kind,
        "job_id": state.job_id,
        "firing": state.firing,
        "reason": state.reason,
        "value": state.value,
        "baseline": state.baseline,
        "run_id": state.run_id,
        "at": state.checked_at,
        "rule": monitor.describe(),
        # Written out so a webhook that shows one field shows a useful one.
        "text": f"[{'FIRING' if state.firing else 'RESOLVED'}] {monitor.name or monitor.describe()} — {state.reason}",
    }
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        target, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # nosec B310
            return 200 <= getattr(response, "status", 200) < 300
    except (urllib.error.URLError, OSError, ValueError) as exc:
        _log.warning("Alert webhook failed: %s", exc)
        return False


# ---------------------------------------------------------------- the store


_SCHEMA = """
CREATE TABLE IF NOT EXISTS monitor (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  job_id TEXT NOT NULL,
  threshold REAL NOT NULL,
  baseline TEXT NOT NULL,
  window INTEGER NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  name TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

/* One row per (rule, Job) — a wildcard rule has as many as there are Jobs. The
   state is what makes a transition detectable, and a transition is the only
   thing worth notifying about. */
CREATE TABLE IF NOT EXISTS monitor_state (
  monitor_id TEXT NOT NULL REFERENCES monitor(id) ON DELETE CASCADE,
  job_id TEXT NOT NULL,
  firing INTEGER NOT NULL,
  reason TEXT NOT NULL,
  since TEXT,
  checked_at TEXT,
  value REAL,
  baseline REAL,
  run_id TEXT,
  PRIMARY KEY (monitor_id, job_id)
);

CREATE TABLE IF NOT EXISTS monitor_event (
  id TEXT PRIMARY KEY,
  monitor_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  at TEXT NOT NULL,
  firing INTEGER NOT NULL,
  reason TEXT NOT NULL,
  value REAL,
  baseline REAL,
  run_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_monitor_event_at ON monitor_event(at DESC);
CREATE INDEX IF NOT EXISTS idx_monitor_event_monitor
  ON monitor_event(monitor_id, at DESC);
"""


class MonitorStore:
    """The rules and their state, in SQLite. Safe to share across threads."""

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
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    # ---- rules -----------------------------------------------------------

    def create(
        self,
        kind: str,
        *,
        job_id: str = ANY_JOB,
        threshold: float = 1.0,
        baseline: str = ABSOLUTE,
        window: int = DEFAULT_WINDOW,
        enabled: bool = True,
        name: Optional[str] = None,
    ) -> Monitor:
        if kind not in KINDS:
            raise ValueError(f"Unknown monitor kind '{kind}'. Known: {', '.join(KINDS)}.")
        if baseline not in BASELINES:
            raise ValueError(f"Unknown baseline '{baseline}'. Known: {', '.join(BASELINES)}.")
        now = _now_iso()
        monitor = Monitor(
            id=uuid.uuid4().hex,
            kind=kind,
            job_id=job_id or ANY_JOB,
            threshold=float(threshold),
            baseline=baseline,
            window=max(1, int(window)),
            enabled=bool(enabled),
            name=name,
            created_at=now,
            updated_at=now,
        )
        with self._lock, closing(self._connect()) as conn:
            conn.execute(
                "INSERT INTO monitor (id, kind, job_id, threshold, baseline, window,"
                " enabled, name, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    monitor.id, monitor.kind, monitor.job_id, monitor.threshold,
                    monitor.baseline, monitor.window, int(monitor.enabled),
                    monitor.name, monitor.created_at, monitor.updated_at,
                ),
            )
            conn.commit()
        return monitor

    def update(self, monitor_id: str, **changes: Any) -> Optional[Monitor]:
        """Changes a rule in place. Unknown fields are ignored, not an error.

        Changing what a rule asks discards its state: the old `firing` was the
        answer to a different question, and keeping it would either hold an alert
        open for a rule nobody has yet evaluated or suppress the first firing of
        the new one.
        """
        current = self.get(monitor_id)
        if current is None:
            return None
        allowed = ("kind", "job_id", "threshold", "baseline", "window", "enabled", "name")
        updates = {key: value for key, value in changes.items() if key in allowed}
        if "kind" in updates and updates["kind"] not in KINDS:
            raise ValueError(f"Unknown monitor kind '{updates['kind']}'.")
        if "baseline" in updates and updates["baseline"] not in BASELINES:
            raise ValueError(f"Unknown baseline '{updates['baseline']}'.")
        if not updates:
            return current
        merged = Monitor(**{**vars(current), **updates})
        merged.window = max(1, int(merged.window))
        merged.threshold = float(merged.threshold)
        merged.enabled = bool(merged.enabled)
        merged.updated_at = _now_iso()
        resets = any(key in updates for key in ("kind", "job_id", "threshold", "baseline", "window"))
        with self._lock, closing(self._connect()) as conn:
            conn.execute(
                "UPDATE monitor SET kind = ?, job_id = ?, threshold = ?, baseline = ?,"
                " window = ?, enabled = ?, name = ?, updated_at = ? WHERE id = ?",
                (
                    merged.kind, merged.job_id, merged.threshold, merged.baseline,
                    merged.window, int(merged.enabled), merged.name,
                    merged.updated_at, merged.id,
                ),
            )
            if resets:
                conn.execute("DELETE FROM monitor_state WHERE monitor_id = ?", (monitor_id,))
            conn.commit()
        return merged

    def delete(self, monitor_id: str) -> bool:
        """Removes the rule, the verdicts it left and the transitions it logged.

        The events go with it deliberately. An event says "this started firing"
        and nothing else — what it was firing *about* lives in the rule, so an
        event whose rule is gone is a timestamp nobody can read. The record worth
        keeping after the fact is the run itself, and that is in the history.
        """
        with self._lock, closing(self._connect()) as conn:
            conn.execute("DELETE FROM monitor_state WHERE monitor_id = ?", (monitor_id,))
            conn.execute("DELETE FROM monitor_event WHERE monitor_id = ?", (monitor_id,))
            cursor = conn.execute("DELETE FROM monitor WHERE id = ?", (monitor_id,))
            conn.commit()
            return cursor.rowcount > 0

    def get(self, monitor_id: str) -> Optional[Monitor]:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT * FROM monitor WHERE id = ?", (monitor_id,)).fetchone()
        return _row_to_monitor(row) if row else None

    def list(self, *, enabled_only: bool = False) -> List[Monitor]:
        sql = "SELECT * FROM monitor"
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY created_at"
        with closing(self._connect()) as conn:
            return [_row_to_monitor(row) for row in conn.execute(sql)]

    # ---- state -----------------------------------------------------------

    def states(self, monitor_id: Optional[str] = None) -> List[MonitorState]:
        sql = "SELECT * FROM monitor_state"
        params: Sequence[Any] = ()
        if monitor_id:
            sql += " WHERE monitor_id = ?"
            params = (monitor_id,)
        sql += " ORDER BY firing DESC, since DESC"
        with closing(self._connect()) as conn:
            return [_row_to_state(row) for row in conn.execute(sql, params)]

    def record(
        self, monitor: Monitor, job_id: str, verdict: Verdict, *, at: Optional[str] = None
    ) -> Optional[MonitorEvent]:
        """Stores the verdict and answers with the transition, if there was one.

        `None` means nothing changed — still firing, or still fine — and nothing
        changed is the overwhelmingly common case, which is why the notification
        hangs off this return value rather than off the verdict.
        """
        moment = at or _now_iso()
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM monitor_state WHERE monitor_id = ? AND job_id = ?",
                (monitor.id, job_id),
            ).fetchone()
            was_firing = bool(row["firing"]) if row else False
            known = row is not None
            since = row["since"] if row else None
            if not known or was_firing != verdict.firing:
                since = moment
            conn.execute(
                "INSERT INTO monitor_state (monitor_id, job_id, firing, reason, since,"
                " checked_at, value, baseline, run_id)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(monitor_id, job_id) DO UPDATE SET"
                " firing = excluded.firing, reason = excluded.reason,"
                " since = excluded.since, checked_at = excluded.checked_at,"
                " value = excluded.value, baseline = excluded.baseline,"
                " run_id = excluded.run_id",
                (
                    monitor.id, job_id, int(verdict.firing), verdict.reason, since,
                    moment, verdict.value, verdict.baseline, verdict.run_id,
                ),
            )
            # A first evaluation that comes out fine is not a transition: it is
            # the rule being asked for the first time, and nobody wants "Job x is
            # not failing" in a channel.
            transition = (known and was_firing != verdict.firing) or (not known and verdict.firing)
            event: Optional[MonitorEvent] = None
            if transition:
                event = MonitorEvent(
                    id=uuid.uuid4().hex,
                    monitor_id=monitor.id,
                    job_id=job_id,
                    at=moment,
                    firing=verdict.firing,
                    reason=verdict.reason,
                    value=verdict.value,
                    baseline=verdict.baseline,
                    run_id=verdict.run_id,
                )
                conn.execute(
                    "INSERT INTO monitor_event (id, monitor_id, job_id, at, firing,"
                    " reason, value, baseline, run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.id, event.monitor_id, event.job_id, event.at,
                        int(event.firing), event.reason, event.value,
                        event.baseline, event.run_id,
                    ),
                )
            conn.commit()
        return event

    def events(self, *, limit: int = 50, monitor_id: Optional[str] = None) -> List[MonitorEvent]:
        sql = "SELECT * FROM monitor_event"
        params: List[Any] = []
        if monitor_id:
            sql += " WHERE monitor_id = ?"
            params.append(monitor_id)
        sql += " ORDER BY at DESC LIMIT ?"
        params.append(max(1, min(int(limit), 500)))
        with closing(self._connect()) as conn:
            return [_row_to_event(row) for row in conn.execute(sql, params)]

    def forget(self, job_ids: Iterable[str]) -> int:
        """Drops the state of Jobs that no longer exist.

        A wildcard rule leaves a row per Job, and a Job that was deleted would
        otherwise keep an alert open forever with nothing behind it.
        """
        keep = set(job_ids)
        removed = 0
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute("SELECT monitor_id, job_id FROM monitor_state").fetchall()
            for row in rows:
                if row["job_id"] not in keep:
                    conn.execute(
                        "DELETE FROM monitor_state WHERE monitor_id = ? AND job_id = ?",
                        (row["monitor_id"], row["job_id"]),
                    )
                    removed += 1
            conn.commit()
        return removed


def _row_to_monitor(row: sqlite3.Row) -> Monitor:
    return Monitor(
        id=row["id"],
        kind=row["kind"],
        job_id=row["job_id"],
        threshold=float(row["threshold"]),
        baseline=row["baseline"],
        window=int(row["window"]),
        enabled=bool(row["enabled"]),
        name=row["name"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_state(row: sqlite3.Row) -> MonitorState:
    return MonitorState(
        monitor_id=row["monitor_id"],
        job_id=row["job_id"],
        firing=bool(row["firing"]),
        reason=row["reason"],
        since=row["since"],
        checked_at=row["checked_at"],
        value=row["value"],
        baseline=row["baseline"],
        run_id=row["run_id"],
    )


def _row_to_event(row: sqlite3.Row) -> MonitorEvent:
    return MonitorEvent(
        id=row["id"],
        monitor_id=row["monitor_id"],
        job_id=row["job_id"],
        at=row["at"],
        firing=bool(row["firing"]),
        reason=row["reason"],
        value=row["value"],
        baseline=row["baseline"],
        run_id=row["run_id"],
    )


# --------------------------------------------------------------- the sweep


def targets(monitor: Monitor, health: Sequence[JobFacts]) -> List[JobFacts]:
    """Which Jobs one rule is about, expanding the wildcard against the library."""
    if monitor.job_id == ANY_JOB:
        return list(health)
    return [facts for facts in health if facts.job_id == monitor.job_id]


def sweep(
    store: MonitorStore,
    health: Sequence[JobFacts],
    *,
    now: Optional[datetime] = None,
    notifier: Optional[Any] = None,
) -> List[MonitorEvent]:
    """Evaluates every enabled rule against every Job it watches.

    Answers with the transitions only. `notifier` is called once per transition
    and defaults to the webhook; passing one in is how a test watches this happen
    without a socket.
    """
    moment = now or datetime.now(timezone.utc)
    at = moment.isoformat(timespec="seconds").replace("+00:00", "Z")
    send = notifier if notifier is not None else notify
    transitions: List[MonitorEvent] = []
    for monitor in store.list(enabled_only=True):
        for facts in targets(monitor, health):
            verdict = evaluate(monitor, facts, moment)
            event = store.record(monitor, facts.job_id, verdict, at=at)
            if event is None:
                continue
            transitions.append(event)
            state = MonitorState(
                monitor_id=monitor.id, job_id=facts.job_id, firing=event.firing,
                reason=event.reason, since=event.at, checked_at=event.at,
                value=event.value, baseline=event.baseline, run_id=event.run_id,
            )
            try:
                send(monitor, state)
            except Exception as exc:  # pragma: no cover - defensive
                _log.warning("Alert notification failed: %s", exc)
    return transitions


#: How often the sweep runs when nobody says otherwise. A minute is short enough
#: that "no successful run in 30 minutes" means what it says, and long enough that
#: the cost is one SQLite query per minute.
DEFAULT_INTERVAL_SECONDS = 60


def interval_seconds(env: Optional[Dict[str, str]] = None) -> int:
    source = env if env is not None else os.environ
    raw = (source.get("SPARQUET_STUDIO_MONITOR_INTERVAL") or "").strip()
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_INTERVAL_SECONDS
    return max(5, value)


def sweeping_enabled(env: Optional[Dict[str, str]] = None) -> bool:
    """On unless turned off, unlike the Spark warm-up.

    The asymmetry is the same one the history purge has: a sweep costs a query
    against a SQLite file, and a rule that is only checked when somebody opens a
    screen is not a monitor. `SPARQUET_STUDIO_MONITORS=off` stops the timer; the
    rules stay, and `POST /monitors/evaluate` still answers.
    """
    source = env if env is not None else os.environ
    return (source.get("SPARQUET_STUDIO_MONITORS") or "on").strip().lower() not in (
        "0", "off", "false", "no",
    )
