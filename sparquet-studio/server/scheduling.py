"""Schedules: running a Job or a Pipeline because the clock said so.

Everything in the runner so far starts because somebody pressed something. That
is fine for the Job being written and useless for the one that is finished: an
ingestion that has to happen every morning at six is not a thing anybody wants to
remember, and "remember to press run" is not an answer a data team accepts.

So a schedule is a five-field cron expression and a timezone, stored **in the
library record itself** rather than in a database of the runner's own. That
placement is the whole design:

* it is committed with the project, so the schedule travels with the Job through
  git, code review and every other machine, instead of living in one laptop's
  SQLite file;
* the Studio already writes records, so nothing new had to be invented to edit
  one;
* and a Job copied to another environment arrives already carrying when it runs.

What the runner keeps in memory is only the anchor — when each schedule last
fired — because that is genuinely local state: it says what *this* process has
already done.

**No catch-up beyond a grace window.** A runner that was switched off overnight
does not replay the eight occurrences it missed; when it comes back it fires at
most one, and only if it is less than `SPARQUET_STUDIO_SCHEDULER_GRACE` seconds
old. Replaying a night of hourly runs at once is never what the person wanted,
and firing nothing after a thirty-second restart is not either — the grace window
is where those two meet. This is the honest limit of a local scheduler and it is
written on the screen: a runner that is off does not run anything.

Parsing and the arithmetic over the clock are pure functions here (`parse_cron`,
`next_fire`, `due_at`), so they can be tested against a fixed instant without a
thread, a database or a Spark session. Everything that touches the runner's state
— reading the library, taking the run lock, recording history — stays in
`main.py`, the same split `monitoring.py` uses.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, FrozenSet, List, Optional, Sequence, Tuple

try:  # pragma: no cover - present on every supported interpreter
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - defensive
    ZoneInfo = None  # type: ignore[assignment]

_log = logging.getLogger("sparquet.studio.scheduling")

#: What the record calls the two things that can be scheduled. The same words the
#: history uses for a run's kind, so a scheduled run is filed where a manual one is.
JOB = "job"
PIPELINE = "pipeline"
KINDS = (JOB, PIPELINE)

#: The timezone a schedule means when it does not say. Not `UTC`: this is a
#: scheduler for a machine somebody sits at, and "every morning at six" means six
#: o'clock where that person is. A project that needs to be explicit says so.
LOCAL = "local"


class CronError(ValueError):
    """A cron expression the runner will not guess at."""


# --------------------------------------------------------------------- cron

#: Names accepted in the month and weekday fields, so `0 6 * * mon-fri` reads the
#: way somebody would say it out loud.
_MONTHS = {
    name: index
    for index, name in enumerate(
        "jan feb mar apr may jun jul aug sep oct nov dec".split(), start=1
    )
}
_WEEKDAYS = {
    name: index
    for index, name in enumerate("sun mon tue wed thu fri sat".split(), start=0)
}

_FIELD = re.compile(r"^[0-9a-z*/,\-]+$")


@dataclass(frozen=True)
class CronSchedule:
    """A parsed five-field expression: minute, hour, day of month, month, weekday.

    Each field is the set of values that match, resolved once at parse time. A set
    is the honest shape — `*/15` and `0,15,30,45` are the same schedule and there
    is no reason for anything downstream to be able to tell them apart.
    """

    minutes: FrozenSet[int]
    hours: FrozenSet[int]
    days: FrozenSet[int]
    months: FrozenSet[int]
    weekdays: FrozenSet[int]
    #: The expression as written, kept for the interface and for error messages.
    raw: str = ""
    #: Whether the day-of-month and weekday fields were both restricted. Cron
    #: unions them in that case rather than intersecting, which surprises everyone
    #: exactly once: `0 0 1 * mon` is the first of the month *and* every Monday.
    day_union: bool = False

    def matches(self, moment: datetime) -> bool:
        """Whether this expression covers the minute `moment` falls in."""
        if moment.month not in self.months:
            return False
        if moment.hour not in self.hours or moment.minute not in self.minutes:
            return False
        return self.day_matches(moment)

    def day_matches(self, moment: datetime) -> bool:
        # `weekday()` is Monday-zero and cron is Sunday-zero.
        weekday = (moment.weekday() + 1) % 7
        by_day = moment.day in self.days
        by_weekday = weekday in self.weekdays
        return (by_day or by_weekday) if self.day_union else (by_day and by_weekday)


def _parse_field(
    text: str, low: int, high: int, names: Optional[Dict[str, int]] = None
) -> Tuple[FrozenSet[int], bool]:
    """One field to the set of values it matches, and whether it was restricted.

    "Restricted" means it is not `*` — the only thing the day fields need to know
    to decide between union and intersection.
    """
    cleaned = (text or "").strip().lower()
    if not cleaned or not _FIELD.match(cleaned):
        raise CronError(f"Cannot read {text!r} as a cron field.")
    restricted = cleaned != "*" and not cleaned.startswith("*/")
    values: set = set()
    for part in cleaned.split(","):
        step = 1
        if "/" in part:
            part, _, raw_step = part.partition("/")
            try:
                step = int(raw_step)
            except ValueError:
                raise CronError(f"Cannot read the step in {text!r}.") from None
            if step < 1:
                raise CronError(f"A step must be at least 1, not {step}, in {text!r}.")
        if part in ("*", ""):
            start, end = low, high
        elif "-" in part.lstrip("-"):
            start_text, _, end_text = part.partition("-")
            start = _value(start_text, low, high, names, text)
            end = _value(end_text, low, high, names, text)
        else:
            start = _value(part, low, high, names, text)
            end = start if step == 1 else high
        if end < start:
            raise CronError(f"The range {part!r} in {text!r} ends before it starts.")
        values.update(range(start, end + 1, step))
    if not values:
        raise CronError(f"Nothing matches {text!r}.")
    return frozenset(values), restricted


def _value(
    text: str, low: int, high: int, names: Optional[Dict[str, int]], field_text: str
) -> int:
    token = text.strip()
    if names and token in names:
        return names[token]
    try:
        number = int(token)
    except ValueError:
        raise CronError(f"Cannot read {token!r} in {field_text!r}.") from None
    # Sunday is both 0 and 7 in every cron there has ever been.
    if names is _WEEKDAYS and number == 7:
        return 0
    if not low <= number <= high:
        raise CronError(f"{number} is outside {low}-{high} in {field_text!r}.")
    return number


def parse_cron(expression: str) -> CronSchedule:
    """`0 6 * * mon-fri` to the set of minutes it names.

    Five fields only. The six-field form with seconds is deliberately refused
    rather than guessed at: reading `0 0 6 * * *` as minute-zero-of-hour-zero
    would run a daily job every minute, and a scheduler that silently misreads an
    expression is worse than one that will not take it.
    """
    parts = (expression or "").strip().split()
    if len(parts) != 5:
        raise CronError(
            f"A schedule has five fields (minute hour day month weekday), "
            f"not {len(parts)}: {expression!r}."
        )
    minutes, _ = _parse_field(parts[0], 0, 59)
    hours, _ = _parse_field(parts[1], 0, 23)
    days, day_restricted = _parse_field(parts[2], 1, 31)
    months, _ = _parse_field(parts[3], 1, 12, _MONTHS)
    weekdays, weekday_restricted = _parse_field(parts[4], 0, 6, _WEEKDAYS)
    return CronSchedule(
        minutes=minutes, hours=hours, days=days, months=months, weekdays=weekdays,
        raw=" ".join(parts), day_union=day_restricted and weekday_restricted,
    )


# ----------------------------------------------------------------- timezone


def resolve_zone(name: Optional[str]) -> Optional[Any]:
    """The `tzinfo` a schedule means, or `None` for the machine's local time.

    An unknown name falls back to local time with a warning instead of refusing to
    fire. The alternative is a Job that stops running because the tz database is
    missing on this machine, which is a silence nobody notices until the data is
    two days old.
    """
    label = (name or "").strip()
    if not label or label.lower() == LOCAL:
        return None
    if label.upper() == "UTC":
        return timezone.utc
    if ZoneInfo is None:  # pragma: no cover - no zoneinfo on this interpreter
        _log.warning("No zoneinfo on this interpreter; %s runs in local time.", label)
        return None
    try:
        return ZoneInfo(label)
    except Exception:
        _log.warning("Unknown timezone %r; the schedule runs in local time.", label)
        return None


# --------------------------------------------------------------- the record


@dataclass(frozen=True)
class Schedule:
    """When one Job or Pipeline of the library runs, as the record states it."""

    kind: str
    id: str
    name: str
    cron: str
    timezone: str = LOCAL
    enabled: bool = True
    #: Who the runs are attributed to and authorized as — the account that saved
    #: the schedule. The runner authenticates a token, not a person, so a
    #: scheduled run has to name somebody or it would run with no identity at all.
    run_as: str = ""
    workflow_id: Optional[str] = None
    #: Why this schedule cannot fire, when the expression does not parse. Kept as
    #: data rather than raised: one unreadable cron must not stop the others, and
    #: the person who typed it needs to see what is wrong with it.
    error: Optional[str] = None
    parsed: Optional[CronSchedule] = field(default=None, compare=False, repr=False)

    @property
    def runnable(self) -> bool:
        return self.enabled and self.error is None and self.parsed is not None

    def describe(self) -> str:
        """The sentence every surface shows, composed in one place so the webhook,
        the API and the screen cannot disagree about what a schedule says."""
        if self.error:
            return f"invalid schedule: {self.error}"
        where = self.timezone if self.timezone and self.timezone != LOCAL else "local time"
        return f"{self.cron} ({where})"

    def to_json(self) -> Dict[str, Any]:
        return {
            "kind": self.kind,
            "id": self.id,
            "name": self.name,
            "cron": self.cron,
            "timezone": self.timezone,
            "enabled": self.enabled,
            "run_as": self.run_as,
            "workflow_id": self.workflow_id,
            "error": self.error,
            "rule": self.describe(),
        }


def from_record(
    kind: str, record: Dict[str, Any], record_id: Optional[str] = None
) -> Optional[Schedule]:
    """The `schedule` block of a library record, or `None` when it has none.

    A record with no schedule is the normal case and not an error: most Jobs are
    run by hand, and the field only appears once somebody asks for it.
    """
    if kind not in KINDS or not isinstance(record, dict):
        return None
    block = record.get("schedule")
    if not isinstance(block, dict):
        return None
    cron = str(block.get("cron") or "").strip()
    if not cron:
        return None
    identifier = str(record_id or record.get("id") or "")
    if not identifier:
        return None
    zone = str(block.get("timezone") or LOCAL).strip() or LOCAL
    error: Optional[str] = None
    parsed: Optional[CronSchedule] = None
    try:
        parsed = parse_cron(cron)
    except CronError as exc:
        error = str(exc)
    return Schedule(
        kind=kind,
        id=identifier,
        name=str(record.get("name") or record_id),
        cron=cron,
        timezone=zone,
        enabled=bool(block.get("enabled", True)),
        run_as=str(block.get("runAs") or block.get("run_as") or "").strip(),
        workflow_id=str(record.get("workflowId") or "") or None,
        error=error,
        parsed=parsed,
    )


def read_schedules(
    documents: Sequence[Tuple[str, str, Dict[str, Any]]]
) -> List[Schedule]:
    """Every schedule in the library, from `(kind, id, record)` triples."""
    found: List[Schedule] = []
    for kind, record_id, record in documents:
        schedule = from_record(kind, record, record_id)
        if schedule is not None:
            found.append(schedule)
    return found


# ------------------------------------------------------------ stage order


def order_stages(
    stages: Sequence[Dict[str, Any]],
    links: Sequence[Dict[str, Any]],
    names: Optional[Dict[str, str]] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """A Pipeline's stages in execution order, and the ids caught in a cycle.

    The record stores the canvas, not an order: stages as they were dropped and
    links saying which runs before which. The Studio resolves that with Kahn's
    algorithm, breaking ties by stage name so the same canvas always produces the
    same order; this mirrors `src/lib/pipeline/describe.ts: topologicalOrder`,
    because a scheduled Pipeline that ran its stages in a different order from the
    one the author saw on screen would be a different Pipeline.

    Stages on a cycle are returned last rather than dropped. The Studio already
    refuses to draw a cycle, so one here means a hand-edited file, and running the
    rest of the Pipeline while reporting the cycle beats running nothing.
    """
    labels = names or {}
    ids = [str(stage.get("id") or "") for stage in stages if isinstance(stage, dict)]
    ids = [stage_id for stage_id in ids if stage_id]
    by_id = {
        str(stage.get("id") or ""): stage for stage in stages if isinstance(stage, dict)
    }
    indegree = {stage_id: 0 for stage_id in ids}
    adjacency: Dict[str, List[str]] = {}
    for link in links or ():
        if not isinstance(link, dict):
            continue
        source = str(link.get("source") or "")
        target = str(link.get("target") or "")
        if source == target or source not in indegree or target not in indegree:
            continue
        adjacency.setdefault(source, []).append(target)
        indegree[target] += 1

    def key(stage_id: str) -> str:
        return f"{labels.get(stage_id, '').lower()} {stage_id}"

    ready = sorted([s for s in ids if indegree[s] == 0], key=key)
    ordered: List[str] = []
    placed = set()
    while ready:
        stage_id = ready.pop(0)
        ordered.append(stage_id)
        placed.add(stage_id)
        queued = False
        for following in adjacency.get(stage_id, ()):
            indegree[following] -= 1
            if indegree[following] == 0:
                ready.append(following)
                queued = True
        if queued:
            ready.sort(key=key)
    cyclic = sorted([s for s in ids if s not in placed], key=key)
    ordered.extend(cyclic)
    return [by_id[stage_id] for stage_id in ordered], cyclic


# -------------------------------------------------------------- the clock


#: How far ahead `next_fire` is willing to look before deciding an expression
#: names nothing. Four years covers the only expression that can legitimately be
#: this sparse — February 29th.
_HORIZON_DAYS = 366 * 4


def next_fire(schedule: Schedule, after: datetime) -> Optional[datetime]:
    """The first occurrence strictly after `after`, in UTC.

    The arithmetic is done in the schedule's own timezone and converted back, so
    `0 6 * * *` stays six in the morning across a daylight-saving change instead
    of drifting to five or seven.
    """
    cron = schedule.parsed
    if cron is None:
        return None
    zone = resolve_zone(schedule.timezone)
    # With no named zone the arithmetic runs on a naive local clock rather than on
    # the fixed offset `astimezone()` would freeze in: a fixed offset is right
    # until the day the clocks change, and then it is an hour wrong for months.
    # A naive datetime converted back with `astimezone` is interpreted as local
    # time by the platform, which is the one thing that knows about that change.
    moment = after.astimezone(zone) if zone is not None else after.astimezone().replace(tzinfo=None)
    candidate = moment.replace(second=0, microsecond=0) + timedelta(minutes=1)
    for _ in range(_HORIZON_DAYS):
        if not cron.day_matches(candidate) or candidate.month not in cron.months:
            candidate = (candidate + timedelta(days=1)).replace(hour=0, minute=0)
            continue
        day = candidate.day
        while candidate.day == day:
            if candidate.hour in cron.hours and candidate.minute in cron.minutes:
                return candidate.astimezone(timezone.utc)
            candidate += timedelta(minutes=1)
    return None


def due_at(
    schedule: Schedule,
    *,
    anchor: Optional[datetime],
    now: datetime,
    grace_seconds: int,
) -> Optional[datetime]:
    """The occurrence this schedule should fire for right now, or `None`.

    `anchor` is when it last fired in this process. Occurrences older than the
    grace window are dropped rather than replayed — a runner that was off for the
    night comes back and runs once, not eleven times — and when several fall
    inside the window the most recent one wins, for the same reason.
    """
    if not schedule.runnable:
        return None
    floor = now - timedelta(seconds=max(0, grace_seconds))
    start = max(anchor, floor) if anchor is not None else floor
    if start >= now:
        return None
    fire = next_fire(schedule, start)
    if fire is None or fire > now:
        return None
    # Collapse whatever else fell inside the window into the latest one. Bounded
    # by the window itself, which is why `start` is floored above.
    while True:
        following = next_fire(schedule, fire)
        if following is None or following > now:
            return fire
        fire = following


# ------------------------------------------------------------- environment

DEFAULT_INTERVAL_SECONDS = 30
DEFAULT_GRACE_SECONDS = 900


def scheduling_enabled(env: Optional[Dict[str, str]] = None) -> bool:
    """On unless turned off, like the monitor sweep.

    A schedule that is only honoured when somebody has the Studio open is not a
    schedule. `SPARQUET_STUDIO_SCHEDULER=off` stops the timer and leaves the
    records alone, so a second runner pointed at the same library can be the one
    that executes them.
    """
    source = env if env is not None else os.environ
    return (source.get("SPARQUET_STUDIO_SCHEDULER") or "on").strip().lower() not in (
        "0", "off", "false", "no",
    )


def interval_seconds(env: Optional[Dict[str, str]] = None) -> int:
    """Seconds between sweeps. Floored at 5, and below a minute on purpose: cron
    resolves to the minute, so a sweep slower than that would miss occurrences."""
    source = env if env is not None else os.environ
    raw = (source.get("SPARQUET_STUDIO_SCHEDULER_INTERVAL") or "").strip()
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_INTERVAL_SECONDS
    return max(5, value)


def grace_seconds(env: Optional[Dict[str, str]] = None) -> int:
    """How late an occurrence may be and still run. Fifteen minutes by default:
    long enough to survive a restart, short enough that nobody is surprised by a
    Job starting hours after the time written next to it."""
    source = env if env is not None else os.environ
    raw = (source.get("SPARQUET_STUDIO_SCHEDULER_GRACE") or "").strip()
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_GRACE_SECONDS
    return max(0, value)
