"""Execution credits: one credit per successful write, charged to a team.

Three decisions shape everything in this module, and each one exists because the
alternative produced a bill nobody could defend.

**What is charged is a successful write, not a run.** A Job that reads a table,
transforms it and writes nothing has produced nothing; a Job that writes four
destinations has produced four. Counting writes also settles the awkward case on
its own: a run that fails has written fewer destinations, so it pays for fewer,
and a run that fails before the first write pays nothing at all. Nobody has to
decide what a "partial failure" costs — the count already knows. The number comes
from `PipelineResult.outputs`, which the framework appends to only after
`WriterFactory.create(...).write(df)` returns, so it is a record of writes that
actually completed.

**Local execution is free.** Running Spark on the laptop that is already running
the runner spends the operator's own CPU, and charging for it would be theatre.
Sending the same JSON to a cluster spends money somebody else is paying for, and
that is the thing worth counting. What counts as local is read from the
configuration that is about to run and never from a flag in the request — a
caller who could declare their own run local would have found the way to run for
free.

**The account is the team, not the person.** A squad has one budget and one
invoice; a per-person balance means the work stops because the one person who
happened to run the nightly Job is out of credits. Every user belongs to exactly
one team (`auth.Team`), and the free monthly allowance belongs to the team too.

On top of that:

- Every team gets **40 free credits per calendar month** by default
  (`SPARQUET_STUDIO_CREDITS_FREE_MONTHLY`). The allowance refills on its own when
  the month turns; it does not accumulate, because an allowance that accumulates
  is just a balance with extra steps. A charge draws from the free allowance
  first and only then from the granted balance, so a team never burns bought
  credits while free ones are sitting unused.
- **Metering and enforcement are separate.** Every remote write is recorded from
  the first run, without anybody turning anything on; only
  `SPARQUET_STUDIO_CREDITS=on` makes a balance actually refuse a run. The split
  exists so upgrading a runner never stops work that used to run — an
  administrator can watch a month of real consumption, grant balances that match
  it, and only then start enforcing. It is also why an account carries both
  `balance` (moves only under enforcement) and `spent` (always climbs).
- Charging happens **after** the run, because that is when the number of
  successful writes is known. What happens *before* the run is a **reservation**:
  the run declares how many destinations it intends to write, and that much is
  held against the account. A team that cannot cover its own declaration is
  refused with `402` before Spark is started, instead of discovering halfway
  through a cluster hour that it could only pay for two of five tables.
- A hold is not a charge. When the run ends, `settle` takes what the writes that
  actually succeeded cost and **the rest is released** — a run that failed before
  writing gives everything back, and so does a run refused by the cluster itself.
  A run that wrote more than it declared (a `targets` expansion, an output written
  twice) is charged the difference: the hold is a floor on honesty, not a cap on
  the bill. A run that got past admission and still could not pay is charged down
  to zero with the shortfall recorded on the entry — the work is done, the cluster
  time is spent, and pretending otherwise would make the ledger a worse record
  than no ledger.
- A hold that nobody settles would silently make an account poorer, so it never
  outlives the process that took it: `release_stale()` runs at start-up and gives
  back everything held by a run the restart killed.

The ledger is append-only and lives in its own SQLite file
(`server/data/credits.sqlite3`, override with `SPARQUET_STUDIO_CREDITS_DB`),
apart from identity: it grows with every execution, and an operator may well want
to archive or reset a year of it without going anywhere near the credentials.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple


class CreditError(Exception):
    """A charge that cannot go through, or an amount that makes no sense.

    `main.py` turns the out-of-credit case into a `402` and the rest into a
    `400`; both carry the message, because each one tells the caller what to do.
    """


class InsufficientCredits(CreditError):
    """The team cannot pay for this execution."""

    def __init__(self, account_id: str, available: int, needed: int) -> None:
        super().__init__(
            f"Not enough execution credits: this run needs at least {needed} and the "
            f"team has {available}. Local runs are free and writes that fail cost "
            f"nothing; this one writes to a cluster. An administrator can grant more "
            f"with POST /credits/{account_id}/grant."
        )
        self.account_id = account_id
        self.available = available
        self.needed = needed


#: The account a runner in token-only mode charges. There is exactly one
#: operator there — the holder of the shared token — so there is one account.
TOKEN_ACCOUNT = "token"

#: Recorded on the ledger row, so a listing can say what was paid for.
REASON_RUN = "run"
REASON_GRANT = "grant"
REASON_ADJUST = "adjust"

#: What the free allowance is worth when nobody overrides it. Forty writes a
#: month is enough for a person to build and test pipelines without ever meeting
#: the billing screen, and small enough that a nightly production Job will.
DEFAULT_FREE_MONTHLY = 40


def enforced() -> bool:
    """Whether a balance actually gates execution, or the ledger is only watching.

    Off by default: turning credits on for a runner that has never had them would
    stop every remote Job at once, and an upgrade must not do that.
    """
    return os.getenv("SPARQUET_STUDIO_CREDITS", "").strip().lower() in {
        "1", "on", "true", "yes", "enforce",
    }


def credits_per_write() -> int:
    """One credit per destination written, unless the operator says otherwise."""
    try:
        return max(0, int(os.getenv("SPARQUET_STUDIO_CREDITS_PER_WRITE", "1")))
    except ValueError:
        return 1


def free_monthly() -> int:
    """The allowance every team gets each calendar month, spent before any
    granted balance is touched."""
    try:
        return max(0, int(os.getenv("SPARQUET_STUDIO_CREDITS_FREE_MONTHLY",
                                    str(DEFAULT_FREE_MONTHLY))))
    except ValueError:
        return DEFAULT_FREE_MONTHLY


def initial_balance() -> int:
    """What a brand-new account starts with on top of the free allowance. Zero,
    so nothing is given away by accident; an administrator grants deliberately."""
    try:
        return max(0, int(os.getenv("SPARQUET_STUDIO_CREDITS_INITIAL", "0")))
    except ValueError:
        return 0


def default_db_path() -> Path:
    configured = os.getenv("SPARQUET_STUDIO_CREDITS_DB", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parent / "data" / "credits.sqlite3"


def current_period(moment: Optional[datetime] = None) -> str:
    """The billing period a moment falls in: `YYYY-MM`, in UTC.

    Calendar months in UTC rather than rolling thirty-day windows, because a
    person reading an invoice thinks in months and because two runners in two
    timezones should agree on which month a run belongs to.
    """
    now = moment or datetime.now(timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


def recent_periods(months: int = 6, moment: Optional[datetime] = None) -> List[str]:
    """The last `months` periods ending in the current one, oldest first."""
    now = moment or datetime.now(timezone.utc)
    year, month = now.year, now.month
    out: List[str] = []
    for _ in range(max(1, int(months))):
        out.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month == 0:
            year, month = year - 1, 12
    return list(reversed(out))


# --------------------------------------------------------------- local or not


def _runner_environment() -> str:
    """`local`, or the managed cluster this runner is already inside.

    Delegates to the framework so there is one definition of "am I on a cluster".
    Falls back to `local` when the framework is not importable — a runner without
    pyspark cannot run anything anyway, and guessing `remote` there would charge
    for executions that never happen.
    """
    try:
        from sparquet.core.context import _detect_environment  # type: ignore
    except Exception:  # pragma: no cover - depends on the install
        return "local"
    try:
        return str(_detect_environment())
    except Exception:  # pragma: no cover - defensive
        return "local"


@dataclass
class Target:
    """Where a configuration is about to run, and what each write there costs."""

    local: bool
    #: Human-readable, for the ledger and for the error message: `local[*]`,
    #: `spark://cluster:7077`, `databricks`.
    label: str
    #: Credits per successful write. Zero for a local target, which is the whole
    #: of "local runs are free".
    unit_cost: int


def target_of(pipeline: Dict[str, Any]) -> Target:
    """Read the destination out of the configuration itself.

    Out of the configuration and not out of the request, because the request is
    written by the caller and the configuration is what Spark will obey.
    """
    environment = _runner_environment()
    if environment != "local":
        # A runner living inside a managed cluster: `spark.master` is ignored
        # there, the session belongs to the platform, and every write is remote.
        return Target(local=False, label=environment, unit_cost=credits_per_write())

    spark = pipeline.get("spark") if isinstance(pipeline, dict) else None
    spark = spark if isinstance(spark, dict) else {}
    configs = spark.get("configs")
    configs = configs if isinstance(configs, dict) else {}

    remote = str(configs.get("spark.remote", "") or "").strip()
    if remote:
        # Spark Connect: the master is irrelevant, the work happens over there.
        return Target(local=False, label=f"connect {remote}", unit_cost=credits_per_write())

    master = str(configs.get("spark.master") or spark.get("master") or "local[*]").strip()
    if master.startswith("local"):
        return Target(local=True, label=master, unit_cost=0)
    return Target(local=False, label=master, unit_cost=credits_per_write())


def declared_writes(pipeline: Dict[str, Any]) -> int:
    """How many destinations a configuration says it will write.

    One per `outputs` entry, which is exactly what `PipelineResult.output_metrics`
    will carry if everything succeeds — the quarantine and the validation report
    write through their own path and are not metered. A configuration with no
    outputs still declares one: it is about to run, and reserving nothing would
    let an empty account start a cluster.
    """
    outputs = pipeline.get("outputs") if isinstance(pipeline, dict) else None
    if isinstance(outputs, list) and outputs:
        return len(outputs)
    return 1


# -------------------------------------------------------------------- storage


_SCHEMA = """
CREATE TABLE IF NOT EXISTS account (
  id TEXT PRIMARY KEY,
  username TEXT NOT NULL,
  balance INTEGER NOT NULL DEFAULT 0,
  spent INTEGER NOT NULL DEFAULT 0,
  period TEXT,
  free_used INTEGER NOT NULL DEFAULT 0,
  held INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reservation (
  id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  amount INTEGER NOT NULL,
  writes INTEGER NOT NULL,
  status TEXT NOT NULL,
  target TEXT,
  job_name TEXT,
  job_run_id TEXT,
  pipeline_run_id TEXT,
  created_at TEXT NOT NULL,
  closed_at TEXT,
  settled_amount INTEGER
);
CREATE INDEX IF NOT EXISTS idx_reservation_account
  ON reservation(account_id, status);

CREATE TABLE IF NOT EXISTS ledger (
  id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  amount INTEGER NOT NULL,
  reason TEXT NOT NULL,
  applied INTEGER NOT NULL DEFAULT 1,
  balance_after INTEGER NOT NULL,
  writes INTEGER NOT NULL DEFAULT 0,
  free_amount INTEGER NOT NULL DEFAULT 0,
  shortfall INTEGER NOT NULL DEFAULT 0,
  period TEXT,
  job_run_id TEXT,
  pipeline_run_id TEXT,
  target TEXT,
  job_name TEXT,
  note TEXT,
  workflow_id TEXT,
  actor TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ledger_account ON ledger(account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ledger_job_run ON ledger(job_run_id);
CREATE INDEX IF NOT EXISTS idx_ledger_period ON ledger(period, reason);

/* The tags that applied to the run this entry paid for, copied here at charge
   time instead of being looked up in the catalog when the bill is read. An
   invoice must not change: retagging a Job next month says what that Job costs
   from now on, and rewrites nothing about the months already closed. */
CREATE TABLE IF NOT EXISTS ledger_tag (
  entry_id TEXT NOT NULL REFERENCES ledger(id),
  tag TEXT NOT NULL,
  PRIMARY KEY (entry_id, tag)
);
CREATE INDEX IF NOT EXISTS idx_ledger_tag_tag ON ledger_tag(tag);
"""

#: A tag is typed by a human, so it is bounded before it reaches the ledger.
#: Same rules as the catalog's, repeated rather than imported: billing must not
#: depend on the history module to keep its own rows sane.
MAX_TAGS = 20
MAX_TAG_LENGTH = 40


def normalize_tags(values: Any) -> List[str]:
    """Trimmed, deduplicated case-insensitively, bounded in size and in number."""
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


#: Columns added after the first release. Applied on every start to a database
#: written before they existed, so an upgrade asks the operator for nothing.
_ADDED_COLUMNS = {
    "account": {
        "period": "ALTER TABLE account ADD COLUMN period TEXT",
        "free_used": "ALTER TABLE account ADD COLUMN free_used INTEGER NOT NULL DEFAULT 0",
        "held": "ALTER TABLE account ADD COLUMN held INTEGER NOT NULL DEFAULT 0",
    },
    "ledger": {
        "writes": "ALTER TABLE ledger ADD COLUMN writes INTEGER NOT NULL DEFAULT 0",
        "free_amount": "ALTER TABLE ledger ADD COLUMN free_amount INTEGER NOT NULL DEFAULT 0",
        "shortfall": "ALTER TABLE ledger ADD COLUMN shortfall INTEGER NOT NULL DEFAULT 0",
        "period": "ALTER TABLE ledger ADD COLUMN period TEXT",
        # The account pays, but the bill has to be readable by who and by what:
        # a team wants to know which workflow and which person spent the month.
        "workflow_id": "ALTER TABLE ledger ADD COLUMN workflow_id TEXT",
        "actor": "ALTER TABLE ledger ADD COLUMN actor TEXT",
    },
}


@dataclass
class Account:
    """A team's standing: what it may still spend, and what it has spent.

    `free_used` is scoped to `period`; reading an account whose period has turned
    resets both, so a caller never has to ask "is this number still this month's?".
    """

    id: str
    username: str
    balance: int
    spent: int
    period: str
    free_used: int
    free_monthly: int
    #: Credits reserved by runs that are in progress. Already promised to
    #: somebody, so not spendable, but not spent either — a hold comes back.
    held: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @property
    def free_remaining(self) -> int:
        return max(0, self.free_monthly - self.free_used)

    @property
    def available(self) -> int:
        """What could be spent right now: this month's allowance, plus the balance,
        minus what runs in progress have already reserved."""
        return max(0, self.free_remaining + self.balance - self.held)


@dataclass
class Entry:
    id: str
    account_id: str
    amount: int
    reason: str
    applied: bool
    balance_after: int
    created_at: str
    #: Successful writes this entry paid for. Zero for a grant.
    writes: int = 0
    #: How much of `amount` came out of the free monthly allowance.
    free_amount: int = 0
    #: What the run owed and could not pay. Non-zero only when a run got past
    #: admission and then wrote more than the team could afford.
    shortfall: int = 0
    period: Optional[str] = None
    job_run_id: Optional[str] = None
    pipeline_run_id: Optional[str] = None
    target: Optional[str] = None
    job_name: Optional[str] = None
    note: Optional[str] = None
    #: Where the spending happened, and who caused it. The account is still the
    #: team — these are how one invoice is read back by workflow and by person.
    workflow_id: Optional[str] = None
    actor: Optional[str] = None
    #: The labels that applied to the run when it was charged. Frozen here: what
    #: this entry says stays true even after the Job is retagged.
    tags: List[str] = field(default_factory=list)


@dataclass
class Reservation:
    """Credits held for a run that has not finished yet.

    `amount` is what the run declared it would cost. `settle` replaces it with
    what the run really cost and gives back the difference; nothing here is on the
    ledger, because a hold is not a movement — only the settlement is.
    """

    id: str
    account_id: str
    amount: int
    writes: int
    target: str
    status: str = "open"

    @property
    def held(self) -> bool:
        return self.status == "open" and self.amount > 0


@dataclass
class Charge:
    """What one execution cost, and where it is recorded.

    Returned even when nothing was taken (`amount == 0`), so a caller can write
    one line — "this run cost N" — instead of branching on local versus remote.
    """

    account_id: str
    amount: int
    applied: bool
    balance_after: int
    target: str
    writes: int = 0
    free_amount: int = 0
    shortfall: int = 0
    entry_id: Optional[str] = None

    @property
    def charged(self) -> bool:
        return self.amount > 0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class CreditLedger(Protocol):
    """What the runner needs from a ledger, and nothing more.

    The local implementation below is SQLite on the operator's disk, which is the
    right answer for one team on one machine. A hosted service needs Postgres with
    per-tenant isolation and a payment gateway behind the same calls, and it gets
    there by satisfying this protocol rather than by forking the runner — see
    `providers.py`.

    Two properties any implementation has to keep, because the routes depend on
    them rather than on the storage:

    - **`reserve` → `settle`/`release` is a pair.** A reservation that is never
      settled has to expire on its own (`release_stale`), or a crashed run holds
      credits forever.
    - **What was charged is frozen at charge time.** The tags, the workflow and the
      job name on an `Entry` are copies, not lookups: a Job renamed in March must
      not rewrite February's invoice.
    """

    def account(self, account_id: str, username: Optional[str] = None) -> Account: ...

    def list_accounts(self) -> List[Account]: ...

    def grant(
        self, account_id: str, amount: int, *, username: Optional[str] = None,
        note: Optional[str] = None, reason: str = REASON_GRANT,
    ) -> Account: ...

    def precheck(
        self, account_id: str, target: Target, *, username: Optional[str] = None
    ) -> Account: ...

    def reserve(
        self, account_id: str, target: Target, writes: int, *,
        username: Optional[str] = None, job_name: Optional[str] = None,
        job_run_id: Optional[str] = None, pipeline_run_id: Optional[str] = None,
    ) -> Reservation: ...

    def settle(
        self, reservation: Optional[Reservation], target: Target, writes: int, *,
        account_id: Optional[str] = None, username: Optional[str] = None,
        job_run_id: Optional[str] = None, pipeline_run_id: Optional[str] = None,
        job_name: Optional[str] = None, workflow_id: Optional[str] = None,
        actor: Optional[str] = None, tags: Optional[Sequence[str]] = None,
    ) -> Charge: ...

    def release(
        self, reservation: Optional[Reservation], *, reason: str = "released"
    ) -> int: ...

    def release_stale(self) -> int: ...

    def ledger(
        self, account_id: Optional[str] = None, limit: int = 100,
        *, period: Optional[str] = None,
    ) -> List[Entry]: ...

    def entries_for_job_runs(self, job_run_ids: Iterable[str]) -> Dict[str, Entry]: ...

    def usage(self, account_id: str, period: Optional[str] = None) -> Dict[str, int]: ...

    def breakdown(
        self, *, group_by: str = "workflow", period: Optional[str] = None,
        account_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]: ...

    def totals(
        self, *, period: Optional[str] = None, account_id: Optional[str] = None
    ) -> Dict[str, int]: ...

    def usage_timeline(
        self, *, months: int = 6, account_id: Optional[str] = None
    ) -> List[Dict[str, Any]]: ...


class CreditStore:
    """Accounts and their ledger, in SQLite.

    One short-lived connection per call under a process lock, matching
    `history.SQLiteExecutionRepository` and `auth.AuthStore`: a handful of rows
    per execution, and the lock is what makes "check what is available, then take
    from it" a single decision rather than two racing ones.
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._path = Path(db_path) if db_path else default_db_path()
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, closing(self._connect()) as conn:
            conn.executescript(_SCHEMA)
            self._migrate(conn)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, timeout=5)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        for table, columns in _ADDED_COLUMNS.items():
            present = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
            for name, statement in columns.items():
                if name not in present:
                    conn.execute(statement)

    # ---- accounts --------------------------------------------------------

    def account(self, account_id: str, username: Optional[str] = None) -> Account:
        """The account, created on first sight and rolled into the current month.

        Created lazily because an account is a consequence of running something,
        not a step somebody has to remember.
        """
        with self._lock, closing(self._connect()) as conn:
            row = self._ensure(conn, account_id, username)
            conn.commit()
            return self._account_of(row)

    def list_accounts(self) -> List[Account]:
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute("SELECT id FROM account ORDER BY username").fetchall()
            accounts = [self._account_of(self._ensure(conn, row["id"], None)) for row in rows]
            conn.commit()
        return accounts

    def grant(
        self, account_id: str, amount: int, *, username: Optional[str] = None,
        note: Optional[str] = None, reason: str = REASON_GRANT,
    ) -> Account:
        """Add credits (or, with a negative amount, take them back).

        A correction is a grant with a negative amount rather than a separate
        operation, so every movement of every account is one table read. The free
        monthly allowance is untouched by this: it is not a balance, and topping
        it up by hand would make the invoice unreadable.
        """
        if amount == 0:
            raise CreditError("A grant of zero credits changes nothing.")
        with self._lock, closing(self._connect()) as conn:
            row = self._ensure(conn, account_id, username)
            balance = int(row["balance"]) + int(amount)
            if balance < 0:
                raise CreditError(
                    f"That would leave the account at {balance}. Take back at most "
                    f"{row['balance']}."
                )
            conn.execute(
                "UPDATE account SET balance = ?, updated_at = ? WHERE id = ?",
                (balance, _now_iso(), account_id),
            )
            self._write_entry(
                conn, account_id=account_id, amount=int(amount), reason=reason,
                applied=True, balance_after=balance, note=note,
                period=str(row["period"] or current_period()),
            )
            conn.commit()
            return self._account_of(
                conn.execute("SELECT * FROM account WHERE id = ?", (account_id,)).fetchone()
            )

    # ---- charging --------------------------------------------------------

    def precheck(
        self, account_id: str, target: Target, *, username: Optional[str] = None
    ) -> Account:
        """Refuse, before Spark is started, a team that cannot pay for one write.

        This is deliberately the weakest possible check: the runner does not know
        how many destinations a Job will manage to write, so the only honest
        question it can ask up front is whether anything at all is available. A
        team that can afford one write and then writes ten is charged what it has
        and refused next time — see the module docstring.
        """
        account = self.account(account_id, username)
        if target.unit_cost <= 0 or not enforced():
            return account
        if account.available < target.unit_cost:
            raise InsufficientCredits(account_id, account.available, target.unit_cost)
        return account

    def reserve(
        self, account_id: str, target: Target, writes: int, *,
        username: Optional[str] = None, job_name: Optional[str] = None,
        job_run_id: Optional[str] = None, pipeline_run_id: Optional[str] = None,
    ) -> Reservation:
        """Hold what the run says it will cost, or refuse it before Spark starts.

        The held amount leaves `available` without leaving `balance`: it is
        promised, not spent, and `settle` decides how much of it was real. A local
        target holds nothing, and so does a runner that only meters — there is
        nothing to protect when nothing is being taken.
        """
        writes = max(1, int(writes))
        cost = writes * max(0, target.unit_cost)
        if cost <= 0 or not enforced():
            return Reservation(
                id="", account_id=account_id, amount=0, writes=writes,
                target=target.label, status="none",
            )

        reservation_id = uuid.uuid4().hex
        with self._lock, closing(self._connect()) as conn:
            row = self._ensure(conn, account_id, username)
            free_remaining = max(0, free_monthly() - int(row["free_used"]))
            available = free_remaining + int(row["balance"]) - int(row["held"] or 0)
            if available < cost:
                raise InsufficientCredits(account_id, max(0, available), cost)
            conn.execute(
                "UPDATE account SET held = held + ?, updated_at = ? WHERE id = ?",
                (cost, _now_iso(), account_id),
            )
            conn.execute(
                "INSERT INTO reservation (id, account_id, amount, writes, status, "
                "target, job_name, job_run_id, pipeline_run_id, created_at) "
                "VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?)",
                (
                    reservation_id, account_id, cost, writes, target.label,
                    job_name, job_run_id, pipeline_run_id, _now_iso(),
                ),
            )
            conn.commit()
        return Reservation(
            id=reservation_id, account_id=account_id, amount=cost, writes=writes,
            target=target.label,
        )

    def settle(
        self, reservation: Optional[Reservation], target: Target, writes: int, *,
        account_id: Optional[str] = None, username: Optional[str] = None,
        job_run_id: Optional[str] = None, pipeline_run_id: Optional[str] = None,
        job_name: Optional[str] = None, workflow_id: Optional[str] = None,
        actor: Optional[str] = None, tags: Optional[Sequence[str]] = None,
    ) -> Charge:
        """Charge what the run really wrote and give back the rest of the hold.

        Releasing before charging is deliberate and not merely tidy: a run that
        wrote more than it declared must be able to spend the credits it was
        holding for itself. Charging first would make it compete with its own
        reservation and report a shortfall that is not real.
        """
        account = account_id or (reservation.account_id if reservation else None)
        if account is None:  # pragma: no cover - defensive
            raise CreditError("A settlement needs an account.")
        self.release(reservation, reason="settled")
        return self.charge(
            account, target, writes, username=username, job_run_id=job_run_id,
            pipeline_run_id=pipeline_run_id, job_name=job_name,
            workflow_id=workflow_id, actor=actor, tags=tags,
        )

    def release(
        self, reservation: Optional[Reservation], *, reason: str = "released"
    ) -> int:
        """Give a hold back, in full. Returns what was released.

        Idempotent on purpose: the run path calls it on the failure branch and
        `settle` calls it on the success branch, and a double release that took
        credits twice would be worse than either.
        """
        if reservation is None or not reservation.id or reservation.amount <= 0:
            return 0
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM reservation WHERE id = ? AND status = 'open'",
                (reservation.id,),
            ).fetchone()
            if row is None:
                return 0
            amount = int(row["amount"])
            conn.execute(
                "UPDATE account SET held = MAX(0, held - ?), updated_at = ? WHERE id = ?",
                (amount, _now_iso(), row["account_id"]),
            )
            conn.execute(
                "UPDATE reservation SET status = ?, closed_at = ? WHERE id = ?",
                (reason, _now_iso(), reservation.id),
            )
            conn.commit()
        reservation.status = reason
        return amount

    def release_stale(self) -> int:
        """Release every open hold. Called at start-up, and only there.

        A hold belongs to a run in progress, and a run in progress belongs to the
        process that started it. If this process is starting, there are none — so
        anything still open was left behind by a crash or a restart, and leaving it
        would make the account permanently poorer for a run that never finished.
        """
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT id, account_id, amount FROM reservation WHERE status = 'open'"
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE account SET held = MAX(0, held - ?), updated_at = ? "
                    "WHERE id = ?",
                    (int(row["amount"]), _now_iso(), row["account_id"]),
                )
            conn.execute(
                "UPDATE reservation SET status = 'abandoned', closed_at = ? "
                "WHERE status = 'open'",
                (_now_iso(),),
            )
            conn.commit()
        return len(rows)

    def charge(
        self, account_id: str, target: Target, writes: int, *,
        username: Optional[str] = None, job_run_id: Optional[str] = None,
        pipeline_run_id: Optional[str] = None, job_name: Optional[str] = None,
        workflow_id: Optional[str] = None, actor: Optional[str] = None,
        tags: Optional[Sequence[str]] = None,
    ) -> Charge:
        """Take the cost of the writes that succeeded, and record what happened.

        A local target, or a run that wrote nothing, costs nothing and writes
        nothing to the ledger — a ledger full of free rows buries the ones that
        matter. A remote target that wrote something always writes a row; whether
        the balance moves depends on `enforced()`.
        """
        writes = max(0, int(writes))
        cost = writes * max(0, target.unit_cost)
        if cost <= 0:
            return Charge(
                account_id=account_id, amount=0, applied=False,
                balance_after=self.account(account_id, username).balance,
                target=target.label, writes=writes,
            )

        gate = enforced()
        with self._lock, closing(self._connect()) as conn:
            row = self._ensure(conn, account_id, username)
            free_remaining = max(0, free_monthly() - int(row["free_used"]))
            balance = int(row["balance"])

            from_free = min(free_remaining, cost)
            from_balance = cost - from_free
            shortfall = 0
            if gate and from_balance > balance:
                # Past admission and out of money: the writes already happened, so
                # the account goes to zero and the gap is recorded rather than
                # forgiven or turned into a negative balance.
                shortfall = from_balance - balance
                from_balance = balance

            if gate:
                free_used = int(row["free_used"]) + from_free
                balance_after = balance - from_balance
            else:
                # Metering only: nothing is consumed, so the free allowance is not
                # burned either — otherwise the month would look spent on a runner
                # that never charged anybody.
                free_used = int(row["free_used"])
                balance_after = balance

            conn.execute(
                "UPDATE account SET balance = ?, free_used = ?, spent = spent + ?, "
                "updated_at = ? WHERE id = ?",
                (balance_after, free_used, cost, _now_iso(), account_id),
            )
            note = None if gate else "metering only; enforcement is off"
            if shortfall:
                note = (
                    f"{shortfall} credit(s) could not be paid — the run had already "
                    f"written. The next run is refused."
                )
            entry_id = self._write_entry(
                conn, account_id=account_id, amount=-cost, reason=REASON_RUN,
                applied=gate, balance_after=balance_after, job_run_id=job_run_id,
                pipeline_run_id=pipeline_run_id, target=target.label, job_name=job_name,
                note=note, writes=writes, free_amount=from_free if gate else 0,
                shortfall=shortfall, period=str(row["period"] or current_period()),
                workflow_id=workflow_id, actor=actor, tags=tags,
            )
            conn.commit()
        return Charge(
            account_id=account_id, amount=cost, applied=gate, balance_after=balance_after,
            target=target.label, writes=writes, free_amount=from_free if gate else 0,
            shortfall=shortfall, entry_id=entry_id,
        )

    # ---- reading ---------------------------------------------------------

    def ledger(
        self, account_id: Optional[str] = None, limit: int = 100,
        *, period: Optional[str] = None,
    ) -> List[Entry]:
        limit = max(1, min(int(limit), 1000))
        clauses: List[str] = []
        params: List[Any] = []
        if account_id:
            clauses.append("account_id = ?")
            params.append(account_id)
        if period:
            clauses.append("period = ?")
            params.append(period)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(
                f"SELECT * FROM ledger {where} ORDER BY created_at DESC, rowid DESC LIMIT ?",
                tuple(params),
            ).fetchall()
            return self._with_tags(conn, [self._entry_of(row) for row in rows])

    def entries_for_job_runs(self, job_run_ids: Iterable[str]) -> Dict[str, Entry]:
        """What each of these job runs was charged, keyed by job run id.

        This is what puts a price on the execution-history screen: the run detail
        already knows what it wrote, and this says what that cost.
        """
        ids = [str(item) for item in job_run_ids if item]
        if not ids:
            return {}
        found: Dict[str, Entry] = {}
        with self._lock, closing(self._connect()) as conn:
            for start in range(0, len(ids), 400):
                chunk = ids[start:start + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"SELECT * FROM ledger WHERE job_run_id IN ({placeholders}) "
                    f"AND reason = ?",
                    (*chunk, REASON_RUN),
                ).fetchall()
                for row in rows:
                    found[row["job_run_id"]] = self._entry_of(row)
            self._with_tags(conn, list(found.values()))
        return found

    def usage(self, account_id: str, period: Optional[str] = None) -> Dict[str, int]:
        """A month in three numbers: writes, credits charged, credits waived.

        Waived is what the free allowance covered. Keeping it separate is the
        difference between "you used 40 of your 40 free" and "you owe 40".
        """
        wanted = period or current_period()
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(writes), 0) AS writes, "
                "       COALESCE(SUM(-amount), 0) AS charged, "
                "       COALESCE(SUM(free_amount), 0) AS waived "
                "FROM ledger WHERE account_id = ? AND period = ? AND reason = ?",
                (account_id, wanted, REASON_RUN),
            ).fetchone()
        return {
            "period": wanted, "writes": int(row["writes"]),
            "charged": int(row["charged"]), "waived": int(row["waived"]),
        }

    #: What a bill can be sliced by, and the ledger column each slice reads.
    #: The account is always the payer — these only decide how one account's month
    #: is read back, which is why "team" is here alongside the rest.
    GROUPS = {
        "team": "account_id",
        "user": "actor",
        "workflow": "workflow_id",
        "job": "job_name",
        # Not a column: tags are many per entry, so this one is a join. See
        # `_breakdown_by_tag`, and mind that its rows overlap.
        "tag": None,
    }

    def breakdown(
        self, *, group_by: str = "workflow", period: Optional[str] = None,
        account_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """One month of run charges, grouped by team, user, workflow or job.

        Entries written before a dimension existed have it null, and so do runs
        that genuinely have none — a job started from a script belongs to no
        workflow. Those are grouped under a null key rather than dropped: a total
        that silently omits part of the spending is worse than an "unattributed"
        row that says so.
        """
        if group_by not in self.GROUPS:
            raise CreditError(f"Cannot group spending by {group_by!r}.")
        if group_by == "tag":
            return self._breakdown_by_tag(
                period=period or current_period(), account_id=account_id
            )
        column = self.GROUPS[group_by]
        wanted = period or current_period()
        query = (
            f"SELECT {column} AS key, "
            "       COALESCE(SUM(writes), 0) AS writes, "
            "       COALESCE(SUM(-amount), 0) AS charged, "
            "       COALESCE(SUM(free_amount), 0) AS waived, "
            "       COUNT(*) AS runs, "
            "       MAX(created_at) AS last_at "
            "FROM ledger WHERE period = ? AND reason = ?"
        )
        args: List[Any] = [wanted, REASON_RUN]
        if account_id:
            query += " AND account_id = ?"
            args.append(account_id)
        query += f" GROUP BY {column} ORDER BY charged DESC, writes DESC"
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(query, args).fetchall()
            names = {
                str(item["id"]): str(item["username"])
                for item in conn.execute("SELECT id, username FROM account").fetchall()
            }
        out: List[Dict[str, Any]] = []
        for row in rows:
            key = row["key"]
            out.append({
                "key": key,
                "label": names.get(str(key), str(key)) if key else None,
                "writes": int(row["writes"]),
                "charged": int(row["charged"]),
                "waived": int(row["waived"]),
                "runs": int(row["runs"]),
                "last_at": row["last_at"],
            })
        return out

    def totals(
        self, *, period: Optional[str] = None, account_id: Optional[str] = None
    ) -> Dict[str, int]:
        """The month's real total, whatever it is being sliced by.

        Every dimension but `tag` partitions the ledger, so a reader could add the
        rows up and get here. Tags do not partition anything — a run wearing two
        of them appears under both — so the total has to be counted once, from the
        entries themselves, or the screen would claim the month cost twice what it
        did.
        """
        wanted = period or current_period()
        clauses = ["period = ?", "reason = ?"]
        args: List[Any] = [wanted, REASON_RUN]
        if account_id:
            clauses.append("account_id = ?")
            args.append(account_id)
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(writes), 0) AS writes, "
                "       COALESCE(SUM(-amount), 0) AS charged, "
                "       COALESCE(SUM(free_amount), 0) AS waived, "
                "       COUNT(*) AS runs "
                f"FROM ledger WHERE {' AND '.join(clauses)}",
                tuple(args),
            ).fetchone()
        return {
            "writes": int(row["writes"]), "charged": int(row["charged"]),
            "waived": int(row["waived"]), "runs": int(row["runs"]),
        }

    def _breakdown_by_tag(
        self, *, period: str, account_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """The same month sliced by tag, plus one row for what carries none.

        Unlike every other dimension, **these rows overlap**: an entry tagged
        both `finance` and `nightly` is counted in full under each, so the column
        adds up to more than the invoice. That is what makes the slice useful —
        "what does finance cost me" is a real question whose answer does not care
        that the run was also nightly — but it is why the total shown to a reader
        has to come from `usage()`, never from summing these rows.
        """
        scope = " AND l.account_id = ?" if account_id else ""
        args: List[Any] = [period, REASON_RUN]
        if account_id:
            args.append(account_id)
        aggregates = (
            "COALESCE(SUM(l.writes), 0) AS writes, "
            "COALESCE(SUM(-l.amount), 0) AS charged, "
            "COALESCE(SUM(l.free_amount), 0) AS waived, "
            "COUNT(*) AS runs, "
            "MAX(l.created_at) AS last_at "
        )
        tagged = (
            f"SELECT t.tag AS key, {aggregates}"
            "FROM ledger l JOIN ledger_tag t ON t.entry_id = l.id "
            f"WHERE l.period = ? AND l.reason = ?{scope} GROUP BY t.tag"
        )
        untagged = (
            f"SELECT NULL AS key, {aggregates}"
            "FROM ledger l "
            f"WHERE l.period = ? AND l.reason = ?{scope} "
            "AND NOT EXISTS (SELECT 1 FROM ledger_tag t WHERE t.entry_id = l.id) "
            "HAVING COUNT(*) > 0"
        )
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(
                f"{tagged} UNION ALL {untagged} ORDER BY charged DESC, writes DESC",
                (*args, *args),
            ).fetchall()
        return [{
            "key": row["key"],
            "label": str(row["key"]) if row["key"] else None,
            "writes": int(row["writes"]),
            "charged": int(row["charged"]),
            "waived": int(row["waived"]),
            "runs": int(row["runs"]),
            "last_at": row["last_at"],
        } for row in rows]

    def usage_timeline(
        self, *, months: int = 6, account_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """One row per month, oldest first — the shape of the spending over time.

        A single month answers "how much"; only the series answers "is this
        normal", which is the question somebody staring at a bill actually has.
        Months with no spending are present with zeros, so a gap reads as a quiet
        month instead of disappearing and making the line lie.
        """
        months = max(1, min(int(months), 36))
        clauses = ["reason = ?"]
        args: List[Any] = [REASON_RUN]
        if account_id:
            clauses.append("account_id = ?")
            args.append(account_id)
        where = " AND ".join(clauses)
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT period, COALESCE(SUM(writes), 0) AS writes, "
                "       COALESCE(SUM(-amount), 0) AS charged, "
                "       COALESCE(SUM(free_amount), 0) AS waived, "
                "       COUNT(*) AS runs "
                f"FROM ledger WHERE {where} GROUP BY period",
                tuple(args),
            ).fetchall()
        found = {str(row["period"] or ""): row for row in rows}
        out: List[Dict[str, Any]] = []
        for period in recent_periods(months):
            row = found.get(period)
            out.append({
                "period": period,
                "writes": int(row["writes"]) if row else 0,
                "charged": int(row["charged"]) if row else 0,
                "waived": int(row["waived"]) if row else 0,
                "runs": int(row["runs"]) if row else 0,
            })
        return out

    # ---- internals -------------------------------------------------------

    def _ensure(
        self, conn: sqlite3.Connection, account_id: str, username: Optional[str]
    ) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM account WHERE id = ?", (account_id,)).fetchone()
        period = current_period()
        if row is not None:
            if username and username != row["username"]:
                # The id is what pays; the name is a label, and it can change when
                # a team is renamed.
                conn.execute(
                    "UPDATE account SET username = ? WHERE id = ?", (username, account_id)
                )
            if str(row["period"] or "") != period:
                # The month turned. The allowance refills and does not carry over.
                conn.execute(
                    "UPDATE account SET period = ?, free_used = 0, updated_at = ? WHERE id = ?",
                    (period, _now_iso(), account_id),
                )
            return conn.execute(
                "SELECT * FROM account WHERE id = ?", (account_id,)
            ).fetchone()
        now = _now_iso()
        opening = initial_balance()
        conn.execute(
            "INSERT INTO account (id, username, balance, spent, period, free_used, "
            "created_at, updated_at) VALUES (?, ?, ?, 0, ?, 0, ?, ?)",
            (account_id, username or account_id, opening, period, now, now),
        )
        if opening:
            self._write_entry(
                conn, account_id=account_id, amount=opening, reason=REASON_GRANT,
                applied=True, balance_after=opening, note="opening balance", period=period,
            )
        return conn.execute("SELECT * FROM account WHERE id = ?", (account_id,)).fetchone()

    @staticmethod
    def _write_entry(
        conn: sqlite3.Connection, *, account_id: str, amount: int, reason: str,
        applied: bool, balance_after: int, job_run_id: Optional[str] = None,
        pipeline_run_id: Optional[str] = None, target: Optional[str] = None,
        job_name: Optional[str] = None, note: Optional[str] = None, writes: int = 0,
        free_amount: int = 0, shortfall: int = 0, period: Optional[str] = None,
        workflow_id: Optional[str] = None, actor: Optional[str] = None,
        tags: Optional[Sequence[str]] = None,
    ) -> str:
        entry_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO ledger (id, account_id, amount, reason, applied, balance_after, "
            "writes, free_amount, shortfall, period, job_run_id, pipeline_run_id, target, "
            "job_name, note, workflow_id, actor, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entry_id, account_id, amount, reason, 1 if applied else 0, balance_after,
                writes, free_amount, shortfall, period or current_period(), job_run_id,
                pipeline_run_id, target, job_name, note, workflow_id, actor,
                _now_iso(),
            ),
        )
        for tag in normalize_tags(tags):
            conn.execute(
                "INSERT OR REPLACE INTO ledger_tag (entry_id, tag) VALUES (?, ?)",
                (entry_id, tag),
            )
        return entry_id

    @staticmethod
    def _with_tags(conn: sqlite3.Connection, entries: List[Entry]) -> List[Entry]:
        """Fills `tags` for a page of entries in one query, not one query each."""
        ids = [entry.id for entry in entries if entry.id]
        if not ids:
            return entries
        by_entry: Dict[str, List[str]] = {}
        for start in range(0, len(ids), 400):
            chunk = ids[start:start + 400]
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"SELECT entry_id, tag FROM ledger_tag WHERE entry_id IN ({placeholders}) "
                "ORDER BY tag",
                tuple(chunk),
            ).fetchall()
            for row in rows:
                by_entry.setdefault(str(row["entry_id"]), []).append(str(row["tag"]))
        for entry in entries:
            entry.tags = by_entry.get(entry.id, [])
        return entries

    @staticmethod
    def _account_of(row: sqlite3.Row) -> Account:
        return Account(
            id=row["id"], username=row["username"], balance=int(row["balance"]),
            spent=int(row["spent"]), period=str(row["period"] or current_period()),
            free_used=int(row["free_used"]), free_monthly=free_monthly(),
            held=int(row["held"] or 0),
            created_at=row["created_at"], updated_at=row["updated_at"],
        )

    @staticmethod
    def _entry_of(row: sqlite3.Row) -> Entry:
        return Entry(
            id=row["id"], account_id=row["account_id"], amount=int(row["amount"]),
            reason=row["reason"], applied=bool(row["applied"]),
            balance_after=int(row["balance_after"]), created_at=row["created_at"],
            writes=int(row["writes"] or 0), free_amount=int(row["free_amount"] or 0),
            shortfall=int(row["shortfall"] or 0), period=row["period"],
            job_run_id=row["job_run_id"], pipeline_run_id=row["pipeline_run_id"],
            target=row["target"], job_name=row["job_name"], note=row["note"],
            workflow_id=row["workflow_id"], actor=row["actor"],
        )


def actor_for(principal: Any) -> Optional[str]:
    """Who to put on the ledger entry — the person, not the payer.

    The account is the team; this is the member of it whose token was used, which
    is what makes "who spent the month's credits" answerable. A shared runner
    token names nobody, and says so with `None` rather than inventing a user.
    """
    if principal is None or getattr(principal, "token_only", False):
        return None
    return getattr(principal, "username", None) or getattr(principal, "user_id", None)


def account_for(principal: Any) -> Tuple[str, str]:
    """Which account pays for what this caller runs, and what to call it.

    The team, when there is one — a squad has one budget, and a balance that
    belonged to whoever happened to press Run would stop the nightly Job the day
    that person left. A runner with no users has no teams either, and there the
    single holder of the shared token is the single account.
    """
    if principal is None:
        return TOKEN_ACCOUNT, TOKEN_ACCOUNT
    if getattr(principal, "token_only", False):
        return TOKEN_ACCOUNT, TOKEN_ACCOUNT
    team_id = getattr(principal, "team_id", None)
    if team_id:
        return str(team_id), str(getattr(principal, "team_name", None) or team_id)
    user_id = getattr(principal, "user_id", None)
    if user_id:
        # A user with no team should not exist — `auth` puts everyone in one — but
        # charging them personally beats refusing to run.
        return str(user_id), str(getattr(principal, "username", None) or user_id)
    return TOKEN_ACCOUNT, TOKEN_ACCOUNT
