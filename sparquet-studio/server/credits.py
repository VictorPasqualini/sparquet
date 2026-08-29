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
  successful writes is known. What happens *before* the run is a cheaper check:
  with enforcement on, a team with nothing available at all is refused with `402`
  before Spark is started. A run that got past that check and then wrote more
  than it could pay for is charged down to zero and the shortfall is recorded on
  the entry — the work is done, the cluster time is spent, and pretending
  otherwise would make the ledger a worse record than no ledger. The next run is
  the one that gets refused.

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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


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


# -------------------------------------------------------------------- storage


_SCHEMA = """
CREATE TABLE IF NOT EXISTS account (
  id TEXT PRIMARY KEY,
  username TEXT NOT NULL,
  balance INTEGER NOT NULL DEFAULT 0,
  spent INTEGER NOT NULL DEFAULT 0,
  period TEXT,
  free_used INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

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
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ledger_account ON ledger(account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ledger_job_run ON ledger(job_run_id);
"""

#: Columns added after the first release. Applied on every start to a database
#: written before they existed, so an upgrade asks the operator for nothing.
_ADDED_COLUMNS = {
    "account": {
        "period": "ALTER TABLE account ADD COLUMN period TEXT",
        "free_used": "ALTER TABLE account ADD COLUMN free_used INTEGER NOT NULL DEFAULT 0",
    },
    "ledger": {
        "writes": "ALTER TABLE ledger ADD COLUMN writes INTEGER NOT NULL DEFAULT 0",
        "free_amount": "ALTER TABLE ledger ADD COLUMN free_amount INTEGER NOT NULL DEFAULT 0",
        "shortfall": "ALTER TABLE ledger ADD COLUMN shortfall INTEGER NOT NULL DEFAULT 0",
        "period": "ALTER TABLE ledger ADD COLUMN period TEXT",
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
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @property
    def free_remaining(self) -> int:
        return max(0, self.free_monthly - self.free_used)

    @property
    def available(self) -> int:
        """What could be spent right now: this month's allowance plus the balance."""
        return self.free_remaining + self.balance


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

    def charge(
        self, account_id: str, target: Target, writes: int, *,
        username: Optional[str] = None, job_run_id: Optional[str] = None,
        pipeline_run_id: Optional[str] = None, job_name: Optional[str] = None,
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
        return [self._entry_of(row) for row in rows]

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
    ) -> str:
        entry_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO ledger (id, account_id, amount, reason, applied, balance_after, "
            "writes, free_amount, shortfall, period, job_run_id, pipeline_run_id, target, "
            "job_name, note, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entry_id, account_id, amount, reason, 1 if applied else 0, balance_after,
                writes, free_amount, shortfall, period or current_period(), job_run_id,
                pipeline_run_id, target, job_name, note, _now_iso(),
            ),
        )
        return entry_id

    @staticmethod
    def _account_of(row: sqlite3.Row) -> Account:
        return Account(
            id=row["id"], username=row["username"], balance=int(row["balance"]),
            spent=int(row["spent"]), period=str(row["period"] or current_period()),
            free_used=int(row["free_used"]), free_monthly=free_monthly(),
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
        )


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
