"""Execution credits: one coin per Job, and only when the Job leaves this machine.

Running a Job on the laptop that is already running the runner costs nobody
anything — the operator is spending their own CPU, and charging for it would be
theatre. Running the same JSON against a cluster spends money that somebody else
is paying for, and that is the thing worth counting. So the rule is narrow on
purpose: **a Job charges one credit when its execution is not local**, and a
Pipeline of five remote Jobs charges five, one at a time, as each stage starts.

What counts as local is decided from the configuration that is about to run,
not from a flag the caller sends: `spark.master` says `local[...]`, no Spark
Connect endpoint is configured, and the runner itself is not already inside a
managed cluster (Databricks, EMR, Dataproc, Synapse — the same detection
`sparquet.core.context` uses, so the answer here and the answer there cannot
drift). Anything else — `spark://`, `yarn`, `k8s://`, a `spark.remote` URL, a
runner living inside Databricks — is remote and costs a coin.

**Metering and enforcement are separate, and that is the important part.**
Every remote Job is always recorded in the ledger, from the first run, without
anybody turning anything on. Only when `SPARQUET_STUDIO_CREDITS=on` does a
balance actually gate execution and a run get refused with `402`. The split
exists so that upgrading a runner never stops work that used to run: an
administrator can look at a month of real consumption first, grant balances that
match it, and only then start enforcing. It is also why an account carries both
`balance` and `spent`: while metering, `spent` climbs and `balance` is untouched,
so switching enforcement on starts from what was granted rather than from an
accumulated debt nobody agreed to.

Charging happens at admission, before the framework is handed the configuration,
and there is **no refund once execution starts** — a cluster that ran for twenty
minutes and then failed cost real money, and pretending otherwise would make the
ledger a worse record than no ledger. A run rejected before it starts (no
credit, the runner busy, a configuration that does not parse in `dry_run`) is
never charged in the first place.

The ledger is append-only and lives in its own SQLite file
(`server/data/credits.sqlite3`, override with `SPARQUET_STUDIO_CREDITS_DB`),
apart from identity: it grows with every execution, and an operator may well
want to archive or reset a year of it without going anywhere near the
credentials.
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
from typing import Any, Dict, List, Optional, Tuple


class CreditError(Exception):
    """A charge that cannot go through, or an amount that makes no sense.

    `main.py` turns the out-of-credit case into a `402` and the rest into a
    `400`; both carry the message, because each one tells the caller what to do.
    """


class InsufficientCredits(CreditError):
    """The account cannot pay for this execution."""

    def __init__(self, account_id: str, balance: int, needed: int) -> None:
        super().__init__(
            f"Not enough execution credits: this run needs {needed} and the account "
            f"has {balance}. Local runs are free; this one targets a cluster. An "
            f"administrator can grant more with POST /credits/{account_id}/grant."
        )
        self.account_id = account_id
        self.balance = balance
        self.needed = needed


#: The account a runner in token-only mode charges. There is exactly one
#: operator there — the holder of the shared token — so there is one account.
TOKEN_ACCOUNT = "token"

#: Recorded on the ledger row, so a listing can say what was paid for.
REASON_RUN = "run"
REASON_GRANT = "grant"
REASON_ADJUST = "adjust"


def enforced() -> bool:
    """Whether a balance actually gates execution, or the ledger is only watching.

    Off by default: turning credits on for a runner that has never had them would
    stop every remote Job at once, and an upgrade must not do that.
    """
    return os.getenv("SPARQUET_STUDIO_CREDITS", "").strip().lower() in {
        "1", "on", "true", "yes", "enforce",
    }


def credits_per_job() -> int:
    """One coin per Job, unless the operator says otherwise."""
    try:
        return max(0, int(os.getenv("SPARQUET_STUDIO_CREDITS_PER_JOB", "1")))
    except ValueError:
        return 1


def initial_balance() -> int:
    """What a brand-new account starts with. Zero, so nothing is given away by
    accident; an administrator grants deliberately."""
    try:
        return max(0, int(os.getenv("SPARQUET_STUDIO_CREDITS_INITIAL", "0")))
    except ValueError:
        return 0


def default_db_path() -> Path:
    configured = os.getenv("SPARQUET_STUDIO_CREDITS_DB", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parent / "data" / "credits.sqlite3"


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
    """Where a configuration is about to run, and what that costs."""

    local: bool
    #: Human-readable, for the ledger and for the error message: `local[*]`,
    #: `spark://cluster:7077`, `databricks`.
    label: str
    cost: int


def target_of(pipeline: Dict[str, Any]) -> Target:
    """Read the destination out of the configuration itself.

    Out of the configuration and not out of the request, because the request is
    written by the caller and the configuration is what Spark will obey. A caller
    who could declare their own run "local" would have found the way to run for
    free.
    """
    environment = _runner_environment()
    if environment != "local":
        # A runner living inside a managed cluster: `spark.master` is ignored
        # there, the session belongs to the platform, and every Job is remote.
        return Target(local=False, label=environment, cost=credits_per_job())

    spark = pipeline.get("spark") if isinstance(pipeline, dict) else None
    spark = spark if isinstance(spark, dict) else {}
    configs = spark.get("configs")
    configs = configs if isinstance(configs, dict) else {}

    remote = str(configs.get("spark.remote", "") or "").strip()
    if remote:
        # Spark Connect: the master is irrelevant, the work happens over there.
        return Target(local=False, label=f"connect {remote}", cost=credits_per_job())

    master = str(configs.get("spark.master") or spark.get("master") or "local[*]").strip()
    if master.startswith("local"):
        return Target(local=True, label=master, cost=0)
    return Target(local=False, label=master, cost=credits_per_job())


# -------------------------------------------------------------------- storage


_SCHEMA = """
CREATE TABLE IF NOT EXISTS account (
  id TEXT PRIMARY KEY,
  username TEXT NOT NULL,
  balance INTEGER NOT NULL DEFAULT 0,
  spent INTEGER NOT NULL DEFAULT 0,
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
  job_run_id TEXT,
  pipeline_run_id TEXT,
  target TEXT,
  job_name TEXT,
  note TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ledger_account ON ledger(account_id, created_at DESC);
"""


@dataclass
class Account:
    id: str
    username: str
    balance: int
    spent: int
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class Entry:
    id: str
    account_id: str
    amount: int
    reason: str
    applied: bool
    balance_after: int
    created_at: str
    job_run_id: Optional[str] = None
    pipeline_run_id: Optional[str] = None
    target: Optional[str] = None
    job_name: Optional[str] = None
    note: Optional[str] = None


@dataclass
class Charge:
    """What a single admission cost, and where it is recorded.

    Returned even when nothing was taken (`amount == 0`), so a caller can write
    one line — "this run cost N" — instead of branching on local versus remote.
    """

    account_id: str
    amount: int
    applied: bool
    balance_after: int
    target: str
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
    per execution, and the lock is what makes "check the balance, then take from
    it" a single decision rather than two racing ones.
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._path = Path(db_path) if db_path else default_db_path()
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, closing(self._connect()) as conn:
            conn.executescript(_SCHEMA)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, timeout=5)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn

    # ---- accounts --------------------------------------------------------

    def account(self, account_id: str, username: Optional[str] = None) -> Account:
        """The account, created on first sight with the configured opening
        balance. Created lazily because an account is a consequence of running
        something, not a step somebody has to remember."""
        with self._lock, closing(self._connect()) as conn:
            row = self._ensure(conn, account_id, username)
            conn.commit()
            return self._account_of(row)

    def list_accounts(self) -> List[Account]:
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute("SELECT * FROM account ORDER BY username").fetchall()
        return [self._account_of(row) for row in rows]

    def grant(
        self, account_id: str, amount: int, *, username: Optional[str] = None,
        note: Optional[str] = None, reason: str = REASON_GRANT,
    ) -> Account:
        """Add credits (or, with a negative amount, take them back).

        A correction is a grant with a negative amount rather than a separate
        operation, so every movement of every account is one table read.
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
            )
            conn.commit()
            return self._account_of(
                conn.execute("SELECT * FROM account WHERE id = ?", (account_id,)).fetchone()
            )

    # ---- charging --------------------------------------------------------

    def charge(
        self, account_id: str, target: Target, *, username: Optional[str] = None,
        job_run_id: Optional[str] = None, pipeline_run_id: Optional[str] = None,
        job_name: Optional[str] = None,
    ) -> Charge:
        """Take the cost of one Job, or explain why it cannot be taken.

        A local target costs nothing and writes nothing — the ledger is about
        cluster work, and filling it with free rows would bury the ones that
        matter. A remote target always writes a row; whether the balance moves
        depends on `enforced()`.
        """
        if target.cost <= 0:
            return Charge(
                account_id=account_id, amount=0, applied=False,
                balance_after=self.account(account_id, username).balance,
                target=target.label,
            )

        gate = enforced()
        with self._lock, closing(self._connect()) as conn:
            row = self._ensure(conn, account_id, username)
            balance = int(row["balance"])
            if gate and balance < target.cost:
                conn.commit()
                raise InsufficientCredits(account_id, balance, target.cost)
            balance_after = balance - target.cost if gate else balance
            conn.execute(
                "UPDATE account SET balance = ?, spent = spent + ?, updated_at = ? "
                "WHERE id = ?",
                (balance_after, target.cost, _now_iso(), account_id),
            )
            entry_id = self._write_entry(
                conn, account_id=account_id, amount=-target.cost, reason=REASON_RUN,
                applied=gate, balance_after=balance_after, job_run_id=job_run_id,
                pipeline_run_id=pipeline_run_id, target=target.label, job_name=job_name,
                note=None if gate else "metering only; enforcement is off",
            )
            conn.commit()
        return Charge(
            account_id=account_id, amount=target.cost, applied=gate,
            balance_after=balance_after, target=target.label, entry_id=entry_id,
        )

    def ledger(self, account_id: Optional[str] = None, limit: int = 100) -> List[Entry]:
        limit = max(1, min(int(limit), 1000))
        with self._lock, closing(self._connect()) as conn:
            if account_id:
                rows = conn.execute(
                    "SELECT * FROM ledger WHERE account_id = ? ORDER BY created_at DESC, "
                    "rowid DESC LIMIT ?",
                    (account_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM ledger ORDER BY created_at DESC, rowid DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [self._entry_of(row) for row in rows]

    # ---- internals -------------------------------------------------------

    def _ensure(
        self, conn: sqlite3.Connection, account_id: str, username: Optional[str]
    ) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM account WHERE id = ?", (account_id,)).fetchone()
        if row is not None:
            if username and username != row["username"]:
                # The id is what pays; the name is a label, and it can change.
                conn.execute(
                    "UPDATE account SET username = ? WHERE id = ?", (username, account_id)
                )
                row = conn.execute(
                    "SELECT * FROM account WHERE id = ?", (account_id,)
                ).fetchone()
            return row
        now = _now_iso()
        opening = initial_balance()
        conn.execute(
            "INSERT INTO account (id, username, balance, spent, created_at, updated_at) "
            "VALUES (?, ?, ?, 0, ?, ?)",
            (account_id, username or account_id, opening, now, now),
        )
        if opening:
            self._write_entry(
                conn, account_id=account_id, amount=opening, reason=REASON_GRANT,
                applied=True, balance_after=opening, note="opening balance",
            )
        return conn.execute("SELECT * FROM account WHERE id = ?", (account_id,)).fetchone()

    @staticmethod
    def _write_entry(
        conn: sqlite3.Connection, *, account_id: str, amount: int, reason: str,
        applied: bool, balance_after: int, job_run_id: Optional[str] = None,
        pipeline_run_id: Optional[str] = None, target: Optional[str] = None,
        job_name: Optional[str] = None, note: Optional[str] = None,
    ) -> str:
        entry_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO ledger (id, account_id, amount, reason, applied, balance_after, "
            "job_run_id, pipeline_run_id, target, job_name, note, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entry_id, account_id, amount, reason, 1 if applied else 0, balance_after,
                job_run_id, pipeline_run_id, target, job_name, note, _now_iso(),
            ),
        )
        return entry_id

    @staticmethod
    def _account_of(row: sqlite3.Row) -> Account:
        return Account(
            id=row["id"], username=row["username"], balance=int(row["balance"]),
            spent=int(row["spent"]), created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _entry_of(row: sqlite3.Row) -> Entry:
        return Entry(
            id=row["id"], account_id=row["account_id"], amount=int(row["amount"]),
            reason=row["reason"], applied=bool(row["applied"]),
            balance_after=int(row["balance_after"]), created_at=row["created_at"],
            job_run_id=row["job_run_id"], pipeline_run_id=row["pipeline_run_id"],
            target=row["target"], job_name=row["job_name"], note=row["note"],
        )


def account_for(principal: Any) -> Tuple[str, str]:
    """`(account id, label)` for whoever is running.

    The user id rather than the username, because a rename must not hand somebody
    a fresh balance. In token-only mode there is no user, and the single shared
    account is the honest answer.
    """
    user_id = getattr(principal, "user_id", None)
    username = str(getattr(principal, "username", "") or TOKEN_ACCOUNT)
    if user_id:
        return str(user_id), username
    return TOKEN_ACCOUNT, username
