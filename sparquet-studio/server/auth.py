"""Who is using this runner, and what they are allowed to do.

The runner executes arbitrary Spark work against whatever the machine can reach,
so the shared token in `main.py` is a password: it says the caller is allowed to
talk to this process at all. That is the right check for one person on one laptop
and the wrong one for a team, where "may run a job" and "may delete a Workflow"
are different answers for different people.

This module adds the second half — identity and permissions — without weakening
the first: the shared token is still required on every request, and everything
here happens on top of it.

**Two modes, decided by whether any user exists.** A runner with no users behaves
exactly as it always has: the token alone authorizes everything, and nobody is
locked out by upgrading. Create the first user (`python server/auth.py
create-admin`, or `POST /auth/users` while the store is empty) and the runner
starts demanding a session on top of the token, for everything except logging in.

**Permissions are IAM-shaped**: a role carries policy statements, a statement is
`{effect, actions, resources}`, and an explicit `deny` beats any `allow`. An
action is `service:Verb` (`workspace:Write`, `run:Execute`, `iam:ManageUsers`); a
resource is `kind/id` (`workflow/w1`, `job/*`) or `*`. That shape is worth
copying because it is the one people already know, and because scoping a role to
one Workflow later is then a data change rather than a code change.

Storage is its own SQLite file (`server/data/auth.sqlite3`, override with
`SPARQUET_STUDIO_AUTH_DB`): identity is not execution history, and keeping it
apart means a history database can be copied for debugging without carrying
credentials along.

Passwords are stored as scrypt hashes (PBKDF2 where the interpreter has no
scrypt), never in plain text, and session tokens are stored hashed too — a stolen
copy of this file must not hand over live sessions.

**Password recovery is a code, not an email.** The runner has no mail server and
should not grow one, so a forgotten password is repaired by whoever can already
reach the machine or already administers it: `recovery-code` mints a single-use
code with a short life, and `POST /auth/recover` trades that code for a new
password. The code is stored hashed like everything else here, expires, dies when
used, and replaces any earlier unused code for the same person. Handing it over
is out of band on purpose — the person doing that already has enough power to
reset the password outright, so the code adds no new authority, only a way to let
somebody choose their own secret.

Run standalone for the operator commands:

    python server/auth.py create-admin
    python server/auth.py list-users
    python server/auth.py reset-password <username>
    python server/auth.py recovery-code <username>
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import sqlite3
import threading
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence


class AuthError(Exception):
    """Something the caller asked for is not allowed or not possible.

    Distinct from a programming error on purpose: `main.py` turns these into a
    400 with the message, because every one of them is worth reading.
    """


# --------------------------------------------------------------------- model


@dataclass
class User:
    id: str
    username: str
    display_name: Optional[str]
    roles: List[str]
    disabled: bool = False
    created_at: Optional[str] = None
    last_login_at: Optional[str] = None


@dataclass
class Role:
    name: str
    description: str
    #: Policy statements, in the shape `{"effect", "actions", "resources"}`.
    statements: List[Dict[str, Any]] = field(default_factory=list)
    #: False for the roles this module ships and keeps up to date.
    custom: bool = False


@dataclass
class Session:
    token: str
    user: User
    expires_at: str


@dataclass
class Principal:
    """Whoever is making the current request, and what they may do.

    `token_only` is the single-user mode: no users exist, so the shared token is
    the identity. It is given the admin policy deliberately — that runner has
    exactly one operator, and pretending otherwise would only mean refusing them
    their own machine.
    """

    username: str
    display_name: Optional[str] = None
    user_id: Optional[str] = None
    roles: List[str] = field(default_factory=list)
    statements: List[Dict[str, Any]] = field(default_factory=list)
    token_only: bool = False

    def allows(self, action: str, resource: str = "*") -> bool:
        return authorize(self.statements, action, resource)


#: The identity of a runner that has no users: the shared token, with the admin
#: policy. See `Principal.token_only`.
TOKEN_PRINCIPAL = Principal(
    username="local",
    display_name="Local runner token",
    roles=["admin"],
    statements=[{"effect": "allow", "actions": ["*"], "resources": ["*"]}],
    token_only=True,
)


# ------------------------------------------------------------------ policies

#: Every action the runner checks, with what it guards. Kept here rather than
#: spread through the endpoints so a reader can see the whole surface at once,
#: and so `GET /auth/roles` can describe a role in words.
ACTIONS: Dict[str, str] = {
    "workspace:Read": "Read the Workflows, Jobs and Pipelines in the library.",
    "workspace:Write": "Create or change a record in the library.",
    "workspace:Delete": "Remove a record from the library.",
    "run:Execute": "Run a Job or a Pipeline on this runner.",
    "run:Cancel": "Stop a run that is in progress.",
    "run:Validate": "Check a JSON without running it.",
    "history:Read": "Read past executions, their steps, logs and configuration.",
    "iam:ReadUsers": "See who has access and with which roles.",
    "iam:ManageUsers": "Create users, change roles, reset passwords, remove access.",
    "credits:Read": "See every account's execution credits and what they were spent on.",
    "credits:Manage": "Grant execution credits, or take them back.",
}

#: The roles the runner ships with. Rewritten on every start, so fixing a policy
#: here fixes it everywhere; a role someone created themselves is left alone.
BUILTIN_ROLES: Dict[str, Role] = {
    "admin": Role(
        name="admin",
        description="Everything, including who else has access.",
        statements=[{"effect": "allow", "actions": ["*"], "resources": ["*"]}],
    ),
    "editor": Role(
        name="editor",
        description="Build and run pipelines. Cannot manage users.",
        statements=[
            {
                "effect": "allow",
                "actions": ["workspace:*", "run:*", "history:Read", "credits:Read"],
                "resources": ["*"],
            }
        ],
    ),
    "operator": Role(
        name="operator",
        description="Run what already exists and read the results. Cannot edit.",
        statements=[
            {
                "effect": "allow",
                "actions": [
                    "workspace:Read",
                    "run:Execute",
                    "run:Cancel",
                    "run:Validate",
                    "history:Read",
                ],
                "resources": ["*"],
            }
        ],
    ),
    "viewer": Role(
        name="viewer",
        description="Read the library and the history. Changes nothing.",
        statements=[
            {
                "effect": "allow",
                "actions": ["workspace:Read", "history:Read"],
                "resources": ["*"],
            }
        ],
    ),
}

DEFAULT_ROLE = "viewer"


def _matches(pattern: str, value: str) -> bool:
    """IAM-style match: `*` anywhere, and nothing else.

    Deliberately not a regular expression — a policy is read by people under
    pressure, and `workspace:*` has to mean what it looks like.
    """
    if pattern == "*":
        return True
    if "*" not in pattern:
        return pattern == value
    head, _, tail = pattern.partition("*")
    if not value.startswith(head) or not value.endswith(tail):
        return False
    return len(value) >= len(head) + len(tail)


def _statement_hits(statement: Dict[str, Any], action: str, resource: str) -> bool:
    actions = statement.get("actions") or []
    resources = statement.get("resources") or ["*"]
    if not isinstance(actions, list) or not isinstance(resources, list):
        return False
    return any(_matches(str(item), action) for item in actions) and any(
        _matches(str(item), resource) for item in resources
    )


def authorize(
    statements: Sequence[Dict[str, Any]], action: str, resource: str = "*"
) -> bool:
    """Whether these policies allow `action` on `resource`.

    Deny wins, and the default is no. Both are the IAM rules, and both matter for
    the same reason: a policy that can be *widened* by adding a statement is one
    where revoking access means auditing every role someone holds. Here, one deny
    settles it.
    """
    allowed = False
    for statement in statements:
        if not isinstance(statement, dict) or not _statement_hits(statement, action, resource):
            continue
        if str(statement.get("effect", "allow")).lower() == "deny":
            return False
        allowed = True
    return allowed


# ------------------------------------------------------------------ passwords

#: scrypt cost. 2**14 keeps a login around a tenth of a second on a laptop, which
#: is the point: cheap once, expensive a million times.
_SCRYPT_N = 1 << 14
_SCRYPT_R = 8
_SCRYPT_P = 1
_PBKDF2_ROUNDS = 480_000

MIN_PASSWORD_LENGTH = 8


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def hash_password(password: str) -> str:
    """`scrypt$n$r$p$salt$hash`, or `pbkdf2$rounds$salt$hash` where the
    interpreter was built without scrypt (it needs OpenSSL 1.1)."""
    if len(password) < MIN_PASSWORD_LENGTH:
        raise AuthError(
            f"A password needs at least {MIN_PASSWORD_LENGTH} characters. This runner "
            "can read and write anything the machine can, so the password on it is "
            "not a formality."
        )
    salt = secrets.token_bytes(16)
    try:
        digest = hashlib.scrypt(
            password.encode("utf-8"), salt=salt, n=_SCRYPT_N, r=_SCRYPT_R,
            p=_SCRYPT_P, dklen=32,
        )
    except (ValueError, AttributeError):  # pragma: no cover - depends on the build
        digest = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), salt, _PBKDF2_ROUNDS, dklen=32
        )
        return f"pbkdf2${_PBKDF2_ROUNDS}${_b64(salt)}${_b64(digest)}"
    return f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}${_b64(salt)}${_b64(digest)}"


def verify_password(password: str, stored: str) -> bool:
    """Constant-time check against a stored hash. False for anything unreadable —
    a corrupted row must not become a way in."""
    try:
        parts = stored.split("$")
        if parts[0] == "scrypt":
            _, n, r, p, salt, digest = parts
            candidate = hashlib.scrypt(
                password.encode("utf-8"), salt=base64.b64decode(salt),
                n=int(n), r=int(r), p=int(p), dklen=len(base64.b64decode(digest)),
            )
        elif parts[0] == "pbkdf2":
            _, rounds, salt, digest = parts
            candidate = hashlib.pbkdf2_hmac(
                "sha256", password.encode("utf-8"), base64.b64decode(salt),
                int(rounds), dklen=len(base64.b64decode(digest)),
            )
        else:
            return False
    except (ValueError, TypeError, IndexError, AttributeError):
        return False
    return secrets.compare_digest(candidate, base64.b64decode(parts[-1]))


def _hash_token(token: str) -> str:
    """Sessions are stored hashed, like passwords: the database is a file on disk,
    and a copy of it must not be a set of live logins."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


# -------------------------------------------------------------------- storage

_SCHEMA = """
CREATE TABLE IF NOT EXISTS user (
  id TEXT PRIMARY KEY,
  username TEXT NOT NULL UNIQUE COLLATE NOCASE,
  display_name TEXT,
  password_hash TEXT NOT NULL,
  disabled_at TEXT,
  created_at TEXT NOT NULL,
  last_login_at TEXT
);

CREATE TABLE IF NOT EXISTS role (
  name TEXT PRIMARY KEY,
  description TEXT,
  statements TEXT NOT NULL,
  custom INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_role (
  user_id TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
  role TEXT NOT NULL,
  PRIMARY KEY (user_id, role)
);

CREATE TABLE IF NOT EXISTS session (
  token_hash TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  last_seen_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_session_user ON session(user_id);

CREATE TABLE IF NOT EXISTS recovery (
  code_hash TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES user(id) ON DELETE CASCADE,
  issued_by TEXT,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  used_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_recovery_user ON recovery(user_id);
"""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse(moment: Optional[str]) -> Optional[datetime]:
    if not moment:
        return None
    try:
        return datetime.fromisoformat(moment.replace("Z", "+00:00"))
    except ValueError:  # pragma: no cover - written by _iso
        return None


def default_db_path() -> Path:
    configured = os.getenv("SPARQUET_STUDIO_AUTH_DB", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parent / "data" / "auth.sqlite3"


def session_hours() -> int:
    """How long a login lasts. Short by default: the runner is a shell in a
    browser tab, and an abandoned tab should stop being one."""
    try:
        return max(1, int(os.getenv("SPARQUET_STUDIO_SESSION_HOURS", "12")))
    except ValueError:
        return 12


def recovery_minutes() -> int:
    """How long a recovery code stays good. Short: it is a password in transit,
    usually pasted into a chat window, and it only has to survive that trip."""
    try:
        return max(1, int(os.getenv("SPARQUET_STUDIO_RECOVERY_MINUTES", "30")))
    except ValueError:
        return 30


class AuthStore:
    """Users, roles and sessions in SQLite. One short-lived connection per call
    under a process lock, matching `history.SQLiteExecutionRepository` — the write
    volume is a handful of rows per login."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._path = Path(db_path) if db_path else default_db_path()
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, closing(self._connect()) as conn:
            conn.executescript(_SCHEMA)
            self._sync_builtin_roles(conn)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, timeout=5)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _sync_builtin_roles(conn: sqlite3.Connection) -> None:
        for role in BUILTIN_ROLES.values():
            conn.execute(
                "INSERT INTO role (name, description, statements, custom) "
                "VALUES (?, ?, ?, 0) ON CONFLICT(name) DO UPDATE SET "
                "description=excluded.description, statements=excluded.statements "
                "WHERE role.custom = 0",
                (role.name, role.description, json.dumps(role.statements)),
            )

    # ---- users -----------------------------------------------------------

    def has_users(self) -> bool:
        """Whether this runner has an identity store at all — the switch between
        token-only mode and requiring a login."""
        with self._lock, closing(self._connect()) as conn:
            return conn.execute("SELECT 1 FROM user LIMIT 1").fetchone() is not None

    def create_user(
        self, username: str, password: str, *, roles: Optional[Iterable[str]] = None,
        display_name: Optional[str] = None,
    ) -> User:
        name = (username or "").strip()
        if not name:
            raise AuthError("A user needs a username.")
        wanted = self._checked_roles(roles)
        password_hash = hash_password(password)
        user_id = uuid.uuid4().hex
        with self._lock, closing(self._connect()) as conn:
            existing = conn.execute(
                "SELECT 1 FROM user WHERE username = ? COLLATE NOCASE", (name,)
            ).fetchone()
            if existing:
                raise AuthError(f"There is already a user called '{name}'.")
            conn.execute(
                "INSERT INTO user (id, username, display_name, password_hash, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (user_id, name, display_name, password_hash, _iso(_now())),
            )
            self._write_roles(conn, user_id, wanted)
            conn.commit()
        return User(
            id=user_id, username=name, display_name=display_name, roles=sorted(wanted),
            created_at=_iso(_now()),
        )

    def list_users(self) -> List[User]:
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute("SELECT * FROM user ORDER BY username").fetchall()
            return [self._user_of(conn, row) for row in rows]

    def get_user(self, user_id: str) -> Optional[User]:
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute("SELECT * FROM user WHERE id = ?", (user_id,)).fetchone()
            return self._user_of(conn, row) if row else None

    def set_roles(self, user_id: str, roles: Iterable[str]) -> User:
        wanted = self._checked_roles(roles)
        with self._lock, closing(self._connect()) as conn:
            row = self._require_user(conn, user_id)
            if "admin" not in wanted:
                self._guard_last_admin(conn, user_id)
            self._write_roles(conn, user_id, wanted)
            conn.commit()
            return self._user_of(conn, row)

    def set_disabled(self, user_id: str, disabled: bool) -> User:
        with self._lock, closing(self._connect()) as conn:
            self._require_user(conn, user_id)
            if disabled:
                self._guard_last_admin(conn, user_id)
            conn.execute(
                "UPDATE user SET disabled_at = ? WHERE id = ?",
                (_iso(_now()) if disabled else None, user_id),
            )
            if disabled:
                # A disabled user with a live session is still in.
                conn.execute("DELETE FROM session WHERE user_id = ?", (user_id,))
            conn.commit()
            row = conn.execute("SELECT * FROM user WHERE id = ?", (user_id,)).fetchone()
            return self._user_of(conn, row)

    def set_password(self, user_id: str, password: str) -> None:
        password_hash = hash_password(password)
        with self._lock, closing(self._connect()) as conn:
            self._require_user(conn, user_id)
            conn.execute(
                "UPDATE user SET password_hash = ? WHERE id = ?", (password_hash, user_id)
            )
            # Every session of theirs, gone: a password is changed either because
            # it leaked or because someone else is being locked out, and both
            # mean the sessions opened with the old one have to end.
            conn.execute("DELETE FROM session WHERE user_id = ?", (user_id,))
            conn.commit()

    def delete_user(self, user_id: str) -> None:
        with self._lock, closing(self._connect()) as conn:
            self._require_user(conn, user_id)
            self._guard_last_admin(conn, user_id)
            conn.execute("DELETE FROM session WHERE user_id = ?", (user_id,))
            conn.execute("DELETE FROM user_role WHERE user_id = ?", (user_id,))
            conn.execute("DELETE FROM user WHERE id = ?", (user_id,))
            conn.commit()

    # ---- sessions --------------------------------------------------------

    def login(self, username: str, password: str) -> Optional[Session]:
        """A session for these credentials, or None.

        None for every kind of failure — no such user, wrong password, disabled
        account — and the password is verified even when the user does not exist,
        against a throwaway hash, so the timing does not answer "is this a real
        username?" for someone guessing.
        """
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM user WHERE username = ? COLLATE NOCASE",
                ((username or "").strip(),),
            ).fetchone()
            stored = row["password_hash"] if row else _DUMMY_HASH
            ok = verify_password(password or "", stored)
            if not row or not ok or row["disabled_at"]:
                return None
            token = secrets.token_urlsafe(32)
            now = _now()
            expires = now + timedelta(hours=session_hours())
            conn.execute(
                "INSERT INTO session (token_hash, user_id, created_at, expires_at, last_seen_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (_hash_token(token), row["id"], _iso(now), _iso(expires), _iso(now)),
            )
            conn.execute(
                "UPDATE user SET last_login_at = ? WHERE id = ?", (_iso(now), row["id"])
            )
            conn.execute("DELETE FROM session WHERE expires_at < ?", (_iso(now),))
            conn.commit()
            return Session(token=token, user=self._user_of(conn, row), expires_at=_iso(expires))

    def verify_credentials(self, username: str, password: str) -> bool:
        """Whether these credentials are good, without opening a session.

        `login` has a side effect — it mints one — so checking a password (before
        letting someone change it, say) has to go through here instead.
        """
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT password_hash, disabled_at FROM user WHERE username = ? COLLATE NOCASE",
                ((username or "").strip(),),
            ).fetchone()
        stored = row["password_hash"] if row else _DUMMY_HASH
        ok = verify_password(password or "", stored)
        return bool(row) and ok and not row["disabled_at"]

    def resolve_session(self, token: str) -> Optional[Principal]:
        """The caller behind a session token, or None when it is unknown, expired
        or belongs to an account that has since been disabled."""
        if not token:
            return None
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT s.expires_at, u.* FROM session s JOIN user u ON u.id = s.user_id "
                "WHERE s.token_hash = ?",
                (_hash_token(token),),
            ).fetchone()
            if not row or row["disabled_at"]:
                return None
            expires = _parse(row["expires_at"])
            if expires is None or expires <= _now():
                conn.execute("DELETE FROM session WHERE token_hash = ?", (_hash_token(token),))
                conn.commit()
                return None
            conn.execute(
                "UPDATE session SET last_seen_at = ? WHERE token_hash = ?",
                (_iso(_now()), _hash_token(token)),
            )
            conn.commit()
            user = self._user_of(conn, row)
            return Principal(
                username=user.username, display_name=user.display_name, user_id=user.id,
                roles=user.roles, statements=self._statements_for(conn, user.roles),
            )

    def logout(self, token: str) -> None:
        with self._lock, closing(self._connect()) as conn:
            conn.execute("DELETE FROM session WHERE token_hash = ?", (_hash_token(token),))
            conn.commit()

    # ---- recovery --------------------------------------------------------

    def issue_recovery(
        self, user_id: str, *, issued_by: Optional[str] = None
    ) -> tuple[str, str]:
        """A single-use code this person can trade for a new password.

        Returns `(code, expires_at)`; only the hash is kept, so the code shown to
        the operator is the only copy that will ever exist. Any earlier unused
        code for the same person stops working: two live codes would mean two
        chances for a leaked one to be the one that still works.
        """
        code = secrets.token_urlsafe(24)
        now = _now()
        expires = now + timedelta(minutes=recovery_minutes())
        with self._lock, closing(self._connect()) as conn:
            self._require_user(conn, user_id)
            conn.execute("DELETE FROM recovery WHERE user_id = ? AND used_at IS NULL", (user_id,))
            conn.execute(
                "INSERT INTO recovery (code_hash, user_id, issued_by, created_at, expires_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (_hash_token(code), user_id, issued_by, _iso(now), _iso(expires)),
            )
            conn.execute("DELETE FROM recovery WHERE expires_at < ?", (_iso(now),))
            conn.commit()
        return code, _iso(expires)

    def redeem_recovery(self, code: str, password: str) -> User:
        """Set a new password with a recovery code, or refuse.

        Every refusal reads the same — unknown, expired, already used, or the
        account since disabled — because the caller here is not logged in, and a
        specific answer would turn this into an oracle. The new password is
        validated before the code is burned, so a password that is too short
        costs the person their typing, not their one code.
        """
        digest = _hash_token(code or "")
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT r.code_hash, r.expires_at, r.used_at, u.* FROM recovery r "
                "JOIN user u ON u.id = r.user_id WHERE r.code_hash = ?",
                (digest,),
            ).fetchone()
            expires = _parse(row["expires_at"]) if row else None
            usable = bool(
                row
                and not row["used_at"]
                and not row["disabled_at"]
                and expires is not None
                and expires > _now()
            )
            if not usable:
                raise AuthError(
                    "That recovery code is not usable — it is unknown, it expired, or "
                    "it has already been used. Ask for a new one."
                )
            password_hash = hash_password(password)
            conn.execute(
                "UPDATE user SET password_hash = ? WHERE id = ?", (password_hash, row["id"])
            )
            conn.execute(
                "UPDATE recovery SET used_at = ? WHERE code_hash = ?", (_iso(_now()), digest)
            )
            # Same reasoning as `set_password`: whoever held a session opened with
            # the old password should not keep it through a recovery.
            conn.execute("DELETE FROM session WHERE user_id = ?", (row["id"],))
            conn.commit()
            return self._user_of(conn, row)

    def find_user(self, username: str) -> Optional[User]:
        """By name, for the operator commands — a person at a terminal knows the
        username, not the id."""
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM user WHERE username = ? COLLATE NOCASE",
                ((username or "").strip(),),
            ).fetchone()
            return self._user_of(conn, row) if row else None

    # ---- roles -----------------------------------------------------------

    def list_roles(self) -> List[Role]:
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute("SELECT * FROM role ORDER BY name").fetchall()
        return [
            Role(
                name=row["name"], description=row["description"] or "",
                statements=json.loads(row["statements"]), custom=bool(row["custom"]),
            )
            for row in rows
        ]

    # ---- internals -------------------------------------------------------

    def _checked_roles(self, roles: Optional[Iterable[str]]) -> List[str]:
        wanted = sorted({str(role).strip() for role in (roles or []) if str(role).strip()})
        if not wanted:
            return [DEFAULT_ROLE]
        with self._lock, closing(self._connect()) as conn:
            known = {row["name"] for row in conn.execute("SELECT name FROM role")}
        unknown = [role for role in wanted if role not in known]
        if unknown:
            raise AuthError(
                f"No such role: {', '.join(unknown)}. Known roles: {', '.join(sorted(known))}."
            )
        return wanted

    @staticmethod
    def _write_roles(conn: sqlite3.Connection, user_id: str, roles: Sequence[str]) -> None:
        conn.execute("DELETE FROM user_role WHERE user_id = ?", (user_id,))
        conn.executemany(
            "INSERT INTO user_role (user_id, role) VALUES (?, ?)",
            [(user_id, role) for role in roles],
        )

    @staticmethod
    def _require_user(conn: sqlite3.Connection, user_id: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM user WHERE id = ?", (user_id,)).fetchone()
        if not row:
            raise AuthError("No such user.")
        return row

    @staticmethod
    def _guard_last_admin(conn: sqlite3.Connection, user_id: str) -> None:
        """Refuses to remove the last way back in.

        Without this, one careless role change leaves a runner nobody can
        administer, and the only fix is editing SQLite by hand.
        """
        remaining = conn.execute(
            "SELECT COUNT(*) AS n FROM user_role ur JOIN user u ON u.id = ur.user_id "
            "WHERE ur.role = 'admin' AND u.disabled_at IS NULL AND u.id != ?",
            (user_id,),
        ).fetchone()["n"]
        if remaining == 0:
            raise AuthError(
                "This is the only administrator left. Give someone else the admin role "
                "first, or nobody will be able to manage access on this runner."
            )

    @staticmethod
    def _roles_of(conn: sqlite3.Connection, user_id: str) -> List[str]:
        return [
            row["role"]
            for row in conn.execute(
                "SELECT role FROM user_role WHERE user_id = ? ORDER BY role", (user_id,)
            )
        ]

    def _user_of(self, conn: sqlite3.Connection, row: sqlite3.Row) -> User:
        return User(
            id=row["id"], username=row["username"], display_name=row["display_name"],
            roles=self._roles_of(conn, row["id"]), disabled=bool(row["disabled_at"]),
            created_at=row["created_at"], last_login_at=row["last_login_at"],
        )

    @staticmethod
    def _statements_for(conn: sqlite3.Connection, roles: Sequence[str]) -> List[Dict[str, Any]]:
        if not roles:
            return []
        placeholders = ",".join("?" for _ in roles)
        rows = conn.execute(
            f"SELECT statements FROM role WHERE name IN ({placeholders})", tuple(roles)
        ).fetchall()
        statements: List[Dict[str, Any]] = []
        for row in rows:
            try:
                parsed = json.loads(row["statements"])
            except json.JSONDecodeError:  # pragma: no cover - written by json.dumps
                continue
            if isinstance(parsed, list):
                statements.extend(item for item in parsed if isinstance(item, dict))
        return statements


#: Verified against when the username does not exist, so a wrong username and a
#: wrong password take the same time to fail.
_DUMMY_HASH = hash_password("x" * MIN_PASSWORD_LENGTH)


# ------------------------------------------------------------ operator commands


def _create_admin(store: AuthStore) -> int:
    import getpass

    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")
    if password != getpass.getpass("Repeat password: "):
        print("The passwords do not match.")
        return 1
    try:
        user = store.create_user(username, password, roles=["admin"])
    except AuthError as error:
        print(str(error))
        return 1
    print(f"Created '{user.username}' with the admin role.")
    print("The runner now requires a login on top of the shared token.")
    return 0


def _list_users(store: AuthStore) -> int:
    users = store.list_users()
    if not users:
        print("No users. This runner is in token-only mode.")
        return 0
    for user in users:
        state = " (disabled)" if user.disabled else ""
        print(f"{user.username}{state}: {', '.join(user.roles) or 'no roles'}")
    return 0


def _resolve(store: AuthStore, username: Optional[str]) -> Optional[User]:
    if not username:
        print("Which user? Pass a username.")
        return None
    user = store.find_user(username)
    if user is None:
        print(f"No user called '{username}'.")
    return user


def _reset_password(store: AuthStore, username: Optional[str]) -> int:
    """The way back in when the only administrator forgot their password.

    Whoever can run this already has the runner's files and its shared token, so
    it grants no authority they did not have; what it saves is a hand-edited
    SQLite file.
    """
    import getpass

    user = _resolve(store, username)
    if user is None:
        return 1
    password = getpass.getpass(f"New password for {user.username}: ")
    if password != getpass.getpass("Repeat password: "):
        print("The passwords do not match.")
        return 1
    try:
        store.set_password(user.id, password)
    except AuthError as error:
        print(str(error))
        return 1
    print(f"Password changed for '{user.username}'. Their open sessions were ended.")
    return 0


def _recovery_code(store: AuthStore, username: Optional[str]) -> int:
    user = _resolve(store, username)
    if user is None:
        return 1
    code, expires = store.issue_recovery(user.id, issued_by="cli")
    print(f"Recovery code for '{user.username}': {code}")
    print(f"Good until {expires}, once. Any earlier code for them no longer works.")
    print("They redeem it in Studio, on the login screen, under 'Forgot password'.")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Users and roles for the Studio runner.")
    parser.add_argument(
        "command",
        choices=["create-admin", "list-users", "reset-password", "recovery-code"],
    )
    parser.add_argument("username", nargs="?", default=None)
    parser.add_argument("--db", default=None, help="Path to the auth database.")
    args = parser.parse_args(argv)

    store = AuthStore(Path(args.db) if args.db else None)
    if args.command == "create-admin":
        return _create_admin(store)
    if args.command == "reset-password":
        return _reset_password(store, args.username)
    if args.command == "recovery-code":
        return _recovery_code(store, args.username)
    return _list_users(store)


if __name__ == "__main__":  # pragma: no cover - operator entry point
    raise SystemExit(main())
