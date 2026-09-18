"""Tests for who may reach the runner, and how fast they may guess.

Stdlib plus FastAPI — no Spark, no HTTP client:

    python sparquet-studio/server/test_login_guard.py

Two things are pinned here, and both exist because of the same trap. The shared
token is typed into Settings, Settings sits behind the login, and the login used
to demand the token: rotating the token locked everybody out of the only screen
that could take the new one, with no way back through the interface. So the three
endpoints a locked-out person needs stop asking for it once the runner has users,
and a live session now counts everywhere the token counts.

Giving that up costs something — `/auth/login` becomes reachable with no secret
at all — and `_LoginThrottle` is what pays for it. The rest of the guard does
not move: the Origin check runs on every one of these paths, with users or
without, token or session.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from typing import Any, Dict, List, Optional

_TMP = tempfile.TemporaryDirectory()
# Point every store at a throwaway directory *before* importing the module: they
# are created at import time, and the developer's own runner database is not a
# test fixture.
os.environ["SPARQUET_STUDIO_AUDIT_DB"] = os.path.join(_TMP.name, "audit.sqlite3")
os.environ["SPARQUET_STUDIO_AUTH_DB"] = os.path.join(_TMP.name, "auth.sqlite3")
os.environ["SPARQUET_STUDIO_CREDITS_DB"] = os.path.join(_TMP.name, "credits.sqlite3")
os.environ["SPARQUET_STUDIO_HISTORY_DB"] = os.path.join(_TMP.name, "history.sqlite3")
os.environ["SPARQUET_STUDIO_WORKSPACE"] = os.path.join(_TMP.name, "workspace")
os.environ["SPARQUET_STUDIO_TOKEN"] = "test-token"

import main  # noqa: E402
from fastapi import HTTPException  # noqa: E402
from starlette.requests import Request  # noqa: E402

GOOD_ORIGIN = "http://localhost:5273"


def request(**headers: str) -> Request:
    """A request carrying only the headers a test cares about."""
    raw = [(name.replace("_", "-").lower().encode(), value.encode())
           for name, value in headers.items()]
    return Request({
        "type": "http", "method": "GET", "path": "/auth/login", "query_string": b"",
        "scheme": "http", "server": ("127.0.0.1", 8787), "client": ("127.0.0.1", 51234),
        "headers": raw, "state": {},
    })


class FakeAuth:
    """Enough identity store for the guard: does anybody exist, and is this
    session real. Used instead of the SQLite one so a test that needs "no users"
    cannot be broken by a test that created one."""

    def __init__(self, users: bool = False, sessions: Optional[Dict[str, Any]] = None) -> None:
        self._users = users
        self._sessions = sessions or {}

    def has_users(self) -> bool:
        return self._users

    def resolve_session(self, token: str) -> Any:
        return self._sessions.get(token)


class GuardTestCase(unittest.TestCase):
    """Swaps the identity store for the duration of one test."""

    def use(self, store: FakeAuth) -> None:
        original = main._auth
        main._auth = store  # type: ignore[assignment]
        self.addCleanup(lambda: setattr(main, "_auth", original))


class TokenGuardTest(GuardTestCase):
    """`require_token` — the guard on everything that is not the login."""

    def test_the_token_alone_is_still_enough(self) -> None:
        self.use(FakeAuth(users=True))
        main.require_token(request(**{"x-sparquet-token": "test-token"}))

    def test_a_live_session_stands_in_for_the_token(self) -> None:
        # The point of the change: somebody who logged in holds a credential in a
        # header of its own, which is exactly what the token was buying.
        principal = object()
        self.use(FakeAuth(users=True, sessions={"s1": principal}))
        call = request(**{"x-sparquet-session": "s1"})
        main.require_token(call)
        self.assertIs(call.state.principal, principal)

    def test_a_bearer_session_works_the_same_way(self) -> None:
        principal = object()
        self.use(FakeAuth(users=True, sessions={"s1": principal}))
        main.require_token(request(authorization="Bearer s1"))

    def test_neither_credential_is_refused(self) -> None:
        self.use(FakeAuth(users=True))
        with self.assertRaises(HTTPException) as raised:
            main.require_token(request())
        self.assertEqual(raised.exception.status_code, 401)
        self.assertIn(main.TOKEN_HEADER, raised.exception.detail)

    def test_a_session_the_runner_forgot_says_so(self) -> None:
        # Answering "no token" to somebody holding an expired session sends them
        # looking for the wrong thing.
        self.use(FakeAuth(users=True))
        with self.assertRaises(HTTPException) as raised:
            main.require_token(request(**{"x-sparquet-session": "gone"}))
        self.assertEqual(raised.exception.status_code, 401)
        self.assertEqual(raised.exception.detail, main.SESSION_EXPIRED_HELP)

    def test_a_foreign_origin_is_refused_before_any_credential(self) -> None:
        self.use(FakeAuth(users=True))
        with self.assertRaises(HTTPException) as raised:
            main.require_token(request(origin="http://evil.example",
                                       **{"x-sparquet-token": "test-token"}))
        self.assertEqual(raised.exception.status_code, 403)

    def test_the_origin_studio_is_served_from_passes(self) -> None:
        self.use(FakeAuth(users=True))
        main.require_token(request(origin=GOOD_ORIGIN, **{"x-sparquet-token": "test-token"}))


class LoginGuardTest(GuardTestCase):
    """`require_token_unless_users` — the guard on /auth/status, /auth/login and
    /auth/recover."""

    def test_with_users_the_login_asks_for_no_token(self) -> None:
        # The whole point: the password is the wall, and the token cannot be
        # required to reach the screen that hands the token over.
        self.use(FakeAuth(users=True))
        main.require_token_unless_users(request())

    def test_with_no_users_the_token_is_still_mandatory(self) -> None:
        # A runner in token-only mode has nothing else to ask for, and /auth/status
        # would otherwise tell any page on the machine that this runner exists.
        self.use(FakeAuth(users=False))
        with self.assertRaises(HTTPException) as raised:
            main.require_token_unless_users(request())
        self.assertEqual(raised.exception.status_code, 401)

    def test_with_no_users_the_token_still_opens_it(self) -> None:
        self.use(FakeAuth(users=False))
        main.require_token_unless_users(request(**{"x-sparquet-token": "test-token"}))

    def test_the_origin_check_is_not_waived_along_with_the_token(self) -> None:
        self.use(FakeAuth(users=True))
        with self.assertRaises(HTTPException) as raised:
            main.require_token_unless_users(request(origin="http://evil.example"))
        self.assertEqual(raised.exception.status_code, 403)


class PrincipalReuseTest(GuardTestCase):
    """What `require_token` resolved is not resolved twice."""

    def test_the_session_looked_up_at_the_door_is_the_one_used(self) -> None:
        principal = object()
        store = FakeAuth(users=True, sessions={"s1": principal})
        self.use(store)
        call = request(**{"x-sparquet-session": "s1"})
        main.require_token(call)
        # Emptying the store proves the second read never happens.
        store._sessions.clear()
        self.assertIs(main.current_principal(call), principal)


class ThrottleTest(unittest.TestCase):
    """`_LoginThrottle` — the ceiling that replaces the token on /auth/login."""

    def test_failures_under_the_limit_cost_nothing(self) -> None:
        throttle = main._LoginThrottle(limit=3, window=60)
        throttle.record_failure(["ip:a"])
        throttle.record_failure(["ip:a"])
        self.assertEqual(throttle.retry_after(["ip:a"]), 0)

    def test_the_limit_closes_the_door_and_says_for_how_long(self) -> None:
        throttle = main._LoginThrottle(limit=3, window=60)
        for _ in range(3):
            throttle.record_failure(["ip:a"])
        wait = throttle.retry_after(["ip:a"])
        self.assertGreater(wait, 0)
        self.assertLessEqual(wait, 61)

    def test_one_caller_being_blocked_does_not_block_another(self) -> None:
        throttle = main._LoginThrottle(limit=2, window=60)
        throttle.record_failure(["ip:a"])
        throttle.record_failure(["ip:a"])
        self.assertGreater(throttle.retry_after(["ip:a"]), 0)
        self.assertEqual(throttle.retry_after(["ip:b"]), 0)

    def test_a_success_forgets_the_failures_before_it(self) -> None:
        # Somebody who mistypes twice and then gets it right pays nothing.
        throttle = main._LoginThrottle(limit=3, window=60)
        throttle.record_failure(["ip:a", "user:ana"])
        throttle.record_failure(["ip:a", "user:ana"])
        throttle.forget(["ip:a", "user:ana"])
        self.assertEqual(throttle.retry_after(["ip:a", "user:ana"]), 0)

    def test_the_window_expires_on_its_own(self) -> None:
        throttle = main._LoginThrottle(limit=1, window=1)
        throttle.record_failure(["ip:a"])
        self.assertGreater(throttle.retry_after(["ip:a"]), 0)
        # Reaching into the recorded time beats sleeping through the window.
        throttle._hits["ip:a"] = [throttle._hits["ip:a"][0] - 5]
        self.assertEqual(throttle.retry_after(["ip:a"]), 0)

    def test_either_axis_alone_is_enough_to_refuse(self) -> None:
        # Counting only the caller lets a botnet spread the guessing out;
        # counting only the account lets one caller sweep every account.
        throttle = main._LoginThrottle(limit=2, window=60)
        throttle.record_failure(["ip:a", "user:ana"])
        throttle.record_failure(["ip:a", "user:ana"])
        self.assertGreater(throttle.retry_after(["ip:b", "user:ana"]), 0)
        self.assertGreater(throttle.retry_after(["ip:a", "user:bob"]), 0)


class ThrottleKeyTest(unittest.TestCase):
    """What a request is counted under."""

    def test_the_caller_and_the_account_are_both_named(self) -> None:
        self.assertEqual(
            main._throttle_keys(request(), "Ana"), ["ip:127.0.0.1", "user:ana"]
        )

    def test_a_recovery_is_counted_against_the_caller_only(self) -> None:
        # The code names no account until it is redeemed, so there is no second
        # axis to count on.
        self.assertEqual(main._throttle_keys(request()), ["ip:127.0.0.1"])

    def test_a_blank_username_adds_no_axis(self) -> None:
        self.assertEqual(main._throttle_keys(request(), "   "), ["ip:127.0.0.1"])


class ThrottleResponseTest(unittest.TestCase):
    """What the caller is told when the ceiling is reached."""

    def test_a_refusal_is_429_and_carries_retry_after(self) -> None:
        original = main._LOGIN_THROTTLE
        main._LOGIN_THROTTLE = main._LoginThrottle(limit=1, window=60)
        self.addCleanup(lambda: setattr(main, "_LOGIN_THROTTLE", original))
        keys: List[str] = ["ip:a"]
        main._LOGIN_THROTTLE.record_failure(keys)
        with self.assertRaises(HTTPException) as raised:
            main._refuse_if_throttled(keys)
        self.assertEqual(raised.exception.status_code, 429)
        self.assertIn("Retry-After", raised.exception.headers)

    def test_nothing_is_raised_while_there_is_room(self) -> None:
        main._refuse_if_throttled(["ip:nobody-has-used-this"])


class LoginPrincipalTest(unittest.TestCase):
    """What `/auth/login` reports about the person who just logged in.

    Studio decides which controls to offer from the principal the login returns,
    and it only asks `/auth/status` again on a reload. A login that answered with
    the role names but not the statements behind them therefore left an
    administrator with an empty policy for the whole session: every button
    greyed out with "your role does not allow ...", and no way to tell from the
    UI that the runner disagreed.
    """

    def test_the_session_carries_the_statements_behind_the_roles(self) -> None:
        import auth as auth_module

        # The store is the throwaway one this module points every database at,
        # so the user is left behind on purpose: deleting it would trip the
        # "last administrator" guard, which is a different rule being tested
        # somewhere else.
        store = main._auth
        username = "login-principal-probe"
        if store.find_user(username) is None:
            store.create_user(username, "probe-password-1", roles=["admin"])

        out = main.auth_login(request(origin=GOOD_ORIGIN), main.LoginRequest(
            username=username, password="probe-password-1",
        ))

        self.assertEqual(out.user.roles, ["admin"])
        self.assertEqual(
            out.user.statements,
            auth_module.BUILTIN_ROLES["admin"].statements,
            "the login must report the policy, not only the role names",
        )
        self.assertTrue(out.user.team_id, "and the team the roles are widened by")


if __name__ == "__main__":
    unittest.main(verbosity=2)
