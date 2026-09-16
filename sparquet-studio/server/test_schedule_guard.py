"""Tests for the permission a schedule needs before it is saved.

Stdlib plus FastAPI — no Spark, no HTTP client:

    python sparquet-studio/server/test_schedule_guard.py

A schedule is stored inside the library record, so writing one travels the
`workspace:Write` route. That is the whole problem this guards: without a check
of its own, permission to *edit* a Job would be permission to have it *run*, and
the `run_as` field would let the editor pick whose access it runs with. The
checks live in `main._authorize_schedule_change`; this file pins the cases a
regression would make silent.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from typing import Any, Dict, Optional

_TMP = tempfile.TemporaryDirectory()
# Point every store at a throwaway directory *before* importing the module: they
# are created at import time, and the runner database of whoever is developing
# is not a test fixture.
os.environ["SPARQUET_STUDIO_AUDIT_DB"] = os.path.join(_TMP.name, "audit.sqlite3")
os.environ["SPARQUET_STUDIO_AUTH_DB"] = os.path.join(_TMP.name, "auth.sqlite3")
os.environ["SPARQUET_STUDIO_CREDITS_DB"] = os.path.join(_TMP.name, "credits.sqlite3")
os.environ["SPARQUET_STUDIO_HISTORY_DB"] = os.path.join(_TMP.name, "history.sqlite3")
os.environ["SPARQUET_STUDIO_WORKSPACE"] = os.path.join(_TMP.name, "workspace")
os.environ.setdefault("SPARQUET_STUDIO_TOKEN", "test-token")

import auth  # noqa: E402
import main  # noqa: E402
from fastapi import HTTPException  # noqa: E402

RUN = "run:Execute"
MANAGE = "iam:ManageUsers"


def allow(*resources: str, action: str = RUN) -> Dict[str, Any]:
    return {"effect": "allow", "actions": [action], "resources": list(resources)}


def deny(*resources: str, action: str = RUN) -> Dict[str, Any]:
    return {"effect": "deny", "actions": [action], "resources": list(resources)}


def principal(username: str, *statements: Dict[str, Any]) -> auth.Principal:
    return auth.Principal(
        username=username, user_id="u-" + username, roles=["operator"],
        statements=list(statements),
    )


def record(cron: str = "0 6 * * *", **block: Any) -> Dict[str, Any]:
    """A Job record carrying a schedule, shaped the way the Studio writes it."""
    return {"id": "j1", "name": "Nightly", "schedule": {"cron": cron, **block}}


class _Directory:
    """Stands in for the auth store: a fixed set of accounts and their access."""

    def __init__(self, **people: auth.Principal) -> None:
        self._people = people

    def has_users(self) -> bool:
        return True

    def principal_for(self, username: str) -> Optional[auth.Principal]:
        return self._people.get(username)


class _Empty:
    """A runner nobody has logged into: the single-operator case."""

    def has_users(self) -> bool:
        return False

    def principal_for(self, username: str) -> None:
        return None


class ScheduleGuardTest(unittest.TestCase):
    def setUp(self) -> None:
        original = main._auth
        self.addCleanup(lambda: setattr(main, "_auth", original))

    def _directory(self, **people: auth.Principal) -> None:
        main._auth = _Directory(**people)

    def guard(self, actor: auth.Principal, before: Any, after: Any) -> None:
        main._authorize_schedule_change(actor, "job", "j1", before, after)

    # ------------------------------------------------------- the writer

    def test_writing_a_schedule_needs_permission_to_run_the_job(self) -> None:
        ana = principal("ana")
        self._directory(ana=ana)
        with self.assertRaises(HTTPException) as caught:
            self.guard(ana, {"id": "j1"}, record())
        self.assertEqual(caught.exception.status_code, 403)
        self.assertIn(RUN, caught.exception.detail)

    def test_someone_who_may_run_the_job_may_schedule_it(self) -> None:
        ana = principal("ana", allow("job/j1"))
        self._directory(ana=ana)
        self.guard(ana, {"id": "j1"}, record())

    def test_a_deny_on_the_job_is_not_widened_by_a_broader_allow(self) -> None:
        ana = principal("ana", allow("*"), deny("job/j1"))
        self._directory(ana=ana)
        with self.assertRaises(HTTPException):
            self.guard(ana, {"id": "j1"}, record())

    def test_removing_a_schedule_is_checked_like_adding_one(self) -> None:
        ana = principal("ana")
        self._directory(ana=ana)
        with self.assertRaises(HTTPException):
            self.guard(ana, record(), {"id": "j1"})

    def test_disabling_a_schedule_is_a_change(self) -> None:
        ana = principal("ana")
        self._directory(ana=ana)
        with self.assertRaises(HTTPException):
            self.guard(ana, record(), record(enabled=False))

    # --------------------------------------------------- an untouched block

    def test_a_save_that_leaves_the_schedule_alone_needs_nothing(self) -> None:
        ana = principal("ana")
        self._directory(ana=ana)
        before = record(runAs="ana")
        after = dict(record(runAs="ana"), description="edited")
        self.guard(ana, before, after)

    def test_a_record_that_never_had_a_schedule_needs_nothing(self) -> None:
        ana = principal("ana")
        self._directory(ana=ana)
        self.guard(ana, {"id": "j1"}, {"id": "j1", "name": "Nightly"})

    # ------------------------------------------------------------ run_as

    def test_scheduling_as_somebody_else_needs_iam_manageusers(self) -> None:
        ana = principal("ana", allow("job/j1"))
        beto = principal("beto", allow("job/j1"))
        self._directory(ana=ana, beto=beto)
        with self.assertRaises(HTTPException) as caught:
            self.guard(ana, {"id": "j1"}, record(runAs="beto"))
        self.assertEqual(caught.exception.status_code, 403)
        self.assertIn(MANAGE, caught.exception.detail)

    def test_an_admin_may_schedule_as_an_account_that_can_run_it(self) -> None:
        ana = principal("ana", allow("job/j1"), allow("*", action=MANAGE))
        beto = principal("beto", allow("job/j1"))
        self._directory(ana=ana, beto=beto)
        self.guard(ana, {"id": "j1"}, record(runAs="beto"))

    def test_an_account_that_cannot_run_it_is_refused_as_run_as(self) -> None:
        ana = principal("ana", allow("job/j1"), allow("*", action=MANAGE))
        beto = principal("beto")
        self._directory(ana=ana, beto=beto)
        with self.assertRaises(HTTPException) as caught:
            self.guard(ana, {"id": "j1"}, record(runAs="beto"))
        self.assertEqual(caught.exception.status_code, 403)
        self.assertIn("beto", caught.exception.detail)

    def test_an_unknown_account_is_refused(self) -> None:
        ana = principal("ana", allow("job/j1"), allow("*", action=MANAGE))
        self._directory(ana=ana)
        with self.assertRaises(HTTPException) as caught:
            self.guard(ana, {"id": "j1"}, record(runAs="ninguem"))
        self.assertEqual(caught.exception.status_code, 400)

    def test_naming_yourself_is_not_acting_for_somebody_else(self) -> None:
        ana = principal("ana", allow("job/j1"))
        self._directory(ana=ana)
        self.guard(ana, {"id": "j1"}, record(runAs="ana"))

    def test_the_snake_case_spelling_of_run_as_is_read_too(self) -> None:
        ana = principal("ana", allow("job/j1"))
        beto = principal("beto", allow("job/j1"))
        self._directory(ana=ana, beto=beto)
        with self.assertRaises(HTTPException):
            self.guard(ana, {"id": "j1"}, record(run_as="beto"))

    # ------------------------------------------------- single-operator mode

    def test_a_runner_with_no_users_schedules_freely(self) -> None:
        main._auth = _Empty()
        self.guard(auth.TOKEN_PRINCIPAL, {"id": "j1"}, record(runAs="qualquer_um"))

    # --------------------------------------------------------- other kinds

    def test_a_workflow_record_carries_no_schedule_to_guard(self) -> None:
        ana = principal("ana")
        self._directory(ana=ana)
        main._authorize_schedule_change(ana, "workflow", "w1", None, record())


class MayRunTest(unittest.TestCase):
    """`_may_run` answers the same question `_authorize_run` raises on."""

    def test_it_agrees_with_authorize_run_when_allowed(self) -> None:
        beto = principal("beto", allow("job/j1"))
        self.assertTrue(main._may_run(beto, RUN, job_id="j1"))

    def test_a_deny_wins_over_an_allow_here_too(self) -> None:
        beto = principal("beto", allow("*"), deny("job/j1"))
        self.assertFalse(main._may_run(beto, RUN, job_id="j1"))


if __name__ == "__main__":
    try:
        unittest.main()
    finally:
        _TMP.cleanup()
