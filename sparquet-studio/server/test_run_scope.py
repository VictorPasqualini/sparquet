"""Tests for authorizing an execution against the thing being executed.

Stdlib plus FastAPI — no Spark, no HTTP client:

    python sparquet-studio/server/test_run_scope.py

`/run`, `/run/stream` and `/run/flow/stream` cannot be authorized by a route
dependency: the dependency runs before the body is read, and the body is the only
place the target is named. So they authorize inside the handler, through
`_authorize_run`, and this file pins the part a mistake would make invisible — a
deny that can be widened away by a broader grant somewhere else.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from typing import Any, Dict, List

_TMP = tempfile.TemporaryDirectory()
# Point every store at a throwaway directory *before* importing the module: they
# are created at import time, and the developer's own runner database is not a
# test fixture.
os.environ["SPARQUET_STUDIO_AUTH_DB"] = os.path.join(_TMP.name, "auth.sqlite3")
os.environ["SPARQUET_STUDIO_CREDITS_DB"] = os.path.join(_TMP.name, "credits.sqlite3")
os.environ["SPARQUET_STUDIO_HISTORY_DB"] = os.path.join(_TMP.name, "history.sqlite3")
os.environ["SPARQUET_STUDIO_WORKSPACE"] = os.path.join(_TMP.name, "workspace")
os.environ.setdefault("SPARQUET_STUDIO_TOKEN", "test-token")

import auth  # noqa: E402
import main  # noqa: E402
from fastapi import HTTPException  # noqa: E402
from starlette.requests import Request  # noqa: E402

RUN = "run:Execute"


def principal(*statements: Dict[str, Any], roles: List[str] = None) -> auth.Principal:
    return auth.Principal(
        username="ana", user_id="u1", roles=roles or ["operator"],
        statements=list(statements),
    )


def allow(*resources: str, action: str = RUN) -> Dict[str, Any]:
    return {"effect": "allow", "actions": [action], "resources": list(resources)}


def deny(*resources: str, action: str = RUN) -> Dict[str, Any]:
    return {"effect": "deny", "actions": [action], "resources": list(resources)}


class RunTargetTest(unittest.TestCase):
    """Which resources a run is checked against."""

    def test_a_run_names_every_identifier_the_body_carried(self) -> None:
        self.assertEqual(
            main._run_targets("w1", "p1", "j1"),
            ["workflow/w1", "pipeline/p1", "job/j1"],
        )

    def test_missing_identifiers_are_left_out_rather_than_invented(self) -> None:
        self.assertEqual(main._run_targets(None, None, "j1"), ["job/j1"])
        self.assertEqual(main._run_targets("w1", None, None), ["workflow/w1"])

    def test_an_unsaved_job_falls_back_to_the_wildcard(self) -> None:
        # The editor can run a Job that was never saved; it belongs to nothing yet.
        self.assertEqual(main._run_targets(None, None, None), ["*"])


class AuthorizeRunTest(unittest.TestCase):
    """`_authorize_run` — one allow is enough, one deny is final."""

    def _authorize(self, who: auth.Principal, **ids: str) -> None:
        main._authorize_run(who, RUN, **ids)

    def test_a_grant_on_the_workflow_covers_a_job_inside_it(self) -> None:
        self._authorize(principal(allow("workflow/w1")), workflow_id="w1", job_id="j1")

    def test_a_grant_on_the_job_alone_is_enough(self) -> None:
        self._authorize(principal(allow("job/j1")), workflow_id="w1", job_id="j1")

    def test_a_grant_on_another_workflow_does_not_carry_over(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            self._authorize(principal(allow("workflow/w2")), workflow_id="w1", job_id="j1")
        self.assertEqual(raised.exception.status_code, 403)

    def test_holding_nothing_is_refused(self) -> None:
        with self.assertRaises(HTTPException):
            self._authorize(principal(), workflow_id="w1", job_id="j1")

    def test_a_deny_on_the_job_is_not_widened_away_by_the_workflow_grant(self) -> None:
        # The whole reason the check moved into the handler: "may run everything in
        # w1, except j1" has to mean it.
        who = principal(allow("workflow/w1"), deny("job/j1"))
        with self.assertRaises(HTTPException) as raised:
            self._authorize(who, workflow_id="w1", job_id="j1")
        self.assertIn("job/j1", raised.exception.detail)
        # …and the sibling Job in the same Workflow still runs.
        self._authorize(who, workflow_id="w1", job_id="j2")

    def test_a_deny_on_the_workflow_stops_a_job_granted_by_name(self) -> None:
        who = principal(allow("job/j1"), deny("workflow/w1"))
        with self.assertRaises(HTTPException):
            self._authorize(who, workflow_id="w1", job_id="j1")

    def test_a_wildcard_grant_still_runs_an_unsaved_job(self) -> None:
        self._authorize(principal(allow("*")))

    def test_a_role_scoped_to_one_workflow_cannot_run_an_unsaved_job(self) -> None:
        # Nothing identifies it, so nothing places it inside the grant.
        with self.assertRaises(HTTPException):
            self._authorize(principal(allow("workflow/w1")))

    def test_the_refusal_says_who_was_refused_and_what_they_hold(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            self._authorize(principal(roles=["viewer"]), job_id="j1")
        detail = raised.exception.detail
        self.assertIn("ana", detail)
        self.assertIn(RUN, detail)
        self.assertIn("viewer", detail)

    def test_cancelling_is_a_permission_of_its_own(self) -> None:
        who = principal(allow("*", action="run:Execute"))
        with self.assertRaises(HTTPException):
            main._authorize_run(who, "run:Cancel", job_id="j1")

    def test_the_token_only_runner_runs_everything(self) -> None:
        main._authorize_run(auth.TOKEN_PRINCIPAL, RUN, workflow_id="w1", job_id="j1")


class WorkspaceResourceTest(unittest.TestCase):
    """The other half of the same idea, on routes where the path *does* name it."""

    @staticmethod
    def _request(**path_params: str) -> Request:
        return Request({"type": "http", "path_params": path_params, "headers": []})

    def test_the_record_being_touched_is_the_resource(self) -> None:
        resource = main._workspace_resource(self._request(kind="job", record_id="j1"))
        self.assertEqual(resource, "job/j1")

    def test_a_call_that_names_nothing_asks_for_everything(self) -> None:
        self.assertEqual(main._workspace_resource(self._request()), "*/*")


if __name__ == "__main__":
    try:
        unittest.main()
    finally:
        _TMP.cleanup()
