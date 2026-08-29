"""Tests for the audit log (`audit.py`).

Stdlib only — no pytest, no FastAPI — so it runs like the rest of the repo's
tests:

    python sparquet-studio/server/test_audit.py

An audit log is only worth having if three things hold. It has to be **complete**
where it matters: every change, and — more importantly — every refusal, because
a refused request is the one somebody will want to explain later. It has to be
**safe to write**: a log that can take a request down with it when the disk fills
is a liability, so a failed write is a gap and never an error. And it has to be
**searchable the way an incident is investigated**: by who, by what was touched,
and by whether it was allowed.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import audit


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.store = audit.AuditStore(Path(self._dir.name) / "audit.sqlite3")
        self.addCleanup(self._dir.cleanup)

    def _record(self, **overrides):
        event = {
            "actor": "ana", "actor_id": "u1", "action": "iam:Write",
            "method": "POST", "path": "/auth/users", "outcome": audit.ALLOWED,
        }
        event.update(overrides)
        return self.store.record(**event)


class RecordTests(StoreTestCase):
    def test_an_event_keeps_what_it_was_given(self):
        self._record(
            team="data", team_id="t1", roles=["admin"], resource="user/u9",
            status=200, detail={"created": "bruno"}, ip="127.0.0.1",
        )
        event = self.store.list()[0]
        self.assertEqual(event.actor, "ana")
        self.assertEqual(event.actor_id, "u1")
        self.assertEqual(event.team, "data")
        self.assertEqual(event.roles, ["admin"])
        self.assertEqual(event.resource, "user/u9")
        self.assertEqual(event.status, 200)
        self.assertEqual(event.detail, {"created": "bruno"})
        self.assertEqual(event.ip, "127.0.0.1")

    def test_every_event_gets_an_id_and_a_timestamp(self):
        first = self._record()
        second = self._record()
        self.assertTrue(first and second)
        self.assertNotEqual(first, second)
        self.assertTrue(self.store.list()[0].at.endswith("Z"))

    def test_a_refusal_is_recorded_like_anything_else(self):
        """The whole point: the requests that did not happen are the ones an
        investigation is about."""
        self._record(outcome=audit.DENIED, status=403, action="run:Execute")
        event = self.store.list()[0]
        self.assertEqual(event.outcome, audit.DENIED)
        self.assertEqual(event.status, 403)

    def test_a_broken_database_is_a_gap_and_never_an_error(self):
        """A log that can take the request down with it is worse than no log."""
        # A directory is never a database: sqlite refuses to open it, which is
        # the same failure a full or read-only disk produces.
        self.store.path = Path(self._dir.name)
        self.assertEqual(
            self.store.record(
                actor="ana", action="iam:Write", method="POST",
                path="/auth/users", outcome=audit.ALLOWED,
            ),
            "",
        )

    def test_the_log_is_append_only(self):
        self._record()
        self._record()
        self.assertEqual(self.store.count(), 2)


class ListTests(StoreTestCase):
    def test_newest_first(self):
        self._record(path="/auth/users")
        self._record(path="/auth/teams")
        self.assertEqual(
            [event.path for event in self.store.list()],
            ["/auth/teams", "/auth/users"],
        )

    def test_filtering_by_actor(self):
        self._record(actor="ana", actor_id="u1")
        self._record(actor="bruno", actor_id="u2")
        events = self.store.list(actor_id="u2")
        self.assertEqual([event.actor for event in events], ["bruno"])

    def test_filtering_by_what_was_touched(self):
        self._record(resource="user/u9")
        self._record(resource="team/t1")
        self.assertEqual(len(self.store.list(resource="user/u9")), 1)

    def test_filtering_by_outcome_finds_the_refusals(self):
        self._record(outcome=audit.ALLOWED)
        self._record(outcome=audit.DENIED)
        self._record(outcome=audit.DENIED)
        self.assertEqual(len(self.store.list(outcome=audit.DENIED)), 2)

    def test_a_service_wildcard_matches_every_verb(self):
        """`iam:*` is how the interface asks for "everything that touched
        access" without knowing every verb."""
        self._record(action="iam:Write")
        self._record(action="iam:Delete")
        self._record(action="run:Execute")
        self.assertEqual(len(self.store.list(action="iam:*")), 2)
        self.assertEqual(len(self.store.list(action="iam:Write")), 1)

    def test_filtering_by_when(self):
        self._record()
        self.assertEqual(len(self.store.list(since="1970-01-01T00:00:00Z")), 1)
        self.assertEqual(len(self.store.list(since="2999-01-01T00:00:00Z")), 0)

    def test_the_limit_is_bounded_on_both_ends(self):
        for _ in range(5):
            self._record()
        self.assertEqual(len(self.store.list(limit=2)), 2)
        self.assertEqual(len(self.store.list(limit=0)), 1)
        self.assertEqual(len(self.store.list(limit=10 ** 6)), 5)


class QuietPathTests(unittest.TestCase):
    """What must not be recorded, or the log buries itself."""

    def test_polling_routes_are_quiet(self):
        for path in ("/health", "/capabilities", "/docs", "/openapi.json"):
            with self.subTest(path=path):
                self.assertTrue(audit.is_quiet(path))

    def test_everything_that_changes_something_is_loud(self):
        for path in ("/auth/users", "/run", "/workspace/jobs", "/credits/grant"):
            with self.subTest(path=path):
                self.assertFalse(audit.is_quiet(path))


class ActionForTests(unittest.TestCase):
    """A request refused before it reached a route still has to be filed
    somewhere, and only the path is known by then."""

    def test_the_service_comes_from_the_first_segment(self):
        self.assertEqual(audit.action_for("POST", "/auth/users"), "iam:Write")
        self.assertEqual(audit.action_for("DELETE", "/auth/roles/x"), "iam:Delete")
        self.assertEqual(audit.action_for("GET", "/credits/me"), "credits:Read")
        self.assertEqual(audit.action_for("PUT", "/workspace/jobs/x"), "workspace:Write")

    def test_history_answers_to_two_paths(self):
        self.assertEqual(audit.action_for("GET", "/runs"), "history:Read")
        self.assertEqual(audit.action_for("GET", "/job-runs/x"), "history:Read")

    def test_every_way_of_running_is_one_action(self):
        for path in ("/run", "/run/stream", "/run/flow/stream"):
            with self.subTest(path=path):
                self.assertEqual(audit.action_for("POST", path), "run:Execute")

    def test_an_unknown_path_still_gets_an_action(self):
        self.assertEqual(audit.action_for("POST", "/"), "http:Write")
        self.assertEqual(audit.action_for("GET", "/whatever"), "whatever:Read")


if __name__ == "__main__":
    unittest.main()
