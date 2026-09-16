"""Tests for the query history the runner keeps, instead of the browser.

Stdlib plus FastAPI — no Spark, no HTTP client:

    python sparquet-studio/server/test_query_history.py

The move from `localStorage` to the runner changes two things that are worth
pinning. The first is who writes it: the runner records what actually happened
to a statement — including the failures — rather than trusting a client to
report its own runs. The second is who reads it: the history of a saved query is
shared, because the query is a file two people can open, while the history of a
buffer nobody has saved is the caller's alone, and that is enforced by the key
being principal-scoped rather than by a filter on the way out.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from typing import Any, Dict, List

_TMP = tempfile.TemporaryDirectory()
# Point every store at a throwaway directory *before* importing the module: they
# are created at import time, and the developer's own runner is not a fixture.
os.environ["SPARQUET_STUDIO_AUDIT_DB"] = os.path.join(_TMP.name, "audit.sqlite3")
os.environ["SPARQUET_STUDIO_AUTH_DB"] = os.path.join(_TMP.name, "auth.sqlite3")
os.environ["SPARQUET_STUDIO_CREDITS_DB"] = os.path.join(_TMP.name, "credits.sqlite3")
os.environ["SPARQUET_STUDIO_HISTORY_DB"] = os.path.join(_TMP.name, "history.sqlite3")
os.environ["SPARQUET_STUDIO_WORKSPACE"] = os.path.join(_TMP.name, "workspace")
os.environ.setdefault("SPARQUET_STUDIO_TOKEN", "test-token")

import auth  # noqa: E402
import history  # noqa: E402
import main  # noqa: E402
from fastapi import HTTPException  # noqa: E402


def principal(**fields: Any) -> auth.Principal:
    base: Dict[str, Any] = {
        "username": "ana", "user_id": "u1", "roles": ["editor"],
        "team_id": "t-analytics",
        "statements": [{"effect": "allow", "actions": ["*"], "resources": ["*"]}],
    }
    base.update(fields)
    return auth.Principal(**base)


def request(**fields: Any) -> main.QueryRequest:
    base: Dict[str, Any] = {"sql": "SELECT 1", "limit": 50}
    base.update(fields)
    return main.QueryRequest(**base)


def answer(rows: int = 1, truncated: bool = False) -> main.QueryResponse:
    return main.QueryResponse(
        query_id="q1",
        columns=["n"],
        fields=[main.SchemaFieldOut(name="n", type="int", nullable=False)],
        rows=[[index] for index in range(rows)],
        truncated=truncated,
        elapsed_ms=42,
    )


class KeyTest(unittest.TestCase):
    def test_a_saved_query_is_keyed_by_its_file(self) -> None:
        self.assertEqual(main._history_key(principal(), "qz9", "tab-1"), "q:qz9")

    def test_a_scratch_buffer_is_keyed_by_the_tab_and_the_person(self) -> None:
        key = main._history_key(principal(), None, "tab-1")
        self.assertEqual(key, "t:ana:tab-1")

    def test_two_people_on_the_same_tab_id_do_not_share_a_history(self) -> None:
        mine = main._history_key(principal(username="ana"), None, "tab-1")
        yours = main._history_key(principal(username="bruno"), None, "tab-1")
        self.assertNotEqual(mine, yours)

    def test_a_tab_id_cannot_carry_anything_but_a_name(self) -> None:
        self.assertEqual(main._history_key(principal(), None, "a b/../c"), "t:ana:ab..c")

    def test_a_run_from_nowhere_is_filed_nowhere(self) -> None:
        # The catalog's row sample runs through the same endpoint and names no
        # tab: recording it would put storage reads in somebody's query history.
        self.assertEqual(main._history_key(principal(), None, None), "")


class RecordingTest(unittest.TestCase):
    """What `/query` writes down, through the wrapper around the execution."""

    def setUp(self) -> None:
        self.ran: List[main.QueryRequest] = []
        self._run_query = main._run_query
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        main._run_query = self._run_query

    def succeeds(self, result: main.QueryResponse) -> None:
        def fake(body: main.QueryRequest, principal: Any) -> main.QueryResponse:
            self.ran.append(body)
            return result

        main._run_query = fake

    def fails(self, exc: Exception) -> None:
        def fake(body: main.QueryRequest, principal: Any) -> main.QueryResponse:
            self.ran.append(body)
            raise exc

        main._run_query = fake

    def runs(self, key: str) -> List[history.QueryRun]:
        return main._history.list_query_runs(key)

    def test_a_successful_run_is_recorded_against_the_query(self) -> None:
        self.succeeds(answer(rows=3, truncated=True))
        main.query(request(sql="SELECT 1", saved_query_id="qa"), principal())
        runs = self.runs("q:qa")
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].sql, "SELECT 1")
        self.assertEqual((runs[0].rows, runs[0].truncated), (3, True))
        self.assertEqual(runs[0].elapsed_ms, 42)
        self.assertEqual(runs[0].run_as, "ana")
        self.assertIsNone(runs[0].error)

    def test_a_failed_run_is_recorded_too(self) -> None:
        self.fails(HTTPException(status_code=400, detail="Table not found.\nplan..."))
        with self.assertRaises(HTTPException):
            main.query(request(saved_query_id="qb"), principal())
        runs = self.runs("q:qb")
        self.assertEqual(len(runs), 1)
        # The first line only: the history is a list, not a log viewer.
        self.assertEqual(runs[0].error, "Table not found.")
        self.assertEqual(runs[0].rows, 0)

    def test_a_run_that_names_no_query_and_no_tab_is_not_recorded(self) -> None:
        before = len(self.runs("q:qc"))
        self.succeeds(answer())
        main.query(request(), principal())
        self.assertEqual(len(self.runs("q:qc")), before)

    def test_a_history_that_cannot_be_written_does_not_fail_the_query(self) -> None:
        self.succeeds(answer())
        broken = history.SQLiteExecutionRepository.record_query_run

        def explode(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("disk is full")

        history.SQLiteExecutionRepository.record_query_run = explode  # type: ignore[assignment]
        self.addCleanup(
            setattr, history.SQLiteExecutionRepository, "record_query_run", broken
        )
        result = main.query(request(saved_query_id="qd"), principal())
        self.assertEqual(result.query_id, "q1")

    def test_the_statement_is_what_was_sent_including_a_selection(self) -> None:
        self.succeeds(answer())
        main.query(request(sql="  SELECT count(*) FROM t  ", tab="tab-9"), principal())
        self.assertEqual(self.runs("t:ana:tab-9")[0].sql, "SELECT count(*) FROM t")


class EndpointTest(unittest.TestCase):
    def setUp(self) -> None:
        for key in ("q:qe", "q:qf", "t:ana:tab-7", "t:bruno:tab-7"):
            main._history.clear_query_runs(key)

    def record(self, key: str, sql: str, **fields: Any) -> None:
        base: Dict[str, Any] = {
            "limit": 50, "elapsed_ms": 1, "rows": 1, "truncated": False, "run_as": "ana",
        }
        base.update(fields)
        main._history.record_query_run(key, sql=sql, **base)

    def test_the_runs_of_a_saved_query_come_back_newest_first(self) -> None:
        self.record("q:qe", "SELECT 1")
        self.record("q:qe", "SELECT 2")
        answered = main.query_history(saved_query_id="qe", principal=principal())
        self.assertEqual([run.sql for run in answered.runs], ["SELECT 2", "SELECT 1"])

    def test_a_shared_history_says_who_ran_each_statement(self) -> None:
        self.record("q:qe", "SELECT 1", run_as="bruno")
        answered = main.query_history(saved_query_id="qe", principal=principal())
        self.assertEqual(answered.runs[0].run_as, "bruno")

    def test_a_scratch_history_is_the_callers_own(self) -> None:
        self.record("t:ana:tab-7", "SELECT mine")
        self.record("t:bruno:tab-7", "SELECT theirs")
        answered = main.query_history(tab="tab-7", principal=principal(username="ana"))
        self.assertEqual([run.sql for run in answered.runs], ["SELECT mine"])

    def test_naming_neither_a_query_nor_a_tab_is_refused(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            main.query_history(principal=principal())
        self.assertEqual(raised.exception.status_code, 400)

    def test_clearing_removes_the_runs_of_that_query(self) -> None:
        self.record("q:qe", "SELECT 1")
        removed = main.clear_query_history(saved_query_id="qe", principal=principal())
        self.assertEqual(removed["removed"], 1)
        self.assertEqual(main._history.list_query_runs("q:qe"), [])

    def test_saving_a_buffer_carries_its_runs_onto_the_file(self) -> None:
        self.record("t:ana:tab-7", "SELECT 1")
        self.record("t:ana:tab-7", "SELECT 2")
        moved = main.move_query_history(
            main.MoveHistoryRequest(tab="tab-7", saved_query_id="qf"), principal()
        )
        self.assertEqual(moved["moved"], 2)
        self.assertEqual(len(main._history.list_query_runs("q:qf")), 2)
        self.assertEqual(main._history.list_query_runs("t:ana:tab-7"), [])

    def test_a_denied_query_hides_its_history(self) -> None:
        main._workspace.write_meta(
            "grants",
            [
                {
                    "id": "g1",
                    "resource": "query",
                    "resourceId": "qe",
                    "principalKind": "user",
                    "principalId": "bruno",
                    "level": "read",
                    "effect": "allow",
                }
            ],
        )
        self.addCleanup(main._workspace.delete_meta, "grants")
        with self.assertRaises(HTTPException) as raised:
            main.query_history(saved_query_id="qe", principal=principal(username="ana"))
        self.assertEqual(raised.exception.status_code, 403)


class RetentionTest(unittest.TestCase):
    def test_a_query_keeps_only_its_most_recent_runs(self) -> None:
        key = "q:qg"
        main._history.clear_query_runs(key)
        for index in range(history.QUERY_RUNS_KEPT + 5):
            main._history.record_query_run(
                key, sql=f"SELECT {index}", limit=50, elapsed_ms=1, rows=1,
                truncated=False, run_as="ana",
            )
        runs = main._history.list_query_runs(key)
        self.assertEqual(len(runs), history.QUERY_RUNS_KEPT)
        # The newest survive: the oldest run is the one nobody is looking for.
        self.assertEqual(runs[0].sql, f"SELECT {history.QUERY_RUNS_KEPT + 4}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
