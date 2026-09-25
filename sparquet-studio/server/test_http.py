"""Tests for the layer between a request and a handler: guards and the wire.

Stdlib plus FastAPI's test client — no Spark, no network, no server process:

    python sparquet-studio/server/test_http.py

Everything else here tests a function. That leaves a gap exactly the width of a
decorator: `test_run_scope.py` proves `_authorize_run` denies what it should, and
`test_login_guard.py` proves `require_token` refuses the wrong caller — and both
kept passing while eleven routes, `/run` among them, were declared without either
guard attached. A route with a perfect guard function and no `Depends` on it is
an open door, and only the route table says so.

So this asks the app object the two questions the source cannot answer on its
own: which routes are reachable with no credential, and which action protects
each of the rest. Then it drives real requests through the stack — 401, 403, 200
— and reads the `/run/flow/stream` event sequence off the wire, with the Spark
call stubbed out, because the frame order and the `stage_id` on every log line
are a contract the Studio's runner client depends on line by line.
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from typing import Any, Dict, List, Optional, Tuple

# Cleanup errors ignored: the stores keep SQLite handles open until the process
# exits, and on Windows the directory cannot be removed while they are. A passing
# run that ends in a teardown traceback teaches people to ignore tracebacks.
_TMP = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
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
from fastapi.routing import APIRoute  # noqa: E402

try:  # The test client needs httpx, which the runner itself does not.
    from fastapi.testclient import TestClient
except Exception as exc:  # pragma: no cover - environment without httpx
    TestClient = None  # type: ignore[assignment]
    _CLIENT_REASON = f"fastapi.testclient unavailable: {exc}"
else:
    _CLIENT_REASON = ""

TOKEN = "test-token"
ORIGIN = "http://localhost:5273"
HEADERS = {main.TOKEN_HEADER: TOKEN, "origin": ORIGIN}

#: Reachable with no credential at all, on purpose: Studio calls these before it
#: has a token to send, to find out whether a runner is there and what it can do.
#: Anything else showing up here is a hole, which is the point of the first test.
OPEN_ROUTES = {("GET", "/health"), ("GET", "/capabilities"), ("GET", "/assistant")}


def _guards(dependant: Any) -> List[Any]:
    """Every dependency callable a route ends up running, nested ones included.

    `requires(...)` calls `require_token` itself rather than declaring it, so the
    tree has to be walked: a route guarded only by an action still has the token
    check, one level down inside a closure.
    """
    found: List[Any] = []
    for sub in dependant.dependencies:
        if sub.call is not None:
            found.append(sub.call)
        found.extend(_guards(sub))
    return found


def _routes() -> List[Tuple[str, str, List[Any]]]:
    """(method, path, guards) for every route the app declares."""
    out: List[Tuple[str, str, List[Any]]] = []
    for route in main.app.routes:
        if not isinstance(route, APIRoute):
            continue  # /docs and /openapi.json: Starlette's own, no dependencies
        for method in sorted(route.methods):
            out.append((method, route.path, _guards(route.dependant)))
    return out


def _actions(guards: List[Any]) -> List[str]:
    """The actions `requires(...)` wrote on its closures, for this route."""
    return sorted({guard.action for guard in guards if hasattr(guard, "action")})


def _protected(guards: List[Any]) -> bool:
    return any(
        guard in (main.require_token, main.require_token_unless_users)
        or hasattr(guard, "action")
        for guard in guards
    )


class RouteTable(unittest.TestCase):
    """What the app object says about itself, which is what actually runs."""

    def test_only_the_three_probes_are_reachable_without_a_credential(self) -> None:
        open_now = {
            (method, path) for method, path, guards in _routes() if not _protected(guards)
        }
        # Named one by one rather than counted: a new endpoint that forgets its
        # guard has to be added to OPEN_ROUTES by hand, which is a decision
        # somebody makes rather than a number that drifts.
        self.assertEqual(open_now, OPEN_ROUTES)

    def test_the_run_endpoints_declare_the_token_guard(self) -> None:
        # They cannot use `requires(...)`: the thing being run is named in the
        # body, which a dependency cannot read, so they authorize inside the
        # handler. That is exactly why the token guard has to be declared here —
        # there is no action dependency to carry it in.
        for path in ("/run", "/run/stream", "/run/flow/stream"):
            guards = [call for _, route, calls in _routes() if route == path for call in calls]
            self.assertIn(main.require_token, guards, path)

    def test_the_action_protecting_a_route_can_be_read_off_the_table(self) -> None:
        expected = {
            ("POST", "/validate"): ["run:Validate"],
            ("DELETE", "/workspace/{kind}/{record_id}"): ["workspace:Delete"],
            ("PUT", "/workspace/{kind}/{record_id}"): ["workspace:Write"],
            ("DELETE", "/secrets/{name}"): ["secrets:Write"],
            ("POST", "/auth/users"): ["iam:ManageUsers"],
            ("GET", "/audit"): ["iam:ReadAudit"],
            ("POST", "/assistant/stream"): ["assistant:Ask"],
            ("POST", "/runs/{run_id}/cancel"): ["run:Cancel"],
        }
        table = {(method, path): _actions(guards) for method, path, guards in _routes()}
        for key, actions in expected.items():
            self.assertEqual(table.get(key), actions, key)

    def test_every_action_names_a_permission_the_policy_knows(self) -> None:
        # An action string is free-form at the call site; a typo would produce a
        # guard nobody can grant, and the endpoint would deny every caller who is
        # not the token holder.
        known = set(main.auth.ACTIONS)
        for method, path, guards in _routes():
            for action in _actions(guards):
                self.assertIn(action, known, f"{method} {path}")


@unittest.skipIf(TestClient is None, _CLIENT_REASON)
class TokenGuard(unittest.TestCase):
    """The same guards, driven through the stack instead of read off it."""

    def setUp(self) -> None:
        self.client = TestClient(main.app)

    def test_the_probes_answer_with_no_headers_at_all(self) -> None:
        for path in ("/health", "/capabilities", "/assistant"):
            self.assertEqual(self.client.get(path).status_code, 200, path)

    def test_run_refuses_a_request_carrying_no_token(self) -> None:
        # The hole this file was written for: these three answered 200 and ran
        # the pipeline, because a `Depends(current_principal)` looks like a guard
        # and checks nothing.
        for path in ("/run", "/run/stream", "/run/flow/stream"):
            response = self.client.post(path, json={"stages": [], "pipeline": {}})
            self.assertEqual(response.status_code, 401, path)
            # The refusal has to name the header, or the only way to fix it is to
            # read the source.
            self.assertIn(main.TOKEN_HEADER, response.json()["detail"], path)

    def test_identity_and_credits_refuse_a_request_carrying_no_token(self) -> None:
        for path in ("/auth/me", "/credits/me", "/credits/usage", "/credits/timeline"):
            self.assertEqual(self.client.get(path).status_code, 401, path)
        # `/iam/access` answers "what may I do", which is worth as much to an
        # attacker mapping the runner as the endpoints it describes.
        self.assertEqual(
            self.client.post("/iam/access", json={"actions": []}).status_code, 401
        )

    def test_a_foreign_origin_is_refused_even_with_the_right_token(self) -> None:
        # The token alone is not the whole guard: a page the developer happens to
        # visit could hold a leaked token, and the Origin is what says the request
        # came from Studio.
        response = self.client.get(
            "/runs", headers={main.TOKEN_HEADER: TOKEN, "origin": "http://evil.example"}
        )
        self.assertEqual(response.status_code, 403)
        self.assertIn("evil.example", response.json()["detail"])

    def test_a_wrong_token_is_refused_from_an_allowed_origin(self) -> None:
        response = self.client.get(
            "/runs", headers={main.TOKEN_HEADER: "not-the-token", "origin": ORIGIN}
        )
        self.assertEqual(response.status_code, 401)

    def test_the_token_from_the_studio_origin_gets_through(self) -> None:
        response = self.client.get("/runs", headers=HEADERS)
        self.assertEqual(response.status_code, 200)


def _sse_events(body: str) -> List[Tuple[str, Dict[str, Any]]]:
    """The stream as (event, payload) pairs, in arrival order."""
    events: List[Tuple[str, Dict[str, Any]]] = []
    name: Optional[str] = None
    for line in body.splitlines():
        if line.startswith("event: "):
            name = line[len("event: "):]
        elif line.startswith("data: ") and name is not None:
            events.append((name, json.loads(line[len("data: "):])))
            name = None
    return events


def _stage(number: int, *, fails: bool = False) -> Dict[str, Any]:
    label = "fail" if fails else "ok"
    return {
        "id": f"s{number}",
        "name": f"{label} {number}",
        # Local paths on purpose: a local run is free, so nothing here depends on
        # an account having credits.
        "pipeline": {
            "name": f"stage {number}",
            "input": {"format": "csv", "path": "/data/in"},
            "output": {"format": "parquet", "path": "/data/out", "mode": "overwrite"},
        },
    }


@unittest.skipIf(TestClient is None, _CLIENT_REASON)
class FlowStream(unittest.TestCase):
    """The event sequence `/run/flow/stream` promises, read off the wire.

    Spark is replaced at `_execute_run`, the seam the two streaming endpoints
    share: everything above it — the queue, the per-stage markers, history, the
    ordering guarantee — is the real code, and a stage becomes a function that
    logs a line and returns a result.
    """

    def setUp(self) -> None:
        self.client = TestClient(main.app)
        self.addCleanup(setattr, main, "_execute_run", main._execute_run)
        main._execute_run = self._fake_execute

    @staticmethod
    def _fake_execute(
        body: Any, name: Optional[str], started: float, collector: Any,
        rendered: Optional[Dict[str, Any]] = None, secret_values: Any = (),
    ) -> Any:
        # Through the collector, not `print`: that is the path a pipeline's own
        # logs take, so the line is carried by the machinery under test.
        collector.append("2026-01-01T00:00:00Z", "INFO", f"{name}: reading", {})
        failing = (name or "").startswith("fail")
        return main.RunResponse(
            success=not failing,
            pipeline_name=name,
            rows_read=3,
            rows_written=0 if failing else 3,
            duration_ms=1,
            error="the source path does not exist" if failing else None,
        )

    def _run(self, **body: Any) -> List[Tuple[str, Dict[str, Any]]]:
        response = self.client.post("/run/flow/stream", headers=HEADERS, json=body)
        self.assertEqual(response.status_code, 200, response.text)
        return _sse_events(response.text)

    def test_the_frames_arrive_in_the_documented_order(self) -> None:
        events = self._run(stages=[_stage(1), _stage(2)])

        self.assertEqual(
            [name for name, _ in events],
            ["start", "stage_start", "log", "stage_result",
             "stage_start", "log", "stage_result", "result"],
        )
        start = events[0][1]
        self.assertEqual(start["total"], 2)
        self.assertTrue(start["pipeline_run_id"])

    def test_a_log_line_is_filed_under_the_stage_that_printed_it(self) -> None:
        # The queue is FIFO for exactly this reason: two stages sharing one
        # logger must not have their lines attributed to each other.
        events = self._run(stages=[_stage(1), _stage(2)])
        logs = [payload for name, payload in events if name == "log"]

        self.assertEqual([entry["stage_id"] for entry in logs], ["s1", "s2"])
        self.assertEqual(logs[0]["message"], "ok 1: reading")
        self.assertEqual(logs[1]["message"], "ok 2: reading")

    def test_each_stage_reports_its_own_outcome(self) -> None:
        events = self._run(stages=[_stage(1), _stage(2)])
        results = [payload for name, payload in events if name == "stage_result"]

        self.assertEqual([entry["id"] for entry in results], ["s1", "s2"])
        self.assertEqual([entry["success"] for entry in results], [True, True])
        self.assertEqual([entry["rows_written"] for entry in results], [3, 3])
        final = events[-1][1]
        self.assertTrue(final["success"])
        self.assertIsNone(final["error"])
        self.assertEqual(len(final["stages"]), 2)

    def test_a_failed_stage_stops_the_flow_and_the_rest_are_announced_skipped(self) -> None:
        events = self._run(stages=[_stage(1, fails=True), _stage(2), _stage(3)])

        self.assertEqual(
            [name for name, _ in events],
            ["start", "stage_start", "log", "stage_result",
             "stage_skipped", "stage_skipped", "result"],
        )
        skipped = [payload["id"] for name, payload in events if name == "stage_skipped"]
        self.assertEqual(skipped, ["s2", "s3"])
        final = events[-1][1]
        self.assertFalse(final["success"])
        # The failure names the stage: "a run failed" is not something anyone can
        # act on when a Pipeline has nine of them.
        self.assertIn("fail 1", final["error"])
        self.assertIn("the source path does not exist", final["error"])

    def test_stop_on_error_false_runs_the_stages_after_the_failure(self) -> None:
        events = self._run(stages=[_stage(1, fails=True), _stage(2)], stop_on_error=False)

        results = [payload for name, payload in events if name == "stage_result"]
        self.assertEqual([entry["success"] for entry in results], [False, True])
        # One stage failed, so the flow failed, however well the rest went.
        self.assertFalse(events[-1][1]["success"])

    def test_a_flow_with_no_stages_is_refused_before_anything_starts(self) -> None:
        response = self.client.post("/run/flow/stream", headers=HEADERS, json={"stages": []})
        self.assertEqual(response.status_code, 422)

    def test_a_single_run_streams_its_logs_and_one_result(self) -> None:
        response = self.client.post(
            "/run/stream", headers=HEADERS,
            json={"pipeline": _stage(1)["pipeline"], "params": {}},
        )
        self.assertEqual(response.status_code, 200, response.text)
        events = _sse_events(response.text)

        self.assertEqual(events[0][0], "start")
        self.assertEqual(events[-1][0], "result")
        self.assertEqual([name for name, _ in events].count("result"), 1)
        self.assertTrue(events[-1][1]["success"])
        self.assertIn("reading", " ".join(
            payload["message"] for name, payload in events if name == "log"
        ))


if __name__ == "__main__":
    unittest.main(verbosity=2)
