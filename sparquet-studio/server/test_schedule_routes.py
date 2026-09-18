"""The runner side of a schedule: what the sweep reads, fires and refuses.

`test_scheduling.py` pins the arithmetic. This pins the wiring, which is where
the decisions with consequences are: a scheduled run goes through the same `/run`
the button goes through, it runs as the account the schedule names, an occurrence
that arrives while the runner is busy is dropped rather than queued, and a
schedule that has fired is not fired again on the next tick thirty seconds later.

Nothing here starts Spark: `main.run` and `main.run_flow_stream` are replaced, so
what is asserted is the request the scheduler builds, not what the framework does
with it.
"""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_TMP = tempfile.TemporaryDirectory()
os.environ["SPARQUET_STUDIO_AUDIT_DB"] = os.path.join(_TMP.name, "audit.sqlite3")
os.environ["SPARQUET_STUDIO_AUTH_DB"] = os.path.join(_TMP.name, "auth.sqlite3")
os.environ["SPARQUET_STUDIO_CREDITS_DB"] = os.path.join(_TMP.name, "credits.sqlite3")
os.environ["SPARQUET_STUDIO_HISTORY_DB"] = os.path.join(_TMP.name, "history.sqlite3")
os.environ["SPARQUET_STUDIO_MONITORS_DB"] = os.path.join(_TMP.name, "monitors.sqlite3")
os.environ["SPARQUET_STUDIO_WORKSPACE"] = os.path.join(_TMP.name, "workspace")
# Both sweeps drive themselves in these tests, so neither timer may race them.
os.environ["SPARQUET_STUDIO_MONITORS"] = "off"
os.environ["SPARQUET_STUDIO_SCHEDULER"] = "off"
os.environ["SPARQUET_STUDIO_HISTORY_PURGE"] = "off"

import main  # noqa: E402
import scheduling  # noqa: E402


class _Document:
    """A workspace record, as the store hands it over."""

    def __init__(self, kind, doc_id, record, path=None, config=None):
        self.kind = kind
        self.id = doc_id
        self.record = record
        self.path = path
        self.config = config


class _Snapshot:
    def __init__(self, jobs=(), pipelines=()):
        self.root = "/library"
        self.jobs = list(jobs)
        self.pipelines = list(pipelines)
        self.workflows = []
        self.queries = []
        self.meta = {}


class _Workspace:
    """Enough of the store for the scheduler: a snapshot and the files it names."""

    def __init__(self, snapshot, files=None):
        self._snapshot = snapshot
        self._files = files or {}

    def snapshot(self):
        return self._snapshot

    def read_file(self, relative):
        if relative not in self._files:
            raise FileNotFoundError(relative)
        return self._files[relative]


def _job(
    doc_id="j1", name="Daily sales", cron="* * * * *",
    path="sales/jobs/daily.json", **block,
):
    """A scheduled Job. The default expression is every minute so that a sweep in
    a test is due without the test having to know what time it is."""
    record = {"id": doc_id, "name": name, "workflowId": "w1"}
    if cron is not None:
        record["schedule"] = {"cron": cron, "timezone": "UTC", **block}
    return _Document(scheduling.JOB, doc_id, record, path=path)


class _Response:
    """What `run()` answers with, reduced to the two fields the sweep reads."""

    def __init__(self, pipeline_run_id="run-1", error=None):
        self.pipeline_run_id = pipeline_run_id
        self.error = error


class SchedulerTestCase(unittest.TestCase):
    def setUp(self):
        self._workspace = main._workspace
        self._run = main.run
        self._flow = main.run_flow_stream
        self._started = main._SCHEDULER_STARTED_AT
        main._SCHEDULE_ANCHORS.clear()
        self.calls = []

    def tearDown(self):
        main._workspace = self._workspace
        main.run = self._run
        main.run_flow_stream = self._flow
        main._SCHEDULER_STARTED_AT = self._started
        main._SCHEDULE_ANCHORS.clear()

    def library(self, jobs=(), pipelines=(), files=None):
        main._workspace = _Workspace(_Snapshot(jobs, pipelines), files)

    def due_now(self):
        """Make every schedule's last occurrence one the sweep has not seen."""
        main._SCHEDULER_STARTED_AT = datetime.now(timezone.utc) - timedelta(days=2)

    def record_run(self, response=None, raises=None):
        def _fake(body, principal):
            self.calls.append((body, principal))
            if raises is not None:
                raise raises
            return response or _Response()

        main.run = _fake


class ReadingTheLibraryTest(SchedulerTestCase):
    def test_only_records_that_carry_a_schedule(self):
        self.library(jobs=[_job(), _job("j2", "Ad hoc", cron=None)])
        schedules, _ = main._library_schedules()
        self.assertEqual([item.id for item in schedules], ["j1"])

    def test_jobs_and_pipelines_both(self):
        pipeline = _Document(
            scheduling.PIPELINE, "p1",
            {"id": "p1", "name": "Nightly", "schedule": {"cron": "0 2 * * *"}},
        )
        self.library(jobs=[_job()], pipelines=[pipeline])
        schedules, _ = main._library_schedules()
        self.assertEqual(
            [(item.kind, item.id) for item in schedules],
            [("job", "j1"), ("pipeline", "p1")],
        )


class PrincipalTest(SchedulerTestCase):
    def test_the_shared_token_is_the_identity_when_there_are_no_users(self):
        principal = main._schedule_principal(
            scheduling.from_record("job", {"id": "j1", "schedule": {"cron": "0 6 * * *"}})
        )
        self.assertTrue(getattr(principal, "token_only", False))

    def test_a_schedule_naming_nobody_does_not_run_once_users_exist(self):
        # Asserted through the sweep, so it also covers the refusal being an
        # outcome rather than an exception.
        self.library(jobs=[_job()])
        self.due_now()
        self.record_run()
        original = main._auth.has_users
        main._auth.has_users = lambda: True
        try:
            report = main._sweep_schedules()
        finally:
            main._auth.has_users = original
        self.assertEqual(report.fired, 0)
        self.assertEqual(self.calls, [])
        self.assertIn("no account", report.fires[0].error)


class SweepTest(SchedulerTestCase):
    def test_a_due_job_runs_through_the_same_path_the_button_uses(self):
        self.library(
            jobs=[_job()], files={"sales/jobs/daily.json": {"name": "daily", "source": {}}}
        )
        self.due_now()
        self.record_run()

        report = main._sweep_schedules()

        self.assertEqual((report.checked, report.fired), (1, 1))
        self.assertEqual(report.fires[0].run_id, "run-1")
        body, _ = self.calls[0]
        self.assertEqual(body.job_id, "j1")
        self.assertEqual(body.workflow_id, "w1")
        self.assertEqual(body.launched, main.history.SCHEDULED)
        # What runs is the compiled file in the library, read as it fires.
        self.assertEqual(body.pipeline, {"name": "daily", "source": {}})

    def test_nothing_fires_twice_for_the_same_occurrence(self):
        self.library(
            jobs=[_job()], files={"sales/jobs/daily.json": {"name": "daily"}}
        )
        self.due_now()
        self.record_run()

        main._sweep_schedules()
        second = main._sweep_schedules()

        self.assertEqual(second.fired, 0)
        self.assertEqual(len(self.calls), 1)

    def test_a_run_already_in_progress_skips_the_occurrence(self):
        # The runner shares one SparkSession, so overlapping runs are refused
        # rather than queued: a slow morning must not become a backlog.
        self.library(jobs=[_job()], files={"sales/jobs/daily.json": {}})
        self.due_now()
        self.record_run(raises=main.HTTPException(status_code=409, detail="busy here"))

        report = main._sweep_schedules()

        self.assertEqual(report.fired, 0)
        self.assertEqual(report.fires[0].error, "busy here")
        # And the occurrence is spent: the next tick does not try again.
        self.record_run()
        self.assertEqual(main._sweep_schedules().fired, 0)

    def test_a_job_whose_file_was_never_compiled(self):
        self.library(jobs=[_job(path=None)])
        self.due_now()
        self.record_run()
        report = main._sweep_schedules()
        self.assertEqual(report.fired, 0)
        self.assertIn("no compiled file", report.fires[0].error)

    def test_a_failed_run_still_counts_as_fired_and_reports_why(self):
        self.library(jobs=[_job()], files={"sales/jobs/daily.json": {}})
        self.due_now()
        self.record_run(_Response(pipeline_run_id="run-9", error="Path does not exist"))

        report = main._sweep_schedules()

        self.assertTrue(report.fires[0].started)
        self.assertEqual(report.fires[0].error, "Path does not exist")

    def test_a_paused_schedule_is_left_alone(self):
        self.library(jobs=[_job(enabled=False)], files={"sales/jobs/daily.json": {}})
        self.due_now()
        self.record_run()
        report = main._sweep_schedules()
        self.assertEqual((report.checked, report.fired), (1, 0))
        self.assertEqual(self.calls, [])

    def test_an_unreadable_expression_never_fires_and_says_so(self):
        self.library(jobs=[_job(cron="every morning")], files={"sales/jobs/daily.json": {}})
        self.due_now()
        self.record_run()
        self.assertEqual(main._sweep_schedules().fired, 0)
        self.assertEqual(self.calls, [])
        [listed] = main.list_schedules()
        self.assertIsNotNone(listed.error)
        self.assertIsNone(listed.next_fire)


class PipelineFlowTest(SchedulerTestCase):
    def setUp(self):
        super().setUp()
        self.flows = []

        def _fake_flow(body, principal):
            self.flows.append((body, principal))
            return object()  # no body_iterator: nothing to drain

        main.run_flow_stream = _fake_flow

    def test_stages_run_in_the_order_the_links_state(self):
        pipeline = _Document(
            scheduling.PIPELINE, "p1",
            {
                "id": "p1", "name": "Nightly", "workflowId": "w1",
                "schedule": {"cron": "* * * * *", "timezone": "UTC"},
                "stages": [
                    {"id": "s2", "jobId": "j2"},
                    {"id": "s1", "jobId": "j1"},
                ],
                "links": [{"source": "s1", "target": "s2"}],
            },
        )
        jobs = [
            _job("j1", "Ingest", cron=None, path="w/jobs/ingest.json"),
            _job("j2", "Publish", cron=None, path="w/jobs/publish.json"),
        ]
        self.library(jobs=jobs, pipelines=[pipeline])
        self.due_now()

        report = main._sweep_schedules()

        self.assertEqual(report.fired, 1)
        body, _ = self.flows[0]
        self.assertEqual([stage.id for stage in body.stages], ["s1", "s2"])
        # Every stage runs the file in the library, never a config compiled here.
        self.assertEqual(
            [stage.path for stage in body.stages],
            ["w/jobs/ingest.json", "w/jobs/publish.json"],
        )
        self.assertEqual(body.pipeline_id, "p1")
        self.assertEqual(body.launched, main.history.SCHEDULED)

    def test_a_stage_pointing_at_a_file_is_passed_through_untouched(self):
        pipeline = _Document(
            scheduling.PIPELINE, "p1",
            {
                "id": "p1", "name": "Nightly",
                "schedule": {"cron": "* * * * *", "timezone": "UTC"},
                "stages": [{"id": "s1", "jobId": "", "path": "shared/theirs.json"}],
                "links": [],
            },
        )
        self.library(pipelines=[pipeline])
        self.due_now()

        main._sweep_schedules()

        body, _ = self.flows[0]
        self.assertEqual(body.stages[0].path, "shared/theirs.json")
        self.assertIsNone(body.stages[0].job_id)

    def test_a_pipeline_with_no_stages_is_refused_rather_than_started(self):
        pipeline = _Document(
            scheduling.PIPELINE, "p1",
            {"id": "p1", "name": "Empty", "schedule": {"cron": "* * * * *"}, "stages": []},
        )
        self.library(pipelines=[pipeline])
        self.due_now()
        report = main._sweep_schedules()
        self.assertEqual(report.fired, 0)
        self.assertIn("no stages", report.fires[0].error)
        self.assertEqual(self.flows, [])


class DrainTest(unittest.TestCase):
    """Consuming a streaming response with nobody watching it.

    A Pipeline only runs through `/run/flow/stream`, and everything that has to
    happen at the end of a flow - releasing the run lock, settling the credit
    holds, closing the run in the history - happens in the generator's `finally`.
    A scheduled flow that stopped reading halfway would leave the runner locked.
    """

    def test_every_chunk_is_consumed_and_the_run_id_comes_back(self):
        from fastapi.responses import StreamingResponse

        closed = []

        def _events():
            try:
                yield 'event: start\ndata: {"pipeline_run_id": "run-7"}\n\n'
                yield 'event: log\ndata: {"message": "working"}\n\n'
                yield 'event: result\ndata: {"success": true}\n\n'
            finally:
                closed.append(True)

        run_id = main._drain_stream(StreamingResponse(_events()))

        self.assertEqual(run_id, "run-7")
        self.assertEqual(closed, [True])

    def test_a_response_that_streams_nothing_is_not_an_error(self):
        self.assertIsNone(main._drain_stream(object()))


class ListSchedulesTest(SchedulerTestCase):
    def test_what_the_screen_reads(self):
        self.library(jobs=[_job(cron="0 6 * * *", **{"runAs": "ana"})])
        [listed] = main.list_schedules()
        self.assertEqual(listed.kind, "job")
        self.assertEqual(listed.name, "Daily sales")
        self.assertEqual(listed.cron, "0 6 * * *")
        self.assertEqual(listed.run_as, "ana")
        self.assertTrue(listed.enabled)
        self.assertIsNotNone(listed.next_fire)
        self.assertIn("0 6 * * *", listed.rule)
        # Never run yet, and the history is empty: nothing invented.
        self.assertIsNone(listed.last_fire)
        self.assertIsNone(listed.last_run_id)

    def test_a_paused_schedule_still_shows_but_has_no_next_fire(self):
        self.library(jobs=[_job(enabled=False)])
        [listed] = main.list_schedules()
        self.assertFalse(listed.enabled)
        self.assertIsNone(listed.next_fire)


class RouteTest(unittest.TestCase):
    """The permissions the two endpoints are declared with.

    Asserted by walking `app.routes` rather than by calling them: `requires()`
    tags its dependency with the action it checks, which makes the wiring
    readable without an HTTP client.
    """

    def _actions(self, path, method):
        for route in main.app.routes:
            if getattr(route, "path", None) == path and method in getattr(route, "methods", ()):
                return [
                    getattr(dependency.call, "action", None)
                    for dependency in route.dependant.dependencies
                    if getattr(dependency.call, "action", None)
                ]
        self.fail(f"No {method} {path} route")

    def test_listing_needs_only_what_reading_the_record_needs(self):
        self.assertEqual(self._actions("/schedules", "GET"), ["workspace:Read"])

    def test_evaluating_needs_permission_to_run(self):
        # Unlike the monitors' evaluate, this one starts executions.
        self.assertEqual(self._actions("/schedules/evaluate", "POST"), ["run:Execute"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
