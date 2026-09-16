"""Tests for the alerting rules, the store behind them and the metrics page.

Two halves, deliberately. `evaluate()` is a pure function and most of what can go
wrong with an alert is a judgement call about numbers, so the bulk of these build
a `JobFacts` by hand and assert what the rule says about it — no database, no
clock, no Spark. The rest exercise the store and the HTTP surface, which is where
transitions, wildcards and the exposition format live.
"""

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

_TMP = tempfile.TemporaryDirectory()
os.environ["SPARQUET_STUDIO_AUDIT_DB"] = os.path.join(_TMP.name, "audit.sqlite3")
os.environ["SPARQUET_STUDIO_AUTH_DB"] = os.path.join(_TMP.name, "auth.sqlite3")
os.environ["SPARQUET_STUDIO_CREDITS_DB"] = os.path.join(_TMP.name, "credits.sqlite3")
os.environ["SPARQUET_STUDIO_HISTORY_DB"] = os.path.join(_TMP.name, "history.sqlite3")
os.environ["SPARQUET_STUDIO_MONITORS_DB"] = os.path.join(_TMP.name, "monitors.sqlite3")
os.environ["SPARQUET_STUDIO_WORKSPACE"] = os.path.join(_TMP.name, "workspace")
# The sweep thread would race every assertion below: these tests drive the sweep
# themselves so they can say exactly when it ran.
os.environ["SPARQUET_STUDIO_MONITORS"] = "off"
os.environ["SPARQUET_STUDIO_HISTORY_PURGE"] = "off"

import monitoring  # noqa: E402


def _facts(**overrides):
    base = dict(
        job_id="job-1",
        name="Daily sales",
        last_run_id="run-1",
        last_status=monitoring._SUCCESS,
        last_started_at=_iso(minutes_ago=5),
        last_finished_at=_iso(minutes_ago=4),
        last_duration_ms=60_000,
        last_rows_written=1_000,
        last_rows_read=1_000,
        last_success_at=_iso(minutes_ago=4),
        runs=5,
        failures=0,
    )
    base.update(overrides)
    return monitoring.JobFacts(**base)


def _iso(minutes_ago=0):
    moment = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return moment.isoformat().replace("+00:00", "Z")


class EvaluateFailedTests(unittest.TestCase):
    def setUp(self):
        self.rule = monitoring.Monitor(
            id="m", kind=monitoring.FAILED, job_id=monitoring.ANY_JOB,
            threshold=1, baseline=monitoring.ABSOLUTE, window=10, enabled=True,
            name=None, created_at="", updated_at="",
        )

    def test_a_job_that_has_never_run_never_fires(self):
        verdict = monitoring.evaluate(self.rule, _facts(last_status=None, runs=0))
        self.assertFalse(verdict.firing)

    def test_a_successful_run_does_not_fire(self):
        self.assertFalse(monitoring.evaluate(self.rule, _facts()).firing)

    def test_one_failure_fires_at_a_threshold_of_one(self):
        verdict = monitoring.evaluate(
            self.rule, _facts(last_status=monitoring._FAILED, consecutive_failures=1)
        )
        self.assertTrue(verdict.firing)
        self.assertEqual(verdict.value, 1)
        self.assertEqual(verdict.run_id, "run-1")

    def test_a_threshold_of_three_tolerates_two(self):
        self.rule.threshold = 3
        facts = _facts(last_status=monitoring._FAILED, consecutive_failures=2)
        self.assertFalse(monitoring.evaluate(self.rule, facts).firing)
        facts.consecutive_failures = 3
        self.assertTrue(monitoring.evaluate(self.rule, facts).firing)

    def test_the_reason_carries_the_error(self):
        verdict = monitoring.evaluate(
            self.rule,
            _facts(
                last_status=monitoring._FAILED, consecutive_failures=1,
                last_error="AnalysisException: table not found",
            ),
        )
        self.assertIn("table not found", verdict.reason)


class EvaluateLateTests(unittest.TestCase):
    def setUp(self):
        # "Nothing for two hours."
        self.rule = monitoring.Monitor(
            id="m", kind=monitoring.LATE, job_id="job-1", threshold=120,
            baseline=monitoring.ABSOLUTE, window=10, enabled=True, name=None,
            created_at="", updated_at="",
        )

    def test_a_recent_run_does_not_fire(self):
        self.assertFalse(monitoring.evaluate(self.rule, _facts()).firing)

    def test_silence_past_the_threshold_fires(self):
        facts = _facts(
            last_started_at=_iso(minutes_ago=180), last_success_at=_iso(minutes_ago=180)
        )
        verdict = monitoring.evaluate(self.rule, facts)
        self.assertTrue(verdict.firing)
        self.assertGreaterEqual(verdict.value, 179)

    def test_it_measures_from_the_last_success_not_the_last_run(self):
        # A Job that has been failing every ten minutes for three hours is not
        # producing anything, and a rule that watched "ran at all" would call it
        # healthy the whole time.
        facts = _facts(
            last_status=monitoring._FAILED, consecutive_failures=18,
            last_started_at=_iso(minutes_ago=1), last_success_at=_iso(minutes_ago=180),
        )
        self.assertTrue(monitoring.evaluate(self.rule, facts).firing)

    def test_a_job_that_never_succeeded_does_not_fire(self):
        # Nothing to be late for. If it ran and failed, `failed` says so, and
        # reporting one event under two names makes both worth less.
        facts = _facts(last_status=None, last_started_at=None, last_success_at=None, runs=0)
        self.assertFalse(monitoring.evaluate(self.rule, facts).firing)

    def test_an_unparseable_timestamp_does_not_fire(self):
        facts = _facts(last_success_at="não é uma data")
        self.assertFalse(monitoring.evaluate(self.rule, facts).firing)


class EvaluateDurationTests(unittest.TestCase):
    def setUp(self):
        self.rule = monitoring.Monitor(
            id="m", kind=monitoring.DURATION, job_id="job-1", threshold=2.0,
            baseline=monitoring.MEDIAN, window=10, enabled=True, name=None,
            created_at="", updated_at="",
        )

    def test_twice_the_median_fires(self):
        facts = _facts(last_duration_ms=240_000, durations=[60_000, 62_000, 58_000])
        verdict = monitoring.evaluate(self.rule, facts)
        self.assertTrue(verdict.firing)
        # The baseline in the verdict is the ceiling the run was measured
        # against — median 60s times the 2.0 the rule allows — not the median
        # itself, so the reason can quote one number and mean it.
        self.assertEqual(verdict.baseline, 120_000)

    def test_a_normal_run_does_not_fire(self):
        facts = _facts(last_duration_ms=61_000, durations=[60_000, 62_000, 58_000])
        self.assertFalse(monitoring.evaluate(self.rule, facts).firing)

    def test_one_slow_run_does_not_move_the_median(self):
        # The point of the median over the mean: yesterday's outlier must not
        # raise the bar high enough to hide today's.
        facts = _facts(last_duration_ms=200_000, durations=[60_000, 600_000, 58_000])
        self.assertTrue(monitoring.evaluate(self.rule, facts).firing)

    def test_no_history_means_no_judgement(self):
        facts = _facts(last_duration_ms=999_999, durations=[])
        self.assertFalse(monitoring.evaluate(self.rule, facts).firing)

    def test_a_failed_run_is_not_judged_on_time(self):
        # A run that died after four seconds is fast, and saying so would be
        # worse than saying nothing.
        facts = _facts(
            last_status=monitoring._FAILED, last_duration_ms=4_000,
            durations=[60_000, 62_000],
        )
        self.assertFalse(monitoring.evaluate(self.rule, facts).firing)

    def test_an_absolute_ceiling_is_in_milliseconds(self):
        # Same unit the history records and the reason quotes. `late` is the one
        # kind in minutes, because silence is measured in hours and a run is not.
        self.rule.baseline = monitoring.ABSOLUTE
        self.rule.threshold = 300_000
        self.assertFalse(monitoring.evaluate(self.rule, _facts(last_duration_ms=120_000)).firing)
        self.assertTrue(monitoring.evaluate(self.rule, _facts(last_duration_ms=600_000)).firing)

    def test_an_absolute_rule_with_no_threshold_judges_nothing(self):
        self.rule.baseline = monitoring.ABSOLUTE
        self.rule.threshold = 0
        self.assertFalse(monitoring.evaluate(self.rule, _facts(last_duration_ms=999_999)).firing)


class EvaluateVolumeTests(unittest.TestCase):
    def setUp(self):
        # "Fewer than half the usual rows."
        self.rule = monitoring.Monitor(
            id="m", kind=monitoring.VOLUME, job_id="job-1", threshold=0.5,
            baseline=monitoring.MEDIAN, window=10, enabled=True, name=None,
            created_at="", updated_at="",
        )

    def test_a_collapse_fires(self):
        facts = _facts(last_rows_written=100, volumes=[1_000, 1_100, 900])
        verdict = monitoring.evaluate(self.rule, facts)
        self.assertTrue(verdict.firing)
        self.assertEqual(verdict.value, 100)
        self.assertEqual(verdict.baseline, 500)

    def test_a_normal_load_does_not_fire(self):
        facts = _facts(last_rows_written=980, volumes=[1_000, 1_100, 900])
        self.assertFalse(monitoring.evaluate(self.rule, facts).firing)

    def test_more_rows_than_usual_does_not_fire(self):
        # This rule is a floor, not a band: a day with twice the rows is a good
        # day until somebody says otherwise.
        facts = _facts(last_rows_written=5_000, volumes=[1_000, 1_100, 900])
        self.assertFalse(monitoring.evaluate(self.rule, facts).firing)

    def test_zero_rows_fires(self):
        facts = _facts(last_rows_written=0, volumes=[1_000, 900])
        self.assertTrue(monitoring.evaluate(self.rule, facts).firing)

    def test_an_absolute_floor(self):
        self.rule.baseline = monitoring.ABSOLUTE
        self.rule.threshold = 500
        self.assertTrue(monitoring.evaluate(self.rule, _facts(last_rows_written=100)).firing)
        self.assertFalse(monitoring.evaluate(self.rule, _facts(last_rows_written=900)).firing)

    def test_a_failed_run_is_not_judged_on_volume(self):
        facts = _facts(
            last_status=monitoring._FAILED, last_rows_written=0, volumes=[1_000, 900]
        )
        self.assertFalse(monitoring.evaluate(self.rule, facts).firing)


class MedianTests(unittest.TestCase):
    def test_odd_and_even_and_empty(self):
        self.assertEqual(monitoring.median([3, 1, 2]), 2)
        self.assertEqual(monitoring.median([1, 2, 3, 4]), 2.5)
        self.assertIsNone(monitoring.median([]))


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.store = monitoring.MonitorStore(Path(self.dir.name) / "m.sqlite3")

    def test_create_and_list_and_delete(self):
        monitor = self.store.create(monitoring.FAILED, name="Anything that fails")
        self.assertEqual([m.id for m in self.store.list()], [monitor.id])
        self.assertTrue(self.store.delete(monitor.id))
        self.assertEqual(self.store.list(), [])
        self.assertFalse(self.store.delete(monitor.id))

    def test_an_unknown_kind_is_refused(self):
        with self.assertRaises(ValueError):
            self.store.create("vibes")

    def test_an_unknown_baseline_is_refused(self):
        with self.assertRaises(ValueError):
            self.store.create(monitoring.DURATION, baseline="average")

    def _record(self, monitor, facts):
        return self.store.record(
            monitor, facts.job_id, monitoring.evaluate(monitor, facts)
        )

    def test_updating_the_question_discards_the_answer(self):
        monitor = self.store.create(monitoring.FAILED)
        self._record(monitor, _facts(last_status=monitoring._FAILED, consecutive_failures=1))
        self.assertTrue(self.store.states()[0].firing)
        self.store.update(monitor.id, threshold=5)
        # The old verdict answered a different question, so keeping it would show
        # an alert nothing is currently asserting.
        self.assertEqual(self.store.states(), [])

    def test_renaming_keeps_the_state(self):
        monitor = self.store.create(monitoring.FAILED)
        self._record(monitor, _facts(last_status=monitoring._FAILED, consecutive_failures=1))
        self.store.update(monitor.id, name="On call")
        self.assertTrue(self.store.states()[0].firing)

    def test_only_transitions_are_recorded(self):
        monitor = self.store.create(monitoring.FAILED)
        broken = _facts(last_status=monitoring._FAILED, consecutive_failures=1)
        self.assertIsNotNone(self._record(monitor, broken))
        self.assertIsNone(self._record(monitor, broken))  # still broken
        self.assertIsNotNone(self._record(monitor, _facts()))  # fixed
        self.assertEqual(len(self.store.events()), 2)

    def test_a_first_look_that_is_fine_is_not_an_event(self):
        monitor = self.store.create(monitoring.FAILED)
        self.assertIsNone(self._record(monitor, _facts()))
        self.assertEqual(self.store.events(), [])

    def test_deleting_a_monitor_takes_its_state_and_events(self):
        monitor = self.store.create(monitoring.FAILED)
        self._record(monitor, _facts(last_status=monitoring._FAILED, consecutive_failures=1))
        self.store.delete(monitor.id)
        self.assertEqual(self.store.states(), [])
        self.assertEqual(self.store.events(), [])

    def test_forget_drops_state_for_jobs_that_no_longer_exist(self):
        monitor = self.store.create(monitoring.FAILED)
        self._record(
            monitor,
            _facts(job_id="gone", last_status=monitoring._FAILED, consecutive_failures=1),
        )
        self.assertEqual(len(self.store.states()), 1)
        self.assertEqual(self.store.forget(["job-1"]), 1)
        self.assertEqual(self.store.states(), [])


class SweepTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.store = monitoring.MonitorStore(Path(self.dir.name) / "m.sqlite3")

    def test_a_wildcard_rule_covers_every_job(self):
        self.store.create(monitoring.FAILED, job_id=monitoring.ANY_JOB)
        health = [
            _facts(job_id="a", last_status=monitoring._FAILED, consecutive_failures=1),
            _facts(job_id="b"),
            _facts(job_id="c", last_status=monitoring._FAILED, consecutive_failures=1),
        ]
        events = monitoring.sweep(self.store, health, notifier=lambda *a: None)
        self.assertEqual(sorted(event.job_id for event in events), ["a", "c"])

    def test_a_targeted_rule_ignores_the_others(self):
        self.store.create(monitoring.FAILED, job_id="b")
        health = [
            _facts(job_id="a", last_status=monitoring._FAILED, consecutive_failures=1),
            _facts(job_id="b", last_status=monitoring._FAILED, consecutive_failures=1),
        ]
        events = monitoring.sweep(self.store, health, notifier=lambda *a: None)
        self.assertEqual([event.job_id for event in events], ["b"])

    def test_a_disabled_rule_is_not_evaluated(self):
        self.store.create(monitoring.FAILED, enabled=False)
        health = [_facts(last_status=monitoring._FAILED, consecutive_failures=1)]
        self.assertEqual(monitoring.sweep(self.store, health, notifier=lambda *a: None), [])

    def test_the_notifier_sees_each_transition_once(self):
        self.store.create(monitoring.FAILED)
        seen = []
        health = [_facts(last_status=monitoring._FAILED, consecutive_failures=1)]
        monitoring.sweep(self.store, health, notifier=lambda m, s: seen.append(s))
        monitoring.sweep(self.store, health, notifier=lambda m, s: seen.append(s))
        self.assertEqual(len(seen), 1)

    def test_a_notifier_that_throws_does_not_stop_the_sweep(self):
        # A webhook that is down is a webhook that is down; the alert is still
        # recorded, and the next rule is still evaluated.
        self.store.create(monitoring.FAILED)
        def angry(monitor, state):
            raise RuntimeError("no route to host")
        health = [_facts(last_status=monitoring._FAILED, consecutive_failures=1)]
        events = monitoring.sweep(self.store, health, notifier=angry)
        self.assertEqual(len(events), 1)
        self.assertTrue(self.store.states()[0].firing)


class ConfigTests(unittest.TestCase):
    def test_the_interval_has_a_floor(self):
        self.assertEqual(monitoring.interval_seconds({"SPARQUET_STUDIO_MONITOR_INTERVAL": "1"}), 5)
        self.assertEqual(monitoring.interval_seconds({"SPARQUET_STUDIO_MONITOR_INTERVAL": "90"}), 90)

    def test_nonsense_falls_back_to_the_default(self):
        self.assertEqual(
            monitoring.interval_seconds({"SPARQUET_STUDIO_MONITOR_INTERVAL": "logo"}),
            monitoring.DEFAULT_INTERVAL_SECONDS,
        )

    def test_sweeping_is_on_unless_turned_off(self):
        self.assertTrue(monitoring.sweeping_enabled({}))
        for value in ("0", "off", "false", "no", "OFF"):
            self.assertFalse(monitoring.sweeping_enabled({"SPARQUET_STUDIO_MONITORS": value}))

    def test_no_webhook_configured_means_no_call(self):
        self.assertEqual(monitoring.webhook_url({}), "")
        # And `notify` with no URL is a no-op rather than an error.
        state = monitoring.MonitorState(
            monitor_id="m", job_id="j", firing=True, reason="broken"
        )
        monitor = monitoring.Monitor(
            id="m", kind=monitoring.FAILED, job_id="*", threshold=1,
            baseline=monitoring.ABSOLUTE, window=10, enabled=True, name=None,
            created_at="", updated_at="",
        )
        self.assertFalse(monitoring.notify(monitor, state, url=""))


class HistoryJobHealthTests(unittest.TestCase):
    """`job_health()` is what turns rows into the facts the rules judge."""

    def setUp(self):
        import history

        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.history = history
        self.repo = history.SQLiteExecutionRepository(Path(self.dir.name) / "h.sqlite3")
        self.repo.upsert_job("job-1", name="Daily sales", workflow_id="wf-1")

    def _run(self, status, *, duration_ms=60_000, rows_written=1_000):
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id="wf-1", pipeline_id=None, job_id="job-1",
            name="Daily sales",
        )
        job_run_id = self.repo.create_job_run(
            run_id, job_id="job-1", name="Daily sales", stage_index=0
        )
        self.repo.finish_job_run(
            job_run_id, status=status, duration_ms=duration_ms,
            rows_read=rows_written, rows_written=rows_written,
            error=None if status == self.history.SUCCESS else "boom",
        )
        self.repo.finish_pipeline_run(
            run_id, status=status, duration_ms=duration_ms, error=None
        )
        return run_id

    def test_a_job_with_no_runs_reports_itself_anyway(self):
        health = {record.job_id: record for record in self.repo.job_health()}
        self.assertIn("job-1", health)
        self.assertIsNone(health["job-1"].last_status)
        self.assertEqual(health["job-1"].runs, 0)

    def test_the_failure_streak_stops_at_the_last_success(self):
        self._run(self.history.SUCCESS)
        self._run(self.history.FAILED)
        self._run(self.history.FAILED)
        record = self.repo.job_health()[0]
        self.assertEqual(record.consecutive_failures, 2)
        self.assertEqual(record.runs, 3)
        self.assertEqual(record.failures, 2)

    def test_the_judged_run_is_not_in_its_own_baseline(self):
        self._run(self.history.SUCCESS, duration_ms=60_000, rows_written=1_000)
        self._run(self.history.SUCCESS, duration_ms=90_000, rows_written=2_000)
        record = self.repo.job_health()[0]
        self.assertEqual(record.last_duration_ms, 90_000)
        self.assertEqual(record.durations, [60_000])
        self.assertEqual(record.volumes, [1_000])

    def test_a_deleted_job_drops_out(self):
        self._run(self.history.SUCCESS)
        self.repo.soft_delete("job", "job-1")
        self.assertEqual(self.repo.job_health(), [])


class ApiTests(unittest.TestCase):
    """The route handlers, called the way FastAPI calls them.

    `starlette.testclient` wants httpx, which the runner does not depend on and
    which is not worth a dependency to exercise nine handlers. So these call the
    functions directly and check separately that every route declares the
    permission it should — which is the part a mistake here would actually cost.
    """

    @classmethod
    def setUpClass(cls):
        import main

        cls.main = main

    def setUp(self):
        for monitor in self.main._monitors.list():
            self.main._monitors.delete(monitor.id)

    def _create(self, **body):
        payload = {"kind": monitoring.FAILED}
        payload.update(body)
        return self.main.create_monitor(self.main.MonitorIn(**payload))

    def test_a_rule_round_trips(self):
        created = self._create(name="Anything that fails")
        self.assertEqual([m.id for m in self.main.list_monitors()], [created.id])
        self.assertTrue(created.rule)

    def test_an_unknown_kind_is_a_400_not_a_500(self):
        with self.assertRaises(self.main.HTTPException) as caught:
            self._create(kind="vibes")
        self.assertEqual(caught.exception.status_code, 400)

    def test_patching_a_rule_that_is_not_there_is_a_404(self):
        with self.assertRaises(self.main.HTTPException) as caught:
            self.main.update_monitor("nope", self.main.MonitorPatch(enabled=False))
        self.assertEqual(caught.exception.status_code, 404)

    def test_deleting_a_rule_that_is_not_there_is_a_404(self):
        with self.assertRaises(self.main.HTTPException) as caught:
            self.main.delete_monitor("nope")
        self.assertEqual(caught.exception.status_code, 404)

    def test_patch_changes_only_what_was_sent(self):
        created = self._create(name="On call", threshold=3)
        patched = self.main.update_monitor(created.id, self.main.MonitorPatch(enabled=False))
        self.assertFalse(patched.enabled)
        self.assertEqual(patched.name, "On call")
        self.assertEqual(patched.threshold, 3)

    def test_every_route_declares_a_permission(self):
        wanted = {
            ("GET", "/health/jobs"): "monitoring:Read",
            ("GET", "/monitors"): "monitoring:Read",
            ("POST", "/monitors"): "monitoring:Manage",
            ("GET", "/monitors/status"): "monitoring:Read",
            ("GET", "/monitors/events"): "monitoring:Read",
            ("POST", "/monitors/evaluate"): "monitoring:Read",
            ("GET", "/metrics"): "monitoring:Read",
        }
        seen = {}
        for route in self.main.app.routes:
            for method in getattr(route, "methods", ()) or ():
                key = (method, getattr(route, "path", ""))
                if key in wanted:
                    seen[key] = [
                        getattr(dependency.dependency, "action", None)
                        for dependency in route.dependencies
                    ]
        for key, action in wanted.items():
            self.assertIn(key, seen, key)
            self.assertIn(action, seen[key], key)

    def test_patch_and_delete_need_manage(self):
        # Reading an alert and turning one off are different decisions.
        for method, path in (("PATCH", "/monitors/{monitor_id}"),
                             ("DELETE", "/monitors/{monitor_id}")):
            actions = [
                getattr(dependency.dependency, "action", None)
                for route in self.main.app.routes
                if getattr(route, "path", "") == path
                and method in (getattr(route, "methods", ()) or ())
                for dependency in route.dependencies
            ]
            self.assertIn("monitoring:Manage", actions, path)

    def test_evaluate_answers_with_what_it_checked(self):
        self._create()
        report = self.main.evaluate_monitors()
        self.assertIsInstance(report.checked, int)
        self.assertIsInstance(report.firing, int)

    def test_status_and_events_are_readable(self):
        self._create()
        self.main.evaluate_monitors()
        self.assertIsInstance(self.main.monitor_status(), list)
        self.assertIsInstance(self.main.monitor_events(), list)

    def test_metrics_is_prometheus_text(self):
        response = self.main.metrics()
        self.assertTrue(response.media_type.startswith("text/plain"))
        body = response.body.decode("utf-8")
        self.assertIn("# TYPE sparquet_monitors_firing gauge", body)
        self.assertIn("sparquet_runner_info{", body)
        for line in body.splitlines():
            if not line:
                continue
            if line.startswith("# TYPE"):
                # Everything is a gauge on purpose: these are read from rows that
                # get purged, and a `_total` that goes down makes every `rate()`
                # over it wrong.
                self.assertTrue(line.endswith(" gauge"), line)
            elif not line.startswith("#"):
                self.assertRegex(line, r"^[a-z_]+(\{.*\})? -?[0-9.e+]+$")

    def test_a_label_with_a_quote_in_it_is_escaped(self):
        rendered = self.main._render_metrics(
            [
                self.main.history.JobHealth(
                    job_id='j"1', name='say "hi"\nagain', workflow_id=None,
                    last_status="success",
                )
            ],
            [],
            {},
        )
        self.assertIn(r'job="say \"hi\"\nagain"', rendered)


if __name__ == "__main__":
    unittest.main(verbosity=1)
