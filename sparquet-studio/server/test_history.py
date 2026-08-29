"""Tests for the execution-history repository (`history.py`).

Stdlib only — no pytest, no FastAPI, no Spark — so it runs the same way the rest of
the repo's tests do:

    python sparquet-studio/server/test_history.py
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

import history


def _log(message: str, **context: object) -> dict:
    return {"timestamp": "2026-01-01T00:00:00Z", "level": "INFO",
            "message": message, "context": context}


class HistoryRepositoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = history.SQLiteExecutionRepository(Path(self._tmp.name) / "history.sqlite3")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_success_run_persists_nested_job_and_step_runs(self) -> None:
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id="w1", pipeline_id=None, job_id="j1", name="orders",
        )
        job_run_id = self.repo.create_job_run(
            run_id, job_id="j1", name="orders", stage_index=0,
        )
        tracker = history.StepTracker(self.repo, job_run_id)
        tracker.handle(_log("Transformation started", type="filter", index=0, total=2, step=True))
        tracker.handle(_log("Transformation applied", type="filter", index=0, total=2, step=True))
        tracker.handle(_log("Transformation started", type="cast", index=1, total=2, step=True))
        tracker.handle(_log("Transformation applied", type="cast", index=1, total=2, step=True))
        tracker.close(error_message=None)

        self.repo.finish_job_run(
            job_run_id, status=history.SUCCESS, duration_ms=120,
            error=None, rows_read=10, rows_written=10,
        )
        self.repo.finish_pipeline_run(run_id, status=history.SUCCESS, duration_ms=120, error=None)

        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        self.assertEqual(run.status, history.SUCCESS)
        self.assertEqual(len(run.jobs), 1)
        job = run.jobs[0]
        self.assertEqual(job.status, history.SUCCESS)
        self.assertEqual([s.status for s in job.steps], [history.SUCCESS, history.SUCCESS])
        self.assertEqual([s.type for s in job.steps], ["filter", "cast"])

    def test_step_that_raises_is_left_failed_by_close(self) -> None:
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j1", name="orders",
        )
        job_run_id = self.repo.create_job_run(run_id, job_id="j1", name="orders", stage_index=0)
        tracker = history.StepTracker(self.repo, job_run_id)
        # Step 0 runs fully; step 1 starts, then the pipeline blows up mid-step —
        # so only a "started" record ever arrives for it.
        tracker.handle(_log("Transformation started", type="filter", index=0, total=2, step=True))
        tracker.handle(_log("Transformation applied", type="filter", index=0, total=2, step=True))
        tracker.handle(_log("Transformation started", type="join", index=1, total=2, step=True))
        tracker.close(error_message="boom: join key not found")

        self.repo.finish_job_run(
            job_run_id, status=history.FAILED, duration_ms=80,
            error="boom: join key not found", rows_read=10, rows_written=0,
        )
        self.repo.finish_pipeline_run(
            run_id, status=history.FAILED, duration_ms=80, error="boom: join key not found",
        )

        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        job = run.jobs[0]
        self.assertEqual(job.status, history.FAILED)
        self.assertEqual(job.steps[0].status, history.SUCCESS)
        self.assertEqual(job.steps[1].status, history.FAILED)
        self.assertEqual(job.steps[1].error_message, "boom: join key not found")

    def test_skipped_transformation_is_recorded_without_start_finish_pair(self) -> None:
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j1", name="orders",
        )
        job_run_id = self.repo.create_job_run(run_id, job_id="j1", name="orders", stage_index=0)
        tracker = history.StepTracker(self.repo, job_run_id)
        tracker.handle(_log(
            "Transformation skipped", type="enrich", skip_if_false="flag", index=0, total=1, step=True,
        ))
        tracker.close(error_message=None)

        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        step = run.jobs[0].steps[0]
        self.assertEqual(step.status, history.SKIPPED)
        self.assertEqual(step.duration_ms, 0)

    def test_downstream_jobs_are_marked_skipped_after_a_stage_failure(self) -> None:
        run_id = self.repo.create_pipeline_run(
            kind="pipeline", workflow_id="w1", pipeline_id="p1", job_id=None, name="flow",
        )
        failed_job = self.repo.create_job_run(run_id, job_id="stage-a", name="A", stage_index=0)
        self.repo.finish_job_run(
            failed_job, status=history.FAILED, duration_ms=50,
            error="stage A blew up", rows_read=0, rows_written=0,
        )
        self.repo.skip_job_run(run_id, job_id="stage-b", name="B", stage_index=1)
        self.repo.finish_pipeline_run(run_id, status=history.FAILED, duration_ms=50, error="stage A blew up")

        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        self.assertEqual([j.status for j in run.jobs], [history.FAILED, history.SKIPPED])
        skipped = run.jobs[1]
        self.assertIsNone(skipped.started_at)
        self.assertIsNone(skipped.finished_at)

    def test_cancelled_run_marks_the_open_step_and_the_stages_that_never_ran(self) -> None:
        # Stop pressed mid-stage: the step that was running is CANCELLED, not
        # FAILED — nothing is wrong with the pipeline — and the stages behind it
        # are CANCELLED too, so none of them is left looking pending forever.
        run_id = self.repo.create_pipeline_run(
            kind="pipeline", workflow_id="w1", pipeline_id="p1", job_id=None, name="flow",
        )
        job_run_id = self.repo.create_job_run(run_id, job_id="stage-a", name="A", stage_index=0)
        tracker = history.StepTracker(self.repo, job_run_id)
        tracker.handle(_log("Output started", scope="output", type="parquet", index=0, step=True))
        tracker.close("Cancelled from Studio while it was running.", status=history.CANCELLED)
        self.repo.finish_job_run(
            job_run_id, status=history.CANCELLED, duration_ms=120,
            error="Cancelled from Studio while it was running.",
            rows_read=10, rows_written=0,
        )
        self.repo.skip_job_run(
            run_id, job_id="stage-b", name="B", stage_index=1, status=history.CANCELLED,
        )
        self.repo.finish_pipeline_run(
            run_id, status=history.CANCELLED, duration_ms=120,
            error="Cancelled from Studio while it was running.",
        )

        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        self.assertEqual(run.status, history.CANCELLED)
        self.assertEqual([j.status for j in run.jobs], [history.CANCELLED, history.CANCELLED])
        step = run.jobs[0].steps[0]
        self.assertEqual(step.status, history.CANCELLED)
        self.assertEqual(step.error_message, "Cancelled from Studio while it was running.")

    def test_skip_job_run_still_defaults_to_skipped(self) -> None:
        # The failure path must not start reporting cancellations.
        run_id = self.repo.create_pipeline_run(
            kind="pipeline", workflow_id=None, pipeline_id="p1", job_id=None, name="flow",
        )
        self.repo.skip_job_run(run_id, job_id="stage-b", name="B", stage_index=1)

        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        self.assertEqual(run.jobs[0].status, history.SKIPPED)

    def test_list_pipeline_runs_filters_and_orders_most_recent_first(self) -> None:
        first = self.repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j1", name="a",
        )
        self.repo.finish_pipeline_run(first, status=history.SUCCESS, duration_ms=10, error=None)
        second = self.repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j1", name="a",
        )
        self.repo.finish_pipeline_run(second, status=history.FAILED, duration_ms=10, error="x")
        other_job = self.repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j2", name="b",
        )
        self.repo.finish_pipeline_run(other_job, status=history.SUCCESS, duration_ms=10, error=None)

        runs = self.repo.list_pipeline_runs(job_id="j1", limit=10)
        self.assertEqual([r.id for r in runs], [second, first])
        # Summaries do not carry nested jobs — fetch get_pipeline_run() for that.
        self.assertEqual(runs[0].jobs, [])

    def test_job_history_includes_the_pipeline_runs_that_ran_it_as_a_stage(self) -> None:
        """A Job run through a Studio Pipeline is recorded under the PIPELINE's
        pipeline_run, whose `job_id` is null. Filtering on that column alone hid
        every such execution from the Job's own history."""
        solo = self.repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j1", name="orders",
        )
        self.repo.finish_pipeline_run(solo, status=history.SUCCESS, duration_ms=10, error=None)
        flow = self.repo.create_pipeline_run(
            kind="pipeline", workflow_id="w1", pipeline_id="p1", job_id=None, name="nightly",
        )
        staged = self.repo.create_job_run(flow, job_id="j1", name="orders", stage_index=0)
        self.repo.finish_job_run(
            staged, status=history.SUCCESS, duration_ms=20,
            error=None, rows_read=5, rows_written=5,
        )
        self.repo.create_job_run(flow, job_id="j2", name="marts", stage_index=1)
        self.repo.finish_pipeline_run(flow, status=history.SUCCESS, duration_ms=40, error=None)

        runs = self.repo.list_pipeline_runs(job_id="j1", limit=10)
        self.assertEqual({r.id for r in runs}, {solo, flow})
        # A Job that never took part is still filtered out.
        self.assertEqual([r.id for r in self.repo.list_pipeline_runs(job_id="j3", limit=10)], [])

    def test_steps_come_back_in_execution_order_not_alphabetical_scope_order(self) -> None:
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j1", name="orders",
        )
        job_run_id = self.repo.create_job_run(run_id, job_id="j1", name="orders", stage_index=0)
        tracker = history.StepTracker(self.repo, job_run_id)
        # Arrives in the order the framework runs it: read, transform, rule,
        # quality dataset, write. Sorted by scope it would read output, input, …
        markers = [
            ("Input started", "Input read", {"scope": "input", "index": 0}),
            ("Transformation started", "Transformation applied", {"index": 0}),
            ("Validation started", "Validation finished", {"scope": "validation", "index": 0}),
            ("Validation output started", "Validation output written",
             {"scope": "validation_sink", "role": "report"}),
            ("Output started", "Output written", {"scope": "output", "index": 0}),
        ]
        clock = 0
        for opening, closing_message, context in markers:
            for message in (opening, closing_message):
                record = _log(message, step=True, **context)
                record["timestamp"] = f"2026-01-01T00:00:{clock:02d}Z"
                clock += 1
                tracker.handle(record)
        tracker.close(error_message=None)

        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        self.assertEqual(
            [s.scope for s in run.jobs[0].steps],
            ["input", "transformation", "validation", "validation_sink", "output"],
        )

    def test_get_pipeline_run_missing_returns_none(self) -> None:
        self.assertIsNone(self.repo.get_pipeline_run("does-not-exist"))

    def test_role_keyed_quality_dataset_steps_are_recorded(self) -> None:
        """The quality datasets carry a `role` and no `index`. They used to be
        dropped, which left the write that most often fails — the validation
        report — with no step for Studio to point at."""
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j1", name="orders",
        )
        job_run_id = self.repo.create_job_run(run_id, job_id="j1", name="orders", stage_index=0)
        tracker = history.StepTracker(self.repo, job_run_id)
        tracker.handle(_log(
            "Validation output started", scope="validation_sink", role="report",
            step=True, format="csv", path="out/report",
        ))
        tracker.handle(_log(
            "Validation output written", scope="validation_sink", role="report",
            step=True, format="csv", path="out/report", rows=3,
        ))
        tracker.handle(_log(
            "Validation output started", scope="validation_sink", role="invalid",
            step=True, format="parquet", path="out/quarantine",
        ))
        # The quarantine write is where the run died: no closing marker arrives.
        tracker.close(error_message="Python worker exited unexpectedly")

        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        steps = run.jobs[0].steps
        self.assertEqual([s.role for s in steps], ["report", "invalid"])
        self.assertEqual([s.scope for s in steps], ["validation_sink"] * 2)
        self.assertEqual([s.status for s in steps], [history.SUCCESS, history.FAILED])
        # Arrival order stands in for the position a role has none of.
        self.assertEqual([s.step_index for s in steps], [0, 1])
        self.assertEqual(
            steps[1].error_message, "Python worker exited unexpectedly",
        )

    def test_step_details_keep_what_the_framework_reported(self) -> None:
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j1", name="orders",
        )
        job_run_id = self.repo.create_job_run(run_id, job_id="j1", name="orders", stage_index=0)
        tracker = history.StepTracker(self.repo, job_run_id)
        tracker.handle(_log(
            "Input started", scope="input", index=0, total=1, step=True,
            format="csv", path="data/in.csv",
        ))
        tracker.handle(_log(
            "Input read", scope="input", index=0, total=1, step=True,
            format="csv", rows=42,
        ))
        # Starts and never closes: what the opening marker said must survive.
        tracker.handle(_log(
            "Output started", scope="output", index=0, total=1, step=True,
            format="delta", path="data/out",
        ))
        tracker.close(error_message="disk full")

        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        read, write = run.jobs[0].steps
        self.assertEqual(
            json.loads(read.details or "{}"),
            {"format": "csv", "path": "data/in.csv", "rows": 42},
        )
        self.assertEqual(
            json.loads(write.details or "{}"),
            {"format": "delta", "path": "data/out"},
        )

    def test_older_database_gains_the_columns_added_later(self) -> None:
        """`CREATE TABLE IF NOT EXISTS` never reaches a table that already exists,
        so a database written by an earlier runner has to be migrated in place."""
        path = Path(self._tmp.name) / "old.sqlite3"
        with closing(sqlite3.connect(path)) as conn:
            conn.executescript(
                "CREATE TABLE step_run (id TEXT PRIMARY KEY, job_run_id TEXT NOT NULL, "
                "scope TEXT NOT NULL, step_index INTEGER NOT NULL, type TEXT, "
                "status TEXT NOT NULL, started_at TEXT, finished_at TEXT, "
                "duration_ms INTEGER, error_message TEXT, error_details TEXT);"
                "INSERT INTO step_run (id, job_run_id, scope, step_index, type, status) "
                "VALUES ('s1', 'j1', 'transformation', 0, 'filter', 'success');"
            )
            conn.commit()

        repo = history.SQLiteExecutionRepository(path)
        with closing(sqlite3.connect(path)) as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(step_run)")}
        self.assertIn("role", columns)
        self.assertIn("details", columns)
        # The row that predates the columns still reads back, with them empty.
        run_id = repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j1", name="orders",
        )
        self.assertIsNotNone(repo.get_pipeline_run(run_id))

    def test_older_pipeline_and_job_run_tables_gain_their_columns(self) -> None:
        """Same reason as the step_run migration above, one level up: `run_as`,
        `launched` and `lineage` were added after databases existed in the wild."""
        path = Path(self._tmp.name) / "old-runs.sqlite3"
        with closing(sqlite3.connect(path)) as conn:
            conn.executescript(
                "CREATE TABLE pipeline_run (id TEXT PRIMARY KEY, kind TEXT NOT NULL, "
                "workflow_id TEXT, pipeline_id TEXT, job_id TEXT, name TEXT, "
                "status TEXT NOT NULL, started_at TEXT, finished_at TEXT, "
                "duration_ms INTEGER, error TEXT);"
                "CREATE TABLE job_run (id TEXT PRIMARY KEY, pipeline_run_id TEXT NOT NULL, "
                "job_id TEXT, name TEXT, stage_index INTEGER NOT NULL, status TEXT NOT NULL, "
                "started_at TEXT, finished_at TEXT, duration_ms INTEGER, error TEXT, "
                "rows_read INTEGER, rows_written INTEGER);"
                "INSERT INTO pipeline_run (id, kind, status) VALUES ('r1', 'job', 'success');"
                "INSERT INTO job_run (id, pipeline_run_id, stage_index, status) "
                "VALUES ('j1', 'r1', 0, 'success');"
            )
            conn.commit()

        repo = history.SQLiteExecutionRepository(path)
        with closing(sqlite3.connect(path)) as conn:
            pipeline_columns = {row[1] for row in conn.execute("PRAGMA table_info(pipeline_run)")}
            job_columns = {row[1] for row in conn.execute("PRAGMA table_info(job_run)")}
        self.assertIn("run_as", pipeline_columns)
        self.assertIn("launched", pipeline_columns)
        self.assertIn("lineage", job_columns)
        # The rows that predate the columns still read back, with them empty.
        old = repo.get_pipeline_run("r1")
        assert old is not None
        self.assertIsNone(old.run_as)
        self.assertIsNone(old.launched)
        self.assertIsNone(old.jobs[0].lineage)

    def test_run_records_who_asked_for_it_and_how_it_was_launched(self) -> None:
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id="w1", pipeline_id=None, job_id="j1", name="orders",
            run_as="victor", launched=history.SCHEDULED,
        )
        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        self.assertEqual(run.run_as, "victor")
        self.assertEqual(run.launched, history.SCHEDULED)

    def test_run_defaults_to_manual_when_the_caller_says_nothing(self) -> None:
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j1", name="orders",
        )
        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        self.assertEqual(run.launched, history.MANUAL)
        self.assertIsNone(run.run_as)

    def test_lineage_is_stored_on_the_job_run_and_read_back_whole(self) -> None:
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j1", name="orders",
        )
        lineage = history.lineage_of(
            {
                "input": {"format": "csv", "path": "data/orders.csv"},
                "output": {"format": "parquet", "path": "out/orders", "mode": "overwrite"},
            }
        )
        job_run_id = self.repo.create_job_run(
            run_id, job_id="j1", name="orders", stage_index=0, lineage=lineage,
        )
        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        stored = json.loads(run.jobs[0].lineage or "{}")
        self.assertEqual(
            stored,
            {
                "inputs": [{"role": "input", "format": "csv", "address": "data/orders.csv"}],
                "outputs": [
                    {
                        "role": "output",
                        "format": "parquet",
                        "address": "out/orders",
                        "mode": "overwrite",
                    }
                ],
            },
        )
        self.assertEqual(job_run_id, run.jobs[0].id)

    def _job_run(self) -> str:
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id=None, pipeline_id=None, job_id="j1", name="orders",
        )
        return self.repo.create_job_run(run_id, job_id="j1", name="orders", stage_index=0)

    def test_logs_are_stored_in_arrival_order_and_read_back_whole(self) -> None:
        job_run_id = self._job_run()
        self.repo.append_logs(job_run_id, [
            {"source": "pipeline", "timestamp": "2026-01-01T00:00:00Z", "level": "INFO",
             "message": "Input started", "context": {"scope": "input", "index": 0}},
            {"source": "spark", "timestamp": "2026-01-01T00:00:01Z", "level": "WARN",
             "message": "spark says something", "context": {}},
        ])

        lines = self.repo.list_logs(job_run_id)
        self.assertEqual([line.seq for line in lines], [1, 2])
        self.assertEqual([line.source for line in lines], ["pipeline", "spark"])
        self.assertEqual(json.loads(lines[0].context or "{}"), {"scope": "input", "index": 0})
        # An empty context is stored as NULL, not as "{}": nothing to show.
        self.assertIsNone(lines[1].context)
        self.assertEqual(self.repo.count_logs(job_run_id), 2)

    def test_logs_keep_one_sequence_across_batches_and_page_by_it(self) -> None:
        job_run_id = self._job_run()
        self.repo.append_logs(job_run_id, [_log(f"first {i}") for i in range(3)])
        last = self.repo.append_logs(job_run_id, [_log(f"second {i}") for i in range(2)])

        self.assertEqual(last, 5)
        page = self.repo.list_logs(job_run_id, after_seq=3, limit=10)
        self.assertEqual([line.message for line in page], ["second 0", "second 1"])
        self.assertEqual(len(self.repo.list_logs(job_run_id, limit=2)), 2)

    def test_logs_of_one_job_run_never_leak_into_another(self) -> None:
        first = self._job_run()
        second = self._job_run()
        self.repo.append_logs(first, [_log("mine")])
        self.repo.append_logs(second, [_log("yours")])

        self.assertEqual([line.message for line in self.repo.list_logs(first)], ["mine"])
        # Each job execution counts from 1: the sequence is per job run, not global.
        self.assertEqual([line.seq for line in self.repo.list_logs(second)], [1])

    def test_appending_no_logs_writes_nothing(self) -> None:
        job_run_id = self._job_run()
        self.assertEqual(self.repo.append_logs(job_run_id, []), 0)
        self.assertEqual(self.repo.count_logs(job_run_id), 0)

    def test_step_run_cannot_be_orphaned_from_its_job_run(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.create_step_run(
                "no-such-job-run", "transform", 0, "filter",
                status=history.RUNNING, timestamp="2026-01-01T00:00:00Z",
            )


class LineageTest(unittest.TestCase):
    """`lineage_of` reads the submitted JSON, never the run's log records — so a
    run that dies on its first read still reports what it was going to touch."""

    def _lineage(self, config: object) -> dict:
        return json.loads(history.lineage_of(config) or "null")

    def test_reads_both_sides_of_a_plain_job(self) -> None:
        lineage = self._lineage(
            {
                "input": {"format": "csv", "path": "data/in.csv"},
                "output": {"format": "delta", "path": "data/out", "mode": "append"},
            }
        )
        self.assertEqual(
            lineage["inputs"], [{"role": "input", "format": "csv", "address": "data/in.csv"}]
        )
        self.assertEqual(
            lineage["outputs"],
            [{"role": "output", "format": "delta", "address": "data/out", "mode": "append"}],
        )

    def test_a_join_counts_as_a_second_input(self) -> None:
        lineage = self._lineage(
            {
                "input": {"format": "parquet", "path": "a"},
                "transformations": [
                    {"type": "filter", "condition": "x > 1"},
                    {"type": "join", "with": {"format": "parquet", "path": "b"}, "on": ["id"]},
                ],
                "output": {"format": "parquet", "path": "c"},
            }
        )
        self.assertEqual(
            [(d["role"], d["address"]) for d in lineage["inputs"]],
            [("input", "a"), ("join", "b")],
        )

    def test_quality_sinks_keep_the_role_that_names_them(self) -> None:
        lineage = self._lineage(
            {
                "input": {"format": "csv", "path": "in"},
                "validations": {
                    "report": {"format": "csv", "path": "out/report"},
                    "outputs": {
                        "valid": {"format": "parquet", "path": "out/valid"},
                        "invalid": {"format": "parquet", "path": "out/invalid"},
                    },
                },
            }
        )
        self.assertEqual(
            [(d["role"], d["address"]) for d in lineage["outputs"]],
            [
                ("validation:report", "out/report"),
                ("validation:valid", "out/valid"),
                ("validation:invalid", "out/invalid"),
            ],
        )

    def test_a_list_of_inputs_is_read_whole(self) -> None:
        lineage = self._lineage(
            {
                "inputs": [
                    {"format": "parquet", "path": "a"},
                    {"format": "parquet", "path": "b"},
                ],
                "outputs": [{"format": "parquet", "path": "c"}],
            }
        )
        self.assertEqual([d["address"] for d in lineage["inputs"]], ["a", "b"])
        self.assertEqual([d["address"] for d in lineage["outputs"]], ["c"])

    def test_a_topic_hidden_in_options_is_still_an_address(self) -> None:
        lineage = self._lineage(
            {"input": {"format": "kafka", "options": {"topic": "orders"}}, "output": {}}
        )
        self.assertEqual(lineage["inputs"][0]["address"], "orders")

    def test_a_table_target_addresses_by_table(self) -> None:
        lineage = self._lineage(
            {"input": {"format": "jdbc", "options": {"dbtable": "public.orders"}}}
        )
        self.assertEqual(lineage["inputs"][0]["address"], "public.orders")

    def test_no_dataset_at_all_records_no_lineage(self) -> None:
        self.assertIsNone(history.lineage_of({"transformations": []}))
        self.assertIsNone(history.lineage_of("not a config"))
        self.assertIsNone(history.lineage_of(None))


class ConfigVersionTest(unittest.TestCase):
    """The history points at a Job; a Job is edited. These pin the one field that
    says *which version of it* an execution actually ran."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = history.SQLiteExecutionRepository(Path(self._tmp.name) / "history.sqlite3")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run(self, config, **kwargs) -> str:
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id="w1", pipeline_id=None, job_id="j1", name="orders",
        )
        self.last_run_id = run_id
        config_hash, text = history.config_version(config)
        return self.repo.create_job_run(
            run_id, job_id="j1", name="orders", stage_index=0,
            config_hash=config_hash, config=text, **kwargs
        )

    def test_formatting_does_not_change_the_fingerprint(self) -> None:
        # Two spellings of one configuration: reordered keys, and a list that
        # keeps its order because order is meaning there.
        first, _ = history.config_version(
            {"name": "orders", "transformations": [{"type": "filter"}, {"type": "cast"}]}
        )
        second, _ = history.config_version(
            {"transformations": [{"type": "filter"}, {"type": "cast"}], "name": "orders"}
        )
        self.assertEqual(first, second)
        self.assertTrue(first.startswith("sha256:"))

    def test_a_changed_step_changes_the_fingerprint(self) -> None:
        before, _ = history.config_version({"name": "orders", "input": {"path": "/a"}})
        after, _ = history.config_version({"name": "orders", "input": {"path": "/b"}})
        self.assertNotEqual(before, after)

    def test_order_of_a_list_is_part_of_the_configuration(self) -> None:
        # `filter` then `cast` is not the same pipeline as `cast` then `filter`.
        first, _ = history.config_version({"transformations": [{"type": "filter"}, {"type": "cast"}]})
        second, _ = history.config_version({"transformations": [{"type": "cast"}, {"type": "filter"}]})
        self.assertNotEqual(first, second)

    def test_the_json_that_ran_is_read_back_whole(self) -> None:
        config = {"name": "orders", "input": {"format": "csv", "path": "/data/in.csv"}}
        job_run_id = self._run(config)
        stored = self.repo.job_config(job_run_id)
        self.assertEqual(stored.config, config)
        self.assertEqual(stored.config_hash, history.config_version(config)[0])

    def test_two_runs_of_an_edited_job_are_told_apart(self) -> None:
        # The whole point: same job_id, same name, different JSON.
        first = self._run({"name": "orders", "input": {"path": "/data/2026-01"}})
        second = self._run({"name": "orders", "input": {"path": "/data/2026-02"}})
        self.assertNotEqual(
            self.repo.job_config(first).config_hash,
            self.repo.job_config(second).config_hash,
        )

    def test_the_fingerprint_travels_with_the_run(self) -> None:
        config = {"name": "orders"}
        job_run_id = self._run(config)
        run = self.repo.get_pipeline_run(self.last_run_id)
        self.assertEqual(run.jobs[0].id, job_run_id)
        self.assertEqual(run.jobs[0].config_hash, history.config_version(config)[0])

    def test_a_configuration_too_large_to_keep_is_still_identified(self) -> None:
        # Generated JSON can dwarf the history it lives in. The copy is dropped;
        # the fingerprint is not, so runs stay comparable.
        huge = {"name": "orders", "filler": "x" * (history.MAX_STORED_CONFIG_BYTES + 1)}
        config_hash, text = history.config_version(huge)
        self.assertIsNone(text)
        self.assertTrue(config_hash.startswith("sha256:"))

    def test_a_run_recorded_before_this_existed_reads_as_unknown(self) -> None:
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id="w1", pipeline_id=None, job_id="j1", name="orders",
        )
        job_run_id = self.repo.create_job_run(run_id, job_id="j1", name="orders", stage_index=0)
        stored = self.repo.job_config(job_run_id)
        self.assertIsNone(stored.config_hash)
        self.assertIsNone(stored.config)

    def test_an_unknown_execution_has_no_configuration(self) -> None:
        self.assertIsNone(self.repo.job_config("nope"))

    def test_something_that_is_not_a_configuration_is_not_fingerprinted(self) -> None:
        self.assertEqual(history.config_version(None), (None, None))
        self.assertEqual(history.config_version("orders.json"), (None, None))


class CatalogTest(unittest.TestCase):
    """The half of the database that says what exists, rather than what happened.

    The point of these rows is the foreign keys: a Job belongs to a Workflow, a
    Pipeline belongs to a Workflow, and a Pipeline runs Jobs through `pipeline_stage`.
    Without them the history is a pile of loose ids that nothing can join.
    """

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = history.SQLiteExecutionRepository(Path(self._tmp.name) / "history.sqlite3")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _catalog(self, kind: str, record_id: str, *, include_deleted: bool = False):
        for record in self.repo.list_catalog(include_deleted=include_deleted):
            if record.kind == kind and record.id == record_id:
                return record
        return None

    def test_foreign_keys_are_enforced(self) -> None:
        """WAL and `PRAGMA foreign_keys=ON` are per connection — this is the proof."""
        with closing(sqlite3.connect(self.repo._path)) as conn:  # noqa: SLF001 - the file is the API here
            conn.execute("PRAGMA foreign_keys=ON")
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO job (id, workflow_id, created_at, updated_at) "
                    "VALUES ('j-orphan', 'w-missing', '2026-01-01', '2026-01-01')"
                )

    def test_a_job_is_attached_to_its_workflow(self) -> None:
        self.repo.upsert_workflow("w1", name="Vendas")
        self.repo.upsert_job("j1", workflow_id="w1", name="Ingestão", path="vendas/jobs/i.json")

        job = self._catalog("job", "j1")
        assert job is not None
        self.assertEqual(job.workflow_id, "w1")
        self.assertEqual(job.name, "Ingestão")
        self.assertEqual(job.path, "vendas/jobs/i.json")

    def test_a_job_saved_before_its_workflow_still_lands_under_it(self) -> None:
        """Save order is the client's, not ours: the parent row is created on demand
        and filled in when the Workflow itself arrives."""
        self.repo.upsert_job("j1", workflow_id="w1", name="Ingestão")
        self.repo.upsert_workflow("w1", name="Vendas")

        workflow = self._catalog("workflow", "w1")
        assert workflow is not None
        self.assertEqual(workflow.name, "Vendas")
        assert self._catalog("job", "j1") is not None
        self.assertEqual(self._catalog("job", "j1").workflow_id, "w1")

    def test_stages_carry_the_job_to_pipeline_relation_in_order(self) -> None:
        self.repo.upsert_workflow("w1", name="Vendas")
        self.repo.upsert_job("j1", workflow_id="w1", name="Ingestão")
        self.repo.upsert_job("j2", workflow_id="w1", name="Curadoria")
        self.repo.upsert_pipeline(
            "p1", workflow_id="w1", name="Diário",
            stages=[{"id": "s1", "jobId": "j1"}, {"id": "s2", "jobId": "j2"}],
        )

        pipeline = self._catalog("pipeline", "p1")
        assert pipeline is not None
        self.assertEqual(
            pipeline.stages,
            [
                {"stage_id": "s1", "job_id": "j1", "stage_index": 0},
                {"stage_id": "s2", "job_id": "j2", "stage_index": 1},
            ],
        )

    def test_the_same_job_can_be_two_stages_of_one_pipeline(self) -> None:
        """Why the junction is keyed by stage and not by job."""
        self.repo.upsert_job("j1", workflow_id="w1")
        self.repo.upsert_pipeline(
            "p1", workflow_id="w1",
            stages=[{"id": "s1", "jobId": "j1"}, {"id": "s2", "jobId": "j1"}],
        )

        pipeline = self._catalog("pipeline", "p1")
        assert pipeline is not None
        self.assertEqual([stage["stage_id"] for stage in pipeline.stages], ["s1", "s2"])
        self.assertEqual({stage["job_id"] for stage in pipeline.stages}, {"j1"})

    def test_saving_a_pipeline_again_replaces_its_stages(self) -> None:
        self.repo.upsert_pipeline("p1", workflow_id="w1", stages=[{"id": "s1", "jobId": "j1"}])
        self.repo.upsert_pipeline("p1", workflow_id="w1", stages=[{"id": "s2", "jobId": "j2"}])

        pipeline = self._catalog("pipeline", "p1")
        assert pipeline is not None
        self.assertEqual([stage["stage_id"] for stage in pipeline.stages], ["s2"])

    def test_deleting_a_workflow_hides_what_belongs_to_it(self) -> None:
        self.repo.upsert_workflow("w1", name="Vendas")
        self.repo.upsert_job("j1", workflow_id="w1")
        self.repo.upsert_pipeline("p1", workflow_id="w1")

        self.repo.soft_delete("workflow", "w1")

        self.assertEqual(self.repo.list_catalog(), [])
        self.assertEqual(len(self.repo.list_catalog(include_deleted=True)), 3)

    def test_a_deleted_job_keeps_its_executions_readable(self) -> None:
        """Soft, not hard: history that loses its subject is history nobody can audit."""
        self.repo.upsert_workflow("w1", name="Vendas")
        self.repo.upsert_job("j1", workflow_id="w1", name="Ingestão")
        run_id = self.repo.create_pipeline_run(
            kind="job", workflow_id="w1", pipeline_id=None, job_id="j1", name="Ingestão",
        )
        self.repo.finish_pipeline_run(run_id, status=history.SUCCESS, duration_ms=1, error=None)

        self.repo.soft_delete("job", "j1")

        run = self.repo.get_pipeline_run(run_id)
        assert run is not None
        self.assertEqual(run.job_id, "j1")
        self.assertIsNone(self._catalog("job", "j1"))
        deleted = self._catalog("job", "j1", include_deleted=True)
        assert deleted is not None
        self.assertIsNotNone(deleted.deleted_at)

    def test_an_unknown_kind_cannot_be_deleted(self) -> None:
        with self.assertRaises(ValueError):
            self.repo.soft_delete("dataset", "d1")

if __name__ == "__main__":
    unittest.main()
