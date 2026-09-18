"""Tests for the owner of a standalone file in the catalog.

Stdlib plus FastAPI — no Spark, no HTTP client:

    python sparquet-studio/server/test_file_job_catalog.py

A Pipeline stage may run a `.json` by path — written by another team, generated
by a script, living in a repository the Studio never wrote to. Those runs used to
land in the history with no `job_id` at all. Enough for a timeline, and nothing
else: the catalog had no record to hold them, so nobody could tag the file, and
because health is measured per Job, `GET /health/jobs` never listed it and no
alert rule could ever fire on it. A file that runs every night could fail every
night in silence.

So the file becomes its own catalog record, identified by its path: two runs of
the same file are two runs of the same object. The tests below pin that identity
(it has to survive spelling), pin what the record is called, and — the part that
is easy to get wrong — pin that this changed nothing about who is allowed to run
what, and that a record somebody curated afterwards is never overwritten by the
next run.
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

_TMP = tempfile.TemporaryDirectory()
# Every store is built at import time, so the environment has to be right before
# `main` is imported — and the developer's own runner is not a test fixture.
os.environ["SPARQUET_STUDIO_AUDIT_DB"] = os.path.join(_TMP.name, "audit.sqlite3")
os.environ["SPARQUET_STUDIO_AUTH_DB"] = os.path.join(_TMP.name, "auth.sqlite3")
os.environ["SPARQUET_STUDIO_CREDITS_DB"] = os.path.join(_TMP.name, "credits.sqlite3")
os.environ["SPARQUET_STUDIO_HISTORY_DB"] = os.path.join(_TMP.name, "history.sqlite3")
os.environ["SPARQUET_HOME"] = os.path.join(_TMP.name, "home")
os.environ["SPARQUET_STUDIO_WORKSPACE"] = os.path.join(_TMP.name, "library")
os.environ.setdefault("SPARQUET_STUDIO_TOKEN", "test-token")

import history  # noqa: E402
import main  # noqa: E402

PIPELINE = {
    "name": "ingestao-vendas",
    "input": {"format": "csv", "path": "vendas.csv"},
    "outputs": [{"format": "parquet", "path": "out"}],
}


def _write(root: Path, relative: str, payload: object = PIPELINE) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    text = payload if isinstance(payload, str) else json.dumps(payload, indent=2)
    path.write_text(text, encoding="utf-8")
    return path


class FileIdentityTest(unittest.TestCase):
    """`_file_job_id` — the same file is the same record, however it is spelled."""

    def test_the_path_is_the_identity(self) -> None:
        self.assertEqual(
            main._file_job_id("vendas/jobs/ingestao.json"),
            "file:vendas/jobs/ingestao.json",
        )

    def test_two_runs_of_the_same_file_are_the_same_object(self) -> None:
        """The point of deriving the id instead of minting one per run."""
        self.assertEqual(
            main._file_job_id("vendas/ingestao.json"),
            main._file_job_id("vendas/ingestao.json"),
        )

    def test_backslashes_and_forward_slashes_name_one_record(self) -> None:
        """A Windows client and a Linux one are looking at the same file."""
        self.assertEqual(
            main._file_job_id("vendas\\jobs\\ingestao.json"),
            main._file_job_id("vendas/jobs/ingestao.json"),
        )

    def test_a_leading_slash_does_not_make_a_second_record(self) -> None:
        self.assertEqual(
            main._file_job_id("/vendas/ingestao.json"),
            main._file_job_id("vendas/ingestao.json"),
        )

    def test_surrounding_whitespace_does_not_make_a_second_record(self) -> None:
        self.assertEqual(
            main._file_job_id("  vendas/ingestao.json  "),
            main._file_job_id("vendas/ingestao.json"),
        )

    def test_the_prefix_keeps_it_apart_from_a_studio_job_id(self) -> None:
        """A Studio id is a uuid; nothing the Studio writes starts with `file:`."""
        self.assertTrue(main._file_job_id("x.json").startswith(main.FILE_JOB_PREFIX))


class FileJobNameTest(unittest.TestCase):
    """What the catalog calls a file: what the file calls itself."""

    def test_it_uses_the_name_the_config_declares(self) -> None:
        self.assertEqual(main._file_job_name("vendas/a.json", PIPELINE), "ingestao-vendas")

    def test_it_falls_back_to_the_file_name(self) -> None:
        self.assertEqual(main._file_job_name("vendas/jobs/a.json", {}), "a.json")

    def test_a_blank_declared_name_is_not_a_name(self) -> None:
        self.assertEqual(main._file_job_name("vendas/a.json", {"name": "   "}), "a.json")

    def test_a_declared_name_that_is_not_a_string_is_ignored(self) -> None:
        self.assertEqual(main._file_job_name("a.json", {"name": 7}), "a.json")


class StageOwnerTest(unittest.TestCase):
    """`_resolve_staged_files` fills the identity; `_stage_job_id` chooses it."""

    def setUp(self) -> None:
        self.root = Path(main._WORKSPACE_ROOT)
        for stale in self.root.rglob("*.json"):
            if ".studio" not in stale.parts:
                stale.unlink()

    def _stage(self, **kwargs: object) -> main.FlowStageRequest:
        return main.FlowStageRequest(id=kwargs.pop("id", "s1"), **kwargs)  # type: ignore[arg-type]

    def test_a_file_stage_nobody_owns_becomes_its_own_record(self) -> None:
        _write(self.root, "vendas/jobs/ingestao.json")
        stage = self._stage(path="vendas/jobs/ingestao.json")

        main._resolve_staged_files([stage])

        self.assertEqual(stage.file_job_id, "file:vendas/jobs/ingestao.json")
        self.assertEqual(main._stage_job_id(stage), "file:vendas/jobs/ingestao.json")

    def test_a_file_a_job_owns_stays_that_jobs_run(self) -> None:
        """The Job is the record; the file is only where it is kept."""
        _write(self.root, "vendas/jobs/ingestao.json")
        stage = self._stage(path="vendas/jobs/ingestao.json", job_id="j-1")

        main._resolve_staged_files([stage])

        self.assertIsNone(stage.file_job_id)
        self.assertEqual(main._stage_job_id(stage), "j-1")

    def test_an_inline_stage_gets_no_file_record(self) -> None:
        """There is no file to be the owner of."""
        stage = self._stage(pipeline=dict(PIPELINE))

        main._resolve_staged_files([stage])

        self.assertIsNone(stage.file_job_id)
        self.assertIsNone(main._stage_job_id(stage))

    def test_resolving_writes_nothing_to_the_catalog(self) -> None:
        """Reading the file happens before the flow is authorized. A run that is
        about to be refused must not leave a record saying it happened."""
        _write(self.root, "vendas/jobs/nao-autorizado.json")
        stage = self._stage(path="vendas/jobs/nao-autorizado.json")

        main._resolve_staged_files([stage])

        self.assertIsNone(_catalog_job(stage.file_job_id))


def _catalog_job(job_id: str) -> dict | None:
    for record in main._history.list_catalog():
        if record.kind == "job" and record.id == job_id:
            return {"name": record.name, "path": record.path, "tags": list(record.tags)}
    return None


class RegisterFileJobsTest(unittest.TestCase):
    """`_register_file_jobs` — the record itself, written once the run is allowed."""

    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = history.SQLiteExecutionRepository(Path(self._dir.name) / "history.sqlite3")
        self._real = main._history
        main._history = self.store
        self.addCleanup(setattr, main, "_history", self._real)

    def _stage(self, path: str, **kwargs: object) -> main.FlowStageRequest:
        stage = main.FlowStageRequest(id="s1", path=path, pipeline=dict(PIPELINE), **kwargs)  # type: ignore[arg-type]
        stage.file_job_id = main._file_job_id(path)
        return stage

    def test_it_creates_the_record_with_the_name_the_file_declares(self) -> None:
        stage = self._stage("vendas/jobs/ingestao.json")

        main._register_file_jobs([stage], "w-1")

        record = self._job("file:vendas/jobs/ingestao.json")
        self.assertEqual(record.name, "ingestao-vendas")
        self.assertEqual(record.path, "vendas/jobs/ingestao.json")

    def test_the_record_hangs_under_the_workflow_that_ran_it(self) -> None:
        stage = self._stage("vendas/jobs/ingestao.json")

        main._register_file_jobs([stage], "w-1")

        self.assertEqual(self._job("file:vendas/jobs/ingestao.json").workflow_id, "w-1")

    def test_the_stored_path_is_spelled_the_way_the_id_is(self) -> None:
        """Otherwise the row and its own identity would disagree."""
        stage = self._stage("vendas\\jobs\\ingestao.json")

        main._register_file_jobs([stage], None)

        self.assertEqual(
            self._job("file:vendas/jobs/ingestao.json").path, "vendas/jobs/ingestao.json"
        )

    def test_a_second_run_does_not_undo_what_somebody_curated(self) -> None:
        """The rule the rest of the catalog already follows: a run knows an id,
        a person knows what the record is called."""
        stage = self._stage("vendas/jobs/ingestao.json")
        main._register_file_jobs([stage], "w-1")
        self.store.upsert_job(
            "file:vendas/jobs/ingestao.json", workflow_id="w-1", name="Ingestão de vendas",
            description="Roda toda madrugada.", path="vendas/jobs/ingestao.json",
            tags=["centro-de-custo:vendas"],
        )

        main._register_file_jobs([stage], "w-1")

        record = self._job("file:vendas/jobs/ingestao.json")
        self.assertEqual(record.name, "Ingestão de vendas")
        self.assertEqual(record.description, "Roda toda madrugada.")
        self.assertEqual(record.tags, ["centro-de-custo:vendas"])

    def test_a_stage_a_job_owns_writes_no_file_record(self) -> None:
        stage = main.FlowStageRequest(
            id="s1", path="vendas/jobs/ingestao.json", pipeline=dict(PIPELINE), job_id="j-1"
        )

        main._register_file_jobs([stage], "w-1")

        self.assertEqual(
            [record.id for record in self.store.list_catalog() if record.kind == "job"], []
        )

    def test_a_catalog_that_refuses_the_write_does_not_fail_the_run(self) -> None:
        """Bookkeeping is never worth losing an execution over."""

        class Broken:
            def ensure_run_targets(self, **_: object) -> None:
                raise RuntimeError("disk is gone")

        main._history = Broken()  # type: ignore[assignment]
        main._register_file_jobs([self._stage("vendas/jobs/ingestao.json")], "w-1")

    def _job(self, job_id: str) -> history.CatalogRecord:
        for record in self.store.list_catalog():
            if record.kind == "job" and record.id == job_id:
                return record
        raise AssertionError(f"{job_id} is not in the catalog")


class FileRunsAreVisibleTest(unittest.TestCase):
    """The reason the record exists: those runs can now be found and watched."""

    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = history.SQLiteExecutionRepository(Path(self._dir.name) / "history.sqlite3")
        self.job_id = main._file_job_id("vendas/jobs/ingestao.json")
        self.store.ensure_run_targets(
            workflow_id="w-1", pipeline_id=None, job_id=self.job_id,
            name="ingestao-vendas", path="vendas/jobs/ingestao.json",
        )

    def _run(self, status: str) -> None:
        run_id = self.store.create_pipeline_run(
            kind="pipeline", workflow_id="w-1", pipeline_id="p-1", job_id=None, name="Vendas",
        )
        job_run_id = self.store.create_job_run(
            run_id, job_id=self.job_id, name="ingestao-vendas", stage_index=0
        )
        self.store.finish_job_run(
            job_run_id, status=status, duration_ms=10, error=None,
            rows_read=1, rows_written=1,
        )
        self.store.finish_pipeline_run(run_id, status=status, duration_ms=10, error=None)

    def test_the_file_shows_up_in_per_job_health(self) -> None:
        """The whole point of item 9.6: before this, a nightly file could fail
        every night and no health row or alert rule would ever mention it."""
        self._run(history.FAILED)

        rows = {row.job_id: row for row in self.store.job_health()}

        self.assertIn(self.job_id, rows)
        self.assertEqual(rows[self.job_id].name, "ingestao-vendas")

    def test_two_runs_of_the_file_are_two_runs_of_one_job(self) -> None:
        self._run(history.SUCCESS)
        self._run(history.FAILED)

        [row] = [r for r in self.store.job_health() if r.job_id == self.job_id]

        self.assertEqual(row.runs, 2)

    def test_the_file_can_be_tagged_like_any_other_job(self) -> None:
        """A tag on the record is what puts these runs on the right invoice."""
        self.store.upsert_job(
            self.job_id, workflow_id="w-1", tags=["centro-de-custo:vendas"]
        )

        self.assertEqual(
            self.store.effective_tags(workflow_id="w-1", job_id=self.job_id),
            ["centro-de-custo:vendas"],
        )


if __name__ == "__main__":
    unittest.main()
