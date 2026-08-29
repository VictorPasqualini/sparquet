"""Tests for the file workspace (`workspace.py`).

Stdlib only — no pytest, no FastAPI — so it runs the same way the rest of the repo's
tests do:

    python sparquet-studio/server/test_workspace.py

What is being protected here is the promise the workspace makes: the library is real
files a person can read, diff and commit. That means the readable file has to be the
compiled Sparquet JSON (not the Studio record), a rename has to move the file instead
of leaving a second copy behind, and a reload has to return exactly what was written.
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

import workspace


def _workflow(doc_id: str = "w1", name: str = "Vendas") -> workspace.Document:
    return workspace.Document(
        kind="workflow",
        id=doc_id,
        record={"id": doc_id, "name": name, "description": "", "accent": "brand"},
    )


def _job(doc_id: str = "j1", name: str = "Ingestão", workflow_id: str = "w1") -> workspace.Document:
    return workspace.Document(
        kind="job",
        id=doc_id,
        record={"id": doc_id, "workflowId": workflow_id, "name": name, "graph": {"nodes": []}},
        config={"name": name, "source": {"format": "csv", "path": "in.csv"}},
    )


def _pipeline(doc_id: str = "p1", name: str = "Diário") -> workspace.Document:
    return workspace.Document(
        kind="pipeline",
        id=doc_id,
        record={
            "id": doc_id,
            "workflowId": "w1",
            "name": name,
            "stages": [{"id": "s1", "jobId": "j1", "position": {"x": 0, "y": 0}}],
        },
    )


class FileWorkspaceStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name) / "sparquet-workspace"
        self.store = workspace.FileWorkspaceStore(self.root)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _read(self, relative: str) -> object:
        return json.loads((self.root / relative).read_text(encoding="utf-8"))

    # ---- the files on disk ----------------------------------------------

    def test_writes_a_readable_tree_named_after_the_workflow(self) -> None:
        self.store.write(_workflow())
        job = self.store.write(_job())
        pipeline = self.store.write(_pipeline())

        self.assertEqual(job.path, "vendas/jobs/ingestao.json")
        self.assertEqual(pipeline.path, "vendas/pipelines/diario.json")
        self.assertTrue((self.root / "vendas" / "workflow.json").exists())

    def test_a_job_file_holds_the_compiled_pipeline_not_the_studio_record(self) -> None:
        """The point of the file: the framework can run it unchanged."""
        self.store.write(_workflow())
        job = self.store.write(_job())

        assert job.path is not None
        content = self._read(job.path)
        self.assertEqual(content, {"name": "Ingestão", "source": {"format": "csv",
                                                                  "path": "in.csv"}})
        self.assertNotIn("graph", content)

    def test_the_record_survives_a_round_trip(self) -> None:
        self.store.write(_workflow())
        self.store.write(_job())

        again = self.store.read("job", "j1")
        assert again is not None
        self.assertEqual(again.record["name"], "Ingestão")
        self.assertEqual(again.record["workflowId"], "w1")

    def test_renaming_a_job_moves_its_file_instead_of_leaving_a_copy(self) -> None:
        """A stale `ingestao.json` next to the new name is what gets run by mistake."""
        self.store.write(_workflow())
        first = self.store.write(_job())
        second = self.store.write(_job(name="Ingestão diária"))

        assert first.path is not None and second.path is not None
        self.assertNotEqual(first.path, second.path)
        self.assertFalse((self.root / first.path).exists())
        self.assertTrue((self.root / second.path).exists())

    def test_renaming_a_workflow_moves_everything_under_it(self) -> None:
        self.store.write(_workflow())
        self.store.write(_job())
        self.store.write(_pipeline())

        self.store.write(_workflow(name="Vendas BR"))

        snapshot = self.store.snapshot()
        paths = [doc.path for doc in snapshot.jobs + snapshot.pipelines]
        self.assertEqual(
            sorted(paths),
            ["vendas-br/jobs/ingestao.json", "vendas-br/pipelines/diario.json"],
        )
        self.assertFalse((self.root / "vendas").exists())

    def test_snapshot_returns_every_kind_and_the_root(self) -> None:
        self.store.write(_workflow())
        self.store.write(_job())
        self.store.write(_pipeline())

        snapshot = self.store.snapshot()
        self.assertEqual(Path(snapshot.root), self.root.resolve())
        self.assertEqual([doc.id for doc in snapshot.workflows], ["w1"])
        self.assertEqual([doc.id for doc in snapshot.jobs], ["j1"])
        self.assertEqual([doc.id for doc in snapshot.pipelines], ["p1"])

    def test_delete_removes_both_files_and_reports_whether_it_existed(self) -> None:
        self.store.write(_workflow())
        job = self.store.write(_job())
        assert job.path is not None

        self.assertTrue(self.store.delete("job", "j1"))
        self.assertFalse((self.root / job.path).exists())
        self.assertIsNone(self.store.read("job", "j1"))
        self.assertFalse(self.store.delete("job", "j1"))

    # ---- meta ------------------------------------------------------------

    def test_meta_travels_with_the_library(self) -> None:
        """Why it is here and not in the browser: a second checkout must not re-seed."""
        self.store.write_meta("seeded", True)
        self.store.write_meta("version", 4)

        self.assertEqual(self.store.read_meta(), {"seeded": True, "version": 4})
        self.assertEqual(self.store.snapshot().meta, {"seeded": True, "version": 4})

        reopened = workspace.FileWorkspaceStore(self.root)
        self.assertEqual(reopened.read_meta(), {"seeded": True, "version": 4})

        self.store.delete_meta("seeded")
        self.assertEqual(self.store.read_meta(), {"version": 4})
        self.store.delete_meta("seeded")  # deleting what is gone is not an error

    # ---- refusals --------------------------------------------------------

    def test_an_unknown_kind_is_refused(self) -> None:
        with self.assertRaises(workspace.WorkspaceError):
            self.store.write(workspace.Document(kind="dataset", id="d1", record={}))

    def test_an_id_that_could_escape_the_root_is_refused(self) -> None:
        """The id reaches the filesystem, so it is checked before it becomes a path."""
        for bad in ("../escape", "a/b", "", "  "):
            with self.assertRaises(workspace.WorkspaceError, msg=bad):
                self.store.write(workspace.Document(kind="workflow", id=bad, record={}))


class WorkspaceLocationTest(unittest.TestCase):
    """Where the library lives.

    The rule that matters is the last one: the runner must not keep a user's Jobs
    inside its own checkout. A checkout is pulled, reset and deleted, and a
    library in one is lost to a `git clean` — or committed by accident long
    before that.
    """

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.home = Path(self._tmp.name) / "home"
        self._env = _Environment(
            SPARQUET_HOME=str(self.home), SPARQUET_STUDIO_WORKSPACE=None
        )
        self._env.apply()

    def tearDown(self) -> None:
        self._env.restore()
        self._tmp.cleanup()

    def test_the_default_is_outside_any_source_tree(self) -> None:
        self.assertEqual(workspace.default_root(), self.home / "workspace")
        self.assertEqual(workspace.resolve_root().source, "default")
        self.assertEqual(workspace.resolve_root().root, self.home / "workspace")

    def test_nothing_is_created_just_by_asking_where_it_would_go(self) -> None:
        """Resolving is a question. Only a store actually makes directories."""
        workspace.resolve_root()
        self.assertFalse(self.home.exists())

    def test_the_environment_wins_over_everything(self) -> None:
        workspace.remember_root(Path(self._tmp.name) / "chosen")
        with _Environment(SPARQUET_STUDIO_WORKSPACE=str(Path(self._tmp.name) / "forced")):
            location = workspace.resolve_root()
        self.assertEqual(location.source, "env")
        self.assertEqual(location.root, Path(self._tmp.name) / "forced")

    def test_a_choice_survives_a_restart(self) -> None:
        chosen = Path(self._tmp.name) / "chosen"
        self.assertEqual(workspace.remember_root(chosen), chosen.resolve())
        location = workspace.resolve_root()
        self.assertEqual(location.source, "settings")
        self.assertEqual(location.root, chosen.resolve())

    def test_the_setting_lives_outside_the_workspace_it_points_at(self) -> None:
        """A setting that says where the workspace is cannot live in the workspace."""
        chosen = Path(self._tmp.name) / "chosen"
        workspace.remember_root(chosen)
        self.assertEqual(workspace.settings_path(), self.home / "studio.json")
        self.assertFalse((chosen / "studio.json").exists())

    def test_clearing_the_choice_goes_back_to_the_default(self) -> None:
        workspace.remember_root(Path(self._tmp.name) / "chosen")
        workspace.write_setting("workspace", None)
        self.assertEqual(workspace.resolve_root().source, "default")

    def test_a_settings_file_that_will_not_parse_is_treated_as_absent(self) -> None:
        """It holds a preference, not data. Refusing to start over one would trade
        a small problem for a total one."""
        workspace.settings_path().parent.mkdir(parents=True, exist_ok=True)
        workspace.settings_path().write_text("{ not json", encoding="utf-8")
        self.assertEqual(workspace.resolve_root().source, "default")

    def test_a_library_left_in_the_old_place_is_adopted_not_abandoned(self) -> None:
        """The default moved. That is no reason for somebody's Jobs to disappear."""
        legacy = Path(self._tmp.name) / "repo" / "sparquet-workspace"
        workspace.FileWorkspaceStore(legacy).write(_workflow())
        location = workspace.resolve_root(legacy)
        self.assertEqual(location.source, "legacy")
        self.assertEqual(location.root, legacy)

    def test_an_empty_old_directory_is_not_a_library(self) -> None:
        """A bare directory left behind must not pin a fresh install to the repo."""
        legacy = Path(self._tmp.name) / "repo" / "sparquet-workspace"
        legacy.mkdir(parents=True)
        self.assertEqual(workspace.resolve_root(legacy).source, "default")

    def test_a_choice_wins_over_the_old_place(self) -> None:
        legacy = Path(self._tmp.name) / "repo" / "sparquet-workspace"
        workspace.FileWorkspaceStore(legacy).write(_workflow())
        chosen = Path(self._tmp.name) / "chosen"
        workspace.remember_root(chosen)
        self.assertEqual(workspace.resolve_root(legacy).root, chosen.resolve())

    def test_other_settings_are_left_alone(self) -> None:
        workspace.write_setting("theme", "dark")
        workspace.remember_root(Path(self._tmp.name) / "chosen")
        self.assertEqual(workspace.read_setting("theme"), "dark")


class _Environment:
    """Sets environment variables for a block, and puts them back afterwards.

    `None` means "unset", which is the case that matters: a developer with
    SPARQUET_STUDIO_WORKSPACE exported must not get different results from these
    tests than CI does.
    """

    def __init__(self, **values: object) -> None:
        self._values = values
        self._before: dict = {}

    def apply(self) -> None:
        for key, value in self._values.items():
            self._before[key] = os.environ.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)

    def restore(self) -> None:
        for key, value in self._before.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self._before = {}

    def __enter__(self) -> "_Environment":
        self.apply()
        return self

    def __exit__(self, *_: object) -> None:
        self.restore()


if __name__ == "__main__":
    unittest.main(verbosity=2)
