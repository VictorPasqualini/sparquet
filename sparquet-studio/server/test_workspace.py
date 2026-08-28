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


if __name__ == "__main__":
    unittest.main(verbosity=2)
