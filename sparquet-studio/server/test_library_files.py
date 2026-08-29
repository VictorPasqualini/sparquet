"""Tests for pointing a Pipeline stage at a file (`/workspace/files`).

Stdlib plus FastAPI — no Spark, no HTTP client:

    python sparquet-studio/server/test_library_files.py

The rule being protected: a stage may run a JSON the Studio did not write. The
file is the source, so nothing is imported and nothing is cached — an edit made
outside the Studio takes effect on the next run. What that costs is a path
arriving from a client, which is why the refusals below matter as much as the
happy path: a path is checked, never trusted, and it is always relative to the
library root, because an absolute one names a directory that exists on exactly
one machine.
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
os.environ["SPARQUET_STUDIO_AUTH_DB"] = os.path.join(_TMP.name, "auth.sqlite3")
os.environ["SPARQUET_STUDIO_CREDITS_DB"] = os.path.join(_TMP.name, "credits.sqlite3")
os.environ["SPARQUET_STUDIO_HISTORY_DB"] = os.path.join(_TMP.name, "history.sqlite3")
os.environ["SPARQUET_HOME"] = os.path.join(_TMP.name, "home")
os.environ["SPARQUET_STUDIO_WORKSPACE"] = os.path.join(_TMP.name, "library")
os.environ.setdefault("SPARQUET_STUDIO_TOKEN", "test-token")

import main  # noqa: E402
import workspace  # noqa: E402
from fastapi import HTTPException  # noqa: E402

PIPELINE = {
    "name": "ingestao",
    "input": {"format": "csv", "path": "vendas.csv"},
    "outputs": [{"format": "parquet", "path": "out"}],
}


def _write(root: Path, relative: str, payload: object = PIPELINE) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    text = payload if isinstance(payload, str) else json.dumps(payload, indent=2)
    path.write_text(text, encoding="utf-8")
    return path


class LibraryFileTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.root = Path(self._dir.name) / "library"
        self.store = workspace.FileWorkspaceStore(self.root)

    def tearDown(self) -> None:
        self._dir.cleanup()

    # ---- listing ---------------------------------------------------------

    def test_it_lists_a_json_the_studio_never_wrote(self) -> None:
        """The whole point: a file another team owns is runnable as it stands."""
        _write(self.root, "vendas/jobs/ingestao.json")

        listed = self.store.list_files()

        self.assertEqual([item.path for item in listed], ["vendas/jobs/ingestao.json"])
        self.assertEqual(listed[0].name, "ingestao")
        self.assertGreater(listed[0].size, 0)
        self.assertGreater(listed[0].modified, 0)

    def test_paths_are_relative_and_use_forward_slashes(self) -> None:
        """An absolute path would stop working the moment the library moved."""
        _write(self.root, "a/b/c.json")

        path = self.store.list_files()[0].path

        self.assertEqual(path, "a/b/c.json")
        self.assertFalse(Path(path).is_absolute())

    def test_the_editors_own_state_is_not_offered_as_something_to_run(self) -> None:
        _write(self.root, ".studio/job/j1.json")
        _write(self.root, "vendas/jobs/ingestao.json")

        self.assertEqual(
            [item.path for item in self.store.list_files()], ["vendas/jobs/ingestao.json"]
        )

    def test_hidden_files_and_half_written_ones_are_skipped(self) -> None:
        """`_write_json` writes through a `.tmp-*.json` in the same directory."""
        _write(self.root, "vendas/.tmp-abc.json")
        _write(self.root, ".hidden/x.json")
        _write(self.root, "vendas/jobs/ingestao.json")

        self.assertEqual(
            [item.path for item in self.store.list_files()], ["vendas/jobs/ingestao.json"]
        )

    def test_a_file_that_is_not_json_is_not_listed(self) -> None:
        _write(self.root, "vendas/notes.txt", "not a pipeline")

        self.assertEqual(self.store.list_files(), [])

    def test_an_empty_library_lists_nothing_rather_than_failing(self) -> None:
        self.assertEqual(self.store.list_files(), [])

    # ---- reading ---------------------------------------------------------

    def test_it_reads_the_file_as_it_is_on_disk(self) -> None:
        _write(self.root, "vendas/jobs/ingestao.json")

        self.assertEqual(self.store.read_file("vendas/jobs/ingestao.json"), PIPELINE)

    def test_a_later_edit_is_what_the_next_read_returns(self) -> None:
        """Nothing is imported and nothing is cached — that is the feature."""
        _write(self.root, "j.json")
        self.store.read_file("j.json")
        _write(self.root, "j.json", {**PIPELINE, "name": "edited-outside"})

        self.assertEqual(self.store.read_file("j.json")["name"], "edited-outside")

    def test_a_windows_separator_reads_the_same_file(self) -> None:
        _write(self.root, "vendas/jobs/ingestao.json")

        self.assertEqual(self.store.read_file("vendas\\jobs\\ingestao.json"), PIPELINE)

    def test_a_leading_slash_is_still_relative_to_the_root(self) -> None:
        _write(self.root, "vendas/jobs/ingestao.json")

        self.assertEqual(self.store.read_file("/vendas/jobs/ingestao.json"), PIPELINE)

    # ---- refusals --------------------------------------------------------

    def test_a_missing_file_says_so(self) -> None:
        with self.assertRaises(workspace.WorkspaceError) as raised:
            self.store.read_file("vendas/jobs/absent.json")
        self.assertIn("absent.json", str(raised.exception))

    def test_a_path_that_climbs_out_of_the_root_is_refused(self) -> None:
        outside = Path(self._dir.name) / "secrets.json"
        outside.write_text(json.dumps({"name": "not yours"}), encoding="utf-8")

        with self.assertRaises(workspace.WorkspaceError):
            self.store.read_file("../secrets.json")

    def test_an_absolute_path_is_refused_with_the_reason(self) -> None:
        with self.assertRaises(workspace.WorkspaceError) as raised:
            self.store.read_file(str(self.root / "j.json"))
        self.assertIn("relative", str(raised.exception))

    def test_something_that_is_not_a_json_file_is_refused(self) -> None:
        _write(self.root, "vendas/notes.txt", "not a pipeline")

        with self.assertRaises(workspace.WorkspaceError):
            self.store.read_file("vendas/notes.txt")

    def test_the_editors_own_state_cannot_be_run(self) -> None:
        _write(self.root, ".studio/job/j1.json")

        with self.assertRaises(workspace.WorkspaceError):
            self.store.read_file(".studio/job/j1.json")

    def test_an_empty_path_is_refused(self) -> None:
        with self.assertRaises(workspace.WorkspaceError):
            self.store.read_file("   ")

    def test_a_file_that_is_not_valid_json_says_which_one(self) -> None:
        _write(self.root, "broken.json", "{ not json")

        with self.assertRaises(workspace.WorkspaceError) as raised:
            self.store.read_file("broken.json")
        self.assertIn("broken.json", str(raised.exception))

    def test_a_json_that_is_not_an_object_is_refused(self) -> None:
        """A list parses fine and is not a pipeline. Better said now than by Spark."""
        _write(self.root, "list.json", [1, 2, 3])

        with self.assertRaises(workspace.WorkspaceError):
            self.store.read_file("list.json")


class StageResolutionTest(unittest.TestCase):
    """`_resolve_staged_files` — what a flow does with a stage that names a file."""

    def setUp(self) -> None:
        self.root = Path(main._WORKSPACE_ROOT)
        for stale in self.root.rglob("*.json"):
            if ".studio" not in stale.parts:
                stale.unlink()

    def _stage(self, **kwargs: object) -> main.FlowStageRequest:
        return main.FlowStageRequest(id=kwargs.pop("id", "s1"), **kwargs)  # type: ignore[arg-type]

    def test_a_file_backed_stage_runs_what_the_file_holds(self) -> None:
        _write(self.root, "vendas/jobs/ingestao.json")
        stage = self._stage(path="vendas/jobs/ingestao.json")

        main._resolve_staged_files([stage])

        self.assertEqual(stage.pipeline, PIPELINE)

    def test_an_inline_stage_is_left_alone(self) -> None:
        stage = self._stage(pipeline=dict(PIPELINE))

        main._resolve_staged_files([stage])

        self.assertEqual(stage.pipeline, PIPELINE)

    def test_a_missing_file_stops_the_flow_before_it_starts(self) -> None:
        """Not halfway through, with earlier stages already written."""
        with self.assertRaises(HTTPException) as caught:
            main._resolve_staged_files([self._stage(path="vendas/jobs/absent.json")])

        self.assertEqual(caught.exception.status_code, 400)
        self.assertIn("absent.json", str(caught.exception.detail))

    def test_naming_both_a_file_and_a_pipeline_is_refused(self) -> None:
        with self.assertRaises(HTTPException) as caught:
            main._resolve_staged_files(
                [self._stage(path="a.json", pipeline=dict(PIPELINE))]
            )
        self.assertEqual(caught.exception.status_code, 422)

    def test_a_stage_with_neither_is_refused(self) -> None:
        with self.assertRaises(HTTPException) as caught:
            main._resolve_staged_files([self._stage()])
        self.assertEqual(caught.exception.status_code, 422)

    def test_the_refusal_names_which_stage(self) -> None:
        _write(self.root, "ok.json")
        stages = [self._stage(id="s1", path="ok.json"), self._stage(id="s2")]

        with self.assertRaises(HTTPException) as caught:
            main._resolve_staged_files(stages)
        self.assertIn("Stage 2", str(caught.exception.detail))

    def test_a_path_out_of_the_root_is_refused(self) -> None:
        with self.assertRaises(HTTPException) as caught:
            main._resolve_staged_files([self._stage(path="../../etc/passwd.json")])
        self.assertEqual(caught.exception.status_code, 400)


class LibraryEndpointTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(main._WORKSPACE_ROOT)
        for stale in self.root.rglob("*.json"):
            if ".studio" not in stale.parts:
                stale.unlink()

    def test_the_listing_names_the_root_it_is_relative_to(self) -> None:
        _write(self.root, "vendas/jobs/ingestao.json")

        out = main.list_library_files()

        self.assertEqual(out.root, str(self.root))
        self.assertEqual([f.path for f in out.files], ["vendas/jobs/ingestao.json"])

    def test_reading_returns_the_json_uncompiled(self) -> None:
        _write(self.root, "vendas/jobs/ingestao.json")

        out = main.read_library_file("vendas/jobs/ingestao.json")

        self.assertEqual(out.pipeline, PIPELINE)
        self.assertEqual(out.path, "vendas/jobs/ingestao.json")

    def test_a_bad_path_is_a_400_with_the_reason(self) -> None:
        with self.assertRaises(HTTPException) as caught:
            main.read_library_file("../outside.json")
        self.assertEqual(caught.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main(verbosity=2)
