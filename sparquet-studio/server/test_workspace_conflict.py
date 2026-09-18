"""Tests for two Studios writing the same library directory.

Stdlib only — no FastAPI, no Spark:

    python sparquet-studio/server/test_workspace_conflict.py

A library is a folder, so there is nothing stopping a second Studio from being
open over it: a network share, a synced folder, two checkouts of the same
repository, two tabs on one machine. Before the revision check, the second save
simply landed on top of the first and nobody was told. These tests pin the check
that refuses it, and — just as important — the cases where it must stay quiet,
because a store that cries conflict on an unchanged file is a store people learn
to override.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import workspace  # noqa: E402


def record(name: str = "Pedidos", **extra: object) -> dict:
    return {"id": "j1", "name": name, "workflowId": "w1", **extra}


CONFIG = {"name": "pedidos", "input": {"format": "parquet", "path": "/in"}}


class RevisionTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.store = workspace.FileWorkspaceStore(Path(self._tmp.name))

    def save(self, doc_record: dict, revision=None) -> workspace.Document:
        return self.store.write(
            workspace.Document(kind="job", id="j1", record=doc_record, config=CONFIG),
            expected_revision=revision,
        )

    # -------------------------------------------------------- the revision

    def test_a_record_that_is_not_there_has_an_empty_revision(self) -> None:
        self.assertIsNone(self.store.read("job", "j1"))

    def test_a_saved_record_reads_back_with_a_revision(self) -> None:
        self.save(record())
        doc = self.store.read("job", "j1")
        self.assertTrue(doc.revision)

    def test_the_snapshot_carries_the_same_revision_as_a_single_read(self) -> None:
        self.save(record())
        [job] = self.store.snapshot().jobs
        self.assertEqual(job.revision, self.store.read("job", "j1").revision)

    def test_the_revision_changes_when_the_record_does(self) -> None:
        first = self.save(record()).revision
        self.assertNotEqual(self.save(record("Pedidos v2")).revision, first)

    def test_reading_twice_without_writing_gives_the_same_revision(self) -> None:
        self.save(record())
        self.assertEqual(
            self.store.read("job", "j1").revision, self.store.read("job", "j1").revision
        )

    # ---------------------------------------------------------- the refusal

    def test_a_stale_revision_is_refused(self) -> None:
        stale = self.save(record()).revision
        self.save(record("Escrito pela outra máquina"))  # the other Studio
        with self.assertRaises(workspace.WorkspaceConflict) as caught:
            self.save(record("O que eu estava escrevendo"), revision=stale)
        self.assertIn("changed on disk", str(caught.exception))

    def test_the_refusal_carries_the_record_that_is_actually_there(self) -> None:
        stale = self.save(record()).revision
        self.save(record("Escrito pela outra máquina"))
        with self.assertRaises(workspace.WorkspaceConflict) as caught:
            self.save(record("O meu"), revision=stale)
        self.assertEqual(caught.exception.record["name"], "Escrito pela outra máquina")
        self.assertEqual(
            caught.exception.revision, self.store.read("job", "j1").revision
        )

    def test_a_refused_write_leaves_the_file_untouched(self) -> None:
        stale = self.save(record()).revision
        self.save(record("Da outra máquina"))
        with self.assertRaises(workspace.WorkspaceConflict):
            self.save(record("O meu"), revision=stale)
        self.assertEqual(self.store.read("job", "j1").record["name"], "Da outra máquina")

    def test_the_current_revision_is_accepted(self) -> None:
        current = self.save(record()).revision
        self.save(record("Editado por mim"), revision=current)
        self.assertEqual(self.store.read("job", "j1").record["name"], "Editado por mim")

    def test_saving_again_with_the_revision_the_write_returned_works(self) -> None:
        """The returned revision is the one to use next — otherwise every second
        save in a row would conflict with the first."""
        doc = self.save(record())
        doc = self.save(record("Dois"), revision=doc.revision)
        self.save(record("Três"), revision=doc.revision)
        self.assertEqual(self.store.read("job", "j1").record["name"], "Três")

    def test_a_new_record_is_claimed_with_an_empty_revision(self) -> None:
        self.save(record(), revision="")
        self.assertIsNotNone(self.store.read("job", "j1"))

    def test_believing_a_record_is_new_when_it_is_not_is_a_conflict(self) -> None:
        self.save(record("Já existia"))
        with self.assertRaises(workspace.WorkspaceConflict):
            self.save(record("Criado do zero"), revision="")

    def test_a_record_deleted_underneath_is_a_conflict_that_says_so(self) -> None:
        stale = self.save(record()).revision
        self.store.delete("job", "j1")
        with self.assertRaises(workspace.WorkspaceConflict) as caught:
            self.save(record("O meu"), revision=stale)
        self.assertIn("deleted on disk", str(caught.exception))

    def test_omitting_the_revision_still_overwrites(self) -> None:
        """Every client wrote this way before the check existed, and an old Studio
        talking to a new runner must keep working."""
        self.save(record()).revision
        self.save(record("Da outra máquina"))
        self.save(record("Sem revisão"))
        self.assertEqual(self.store.read("job", "j1").record["name"], "Sem revisão")

    # ------------------------------------------- the file edited by hand

    def test_editing_the_readable_file_by_hand_changes_the_revision(self) -> None:
        """A `git pull`, a sync client or somebody with an editor open. The
        sidecar is untouched in all three, so watching only it would miss them."""
        doc = self.save(record())
        target = Path(self._tmp.name) / doc.path
        payload = json.loads(target.read_text(encoding="utf-8"))
        payload["name"] = "editado-fora-do-studio"
        target.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaises(workspace.WorkspaceConflict):
            self.save(record("O meu"), revision=doc.revision)

    def test_rewriting_the_same_bytes_is_not_a_conflict(self) -> None:
        """Sync clients rewrite files they did not change. The revision is over
        content for exactly this reason — an mtime would have flipped here."""
        doc = self.save(record())
        target = Path(self._tmp.name) / doc.path
        raw = target.read_bytes()
        os.utime(target, (0, 0))
        target.write_bytes(raw)
        self.save(record("O meu"), revision=doc.revision)


if __name__ == "__main__":
    unittest.main()
