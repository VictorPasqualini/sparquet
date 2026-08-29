"""Tests for choosing where the library is kept (`/workspace/root`).

Stdlib plus FastAPI — no Spark, no HTTP client:

    python sparquet-studio/server/test_workspace_root.py

The rule being protected is that the runner does not keep a user's Jobs inside
its own checkout. A checkout is code: it gets pulled, reset and deleted, and a
library living in one is lost to the first `git clean` — or committed by accident
long before that. So the default is the platform's per-user data directory, the
interface can point it somewhere else, and a path inside the source tree is
refused outright rather than merely discouraged.
"""
from __future__ import annotations

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
# A fake source tree, so "inside the source tree" is a directory these tests own
# rather than the repository they happen to be running from.
os.environ["SPARQUET_FRAMEWORK_PATH"] = os.path.join(_TMP.name, "checkout")
os.environ.pop("SPARQUET_STUDIO_WORKSPACE", None)
os.environ.setdefault("SPARQUET_STUDIO_TOKEN", "test-token")

import main  # noqa: E402
import workspace  # noqa: E402
from fastapi import HTTPException  # noqa: E402

HOME = Path(os.environ["SPARQUET_HOME"])
CHECKOUT = Path(os.environ["SPARQUET_FRAMEWORK_PATH"])

# `main` resolves the old default once, at import time, from wherever it was
# imported from — which under `unittest discover` is another test module that got
# there first, and therefore the real repository. Point it at the fake checkout so
# these tests are about the rule and not about the machine running them.
main._LEGACY_WORKSPACE = CHECKOUT / "sparquet-workspace"


def _set_root(path: object) -> object:
    return main.put_workspace_root(main.WorkspaceRootRequest(root=path))


class WorkspaceRootTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.elsewhere = Path(self._dir.name) / "my-pipelines"

    def tearDown(self) -> None:
        # Every test moves the process-wide store; put it back on the default so
        # the next one starts from the documented state.
        os.environ.pop("SPARQUET_STUDIO_WORKSPACE", None)
        _set_root(None)
        self._dir.cleanup()

    # ---- reading ---------------------------------------------------------

    def test_the_default_is_the_per_user_directory(self) -> None:
        out = main.get_workspace_root()
        self.assertEqual(Path(out.default), HOME / "workspace")
        self.assertFalse(out.inside_source_tree)
        self.assertFalse(out.locked)

    def test_it_says_why_the_root_is_the_one_it_is(self) -> None:
        """Somebody who cannot find their Jobs is looking at another directory.
        The reason is what tells them which."""
        self.assertEqual(main.get_workspace_root().source, "default")
        _set_root(str(self.elsewhere))
        self.assertEqual(main.get_workspace_root().source, "settings")

    # ---- choosing --------------------------------------------------------

    def test_a_chosen_directory_becomes_the_library(self) -> None:
        out = _set_root(str(self.elsewhere))
        self.assertEqual(Path(out.root), self.elsewhere.resolve())
        self.assertEqual(main._workspace.root, self.elsewhere.resolve())

    def test_what_is_saved_next_lands_in_the_new_place(self) -> None:
        """Rebinding the store is the whole move: nothing is copied, so the proof
        is that the next write appears there."""
        _set_root(str(self.elsewhere))
        main._workspace.write(
            workspace.Document(kind="workflow", id="w1", record={"id": "w1", "name": "Vendas"})
        )
        self.assertTrue((self.elsewhere / ".studio" / "workflow" / "w1.json").exists())

    def test_the_choice_is_remembered_for_the_next_start(self) -> None:
        _set_root(str(self.elsewhere))
        self.assertEqual(workspace.resolve_root().root, self.elsewhere.resolve())
        self.assertEqual(workspace.resolve_root().source, "settings")

    def test_clearing_it_goes_back_to_the_default(self) -> None:
        _set_root(str(self.elsewhere))
        out = _set_root(None)
        self.assertEqual(out.source, "default")
        self.assertEqual(Path(out.root), (HOME / "workspace").resolve())

    def test_an_empty_string_is_the_same_as_clearing_it(self) -> None:
        _set_root(str(self.elsewhere))
        self.assertEqual(_set_root("   ").source, "default")

    # ---- what is refused -------------------------------------------------

    def test_the_source_tree_is_refused(self) -> None:
        """The one directory it must not be. Not a warning — a refusal."""
        with self.assertRaises(HTTPException) as caught:
            _set_root(str(CHECKOUT / "sparquet-workspace"))
        self.assertEqual(caught.exception.status_code, 400)
        self.assertIn("source tree", caught.exception.detail)

    def test_a_relative_path_is_refused(self) -> None:
        """It would mean whatever directory the runner happened to start in."""
        with self.assertRaises(HTTPException) as caught:
            _set_root("./pipelines")
        self.assertEqual(caught.exception.status_code, 400)

    def test_a_directory_that_cannot_be_created_is_refused(self) -> None:
        blocker = Path(self._dir.name) / "a-file"
        blocker.write_text("not a directory", encoding="utf-8")
        with self.assertRaises(HTTPException) as caught:
            _set_root(str(blocker / "under-a-file"))
        self.assertEqual(caught.exception.status_code, 400)

    def test_the_environment_variable_cannot_be_overridden_from_the_interface(self) -> None:
        """A deployment that decides centrally decides centrally."""
        os.environ["SPARQUET_STUDIO_WORKSPACE"] = str(self.elsewhere)
        try:
            with self.assertRaises(HTTPException) as caught:
                _set_root(str(self.elsewhere / "other"))
            self.assertEqual(caught.exception.status_code, 409)
            self.assertTrue(main.get_workspace_root().locked)
        finally:
            os.environ.pop("SPARQUET_STUDIO_WORKSPACE", None)

    # ---- who may do it ---------------------------------------------------

    def test_moving_the_library_is_not_an_editor_action(self) -> None:
        """`editor` holds `workspace:*`. Deciding where the runner writes on the
        host is an administrator's call, so the action sits outside that family."""
        import auth

        self.assertIn("runner:Configure", auth.ACTIONS)
        editor = auth.BUILTIN_ROLES["editor"]
        granted = [a for s in editor.statements for a in s["actions"]]
        self.assertNotIn("runner:Configure", granted)
        self.assertNotIn("*", granted)


if __name__ == "__main__":
    unittest.main(verbosity=2)
