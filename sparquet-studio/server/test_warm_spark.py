"""Tests for the SparkSession the runner builds before anybody asks for one.

Stdlib plus FastAPI — no Spark:

    python sparquet-studio/server/test_warm_spark.py

The warm-up is an optimisation, so every test here is about it staying one: it
must be off unless asked for (importing this module must not cost a JVM), it must
build the session the library will actually need rather than a plain one that the
first Delta query would rebuild, and it must never turn a failure of its own into
a runner that does not start.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from typing import Any, Dict, List, Optional

_TMP = tempfile.TemporaryDirectory()
# Point every store at a throwaway directory *before* importing the module: they
# are created at import time, and the developer's own runner is not a fixture.
os.environ["SPARQUET_STUDIO_AUDIT_DB"] = os.path.join(_TMP.name, "audit.sqlite3")
os.environ["SPARQUET_STUDIO_AUTH_DB"] = os.path.join(_TMP.name, "auth.sqlite3")
os.environ["SPARQUET_STUDIO_CREDITS_DB"] = os.path.join(_TMP.name, "credits.sqlite3")
os.environ["SPARQUET_STUDIO_HISTORY_DB"] = os.path.join(_TMP.name, "history.sqlite3")
os.environ["SPARQUET_STUDIO_WORKSPACE"] = os.path.join(_TMP.name, "workspace")
os.environ.setdefault("SPARQUET_STUDIO_TOKEN", "test-token")

import main  # noqa: E402
import workspace  # noqa: E402


def job(configs: Optional[Dict[str, str]], doc_id: str = "j1") -> workspace.Document:
    record: Dict[str, Any] = {"id": doc_id, "name": doc_id, "settings": {}}
    if configs is not None:
        record["settings"] = {"spark": {"configs": configs}}
    return workspace.Document(kind=workspace.JOB, id=doc_id, record=record)


class FakeSnapshot:
    def __init__(self, jobs: List[workspace.Document]) -> None:
        self.jobs = jobs


class EnabledTest(unittest.TestCase):
    def test_off_when_nothing_is_set(self) -> None:
        self.assertFalse(main._warm_enabled({}))

    def test_on_for_the_words_a_person_would_write(self) -> None:
        for value in ("1", "on", "true", "yes", "ON", " true "):
            with self.subTest(value=value):
                self.assertTrue(main._warm_enabled({"SPARQUET_STUDIO_WARM_SPARK": value}))

    def test_off_for_anything_else(self) -> None:
        for value in ("", "0", "off", "no", "later"):
            with self.subTest(value=value):
                self.assertFalse(main._warm_enabled({"SPARQUET_STUDIO_WARM_SPARK": value}))


class ConfigsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._snapshot = main._workspace.snapshot
        self.addCleanup(setattr, main._workspace, "snapshot", self._snapshot)

    def library(self, *jobs: workspace.Document) -> None:
        main._workspace.snapshot = lambda: FakeSnapshot(list(jobs))  # type: ignore[method-assign]

    def test_an_empty_library_warms_a_plain_session(self) -> None:
        self.library()
        self.assertEqual(main._warm_configs(), {})

    def test_a_job_without_a_spark_block_asks_for_nothing(self) -> None:
        self.library(job(None))
        self.assertEqual(main._warm_configs(), {})

    def test_the_configs_of_a_job_are_what_the_session_is_built_with(self) -> None:
        self.library(job({"spark.sql.shuffle.partitions": "8"}))
        self.assertEqual(main._warm_configs(), {"spark.sql.shuffle.partitions": "8"})

    def test_jar_lists_are_unioned_rather_than_overwritten(self) -> None:
        # The whole point of warming with the library's configs: a runner with a
        # Delta Job and an Iceberg Job needs both, and one session is built.
        self.library(
            job({"spark.jars.packages": "io.delta:delta-spark_2.13:4.0.0"}, "j1"),
            job({"spark.jars.packages": "org.apache.iceberg:iceberg-spark:1.5.0"}, "j2"),
        )
        packages = main._warm_configs()["spark.jars.packages"]
        self.assertIn("delta-spark", packages)
        self.assertIn("iceberg-spark", packages)

    def test_a_plain_config_is_decided_by_the_last_job_that_states_it(self) -> None:
        self.library(
            job({"spark.sql.shuffle.partitions": "8"}, "j1"),
            job({"spark.sql.shuffle.partitions": "32"}, "j2"),
        )
        self.assertEqual(main._warm_configs()["spark.sql.shuffle.partitions"], "32")

    def test_a_library_that_cannot_be_read_warms_a_plain_session(self) -> None:
        def explode() -> Any:
            raise RuntimeError("the workspace is gone")

        main._workspace.snapshot = explode  # type: ignore[method-assign]
        self.assertEqual(main._warm_configs(), {})


class WarmTest(unittest.TestCase):
    """The warm-up itself, with the session build stubbed out."""

    def setUp(self) -> None:
        self._ensure = main._ensure_framework
        self._configs = main._warm_configs
        self.addCleanup(self._restore)
        self.asked: List[Optional[Dict[str, Any]]] = []

    def _restore(self) -> None:
        main._ensure_framework = self._ensure
        main._warm_configs = self._configs

    def stub(self, configs: Dict[str, str], framework: Any) -> None:
        main._warm_configs = lambda: configs  # type: ignore[assignment]

        def fake(settings: Optional[Dict[str, Any]], **kwargs: Any) -> Any:
            self.asked.append(settings)
            return framework, False

        main._ensure_framework = fake  # type: ignore[assignment]

    def test_it_builds_the_session_with_what_the_library_declares(self) -> None:
        class Framework:
            spark = object()

        self.stub({"spark.jars.packages": "io.delta:delta-spark_2.13:4.0.0"}, Framework())
        main._warm_spark()
        self.assertEqual(
            self.asked,
            [{"configs": {"spark.jars.packages": "io.delta:delta-spark_2.13:4.0.0"}}],
        )

    def test_a_library_that_declares_nothing_builds_a_default_session(self) -> None:
        class Framework:
            spark = object()

        self.stub({}, Framework())
        main._warm_spark()
        self.assertEqual(self.asked, [None])

    def test_the_session_is_touched_so_the_cost_is_paid_on_this_thread(self) -> None:
        class Framework:
            def __init__(self) -> None:
                self.touched = 0

            @property
            def spark(self) -> Any:
                self.touched += 1
                return object()

        framework = Framework()
        self.stub({}, framework)
        main._warm_spark()
        self.assertEqual(framework.touched, 1)

    def test_a_session_that_will_not_build_does_not_stop_the_runner(self) -> None:
        main._warm_configs = dict  # type: ignore[assignment]

        def explode(settings: Optional[Dict[str, Any]], **kwargs: Any) -> Any:
            raise RuntimeError("no Java on this machine")

        main._ensure_framework = explode  # type: ignore[assignment]
        main._warm_spark()  # the assertion is that this returns at all


if __name__ == "__main__":
    unittest.main(verbosity=2)
