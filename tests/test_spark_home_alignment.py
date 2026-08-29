"""SPARK_HOME alignment on a local master — pure environment work, no SparkSession.

A `SPARK_HOME` left in the environment by an older install beats pyspark's own path
discovery: the driver imports the package from the current venv while the JVM and the
`pyspark.zip` that reaches the worker's PYTHONPATH come from `SPARK_HOME`. Mismatched
versions and the worker dies with `Python worker exited unexpectedly (crashed)` and no
traceback at all — and only on the first step that spawns a worker, typically the
`validations.report`, so everything before it looks healthy.

What matters here is the REFUSALS. Rewriting `SPARK_HOME` on a cluster, or when the
divergence cannot be proven, would break jobs that are configured correctly, so each
guard gets its own case.

    PYTHONPATH=. python tests/test_spark_home_alignment.py
"""
from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pyspark

from sparquet.core.context import _align_local_spark_home, _declared_spark_version
from sparquet.utils.logger import _deferred_warnings


class SparkHomeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.previous = os.environ.get("SPARK_HOME")
        self.deferred_before = list(_deferred_warnings)
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.restore)

    def restore(self) -> None:
        if self.previous is None:
            os.environ.pop("SPARK_HOME", None)
        else:
            os.environ["SPARK_HOME"] = self.previous
        _deferred_warnings[:] = self.deferred_before

    def home(self, version: str | None, layout: str = "version.py") -> str:
        """A directory that looks like a `SPARK_HOME` declaring `version`."""
        root = Path(self.tmp.name) / (version or "unversioned")
        if version is not None:
            path = root / layout
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f'__version__ = "{version}"\n', encoding="utf-8")
        else:
            root.mkdir(parents=True, exist_ok=True)
        return str(root)

    @property
    def ours(self) -> str:
        return str(Path(pyspark.__file__).resolve().parent)


class TestDeclaredSparkVersion(SparkHomeTestCase):
    def test_reads_the_pip_layout(self):
        self.assertEqual(_declared_spark_version(Path(self.home("4.1.1"))), "4.1.1")

    def test_reads_the_distribution_layout(self):
        home = self.home("3.5.1", layout="python/pyspark/version.py")
        self.assertEqual(_declared_spark_version(Path(home)), "3.5.1")

    def test_a_home_without_a_version_file_says_nothing(self):
        self.assertIsNone(_declared_spark_version(Path(self.home(None))))

    def test_a_missing_directory_says_nothing(self):
        self.assertIsNone(_declared_spark_version(Path(self.tmp.name) / "absent"))


class TestAlignLocalSparkHome(SparkHomeTestCase):
    def test_realigns_a_home_declaring_another_version(self):
        stale = self.home("0.0.1-stale")
        os.environ["SPARK_HOME"] = stale

        _align_local_spark_home("local", "local[*]")

        self.assertEqual(os.environ["SPARK_HOME"], self.ours)
        message, context = _deferred_warnings[-1]
        self.assertIn("SPARK_HOME", message)
        self.assertEqual(context["spark_home_descartado"], stale)
        self.assertEqual(context["versao_descartada"], "0.0.1-stale")
        self.assertEqual(context["versao"], pyspark.__version__)

    def test_keeps_a_home_that_agrees_with_the_imported_pyspark(self):
        agreeing = self.home(pyspark.__version__)
        os.environ["SPARK_HOME"] = agreeing

        _align_local_spark_home("local", "local[*]")

        self.assertEqual(os.environ["SPARK_HOME"], agreeing)
        self.assertEqual(_deferred_warnings, self.deferred_before)

    def test_keeps_a_home_that_declares_no_version(self):
        # Hand-assembled distribution: no way to prove a divergence, so no rewrite.
        opaque = self.home(None)
        os.environ["SPARK_HOME"] = opaque

        _align_local_spark_home("local", "local[*]")

        self.assertEqual(os.environ["SPARK_HOME"], opaque)

    def test_keeps_the_home_pyspark_itself_lives_in(self):
        os.environ["SPARK_HOME"] = self.ours

        _align_local_spark_home("local", "local[*]")

        self.assertEqual(os.environ["SPARK_HOME"], self.ours)
        self.assertEqual(_deferred_warnings, self.deferred_before)

    def test_leaves_an_unset_home_unset(self):
        os.environ.pop("SPARK_HOME", None)

        _align_local_spark_home("local", "local[*]")

        self.assertNotIn("SPARK_HOME", os.environ)

    def test_never_touches_a_cluster(self):
        # On yarn/k8s/EMR the variable belongs to the submitting environment, and a
        # driver-side path would not exist on the executor host.
        stale = self.home("0.0.1-stale")
        for env, master in (("emr", "yarn"), ("dataproc", "yarn"), ("local", "spark://host:7077")):
            with self.subTest(env=env, master=master):
                os.environ["SPARK_HOME"] = stale
                _align_local_spark_home(env, master)
                self.assertEqual(os.environ["SPARK_HOME"], stale)


if __name__ == "__main__":
    unittest.main(verbosity=2)
