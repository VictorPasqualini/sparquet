"""The six native formats, executed the way the Studio executes them.

The compiler round trip is pinned on the client (`lib/compiler/compiler.test.ts`)
and the formats are pinned in the framework (`tests/test_formats_roundtrip_spark.py`).
Neither says the thing a user actually cares about: that a JSON the Studio
produced **runs** through the runner, writes real files and reads them back. A
config that round-trips perfectly and does not run is still broken.

So this executes `fixtures/formats/<format>/write.json` and then `read.json` for
`parquet`, `orc`, `json`, `csv`, `txt` and `view`, through the same code the
`/run` and `/run/flow/stream` endpoints call — `_resolve_staged_files` to turn a
library path into a pipeline, `_execute_run` to run one. The two halves run in
one process on purpose: the runner keeps a single SparkSession, which is what
makes the `view` handoff between stages work at all.

Needs pyspark and a JVM; without either the whole class is **skipped**, never
failed:

    python sparquet-studio/server/test_formats_studio_spark.py

The seed is written with plain `csv` text rather than `createDataFrame`: the
second spawns a Python worker, and on a local master with a mismatched
`PYSPARK_PYTHON` the file would die for a reason that has nothing to do with
formats.
"""
from __future__ import annotations

import csv
import json
import os
import shutil
import sys
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
# Same reason as `sparquet/core/context.py`: on a local master the worker has to
# be the same Python as the driver.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

import main  # noqa: E402

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "formats"

#: The formats that run with no extra jar and no external service. Everything
#: else (delta, iceberg, jdbc, bigquery, kafka) needs one, and is covered by
#: option-assembly tests instead.
FORMATS = ("parquet", "orc", "json", "csv", "txt", "view")

#: One ordinary row, one full of the characters that break delimited text, and
#: one that is mostly null — the null is what a trip through text loses first.
ROWS = [
    {"id": "1", "nome": "alpha", "valor": "1.5"},
    {"id": "2", "nome": 'com "aspas", virgula e acento: cessao', "valor": "-0.25"},
    {"id": "3", "nome": "", "valor": ""},
]


def _pyspark_available() -> bool:
    try:
        import pyspark  # noqa: F401
    except Exception:
        return False
    return bool(shutil.which("java") or os.environ.get("JAVA_HOME"))


def _seed(directory: Path) -> Path:
    path = directory / "seed.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["id", "nome", "valor"])
        writer.writeheader()
        writer.writerows(ROWS)
    return path


def _read_csv_output(directory: Path, name: str) -> list[dict]:
    """Spark writes a directory of parts; this is what the user sees as `back`."""
    rows: list[dict] = []
    for part in sorted((directory / name).glob("*.csv")):
        with part.open(encoding="utf-8", newline="") as handle:
            rows.extend(csv.DictReader(handle))
    return rows


@unittest.skipUnless(_pyspark_available(), "pyspark and a JVM are needed to execute")
class StudioFormatExecutionTest(unittest.TestCase):
    """Write, then read back, through the runner — one format per test."""

    @classmethod
    def setUpClass(cls) -> None:
        cls._dir = tempfile.TemporaryDirectory()
        cls.work = Path(cls._dir.name)
        _seed(cls.work)
        # The fixtures are copied into the library, so the stages can name them
        # by path — the same reference a Pipeline stage backed by a file uses.
        cls.library = Path(main._WORKSPACE_ROOT)
        for format_name in FORMATS:
            target = cls.library / "formats" / format_name
            target.mkdir(parents=True, exist_ok=True)
            for half in ("write.json", "read.json"):
                shutil.copy(FIXTURES / format_name / half, target / half)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._dir.cleanup()

    # ---- helpers ---------------------------------------------------------

    def _staged(self, format_name: str, half: str) -> dict:
        """The pipeline the runner would run for that library path."""
        stage = main.FlowStageRequest(id=f"{format_name}-{half}", path=f"formats/{format_name}/{half}.json")
        main._resolve_staged_files([stage])
        return stage.pipeline

    def _run(self, pipeline: dict, limit: int = 50):
        body = main.RunRequest(pipeline=pipeline, params={"dir": self.work.as_posix()}, limit=limit)
        collector = main._LogCollector()
        return main._execute_run(body, pipeline.get("name"), 0.0, collector)

    def _round_trip(self, format_name: str):
        written = self._run(self._staged(format_name, "write"))
        self.assertTrue(written.success, msg=written.error)
        read_back = self._run(self._staged(format_name, "read"))
        self.assertTrue(read_back.success, msg=read_back.error)
        return written, read_back

    # ---- the formats -----------------------------------------------------

    def test_parquet_survives_the_studio_path(self) -> None:
        written, read_back = self._round_trip("parquet")

        self.assertEqual(written.rows_written, len(ROWS))
        self.assertEqual(read_back.rows_read, len(ROWS))
        rows = _read_csv_output(self.work, "back-parquet")
        self.assertEqual({row["id"] for row in rows}, {"1", "2", "3"})

    def test_orc_survives_the_studio_path(self) -> None:
        written, read_back = self._round_trip("orc")

        self.assertEqual(written.rows_written, len(ROWS))
        self.assertEqual(read_back.rows_read, len(ROWS))

    def test_json_keeps_the_values_it_wrote(self) -> None:
        _, read_back = self._round_trip("json")

        rows = _read_csv_output(self.work, "back-json")
        by_id = {row["id"]: row for row in rows}
        self.assertEqual(by_id["1"]["nome"], "alpha")
        self.assertEqual(by_id["1"]["valor"], "1.5")

    def test_csv_returns_the_field_with_quotes_and_a_comma_whole(self) -> None:
        """The RFC 4180 dialect the framework writes has to be the one it reads."""
        _, read_back = self._round_trip("csv")

        rows = _read_csv_output(self.work, "back-csv")
        by_id = {row["id"]: row for row in rows}
        self.assertEqual(by_id["2"]["nome"], 'com "aspas", virgula e acento: cessao')
        self.assertEqual(read_back.rows_read, len(ROWS))

    def test_txt_writes_one_line_per_record(self) -> None:
        written, read_back = self._round_trip("txt")

        self.assertEqual(written.rows_written, len(ROWS))
        self.assertEqual(read_back.rows_read, len(ROWS))

    def test_a_view_hands_the_data_to_the_next_stage_without_touching_disk(self) -> None:
        """The reason both halves run in one process: one SparkSession, one view."""
        written, read_back = self._round_trip("view")

        self.assertEqual(written.rows_written, len(ROWS))
        self.assertEqual(read_back.rows_read, len(ROWS))
        self.assertFalse((self.work / "formatos_round_trip").exists())

    # ---- what the Studio shows for a run ---------------------------------

    def test_the_run_carries_a_preview_the_studio_can_paint(self) -> None:
        read_back = self._run(self._staged("parquet", "read"), limit=2)

        self.assertIsNotNone(read_back.preview)
        # `ingestion_ts` comes from the framework's own default on a write; the
        # point here is that the seed's columns survive to the preview.
        self.assertEqual(read_back.preview.columns[:3], ["id", "nome", "valor"])
        # The limit is the Studio's, not the pipeline's: the write is unaffected.
        self.assertEqual(len(read_back.preview.rows), 2)

    def test_a_failing_stage_reports_the_error_instead_of_raising(self) -> None:
        """`PipelineResult` never throws; the Studio paints `error` on the box."""
        broken = json.loads(json.dumps(self._staged("parquet", "read")))
        broken["input"]["path"] = "{dir}/does-not-exist"

        result = self._run(broken)

        self.assertFalse(result.success)
        self.assertTrue(result.error)


if __name__ == "__main__":
    unittest.main(verbosity=2)
