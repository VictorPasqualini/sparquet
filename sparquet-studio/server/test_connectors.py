"""Tests for the connector configs the runner supplies on its own.

Stdlib plus FastAPI — no Spark:

    python sparquet-studio/server/test_connectors.py

What is being pinned is the answer to "why does Delta work in a Job and not in
the SQL editor". Connector jars are resolved by SparkSubmit while the JVM is
starting and never afterwards, so the session that happens to be alive decides
what every later request can open. Two things follow, and both are tested here:
the runner has to know what a format needs even when nobody told it, and a
rebuild has to drop the gateway or it changes nothing.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from typing import Any, Dict

_TMP = tempfile.TemporaryDirectory()
os.environ["SPARQUET_STUDIO_AUTH_DB"] = os.path.join(_TMP.name, "auth.sqlite3")
os.environ["SPARQUET_STUDIO_CREDITS_DB"] = os.path.join(_TMP.name, "credits.sqlite3")
os.environ["SPARQUET_STUDIO_HISTORY_DB"] = os.path.join(_TMP.name, "history.sqlite3")
os.environ["SPARQUET_STUDIO_WORKSPACE"] = os.path.join(_TMP.name, "workspace")
os.environ.setdefault("SPARQUET_STUDIO_TOKEN", "test-token")

import main  # noqa: E402


class ConnectorConfigTest(unittest.TestCase):
    """What a format needs, when the caller said nothing."""

    def tearDown(self) -> None:
        for key in (
            "SPARQUET_STUDIO_NO_AUTO_CONNECTORS",
            "SPARQUET_STUDIO_DELTA_PACKAGE",
            "SPARQUET_STUDIO_ICEBERG_PACKAGE",
        ):
            os.environ.pop(key, None)

    def test_a_format_spark_already_ships_needs_nothing(self) -> None:
        # A Parquet-only runner must never be made to resolve a jar, or an
        # offline machine stops being able to read a local file.
        self.assertEqual(main._connector_configs(["parquet", "csv", "json"]), {})
        self.assertEqual(main._connector_configs([]), {})

    def test_delta_brings_its_jar_its_extension_and_its_catalog(self) -> None:
        configs = main._connector_configs(["delta"])
        self.assertIn("io.delta:delta-spark", configs["spark.jars.packages"])
        self.assertEqual(
            configs["spark.sql.extensions"], "io.delta.sql.DeltaSparkSessionExtension"
        )
        self.assertEqual(
            configs["spark.sql.catalog.spark_catalog"],
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

    def test_two_formats_stack_in_the_list_configs(self) -> None:
        configs = main._connector_configs(["delta", "iceberg"])
        packages = configs["spark.jars.packages"].split(",")
        self.assertEqual(len(packages), 2)
        self.assertTrue(any("delta" in item for item in packages))
        self.assertTrue(any("iceberg" in item for item in packages))
        self.assertIn(
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            configs["spark.sql.extensions"],
        )

    def test_the_format_name_is_read_case_insensitively(self) -> None:
        self.assertTrue(main._connector_configs(["DELTA"]))

    def test_an_operator_can_pin_another_version(self) -> None:
        os.environ["SPARQUET_STUDIO_DELTA_PACKAGE"] = "io.delta:delta-spark_2.12:3.2.0"
        self.assertEqual(
            main._connector_configs(["delta"])["spark.jars.packages"],
            "io.delta:delta-spark_2.12:3.2.0",
        )

    def test_an_operator_can_turn_the_whole_fallback_off(self) -> None:
        os.environ["SPARQUET_STUDIO_NO_AUTO_CONNECTORS"] = "1"
        self.assertEqual(main._connector_configs(["delta", "iceberg"]), {})


class MergeTest(unittest.TestCase):
    """The caller's own settings against the fallback."""

    def test_nothing_is_added_for_a_format_that_needs_nothing(self) -> None:
        asked = {"configs": {"spark.sql.shuffle.partitions": "8"}}
        self.assertIs(main._spark_for_formats(asked, ["parquet"]), asked)

    def test_the_caller_wins_on_a_key_it_set(self) -> None:
        # A Job pinned to one Delta version must not be moved to another because
        # the runner shipped a newer default.
        asked = {"configs": {"spark.sql.catalog.spark_catalog": "com.example.MyCatalog"}}
        merged = main._spark_for_formats(asked, ["delta"])
        self.assertEqual(
            merged["configs"]["spark.sql.catalog.spark_catalog"], "com.example.MyCatalog"
        )

    def test_a_list_config_keeps_both_sides(self) -> None:
        asked = {"configs": {"spark.jars.packages": "com.example:thing:1.0"}}
        packages = main._spark_for_formats(asked, ["delta"])["configs"]["spark.jars.packages"]
        self.assertIn("com.example:thing:1.0", packages)
        self.assertIn("io.delta:delta-spark", packages)

    def test_the_caller_may_have_said_nothing_at_all(self) -> None:
        merged = main._spark_for_formats(None, ["delta"])
        self.assertIn("io.delta:delta-spark", merged["configs"]["spark.jars.packages"])

    def test_the_master_and_app_name_the_caller_sent_survive(self) -> None:
        merged = main._spark_for_formats({"app_name": "editor"}, ["delta"])
        self.assertEqual(merged["app_name"], "editor")


class PipelineFormatsTest(unittest.TestCase):
    """What a submitted JSON needs, read off the same lineage the run records."""

    def test_both_sides_of_the_pipeline_are_counted(self) -> None:
        config = {
            "name": "bronze",
            "input": {"format": "csv", "path": "/raw/orders.csv"},
            "output": {"format": "delta", "path": "/bronze/orders"},
        }
        self.assertEqual(sorted(set(main._formats_of(config))), ["csv", "delta"])

    def test_a_join_side_input_counts_too(self) -> None:
        config = {
            "name": "join",
            "input": {"format": "parquet", "path": "/a"},
            "transformations": [
                {"type": "join", "input": {"format": "iceberg", "path": "local.sales.orders"}}
            ],
            "output": {"format": "parquet", "path": "/b"},
        }
        self.assertIn("iceberg", main._formats_of(config))

    def test_a_configuration_naming_no_dataset_asks_for_nothing(self) -> None:
        self.assertEqual(main._formats_of({"name": "empty"}), [])
        self.assertEqual(main._formats_of("not a pipeline"), [])


class RestartContractTest(unittest.TestCase):
    """The rebuild path, without building anything."""

    def test_the_jar_configs_are_known_to_be_creation_only(self) -> None:
        # If this stopped being true the runner would silently accept a Delta
        # config on a live session and never restart for it.
        for key in (
            "spark.jars.packages",
            "spark.sql.extensions",
            "spark.sql.catalog.spark_catalog",
        ):
            self.assertTrue(main._creation_only(key), key)

    def test_a_gap_is_only_the_part_the_session_lacks(self) -> None:
        class Conf:
            def __init__(self, values: Dict[str, str]) -> None:
                self.values = values

            def get(self, key: str, default: Any = None) -> Any:
                return self.values.get(key, default)

        class Session:
            def __init__(self, values: Dict[str, str]) -> None:
                self.sparkContext = type("C", (), {"getConf": lambda _self: Conf(values)})()

        live = Session({"spark.jars.packages": "io.delta:delta-spark_2.13:4.3.1"})
        wanted = {
            "spark.jars.packages": "io.delta:delta-spark_2.13:4.3.1",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        }
        self.assertEqual(list(main._session_gap(live, wanted)), ["spark.sql.extensions"])

    def test_a_second_package_is_added_to_the_first_not_swapped_for_it(self) -> None:
        self.assertEqual(
            main._merge_list("io.delta:delta-spark_2.13:4.3.1", "org.apache.iceberg:x:1"),
            "io.delta:delta-spark_2.13:4.3.1,org.apache.iceberg:x:1",
        )

    def test_the_rebuild_drops_the_py4j_gateway(self) -> None:
        # Without this the restart is theatre: `spark.jars.packages` is read by
        # SparkSubmit as the JVM starts, so a session rebuilt over a live gateway
        # reports the package and still cannot open the format.
        import inspect

        source = inspect.getsource(main._ensure_framework)
        self.assertIn("_release_jvm()", source)
        released = inspect.getsource(main._release_jvm)
        self.assertIn("gateway.shutdown()", released)
        self.assertIn("databricks", released)


if __name__ == "__main__":
    unittest.main(verbosity=2)
