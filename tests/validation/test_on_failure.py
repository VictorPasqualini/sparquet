"""What `validations.on_failure` actually decides — no SparkSession needed.

The key is one word in the JSON and it changes whether a run writes anything at
all, so the three modes are pinned here rather than left to be discovered in
production:

- `fail` (the default) — a violated rule ABORTS. Nothing is written: not the
  destinations, and not the validation report either, because the engine raises
  before the pipeline gets that far. The run comes back as
  `PipelineResult(success=False)` with the failures in `error` — never as an
  exception, which is the framework's contract everywhere else too.
- `warn` — the violation is recorded and the pipeline CONTINUES: report and
  destinations are written exactly as on a clean run.
- `skip` — behaves like `warn` at this level. It does not abort either.

And one asymmetry that is easy to get backwards: a result whose SEVERITY is
`warn` (SODA-style checks) is never a failure, so even `on_failure="fail"` lets
it through. Severity is about the rule; `on_failure` is about the pipeline.

Everything here fakes Spark: the writers only record what they were handed, and
the validation engine is a stub that returns the results the test wants. What is
being tested is the CONTROL FLOW around validation, which is pure Python.

    PYTHONPATH=. python tests/validation/test_on_failure.py
"""
from __future__ import annotations

import unittest
from typing import Any, Dict, List
from unittest import mock

from sparquet.core.config import PipelineConfig, ValidationConfig
from sparquet.core.pipeline import Pipeline
from sparquet.validation.engine import ValidationEngine, ValidationResult


# --------------------------------------------------------------------- fakes


class FakeDataFrame:
    """Enough of a DataFrame for the orchestration to run: every call gives back
    the same object, so the pipeline can chain as much as it likes."""

    columns = ["id", "email"]

    def withColumn(self, *_args, **_kwargs) -> "FakeDataFrame":
        return self

    def select(self, *_args, **_kwargs) -> "FakeDataFrame":
        return self

    def count(self) -> int:
        return 3

    def cache(self) -> "FakeDataFrame":
        return self

    def unpersist(self) -> "FakeDataFrame":
        return self

    def coalesce(self, *_args, **_kwargs) -> "FakeDataFrame":
        return self


class FakeSpark:
    def createDataFrame(self, rows, schema=None) -> FakeDataFrame:  # noqa: ARG002
        return FakeDataFrame()


class RecordingWriter:
    """Every write lands in the shared list, which is the whole assertion."""

    def __init__(self, written: List[str], config: Any) -> None:
        self._written = written
        self._config = config

    def write(self, _df) -> None:
        self._written.append(self._config.path)


class StubValidationEngine:
    """Returns the results the test asked for, and aborts on `fail` exactly as the
    real engine does — the raise is the behaviour under test, so it is reproduced
    rather than mocked away."""

    def __init__(self, results: List[ValidationResult]) -> None:
        self._results = results
        self.called = False

    def validate(self, _df, config: ValidationConfig) -> List[ValidationResult]:
        self.called = True
        failures = [r for r in self._results if not r.passed and r.severity != "warn"]
        if failures and config.on_failure == "fail":
            raise ValueError("Pipeline aborted due to validation failures:\n  broken")
        return self._results

    def codes(self, config: ValidationConfig) -> List[str]:  # noqa: ARG002
        return []


class PassthroughTransformEngine:
    def reset_runtime(self) -> None:
        pass

    def apply(self, df, _transformations, top_level: bool = False):  # noqa: ARG002
        return df


def failed(rule: str = "not_null", severity: str = "error") -> ValidationResult:
    return ValidationResult(
        rule_type=rule, passed=False, message=f"{rule} violated",
        failed_count=2, severity=severity,
    )


def passed(rule: str = "row_count") -> ValidationResult:
    return ValidationResult(rule_type=rule, passed=True, message="", severity="error")


def config_with(on_failure: str) -> PipelineConfig:
    """One rule, one report, one destination — the three things `on_failure`
    decides between."""
    return PipelineConfig.from_dict({
        "name": "on_failure",
        "input": {"format": "csv", "path": "/data/in"},
        "validations": {
            "on_failure": on_failure,
            "rules": [{"type": "not_null", "columns": ["email"]}],
            "report": {"format": "parquet", "path": "/data/report"},
        },
        "output": {"format": "parquet", "path": "/data/out"},
    })


def run_with(on_failure: str, results: List[ValidationResult]) -> Dict[str, Any]:
    """Runs a pipeline whose Spark is fake, and reports what got written."""
    written: List[str] = []
    engine = StubValidationEngine(results)
    pipeline = Pipeline(
        config_with(on_failure),
        transform_engine=PassthroughTransformEngine(),
        validation_engine=engine,
        input_df=FakeDataFrame(),
    )
    with mock.patch(
        "sparquet.core.pipeline.SparkContextManager.get_or_create",
        return_value=FakeSpark(),
    ), mock.patch(
        "sparquet.core.pipeline.WriterFactory.create",
        side_effect=lambda _spark, cfg: RecordingWriter(written, cfg),
    ), mock.patch(
        # The report stamps `F.current_timestamp()` on its DataFrame, and every
        # `pyspark.sql.functions` call reaches for the JVM. There is no JVM here,
        # and the column is not what this file is about.
        "sparquet.core.pipeline.F",
        mock.Mock(),
    ):
        result = pipeline.run()
    return {"result": result, "written": written, "engine": engine}


# --------------------------------------------------------------------- tests


class TestDefault(unittest.TestCase):
    def test_absent_on_failure_means_fail(self):
        """The strict mode is the one you get by not thinking about it."""
        config = PipelineConfig.from_dict({
            "name": "p",
            "input": {"format": "csv", "path": "/in"},
            "output": {"format": "parquet", "path": "/out"},
            "validations": {"rules": [{"type": "row_count", "min": 1}]},
        })
        self.assertEqual(config.validations.on_failure, "fail")


class TestFail(unittest.TestCase):
    """`fail`: nothing is written, and the run says why."""

    def test_a_violation_writes_nothing_at_all(self):
        run = run_with("fail", [failed()])
        self.assertEqual(run["written"], [])

    def test_not_even_the_validation_report_is_written(self):
        """The report is generated AFTER validation, so an abort takes it too.
        Anyone who needs the report of a failing run wants `warn`, not `fail`."""
        run = run_with("fail", [failed()])
        self.assertNotIn("/data/report", run["written"])

    def test_the_run_fails_without_raising(self):
        result = run_with("fail", [failed()])["result"]
        self.assertFalse(result.success)
        self.assertIn("validation failures", result.error)
        self.assertFalse(result.skipped)

    def test_rules_that_all_pass_write_everything(self):
        run = run_with("fail", [passed()])
        self.assertTrue(run["result"].success)
        self.assertEqual(run["written"], ["/data/report", "/data/out"])

    def test_a_warn_severity_result_is_not_a_failure(self):
        """Severity is about the rule, `on_failure` is about the pipeline: a
        SODA-style `warn` check never aborts, whatever the mode."""
        run = run_with("fail", [failed(severity="warn")])
        self.assertTrue(run["result"].success)
        self.assertEqual(run["written"], ["/data/report", "/data/out"])


class TestWarn(unittest.TestCase):
    """`warn`: the violation is recorded and the data still lands."""

    def test_a_violation_writes_the_report_and_the_destinations(self):
        run = run_with("warn", [failed()])
        self.assertTrue(run["result"].success)
        self.assertEqual(run["written"], ["/data/report", "/data/out"])

    def test_the_failure_is_still_reported_on_the_result(self):
        """Continuing is not the same as passing: the results are on the object,
        so a caller can act on them even though the pipeline did not stop."""
        result = run_with("warn", [failed()])["result"]
        self.assertEqual(len(result.validation_results), 1)
        self.assertFalse(result.validation_results[0].passed)
        self.assertIsNone(result.error)


class TestSkip(unittest.TestCase):
    def test_skip_does_not_abort_either(self):
        run = run_with("skip", [failed()])
        self.assertTrue(run["result"].success)
        self.assertEqual(run["written"], ["/data/report", "/data/out"])

    def test_skip_is_not_the_stop_if_empty_kind_of_skip(self):
        """`PipelineResult.skipped` means "no data, ended gracefully". A skipped
        VALIDATION is a different thing and must not set it."""
        self.assertFalse(run_with("skip", [failed()])["result"].skipped)


class TestEngineContract(unittest.TestCase):
    """The abort itself lives in `ValidationEngine`, over a stubbed Cola."""

    def engine(self, results: List[ValidationResult]) -> ValidationEngine:
        """The engine delegates each rule to Cola, one call per rule, and Cola
        answers with a LIST. Only the delegation is stubbed — the decision that
        follows it is the real code."""
        pending = iter(results)
        engine = ValidationEngine()
        engine._cola = mock.Mock(run=mock.Mock(side_effect=lambda _df, _rules: [next(pending)]))
        return engine

    def validate(self, on_failure: str, results: List[ValidationResult]):
        engine = self.engine(results)
        config = ValidationConfig(
            rules=PipelineConfig.from_dict({
                "name": "p",
                "input": {"format": "csv", "path": "/in"},
                "output": {"format": "parquet", "path": "/out"},
                "validations": {"rules": [{"type": "not_null", "columns": ["email"]}]},
            }).validations.rules,
            on_failure=on_failure,
        )
        return engine.validate(FakeDataFrame(), config)

    def test_fail_raises_with_every_failure_in_the_message(self):
        with self.assertRaises(ValueError) as raised:
            self.validate("fail", [failed()])
        self.assertIn("Pipeline aborted due to validation failures", str(raised.exception))

    def test_warn_returns_the_results_instead_of_raising(self):
        results = self.validate("warn", [failed()])
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].passed)

    def test_no_rules_is_no_opinion(self):
        engine = ValidationEngine()
        self.assertEqual(engine.validate(FakeDataFrame(), ValidationConfig()), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
