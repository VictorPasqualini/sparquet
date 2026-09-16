"""Tests for the parse-only syntax check behind the SQL editor's markers.

Stdlib plus FastAPI — no Spark:

    python sparquet-studio/server/test_query_validate.py

The endpoint exists so the editor can underline a syntax error without running
anything, and the rule that makes it trustworthy is negative: when there is no
live SparkSession to ask, it must say so rather than answer. A "checked: false"
leaves the editor clean; a wrong "ok: false" underlines somebody's valid SQL.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from typing import Any, List, Optional

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


class FakeParser:
    """Stands in for `sessionState().sqlParser()`.

    It raises whatever it was given, which is the only thing about the real one
    this endpoint depends on: a parser that refuses a statement raises, and the
    message carries the position.
    """

    def __init__(self, error: Optional[Exception] = None) -> None:
        self.error = error
        self.seen: List[str] = []

    def parsePlan(self, statement: str) -> Any:  # noqa: N802 - the JVM's name
        self.seen.append(statement)
        if self.error is not None:
            raise self.error
        return object()


class FakeSession:
    def __init__(self, parser: Any) -> None:
        self._jsparkSession = self  # noqa: N803 - mirrors PySpark's attribute
        self._parser = parser

    def sessionState(self) -> Any:  # noqa: N802 - the JVM's name
        return self

    def sqlParser(self) -> Any:  # noqa: N802 - the JVM's name
        return self._parser


class FakeFramework:
    def __init__(self, parser: Any) -> None:
        self.spark = FakeSession(parser)


PARSE_ERROR = (
    "[PARSE_SYNTAX_ERROR] Syntax error at or near 'form'. SQLSTATE: 42601 "
    "(line 2, pos 0)\n\n"
    "== SQL ==\nSELECT *\nform orders\n^^^\n"
)


class ValidateQueryTest(unittest.TestCase):
    def setUp(self) -> None:
        self._framework = main._framework
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        main._framework = self._framework

    def validate(self, sql: str) -> Any:
        return main.validate_query(main.ValidateQueryRequest(sql=sql))

    def test_nothing_checked_without_a_session(self) -> None:
        main._framework = None
        answer = self.validate("SELECT 1")
        self.assertFalse(answer.checked)
        self.assertIn("SparkSession", answer.reason)

    def test_empty_statement_is_not_an_error(self) -> None:
        main._framework = FakeFramework(FakeParser())
        answer = self.validate("   \n  ")
        self.assertFalse(answer.checked)
        self.assertTrue(answer.ok)

    def test_a_buffer_of_only_comments_is_not_an_error(self) -> None:
        parser = FakeParser()
        main._framework = FakeFramework(parser)
        answer = self.validate("-- what was that table called again")
        self.assertFalse(answer.checked)
        self.assertTrue(answer.ok)
        self.assertEqual(parser.seen, [])

    def test_a_statement_the_parser_accepts_comes_back_ok(self) -> None:
        parser = FakeParser()
        main._framework = FakeFramework(parser)
        answer = self.validate("SELECT 1")
        self.assertTrue(answer.checked)
        self.assertTrue(answer.ok)
        self.assertEqual(parser.seen, ["SELECT 1"])

    def test_a_parse_error_carries_its_line_and_column(self) -> None:
        main._framework = FakeFramework(FakeParser(RuntimeError(PARSE_ERROR)))
        answer = self.validate("SELECT *\nform orders")
        self.assertTrue(answer.checked)
        self.assertFalse(answer.ok)
        self.assertEqual(answer.line, 2)
        self.assertEqual(answer.column, 0)
        self.assertIn("PARSE_SYNTAX_ERROR", answer.message)
        # The SQL dump under the message is noise in a one-line tooltip.
        self.assertNotIn("== SQL ==", answer.message)

    def test_an_error_without_a_position_still_reports_the_message(self) -> None:
        main._framework = FakeFramework(FakeParser(RuntimeError("Something broke")))
        answer = self.validate("SELECT 1")
        self.assertFalse(answer.ok)
        self.assertIsNone(answer.line)
        self.assertIsNone(answer.column)
        self.assertIn("Something broke", answer.message)

    def test_a_write_is_reported_as_the_editor_refusing_it(self) -> None:
        parser = FakeParser()
        main._framework = FakeFramework(parser)
        answer = self.validate("DELETE FROM orders")
        self.assertTrue(answer.checked)
        self.assertFalse(answer.ok)
        self.assertEqual(answer.line, 1)
        self.assertIn("only reads", answer.message)
        # Refused before the parser saw it: the rule is ours, not Spark's.
        self.assertEqual(parser.seen, [])

    def test_the_trailing_semicolon_is_dropped_before_parsing(self) -> None:
        parser = FakeParser()
        main._framework = FakeFramework(parser)
        self.validate("SELECT 1;")
        self.assertEqual(parser.seen, ["SELECT 1"])

    def test_a_spark_without_the_parser_accessor_checks_nothing(self) -> None:
        class Bare:
            spark = object()

        main._framework = Bare()
        answer = self.validate("SELECT 1")
        self.assertFalse(answer.checked)
        self.assertIn("parser", answer.reason)


class ParseFailureTest(unittest.TestCase):
    def test_reads_the_position_out_of_the_message(self) -> None:
        message, line, column = main._parse_failure(RuntimeError(PARSE_ERROR))
        self.assertEqual((line, column), (2, 0))
        self.assertTrue(message.startswith("[PARSE_SYNTAX_ERROR]"))

    def test_survives_a_message_that_names_no_position(self) -> None:
        message, line, column = main._parse_failure(ValueError("nope"))
        self.assertEqual((line, column), (None, None))
        self.assertIn("nope", message)


if __name__ == "__main__":
    unittest.main(verbosity=2)
