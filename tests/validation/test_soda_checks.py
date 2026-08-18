"""Testes das partes puras dos checks estilo SODA (sem Spark).

Cobre: parser/avaliador de thresholds, o mapeamento valor→severidade
(evaluate_check), os formatos nomeados e o casamento de tipos do schema.

    python tests/validation/test_soda_checks.py
"""
from __future__ import annotations

import unittest

from sparquet.validation.checks import (
    NAMED_FORMATS,
    _type_matches,
    evaluate_check,
)
from sparquet.validation.thresholds import Threshold, parse_number


class TestThresholds(unittest.TestCase):
    def test_operators(self):
        cases = [
            ("> 0", 5, True), ("> 0", 0, False),
            (">= 10", 10, True), ("<= 10", 11, False),
            ("< 5", 4.9, True), ("= 0", 0, True), ("= 0", 1, False),
            ("!= 0", 1, True), ("<> 0", 0, False),
            ("== 3", 3, True),
        ]
        for expr, val, exp in cases:
            self.assertEqual(Threshold.parse(expr).satisfies(val), exp, expr)

    def test_between(self):
        self.assertTrue(Threshold.parse("between 10 and 20").satisfies(15))
        self.assertFalse(Threshold.parse("between 10 and 20").satisfies(25))
        self.assertTrue(Threshold.parse("not between 1 and 2").satisfies(5))
        self.assertFalse(Threshold.parse("not between 1 and 2").satisfies(1.5))

    def test_percent_and_duration_suffixes(self):
        self.assertEqual(parse_number("5%"), 5.0)
        self.assertEqual(parse_number("1d"), 86400.0)
        self.assertEqual(parse_number("2h"), 7200.0)
        self.assertEqual(parse_number("30m"), 1800.0)
        self.assertEqual(parse_number("1w"), 604800.0)
        self.assertTrue(Threshold.parse("< 1d").satisfies(3600))
        self.assertFalse(Threshold.parse("< 1d").satisfies(90000))

    def test_bare_number_is_equality(self):
        self.assertTrue(Threshold.parse("0").satisfies(0))
        self.assertFalse(Threshold.parse("0").satisfies(1))

    def test_none_never_satisfies(self):
        self.assertFalse(Threshold.parse("> 0").satisfies(None))

    def test_empty_raises(self):
        with self.assertRaises(ValueError):
            Threshold.parse("")


class TestEvaluateCheck(unittest.TestCase):
    def _sev(self, value, must_be, warn=None):
        w = Threshold.parse(warn) if warn else None
        return evaluate_check("missing_percent", value, 0, Threshold.parse(must_be), w, column_label="cpf")

    def test_fail_warn_pass_levels(self):
        self.assertEqual(self._sev(8.0, "< 5%", "= 0").severity, "fail")
        self.assertEqual(self._sev(2.0, "< 5%", "= 0").severity, "warn")
        self.assertEqual(self._sev(0.0, "< 5%", "= 0").severity, "pass")

    def test_warn_absent_means_pass_or_fail_only(self):
        self.assertEqual(self._sev(2.0, "< 5%").severity, "pass")
        self.assertEqual(self._sev(9.0, "< 5%").severity, "fail")

    def test_warn_result_does_not_abort(self):
        # severity "warn" tem passed=True (não entra em failures do engine)
        self.assertTrue(self._sev(2.0, "< 5%", "= 0").passed)
        self.assertFalse(self._sev(8.0, "< 5%", "= 0").passed)

    def test_metric_value_and_label_in_message(self):
        r = evaluate_check("avg", 42.5, 0, Threshold.parse("between 10 and 100"), None, column_label="valor")
        self.assertEqual(r.metric_value, 42.5)
        self.assertIn("avg(valor)", r.message)
        self.assertIn("42.5", r.message)


class TestNamedFormatsAndSchema(unittest.TestCase):
    def test_named_formats_present(self):
        for fmt in ("email", "uuid", "cpf", "cnpj", "integer", "date"):
            self.assertIn(fmt, NAMED_FORMATS)

    def test_type_matches_aliases(self):
        self.assertTrue(_type_matches("bigint", "long"))
        self.assertTrue(_type_matches("int", "integer"))
        self.assertTrue(_type_matches("string", "str"))
        self.assertTrue(_type_matches("decimal(10,2)", "decimal"))
        self.assertTrue(_type_matches("double", "double"))
        self.assertFalse(_type_matches("int", "string"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
