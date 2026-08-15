"""Testes da biblioteca sparquet_cola (partes puras — sem SparkSession).

Constrói checks a partir de dicts, verifica o registry, o contrato de check
customizado e quais checks sabem apontar linhas (violation) para o split
valid/invalid. Colunas do Spark são construídas de forma lazy, então não é preciso
uma SparkSession ativa.
"""
from __future__ import annotations

import unittest

from sparquet_cola import Cola, CheckResult
from sparquet_cola.checks import (
    BaseCheck,
    MetricCheck,
    NotNullCheck,
    RangeCheck,
    RegexCheck,
    RowCountCheck,
    SchemaCheck,
    SqlCheck,
    UniqueCheck,
)


class TestRegistry(unittest.TestCase):
    def test_default_checks(self):
        self.assertEqual(
            Cola().available,
            ["check", "not_null", "range", "regex", "row_count", "schema", "sql", "unique"],
        )

    def test_build_from_dict(self):
        chk = Cola().build({"type": "not_null", "columns": ["id"]})
        self.assertIsInstance(chk, NotNullCheck)
        self.assertEqual(chk.params["columns"], ["id"])

    def test_build_strips_type(self):
        chk = Cola().build({"type": "sql", "query": "SELECT true"})
        self.assertIsInstance(chk, SqlCheck)
        self.assertNotIn("type", chk.params)

    def test_unknown_check_raises(self):
        with self.assertRaises(ValueError):
            Cola().build({"type": "nao_existe"})

    def test_register_custom_check(self):
        class AlwaysOk(BaseCheck):
            def run(self, df):
                return CheckResult("always_ok", True)

        cola = Cola()
        cola.register("always_ok", AlwaysOk)
        self.assertIn("always_ok", cola.available)
        self.assertIsInstance(cola.build({"type": "always_ok"}), AlwaysOk)


class TestViolationContract(unittest.TestCase):
    """Contrato do split: quais checks sabem apontar linhas (violation).

    A construção das Columns de violação envolve literais e window functions que
    exigem uma SparkSession ativa, então aqui verificamos apenas o CONTRATO (qual
    check sobrescreve `violation`); a correção dos predicados é coberta em integração.
    """

    def test_row_level_checks_override_violation(self):
        for cls in (NotNullCheck, RangeCheck, RegexCheck, UniqueCheck, MetricCheck):
            self.assertIsNot(cls.violation, BaseCheck.violation, cls.__name__)

    def test_table_level_checks_use_base_violation(self):
        # row_count, sql e schema não atribuem violação a linhas → não entram no split.
        for cls in (RowCountCheck, SqlCheck, SchemaCheck):
            self.assertIs(cls.violation, BaseCheck.violation, cls.__name__)


class TestCheckResult(unittest.TestCase):
    def test_failed_rows_defaults_none_and_severity_derived(self):
        r = CheckResult("sql", passed=False, message="x", failed_count=3)
        self.assertIsNone(r.failed_rows)
        self.assertEqual(r.severity, "fail")
        self.assertEqual(CheckResult("x", passed=True).severity, "pass")


if __name__ == "__main__":
    unittest.main(verbosity=2)
