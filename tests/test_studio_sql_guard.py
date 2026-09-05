"""Guarda de somente-leitura do editor SQL do Studio.

O editor manda SQL escrito à mão para o runner, que a executa com o mesmo Spark
que roda os Jobs. O que estes testes protegem:

1. **Só leitura.** `INSERT`, `DROP`, `CREATE`, `SET` — qualquer coisa que não seja
   SELECT/WITH/EXPLAIN/DESCRIBE/SHOW morre antes de o Spark ver.
2. **Um comando por vez.** Um `;` no meio esconderia um segundo comando atrás do
   primeiro, que passou na checagem de prefixo.
3. **Comentário não é esconderijo.** `select 1 --` mais quebra de linha e
   `; drop table t` continua sendo dois comandos.
4. **String literal não é separador.** Um `;` dentro de aspas separa nada, e
   recusar a consulta por causa dele seria um falso positivo.

Não precisa de Spark nem de runner: o que está sob teste é a checagem, que roda
antes de qualquer sessão existir.

    PYTHONPATH=. python tests/test_studio_sql_guard.py
"""
from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "sparquet-studio"))

try:
    from fastapi import HTTPException  # noqa: E402

    from server.main import _read_only_sql, _strip_sql_comments  # noqa: E402
except Exception as exc:  # fastapi ausente: o runner é opcional
    print(f"skipped: {exc}")
    raise SystemExit(0)


class ReadOnlyGuard(unittest.TestCase):
    def refused(self, sql: str) -> str:
        with self.assertRaises(HTTPException) as caught:
            _read_only_sql(sql)
        self.assertEqual(caught.exception.status_code, 400)
        return str(caught.exception.detail)

    def test_a_select_passes_unchanged(self):
        self.assertEqual(_read_only_sql("select * from t"), "select * from t")

    def test_a_cte_passes(self):
        self.assertEqual(
            _read_only_sql("WITH a AS (select 1) select * from a;"),
            "WITH a AS (select 1) select * from a",
        )

    def test_explain_describe_and_show_are_reads(self):
        for sql in ("explain select 1", "describe t", "show tables"):
            self.assertEqual(_read_only_sql(sql), sql)

    def test_a_write_is_refused(self):
        for sql in (
            "insert into t values (1)",
            "drop table t",
            "create table t (a int)",
            "set spark.sql.shuffle.partitions=1",
            "delete from t where a = 1",
        ):
            self.assertIn("only reads", self.refused(sql))

    def test_two_statements_are_refused(self):
        self.assertIn("one statement", self.refused("select 1; drop table t"))

    def test_a_comment_does_not_hide_a_second_statement(self):
        self.assertIn("one statement", self.refused("select 1 -- ok\n; drop table t"))

    def test_a_block_comment_does_not_hide_a_write(self):
        self.assertIn("only reads", self.refused("/* select */ insert into t values (1)"))

    def test_a_semicolon_inside_a_string_separates_nothing(self):
        sql = "select 'a; b' as s from t"
        self.assertEqual(_read_only_sql(sql), sql)

    def test_a_semicolon_inside_a_quoted_identifier_separates_nothing(self):
        sql = "select `we;ird` from t"
        self.assertEqual(_read_only_sql(sql), sql)

    def test_an_empty_query_is_refused(self):
        self.assertIn("Write a query", self.refused("   \n  "))

    def test_comments_are_stripped_but_strings_are_not(self):
        self.assertEqual(_strip_sql_comments("select 1 -- tail").strip(), "select 1")
        self.assertEqual(_strip_sql_comments("select '-- not a comment'"), "select '-- not a comment'")


if __name__ == "__main__":
    unittest.main(verbosity=2)
