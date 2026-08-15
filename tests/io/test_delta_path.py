"""Testes da heurística path-vs-tabela do Delta (_is_table_name).

Função pura — não precisa de Spark. Garante que qualquer path/URI (inclusive
s3a:// e outros schemes fora da antiga whitelist) NÃO seja confundido com nome de
tabela de catálogo.

    python tests/io/test_delta_path.py
"""
from __future__ import annotations

import unittest

from sparquet.io.delta import _is_table_name


class TestIsTableName(unittest.TestCase):
    def test_catalog_identifiers_are_tables(self):
        for name in ("catalog.schema.tabela", "schema.tabela", "lastros.titulos"):
            self.assertTrue(_is_table_name(name), name)

    def test_object_storage_uris_are_paths(self):
        # inclui schemes que NÃO estavam na whitelist antiga (s3a, s3n, abfs, wasb, adl, oss)
        for path in (
            "s3://bucket/warehouse/clientes",
            "s3a://company.data.lake/clientes",   # tem ponto: antes virava "tabela"
            "s3a://bucket/schema.db/tabela",       # tem ponto: antes virava "tabela"
            "s3n://bucket/x",
            "gs://bucket/x",
            "abfss://cont@acct.dfs.core.windows.net/x",
            "abfs://cont@acct/x",
            "wasbs://cont@acct.blob.core.windows.net/x",
            "wasb://cont@acct/x",
            "adl://acct.azuredatalakestore.net/x",
            "oss://bucket/x",
            "hdfs://namenode:8020/data/x",
            "dbfs:/mnt/data/x",
            "file:/tmp/x",
        ):
            self.assertFalse(_is_table_name(path), path)

    def test_filesystem_and_relative_and_windows_paths_are_paths(self):
        for path in ("/mnt/data/clientes", "./out.delta", "output/tabela.delta", "C:/data/my.delta"):
            self.assertFalse(_is_table_name(path), path)

    def test_bare_dotted_name_without_slash_is_still_a_table(self):
        # ambiguidade residual documentada: sem barra nem ':' → tabela
        self.assertTrue(_is_table_name("out.delta"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
