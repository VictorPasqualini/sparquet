"""Testes do conector JDBC que rodam sem sessao Spark.

Cobrem a parte deterministica do conector — montagem de URL, resolucao de
credenciais, traducao de opcoes e SQL de upsert — que e onde mora a maior parte
dos erros silenciosos. A leitura/escrita em si depende de um banco real e fica
para os testes de integracao.

    pip install pytest
    pytest tests/test_jdbc.py -q
"""

from __future__ import annotations

import pytest

from spark_framework.io.jdbc import (
    build_url,
    canonical_vendor,
    quote_identifier,
    resolve_secret,
    spark_options,
    staging_name,
    upsert_sql,
)


class TestCanonicalVendor:
    def test_aliases(self):
        assert canonical_vendor("postgresql") == "postgres"
        assert canonical_vendor("mssql") == "sqlserver"

    def test_case_and_spacing(self):
        assert canonical_vendor("  PostgreS ") == "postgres"

    def test_unknown_passes_through(self):
        assert canonical_vendor("db2") == "db2"


class TestBuildUrl:
    def test_postgres_defaults_port(self):
        url = build_url("postgres", {"host": "db", "database": "vendas"})
        assert url == "jdbc:postgresql://db:5432/vendas"

    def test_mysql_custom_port(self):
        url = build_url("mysql", {"host": "db", "database": "loja", "port": 3307})
        assert url == "jdbc:mysql://db:3307/loja"

    def test_sqlserver_uses_database_name_property(self):
        url = build_url("sqlserver", {"host": "db", "database": "dw"})
        assert url == "jdbc:sqlserver://db:1433;databaseName=dw"

    def test_oracle_service_name(self):
        url = build_url("oracle", {"host": "db", "database": "ORCLPDB1"})
        assert url == "jdbc:oracle:thin:@//db:1521/ORCLPDB1"

    def test_explicit_url_wins(self):
        url = build_url("postgres", {"url": "jdbc:postgresql://outro:5432/x", "host": "db"})
        assert url == "jdbc:postgresql://outro:5432/x"

    def test_generic_requires_url(self):
        with pytest.raises(ValueError, match="informe 'url'"):
            build_url("jdbc", {"host": "db", "database": "x"})

    def test_missing_host(self):
        with pytest.raises(ValueError, match="informe 'host'"):
            build_url("postgres", {"database": "vendas"})

    def test_missing_database(self):
        with pytest.raises(ValueError, match="informe 'database'"):
            build_url("mysql", {"host": "db"})


class TestResolveSecret:
    def test_literal(self):
        assert resolve_secret({"password": "s3cr3t"}, "password") == "s3cr3t"

    def test_from_environment(self, monkeypatch):
        monkeypatch.setenv("PG_PASSWORD", "do-ambiente")
        assert resolve_secret({"password_env": "PG_PASSWORD"}, "password") == "do-ambiente"

    def test_environment_wins_over_literal(self, monkeypatch):
        monkeypatch.setenv("PG_PASSWORD", "do-ambiente")
        options = {"password": "inline", "password_env": "PG_PASSWORD"}
        assert resolve_secret(options, "password") == "do-ambiente"

    def test_missing_variable_is_explicit(self, monkeypatch):
        monkeypatch.delenv("NAO_DEFINIDA", raising=False)
        with pytest.raises(ValueError, match="NAO_DEFINIDA"):
            resolve_secret({"password_env": "NAO_DEFINIDA"}, "password")

    def test_absent_is_none(self):
        assert resolve_secret({}, "password") is None


class TestSparkOptions:
    def test_translates_snake_case_aliases(self):
        resolved = spark_options(
            "postgres",
            {
                "host": "db",
                "database": "vendas",
                "partition_column": "id",
                "lower_bound": 1,
                "upper_bound": 1000,
                "num_partitions": 4,
                "fetch_size": 5000,
            },
        )
        assert resolved["partitionColumn"] == "id"
        assert resolved["lowerBound"] == "1"
        assert resolved["upperBound"] == "1000"
        assert resolved["numPartitions"] == "4"
        assert resolved["fetchsize"] == "5000"

    def test_drops_framework_only_keys(self):
        resolved = spark_options(
            "postgres",
            {
                "host": "db",
                "database": "vendas",
                "merge_keys": ["id"],
                "staging_table": "public.stg",
                "query": "SELECT 1",
            },
        )
        for key in ("host", "database", "merge_keys", "staging_table", "query"):
            assert key not in resolved

    def test_fills_vendor_driver(self):
        resolved = spark_options("mysql", {"host": "db", "database": "loja"})
        assert resolved["driver"] == "com.mysql.cj.jdbc.Driver"

    def test_explicit_driver_wins(self):
        resolved = spark_options(
            "postgres", {"host": "db", "database": "x", "driver": "meu.Driver"}
        )
        assert resolved["driver"] == "meu.Driver"

    def test_generic_requires_driver(self):
        with pytest.raises(ValueError, match="informe 'driver'"):
            spark_options("jdbc", {"url": "jdbc:db2://host:50000/base"})

    def test_credentials_from_environment(self, monkeypatch):
        monkeypatch.setenv("PG_USER", "app")
        monkeypatch.setenv("PG_PASSWORD", "s3cr3t")
        resolved = spark_options(
            "postgres",
            {"host": "db", "database": "x", "user_env": "PG_USER", "password_env": "PG_PASSWORD"},
        )
        assert resolved["user"] == "app"
        assert resolved["password"] == "s3cr3t"


class TestQuoteIdentifier:
    def test_postgres_double_quotes_each_part(self):
        assert quote_identifier("postgres", "public.clientes") == '"public"."clientes"'

    def test_mysql_backticks(self):
        assert quote_identifier("mysql", "loja.pedidos") == "`loja`.`pedidos`"

    def test_sqlserver_brackets(self):
        assert quote_identifier("sqlserver", "dbo.clientes") == "[dbo].[clientes]"

    def test_escapes_the_delimiter(self):
        assert quote_identifier("postgres", 'we"ird') == '"we""ird"'


class TestStagingName:
    def test_keeps_schema_and_suffixes_table(self):
        name = staging_name("public.clientes")
        assert name.startswith("public.clientes_sparquet_stg_")
        assert name.count(".") == 1

    def test_unique_per_call(self):
        assert staging_name("clientes") != staging_name("clientes")


class TestUpsertSql:
    COLUMNS = ["id", "nome", "total"]
    KEYS = ["id"]

    def test_postgres_on_conflict(self):
        sql = upsert_sql("postgres", "public.clientes", "public.stg", self.COLUMNS, self.KEYS)
        assert 'INSERT INTO "public"."clientes"' in sql
        assert 'ON CONFLICT ("id") DO UPDATE SET' in sql
        assert '"nome" = EXCLUDED."nome"' in sql
        assert '"id" = EXCLUDED."id"' not in sql

    def test_postgres_do_nothing_when_only_keys(self):
        sql = upsert_sql("postgres", "t", "stg", ["id"], ["id"])
        assert "DO NOTHING" in sql

    def test_mysql_on_duplicate_key(self):
        sql = upsert_sql("mysql", "loja.clientes", "loja.stg", self.COLUMNS, self.KEYS)
        assert "ON DUPLICATE KEY UPDATE" in sql
        assert "`nome` = VALUES(`nome`)" in sql

    def test_mysql_insert_ignore_when_only_keys(self):
        assert "INSERT IGNORE" in upsert_sql("mysql", "t", "stg", ["id"], ["id"])

    def test_sqlserver_merge_ends_with_semicolon(self):
        sql = upsert_sql("sqlserver", "dbo.clientes", "dbo.stg", self.COLUMNS, self.KEYS)
        assert sql.startswith("MERGE INTO [dbo].[clientes] T USING [dbo].[stg] S")
        assert "WHEN MATCHED THEN UPDATE SET" in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql
        assert sql.endswith(";")

    def test_oracle_merge_without_semicolon(self):
        sql = upsert_sql("oracle", "clientes", "stg", self.COLUMNS, self.KEYS)
        assert sql.startswith("MERGE INTO")
        assert not sql.endswith(";")

    def test_restricts_updated_columns(self):
        sql = upsert_sql(
            "postgres", "t", "stg", self.COLUMNS, self.KEYS, update_columns=["total"]
        )
        assert '"total" = EXCLUDED."total"' in sql
        assert '"nome" = EXCLUDED."nome"' not in sql

    def test_key_missing_from_dataframe(self):
        with pytest.raises(ValueError, match="merge_keys"):
            upsert_sql("postgres", "t", "stg", ["nome"], ["id"])

    def test_composite_keys(self):
        sql = upsert_sql("postgres", "t", "stg", ["a", "b", "c"], ["a", "b"])
        assert 'ON CONFLICT ("a", "b")' in sql
        assert '"c" = EXCLUDED."c"' in sql
