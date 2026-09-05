"""Testes de unidade dos conectores de IO (readers/writers).

Validam a **lógica** de cada conector — format string do Spark, opções montadas,
url JDBC, mapeamento de `path` para tabela/coleção/índice — sem tocar num banco real
nem exigir jars de conector. Usam um Spark "fake" que apenas grava as chamadas
fluentes (.format().option().options().load()/.save()).

    python tests/io/test_connectors.py        # roda com unittest (stdlib, sem pytest)
"""
from __future__ import annotations

import unittest

from sparquet.core.config import InputConfig, OutputConfig
from sparquet.io.factory import ReaderFactory, WriterFactory
from sparquet.utils.logger import _deferred_warnings


def deferred_warnings():
    return list(_deferred_warnings)


def clear_deferred_warnings():
    _deferred_warnings.clear()


# --------------------------------------------------------------------- fakes


class FakeReadBuilder:
    def __init__(self) -> None:
        self.format_name = None
        self.opts: dict = {}
        self.load_arg = "__unset__"

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.opts[key] = value
        return self

    def options(self, **kwargs):
        self.opts.update(kwargs)
        return self

    def load(self, path=None):
        self.load_arg = path
        return "DATAFRAME"


class FakeWriteBuilder:
    def __init__(self) -> None:
        self.format_name = None
        self.mode_name = None
        self.opts: dict = {}
        self.partition_by = None
        self.save_arg = "__unset__"

    def format(self, name):
        self.format_name = name
        return self

    def mode(self, name):
        self.mode_name = name
        return self

    def option(self, key, value):
        self.opts[key] = value
        return self

    def options(self, **kwargs):
        self.opts.update(kwargs)
        return self

    def partitionBy(self, *cols):
        self.partition_by = list(cols)
        return self

    def save(self, path=None):
        self.save_arg = path


class FakeSpark:
    def __init__(self) -> None:
        self.read = FakeReadBuilder()


class FakeDF:
    """DataFrame mínimo: expõe .write com um builder inspecionável."""

    def __init__(self) -> None:
        self.write = FakeWriteBuilder()


def read_with(fmt: str, path: str, options: dict) -> FakeReadBuilder:
    spark = FakeSpark()
    reader = ReaderFactory.create(spark, InputConfig(format=fmt, path=path, options=options))
    reader.read()
    return spark.read


def write_with(fmt: str, path: str, options: dict, mode: str = "overwrite") -> FakeWriteBuilder:
    df = FakeDF()
    writer = WriterFactory.create(
        FakeSpark(), OutputConfig(format=fmt, path=path, mode=mode, options=options)
    )
    writer.write(df)
    return df.write


# ------------------------------------------------------------------ JDBC


class TestJdbc(unittest.TestCase):
    def test_postgres_read_builds_url_from_host(self):
        rb = read_with("postgresql", "public.clientes", {"host": "db", "database": "app", "user": "u"})
        self.assertEqual(rb.format_name, "jdbc")
        self.assertEqual(rb.opts["url"], "jdbc:postgresql://db:5432/app")
        self.assertEqual(rb.opts["driver"], "org.postgresql.Driver")
        self.assertEqual(rb.opts["dbtable"], "public.clientes")
        self.assertEqual(rb.opts["user"], "u")  # passthrough
        # host/port/database/url NÃO viram opções soltas do Spark
        for k in ("host", "port", "database"):
            self.assertNotIn(k, rb.opts)

    def test_explicit_url_takes_precedence(self):
        rb = read_with("mysql", "t", {"url": "jdbc:mysql://x:3306/db", "host": "ignored"})
        self.assertEqual(rb.opts["url"], "jdbc:mysql://x:3306/db")
        self.assertEqual(rb.opts["driver"], "com.mysql.cj.jdbc.Driver")

    def test_query_overrides_dbtable(self):
        rb = read_with("postgresql", "t", {"host": "h", "query": "SELECT 1 AS x"})
        self.assertEqual(rb.opts["query"], "SELECT 1 AS x")
        self.assertNotIn("dbtable", rb.opts)

    def test_partition_column_without_the_other_three_raises(self):
        with self.assertRaises(ValueError) as ctx:
            read_with("postgresql", "t", {"host": "h", "partitionColumn": "id"})
        message = str(ctx.exception)
        for missing in ("lowerBound", "upperBound", "numPartitions"):
            self.assertIn(missing, message)

    def test_complete_partition_quartet_passes_through(self):
        rb = read_with(
            "postgresql",
            "t",
            {
                "host": "h",
                "partitionColumn": "id",
                "lowerBound": "1",
                "upperBound": "1000",
                "numPartitions": "8",
            },
        )
        self.assertEqual(rb.opts["partitionColumn"], "id")
        self.assertEqual(rb.opts["numPartitions"], "8")

    def test_partition_bounds_without_partition_column_only_warn(self):
        # O Spark ignora as três em silêncio; o framework avisa mas não recusa.
        clear_deferred_warnings()
        rb = read_with("postgresql", "t", {"host": "h", "numPartitions": "8"})
        self.assertEqual(rb.opts["numPartitions"], "8")
        self.assertEqual(len(deferred_warnings()), 1)
        self.assertIn("partitionColumn", deferred_warnings()[0][0])
        clear_deferred_warnings()

    def test_query_with_dbtable_raises(self):
        with self.assertRaises(ValueError):
            read_with("postgresql", "t", {"host": "h", "query": "SELECT 1", "dbtable": "x"})

    def test_query_with_partition_column_raises(self):
        with self.assertRaises(ValueError) as ctx:
            read_with(
                "postgresql",
                "t",
                {
                    "host": "h",
                    "query": "SELECT 1",
                    "partitionColumn": "id",
                    "lowerBound": "1",
                    "upperBound": "9",
                    "numPartitions": "4",
                },
            )
        self.assertIn("dbtable", str(ctx.exception))

    def test_sqlserver_and_oracle_and_mariadb_urls(self):
        self.assertEqual(
            read_with("sqlserver", "dbo.t", {"host": "h", "database": "d"}).opts["url"],
            "jdbc:sqlserver://h:1433;databaseName=d",
        )
        self.assertEqual(
            read_with("oracle", "t", {"host": "h", "database": "svc"}).opts["url"],
            "jdbc:oracle:thin:@//h:1521/svc",
        )
        # MariaDB monta url de MySQL de propósito: é o que traz o MySQLDialect
        # (o Spark 4 não tem dialeto MariaDB). Ver `_MariaDbDialect`.
        maria = read_with("mariadb", "t", {"host": "h", "database": "d"})
        self.assertEqual(maria.opts["url"], "jdbc:mysql://h:3306/d")
        self.assertEqual(maria.opts["driver"], "com.mysql.cj.jdbc.Driver")

    def test_mariadb_url_explicita_leva_o_driver_do_mariadb(self):
        explicita = read_with("mariadb", "t", {"url": "jdbc:mariadb://h:3306/d"})
        self.assertEqual(explicita.opts["driver"], "org.mariadb.jdbc.Driver")

    def test_mariadb_driver_informado_vence(self):
        forcado = read_with(
            "mariadb", "t", {"host": "h", "database": "d", "driver": "x.Y"}
        )
        self.assertEqual(forcado.opts["driver"], "x.Y")

    def test_mariadb_url_explicita_sem_ansi_quotes_avisa(self):
        # O Spark 4 não tem dialeto MariaDB: com `jdbc:mariadb://` ele cita
        # identificador com aspas duplas e o servidor recusa — leitura inclusive.
        clear_deferred_warnings()
        read_with("mariadb", "t", {"url": "jdbc:mariadb://h:3306/d"})
        self.assertEqual(len(deferred_warnings()), 1)
        self.assertIn("ANSI_QUOTES", deferred_warnings()[0][0])
        clear_deferred_warnings()

    def test_mariadb_url_explicita_com_ansi_quotes_nao_avisa(self):
        clear_deferred_warnings()
        read_with(
            "mariadb",
            "t",
            {
                "url": "jdbc:mariadb://h:3306/d",
                "sessionVariables": "sql_mode='ANSI_QUOTES'",
            },
        )
        self.assertEqual(deferred_warnings(), [])
        clear_deferred_warnings()

    def test_write_uses_dbtable_and_mode(self):
        wb = write_with("postgresql", "public.saida", {"host": "h", "database": "d"}, mode="append")
        self.assertEqual(wb.format_name, "jdbc")
        self.assertEqual(wb.mode_name, "append")
        self.assertEqual(wb.opts["dbtable"], "public.saida")
        self.assertEqual(wb.opts["url"], "jdbc:postgresql://h:5432/d")

    def test_missing_url_and_host_raises(self):
        with self.assertRaises(ValueError):
            read_with("postgresql", "t", {"user": "u"})


# ----------------------------------------------------- warehouses / connectors


class TestWarehouses(unittest.TestCase):
    def test_bigquery_read_load_path_and_write_table(self):
        rb = read_with("bigquery", "proj.ds.tab", {"parentProject": "billing"})
        self.assertEqual(rb.format_name, "bigquery")
        self.assertEqual(rb.load_arg, "proj.ds.tab")
        self.assertEqual(rb.opts["parentProject"], "billing")

        wb = write_with("bigquery", "proj.ds.tab", {"temporaryGcsBucket": "b"})
        self.assertEqual(wb.format_name, "bigquery")
        self.assertEqual(wb.opts["table"], "proj.ds.tab")
        self.assertIsNone(wb.save_arg)

    def test_bigquery_query_option(self):
        rb = read_with("bigquery", "ignored", {"query": "SELECT 1", "viewsEnabled": "true"})
        self.assertEqual(rb.opts["query"], "SELECT 1")
        self.assertIsNone(rb.load_arg)  # load() sem path quando há query

    def test_snowflake_dbtable_and_format(self):
        rb = read_with("snowflake", "ANALYTICS.PUBLIC.T", {"sfUrl": "x"})
        self.assertEqual(rb.format_name, "net.snowflake.spark.snowflake")
        self.assertEqual(rb.opts["dbtable"], "ANALYTICS.PUBLIC.T")
        wb = write_with("snowflake", "ANALYTICS.PUBLIC.T", {"sfUrl": "x"})
        self.assertEqual(wb.opts["dbtable"], "ANALYTICS.PUBLIC.T")

    def test_redshift_dbtable(self):
        rb = read_with("redshift", "public.vendas", {"url": "jdbc:redshift://h/db", "tempdir": "s3://b"})
        self.assertTrue(rb.format_name.endswith("redshift"))
        self.assertEqual(rb.opts["dbtable"], "public.vendas")
        self.assertEqual(rb.opts["tempdir"], "s3://b")


# --------------------------------------------------------- NoSQL / search


class TestNoSql(unittest.TestCase):
    def test_mongodb_collection_from_path(self):
        rb = read_with("mongodb", "clientes", {"connection.uri": "mongodb://h", "database": "app"})
        self.assertEqual(rb.format_name, "mongodb")
        self.assertEqual(rb.opts["collection"], "clientes")
        self.assertEqual(rb.opts["database"], "app")

    def test_documentdb_reuses_mongo(self):
        wb = write_with("documentdb", "pedidos", {"connection.uri": "mongodb://docdb"}, mode="append")
        self.assertEqual(wb.format_name, "mongodb")
        self.assertEqual(wb.opts["collection"], "pedidos")

    def test_dynamodb_tablename(self):
        rb = read_with("dynamodb", "Orders", {"region": "us-east-1"})
        self.assertEqual(rb.format_name, "dynamodb")
        self.assertEqual(rb.opts["tableName"], "Orders")
        self.assertEqual(rb.opts["region"], "us-east-1")

    def test_cassandra_keyspace_table_split(self):
        rb = read_with("cassandra", "loja.pedidos", {"spark.cassandra.connection.host": "h"})
        self.assertEqual(rb.format_name, "org.apache.spark.sql.cassandra")
        self.assertEqual(rb.opts["keyspace"], "loja")
        self.assertEqual(rb.opts["table"], "pedidos")

    def test_cassandra_table_only_with_keyspace_option(self):
        rb = read_with("cassandra", "pedidos", {"keyspace": "loja"})
        self.assertEqual(rb.opts["keyspace"], "loja")
        self.assertEqual(rb.opts["table"], "pedidos")

    def test_elasticsearch_resource_as_load_path(self):
        rb = read_with("elasticsearch", "clientes", {"es.nodes": "h"})
        self.assertEqual(rb.format_name, "es")
        self.assertEqual(rb.load_arg, "clientes")
        wb = write_with("elasticsearch", "clientes", {"es.nodes": "h"}, mode="append")
        self.assertEqual(wb.save_arg, "clientes")


# ---------------------------------------------------------------- Kafka read


class TestKafkaRead(unittest.TestCase):
    def test_subscribe_from_path_and_bootstrap_alias_and_batch_defaults(self):
        rb = read_with("kafka", "meu-topico", {"bootstrap_servers": "b:9092"})
        self.assertEqual(rb.format_name, "kafka")
        self.assertEqual(rb.opts["subscribe"], "meu-topico")
        self.assertEqual(rb.opts["kafka.bootstrap.servers"], "b:9092")
        self.assertNotIn("bootstrap_servers", rb.opts)
        self.assertEqual(rb.opts["startingOffsets"], "earliest")
        self.assertEqual(rb.opts["endingOffsets"], "latest")

    def test_explicit_selector_keeps_path_out(self):
        rb = read_with("kafka", "ignored", {"bootstrap_servers": "b:9092", "assign": '{"t":[0]}'})
        self.assertNotIn("subscribe", rb.opts)
        self.assertEqual(rb.opts["assign"], '{"t":[0]}')

    def test_missing_bootstrap_raises(self):
        with self.assertRaises(ValueError):
            read_with("kafka", "t", {"startingOffsets": "earliest"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
