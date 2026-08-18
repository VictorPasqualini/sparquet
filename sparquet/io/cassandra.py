from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter

# spark-cassandra-connector (com.datastax.spark).
#
# A MESMA classe atende Cassandra E ScyllaDB — não há reader/writer separado.
# ScyllaDB é compatível com o protocolo CQL/Cassandra; basta apontar
# `spark.cassandra.connection.host` para o cluster Scylla.
_FMT = "org.apache.spark.sql.cassandra"


def _keyspace_table(path: str, opts: dict) -> None:
    """Resolve keyspace/table a partir de 'path' ("keyspace.tabela") ou de options."""
    if "keyspace" not in opts and "." in path:
        keyspace, table = path.split(".", 1)
        opts["keyspace"], opts["table"] = keyspace, table
    else:
        opts.setdefault("table", path)


class CassandraReader(BaseReader):
    """Lê de Cassandra / ScyllaDB (spark-cassandra-connector).

    path = "keyspace.tabela" (ou só a tabela, com 'keyspace' em options).

    Opções (options):
      keyspace / table – sobrepõem o 'path'
      spark.cassandra.connection.host / .port – contato do cluster
      spark.cassandra.auth.username / .password
      Filtros por partition key são empurrados ao Cassandra automaticamente.
    """

    def read(self) -> DataFrame:
        opts = dict(self.config.options)
        _keyspace_table(self.config.path, opts)
        return self.spark.read.format(_FMT).options(**opts).load()


class CassandraWriter(BaseWriter):
    """Escreve em Cassandra / ScyllaDB. Modo: append (INSERT/upsert por chave).

    path = "keyspace.tabela". A tabela precisa existir (o conector não cria schema).
    Opções de conexão como no reader (+ 'spark.cassandra.output.consistency.level').
    """

    def write(self, df: DataFrame) -> None:
        opts = dict(self.config.options)
        _keyspace_table(self.config.path, opts)
        df.write.format(_FMT).mode(self.config.mode).options(**opts).save()
