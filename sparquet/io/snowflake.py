from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter

# net.snowflake:spark-snowflake_2.12 + snowflake-jdbc no classpath
_FMT = "net.snowflake.spark.snowflake"


class SnowflakeReader(BaseReader):
    """Lê de Snowflake (spark-snowflake connector).

    path = tabela lida (dbtable). Ignorado quando 'query' é informado.

    Opções (options) — conexão sfXxx:
      sfUrl        – ex: minhaorg-conta.snowflakecomputing.com
      sfUser / sfPassword (ou pem_private_key para key-pair auth)
      sfDatabase / sfSchema / sfWarehouse / sfRole
      query        – SELECT lido no lugar de 'dbtable'
    """

    def read(self) -> DataFrame:
        opts = dict(self.config.options)
        query = opts.pop("query", None)
        reader = self.spark.read.format(_FMT).options(**opts)
        if query:
            return reader.option("query", query).load()
        return reader.option("dbtable", self.config.path).load()


class SnowflakeWriter(BaseWriter):
    """Escreve em Snowflake. Modos: overwrite / append.

    path = tabela de destino (dbtable). Opções de conexão sfXxx como no reader
    (+ 'truncate_table', 'column_mapping', 'sfCompress').
    """

    def write(self, df: DataFrame) -> None:
        (
            df.write.format(_FMT)
            .mode(self.config.mode)
            .options(**self.config.options)
            .option("dbtable", self.config.path)
            .save()
        )
