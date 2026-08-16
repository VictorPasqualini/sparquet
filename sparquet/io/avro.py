"""Apache Avro.

Requer o pacote `org.apache.spark:spark-avro_<scala>:<versao>` no classpath
(spark.jars.packages). path = diretório Avro.

Opções: avroSchema (JSON do schema), recordName / recordNamespace (write),
compression (snappy/deflate/bzip2/xz, write), mode / ignoreExtension (read).
"""
from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io._formats import read_via, write_via
from sparquet.io.base import BaseReader, BaseWriter


class AvroReader(BaseReader):
    def read(self) -> DataFrame:
        return read_via(self.spark, self.config, "avro")


class AvroWriter(BaseWriter):
    def write(self, df: DataFrame) -> None:
        write_via(df, self.config, "avro")
