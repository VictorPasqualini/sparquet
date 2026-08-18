"""Apache Hudi.

Requer `org.apache.hudi:hudi-spark<versao>-bundle` no classpath e as extensões de
sessão do Hudi. path = caminho base da tabela Hudi.

Escrita: o particionamento e o upsert são controlados por opções `hoodie.*`, então o
`partition_by` do Spark NÃO é usado aqui (use hoodie.datasource.write.partitionpath.field).

Opções de escrita (via `options`):
  hoodie.table.name                          – OBRIGATÓRIO
  hoodie.datasource.write.recordkey.field    – chave do registro (upsert)
  hoodie.datasource.write.precombine.field   – coluna de desempate (mais recente vence)
  hoodie.datasource.write.partitionpath.field
  hoodie.datasource.write.operation          – upsert | insert | bulk_insert | delete
  hoodie.datasource.write.table.type         – COPY_ON_WRITE | MERGE_ON_READ

Leitura:
  hoodie.datasource.query.type               – snapshot | incremental | read_optimized
  hoodie.datasource.read.begin.instanttime   – para leitura incremental
"""
from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io._formats import read_via, write_via
from sparquet.io.base import BaseReader, BaseWriter


class HudiReader(BaseReader):
    def read(self) -> DataFrame:
        return read_via(self.spark, self.config, "hudi")


class HudiWriter(BaseWriter):
    def write(self, df: DataFrame) -> None:
        # Hudi gerencia particionamento/upsert via opções hoodie.* — sem partitionBy.
        write_via(df, self.config, "hudi", partitioning=False)
