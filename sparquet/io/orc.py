"""ORC (nativo do Spark).

path = diretório ORC. Opções: compression (zlib/snappy/lz4/zstd/none, write),
mergeSchema (read).
"""
from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io._formats import read_via, write_via
from sparquet.io.base import BaseReader, BaseWriter


class OrcReader(BaseReader):
    def read(self) -> DataFrame:
        return read_via(self.spark, self.config, "orc")


class OrcWriter(BaseWriter):
    def write(self, df: DataFrame) -> None:
        write_via(df, self.config, "orc")
