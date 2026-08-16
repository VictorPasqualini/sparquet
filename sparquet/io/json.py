"""JSON (nativo do Spark).

path = diretório de arquivos JSON. Opções úteis (via `options`): multiLine,
primitivesAsString, dateFormat, timestampFormat, mode (parser), compression (write).
"""
from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io._formats import read_via, write_via
from sparquet.io.base import BaseReader, BaseWriter


class JsonReader(BaseReader):
    def read(self) -> DataFrame:
        return read_via(self.spark, self.config, "json")


class JsonWriter(BaseWriter):
    def write(self, df: DataFrame) -> None:
        write_via(df, self.config, "json")
