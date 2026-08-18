"""XML (via spark-xml).

Requer `com.databricks:spark-xml_<scala>:<versao>` no classpath (registra o
formato "xml"). path = diretório XML.

Opções:
  rowTag  – OBRIGATÓRIO: tag que delimita cada registro (linha). Ex: "book".
  rootTag – tag raiz na escrita (default "rows").
  attributePrefix / valueTag / nullValue / mode – parsing.
"""
from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io._formats import read_via, write_via
from sparquet.io.base import BaseReader, BaseWriter


class XmlReader(BaseReader):
    def read(self) -> DataFrame:
        return read_via(self.spark, self.config, "xml")


class XmlWriter(BaseWriter):
    def write(self, df: DataFrame) -> None:
        write_via(df, self.config, "xml")
