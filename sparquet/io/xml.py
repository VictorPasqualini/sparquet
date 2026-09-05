"""XML.

path = diretório XML. O datasource "xml" é nativo a partir do Spark 4.0 — foi
o próprio `spark-xml` doado ao projeto. No Spark 3.x ele não existe, e sem o
jar a leitura morre com
`[DATA_SOURCE_NOT_FOUND] Failed to find the data source: xml`; lá é preciso
`com.databricks:spark-xml_<scala>:<versao>` no classpath, e esse jar **não**
implementa `mode: append` (`Append mode is not supported by
com.databricks.spark.xml.DefaultSource`).

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
