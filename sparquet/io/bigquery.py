from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter

# spark-bigquery-connector (com.google.cloud.spark:spark-bigquery-with-dependencies)
_FMT = "bigquery"


class BigQueryReader(BaseReader):
    """Lê tabelas do Google BigQuery (spark-bigquery-connector).

    path = 'projeto.dataset.tabela' (ou 'dataset.tabela' com projeto default).

    Opções (options):
      query          – SQL lido no lugar da tabela (requer 'viewsEnabled': 'true'
                       e materialização; a leitura por tabela é preferível)
      parentProject  – projeto de faturamento (billing) quando difere do da tabela
      credentialsFile / credentials – service account (arquivo ou JSON base64)
      filter         – predicado empurrado ao BigQuery
      maxParallelism / preferredMinParallelism – paralelismo de leitura
    """

    def read(self) -> DataFrame:
        opts = dict(self.config.options)
        query = opts.pop("query", None)
        reader = self.spark.read.format(_FMT).options(**opts)
        if query:
            return reader.option("query", query).load()
        return reader.load(self.config.path)


class BigQueryWriter(BaseWriter):
    """Escreve em tabela BigQuery (spark-bigquery-connector).

    path = 'projeto.dataset.tabela'. Modos: overwrite / append.

    Opções (options):
      temporaryGcsBucket – bucket GCS de staging (writeMethod 'indirect', default)
      writeMethod        – 'indirect' (via GCS) ou 'direct' (BigQuery Storage Write API)
      partitionField / partitionType / clusteredFields – particionamento nativo BQ
      createDisposition / intermediateFormat
    """

    def write(self, df: DataFrame) -> None:
        (
            df.write.format(_FMT)
            .mode(self.config.mode)
            .options(**self.config.options)
            .option("table", self.config.path)
            .save()
        )
