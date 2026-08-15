from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter

# spark-redshift community connector (io.github.spark-redshift-community).
# Usa UNLOAD/COPY via um bucket S3 de staging ('tempdir').
_FMT = "io.github.spark_redshift_community.spark.redshift"


class RedshiftReader(BaseReader):
    """Lê do Amazon Redshift (spark-redshift; UNLOAD para S3 e leitura do S3).

    path = tabela lida (dbtable). Ignorado quando 'query' é informado.

    Opções (options):
      url        – jdbc:redshift://host:5439/db   (OBRIGATÓRIO)
      tempdir    – s3://bucket/prefixo de staging (OBRIGATÓRIO)
      user/password ou aws_iam_role
      forward_spark_s3_credentials – 'true' repassa as credenciais S3 da sessão
      query      – SELECT lido no lugar de 'dbtable'
    """

    def read(self) -> DataFrame:
        opts = dict(self.config.options)
        query = opts.pop("query", None)
        reader = self.spark.read.format(_FMT).options(**opts)
        if query:
            return reader.option("query", query).load()
        return reader.option("dbtable", self.config.path).load()


class RedshiftWriter(BaseWriter):
    """Escreve no Amazon Redshift (staging em S3 + COPY). Modos: overwrite / append.

    path = tabela de destino (dbtable). Requer 'url' e 'tempdir' (S3) em options,
    como no reader (+ 'aws_iam_role', 'tempformat', 'diststyle', 'distkey', 'sortkeyspec').
    """

    def write(self, df: DataFrame) -> None:
        (
            df.write.format(_FMT)
            .mode(self.config.mode)
            .options(**self.config.options)
            .option("dbtable", self.config.path)
            .save()
        )
