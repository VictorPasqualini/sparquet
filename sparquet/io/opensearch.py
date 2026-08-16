"""OpenSearch (via opensearch-hadoop).

Conector PRÓPRIO do OpenSearch — distinto do Elasticsearch. Requer
`org.opensearch.client:opensearch-spark-30_<scala>:<versao>` no classpath, que
registra o formato "opensearch". As opções usam o prefixo `opensearch.*` (espelho
do `es.*` do es-hadoop).

path = índice/resource lido/escrito (ex: 'clientes' ou 'clientes/_doc').

Opções (options):
  opensearch.nodes / opensearch.port            – contato do cluster
  opensearch.net.http.auth.user / .pass         – autenticação
  opensearch.nodes.wan.only                      – 'true' para clusters cloud/atrás de proxy
  opensearch.query                               – query DSL empurrada ao cluster (read)
  opensearch.mapping.id                          – coluna usada como _id (write)
  opensearch.write.operation                     – index | create | update | upsert (write)
"""
from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter

_FMT = "opensearch"


class OpenSearchReader(BaseReader):
    def read(self) -> DataFrame:
        return (
            self.spark.read.format(_FMT)
            .options(**self.config.options)
            .load(self.config.path)
        )


class OpenSearchWriter(BaseWriter):
    def write(self, df: DataFrame) -> None:
        (
            df.write.format(_FMT)
            .mode(self.config.mode)
            .options(**self.config.options)
            .save(self.config.path)
        )
