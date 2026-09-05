"""OpenSearch (via opensearch-hadoop).

Conector PRÓPRIO do OpenSearch — distinto do Elasticsearch. Requer
`org.opensearch.client:opensearch-spark-<linha>_<scala>:<versao>` no classpath, que
registra o formato "opensearch". As opções usam o prefixo `opensearch.*` (espelho
do `es.*` do es-hadoop).

Em Spark 4 a coordenada é `opensearch-spark-40_2.13:2.0.0` — o único conector de
busca com build de Spark 4 (o `-spark-30` chama `Dataset.sqlContext()`, que saiu da
API). Ele declara as próprias dependências de Spark, então com `spark.jars.packages`
convém excluí-las em `spark.jars.excludes` para não carregar um segundo jogo de jars
do Spark.

**Serve também para Elasticsearch**: o opensearch-hadoop é fork do es-hadoop e fala a
mesma API REST — medido contra um servidor ES 8.16.1, escrita e leitura passam. É a
rota para Elasticsearch em Spark 4, porque o `elasticsearch-spark` não tem build. O
formato tem de ser `opensearch` e as opções `opensearch.*`: este jar não registra o
formato "es" nem entende o prefixo `es.*`.

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
