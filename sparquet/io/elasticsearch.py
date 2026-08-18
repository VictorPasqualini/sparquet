from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter

# elasticsearch-hadoop (org.elasticsearch:elasticsearch-spark-30). Registra o
# formato "es". Para OpenSearch use o formato `opensearch` (conector próprio,
# io/opensearch.py) — são conectores distintos, com prefixos de opção diferentes
# (`es.*` aqui, `opensearch.*` lá).
_FMT = "es"


class ElasticsearchReader(BaseReader):
    """Lê de Elasticsearch (elasticsearch-hadoop). Para OpenSearch, use `opensearch`.

    path = índice/resource lido (ex: 'clientes' ou 'clientes/_doc').

    Opções (options):
      es.nodes / es.port          – contato do cluster
      es.net.http.auth.user / .pass
      es.nodes.wan.only           – 'true' para clusters atrás de proxy/cloud
      es.query                    – query DSL empurrada ao cluster
      es.read.field.include / .exclude
    """

    def read(self) -> DataFrame:
        return (
            self.spark.read.format(_FMT)
            .options(**self.config.options)
            .load(self.config.path)
        )


class ElasticsearchWriter(BaseWriter):
    """Escreve em Elasticsearch. Modos: append / overwrite. (OpenSearch → `opensearch`.)

    path = índice de destino. Opções de conexão como no reader (+ 'es.mapping.id'
    para usar uma coluna como _id, 'es.write.operation' = index|create|update|upsert).
    """

    def write(self, df: DataFrame) -> None:
        (
            df.write.format(_FMT)
            .mode(self.config.mode)
            .options(**self.config.options)
            .save(self.config.path)
        )
