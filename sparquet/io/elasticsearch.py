from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter

# elasticsearch-hadoop (org.elasticsearch:elasticsearch-spark-30). Registra o
# formato "es". Para OpenSearch use o formato `opensearch` (conector próprio,
# io/opensearch.py) — são conectores distintos, com prefixos de opção diferentes
# (`es.*` aqui, `opensearch.*` lá).
#
# Este conector **não roda em Spark 4**: a última publicada (elasticsearch-spark-30
# 9.5.3) ainda compila contra Spark 3.4.3 e morre com
# `java.lang.NoSuchMethodError: 'org.apache.spark.sql.SQLContext
# org.apache.spark.sql.Dataset.sqlContext()'`, e não existe artefato
# `elasticsearch-spark-40`. Em Spark 4, um servidor Elasticsearch se alcança pelo
# formato `opensearch` (mesma API REST; medido contra ES 8.16.1) — ou o pipeline
# fica em Spark 3.5. Ver docs/PIPELINE_SCHEMA.md, "Busca (Elasticsearch e
# OpenSearch) no Spark 4".
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
