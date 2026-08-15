from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter

# MongoDB Spark Connector v10+ (org.mongodb.spark:mongo-spark-connector).
# O mesmo conector atende o Amazon DocumentDB (compatível com o protocolo Mongo);
# lá a 'connection.uri' aponta para o cluster DocumentDB (TLS + retryWrites=false).
_FMT = "mongodb"


class MongoReader(BaseReader):
    """Lê uma coleção MongoDB / Amazon DocumentDB.

    path = nome da coleção (collection).

    Opções (options):
      connection.uri – mongodb://user:pass@host:27017/  (OBRIGATÓRIO)
      database       – banco de origem
      collection     – sobrepõe 'path'
      aggregation.pipeline – pipeline de agregação empurrado ao Mongo
      DocumentDB: use a connection string com ?tls=true&retryWrites=false
    """

    def read(self) -> DataFrame:
        opts = dict(self.config.options)
        opts.setdefault("collection", self.config.path)
        return self.spark.read.format(_FMT).options(**opts).load()


class MongoWriter(BaseWriter):
    """Escreve numa coleção MongoDB / Amazon DocumentDB.

    path = coleção de destino. Modos: overwrite (dropa/recria) / append.

    Opções (options):
      connection.uri, database, collection (sobrepõe 'path')
      operationType  – insert | replace | update
      idFieldList    – campos que identificam o documento (replace/update)
      ordered        – 'true'/'false' para bulk write ordenado
    """

    def write(self, df: DataFrame) -> None:
        opts = dict(self.config.options)
        opts.setdefault("collection", self.config.path)
        df.write.format(_FMT).mode(self.config.mode).options(**opts).save()
