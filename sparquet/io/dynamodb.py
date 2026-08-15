from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter

# spark-dynamodb (com.audienceproject:spark-dynamodb) — conector DataSource v2.
# Também atende endpoints compatíveis (DynamoDB local) via 'endpoint'.
_FMT = "dynamodb"


class DynamoDbReader(BaseReader):
    """Lê uma tabela DynamoDB (spark-dynamodb).

    path = nome da tabela (tableName).

    Opções (options):
      region      – ex: us-east-1
      roleArn     – assume role para acesso cross-account
      endpoint    – override do endpoint (ex: DynamoDB local)
      throughput / targetCapacity – controle de consumo de RCU
      readPartitions / stronglyConsistentReads – tuning da leitura
    """

    def read(self) -> DataFrame:
        opts = dict(self.config.options)
        opts.setdefault("tableName", self.config.path)
        return self.spark.read.format(_FMT).options(**opts).load()


class DynamoDbWriter(BaseWriter):
    """Escreve numa tabela DynamoDB (spark-dynamodb). Modo: append (upsert por chave).

    path = tabela de destino (tableName). O DynamoDB não tem 'overwrite' de tabela;
    a escrita é sempre um PutItem por linha (upsert pela chave primária).

    Opções (options):
      region, roleArn, endpoint
      writeBatchSize        – itens por BatchWriteItem
      throughput / targetCapacity – controle de consumo de WCU
    """

    def write(self, df: DataFrame) -> None:
        opts = dict(self.config.options)
        opts.setdefault("tableName", self.config.path)
        df.write.format(_FMT).mode(self.config.mode).options(**opts).save()
