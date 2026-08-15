from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter


class KafkaReader(BaseReader):
    """Lê de um tópico Kafka em batch (Spark structured batch read).

    Requer o conector 'spark-sql-kafka-0-10' no classpath (mesmo do writer).
    Atende Amazon MSK: passe kafka.security.protocol=SASL_SSL e
    kafka.sasl.mechanism=AWS_MSK_IAM em options (+ o jar aws-msk-iam-auth).

    path = tópico assinado (subscribe). Para outras estratégias, informe
    'assign' ou 'subscribePattern' em options no lugar do subscribe.

    O DataFrame retornado tem o schema padrão do Kafka (key/value binários, topic,
    partition, offset, timestamp, timestampType) — normalmente seguido de um
    CAST(value AS STRING) numa transformação.

    Opções (options):
      bootstrap_servers         – alias amigável para kafka.bootstrap.servers
      kafka.bootstrap.servers   – forma original do Spark
      startingOffsets           – default 'earliest' (batch lê o tópico inteiro)
      endingOffsets             – default 'latest'
      kafka.security.protocol / kafka.sasl.* – auth (SASL/SSL, MSK IAM)

    Exemplo JSON:
      {
        "format": "kafka",
        "path": "meu-topico",
        "options": { "bootstrap_servers": "broker:9092", "startingOffsets": "earliest" }
      }
    """

    # Estratégias de origem mutuamente exclusivas do conector Kafka
    _SELECTORS = ("subscribe", "assign", "subscribePattern")
    # Em batch, sem offsets explícitos o Spark leria só as mensagens novas; os
    # defaults abaixo fazem um read ler o tópico inteiro.
    _BATCH_DEFAULTS = {"startingOffsets": "earliest", "endingOffsets": "latest"}

    def read(self) -> DataFrame:
        opts = dict(self.config.options)

        if "bootstrap_servers" in opts:
            opts["kafka.bootstrap.servers"] = opts.pop("bootstrap_servers")

        if "kafka.bootstrap.servers" not in opts:
            raise ValueError(
                "KafkaReader: informe 'bootstrap_servers' em options. "
                "Ex: { \"bootstrap_servers\": \"broker:9092\" }"
            )

        merged = {**self._BATCH_DEFAULTS, **opts}
        if not any(sel in merged for sel in self._SELECTORS):
            merged["subscribe"] = self.config.path

        return self.spark.read.format("kafka").options(**merged).load()


class KafkaWriter(BaseWriter):
    """Publica registros em um tópico Kafka via Spark (batch).

    Requer o pacote 'spark-sql-kafka' no classpath do cluster.
    No Databricks: adicione 'org.apache.spark:spark-sql-kafka-0-10_2.12:<versao>'
    nas bibliotecas do cluster ou via spark.jars.packages.

    path   = nome do tópico Kafka

    Colunas esperadas no DataFrame (o framework projeta automaticamente via 'columns'):
      value  – mensagem serializada (string ou bytes) — OBRIGATÓRIO
      key    – chave de particionamento (string, opcional)

    Mapeamento de colunas via 'options' no JSON:
      value_column – coluna a usar como 'value' (default: "payload")
      key_column   – coluna a usar como 'key'   (default: "header", use null para omitir)

    Opções de conexão (passadas direto ao Spark Kafka):
      bootstrap_servers         – alias amigável para kafka.bootstrap.servers
      kafka.bootstrap.servers   – forma original do Spark
      kafka.security.protocol   – ex: SASL_SSL
      kafka.sasl.mechanism      – ex: PLAIN
      kafka.sasl.jaas.config    – credenciais SASL

    Exemplo JSON:
      {
        "format": "kafka",
        "path": "meu-topico",
        "mode": "append",
        "columns": ["header", "payload"],
        "options": {
          "bootstrap_servers": "broker1:9092,broker2:9092",
          "value_column": "payload",
          "key_column": "header"
        }
      }
    """

    # Colunas reconhecidas pelo conector Kafka do Spark
    _KAFKA_COLS = {"key", "value", "topic", "partition", "timestamp", "headers"}

    def write(self, df: DataFrame) -> None:
        opts = dict(self.config.options)

        # Alias amigável → nome canônico do Spark Kafka
        if "bootstrap_servers" in opts:
            opts["kafka.bootstrap.servers"] = opts.pop("bootstrap_servers")

        if "kafka.bootstrap.servers" not in opts:
            raise ValueError(
                "KafkaWriter: informe 'bootstrap_servers' em options. "
                "Ex: { \"bootstrap_servers\": \"broker:9092\" }"
            )

        value_col = opts.pop("value_column", "payload")
        key_col   = opts.pop("key_column",   "header")

        kafka_df = df

        if value_col in kafka_df.columns and value_col != "value":
            kafka_df = kafka_df.withColumnRenamed(value_col, "value")

        if key_col and key_col in kafka_df.columns and key_col != "key":
            kafka_df = kafka_df.withColumnRenamed(key_col, "key")

        # Mantém apenas colunas que o conector Kafka aceita
        kafka_df = kafka_df.select(
            *[c for c in kafka_df.columns if c in self._KAFKA_COLS]
        )

        if "value" not in kafka_df.columns:
            raise ValueError(
                f"KafkaWriter: coluna '{value_col}' nao encontrada no DataFrame. "
                f"Colunas disponiveis: {df.columns}"
            )

        writer = kafka_df.write.format("kafka").option("topic", self.config.path)
        for k, v in opts.items():
            writer = writer.option(k, v)
        writer.save()
