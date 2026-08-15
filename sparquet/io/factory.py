from __future__ import annotations

from typing import Dict, Type

from pyspark.sql import SparkSession

from sparquet.core.config import InputConfig, OutputConfig
from sparquet.io.base import BaseReader, BaseWriter
from sparquet.io.bigquery import BigQueryReader, BigQueryWriter
from sparquet.io.cassandra import CassandraReader, CassandraWriter
from sparquet.io.csv import CsvReader, CsvWriter
from sparquet.io.delta import DeltaReader, DeltaWriter
from sparquet.io.dynamodb import DynamoDbReader, DynamoDbWriter
from sparquet.io.elasticsearch import ElasticsearchReader, ElasticsearchWriter
from sparquet.io.iceberg import IcebergReader, IcebergWriter
from sparquet.io.jdbc import (
    MariaDbReader,
    MariaDbWriter,
    MySqlReader,
    MySqlWriter,
    OracleReader,
    OracleWriter,
    PostgresReader,
    PostgresWriter,
    SqlServerReader,
    SqlServerWriter,
)
from sparquet.io.kafka import KafkaReader, KafkaWriter
from sparquet.io.mongodb import MongoReader, MongoWriter
from sparquet.io.parquet import ParquetReader, ParquetWriter
from sparquet.io.redshift import RedshiftReader, RedshiftWriter
from sparquet.io.snowflake import SnowflakeReader, SnowflakeWriter
from sparquet.io.txt import TxtReader, TxtWriter
from sparquet.io.view import ViewReader, ViewWriter

_READERS: Dict[str, Type[BaseReader]] = {
    # Arquivos / lakehouse
    "parquet": ParquetReader,
    "iceberg": IcebergReader,
    "csv": CsvReader,
    "delta": DeltaReader,
    "txt": TxtReader,
    "view": ViewReader,
    # Relacionais (JDBC)
    "postgresql": PostgresReader,
    "mysql": MySqlReader,
    "mariadb": MariaDbReader,
    "sqlserver": SqlServerReader,
    "oracle": OracleReader,
    # Data warehouses
    "bigquery": BigQueryReader,
    "snowflake": SnowflakeReader,
    "redshift": RedshiftReader,
    # NoSQL
    "mongodb": MongoReader,
    "documentdb": MongoReader,
    "dynamodb": DynamoDbReader,
    "cassandra": CassandraReader,
    "elasticsearch": ElasticsearchReader,
    # Streaming (leitura batch)
    "kafka": KafkaReader,
}

_WRITERS: Dict[str, Type[BaseWriter]] = {
    # Arquivos / lakehouse
    "parquet": ParquetWriter,
    "iceberg": IcebergWriter,
    "csv": CsvWriter,
    "delta": DeltaWriter,
    "txt": TxtWriter,
    "view": ViewWriter,
    # Relacionais (JDBC)
    "postgresql": PostgresWriter,
    "mysql": MySqlWriter,
    "mariadb": MariaDbWriter,
    "sqlserver": SqlServerWriter,
    "oracle": OracleWriter,
    # Data warehouses
    "bigquery": BigQueryWriter,
    "snowflake": SnowflakeWriter,
    "redshift": RedshiftWriter,
    # NoSQL
    "mongodb": MongoWriter,
    "documentdb": MongoWriter,
    "dynamodb": DynamoDbWriter,
    "cassandra": CassandraWriter,
    "elasticsearch": ElasticsearchWriter,
    # Streaming
    "kafka": KafkaWriter,
}


class ReaderFactory:
    _registry: Dict[str, Type[BaseReader]] = dict(_READERS)

    @classmethod
    def register(cls, format_name: str, reader_cls: Type[BaseReader]) -> None:
        cls._registry[format_name] = reader_cls

    @classmethod
    def create(cls, spark: SparkSession, config: InputConfig) -> BaseReader:
        reader_cls = cls._registry.get(config.format)
        if reader_cls is None:
            raise ValueError(
                f"Formato de leitura '{config.format}' nao suportado. "
                f"Disponivel: {sorted(cls._registry)}"
            )
        return reader_cls(spark, config)


class WriterFactory:
    _registry: Dict[str, Type[BaseWriter]] = dict(_WRITERS)

    @classmethod
    def register(cls, format_name: str, writer_cls: Type[BaseWriter]) -> None:
        cls._registry[format_name] = writer_cls

    @classmethod
    def create(cls, spark: SparkSession, config: OutputConfig) -> BaseWriter:
        writer_cls = cls._registry.get(config.format)
        if writer_cls is None:
            raise ValueError(
                f"Formato de escrita '{config.format}' nao suportado. "
                f"Disponivel: {sorted(cls._registry)}"
            )
        return writer_cls(spark, config)
