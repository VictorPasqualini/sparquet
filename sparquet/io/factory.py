from __future__ import annotations

from typing import Dict, Type

from pyspark.sql import SparkSession

from sparquet.core.config import InputConfig, OutputConfig
from sparquet.io.base import BaseReader, BaseWriter
from sparquet.io.csv import CsvReader, CsvWriter
from sparquet.io.delta import DeltaReader, DeltaWriter
from sparquet.io.iceberg import IcebergReader, IcebergWriter
from sparquet.io.kafka import KafkaWriter
from sparquet.io.parquet import ParquetReader, ParquetWriter
from sparquet.io.txt import TxtReader, TxtWriter
from sparquet.io.view import ViewReader, ViewWriter

_READERS: Dict[str, Type[BaseReader]] = {
    "parquet": ParquetReader,
    "iceberg": IcebergReader,
    "csv": CsvReader,
    "delta": DeltaReader,
    "txt": TxtReader,
    "view": ViewReader,
}

_WRITERS: Dict[str, Type[BaseWriter]] = {
    "parquet": ParquetWriter,
    "iceberg": IcebergWriter,
    "csv": CsvWriter,
    "delta": DeltaWriter,
    "txt": TxtWriter,
    "kafka": KafkaWriter,
    "view": ViewWriter,
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
