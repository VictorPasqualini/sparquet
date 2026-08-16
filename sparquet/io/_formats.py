"""Helpers finos para formatos que são só `spark.read/write.format(...).load/save`."""
from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from sparquet.core.config import InputConfig, OutputConfig


def read_via(
    spark: SparkSession, config: InputConfig, fmt: str, defaults: Optional[Dict] = None
) -> DataFrame:
    options = {**(defaults or {}), **config.options}
    return spark.read.format(fmt).options(**options).load(config.path)


def write_via(
    df: DataFrame,
    config: OutputConfig,
    fmt: str,
    defaults: Optional[Dict] = None,
    partitioning: bool = True,
) -> None:
    options = {**(defaults or {}), **config.options}
    writer = df.write.format(fmt).mode(config.mode).options(**options)
    if partitioning and config.partition_by:
        writer = writer.partitionBy(*config.partition_by)
    writer.save(config.path)
