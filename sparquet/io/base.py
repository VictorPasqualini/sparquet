from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from sparquet.core.config import InputConfig, OutputConfig


class BaseReader(ABC):
    def __init__(self, spark: SparkSession, config: InputConfig) -> None:
        self.spark = spark
        self.config = config

    @abstractmethod
    def read(self) -> DataFrame:
        ...


class BaseWriter(ABC):
    def __init__(self, spark: SparkSession, config: OutputConfig) -> None:
        self.spark = spark
        self.config = config

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        ...
