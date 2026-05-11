from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from spark_framework.core.config import TransformationConfig


class BaseTransformation(ABC):
    def __init__(self, config: TransformationConfig) -> None:
        self.config = config

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        ...
