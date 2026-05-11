from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from spark_framework.core.config import TransformationConfig


class BaseTransformation(ABC):
    def __init__(
        self,
        config: TransformationConfig,
        runtime_params: dict | None = None,
    ) -> None:
        self.config = config
        self.runtime_params: dict = runtime_params or {}

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        ...
