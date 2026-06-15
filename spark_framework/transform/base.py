from abc import ABC, abstractmethod
from typing import Any, Dict

from pyspark.sql import DataFrame

from spark_framework.core.config import TransformationConfig


class BaseTransformation(ABC):
    def __init__(self, config: TransformationConfig) -> None:
        self.config = config
        # Store de variáveis de runtime compartilhado pelo engine durante uma
        # execução (ex: valores coletados por CollectTransformation). O engine
        # injeta o dict real antes de chamar apply(); o default vazio serve para
        # uso isolado/testes.
        self.runtime: Dict[str, Any] = {}

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        ...
