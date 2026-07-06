from abc import ABC, abstractmethod
from typing import Any, Dict

from pyspark.sql import DataFrame

from spark_framework.core.config import TransformationConfig


class PipelineStop(Exception):
    """Sinaliza o encerramento *gracioso* do pipeline — sem erro.

    Levantada por transformações como `stop_if_empty` quando não há motivo para
    continuar (ex: nenhum dado a processar). É capturada por `Pipeline.run`, que
    retorna um PipelineResult com success=True, skipped=True e rows_written=0,
    sem aplicar as transformações restantes nem escrever nas saídas.
    """


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
