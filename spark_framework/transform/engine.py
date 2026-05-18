from __future__ import annotations

from typing import Any, Dict, List, Optional, Type

from pyspark.sql import DataFrame

from spark_framework.core.config import TransformationConfig
from spark_framework.transform.base import BaseTransformation
from spark_framework.transform.builtin import (
    AddColumnTransformation,
    CacheTransformation,
    CastTransformation,
    CheckpointTransformation,
    DropDuplicatesTransformation,
    DropTransformation,
    FillNaTransformation,
    FilterTransformation,
    GroupByTransformation,
    JoinTransformation,
    RenameTransformation,
    SelectTransformation,
    SortTransformation,
    SqlTransformation,
    UnionTransformation,
    WithColumnTransformation,
    WithColumnsTransformation,
)
from spark_framework.utils.logger import logger

_BUILTIN_TRANSFORMATIONS: Dict[str, Type[BaseTransformation]] = {
    "filter": FilterTransformation,
    "select": SelectTransformation,
    "drop": DropTransformation,
    "rename": RenameTransformation,
    "cast": CastTransformation,
    "with_column": WithColumnTransformation,
    "with_columns": WithColumnsTransformation,
    "add_column": AddColumnTransformation,   # alias backward-compat
    "drop_duplicates": DropDuplicatesTransformation,
    "group_by": GroupByTransformation,
    "sql": SqlTransformation,
    "fill_na": FillNaTransformation,
    "sort": SortTransformation,
    "join": JoinTransformation,
    "union": UnionTransformation,
    "checkpoint": CheckpointTransformation,
    "cache": CacheTransformation,
}


def _evaluate_skip_if(df: DataFrame, expression: str) -> bool:
    """Avalia uma expressão SQL e retorna True quando a transformação deve ser skipada.

    A expressão é avaliada via spark.sql() em uma linha sintética (sem df de
    entrada — uso típico é checar params de runtime já substituídos).

    Skipa quando o resultado é FALSE ou NULL (semântica "se NÃO der true, pula").

    Ex:
      skip_if: "${param_lista_cessoes} IS NULL"
        - param ausente / None / lista vazia → ${param} vira NULL → "NULL IS NULL" → TRUE → skipa
        - param com valor → ${param} vira array(...) ou 'string' → IS NULL → FALSE → não skipa
    """
    try:
        spark = df.sparkSession
        result = spark.sql(f"SELECT ({expression}) AS _skip_check").first()[0]
    except Exception as exc:
        logger.warning(
            "Erro avaliando skip_if — skipando por seguranca",
            expression=expression,
            error=str(exc),
        )
        return True
    return result is None or result is False


class TransformationEngine:
    """Applies a sequence of transformations to a DataFrame.

    Supports registering custom transformations at runtime via `register()`.

    Cada transformação suporta o campo opcional `skip_if`: uma expressão SQL
    avaliada antes da execução. Se retornar FALSE ou NULL, a transformação é
    skipada. Combinado com substituição ${param} permite condicionais elegantes:

      { "type": "filter",
        "skip_if": "${param_lista_cessoes} IS NULL",
        "condition": "array_contains(${param_lista_cessoes}, id_cessao)" }
    """

    def __init__(self) -> None:
        self._registry: Dict[str, Type[BaseTransformation]] = dict(
            _BUILTIN_TRANSFORMATIONS
        )

    def register(self, name: str, cls: Type[BaseTransformation]) -> None:
        self._registry[name] = cls

    def apply(
        self,
        df: DataFrame,
        configs: List[TransformationConfig],
    ) -> DataFrame:
        for config in configs:
            cls = self._registry.get(config.type)
            if cls is None:
                raise ValueError(
                    f"Unknown transformation '{config.type}'. "
                    f"Available: {sorted(self._registry)}"
                )

            skip_expression = config.params.get("skip_if")
            if skip_expression and _evaluate_skip_if(df, skip_expression):
                logger.info(
                    "Transformation skipada (skip_if falso)",
                    type=config.type,
                    expression=skip_expression,
                )
                continue

            logger.info("Applying transformation", type=config.type)
            df = cls(config).apply(df)
        return df

    @property
    def available(self) -> list[str]:
        return sorted(self._registry)
