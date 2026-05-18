from __future__ import annotations

from typing import Any, Dict, List, Optional, Type

from pyspark.sql import DataFrame

from spark_framework.core.config import TransformationConfig
from spark_framework.transform.base import BaseTransformation
from spark_framework.transform.builtin import (
    AddColumnTransformation,
    CastTransformation,
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
}


def _should_skip(config: TransformationConfig, columns: Dict[str, Any]) -> Optional[str]:
    """Avalia 'skip_if_null' na config e retorna o nome do param vazio (ou None).

    A condição é avaliada contra o dicionário 'columns' (params de runtime do
    Pipeline), NÃO contra valores do DataFrame. Isso permite skipar uma
    transformação inteira quando o param está ausente/None/vazio, sem ter
    que filtrar linha-a-linha.

    Aceita um único nome de param (string) ou uma lista — a transformação é
    skipada quando QUALQUER um dos params estiver vazio.
    """
    skip_if = config.params.get("skip_if_null")
    if skip_if is None:
        return None

    names = [skip_if] if isinstance(skip_if, str) else list(skip_if)
    for name in names:
        value = columns.get(name)
        if value is None:
            return name
        if isinstance(value, (list, tuple, dict, str)) and len(value) == 0:
            return name
    return None


class TransformationEngine:
    """Applies a sequence of transformations to a DataFrame.

    Supports registering custom transformations at runtime via `register()`.
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
        columns: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        runtime_columns: Dict[str, Any] = columns or {}
        for config in configs:
            cls = self._registry.get(config.type)
            if cls is None:
                raise ValueError(
                    f"Unknown transformation '{config.type}'. "
                    f"Available: {sorted(self._registry)}"
                )

            skipped_param = _should_skip(config, runtime_columns)
            if skipped_param is not None:
                logger.info(
                    "Transformation skipada (param vazio)",
                    type=config.type,
                    param=skipped_param,
                )
                continue

            logger.info("Applying transformation", type=config.type)
            df = cls(config).apply(df)
        return df

    @property
    def available(self) -> list[str]:
        return sorted(self._registry)
