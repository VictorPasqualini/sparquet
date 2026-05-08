from __future__ import annotations

from typing import Dict, List, Type

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
    JoinTransformation,
    RenameTransformation,
    SelectTransformation,
    SortTransformation,
    SqlTransformation,
    UnionTransformation,
    WithTimestampTransformation,
)
from spark_framework.utils.logger import logger

_BUILTIN_TRANSFORMATIONS: Dict[str, Type[BaseTransformation]] = {
    "filter": FilterTransformation,
    "select": SelectTransformation,
    "drop": DropTransformation,
    "rename": RenameTransformation,
    "cast": CastTransformation,
    "add_column": AddColumnTransformation,
    "drop_duplicates": DropDuplicatesTransformation,
    "sql": SqlTransformation,
    "fill_na": FillNaTransformation,
    "sort": SortTransformation,
    "with_timestamp": WithTimestampTransformation,
    "join": JoinTransformation,
    "union": UnionTransformation,
}


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

    def apply(self, df: DataFrame, configs: List[TransformationConfig]) -> DataFrame:
        for config in configs:
            cls = self._registry.get(config.type)
            if cls is None:
                raise ValueError(
                    f"Unknown transformation '{config.type}'. "
                    f"Available: {sorted(self._registry)}"
                )
            logger.info("Applying transformation", type=config.type)
            df = cls(config).apply(df)
        return df

    @property
    def available(self) -> list[str]:
        return sorted(self._registry)
