from __future__ import annotations

from typing import Dict, List, Type

from pyspark.sql import DataFrame

from spark_framework.core.config import TransformationConfig
from spark_framework.transform.base import BaseTransformation
from spark_framework.transform.builtin import (
    AddColumnTransformation,
    CastTransformation,
    DebugTransformation,
    DropDuplicatesTransformation,
    DropTransformation,
    FillNaTransformation,
    FilterInTransformation,
    FilterTransformation,
    GroupByTransformation,
    JoinTransformation,
    RenameTransformation,
    SelectTransformation,
    SortTransformation,
    SqlTransformation,
    UnionTransformation,
    WithColumnTransformation,
)
from spark_framework.utils.logger import logger

_BUILTIN_TRANSFORMATIONS: Dict[str, Type[BaseTransformation]] = {
    "filter":    FilterTransformation,
    "filter_in": FilterInTransformation,
    "select": SelectTransformation,
    "drop": DropTransformation,
    "rename": RenameTransformation,
    "cast": CastTransformation,
    "with_column": WithColumnTransformation,
    "add_column": AddColumnTransformation,   # alias backward-compat
    "drop_duplicates": DropDuplicatesTransformation,
    "group_by": GroupByTransformation,
    "sql": SqlTransformation,
    "fill_na": FillNaTransformation,
    "sort": SortTransformation,
    "join": JoinTransformation,
    "union": UnionTransformation,
    "debug": DebugTransformation,
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

    def apply(
        self,
        df: DataFrame,
        configs: List[TransformationConfig],
        runtime_params: dict | None = None,
    ) -> DataFrame:
        _runtime = runtime_params or {}
        for config in configs:
            if config.skip_if_false and not _runtime.get(config.skip_if_false):
                logger.info(
                    "Transformacao pulada",
                    type=config.type,
                    skip_if_false=config.skip_if_false,
                )
                continue
            cls = self._registry.get(config.type)
            if cls is None:
                raise ValueError(
                    f"Unknown transformation '{config.type}'. "
                    f"Available: {sorted(self._registry)}"
                )
            logger.info("Applying transformation", type=config.type)
            df = cls(config, _runtime).apply(df)
        return df

    @property
    def available(self) -> list[str]:
        return sorted(self._registry)
