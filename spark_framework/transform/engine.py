from __future__ import annotations

import re
from typing import Any, Dict, List, Type

from pyspark.sql import DataFrame

from spark_framework.core.config import TransformationConfig
from spark_framework.transform.base import BaseTransformation
from spark_framework.transform.builtin import (
    AddColumnTransformation,
    CastTransformation,
    CheckpointTransformation,
    CollectTransformation,
    DebugTransformation,
    DistinctTransformation,
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
)
from spark_framework.utils.logger import logger

_BUILTIN_TRANSFORMATIONS: Dict[str, Type[BaseTransformation]] = {
    "filter":  FilterTransformation,
    "select": SelectTransformation,
    "drop": DropTransformation,
    "rename": RenameTransformation,
    "cast": CastTransformation,
    "with_column": WithColumnTransformation,
    "add_column": AddColumnTransformation,   # alias backward-compat
    "drop_duplicates": DropDuplicatesTransformation,
    "distinct": DistinctTransformation,
    "checkpoint": CheckpointTransformation,
    "collect": CollectTransformation,
    "group_by": GroupByTransformation,
    "sql": SqlTransformation,
    "fill_na": FillNaTransformation,
    "sort": SortTransformation,
    "join": JoinTransformation,
    "union": UnionTransformation,
    "debug": DebugTransformation,
}

# Placeholder de runtime: {{var}} — distinto do {var} de template (pré-parse).
_RUNTIME_PLACEHOLDER = re.compile(r"\{\{(\w+)\}\}")


def _format_runtime_value(value: Any) -> str:
    """Formata um valor de runtime como literal SQL pronto para IN (...).

    list não vazia → "'a', 'b'" (str, com aspas escapadas) ou "1, 2" (num)
    list vazia      → "NULL"  (IN (NULL) não casa nada — sem cessões aptas)
    str             → "'valor'" (aspas escapadas)
    outros          → str(value)
    """
    def sql_str(v: Any) -> str:
        return "'" + str(v).replace("'", "''") + "'"

    if isinstance(value, list):
        if not value:
            return "NULL"
        if isinstance(value[0], str):
            return ", ".join(sql_str(v) for v in value)
        return ", ".join(str(v) for v in value)
    if isinstance(value, str):
        return sql_str(value)
    return str(value)


class TransformationEngine:
    """Applies a sequence of transformations to a DataFrame.

    Supports registering custom transformations at runtime via `register()`.
    """

    def __init__(self, runtime: Dict[str, Any] | None = None) -> None:
        self._registry: Dict[str, Type[BaseTransformation]] = dict(
            _BUILTIN_TRANSFORMATIONS
        )
        # Store de variáveis de runtime (ex: valores de CollectTransformation).
        # Engines aninhados (joins) recebem o mesmo dict por referência, então
        # variáveis coletadas no escopo externo são visíveis lá dentro.
        self.runtime: Dict[str, Any] = runtime if runtime is not None else {}

    def register(self, name: str, cls: Type[BaseTransformation]) -> None:
        self._registry[name] = cls

    def reset_runtime(self) -> None:
        """Limpa o store de runtime (in place). Chamado a cada novo pipeline,
        pois o engine é reusado entre execuções no SparkFramework."""
        self.runtime.clear()

    def apply(
        self,
        df: DataFrame,
        configs: List[TransformationConfig],
    ) -> DataFrame:
        for config in configs:
            if config.skip_if_false is not None and not config.skip_if_false:
                logger.info(
                    "Transformacao pulada",
                    type=config.type,
                    skip_if_false=config.skip_if_false,
                )
                continue
            config = self._resolve_runtime(config)
            cls = self._registry.get(config.type)
            if cls is None:
                raise ValueError(
                    f"Unknown transformation '{config.type}'. "
                    f"Available: {sorted(self._registry)}"
                )
            logger.info("Applying transformation", type=config.type)
            transformation = cls(config)
            transformation.runtime = self.runtime
            df = transformation.apply(df)
        return df

    def _resolve_runtime(self, config: TransformationConfig) -> TransformationConfig:
        """Substitui placeholders {{var}} nos params usando o store de runtime.

        Reconstrói recursivamente strings/listas/dicts (sem mutar o config
        original, preservando os placeholders para re-execuções). Placeholders
        cujas variáveis ainda não foram coletadas ficam literais — serão
        resolvidos por um engine aninhado quando a variável existir.
        """
        if not self.runtime:
            return config

        def resolve(obj: Any) -> Any:
            if isinstance(obj, str):
                return _RUNTIME_PLACEHOLDER.sub(self._sub_placeholder, obj)
            if isinstance(obj, dict):
                return {k: resolve(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [resolve(v) for v in obj]
            return obj

        return TransformationConfig(
            type=config.type,
            params=resolve(config.params),
            skip_if_false=config.skip_if_false,
        )

    def _sub_placeholder(self, match: re.Match) -> str:
        key = match.group(1)
        if key not in self.runtime:
            return match.group(0)  # mantém literal; engine aninhado resolve depois
        return _format_runtime_value(self.runtime[key])

    @property
    def available(self) -> list[str]:
        return sorted(self._registry)
