from __future__ import annotations

from typing import Dict, List, Type

from pyspark.sql import DataFrame

from sparquet.core.config import ValidationConfig
from sparquet.validation.base import BaseValidator, ValidationResult
from sparquet.validation.builtin import (
    CustomSqlValidator,
    NotNullValidator,
    RangeValidator,
    RegexValidator,
    RowCountValidator,
    UniqueValidator,
)
from sparquet.utils.logger import logger

_BUILTIN_VALIDATORS: Dict[str, Type[BaseValidator]] = {
    "not_null": NotNullValidator,
    "unique": UniqueValidator,
    "range": RangeValidator,
    "regex": RegexValidator,
    "row_count": RowCountValidator,
    "custom_sql": CustomSqlValidator,
}


class ValidationEngine:
    """Runs all validation rules against a DataFrame and handles failures.

    Supports registering custom validators at runtime via `register()`.
    """

    def __init__(self) -> None:
        self._registry: Dict[str, Type[BaseValidator]] = dict(_BUILTIN_VALIDATORS)

    def register(self, name: str, cls: Type[BaseValidator]) -> None:
        self._registry[name] = cls

    def validate(
        self, df: DataFrame, config: ValidationConfig
    ) -> List[ValidationResult]:
        results: List[ValidationResult] = []
        failures: List[ValidationResult] = []

        for rule in config.rules:
            cls = self._registry.get(rule.type)
            if cls is None:
                raise ValueError(
                    f"Unknown validator '{rule.type}'. "
                    f"Available: {sorted(self._registry)}"
                )

            result = cls(rule).validate(df)
            results.append(result)

            if result.passed:
                logger.info("Validation passed", rule=rule.type)
            else:
                logger.warning(
                    "Validation failed",
                    rule=rule.type,
                    validation_message=result.message,
                    failed_count=result.failed_count,
                )
                failures.append(result)

        if failures and config.on_failure == "fail":
            summary = "\n".join(f"  {r}" for r in failures)
            raise ValueError(f"Pipeline aborted due to validation failures:\n{summary}")

        return results

    @property
    def available(self) -> list[str]:
        return sorted(self._registry)
