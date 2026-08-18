"""Motor de validação do framework — fino adaptador sobre o `sparquet_cola`.

O bloco `validations` do JSON não muda. Internamente delega ao `Cola` (a biblioteca
de qualidade de dados separável). Mantém `_registry` (dict) para introspecção do
Studio e o contrato `validate(df, config)` usado pelo `Pipeline`.
"""
from __future__ import annotations

from typing import Dict, List, Type

from pyspark.sql import DataFrame

from sparquet.core.config import ValidationConfig
from sparquet.utils.logger import logger
from sparquet_cola.checks import BaseCheck, CheckResult
from sparquet_cola.engine import Cola, ColaSplit

# Aliases históricos
BaseValidator = BaseCheck
ValidationResult = CheckResult


class ValidationEngine:
    """Roda as regras de `validations` e trata falhas conforme `on_failure`.

    Suporta registrar validators/checks customizados via `register()`. A severidade
    `warn` (checks estilo SODA) é registrada mas **não** aborta o pipeline.
    """

    def __init__(self) -> None:
        self._cola = Cola()
        # O Studio introspecta este dict (server/main.py: getattr(engine, "_registry")).
        self._registry: Dict[str, Type[BaseCheck]] = self._cola._registry

    def register(self, name: str, cls: Type[BaseCheck]) -> None:
        self._cola.register(name, cls)

    @property
    def available(self) -> List[str]:
        return self._cola.available

    def validate(
        self, df: DataFrame, config: ValidationConfig
    ) -> List[ValidationResult]:
        results = self._cola.run(df, config.rules)
        failures: List[ValidationResult] = []

        for result in results:
            if result.severity == "warn":
                logger.warning(
                    "Validation warning",
                    rule=result.rule_type,
                    validation_message=result.message,
                    metric_value=result.metric_value,
                )
            elif result.passed:
                logger.info("Validation passed", rule=result.rule_type)
            else:
                logger.warning(
                    "Validation failed",
                    rule=result.rule_type,
                    validation_message=result.message,
                    failed_count=result.failed_count,
                    metric_value=result.metric_value,
                )
                failures.append(result)

        if failures and config.on_failure == "fail":
            summary = "\n".join(f"  {r}" for r in failures)
            raise ValueError(f"Pipeline aborted due to validation failures:\n{summary}")

        return results

    def split(self, df: DataFrame, config: ValidationConfig) -> ColaSplit:
        """Divide o df em válidas/inválidas a partir das regras (ver Cola.split)."""
        return self._cola.split(df, config.rules)
