"""Motor de validação do framework — fino adaptador sobre o `sparquet_cola`.

O bloco `validations` do JSON não muda. Internamente delega ao `Cola` (a biblioteca
de qualidade de dados separável). Mantém `_registry` (dict) para introspecção do
Studio e o contrato `validate(df, config)` usado pelo `Pipeline`.
"""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Type

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
        # Marcadores de etapa por regra (scope="validation"), para o Studio pintar o
        # status de cada nó de validação ao vivo. Ao contrário das transformações,
        # que são lazy, cada regra dispara uma action de verdade — então o "running"
        # aqui representa trabalho real, e o tempo entre started/finished é o custo
        # daquela regra.
        total = len(config.rules)
        results: List[ValidationResult] = []
        for index, rule in enumerate(config.rules):
            logger.info(
                "Validation started",
                rule=rule.type, index=index, total=total, step=True, scope="validation",
            )
            result = self._cola.run(df, [rule])[0]
            logger.info(
                "Validation finished",
                rule=rule.type, index=index, total=total, step=True, scope="validation",
                passed=result.passed, severity=result.severity,
            )
            results.append(result)

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

    def codes(self, config: ValidationConfig) -> List[str]:
        """O código de cada regra, na ordem — declarado (`code`) ou derivado.

        É por esses códigos que uma quarentena se escopa (`outputs.*.rules`) e é o que
        `annotate` grava na linha.
        """
        return self._cola.codes(config.rules)

    def split(
        self,
        df: DataFrame,
        config: ValidationConfig,
        annotate: Optional[str] = None,
        only: Optional[Iterable[str]] = None,
    ) -> ColaSplit:
        """Divide o df em válidas/inválidas a partir das regras (ver Cola.split).

        `annotate` nomeia a coluna `array<string>` com os códigos das regras violadas
        (adicionada só ao lado inválido); `only` restringe o split aos códigos da lista.
        """
        return self._cola.split(df, config.rules, annotate=annotate, only=only)
