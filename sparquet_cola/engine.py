"""Motor do sparquet_cola: roda checks e faz o split valid/invalid (quarentena)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Type

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparquet_cola.checks import (
    BaseCheck,
    CheckResult,
    MetricCheck,
    NotNullCheck,
    RangeCheck,
    RegexCheck,
    RowCountCheck,
    SchemaCheck,
    SqlCheck,
    UniqueCheck,
)

# Registry padrão: nome no JSON → classe de check. O `type` do JSON continua o mesmo
# ("validations" → rules[].type); o branding sparquet_cola é só interno.
_DEFAULT_CHECKS: Dict[str, Type[BaseCheck]] = {
    "not_null": NotNullCheck,
    "unique": UniqueCheck,
    "range": RangeCheck,
    "regex": RegexCheck,
    "row_count": RowCountCheck,
    "sql": SqlCheck,
    "check": MetricCheck,
    "schema": SchemaCheck,
}


@dataclass
class ColaSplit:
    """Resultado do split de quarentena: linhas válidas e inválidas."""

    valid: DataFrame
    invalid: DataFrame


class Cola:
    """API pública do sparquet_cola — use como biblioteca independente.

        from sparquet_cola import Cola
        cola = Cola()

        results = cola.run(df, [
            {"type": "not_null", "columns": ["id"]},
            {"type": "check", "metric": "missing_percent", "column": "cpf", "must_be": "< 5%"},
        ])

        split = cola.split(df, [
            {"type": "not_null", "columns": ["id"]},
            {"type": "check", "metric": "invalid_percent", "column": "email",
             "valid_format": "email", "must_be": "< 100%"},
        ])
        split.valid.write...    split.invalid.write...

    Cada regra é um dict com `type` + params (o mesmo shape do bloco `validations`
    do sparquet). Depende só de pyspark — pode virar um pacote separado.
    """

    def __init__(self) -> None:
        self._registry: Dict[str, Type[BaseCheck]] = dict(_DEFAULT_CHECKS)

    def register(self, name: str, cls: Type[BaseCheck]) -> None:
        """Registra um check customizado disponível por `type`."""
        self._registry[name] = cls

    @property
    def available(self) -> List[str]:
        return sorted(self._registry)

    def build(self, rule: Any) -> BaseCheck:
        """Instancia o check de uma regra (dict com `type`, ou um ValidationRule)."""
        rule_type = rule.type if hasattr(rule, "type") else rule["type"]
        cls = self._registry.get(rule_type)
        if cls is None:
            raise ValueError(f"Unknown check '{rule_type}'. Available: {self.available}")
        if hasattr(rule, "params"):
            return cls(rule)
        params = {k: v for k, v in rule.items() if k != "type"}
        return cls(params)

    def run(self, df: DataFrame, rules: List[Any]) -> List[CheckResult]:
        """Roda todos os checks e devolve um CheckResult por regra."""
        return [self.build(rule).validate(df) for rule in rules]

    def split(self, df: DataFrame, rules: List[Any]) -> ColaSplit:
        """Divide o df em válidas/inválidas usando os checks *row-level*.

        Uma linha é **inválida** quando viola QUALQUER check que sabe apontar linhas
        (not_null, range, regex, unique, e o `check` de missing/invalid). Checks
        agregados (row_count, avg, freshness, duplicate_count…) não entram no split.
        Se nenhum check for row-level, tudo é válido.
        """
        predicates = []
        for rule in rules:
            violation = self.build(rule).violation(df)
            if violation is not None:
                predicates.append(violation)

        if not predicates:
            return ColaSplit(valid=df, invalid=df.limit(0))

        cond = predicates[0]
        for p in predicates[1:]:
            cond = cond | p
        # coalesce(false): uma comparação nula (ex: col fora de range mas nula) não
        # deve nem invalidar nem sumir — vira válida.
        is_invalid = F.coalesce(cond, F.lit(False))
        return ColaSplit(valid=df.filter(~is_invalid), invalid=df.filter(is_invalid))
