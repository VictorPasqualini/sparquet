"""Compat: os checks estilo SODA vivem em `sparquet_cola.checks`.

`check` → `MetricCheck`, `schema` → `SchemaCheck`. Reexportado aqui com os nomes
históricos (`CheckValidator`, `SchemaValidator`) e utilitários usados em testes.
"""
from sparquet_cola.checks import (
    NAMED_FORMATS,
    MetricCheck as CheckValidator,
    SchemaCheck as SchemaValidator,
    _named_format,
    _type_matches,
    evaluate_check,
)

__all__ = [
    "CheckValidator",
    "SchemaValidator",
    "evaluate_check",
    "NAMED_FORMATS",
    "_named_format",
    "_type_matches",
]
