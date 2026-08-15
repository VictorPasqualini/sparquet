"""Compat: os checks nativos vivem em `sparquet_cola.checks`.

Reexportados com os nomes históricos de validator do framework.
"""
from sparquet_cola.checks import (
    NotNullCheck as NotNullValidator,
    RangeCheck as RangeValidator,
    RegexCheck as RegexValidator,
    RowCountCheck as RowCountValidator,
    SqlCheck as SqlValidator,
    UniqueCheck as UniqueValidator,
)

__all__ = [
    "NotNullValidator",
    "UniqueValidator",
    "RangeValidator",
    "RegexValidator",
    "RowCountValidator",
    "SqlValidator",
]
