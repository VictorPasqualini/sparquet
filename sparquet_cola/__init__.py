"""sparquet_cola — camada de qualidade de dados (Data Quality) para Spark.

"Cola" é a camada que **gruda** qualidade nos seus DataFrames: checks de métrica e
threshold no estilo SODA Core, regras SQL livres, verificação de schema e o split
válidas/inválidas (quarentena) — tudo em cima de PySpark.

Depende apenas de `pyspark`, então pode ser usada de forma independente do framework
sparquet, em qualquer job Spark:

    from sparquet_cola import Cola

    cola = Cola()

    # 1) Rodar checks e ler os resultados
    for r in cola.run(df, [
        {"type": "row_count", "min": 1},
        {"type": "check", "metric": "missing_percent", "column": "cpf", "must_be": "< 5%", "warn": "= 0"},
        {"type": "sql", "failed_rows": "SELECT * FROM _validation_df WHERE valor < 0"},
    ]):
        print(r)   # [FAIL] 'nome' check: missing_percent(cpf) = 8 viola must_be (< 5%)

    # 2) Separar válidas de inválidas (quarentena)
    split = cola.split(df, [
        {"type": "not_null", "columns": ["id"]},
        {"type": "check", "metric": "invalid_count", "column": "email", "valid_format": "email", "must_be": "= 0"},
    ])
    split.valid.write.format("delta").save(".../silver_ok")
    split.invalid.write.format("delta").save(".../silver_quarentena")

No sparquet, o mesmo motor roda pelo bloco `validations` do JSON — o `type` das
regras é idêntico ao dos exemplos acima.
"""
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
    evaluate_check,
)
from sparquet_cola.engine import Cola, ColaSplit
from sparquet_cola.thresholds import Threshold, parse_number

__version__ = "0.1.0"

__all__ = [
    "Cola",
    "ColaSplit",
    "CheckResult",
    "BaseCheck",
    "Threshold",
    "parse_number",
    "evaluate_check",
    # checks
    "NotNullCheck",
    "UniqueCheck",
    "RangeCheck",
    "RegexCheck",
    "RowCountCheck",
    "SqlCheck",
    "MetricCheck",
    "SchemaCheck",
]
