"""Checks de qualidade de dados do **sparquet_cola**.

Este módulo é o coração da biblioteca. Cada check mede algo sobre um DataFrame Spark
e devolve um `CheckResult`. Checks *row-level* (not_null, range, regex, unique, e o
`check` de invalidade) também sabem apontar **quais linhas** violam via `violation()`,
o que permite o split valid/invalid (quarentena).

Depende apenas de `pyspark` — pode ser extraído para uma lib independente.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# --------------------------------------------------------------------- result


@dataclass
class CheckResult:
    """Resultado de um check. `severity` no estilo SODA: pass | warn | fail."""

    rule_type: str
    passed: bool
    message: str = ""
    failed_count: int = 0
    severity: str = ""
    metric_value: Optional[float] = None
    check_name: str = ""
    # DataFrame das linhas que falharam (quando o check produz linhas, ex: sql
    # failed_rows). Transiente — não entra no relatório de métricas.
    failed_rows: Any = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.severity:
            self.severity = "pass" if self.passed else "fail"

    def __str__(self) -> str:
        status = self.severity.upper()
        name = f" '{self.check_name}'" if self.check_name else ""
        return f"[{status}]{name} {self.rule_type}: {self.message or 'OK'}"


# ----------------------------------------------------------------------- base


class _RuleShim:
    """Adapta um dict de params para o mesmo `.params` de um ValidationRule."""

    __slots__ = ("params",)

    def __init__(self, params: dict) -> None:
        self.params = params


class BaseCheck:
    """Contrato de um check.

    Aceita tanto um **dict** (uso como biblioteca) quanto um objeto com `.params`
    (o `ValidationRule` do framework) — assim o mesmo código serve aos dois mundos.

    Checks do sparquet_cola implementam `run()`. Validators legados do framework
    (registrados via `register_validator`) sobrescrevem `validate()` e usam
    `self.rule.params` — ambos os contratos funcionam.
    """

    def __init__(self, spec: Any) -> None:
        if hasattr(spec, "params"):
            self.rule = spec
            self.params = spec.params
        else:
            self.params = dict(spec)
            self.rule = _RuleShim(self.params)

    def run(self, df: DataFrame) -> CheckResult:
        raise NotImplementedError("implemente run() (ou validate() para validators legados)")

    # Ponto de entrada chamado pelo motor.
    def validate(self, df: DataFrame) -> CheckResult:
        return self.run(df)

    def violation(self, df: DataFrame) -> Optional[Column]:
        """Column True para as linhas que violam este check.

        Row-level checks sobrescrevem; checks agregados (row_count, avg, freshness,
        duplicate_count…) retornam None — não dá para atribuir a violação a linhas.
        """
        return None

    def _name(self) -> str:
        return self.params.get("name", "")


# ------------------------------------------------------------ builtin checks


class NotNullCheck(BaseCheck):
    def run(self, df: DataFrame) -> CheckResult:
        columns: List[str] = self.params["columns"]
        violations = {}
        for col in columns:
            count = df.filter(F.col(col).isNull()).count()
            if count > 0:
                violations[col] = count
        if violations:
            return CheckResult(
                "not_null", False,
                f"Null values found in columns: {violations}",
                sum(violations.values()), check_name=self._name(),
            )
        return CheckResult("not_null", True, check_name=self._name())

    def violation(self, df: DataFrame) -> Column:
        cols = self.params["columns"]
        cond = F.col(cols[0]).isNull()
        for c in cols[1:]:
            cond = cond | F.col(c).isNull()
        return cond


class UniqueCheck(BaseCheck):
    def run(self, df: DataFrame) -> CheckResult:
        columns: List[str] = self.params["columns"]
        total = df.count()
        distinct = df.select(*columns).distinct().count()
        duplicates = total - distinct
        if duplicates > 0:
            return CheckResult(
                "unique", False,
                f"Found {duplicates} duplicate rows for columns {columns}",
                duplicates, check_name=self._name(),
            )
        return CheckResult("unique", True, check_name=self._name())

    def violation(self, df: DataFrame) -> Column:
        cols = [F.col(c) for c in self.params["columns"]]
        return F.count(F.lit(1)).over(Window.partitionBy(*cols)) > 1


class RangeCheck(BaseCheck):
    def _condition(self) -> Optional[Column]:
        column = self.params["column"]
        min_val = self.params.get("min")
        max_val = self.params.get("max")
        filters = []
        if min_val is not None:
            filters.append(F.col(column) < min_val)
        if max_val is not None:
            filters.append(F.col(column) > max_val)
        if not filters:
            return None
        cond = filters[0]
        for f in filters[1:]:
            cond = cond | f
        return cond

    def run(self, df: DataFrame) -> CheckResult:
        cond = self._condition()
        if cond is None:
            return CheckResult("range", True, check_name=self._name())
        failed = df.filter(cond).count()
        if failed > 0:
            column = self.params["column"]
            return CheckResult(
                "range", False,
                f"Column '{column}' has {failed} values outside range "
                f"[{self.params.get('min')}, {self.params.get('max')}]",
                failed, check_name=self._name(),
            )
        return CheckResult("range", True, check_name=self._name())

    def violation(self, df: DataFrame) -> Optional[Column]:
        return self._condition()


class RegexCheck(BaseCheck):
    def _condition(self) -> Column:
        column = self.params["column"]
        pattern = self.params["pattern"]
        return ~F.col(column).rlike(pattern) | F.col(column).isNull()

    def run(self, df: DataFrame) -> CheckResult:
        failed = df.filter(self._condition()).count()
        if failed > 0:
            return CheckResult(
                "regex", False,
                f"Column '{self.params['column']}' has {failed} values "
                f"not matching pattern '{self.params['pattern']}'",
                failed, check_name=self._name(),
            )
        return CheckResult("regex", True, check_name=self._name())

    def violation(self, df: DataFrame) -> Column:
        return self._condition()


class RowCountCheck(BaseCheck):
    def run(self, df: DataFrame) -> CheckResult:
        min_count = self.params.get("min", 0)
        max_count = self.params.get("max")
        count = df.count()
        if count < min_count or (max_count is not None and count > max_count):
            return CheckResult(
                "row_count", False,
                f"Row count {count} is outside expected range [{min_count}, {max_count}]",
                1, metric_value=float(count), check_name=self._name(),
            )
        return CheckResult("row_count", True, metric_value=float(count), check_name=self._name())


class SqlCheck(BaseCheck):
    """Regra SQL livre sobre a temp view `_validation_df`.

    Dois modos:
      - `query`       – expressa o INVARIANTE (pass-when-true): a query retorna um
                        booleano; passa quando True.
      - `failed_rows` – expressa a VIOLAÇÃO: a query retorna as **linhas ruins**;
                        falha se vier alguma, e anexa o DataFrame em `result.failed_rows`
                        (roteável para um destino, ver o writer do framework).
    """

    def run(self, df: DataFrame) -> CheckResult:
        view = self.params.get("view_name", "_validation_df")
        df.createOrReplaceTempView(view)

        failed_query = self.params.get("failed_rows")
        if failed_query:
            bad = df.sparkSession.sql(failed_query)
            n = bad.count()
            if n > 0:
                return CheckResult(
                    "sql", False,
                    self.params.get("error_message", f"{n} failed rows"),
                    n, metric_value=float(n), failed_rows=bad, check_name=self._name(),
                )
            return CheckResult("sql", True, metric_value=0.0, check_name=self._name())

        query = self.params["query"]
        passed = bool(df.sparkSession.sql(query).collect()[0][0])
        if not passed:
            return CheckResult(
                "sql", False,
                self.params.get("error_message", "SQL validation failed"),
                1, check_name=self._name(),
            )
        return CheckResult("sql", True, check_name=self._name())


# --------------------------------------------------------------- named formats

NAMED_FORMATS = {
    "email": r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$",
    "uuid": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
    "phone": r"^\+?[0-9 ()\-]{7,}$",
    "integer": r"^[-+]?\d+$",
    "decimal": r"^[-+]?\d+(\.\d+)?$",
    "number": r"^[-+]?\d+(\.\d+)?$",
    "percentage": r"^[-+]?\d+(\.\d+)?\s*%?$",
    "date": r"^\d{4}-\d{2}-\d{2}$",
    "timestamp": r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}",
    "ip": r"^(\d{1,3}\.){3}\d{1,3}$",
    "ipv4": r"^(\d{1,3}\.){3}\d{1,3}$",
    "url": r"^https?://[^\s]+$",
    "boolean": r"^(true|false|0|1|t|f|yes|no)$",
    "alphanumeric": r"^[A-Za-z0-9]+$",
    "credit_card": r"^\d{13,19}$",
    "cpf": r"^\d{11}$|^\d{3}\.\d{3}\.\d{3}-\d{2}$",
    "cnpj": r"^\d{14}$|^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$",
}

_NUMERIC_AGGS = {
    "min": F.min, "max": F.max, "avg": F.avg, "mean": F.avg, "sum": F.sum, "stddev": F.stddev,
}


def _named_format(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    regex = NAMED_FORMATS.get(str(name).lower())
    if regex is None:
        raise ValueError(
            f"check: valid_format '{name}' desconhecido. Disponiveis: {sorted(NAMED_FORMATS)}"
        )
    return regex


def _fmt(value: Optional[float]) -> str:
    if value is None:
        return "None"
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value == float("inf"):
            return "inf"
        if value.is_integer():
            return str(int(value))
        return f"{value:.4f}"
    return str(value)


def evaluate_check(metric, value, failed_count, must_be, warn, name="", column_label=""):
    """Compara o valor da métrica com os thresholds → CheckResult (função pura)."""
    label = f"{metric}({column_label})" if column_label else metric
    shown = _fmt(value)

    if not must_be.satisfies(value):
        return CheckResult(
            "check", False, f"{label} = {shown} viola must_be ({must_be.describe()})",
            failed_count, severity="fail", metric_value=value, check_name=name,
        )
    if warn is not None and not warn.satisfies(value):
        return CheckResult(
            "check", True,
            f"{label} = {shown} passa must_be ({must_be.describe()}) mas viola warn ({warn.describe()})",
            failed_count, severity="warn", metric_value=value, check_name=name,
        )
    return CheckResult(
        "check", True, f"{label} = {shown} (ok: {must_be.describe()})",
        severity="pass", metric_value=value, check_name=name,
    )


# ---------------------------------------------------------------- metric check


class MetricCheck(BaseCheck):
    """Check SODA-style: métrica + threshold (warn/fail) + configs de validade."""

    def run(self, df: DataFrame) -> CheckResult:
        from sparquet_cola.thresholds import Threshold

        metric = self.params.get("metric")
        if not metric:
            raise ValueError("check requer 'metric' (ex: row_count, missing_percent, avg, freshness).")
        must_be_raw = self.params.get("must_be", self.params.get("condition"))
        if must_be_raw is None:
            raise ValueError("check requer 'must_be' (threshold), ex: '> 0', 'between 10 and 20', '< 5%'.")
        must_be = Threshold.parse(must_be_raw)
        warn = Threshold.parse(self.params["warn"]) if self.params.get("warn") else None

        value, failed_count = self._compute(df, metric)
        return evaluate_check(metric, value, failed_count, must_be, warn,
                              name=self._name(), column_label=self._column_label())

    def violation(self, df: DataFrame) -> Optional[Column]:
        metric = self.params.get("metric", "")
        if metric in ("invalid_count", "invalid_percent"):
            column = self._require_column(metric)
            return ~self._missing_predicate(column) & ~self._valid_predicate(column)
        if metric in ("missing_count", "missing_percent"):
            return self._missing_predicate(self._require_column(metric))
        return None

    def _compute(self, df: DataFrame, metric: str) -> Tuple[Optional[float], int]:
        columns = self._columns()

        if metric == "row_count":
            return float(df.count()), 0
        if metric == "distinct_count":
            n = (df.select(*columns).distinct() if columns else df.distinct()).count()
            return float(n), 0
        if metric in ("duplicate_count", "duplicate_percent"):
            total = df.count()
            distinct = (df.select(*columns).distinct() if columns else df.distinct()).count()
            dups = total - distinct
            if metric == "duplicate_count":
                return float(dups), dups
            return (100.0 * dups / total if total else 0.0), dups
        if metric in ("missing_count", "missing_percent"):
            column = self._require_column(metric)
            cnt = df.filter(self._missing_predicate(column)).count()
            if metric == "missing_count":
                return float(cnt), cnt
            total = df.count()
            return (100.0 * cnt / total if total else 0.0), cnt
        if metric in ("invalid_count", "invalid_percent"):
            column = self._require_column(metric)
            invalid = ~self._missing_predicate(column) & ~self._valid_predicate(column)
            cnt = df.filter(invalid).count()
            if metric == "invalid_count":
                return float(cnt), cnt
            total = df.count()
            return (100.0 * cnt / total if total else 0.0), cnt
        if metric == "freshness":
            column = self._require_column(metric)
            age = df.agg(
                (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max(F.col(column)))).alias("age")
            ).collect()[0]["age"]
            return (float(age) if age is not None else float("inf")), 0

        agg = _NUMERIC_AGGS.get(metric)
        if agg is not None:
            column = self._require_column(metric)
            v = df.agg(agg(F.col(column)).alias("v")).collect()[0]["v"]
            return (float(v) if v is not None else float("nan")), 0

        raise ValueError(
            f"check: metric '{metric}' desconhecida. Disponiveis: row_count, distinct_count, "
            f"missing_count, missing_percent, duplicate_count, duplicate_percent, invalid_count, "
            f"invalid_percent, min, max, avg, mean, sum, stddev, freshness."
        )

    def _missing_predicate(self, column: str) -> Column:
        pred = F.col(column).isNull()
        missing_values = self.params.get("missing_values")
        if missing_values:
            pred = pred | F.col(column).cast("string").isin([str(v) for v in missing_values])
        return pred

    def _valid_predicate(self, column: str) -> Column:
        p = self.params
        col = F.col(column)
        as_str = col.cast("string")
        conds: List[Column] = []
        if "valid_values" in p:
            conds.append(col.isin(p["valid_values"]))
        if "invalid_values" in p:
            conds.append(~col.isin(p["invalid_values"]))
        regex = p.get("valid_regex") or _named_format(p.get("valid_format"))
        if regex:
            conds.append(as_str.rlike(regex))
        if "valid_min" in p:
            conds.append(col >= p["valid_min"])
        if "valid_max" in p:
            conds.append(col <= p["valid_max"])
        if "valid_min_length" in p:
            conds.append(F.length(as_str) >= p["valid_min_length"])
        if "valid_max_length" in p:
            conds.append(F.length(as_str) <= p["valid_max_length"])
        if "valid_length" in p:
            conds.append(F.length(as_str) == p["valid_length"])
        if not conds:
            return F.lit(True)
        cond = conds[0]
        for c in conds[1:]:
            cond = cond & c
        return cond

    def _columns(self) -> List[str]:
        if self.params.get("columns"):
            return list(self.params["columns"])
        if self.params.get("column"):
            return [self.params["column"]]
        return []

    def _require_column(self, metric: str) -> str:
        column = self.params.get("column")
        if column:
            return column
        cols = self.params.get("columns")
        if cols:
            return cols[0]
        raise ValueError(f"check metric '{metric}' requer 'column'.")

    def _column_label(self) -> str:
        return ", ".join(self._columns())


# ----------------------------------------------------------------- schema check

_TYPE_ALIASES = {
    "integer": "int", "int": "int", "long": "bigint", "bigint": "bigint",
    "short": "smallint", "byte": "tinyint", "str": "string", "string": "string",
    "text": "string", "float": "float", "double": "double", "bool": "boolean",
    "boolean": "boolean", "timestamp": "timestamp", "date": "date",
}


def _type_matches(actual: str, expected: str) -> bool:
    a = actual.strip().lower()
    e = _TYPE_ALIASES.get(expected.strip().lower(), expected.strip().lower())
    if e.startswith("decimal") or a.startswith("decimal"):
        return a.split("(")[0] == e.split("(")[0]
    return a == e


class SchemaCheck(BaseCheck):
    """Colunas obrigatórias/proibidas + tipos esperados (data contract básico)."""

    def run(self, df: DataFrame) -> CheckResult:
        p = self.params
        present = set(df.columns)
        dtypes = dict(df.dtypes)
        problems: List[str] = []

        for c in p.get("required_columns", []):
            if c not in present:
                problems.append(f"coluna obrigatoria ausente: '{c}'")
        for c in p.get("forbidden_columns", []):
            if c in present:
                problems.append(f"coluna proibida presente: '{c}'")
        for c, expected in (p.get("column_types") or {}).items():
            if c not in present:
                problems.append(f"coluna '{c}' ausente (tipo esperado {expected})")
            elif not _type_matches(dtypes.get(c, ""), expected):
                problems.append(f"coluna '{c}' tem tipo '{dtypes.get(c)}', esperado '{expected}'")

        if problems:
            return CheckResult("schema", False, "; ".join(problems), len(problems), check_name=self._name())
        return CheckResult("schema", True, "schema ok", check_name=self._name())
