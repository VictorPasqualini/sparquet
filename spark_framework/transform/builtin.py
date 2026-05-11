from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_framework.core.config import InputConfig
from spark_framework.transform.base import BaseTransformation


class FilterTransformation(BaseTransformation):
    """Keeps rows that satisfy a SQL-style boolean expression."""

    def apply(self, df: DataFrame) -> DataFrame:
        return df.filter(self.config.params["condition"])


class SelectTransformation(BaseTransformation):
    """Projects a subset of columns."""

    def apply(self, df: DataFrame) -> DataFrame:
        return df.select(*self.config.params["columns"])


class DropTransformation(BaseTransformation):
    """Removes columns from the DataFrame."""

    def apply(self, df: DataFrame) -> DataFrame:
        return df.drop(*self.config.params["columns"])


class RenameTransformation(BaseTransformation):
    """Renames columns using an old→new mapping."""

    def apply(self, df: DataFrame) -> DataFrame:
        result = df
        for old, new in self.config.params["mappings"].items():
            result = result.withColumnRenamed(old, new)
        return result


class CastTransformation(BaseTransformation):
    """Casts columns to new data types."""

    def apply(self, df: DataFrame) -> DataFrame:
        result = df
        for col_name, dtype in self.config.params["columns"].items():
            result = result.withColumn(col_name, F.col(col_name).cast(dtype))
        return result


class WithColumnTransformation(BaseTransformation):
    """Adds or replaces a column computed from a SQL expression.

    JSON: { "type": "with_column", "name": "col", "expression": "SQL expr" }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            self.config.params["name"],
            F.expr(self.config.params["expression"]),
        )


# Backward-compatible alias — use "with_column" in new configs
AddColumnTransformation = WithColumnTransformation


class DropDuplicatesTransformation(BaseTransformation):
    """Removes duplicate rows, optionally scoped to specific columns."""

    def apply(self, df: DataFrame) -> DataFrame:
        columns: list[str] | None = self.config.params.get("columns")
        return df.dropDuplicates(columns) if columns else df.dropDuplicates()


class SqlTransformation(BaseTransformation):
    """Runs an arbitrary SQL query against the DataFrame exposed as a temp view."""

    def apply(self, df: DataFrame) -> DataFrame:
        view = self.config.params.get("view_name", "_df")
        df.createOrReplaceTempView(view)
        return df.sparkSession.sql(self.config.params["query"])


class FillNaTransformation(BaseTransformation):
    """Fills null values with a constant or per-column mapping."""

    def apply(self, df: DataFrame) -> DataFrame:
        value = self.config.params["value"]
        columns: list[str] | None = self.config.params.get("columns")
        return df.fillna(value, subset=columns)


class SortTransformation(BaseTransformation):
    """Orders the DataFrame by one or more columns."""

    def apply(self, df: DataFrame) -> DataFrame:
        columns: list[str] = self.config.params["columns"]
        ascending = self.config.params.get("ascending", True)

        if isinstance(ascending, list):
            order_cols = [
                F.col(c).asc() if asc else F.col(c).desc()
                for c, asc in zip(columns, ascending)
            ]
        else:
            order_cols = [
                F.col(c).asc() if ascending else F.col(c).desc()
                for c in columns
            ]
        return df.orderBy(*order_cols)


_AGG_FUNCTIONS = {
    "min":            F.min,
    "max":            F.max,
    "sum":            F.sum,
    "avg":            F.avg,
    "mean":           F.avg,
    "count":          F.count,
    "first":          F.first,
    "last":           F.last,
    "count_distinct": F.countDistinct,
    "collect_list":   F.collect_list,
    "collect_set":    F.collect_set,
}


class GroupByTransformation(BaseTransformation):
    """Groups the DataFrame and applies aggregations without raw SQL.

    JSON params:
      by  – list of columns to group by
      agg – list of aggregation specs, each with:
              func   – aggregation function name (see supported list below)
              column – column to aggregate (optional for count)
              alias  – output column name (optional; defaults to Spark naming)

    Supported func values:
      min, max, sum, avg/mean, count, first, last,
      count_distinct, collect_list, collect_set

    Example:
      { "type": "group_by",
        "by": ["id", "categoria"],
        "agg": [
          { "func": "sum",   "column": "valor",  "alias": "total"  },
          { "func": "min",   "column": "status", "alias": "status" },
          { "func": "count",                     "alias": "n"      }
        ] }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        by: list[str] = self.config.params["by"]
        agg_specs: list[dict] = self.config.params["agg"]

        agg_exprs = []
        for spec in agg_specs:
            func_name = spec["func"].lower()
            func = _AGG_FUNCTIONS.get(func_name)
            if func is None:
                raise ValueError(
                    f"Função de agregação '{func_name}' não suportada. "
                    f"Disponíveis: {sorted(_AGG_FUNCTIONS)}"
                )
            col = spec.get("column")
            alias = spec.get("alias")

            expr = func(F.col(col)) if col else func("*")
            if alias:
                expr = expr.alias(alias)
            agg_exprs.append(expr)

        return df.groupBy(*by).agg(*agg_exprs)


_VALID_JOIN_TYPES = {
    "inner", "cross",
    "outer", "full", "fullouter", "full_outer",
    "left", "leftouter", "left_outer",
    "right", "rightouter", "right_outer",
    "semi", "leftsemi", "left_semi",
    "anti", "leftanti", "left_anti",
}


class JoinTransformation(BaseTransformation):
    """Joins the main DataFrame with a second source.

    Supports all Spark join types: inner, cross, left, right, full/outer,
    semi/leftsemi, anti/leftanti and their underscore variants.

    The left DataFrame is aliased as 'l' and the right as 'r', so SQL
    expressions in 'on' and in downstream transformations can use l.campo
    and r.campo to disambiguate columns.

    JSON params:
      with                 – source config (format + path + options)
      on                   – column name, list of column names, or SQL expression
                             (SQL expressions containing spaces use l./r. aliases)
      how                  – join type (default: inner)
      with_transformations – list of transformations applied to the right-side
                             DataFrame before the join (all builtin types supported)

    Examples:
      { "type": "join",
        "with": { "format": "parquet", "path": "/ref/products" },
        "on": "product_id",
        "how": "leftanti" }

      { "type": "join",
        "with": { "format": "delta", "path": "catalog.schema.contratos" },
        "with_transformations": [
          { "type": "filter", "condition": "status = 1" },
          { "type": "select", "columns": ["id", "nome"] }
        ],
        "on": "id",
        "how": "inner" }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        from spark_framework.io.factory import ReaderFactory

        source_cfg = InputConfig.from_dict(self.config.params["with"])
        other = ReaderFactory.create(df.sparkSession, source_cfg).read()

        raw_transforms = self.config.params.get("with_transformations", [])
        if raw_transforms:
            from spark_framework.core.config import TransformationConfig
            from spark_framework.transform.engine import TransformationEngine
            engine = TransformationEngine()
            cfgs = [TransformationConfig.from_dict(t) for t in raw_transforms]
            other = engine.apply(other, cfgs)

        on = self.config.params["on"]
        how: str = self.config.params.get("how", "inner").lower()

        if how not in _VALID_JOIN_TYPES:
            raise ValueError(
                f"Tipo de join '{how}' invalido. "
                f"Opcoes: {sorted(_VALID_JOIN_TYPES)}"
            )

        left = df.alias("l")
        right = other.alias("r")

        # String with spaces → SQL expression (supports l.campo / r.campo)
        if isinstance(on, str) and " " in on:
            return left.join(right, F.expr(on), how)
        return left.join(right, on, how)


class DebugTransformation(BaseTransformation):
    """Executes inspection actions on the DataFrame without modifying it.

    JSON params:
      actions    – list of actions to run (default: ["show", "print_schema"])
      label      – optional label shown in the separator line
      show_rows  – rows for show (default: 20)
      truncate   – truncate show output (default: true)
      vertical   – vertical layout for show (default: false)
      extended   – extended plan for explain (default: false)

    Supported actions:
      show, print_schema, count, explain, columns, dtypes

    Example:
      { "type": "debug", "label": "após join contratos", "actions": ["count", "print_schema", "show"] }
      { "type": "debug", "actions": ["show"], "show_rows": 5, "truncate": false }
    """

    _SUPPORTED_ACTIONS = {"show", "print_schema", "count", "explain", "columns", "dtypes"}

    def apply(self, df: DataFrame) -> DataFrame:
        params = self.config.params
        label = params.get("label", "")
        actions: list[str] = params.get("actions", ["show", "print_schema"])

        header = f"[DEBUG{f' — {label}' if label else ''}]"
        print(f"\n{'─' * 60}\n{header}\n{'─' * 60}")

        for raw in actions:
            action = raw.lower().replace("-", "_").replace("printschema", "print_schema")
            if action == "show":
                df.show(
                    n=params.get("show_rows", 20),
                    truncate=params.get("truncate", True),
                    vertical=params.get("vertical", False),
                )
            elif action == "print_schema":
                df.printSchema()
            elif action == "count":
                print(f"count: {df.count()}")
            elif action == "explain":
                df.explain(extended=params.get("extended", False))
            elif action == "columns":
                print(f"columns: {df.columns}")
            elif action == "dtypes":
                print(f"dtypes: {df.dtypes}")
            else:
                print(
                    f"⚠️  ação desconhecida: '{raw}'. "
                    f"Disponíveis: {sorted(self._SUPPORTED_ACTIONS)}"
                )

        print(f"{'─' * 60}\n")
        return df


class UnionTransformation(BaseTransformation):
    """Appends rows from a second source to the main DataFrame.

    JSON params:
      with                  – source config (format + path + options)
      allow_missing_columns – fill missing columns with null (default: false)

    Example:
      { "type": "union",
        "with": { "format": "parquet", "path": "/data/extra_orders" },
        "allow_missing_columns": true }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        from spark_framework.io.factory import ReaderFactory

        source_cfg = InputConfig.from_dict(self.config.params["with"])
        other = ReaderFactory.create(df.sparkSession, source_cfg).read()

        if self.config.params.get("allow_missing_columns", False):
            return df.unionByName(other, allowMissingColumns=True)
        return df.union(other)
