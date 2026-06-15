from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_framework.core.config import InputConfig
from spark_framework.transform.base import BaseTransformation
from spark_framework.utils.logger import defer_warning


class FilterTransformation(BaseTransformation):
    """Keeps rows that satisfy a SQL-style boolean expression."""

    def apply(self, df: DataFrame) -> DataFrame:
        return df.filter(self.config.params["condition"])


class SelectTransformation(BaseTransformation):
    """Projects a subset of columns or SQL expressions.

    Cada item pode ser um nome de coluna simples ou uma expressão SQL completa
    com alias, ex: "to_json(payload) AS value", "CAST(id AS STRING) AS id_str".
    """

    def apply(self, df: DataFrame) -> DataFrame:
        return df.select(*[F.expr(c) for c in self.config.params["columns"]])


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
    

class DistinctTransformation(BaseTransformation):
    """Removes duplicate rows, using all columns."""

    def apply(self, df: DataFrame) -> DataFrame:
        return df.distinct()


class CheckpointTransformation(BaseTransformation):
    """Materializa o DataFrame e trunca seu plano lógico (checkpoint).

    Equivale aos checkpoint()/localCheckpoint() usados em jobs Spark longos para
    quebrar a linhagem após joins pesados — mantém o planner rápido e evita
    recomputar estágios anteriores a cada ação. Não altera os dados.

    JSON params:
      method – qual método Spark chamar (default: "localCheckpoint")
                 "localCheckpoint" → df.localCheckpoint() — grava no disco local
                                     dos executors; rápido, mas perdido se um
                                     executor morre (recomputa). É o usado no job.
                 "checkpoint"      → df.checkpoint() — grava em storage confiável;
                                     requer spark.sparkContext.setCheckpointDir.
      eager  – materializa imediatamente (default: true)

    Se `method` vier com valor inválido, a transformação é ignorada (o df segue
    intacto para as próximas etapas) e um warning é emitido no fim do pipeline.

    Exemplo:
      { "type": "checkpoint" }                              // localCheckpoint(eager=True)
      { "type": "checkpoint", "method": "checkpoint" }      // checkpoint confiável
      { "type": "checkpoint", "eager": false }
    """

    _METHODS = {"localcheckpoint", "checkpoint"}

    def apply(self, df: DataFrame) -> DataFrame:
        eager = self.config.params.get("eager", True)
        method = self.config.params.get("method", "localCheckpoint")
        normalized = method.lower() if isinstance(method, str) else None

        if normalized not in self._METHODS:
            defer_warning(
                "checkpoint ignorado: method inválido",
                method=method,
                opcoes=["localCheckpoint", "checkpoint"],
            )
            return df

        if normalized == "checkpoint":
            return df.checkpoint(eager=eager)
        return df.localCheckpoint(eager=eager)


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


class GroupByTransformation(BaseTransformation):
    """Groups the DataFrame and applies SQL aggregation expressions.

    JSON params:
      by    – lista de colunas para agrupar
      agg   – lista de expressões SQL de agregação completas (strings). Cada item
              é passado direto para F.expr(), então qualquer função/sintaxe SQL do
              Spark é válida, incluindo alias e expressões compostas.
      pivot – (opcional) pivota os grupos por uma coluna. Aceita:
                "coluna"                                   → pivot simples
                { "column": "coluna", "values": [...] }    → pivot com valores
                  explícitos (mais eficiente, evita um scan extra do Spark)

    Example:
      { "type": "group_by",
        "by": ["id", "categoria"],
        "agg": [
          "sum(valor) as total",
          "min(status) as status",
          "count(*) as n",
          "first(tipo_contrato) as tipo_contrato",
          "count(distinct struct(tipo_ativo, registradora)) > 1 as multi_ativos"
        ] }

      { "type": "group_by",
        "by": ["id"],
        "pivot": { "column": "mes", "values": ["jan", "fev", "mar"] },
        "agg": ["sum(valor) as total"] }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        by: list[str] = self.config.params["by"]
        agg_specs: list[str] = self.config.params["agg"]
        agg_exprs = [F.expr(spec) for spec in agg_specs]

        grouped = df.groupBy(*by)

        pivot = self.config.params.get("pivot")
        if pivot is not None:
            if isinstance(pivot, dict):
                values = pivot.get("values")
                grouped = (
                    grouped.pivot(pivot["column"], values)
                    if values
                    else grouped.pivot(pivot["column"])
                )
            else:
                grouped = grouped.pivot(pivot)

        return grouped.agg(*agg_exprs)


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

    The "show" action always uses df.show(). Databricks' display() is a notebook
    widget and only works reliably when called directly in a cell — from an
    imported module it emits DataFrame.__repr__() (the schema string) to stdout
    instead of the actual rows.

    JSON params:
      actions    – list of actions to run (default: ["show", "print_schema"])
      label      – optional label shown in the separator line
      show_rows  – rows for show/display (default: 20)
      truncate   – truncate show output (default: true)
      vertical   – vertical layout for show (default: false)
      extended   – extended plan for explain (default: false)

    Supported actions:
      show, print_schema, count, explain, columns, dtypes

    Example:
      { "type": "debug", "label": "após join", "actions": ["count", "print_schema", "show"] }
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
