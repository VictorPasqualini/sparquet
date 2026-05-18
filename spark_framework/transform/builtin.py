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
    """Projects columns. Cada item é uma string Spark SQL.

    Suporta tanto nomes simples quanto expressões com alias (sintaxe SQL nativa):
      - "id"                              → coluna 'id' (inalterada)
      - "antigo as novo"                  → renomeia 'antigo' para 'novo'
      - "a * b as produto"                → cria coluna computada 'produto'
      - "current_timestamp() as ts"       → função sem args
      - "lpad(doc, 14, '0') as doc_pad"   → expressão arbitrária

    Internamente cada item passa por F.expr(), que aceita qualquer expressão
    Spark SQL válida — incluindo o alias via 'as'.

    Ex:
      { "type": "select",
        "columns": [
          "id_operacao",
          "id_cessao",
          "current_timestamp() as data_envio",
          "lpad(regexp_replace(documento, '\\\\D', ''), 14, '0') as documento_norm"
        ] }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        items = self.config.params["columns"]
        exprs = []
        for item in items:
            if isinstance(item, str):
                exprs.append(F.expr(item))
            elif isinstance(item, dict):
                # Backward-compat: {name, expression}
                exprs.append(F.expr(item["expression"]).alias(item["name"]))
            else:
                raise ValueError(
                    f"select columns item invalido: {item!r}. "
                    f"Use string SQL (com 'as nome' para alias)."
                )
        return df.select(*exprs)


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


class WithColumnsTransformation(BaseTransformation):
    """Adds or replaces multiple columns in a single batch transformation.

    Mais legível que N with_column quando há várias regras correlatas
    (ex: regras 'api_*' no CCB). Cada coluna é uma SQL expression e pode
    referenciar colunas criadas em entradas anteriores da MESMA lista
    (Spark aplica em ordem).

    JSON:
      { "type": "with_columns",
        "columns": [
          { "name": "tipo_pessoa_checked",
            "expression": "CASE WHEN tipo_pessoa IS NOT NULL AND tipo_pessoa != '' THEN tipo_pessoa ELSE ... END" },
          { "name": "documento_checked",
            "expression": "CASE WHEN tipo_pessoa_checked = 1 THEN lpad(documento, 11, '0') ELSE lpad(documento, 14, '0') END" }
        ] }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        cols: list[dict] = self.config.params["columns"]
        result = df
        for spec in cols:
            name = spec["name"]
            expression = spec["expression"]
            result = result.withColumn(name, F.expr(expression))
        return result


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
              func       – aggregation function name (see supported list below)
              column     – column to aggregate (optional for count)
              expression – arbitrary Spark SQL expression (only when func='expr')
              alias      – output column name (optional; defaults to Spark naming)

    Supported func values:
      min, max, sum, avg/mean, count, first, last,
      count_distinct, collect_list, collect_set,
      expr — para agregações arbitrárias via Spark SQL

    Example:
      { "type": "group_by",
        "by": ["id", "categoria"],
        "agg": [
          { "func": "sum",   "column": "valor",  "alias": "total"  },
          { "func": "min",   "column": "status", "alias": "status" },
          { "func": "count",                     "alias": "n"      },
          { "func": "expr",
            "expression": "count(distinct struct(tipo_ativo, registradora)) > 1",
            "alias": "multi_ativos" }
        ] }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        by: list[str] = self.config.params["by"]
        agg_specs: list[dict] = self.config.params["agg"]

        agg_exprs = []
        for spec in agg_specs:
            func_name = spec["func"].lower()
            alias = spec.get("alias")

            if func_name == "expr":
                expression = spec.get("expression")
                if not expression:
                    raise ValueError(
                        "agg func='expr' exige campo 'expression' com SQL Spark"
                    )
                expr = F.expr(expression)
                if alias:
                    expr = expr.alias(alias)
                agg_exprs.append(expr)
                continue

            func = _AGG_FUNCTIONS.get(func_name)
            if func is None:
                raise ValueError(
                    f"Função de agregação '{func_name}' não suportada. "
                    f"Disponíveis: {sorted(list(_AGG_FUNCTIONS) + ['expr'])}"
                )
            col = spec.get("column")

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
      with                 – source config (format + path + options) OR the
                             string "self" para reusar o df corrente como
                             right-side. **CUIDADO**: self-join causa shuffle
                             em ambos os lados pela join key — caro em datasets
                             grandes. Prefira window functions com with_column
                             quando o objetivo é apenas criar coluna agregada:
                               size(collect_set(struct(a, b)) over (partition by k)) > 1
                             em vez de:
                               self-join + group_by(expr count distinct struct...)
                             O self-join é útil quando você precisa de filtros
                             cruzados que window não cobre (ex: anti-join contra
                             versão filtrada do mesmo df).
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

      // Self-join — use com cautela (window é mais barato)
      { "type": "join",
        "with": "self",
        "with_transformations": [
          { "type": "select", "columns": ["id", "categoria"] },
          { "type": "filter", "condition": "categoria = 'A'" }
        ],
        "on":  ["id"],
        "how": "leftanti" }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        with_param = self.config.params["with"]

        if isinstance(with_param, str) and with_param.lower() == "self":
            # Self-join: usa o próprio df como base do right-side.
            # with_transformations é obrigatório aqui (sem elas o join seria
            # com o df idêntico, gerando colisões massivas).
            other = df
        else:
            from spark_framework.io.factory import ReaderFactory
            source_cfg = InputConfig.from_dict(with_param)
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


class CheckpointTransformation(BaseTransformation):
    """Materializa o DataFrame e QUEBRA a lineage (localCheckpoint).

    Pode ser usado em qualquer ponto da lista de transformations — por
    exemplo, logo após um join pesado para evitar que o Catalyst tenha que
    re-otimizar uma DAG enorme em transformações subsequentes.

    Suporta também `eager` (default true — força execução imediata).

    JSON:
      { "type": "checkpoint" }
      { "type": "checkpoint", "eager": false }   // checkpoint lazy

    Nota: requer setCheckpointDir configurado na SparkSession (Spark faz
    automaticamente para localCheckpoint em disco local, sem precisar).
    """

    def apply(self, df: DataFrame) -> DataFrame:
        eager = bool(self.config.params.get("eager", True))
        return df.localCheckpoint(eager=eager)


class CacheTransformation(BaseTransformation):
    """Cache o DataFrame em memória (e disco se necessário) — MANTÉM a lineage.

    Útil quando o df será reusado várias vezes nas transformações seguintes
    OU em outputs múltiplos. Diferente de checkpoint: a lineage NÃO é quebrada,
    apenas o resultado é cacheado.

    Materializa imediatamente via df.count() para evitar lazy cache.

    JSON:
      { "type": "cache" }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        df.cache()
        df.count()  # força materialização
        return df
