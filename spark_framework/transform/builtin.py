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


class AddColumnTransformation(BaseTransformation):
    """Adds a new column computed from a SQL expression."""

    def apply(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            self.config.params["name"],
            F.expr(self.config.params["expression"]),
        )


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


class WithTimestampTransformation(BaseTransformation):
    """Appends an ingestion timestamp column."""

    def apply(self, df: DataFrame) -> DataFrame:
        col_name = self.config.params.get("column_name", "ingestion_timestamp")
        return df.withColumn(col_name, F.current_timestamp())


class JoinTransformation(BaseTransformation):
    """Joins the main DataFrame with a second source.

    JSON params:
      with   – source config (format + path + options)
      on     – column name (str) or list of column names
      how    – join type: inner | left | right | full (default: inner)

    Example:
      { "type": "join",
        "with": { "format": "parquet", "path": "/ref/products" },
        "on": "product_id",
        "how": "left" }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        from spark_framework.io.factory import ReaderFactory

        source_cfg = InputConfig.from_dict(self.config.params["with"])
        other = ReaderFactory.create(df.sparkSession, source_cfg).read()

        on = self.config.params["on"]
        how: str = self.config.params.get("how", "inner")
        return df.join(other, on, how)


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
