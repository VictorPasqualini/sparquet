from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparquet.validation.base import BaseValidator, ValidationResult


class NotNullValidator(BaseValidator):
    """Ensures that the specified columns contain no null values."""

    def validate(self, df: DataFrame) -> ValidationResult:
        columns: list[str] = self.rule.params["columns"]
        violations: dict[str, int] = {}

        for col in columns:
            count = df.filter(F.col(col).isNull()).count()
            if count > 0:
                violations[col] = count

        if violations:
            return ValidationResult(
                rule_type="not_null",
                passed=False,
                message=f"Null values found in columns: {violations}",
                failed_count=sum(violations.values()),
            )
        return ValidationResult(rule_type="not_null", passed=True)


class UniqueValidator(BaseValidator):
    """Ensures that the combination of specified columns is unique."""

    def validate(self, df: DataFrame) -> ValidationResult:
        columns: list[str] = self.rule.params["columns"]
        total = df.count()
        distinct = df.select(*columns).distinct().count()
        duplicates = total - distinct

        if duplicates > 0:
            return ValidationResult(
                rule_type="unique",
                passed=False,
                message=f"Found {duplicates} duplicate rows for columns {columns}",
                failed_count=duplicates,
            )
        return ValidationResult(rule_type="unique", passed=True)


class RangeValidator(BaseValidator):
    """Ensures numeric column values fall within [min, max]."""

    def validate(self, df: DataFrame) -> ValidationResult:
        column: str = self.rule.params["column"]
        min_val = self.rule.params.get("min")
        max_val = self.rule.params.get("max")

        filters = []
        if min_val is not None:
            filters.append(F.col(column) < min_val)
        if max_val is not None:
            filters.append(F.col(column) > max_val)

        if not filters:
            return ValidationResult(rule_type="range", passed=True)

        condition = filters[0]
        for f in filters[1:]:
            condition = condition | f

        failed = df.filter(condition).count()
        if failed > 0:
            return ValidationResult(
                rule_type="range",
                passed=False,
                message=(
                    f"Column '{column}' has {failed} values "
                    f"outside range [{min_val}, {max_val}]"
                ),
                failed_count=failed,
            )
        return ValidationResult(rule_type="range", passed=True)


class RegexValidator(BaseValidator):
    """Ensures all values in a string column match a given regex pattern."""

    def validate(self, df: DataFrame) -> ValidationResult:
        column: str = self.rule.params["column"]
        pattern: str = self.rule.params["pattern"]

        failed = df.filter(
            ~F.col(column).rlike(pattern) | F.col(column).isNull()
        ).count()

        if failed > 0:
            return ValidationResult(
                rule_type="regex",
                passed=False,
                message=(
                    f"Column '{column}' has {failed} values "
                    f"not matching pattern '{pattern}'"
                ),
                failed_count=failed,
            )
        return ValidationResult(rule_type="regex", passed=True)


class RowCountValidator(BaseValidator):
    """Ensures the DataFrame row count is within an expected range."""

    def validate(self, df: DataFrame) -> ValidationResult:
        min_count: int = self.rule.params.get("min", 0)
        max_count: int | None = self.rule.params.get("max")
        count = df.count()

        too_few = count < min_count
        too_many = max_count is not None and count > max_count

        if too_few or too_many:
            return ValidationResult(
                rule_type="row_count",
                passed=False,
                message=(
                    f"Row count {count} is outside expected range "
                    f"[{min_count}, {max_count}]"
                ),
                failed_count=1,
            )
        return ValidationResult(rule_type="row_count", passed=True)


class CustomSqlValidator(BaseValidator):
    """Runs a SQL query that must return a single truthy value to pass.

    The DataFrame is exposed as the temp view '_validation_df'.
    Example query: "SELECT COUNT(*) = 0 FROM _validation_df WHERE amount < 0"
    """

    def validate(self, df: DataFrame) -> ValidationResult:
        df.createOrReplaceTempView("_validation_df")
        result = df.sparkSession.sql(self.rule.params["query"])
        passed = bool(result.collect()[0][0])

        if not passed:
            return ValidationResult(
                rule_type="custom_sql",
                passed=False,
                message=self.rule.params.get(
                    "error_message", "Custom SQL validation failed"
                ),
                failed_count=1,
            )
        return ValidationResult(rule_type="custom_sql", passed=True)
