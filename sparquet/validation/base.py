from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyspark.sql import DataFrame

from sparquet.core.config import ValidationRule


@dataclass
class ValidationResult:
    rule_type: str
    passed: bool
    message: str = ""
    failed_count: int = 0

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.rule_type}: {self.message or 'OK'}"


class BaseValidator(ABC):
    def __init__(self, rule: ValidationRule) -> None:
        self.rule = rule

    @abstractmethod
    def validate(self, df: DataFrame) -> ValidationResult:
        ...
