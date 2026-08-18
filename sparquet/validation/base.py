"""Compat: os tipos base de validação agora vivem em `sparquet_cola`.

O bloco JSON continua sendo `validations`; internamente o motor de qualidade de
dados é o **sparquet_cola** (biblioteca separável). Estes aliases preservam os nomes
históricos do framework (`BaseValidator`, `ValidationResult`) usados por validators
customizados via `register_validator`.
"""
from sparquet_cola.checks import BaseCheck, CheckResult

BaseValidator = BaseCheck
ValidationResult = CheckResult

__all__ = ["BaseValidator", "ValidationResult", "BaseCheck", "CheckResult"]
