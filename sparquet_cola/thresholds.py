"""DSL de thresholds no estilo SODA Core (parte do sparquet_cola).

Um threshold é uma condição sobre um valor numérico de métrica, escrita como texto:

    "> 0"                 "< 5"          ">= 100"        "<= 10"
    "= 0"   "!= 0"        "between 10 and 20"            "not between 1 and 2"

Sufixos aceitos no número:
    "5%"   → percentual (o '%' é apenas cosmético; o valor é 5)
    "1d" "2h" "30m" "45s" "1w" → duração convertida para SEGUNDOS (para freshness)

O threshold é a **condição de aprovação**: se `satisfies(valor)` for False, a métrica
violou o limite.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
_NUM = r"[-+]?\d+(?:\.\d+)?"
_DURATION_RE = re.compile(rf"^({_NUM})\s*([smhdw])$", re.IGNORECASE)
_OP_RE = re.compile(r"^(>=|<=|<>|!=|==|=|>|<)\s*(.+)$")
_BETWEEN_RE = re.compile(r"^(not\s+between|between)\s+(.+?)\s+and\s+(.+)$", re.IGNORECASE)


def parse_number(token: str) -> float:
    """Converte um token numérico com sufixo opcional (%, duração) em float."""
    token = token.strip()
    m = _DURATION_RE.match(token)
    if m:
        return float(m.group(1)) * _DURATION_UNITS[m.group(2).lower()]
    token = token.rstrip("%").strip()
    return float(token)


@dataclass
class Threshold:
    raw: str
    op: str            # ">" "<" ">=" "<=" "=" "!=" "between" "not_between"
    a: float
    b: float = 0.0

    @classmethod
    def parse(cls, expr: str) -> "Threshold":
        if not isinstance(expr, str) or not expr.strip():
            raise ValueError("threshold vazio — ex: '> 0', 'between 10 and 20', '< 5%'")
        s = expr.strip()

        m = _BETWEEN_RE.match(s)
        if m:
            op = "not_between" if m.group(1).lower().startswith("not") else "between"
            return cls(expr, op, parse_number(m.group(2)), parse_number(m.group(3)))

        m = _OP_RE.match(s)
        if m:
            op = {"==": "=", "<>": "!="}.get(m.group(1), m.group(1))
            return cls(expr, op, parse_number(m.group(2)))

        # número solo → igualdade (ex: "row_count" com must_be "0")
        return cls(expr, "=", parse_number(s))

    def satisfies(self, value: float) -> bool:
        if value is None:
            return False
        op = self.op
        if op == "between":
            return self.a <= value <= self.b
        if op == "not_between":
            return not (self.a <= value <= self.b)
        if op == ">":
            return value > self.a
        if op == "<":
            return value < self.a
        if op == ">=":
            return value >= self.a
        if op == "<=":
            return value <= self.a
        if op == "=":
            return value == self.a
        if op == "!=":
            return value != self.a
        raise ValueError(f"operador de threshold desconhecido: {op}")

    def describe(self) -> str:
        return self.raw.strip()
