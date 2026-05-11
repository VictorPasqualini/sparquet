from __future__ import annotations

import re
from typing import Any, Dict


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else ""
    if isinstance(value, list):
        if not value:
            return ""
        if isinstance(value[0], str):
            return ", ".join(f"'{v}'" for v in value)
        return ", ".join(str(v) for v in value)
    return str(value)


def apply_template(raw: str, params: Dict[str, Any]) -> str:
    """Substitui {chave} no texto pelos valores formatados de params.

    Regras de formatação:
      bool True  → "true"          (truthy — não dispara skip_if_false)
      bool False → ""              (falsy  — dispara skip_if_false)
      list vazia → ""              (falsy  — dispara skip_if_false)
      list str   → "'a', 'b'"     (pronto para IN (...) no SQL)
      list num   → "1, 2"         (pronto para IN (...) no SQL)
      outros     → str(value)
    """

    def replace(m: re.Match) -> str:
        key = m.group(1)
        return _format_value(params[key]) if key in params else m.group(0)

    return re.sub(r"\{(\w+)\}", replace, raw)
