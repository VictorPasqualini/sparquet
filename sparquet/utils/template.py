from __future__ import annotations

import re
from typing import Any, Dict


#: `{chave}` sozinha. Os dois lookarounds excluem `{{chave}}`, que é a variável de
#: runtime das transformações e pertence ao TransformationEngine, não a `params`.
_PARAM = re.compile(r"(?<!\{)\{(\w+)\}(?!\})")


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
    """Substitui {chave} no texto bruto pelos valores formatados de params.

    Chaves sem correspondência em params ficam literais — não causam erro.

    Regras de formatação:
      bool True  → "true"       truthy — não dispara skip_if_false
      bool False → ""           falsy  — dispara skip_if_false
      list vazia → ""           falsy  — dispara skip_if_false ou filtro vazio
      list str   → "'a', 'b'"  pronto para IN (...) no SQL
      list num   → "1, 2"      pronto para IN (...) no SQL
      outros     → str(value)

    `{{nome}}`, a variável de runtime das transformações, **não** é tocada, mesmo
    quando `params` tem uma chave de mesmo nome: as chaves duplas em volta são o
    que distingue as duas sintaxes, e sem essa exclusão um param chamado como uma
    variável de runtime reescrevia a referência antes de o TransformationEngine
    vê-la — o `{{nome}}` virava `{valor}` e a variável sumia. Ver `_PARAM`.
    """

    def replace(m: re.Match) -> str:
        key = m.group(1)
        return _format_value(params[key]) if key in params else m.group(0)

    return _PARAM.sub(replace, raw)
