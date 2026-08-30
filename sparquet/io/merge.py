"""Cláusulas de exclusão do MERGE INTO, compartilhadas por Delta e Iceberg.

Os dois escritores montam o mesmo comando SQL padrão e diferem só no alvo, então
a parte que decide *quando apagar* mora aqui em vez de ser duplicada — do mesmo
jeito que `is_table_name` mora em `base.py`.

São dois casos de exclusão, e eles não são intercambiáveis:

  `delete_when`  — a origem **traz** a linha, marcada como excluída (`op = 'D'`,
                   `deleted = true`, o que o CDC do sistema de origem emitir).
                   Vira `WHEN MATCHED AND <cond> THEN DELETE`, avaliado antes do
                   UPDATE porque a primeira cláusula que casa é a que vale: sem
                   essa ordem a linha seria atualizada e nunca apagada.

  `delete_not_matched_by_source`
                 — a origem **não traz** a linha, e isso é o que significa que ela
                   sumiu. Vira `WHEN NOT MATCHED BY SOURCE THEN DELETE`, opcional-
                   mente com uma condição sobre `T`. Só é correto quando a origem é
                   um snapshot **completo** do conjunto: contra uma carga
                   incremental, apaga tudo o que aquela carga não repetiu.

Nenhuma das duas é ligada por default — um MERGE sem elas continua exatamente o
que era, `UPDATE` mais `INSERT`.
"""
from __future__ import annotations

from typing import Any, Dict, Tuple

#: Valores textuais aceitos como "sim" em `delete_not_matched_by_source`. Uma
#: string qualquer fora desta lista é tratada como condição SQL, não como flag.
_TRUE = {"true", "yes", "1"}
_FALSE = {"false", "no", "0", "", "none"}


def delete_clauses(options: Dict[str, Any]) -> Tuple[str, str]:
    """Devolve `(cláusula WHEN MATCHED ... DELETE, cláusula NOT MATCHED BY SOURCE)`.

    Cada uma vem pronta para ser interpolada no comando, ou vazia quando a opção
    correspondente não foi pedida. A condição é injetada crua: é SQL escrito por
    quem monta o pipeline, como `merge_condition` já era.
    """
    matched = ""
    delete_when = options.get("delete_when")
    if isinstance(delete_when, str) and delete_when.strip():
        matched = f"WHEN MATCHED AND ({delete_when.strip()}) THEN DELETE"

    by_source = ""
    raw = options.get("delete_not_matched_by_source")
    if raw is True:
        by_source = "WHEN NOT MATCHED BY SOURCE THEN DELETE"
    elif isinstance(raw, str):
        valor = raw.strip()
        if valor.lower() in _TRUE:
            by_source = "WHEN NOT MATCHED BY SOURCE THEN DELETE"
        elif valor and valor.lower() not in _FALSE:
            by_source = f"WHEN NOT MATCHED BY SOURCE AND ({valor}) THEN DELETE"
    elif raw not in (None, False):
        raise ValueError(
            "'delete_not_matched_by_source' aceita true/false ou uma condicao SQL "
            f"sobre T; veio {raw!r}"
        )

    return matched, by_source
