"""Montagem do MERGE INTO, compartilhada por Delta e Iceberg.

Os dois escritores emitem o mesmo comando e diferem só no alvo e nas cláusulas
default, então a montagem mora aqui em vez de ser duplicada — do mesmo jeito que
`is_table_name` mora em `base.py`.

Há duas formas de escrever um merge no JSON, e elas não se misturam:

**Forma declarativa** (a de sempre): `merge_keys`, opcionalmente
`merge_condition`, `delete_when` e `delete_not_matched_by_source`. O framework
monta o `ON` e as cláusulas. Cobre o upsert normal sem que ninguém precise
escrever SQL.

**Forma explícita**: `on` (a condição inteira, como no `on` do join) e `actions`
(a lista de cláusulas `WHEN ...`, escritas à mão e emitidas na ordem dada). É a
saída para tudo que a forma declarativa não expressa — `UPDATE SET` parcial,
constante literal, coluna com nome diferente dos dois lados, várias cláusulas
condicionais. O framework não interpreta o conteúdo: valida a forma e interpola.

    "options": {
      "on": "S.id = T.id AND S.loja = T.loja",
      "actions": [
        "WHEN MATCHED AND S.op = 'D' THEN DELETE",
        "WHEN MATCHED THEN UPDATE SET T.nome = S.nome, T.atualizado_em = current_timestamp()",
        "WHEN NOT MATCHED THEN INSERT (id, loja, nome) VALUES (S.id, S.loja, S.nome)"
      ]
    }

`on` e `actions` são independentes: dá para escrever o `ON` à mão e deixar as
cláusulas no default, ou o contrário.

Sobre as duas exclusões da forma declarativa, que não são intercambiáveis:

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

import re
from typing import Any, Dict, List, Sequence, Tuple

#: Chaves de `options` que configuram o MERGE e não são opções do writer. Numa
#: escrita comum elas seriam repassadas ao Spark como opção desconhecida —
#: aceita em silêncio e sem efeito, que é a pior forma de errar.
MERGE_OPTIONS = (
    "merge_keys",
    "merge_condition",
    "delete_when",
    "delete_not_matched_by_source",
    "on",
    "actions",
)

#: Valores textuais aceitos como "sim" em `delete_not_matched_by_source`. Uma
#: string qualquer fora desta lista é tratada como condição SQL, não como flag.
_TRUE = {"true", "yes", "1"}
_FALSE = {"false", "no", "0", "", "none"}

#: Os três grupos de cláusula do MERGE INTO. Dentro de cada grupo o SQL padrão
#: exige que só a última possa ser incondicional — as anteriores precisam de
#: `AND <cond>`, senão nunca seriam alcançadas.
_GRUPOS = (
    ("NOT MATCHED BY SOURCE", re.compile(r"^WHEN\s+NOT\s+MATCHED\s+BY\s+SOURCE\b", re.I)),
    ("NOT MATCHED", re.compile(r"^WHEN\s+NOT\s+MATCHED\b", re.I)),
    ("MATCHED", re.compile(r"^WHEN\s+MATCHED\b", re.I)),
)

_CONDICIONAL = re.compile(r"^WHEN\s+(NOT\s+MATCHED(\s+BY\s+SOURCE)?|MATCHED)\s+AND\b", re.I)


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


def on_condition(options: Dict[str, Any], writer: str) -> str:
    """A condição do `ON`, escrita à mão em `on` ou montada de `merge_keys`.

    `writer` só entra na mensagem de erro, para dizer qual destino recusou.
    """
    bruto = options.get("on")
    if bruto is not None:
        if not isinstance(bruto, str) or not bruto.strip():
            raise ValueError(
                f"{writer} mode='merge': 'on' precisa ser uma condicao SQL sobre "
                f"T e S; veio {bruto!r}"
            )
        return bruto.strip()

    merge_keys = options.get("merge_keys") or []
    if not merge_keys:
        raise ValueError(
            f"{writer} mode='merge' requer 'merge_keys' em options, ou 'on' com a "
            "condicao inteira. Ex: {\"merge_keys\": [\"id\"]} ou "
            "{\"on\": \"T.id = S.id\"}"
        )

    condicao = " AND ".join(f"T.{k} = S.{k}" for k in merge_keys)
    extra = options.get("merge_condition", "")
    if extra:
        condicao = f"({condicao}) AND ({extra})"
    return condicao


def action_clauses(
    options: Dict[str, Any], defaults: Sequence[str], writer: str
) -> List[str]:
    """As cláusulas `WHEN ...`, escritas à mão em `actions` ou montadas.

    Com `actions`, a ordem da lista é a ordem emitida: em `MERGE INTO` a primeira
    cláusula que casa é a que vale, então mover o DELETE para depois do UPDATE
    muda o resultado. Sem `actions`, os deletes declarativos entram em volta das
    cláusulas default do writer.
    """
    brutas = options.get("actions")
    if brutas is None:
        matched_delete, by_source = delete_clauses(options)
        return [c for c in [matched_delete, *defaults, by_source] if c]

    if not isinstance(brutas, list) or not brutas:
        raise ValueError(
            f"{writer} mode='merge': 'actions' precisa ser uma lista nao vazia de "
            f"clausulas WHEN ...; veio {brutas!r}"
        )

    acoes: List[str] = []
    for item in brutas:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(
                f"{writer} mode='merge': cada item de 'actions' e uma string com "
                f"uma clausula WHEN ...; veio {item!r}"
            )
        acoes.append(" ".join(item.split()))

    _validar_acoes(acoes, writer)
    return acoes


def merge_sql(
    target: str,
    source_view: str,
    options: Dict[str, Any],
    defaults: Sequence[str],
    writer: str,
) -> str:
    """O comando `MERGE INTO` inteiro, pronto para `spark.sql`."""
    clausulas = "\n            ".join(action_clauses(options, defaults, writer))
    return f"""
            MERGE INTO {target} AS T
            USING {source_view} AS S
            ON {on_condition(options, writer)}
            {clausulas}
        """


def _validar_acoes(acoes: Sequence[str], writer: str) -> None:
    """Recusa o que o Spark recusaria, mas com o erro dizendo o que fazer.

    Duas regras: toda cláusula começa com `WHEN`, e dentro de cada grupo só a
    última pode ser incondicional. Uma incondicional no meio torna as seguintes
    do mesmo grupo inalcançáveis — é o erro fácil de cometer ao mover o DELETE
    para o fim da lista, onde ele parece mais legível e deixa de funcionar.
    """
    vistos: Dict[str, int] = {}
    for i, acao in enumerate(acoes):
        grupo = _grupo(acao)
        if grupo is None:
            raise ValueError(
                f"{writer} mode='merge': clausula de 'actions' precisa comecar com "
                f"WHEN MATCHED, WHEN NOT MATCHED ou WHEN NOT MATCHED BY SOURCE; "
                f"veio {acao!r}"
            )
        anterior = vistos.get(grupo)
        if anterior is not None:
            raise ValueError(
                f"{writer} mode='merge': a clausula {acao!r} vem depois de uma "
                f"'{grupo}' sem condicao (item {anterior + 1} de 'actions'), que "
                "sempre casa primeiro e a torna inalcancavel. Uma clausula sem AND "
                "tem de ser a ultima do seu grupo."
            )
        if not _CONDICIONAL.match(acao):
            vistos[grupo] = i


def _grupo(acao: str) -> str | None:
    for nome, padrao in _GRUPOS:
        if padrao.match(acao):
            return nome
    return None
