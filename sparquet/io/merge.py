"""Montagem do MERGE INTO, compartilhada por Delta e Iceberg.

Os dois escritores emitem o mesmo comando e diferem só no alvo, então a montagem
mora aqui em vez de ser duplicada — do mesmo jeito que `is_table_name` mora em
`base.py`.

Há **uma** forma de escrever um merge no JSON: `on` recebe a condição inteira
(como o `on` do join) e `actions` a lista de cláusulas `WHEN ...`, escritas à mão
e emitidas na ordem dada. As duas são obrigatórias. O framework não interpreta o
conteúdo: valida a forma e interpola.

    "options": {
      "on": "S.id = T.id AND S.loja = T.loja",
      "actions": [
        "WHEN MATCHED AND S.op = 'D' THEN DELETE",
        "WHEN MATCHED THEN UPDATE SET T.nome = S.nome, T.atualizado_em = current_timestamp()",
        "WHEN NOT MATCHED THEN INSERT (id, loja, nome) VALUES (S.id, S.loja, S.nome)"
      ]
    }

O upsert de sempre são duas linhas — `UPDATE SET *` e `INSERT *` casam as colunas
por nome:

    "actions": [
      "WHEN MATCHED THEN UPDATE SET *",
      "WHEN NOT MATCHED THEN INSERT *"
    ]

No Iceberg isso vale sempre. No Delta o `*` exige que os dois lados tenham as
mesmas colunas: uma origem de CDC que traz `op`, coluna que o destino não tem,
falha ao resolvê-la — aí as colunas do destino entram listadas à mão.

A ordem das cláusulas é a ordem do comando, e em `MERGE INTO` a primeira que casa
é a que vale. Um DELETE condicional escrito depois de um UPDATE incondicional
nunca é alcançado — a linha seria atualizada e nunca apagada. Este módulo recusa
esse caso na montagem, antes de o Spark rodar.

As duas exclusões que antes tinham opção própria continuam existindo como
cláusula, e não são intercambiáveis:

  `WHEN MATCHED AND <cond> THEN DELETE`
                 — a origem **traz** a linha, marcada como excluída (`op = 'D'`,
                   `deleted = true`, o que o CDC do sistema de origem emitir).
                   Precisa vir antes do UPDATE.

  `WHEN NOT MATCHED BY SOURCE THEN DELETE`
                 — a origem **não traz** a linha, e isso é o que significa que
                   ela sumiu. Só é correto quando a origem é um snapshot
                   **completo** do conjunto: contra uma carga incremental, apaga
                   tudo o que aquela carga não repetiu.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Sequence

#: Chaves de `options` que configuram o MERGE e não são opções do writer. Numa
#: escrita comum elas seriam repassadas ao Spark como opção desconhecida —
#: aceita em silêncio e sem efeito, que é a pior forma de errar.
MERGE_OPTIONS = ("on", "actions")

#: As opções da forma declarativa antiga, e o que escrever no lugar de cada uma.
#: Ignorá-las em silêncio seria pior do que recusar: o merge rodaria sem a chave
#: e sem a condição de exclusão que quem escreveu o JSON pediu.
_REMOVIDAS = {
    "merge_keys": "'on' com a condicao inteira. Ex: \"on\": \"T.id = S.id\"",
    "merge_condition": "'on', que ja recebe a condicao inteira",
    "delete_when": (
        "uma clausula em 'actions': \"WHEN MATCHED AND <cond> THEN DELETE\", "
        "escrita ANTES do UPDATE"
    ),
    "delete_not_matched_by_source": (
        "uma clausula em 'actions': \"WHEN NOT MATCHED BY SOURCE THEN DELETE\""
    ),
}

#: Os três grupos de cláusula do MERGE INTO. Dentro de cada grupo o SQL padrão
#: exige que só a última possa ser incondicional — as anteriores precisam de
#: `AND <cond>`, senão nunca seriam alcançadas.
_GRUPOS = (
    ("NOT MATCHED BY SOURCE", re.compile(r"^WHEN\s+NOT\s+MATCHED\s+BY\s+SOURCE\b", re.I)),
    ("NOT MATCHED", re.compile(r"^WHEN\s+NOT\s+MATCHED\b", re.I)),
    ("MATCHED", re.compile(r"^WHEN\s+MATCHED\b", re.I)),
)

_CONDICIONAL = re.compile(r"^WHEN\s+(NOT\s+MATCHED(\s+BY\s+SOURCE)?|MATCHED)\s+AND\b", re.I)


def check_merge_options(options: Dict[str, Any], writer: str) -> None:
    """Recusa as opções da forma antiga, dizendo o que escrever no lugar."""
    for chave, substituta in _REMOVIDAS.items():
        if chave in options:
            raise ValueError(
                f"{writer} mode='merge': '{chave}' nao existe mais. Use {substituta}."
            )


def validate_merge_options(options: Dict[str, Any], writer: str) -> None:
    """Roda toda a validação do merge sem montar o comando.

    Existe para o Iceberg, que valida antes de decidir se a primeira carga vira
    `append`: sem isto um JSON errado passaria batido na execução em que a
    tabela ainda não existe e só falharia na seguinte.
    """
    check_merge_options(options, writer)
    on_condition(options, writer)
    action_clauses(options, writer)


def on_condition(options: Dict[str, Any], writer: str) -> str:
    """A condição do `ON`, escrita à mão em `on`.

    `writer` só entra na mensagem de erro, para dizer qual destino recusou.
    """
    bruto = options.get("on")
    if not isinstance(bruto, str) or not bruto.strip():
        veio = f"; veio {bruto!r}" if bruto is not None else ""
        raise ValueError(
            f"{writer} mode='merge' requer 'on' em options: a condicao SQL "
            "inteira sobre T (destino) e S (origem). Ex: "
            "{\"on\": \"T.id = S.id\"}" + veio
        )
    return bruto.strip()


def action_clauses(options: Dict[str, Any], writer: str) -> List[str]:
    """As cláusulas `WHEN ...`, na ordem em que foram escritas."""
    brutas = options.get("actions")
    if not isinstance(brutas, list) or not brutas:
        veio = f"; veio {brutas!r}" if brutas is not None else ""
        raise ValueError(
            f"{writer} mode='merge' requer 'actions' em options: a lista nao "
            "vazia de clausulas WHEN ..., na ordem em que devem ser avaliadas. "
            "Ex: [\"WHEN MATCHED THEN UPDATE SET *\", "
            "\"WHEN NOT MATCHED THEN INSERT *\"]" + veio
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
    writer: str,
) -> str:
    """O comando `MERGE INTO` inteiro, pronto para `spark.sql`."""
    check_merge_options(options, writer)
    # `on` primeiro: com as duas opções faltando, o erro que aparece é o da
    # primeira parte do comando, e não o da última.
    condicao = on_condition(options, writer)
    clausulas = "\n            ".join(action_clauses(options, writer))
    return f"""
            MERGE INTO {target} AS T
            USING {source_view} AS S
            ON {condicao}
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
