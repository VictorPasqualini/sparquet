from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

#: Teto de profundidade. Um ciclo já é barrado pela pilha de arquivos abertos;
#: este limite pega a outra forma de explosão, uma cadeia longa de includes que
#: se incluem em sequência sem nunca repetir arquivo.
_MAX_PROFUNDIDADE = 20


def resolve_includes(
    data: Dict[str, Any],
    base_dir: Path,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Expande diretivas $include na lista de transformations.

    Cada item { "$include": "caminho/arquivo.json" } é substituído inline
    pelo conteúdo do arquivo referenciado. O caminho é relativo a base_dir
    (diretório do JSON principal). O arquivo pode ser um único objeto de
    transformação ou uma lista de objetos.

    Quando params é fornecido, apply_template é aplicado ao arquivo incluído
    antes do parse — variáveis como {tipo_ativo} em arquivos compartilhados
    são resolvidas com os mesmos params do pipeline principal.

    A expansão é recursiva: um arquivo incluído pode conter outras diretivas
    $include, e o caminho delas é relativo ao **arquivo que as escreveu**, não
    ao JSON principal — é o que permite mover uma pasta de includes inteira sem
    reescrever os caminhos de dentro. Um ciclo (A inclui B que inclui A) levanta
    ValueError nomeando os arquivos, em vez de estourar a pilha.
    """
    raw_transformations: List[Dict[str, Any]] = data.get("transformations", [])
    if not any("$include" in t for t in raw_transformations):
        return data

    resolved = _expandir(raw_transformations, base_dir, params, [])
    return {**data, "transformations": resolved}


def _expandir(
    itens: List[Dict[str, Any]],
    base_dir: Path,
    params: Optional[Dict[str, Any]],
    pilha: List[Path],
) -> List[Dict[str, Any]]:
    """Expande uma lista de transformações, seguindo os includes de dentro.

    `pilha` são os arquivos abertos acima deste ponto, na ordem — serve para
    detectar ciclo e para escrever a mensagem de erro com o caminho percorrido.
    """
    resolved: List[Dict[str, Any]] = []
    for item in itens:
        if not isinstance(item, dict) or "$include" not in item:
            resolved.append(item)
            continue

        caminho = (base_dir / item["$include"]).resolve()
        _checar_ciclo(caminho, pilha)
        if len(pilha) >= _MAX_PROFUNDIDADE:
            raise ValueError(
                f"$include passou de {_MAX_PROFUNDIDADE} niveis de profundidade "
                f"em '{caminho}'; a cadeia provavelmente nao termina"
            )

        raw = caminho.read_text(encoding="utf-8")
        if params:
            from sparquet.utils.template import apply_template
            raw = apply_template(raw, params)

        incluido = json.loads(raw)
        lista = incluido if isinstance(incluido, list) else [incluido]
        # O caminho de um include aninhado é relativo ao arquivo que o escreveu.
        resolved.extend(
            _expandir(lista, caminho.parent, params, [*pilha, caminho])
        )

    return resolved


def _checar_ciclo(caminho: Path, pilha: List[Path]) -> None:
    if caminho not in pilha:
        return
    percurso = " -> ".join(p.name for p in [*pilha, caminho])
    raise ValueError(
        f"$include ciclico: '{caminho.name}' ja esta sendo incluido nesta cadeia "
        f"({percurso})"
    )
