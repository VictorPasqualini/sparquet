from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


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

    Inclusions aninhadas não são suportadas.
    """
    raw_transformations: List[Dict[str, Any]] = data.get("transformations", [])
    if not any("$include" in t for t in raw_transformations):
        return data

    resolved: List[Dict[str, Any]] = []
    for t in raw_transformations:
        if "$include" not in t:
            resolved.append(t)
            continue

        include_path = base_dir / t["$include"]
        raw = include_path.read_text(encoding="utf-8")

        if params:
            from sparquet.utils.template import apply_template
            raw = apply_template(raw, params)

        included = json.loads(raw)
        if isinstance(included, list):
            resolved.extend(included)
        else:
            resolved.append(included)

    return {**data, "transformations": resolved}
