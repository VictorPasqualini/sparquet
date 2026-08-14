"""Valida a tipagem/estrutura do payload gerado por cada conf de registro contra
um JSON de exemplo (golden) — o payload realmente enviado ao destino.

NAO compara valores, so o shape (campos + tipos): garante que o contrato do payload
nao regrediu (campo faltando, tipo trocado de int->string, struct achatado, etc.).
Como id_externo e uuid (string) e datas sao strings, batem por tipo normalmente.

Roda contra as tabelas reais do ambiente (view_cessoes_pendentes + Delta). Onde a
fonte de entrada nao existe, o caso e pulado (skip), nao falha.

Para adicionar um fluxo: gere o JSON de exemplo em golden/<nome>.json (o payload
enviado ao destino) e acrescente uma entrada em CASES.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, List

import pytest

from conftest import conf_path, golden_path

# (id, conf, golden, params)
CASES = [
    (
        "nota_comercial_b3",
        "conf_registro_b3_nota_comercial.json",
        "nota_comercial_b3.json",
        {
            "tipo_ativo": "NOTA_COMERCIAL",
            "registradora": "B3",
            "tipo_contrato": "8",
            "fluxo_operacao": "REGISTRO",
        },
    ),
]

# Trechos de erro que indicam "tabela/fonte de entrada ausente" -> pula o teste.
_MISSING_SOURCE_MARKERS = (
    "table or view not found",
    "table_or_view_not_found",
    "cannot be resolved",
    "does not exist",
    "path does not exist",
    "is not a delta table",
    "no such table",
    "unable to infer schema",
)


class GoldenMismatch(AssertionError):
    """Payload gerado nao bate com a tipagem do golden."""


def _typename(v: Any) -> str:
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if v is None:
        return "null"
    if isinstance(v, dict):
        return "object"
    if isinstance(v, list):
        return "array"
    return type(v).__name__


def check_shape(gen: Any, exp: Any, path: str = "$") -> None:
    """Levanta GoldenMismatch se o shape/tipo de `gen` nao casar com `exp`.

    - objeto: mesmas chaves; cada valor confere recursivamente.
    - lista:  cada elemento gerado precisa casar com o shape do 1o do golden.
    - escalar: tipo precisa bater (bool/int/float/str/null distintos — reflete
      cast(... as int) vs as double vs string nas confs). null no golden aceita
      qualquer escalar (campo opcional sem exemplo de tipo).
    """
    if isinstance(exp, dict):
        if not isinstance(gen, dict):
            raise GoldenMismatch(f"{path}: esperado object, veio {_typename(gen)}")
        faltando = sorted(set(exp) - set(gen))
        sobrando = sorted(set(gen) - set(exp))
        if faltando:
            raise GoldenMismatch(f"{path}: campos faltando no payload: {faltando}")
        if sobrando:
            raise GoldenMismatch(f"{path}: campos a mais no payload: {sobrando}")
        for k in exp:
            check_shape(gen[k], exp[k], f"{path}.{k}")
        return

    if isinstance(exp, list):
        if not isinstance(gen, list):
            raise GoldenMismatch(f"{path}: esperado array, veio {_typename(gen)}")
        if not exp:
            return  # golden nao especifica shape dos elementos
        if not gen:
            raise GoldenMismatch(f"{path}: array vazio (golden tem elementos)")
        for i, elem in enumerate(gen):
            check_shape(elem, exp[0], f"{path}[{i}]")
        return

    et, gt = _typename(exp), _typename(gen)
    if et == "null":
        return
    if et != gt:
        raise GoldenMismatch(f"{path}: esperado {et}, veio {gt}")


def _run_payloads(fw, conf: str, params: dict) -> List[dict]:
    """Executa a conf e devolve os payloads (lista de dicts). Pula o teste se a
    fonte de entrada nao existir no ambiente ou se nao houver dados."""
    r = fw.run(conf, params=params)
    nome = Path(conf).name

    if not r.success:
        err = (r.error or "").lower()
        if any(m in err for m in _MISSING_SOURCE_MARKERS):
            pytest.skip(f"fonte de entrada indisponivel para {nome}: {r.error}")
        raise RuntimeError(f"Pipeline '{nome}' falhou: {r.error}")

    if r.skipped or r.output_df is None:
        pytest.skip(f"{nome} encerrou sem dados (stop_if_empty) — nada a comparar")

    from pyspark.sql import functions as F

    rows = r.output_df.select(F.to_json("payload").alias("_p")).collect()
    return [json.loads(row["_p"]) for row in rows]


@pytest.mark.parametrize(
    "conf,golden,params",
    [(c, g, p) for _, c, g, p in CASES],
    ids=[i for i, *_ in CASES],
)
def test_payload_shape(fw, conf, golden, params):
    """Algum payload gerado precisa casar com a tipagem do golden."""
    expected = json.loads(Path(golden_path(golden)).read_text(encoding="utf-8"))
    generated = _run_payloads(fw, conf_path(conf), params)

    if not generated:
        raise GoldenMismatch(f"{conf}: nenhum payload gerado para comparar.")

    erros = []
    for i, gen in enumerate(generated):
        try:
            check_shape(gen, expected)
            return  # achou um payload com o shape esperado
        except GoldenMismatch as e:
            erros.append(f"  linha {i}: {e}")

    raise GoldenMismatch(
        f"{conf}: nenhum dos {len(generated)} payloads bate com a tipagem do "
        f"golden '{golden}'.\n" + "\n".join(erros[:10])
    )
