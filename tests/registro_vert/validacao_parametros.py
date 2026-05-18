"""Validação de parâmetros do Databricks Jobs / widgets.

Mantido inalterado em relação ao projeto original: leitura via dbutils.widgets
e parsing JSON com validação de tipo. Esta parte permanece em Python porque
depende de dbutils (Databricks runtime) e da forma como o job recebe params.
"""
from __future__ import annotations

import json


def validar_leitura_param(nome_param: str) -> str:
    try:
        return dbutils.widgets.get(nome_param)  # noqa: F821 — dbutils no runtime
    except Exception as e:
        raise ValueError(f"Parâmetro '{nome_param}' não pode ser lido") from e


def validar_param_lista(nome_param: str, tipo_item: type) -> list:
    valor_param = validar_leitura_param(nome_param)

    try:
        valor_json = json.loads(valor_param)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Parâmetro '{nome_param}' deve ser uma lista no formato [\"a\", \"b\"]"
        ) from e

    if not isinstance(valor_json, list):
        raise ValueError(
            f"Parâmetro '{nome_param}' deve ser uma lista no formato [\"a\", \"b\"]"
        )

    for item in valor_json:
        if not isinstance(item, tipo_item):
            raise ValueError(
                f"Parâmetro '{nome_param}' deve conter apenas itens do tipo {tipo_item.__name__}"
            )

    return valor_json


def validar_param_unico(nome_param: str, tipo_param: type):
    valor_param = validar_leitura_param(nome_param)

    try:
        valor_json = json.loads(valor_param)
        if isinstance(valor_json, (list, dict)):
            raise ValueError(
                f"Parâmetro '{nome_param}' deve ser um valor único do tipo {tipo_param.__name__}"
            )
    except json.JSONDecodeError:
        pass

    if not isinstance(valor_param, tipo_param):
        raise ValueError(
            f"Parâmetro '{nome_param}' deve ser um valor único do tipo {tipo_param.__name__}"
        )

    return valor_param
