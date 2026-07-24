"""Fixtures dos testes do case-of-success.

Estes testes executam as confs de registro **contra as tabelas reais do ambiente**
(view_cessoes_pendentes + tabelas Delta do catalogo) e comparam a *tipagem/estrutura*
do payload gerado com um JSON de exemplo (golden) — nao comparam valores. Por isso
rodam onde os dados existem (ex: cluster Databricks). Quando a fonte de entrada nao
esta disponivel, o teste e automaticamente pulado (skip), nao falha.

    pip install pyspark pytest
    pip install -e .
    pytest tests/case-of-success/tests_unit -q
"""
from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession

from spark_framework import SparkFramework

# Diretorio das confs (um nivel acima de tests_unit/) e dos goldens.
CONF_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLDEN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "golden")


def conf_path(nome: str) -> str:
    return os.path.join(CONF_DIR, nome)


def golden_path(nome: str) -> str:
    return os.path.join(GOLDEN_DIR, nome)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("sparquet-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture(scope="session")
def fw(spark) -> SparkFramework:
    # SparkFramework reusa a SparkSession ativa (criada pela fixture `spark`).
    return SparkFramework()
