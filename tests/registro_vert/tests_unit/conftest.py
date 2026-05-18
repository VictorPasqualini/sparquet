"""Fixtures pytest para os testes unitários do registro_vert.

Cada teste recebe `fw` (SparkFramework já configurado) e o helper `register_fixtures`
que lê os CSVs em fixtures/ e registra como temp views com o mesmo nome das
tabelas Delta usadas em produção (lastros.silver_*, lastros.bronze_*).

Estratégia: as confs JSON apontam para tabelas Delta reais, mas o ViewReader
do framework usa `spark.table(name)`. Se a temp view existir com esse nome
qualificado, o Spark a resolve antes da tabela Delta — desde que o catalog
não esteja configurado para um Unity ou Hive que sobrescreva temp views.

Em local mode (default do framework), basta criar a temp view com o nome
qualificado (ex: 'lastros.silver_cessao'). Spark Local cataloga views por
nome literal.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType, DateType, ArrayType, MapType

from spark_framework import SparkFramework


FIXTURES_DIR  = Path(__file__).parent / "fixtures"
GABARITOS_DIR = Path(__file__).parent / "gabaritos"
CONFS_DIR     = Path(__file__).parent.parent / "confs"


@pytest.fixture(scope="session")
def fw() -> SparkFramework:
    """SparkFramework com configs de teste (local mode, sem cluster)."""
    framework = SparkFramework(spark={
        "app_name": "registro_vert_tests",
        "master":   "local[2]",
        "configs":  {
            "spark.sql.shuffle.partitions": "2",
            "spark.sql.session.timeZone":   "UTC",
        },
    })
    yield framework
    framework.stop()


@pytest.fixture(scope="session")
def spark(fw: SparkFramework) -> SparkSession:
    from spark_framework.core.context import SparkContextManager
    return SparkContextManager.get_or_create(fw._spark_config)


def _read_fixture_csv(spark: SparkSession, name: str) -> DataFrame:
    """Lê um CSV de fixtures/ inferindo schema (header obrigatório)."""
    path = FIXTURES_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Fixture não encontrada: {path}")
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("nullValue", "")
        .csv(str(path))
    )


@pytest.fixture
def register_fixtures(spark: SparkSession):
    """Função para registrar fixtures CSV como temp views.

    Uso:
        register_fixtures(["silver_cessao", "silver_cessoes_status", ...])

    Cada nome vira temp view 'lastros.<name>' (qualificado).
    """
    registered: List[str] = []

    def _register(names: List[str]) -> Dict[str, DataFrame]:
        result = {}
        for name in names:
            df = _read_fixture_csv(spark, name)
            view = f"lastros.{name}"
            df.createOrReplaceTempView(view)
            registered.append(view)
            result[name] = df
        return result

    yield _register

    # Cleanup
    for view in registered:
        try:
            spark.catalog.dropTempView(view)
        except Exception:
            pass


@pytest.fixture
def confs_dir() -> Path:
    return CONFS_DIR


@pytest.fixture
def gabaritos_dir() -> Path:
    return GABARITOS_DIR


def _convert_delta_to_view(data):
    """Recursivamente troca format:delta por format:view nas confs.

    Permite reusar as confs de produção nos testes — as tabelas Delta
    são substituídas pelas temp views registradas via fixtures CSV.
    """
    if isinstance(data, dict):
        if data.get("format") == "delta":
            data = {**data, "format": "view"}
        return {k: _convert_delta_to_view(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_convert_delta_to_view(item) for item in data]
    return data


@pytest.fixture
def run_conf(fw: SparkFramework):
    """Helper que carrega uma conf, troca format:delta → format:view, e roda.

    Uso:
        result = run_conf("nota_comercial_b3/cessoes_pendentes.json",
                          columns={"param_tipo_ativo": "NOTA_COMERCIAL", ...})

    NÃO faz a escrita em Kafka real — o teste deve usar uma conf intermediária
    que termine numa view (cessoes_pendentes) ou usar mocks para Kafka.
    """
    def _run(conf_relative_path: str, columns: dict = None):
        conf_path = CONFS_DIR / conf_relative_path
        raw = json.loads(conf_path.read_text(encoding="utf-8"))
        raw = _convert_delta_to_view(raw)

        # Remover outputs que escrevem em Kafka/Delta — em teste, só queremos
        # validar a view de payload. Mantém apenas outputs format:view.
        if "outputs" in raw:
            raw["outputs"] = [o for o in raw["outputs"] if o.get("format") == "view"]
            if not raw["outputs"]:
                raise ValueError(f"Conf {conf_relative_path} não tem output view para teste")
        elif "output" in raw:
            if raw["output"].get("format") != "view":
                raise ValueError(f"Conf {conf_relative_path} não tem output view para teste")

        return fw.run_from_dict(raw, columns=columns or {})

    return _run


def assert_view_matches_gabarito(
    spark: SparkSession,
    view_name: str,
    gabarito_name: str,
    columns_to_compare: List[str] | None = None,
):
    """Compara uma temp view com um gabarito JSON (linha por linha).

    O gabarito é um arquivo JSON em gabaritos/<gabarito_name>.json contendo
    uma lista de dicts. Cada dict representa uma linha esperada.

    Se columns_to_compare for None, compara TODAS as colunas do gabarito
    (ignorando colunas extras no DataFrame).
    """
    actual_df = spark.table(view_name)
    actual = sorted(
        [row.asDict(recursive=True) for row in actual_df.collect()],
        key=lambda r: json.dumps(r, default=str, sort_keys=True),
    )

    gabarito_path = GABARITOS_DIR / f"{gabarito_name}.json"
    if not gabarito_path.exists():
        raise FileNotFoundError(
            f"Gabarito não encontrado: {gabarito_path}\n"
            f"Para criar: salve o output esperado da view '{view_name}' em JSON."
        )
    expected = sorted(
        json.loads(gabarito_path.read_text(encoding="utf-8")),
        key=lambda r: json.dumps(r, default=str, sort_keys=True),
    )

    if columns_to_compare:
        actual   = [{k: v for k, v in r.items() if k in columns_to_compare}   for r in actual]
        expected = [{k: v for k, v in r.items() if k in columns_to_compare} for r in expected]

    assert len(actual) == len(expected), (
        f"View '{view_name}' tem {len(actual)} linhas, gabarito tem {len(expected)}"
    )
    for i, (a, e) in enumerate(zip(actual, expected)):
        assert a == e, f"Linha {i} diverge:\n  actual={a}\n  expected={e}"
