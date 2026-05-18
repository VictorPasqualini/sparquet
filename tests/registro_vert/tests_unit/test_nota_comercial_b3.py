"""Testes unitários do fluxo Nota Comercial / B3.

Valida 3 confs:
- cessoes_base.json:                    base filtrada por status + parametrização + multi_ativos
- nota_comercial_b3/cessoes_pendentes:  enriquece com bronze_remessa + parsing JSON
- nota_comercial_b3/payload:            struct payload com schema esperado

Cada teste usa fixtures CSV registradas como temp views.
"""
from __future__ import annotations

import pytest


FIXTURES_BASICAS = [
    "silver_cessao",
    "silver_cessoes_status",
    "silver_parametrizacao_registro_lastro",
    "silver_contrato",
    "silver_status_criterios_contratos",
    "bronze_remessa",
    "silver_controle_registro_cessoes",
]


def test_cessoes_base_filtra_e_aplica_multi_ativos(register_fixtures, run_conf, spark):
    """cessoes_base.json deve produzir 3 linhas com multi_ativos=False para todas
    (cada id_operacao tem apenas 1 par tipo_ativo/registradora nas fixtures)."""
    register_fixtures(FIXTURES_BASICAS)

    result = run_conf("cessoes_base.json", columns={
        "param_processar_somente_pendentes": True,
    })
    assert result.success, result.error

    df = spark.table("cessoes_base")
    rows = df.collect()
    assert len(rows) == 3, f"Esperado 3 cessões, got {len(rows)}"

    # multi_ativos sempre False (operações não têm múltiplos pares)
    for row in rows:
        assert row["multi_ativos"] is False, f"{row['id_cessao']} deveria ter multi_ativos=False"

    # tipo_contrato deve vir da bronze_remessa
    tipos = {r["id_cessao"]: r["tipo_contrato"] for r in rows}
    assert tipos["CESS001"] == "8"  # NC
    assert tipos["CESS002"] == "8"  # NC
    assert tipos["CESS003"] == "0"  # CCB


def test_cessoes_base_filtra_lista_cessoes(register_fixtures, run_conf, spark):
    """Quando param_lista_cessoes é fornecido, filtra apenas essas cessões."""
    register_fixtures(FIXTURES_BASICAS)

    result = run_conf("cessoes_base.json", columns={
        "param_lista_cessoes": ["CESS001"],
    })
    assert result.success, result.error

    rows = spark.table("cessoes_base").collect()
    assert len(rows) == 1
    assert rows[0]["id_cessao"] == "CESS001"


def test_cessoes_base_anti_join_controle(register_fixtures, run_conf, spark):
    """Quando processar_somente_pendentes=True, exclui CESS999 do controle (mas
    CESS999 nem está nas cessões base, então não muda nada — testa o pipeline
    rodar sem erro)."""
    register_fixtures(FIXTURES_BASICAS)

    result = run_conf("cessoes_base.json", columns={
        "param_processar_somente_pendentes": True,
    })
    assert result.success, result.error
    assert spark.table("cessoes_base").count() == 3


def test_cessoes_base_skip_anti_join_quando_lista_explicita(register_fixtures, run_conf, spark):
    """Quando param_lista_cessoes é fornecido, anti-join controle é skipado
    (skip_if_null em param_processar_somente_pendentes=None)."""
    register_fixtures(FIXTURES_BASICAS)

    result = run_conf("cessoes_base.json", columns={
        "param_lista_cessoes":               ["CESS001", "CESS002", "CESS003"],
        "param_processar_somente_pendentes": None,
    })
    assert result.success, result.error
    assert spark.table("cessoes_base").count() == 3


def test_nc_cessoes_pendentes_filtra_por_tipo_e_registradora(
    register_fixtures, run_conf, spark
):
    """nota_comercial_b3/cessoes_pendentes filtra cessoes_base por NC/B3/REGISTRO
    e enriquece com bronze_remessa."""
    register_fixtures(FIXTURES_BASICAS)

    # Roda cessoes_base primeiro
    r = run_conf("cessoes_base.json", columns={"param_processar_somente_pendentes": True})
    assert r.success

    # Agora cessoes_pendentes do NC
    r = run_conf("nota_comercial_b3/cessoes_pendentes.json", columns={
        "param_tipo_ativo":              "NOTA_COMERCIAL",
        "param_registradora":            "B3",
        "param_fluxo_operacao":          "REGISTRO",
        "param_lista_cessoes_pendentes": ["CESS001", "CESS002"],
    })
    assert r.success, r.error

    df = spark.table("nota_comercial_b3_pendentes")
    rows = df.collect()
    assert len(rows) == 2  # só CESS001 e CESS002 são NC

    ids = sorted([r["id_cessao"] for r in rows])
    assert ids == ["CESS001", "CESS002"]

    # informacoes_adicionais deve ter sido parseada para struct
    info = rows[0]["informacoes_adicionais"]
    assert info is not None
    assert hasattr(info, "asDict")  # é struct
    info_d = info.asDict()
    assert "numero_da_emissao" in info_d
    assert "valor_da_unidade_de_emissao" in info_d


def test_nc_cessoes_pendentes_max_data_vencimento(register_fixtures, run_conf, spark):
    """A coluna max_data_vencimento_parcela deve ser calculada via window."""
    register_fixtures(FIXTURES_BASICAS)
    run_conf("cessoes_base.json", columns={"param_processar_somente_pendentes": True})
    r = run_conf("nota_comercial_b3/cessoes_pendentes.json", columns={
        "param_tipo_ativo":     "NOTA_COMERCIAL",
        "param_registradora":   "B3",
        "param_fluxo_operacao": "REGISTRO",
    })
    assert r.success, r.error

    df = spark.table("nota_comercial_b3_pendentes")
    # Cada linha tem max_data_vencimento_parcela = data_vencimento_parcela
    # (só há 1 parcela por contrato nas fixtures)
    for row in df.collect():
        assert row["max_data_vencimento_parcela"] is not None
