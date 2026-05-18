# =============================================================================
# Orquestrador de registro de cessões — CERC / B3 (Python = só orquestração)
#
# Todas as transformações, montagem de payload e escritas estão em JSON.
# Este script apenas:
#   1. Lê widgets do Databricks Jobs e valida parâmetros
#   2. Roda cessoes_base.json (base materializada via localCheckpoint)
#   3. Coleta a lista de id_cessao pendentes (passada como param para os
#      próximos pipelines, otimizando a leitura das tabelas de cada ativo)
#   4. Para cada fluxo (tipo_ativo, registradora, tipo_fluxo):
#        - cessoes_pendentes.json: enriquece com tabelas específicas do ativo
#        - payload.json:           monta struct + Kafka + 3 Deltas (output múltiplo)
#   5. Cleanup de temp views
#
# Instalação no Databricks:
#   %pip install git+https://github.com/VictorPasqualini/sparquet.git
# =============================================================================
from __future__ import annotations

import traceback

from spark_framework import SparkFramework
from spark_framework.core.context import SparkContextManager

from constants import (  # noqa: E402
    CODIGOS_TIPO_CONTRATO,
    FLUXOS_OPERACOES,
    KAFKA_BROKER,
    VIEWS_INTERMEDIARIAS,
)
from validacao_parametros import validar_param_lista, validar_param_unico  # noqa: E402

BASE_PATH = "/Workspace/Repos/SEU_USUARIO/sparquet/tests/registro_vert/confs"

fw = SparkFramework()
spark = SparkContextManager.get_or_create(fw._spark_config)

# ---------------------------------------------------------------------------
# ETAPA 1 — Params do job
# ---------------------------------------------------------------------------
try:
    param_lista_cessoes   = validar_param_lista("cessoes_lista_string", str)   or []
    param_lista_operacoes = validar_param_lista("operacoes_lista_int", int)    or []
    param_lista_contratos = validar_param_lista("contratos_lista_string", str) or []
    param_tipo_ativo      = validar_param_unico("tipo_ativo_string", str)
    param_registradora    = validar_param_unico("registradora_string", str)

    processar_somente_pendentes = not (param_lista_cessoes or param_lista_contratos)

    if param_tipo_ativo and param_registradora:
        chave = (param_tipo_ativo, param_registradora)
        if chave not in FLUXOS_OPERACOES:
            raise ValueError(f"Sem fluxos mapeados para {param_tipo_ativo}-{param_registradora}")
        fluxos_ativos = {chave: FLUXOS_OPERACOES[chave]}
    else:
        fluxos_ativos = FLUXOS_OPERACOES

except Exception:
    raise RuntimeError(f"ERRO params\n{traceback.format_exc()}")


# ---------------------------------------------------------------------------
# ETAPA 2 — Base de cessões (declarativo, com filtros opcionais + multi_ativos)
# ---------------------------------------------------------------------------
r = fw.run(
    f"{BASE_PATH}/cessoes_base.json",
    params={
        "param_lista_cessoes":               param_lista_cessoes,
        "param_lista_operacoes":             param_lista_operacoes,
        "param_lista_contratos":             param_lista_contratos,
        "param_processar_somente_pendentes": processar_somente_pendentes,
    },
)
print(r.summary())
if not r.success:
    raise RuntimeError(f"Falha em cessoes_base.json: {r.error}")


# ---------------------------------------------------------------------------
# ETAPA 3 — Coleta lista de id_cessao pendentes (otimiza leitura por ativo)
# ---------------------------------------------------------------------------
# Esta lista é injetada via param em cada conf de ativo para reduzir o
# volume lido de bronze_remessa / silver_contrato / silver_parcela etc.
lista_cessoes_pendentes = [
    r.id_cessao
    for r in spark.table("cessoes_base").select("id_cessao").distinct().collect()
]

if not lista_cessoes_pendentes:
    print("ℹ️ Sem cessões pendentes — encerrando.")
    fw.drop_views(VIEWS_INTERMEDIARIAS)
    fw.stop()
    raise SystemExit(0)

print(f"ℹ️ {len(lista_cessoes_pendentes)} cessões pendentes")
df_nao_processadas = spark.table("cessoes_base")


# ---------------------------------------------------------------------------
# ETAPA 4 — Loop por fluxo (cessoes_pendentes + payload com 4 outputs)
# ---------------------------------------------------------------------------
for (tipo_ativo, registradora), fluxo_operacao in fluxos_ativos.items():
    for tipo_fluxo, attrs in fluxo_operacao.items():
        print(f"\n⌛ {tipo_ativo}-{registradora} ({tipo_fluxo})")

        runtime_params = {
            "param_tipo_ativo":             tipo_ativo,
            "param_registradora":           registradora,
            "param_fluxo_operacao":         tipo_fluxo,
            "param_codigo_tipo_contrato":   CODIGOS_TIPO_CONTRATO[tipo_ativo],
            "param_lista_cessoes_pendentes": lista_cessoes_pendentes,
            "param_topico":                 attrs["topico"],
            "param_kafka_broker":           KAFKA_BROKER,
        }

        # --- 1. Cessoes_pendentes genérica (filter por params) ---
        # Para Duplicata o fluxo_operacao é skipado (3 subfluxos compartilham);
        # cada payload de Duplicata aplica seu próprio filter de fluxo.
        params_pendentes = dict(runtime_params)
        if tipo_ativo == "DUPLICATA":
            params_pendentes["param_fluxo_operacao"] = None
        r_pend = fw.run(f"{BASE_PATH}/cessoes_pendentes.json", params=params_pendentes)
        if not r_pend.success:
            print(f"❌ cessoes_pendentes — {r_pend.error}")
            continue
        if r_pend.rows_written == 0:
            print(f"ℹ️ sem cessões para este fluxo")
            continue

        # --- 2. Enriquecimento (opcional, apenas Duplicata por agora) ---
        if attrs.get("conf_enriquecimento"):
            r_enriq = fw.run(f"{BASE_PATH}/{attrs['conf_enriquecimento']}", params=runtime_params)
            if not r_enriq.success:
                print(f"❌ enriquecimento — {r_enriq.error}")
                continue

        # --- 3. Payload + Kafka + 3 Deltas (output múltiplo) ---
        r_payload = fw.run(f"{BASE_PATH}/{attrs['conf_payload']}", params=runtime_params)
        if not r_payload.success:
            print(f"❌ payload — {r_payload.error}")
            continue

        # Atualiza o controle de não-processadas
        df_proc = spark.table(attrs["view_payload"]).select("id_cessao").distinct()
        df_nao_processadas = df_nao_processadas.join(df_proc, on=["id_cessao"], how="leftanti")
        print(f"✅ enviado: {r_payload.rows_written} mensagens Kafka")


# ---------------------------------------------------------------------------
# ETAPA 5 — Diagnóstico final + cleanup
# ---------------------------------------------------------------------------
if df_nao_processadas.isEmpty():
    print("\n✅ Todas as cessões foram processadas.")
else:
    print("\n⚠️ Cessões NÃO processadas (verificar tipo_contrato vs tipo_ativo):")
    df_nao_processadas.select("id_operacao", "id_cessao").dropDuplicates().show()

fw.drop_views(VIEWS_INTERMEDIARIAS)
fw.stop()
