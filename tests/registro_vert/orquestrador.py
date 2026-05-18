# =============================================================================
# Orquestrador de registro de cessões — CERC / B3 (Python = só orquestração)
#
# Toda a lógica de transformação, montagem de payload e escrita está em JSON.
# Este script apenas:
#   1. Lê widgets do Databricks Jobs e valida parâmetros
#   2. Resolve quais fluxos serão executados
#   3. Roda as confs: base → para_processar → (por fluxo) enriquecimento +
#      payload + envio
#   4. Faz cleanup de temp views ao final
#
# Instalação no Databricks:
#   %pip install git+https://github.com/VictorPasqualini/sparquet.git
#
# Dependências Spark no cluster:
#   org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1
# =============================================================================
from __future__ import annotations

import traceback

from spark_framework import SparkFramework
from spark_framework.core.context import SparkContextManager

from constants import (  # noqa: E402
    FLUXOS_OPERACOES,
    KAFKA_BROKER,
    VIEWS_INTERMEDIARIAS,
)
from validacao_parametros import (  # noqa: E402
    validar_param_lista,
    validar_param_unico,
)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
BASE_PATH = "/Workspace/Repos/SEU_USUARIO/sparquet/tests/registro_vert/confs"

fw = SparkFramework()

# ---------------------------------------------------------------------------
# ETAPA 1 — Leitura e validação de parâmetros do job
# ---------------------------------------------------------------------------
try:
    param_lista_cessoes   = validar_param_lista("cessoes_lista_string", str)   or []
    param_lista_operacoes = validar_param_lista("operacoes_lista_int", int)    or []
    param_lista_contratos = validar_param_lista("contratos_lista_string", str) or []
    param_tipo_ativo      = validar_param_unico("tipo_ativo_string", str)
    param_registradora    = validar_param_unico("registradora_string", str)

    # Se foi passada uma lista explícita de cessões/contratos, não filtra por
    # cessões já registradas — o usuário quer reprocessar especificamente.
    processar_somente_pendentes = not (param_lista_cessoes or param_lista_contratos)

    # Resolve fluxos a executar (todos ou apenas o (tipo_ativo, registradora) informado)
    if param_tipo_ativo and param_registradora:
        chave = (param_tipo_ativo, param_registradora)
        if chave not in FLUXOS_OPERACOES:
            raise ValueError(
                f"Sem fluxos mapeados para {param_tipo_ativo}-{param_registradora}"
            )
        fluxos_ativos = {chave: FLUXOS_OPERACOES[chave]}
        print(f"✅ Filtro de fluxo: {param_tipo_ativo}-{param_registradora}")
    else:
        fluxos_ativos = FLUXOS_OPERACOES
        print("ℹ️ Sem filtros para tipo_ativo/registradora — todos os fluxos")

except Exception as e:
    raise RuntimeError(f"ERRO ETAPA 1 — params do job\n{traceback.format_exc()}") from e


# ---------------------------------------------------------------------------
# ETAPA 2 — Base de cessões aprovadas (declarativo)
# ---------------------------------------------------------------------------
# cessoes_base.json:
#   - silver_cessao + silver_cessoes_status(1,10,11) + silver_parametrizacao
#   - join silver_contrato com critérios validados (via with_transformations)
#   - join bronze_remessa para tipo_contrato
#   - filtros opcionais por listas (param_lista_*) via skip_if_null
#   → grava view 'cessoes_base'
r = fw.run(
    f"{BASE_PATH}/cessoes_base.json",
    columns={
        "param_lista_cessoes":   param_lista_cessoes,
        "param_lista_operacoes": param_lista_operacoes,
        "param_lista_contratos": param_lista_contratos,
    },
)
print(r.summary())
if not r.success:
    raise RuntimeError(f"Falha em cessoes_base.json: {r.error}")


# ---------------------------------------------------------------------------
# ETAPA 3 — Cessões prontas para processar (multi_ativos + anti-join controle)
# ---------------------------------------------------------------------------
# cessoes_para_processar.json:
#   - lê view 'cessoes_base'
#   - self-join + group_by(expr) para criar coluna 'multi_ativos'
#   - anti-join com silver_controle_registro_cessoes (skipável)
#   → grava view 'cessoes_para_processar' (cached)
r = fw.run(
    f"{BASE_PATH}/cessoes_para_processar.json",
    columns={
        # bool → escalar; quando False, o anti-join é skipado via skip_if_null
        # (None / [] / False são todos tratados como "skip")
        "param_processar_somente_pendentes": True if processar_somente_pendentes else None,
    },
)
print(r.summary())
if not r.success:
    raise RuntimeError(f"Falha em cessoes_para_processar.json: {r.error}")


# ---------------------------------------------------------------------------
# ETAPA 4 — Loop por fluxo (enriquecimento + payload + envio)
# ---------------------------------------------------------------------------
spark = SparkContextManager.get_or_create(fw._spark_config)
df_nao_processadas = spark.table("cessoes_para_processar")

for (tipo_ativo, registradora), fluxo_operacao in fluxos_ativos.items():
    for tipo_fluxo, attrs in fluxo_operacao.items():
        print(f"\n⌛ {tipo_ativo}-{registradora} ({tipo_fluxo})")

        params_fluxo = {
            "param_tipo_ativo":     tipo_ativo,
            "param_registradora":   registradora,
            "param_fluxo_operacao": tipo_fluxo,
        }

        # --- Enriquecimento (JSON) ---
        r_enriq = fw.run(f"{BASE_PATH}/{attrs['conf_enriquecimento']}", columns=params_fluxo)
        if not r_enriq.success:
            print(f"❌ enriquecimento — {r_enriq.error}")
            continue
        if r_enriq.rows_written == 0:
            print(f"ℹ️ sem cessões para {tipo_ativo}-{registradora} ({tipo_fluxo})")
            continue

        # --- Montagem do payload (JSON) ---
        r_payload = fw.run(f"{BASE_PATH}/{attrs['conf_payload']}", columns=params_fluxo)
        if not r_payload.success:
            print(f"❌ payload — {r_payload.error}")
            continue

        # --- Envio em 4 etapas declarativas (JSON) ---
        # Cada conf de envio tem sua transformação específica (Kafka usa to_json,
        # parcelas faz explode, controle faz distinct, contratos drop parcelas)
        # — não dá para empilhar tudo numa conf só pois mudam a cardinalidade.
        envio_cols = {
            **params_fluxo,
            "param_topico":        attrs["topico"],
            "param_kafka_broker":  KAFKA_BROKER,
            "param_view_payload":  attrs["view_payload"],
        }

        falhou = False
        for conf_envio in [
            "envio_kafka.json",
            "envio_delta_controle.json",
            "envio_delta_contratos.json",
            "envio_delta_parcelas.json",
        ]:
            r_envio = fw.run(f"{BASE_PATH}/{conf_envio}", columns=envio_cols)
            if not r_envio.success:
                print(f"❌ {conf_envio} — {r_envio.error}")
                falhou = True
                break

        if falhou:
            continue

        # Atualiza controle de cessões não processadas
        df_proc = spark.table(attrs["view_payload"]).select("id_cessao").distinct()
        df_nao_processadas = df_nao_processadas.join(df_proc, on=["id_cessao"], how="leftanti")
        print(f"✅ enviado: {tipo_ativo}-{registradora} ({tipo_fluxo})")


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
