# =============================================================================
# Orquestrador de registro de cessões — CERC / B3
#
# Responsabilidades:
#   1. Preparação via framework  (JSON declarativo, sem lógica no código)
#        cessoes_base.json → view 'cessoes_base'
#          └─ inclui: filtro por status, join parametrizacao, join contratos
#             (com critérios avaliados via with_transformations) e tipo_contrato
#   2. Enriquecimento Python    (multi_ativos, params de job, anti-join de controle)
#   3. Loop por fluxo           (tipo_ativo × registradora × tipo_fluxo)
#        → funções customizadas de payload
#        → escrita Kafka + tabelas Delta
#
# Instalação:
#   %pip install git+https://github.com/VictorPasqualini/sparquet.git
#
# Dependência Kafka no cluster:
#   org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1
# =============================================================================

from pyspark.sql import functions as F
from spark_framework import SparkFramework

# ---------------------------------------------------------------------------
# Configuração — ajuste antes de executar
# ---------------------------------------------------------------------------
BASE_PATH    = "/Workspace/Repos/SEU_USUARIO/sparquet/tests/databricks"
KAFKA_BROKER = "BROKER1:9092,BROKER2:9092"
ENVIADO      = 1

# Mapeamento de fluxos de operação por (tipo_ativo, registradora).
# Cada fluxo define: funcao_preparacao_objeto, funcao_construcao_payload, topico.
# Exemplo:
#   FLUXOS_OPERACOES = {
#       ("CCB", "CERC"): {
#           "EMISSAO_E_REGISTRO": {
#               "funcao_preparacao_objeto": preparar_ccb_cerc,
#               "funcao_construcao_payload": construir_payload_ccb_cerc,
#               "topico": "topico-ccb-cerc",
#           }
#       },
#   }
FLUXOS_OPERACOES = {}  # preencha conforme seu negócio


# ---------------------------------------------------------------------------
# Helpers de parâmetros (lê widgets do Databricks Jobs / notebook)
# ---------------------------------------------------------------------------
def _get_widget(nome: str) -> str | None:
    try:
        valor = dbutils.widgets.get(nome).strip()
        return valor if valor else None
    except Exception:
        return None


def validar_param_lista(nome: str, tipo: type) -> list | None:
    valor = _get_widget(nome)
    if not valor:
        return None
    return [tipo(v.strip()) for v in valor.split(",") if v.strip()]


def validar_param_unico(nome: str, tipo: type):
    valor = _get_widget(nome)
    return tipo(valor) if valor else None


# =============================================================================
# ETAPA 1 — Preparação de dados via framework (JSON declarativo)
# =============================================================================
fw = SparkFramework()

r_cessoes = fw.run(f"{BASE_PATH}/cessoes_base.json")
print(r_cessoes.summary())
if not r_cessoes.success:
    raise RuntimeError(f"Falha na preparação de cessões: {r_cessoes.error}")


# =============================================================================
# ETAPA 2 — Enriquecimento e filtragem (lógica não-trivial em Python)
# =============================================================================
df = spark.table("cessoes_base")

# --- multi_ativos: flag True quando a operação tem mais de 1 (tipo_ativo, registradora) ---
df_multi = (
    df.select("id_operacao", "tipo_ativo", "registradora").distinct()
    .groupBy("id_operacao")
    .agg((F.countDistinct(F.struct("tipo_ativo", "registradora")) > 1).alias("multi_ativos"))
)
df = df.join(df_multi.select("id_operacao", "multi_ativos"), on=["id_operacao"], how="left")

# --- Parâmetros do job ---
processar_somente_cessoes_pendentes = True

param_lista_contratos = validar_param_lista("contratos_lista_string", str)
param_lista_cessoes   = validar_param_lista("cessoes_lista_string",   str)
param_lista_operacoes = validar_param_lista("operacoes_lista_int",    int)
param_tipo_ativo      = validar_param_unico("tipo_ativo_string",      str)
param_registradora    = validar_param_unico("registradora_string",    str)

if param_lista_contratos:
    processar_somente_cessoes_pendentes = False
    print("✅ CONTRATOS FILTRADOS:", param_lista_contratos)
    df = df.filter(F.col("numero_contrato").isin(param_lista_contratos))
else:
    print("ℹ️ Sem filtros para contratos")

if param_lista_cessoes:
    processar_somente_cessoes_pendentes = False
    print("✅ CESSÕES FILTRADAS:", param_lista_cessoes)
    df = df.filter(F.col("id_cessao").isin(param_lista_cessoes))
else:
    print("ℹ️ Sem filtros para cessões")

if param_lista_operacoes:
    print("✅ OPERAÇÕES FILTRADAS:", param_lista_operacoes)
    df = df.filter(F.col("id_operacao").isin(param_lista_operacoes))
else:
    print("ℹ️ Sem filtros para operações")

# --- Resolve fluxos ativos ---
if param_tipo_ativo and param_registradora:
    chave = (param_tipo_ativo, param_registradora)
    if chave not in FLUXOS_OPERACOES:
        raise ValueError(f"Sem fluxo mapeado para {param_tipo_ativo}-{param_registradora}")
    fluxos_ativos = {chave: FLUXOS_OPERACOES[chave]}
    print(f"✅ Filtro tipo_ativo-registradora: {param_tipo_ativo}-{param_registradora}")
else:
    fluxos_ativos = FLUXOS_OPERACOES
    print("ℹ️ Sem filtros para tipo_ativo/registradora — todos os fluxos")

# --- Anti-join: descarta cessões já registradas (exceto com filtros explícitos) ---
if processar_somente_cessoes_pendentes:
    df = df.join(
        spark.table("lastros.silver_controle_registro_cessoes").select("id_cessao"),
        on=["id_cessao"],
        how="leftanti"
    )

df.cache()
df.createOrReplaceTempView("cessoes_para_processar")
df_nao_processadas = df


# =============================================================================
# ETAPA 3 — Loop de processamento por tipo_ativo / registradora / fluxo
# =============================================================================
for (tipo_ativo, registradora), fluxo_operacao in fluxos_ativos.items():
    for tipo_fluxo, atributos_fluxo in fluxo_operacao.items():

        df_registro = df.filter(
            (F.col("tipo_ativo")       == tipo_ativo)
            & (F.col("registradora")   == registradora)
            & (F.col("fluxo_operacao") == tipo_fluxo)
            & (
                ((F.col("tipo_contrato") == tipo_ativo) & F.col("multi_ativos"))
                | (~F.col("multi_ativos"))
            )
        )

        df_nao_processadas = df_nao_processadas.join(
            df_registro.select("id_cessao").distinct(),
            on=["id_cessao"],
            how="leftanti"
        )

        print(f"\n⌛ PROCESSANDO: {tipo_ativo}-{registradora} ({tipo_fluxo})")

        if df_registro.isEmpty():
            print(f"ℹ️ Sem cessões pendentes — {tipo_ativo}-{registradora} ({tipo_fluxo})")
            continue

        try:
            objeto_preparacao              = atributos_fluxo["funcao_preparacao_objeto"](df_registro)
            df_envio_registro, df_registro_estrutura = atributos_fluxo["funcao_construcao_payload"](obj=objeto_preparacao)
        except Exception:
            import traceback
            print(f"❌ Erro no processamento — {tipo_ativo}-{registradora} ({tipo_fluxo})\n{traceback.format_exc()}")
            continue

        topico_kafka = atributos_fluxo["topico"]

        try:
            # Publica no Kafka
            (
                df_envio_registro.write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKER)
                .option("topic", topico_kafka)
                .option("kafka.max.request.size", "4194304")
                .save()
            )

            # Controle de cessões enviadas
            (
                df_registro.select("id_cessao", "id_operacao").distinct()
                .withColumn("data_envio", F.current_timestamp())
                .write.mode("append").option("mergeSchema", "true")
                .saveAsTable("lastros.silver_controle_registro_cessoes")
            )

            # Estrutura de monitoramento
            (
                df_registro_estrutura
                .withColumn("payload",          F.to_json(F.col("payload")))
                .withColumn("status",           F.lit(ENVIADO))
                .withColumn("data_atualizacao", F.lit(None).cast("date"))
                .withColumn("data_envio",       F.current_timestamp())
                .withColumn("id_registro",      F.lit(None).cast("string"))
                .withColumn("id",               F.lit(None).cast("bigint"))
                .withColumn("registradora",     F.lit(registradora))
                .withColumn("tipo_ativo",       F.lit(tipo_ativo))
                .drop("parcelas")
                .write.mode("append").option("mergeSchema", "true")
                .saveAsTable("lastros.silver_registro_contratos")
            )

            # Parcelas
            (
                df_registro_estrutura
                .withColumn("parcela", F.explode("parcelas"))
                .select(
                    "id_operacao", "id_vert", "id_cessao", "numero_contrato",
                    F.col("parcela.codigo_controle_parcela_contrato_if").alias("identificador_parcela"),
                    F.col("parcela.numero_parcela").alias("numero_parcela"),
                    F.lit(ENVIADO).alias("status_parcela"),
                )
                .withColumn("data_baixa", F.lit(None).cast("date"))
                .drop("parcelas")
                .write.mode("append").option("mergeSchema", "true")
                .saveAsTable("lastros.silver_registro_parcelas")
            )

        except Exception:
            import traceback
            print(f"❌ Erro Kafka/persistência — {tipo_ativo}-{registradora} ({tipo_fluxo})\n{traceback.format_exc()}")
            continue

        finally:
            df_registro_estrutura.unpersist()
            df_envio_registro.unpersist()


# =============================================================================
# Resultado final
# =============================================================================
if df_nao_processadas.isEmpty():
    print("\n✅ Todas as cessões foram processadas.")
else:
    print("\n⚠️ Cessões não processadas (tipo_contrato incompatível com tipo_ativo?):")
    df_nao_processadas.select("id_operacao").dropDuplicates().display()

fw.stop()
