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
#        → fw.run(config, input_df, columns) filtra e enriquece via conf JSON
#        → funcao_construcao_payload monta o payload e retorna df_envio + df_estrutura
#        → escrita Kafka + tabelas Delta
#
# Instalação:
#   %pip install git+https://github.com/VictorPasqualini/sparquet.git
#
# Dependência Kafka no cluster:
#   org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1
# =============================================================================

import traceback
from typing import Any, Tuple

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from spark_framework import SparkFramework

# ---------------------------------------------------------------------------
# Configuração — ajuste antes de executar
# ---------------------------------------------------------------------------
BASE_PATH    = "/Workspace/Repos/SEU_USUARIO/sparquet/tests/databricks"
KAFKA_BROKER = "BROKER1:9092,BROKER2:9092"
ENVIADO      = 1

# Mapeamento de fluxos de operação por (tipo_ativo, registradora).
# Cada fluxo define:
#   config                   – arquivo JSON que filtra e enriquece o df (recebe input_df + params)
#   funcao_construcao_payload – função Python que recebe df_registro e retorna (df_envio, df_estrutura)
#   topico                   – tópico Kafka de destino
#
# Exemplo:
#   FLUXOS_OPERACOES = {
#       ("NC", "CERC"): {
#           "EMISSAO_E_REGISTRO": {
#               "config":                    "cessoes_nota_comercial.json",
#               "view":                      "cessoes_nota_comercial_reg",
#               "funcao_construcao_payload": cessao_registro_lastro_nota_comercial,
#               "topico":                    "topico-nc-cerc",
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
# ETAPA 2 — Filtragem e seleção de fluxo
# =============================================================================

# --- Parâmetros do job (leitura de widgets — único trecho que exige Python) ---
param_lista_contratos = validar_param_lista("contratos_lista_string", str)
param_lista_cessoes   = validar_param_lista("cessoes_lista_string",   str)
param_lista_operacoes = validar_param_lista("operacoes_lista_int",    int)
param_tipo_ativo      = validar_param_unico("tipo_ativo_string",      str)
param_registradora    = validar_param_unico("registradora_string",    str)

processar_pendentes = not any([param_lista_contratos, param_lista_cessoes])

print("✅ CONTRATOS FILTRADOS:",  param_lista_contratos) if param_lista_contratos else print("ℹ️ Sem filtros para contratos")
print("✅ CESSÕES FILTRADAS:",    param_lista_cessoes)   if param_lista_cessoes   else print("ℹ️ Sem filtros para cessões")
print("✅ OPERAÇÕES FILTRADAS:",  param_lista_operacoes) if param_lista_operacoes else print("ℹ️ Sem filtros para operações")

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

# --- Filtragem e anti-join declarativos via conf ---
fw.run(
    f"{BASE_PATH}/cessoes_para_processar.json",
    params={
        "processar_pendentes": processar_pendentes,
        "lista_contratos":     param_lista_contratos or [],
        "lista_cessoes":       param_lista_cessoes   or [],
        "lista_operacoes":     param_lista_operacoes or [],
    },
)

df = spark.table("cessoes_para_processar")
df.cache()
df.createOrReplaceTempView("cessoes_para_processar")
df_nao_processadas = df


# =============================================================================
# ETAPA 3 — Loop de processamento por tipo_ativo / registradora / fluxo
# =============================================================================
for (tipo_ativo, registradora), fluxo_operacao in fluxos_ativos.items():
    for tipo_fluxo, atributos_fluxo in fluxo_operacao.items():

        print(f"\n⌛ PROCESSANDO: {tipo_ativo}-{registradora} ({tipo_fluxo})")

        # A conf lê cessoes_para_processar e filtra/enriquece via colunas de runtime
        r = fw.run(
            f"{BASE_PATH}/{atributos_fluxo['config']}",
            columns={
                "param_tipo_ativo":     tipo_ativo,
                "param_registradora":   registradora,
                "param_fluxo_operacao": tipo_fluxo,
            },
        )

        if not r.success:
            print(f"❌ Erro na conf — {tipo_ativo}-{registradora} ({tipo_fluxo})\n{r.error}")
            continue

        # localCheckpoint materializa o df desta iteração — evita que df_nao_processadas
        # resolva para a última view gravada ao ser avaliado no final do loop
        df_registro = spark.table(atributos_fluxo["view"]).localCheckpoint()

        df_nao_processadas = df_nao_processadas.join(
            df_registro.select("id_cessao").distinct(),
            on=["id_cessao"],
            how="leftanti",
        )

        if df_registro.isEmpty():
            print(f"ℹ️ Sem cessões pendentes — {tipo_ativo}-{registradora} ({tipo_fluxo})")
            continue

        try:
            df_envio_registro, df_registro_estrutura = atributos_fluxo["funcao_construcao_payload"](df_registro)
        except Exception:
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

            # Estrutura de monitoramento
            # (controle de cessões enviadas movido para cessoes_nota_comercial.json)
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


# =============================================================================
# Funções de construção de payload — Nota Comercial / CERC
# =============================================================================

def _montar_estrutura_registro_nota_comercial(window: Window):
    return F.struct(
        F.col("id_vert").alias("id_externo"),
        F.struct(
            F.struct(
                F.lit("N").alias("ncEmissionTypeCode"),
                F.lit("55639.40-2").alias("otcIssuerAccountCode"),
                F.col("codigo_tipo_emissao").alias("issueTypeCode"),
                F.col("codigo_conta_contabil_otc").alias("otcBookkeeperAccountCode"),
                F.col("informacoes_adicionais.numero_da_emissao").cast("int").alias("issueNumber"),
                F.col("informacoes_adicionais.numero_da_serie").alias("serieNumber"),
                F.col("data_emissao_contrato").alias("issueDate"),
                F.col("max_data_vencimento_parcela").alias("maturityDate"),
                F.col("data_referencia").alias("profitabilityStartDate"),
                F.col("informacoes_adicionais.quantidade_emitida").cast("int").alias("issuedQuantity"),
                F.col("informacoes_adicionais.valor_da_unidade_de_emissao").cast("double").alias("unitIssueValue"),
                (
                    F.col("informacoes_adicionais.quantidade_emitida").cast("double")
                    * F.col("informacoes_adicionais.valor_da_unidade_de_emissao").cast("double")
                ).cast("double").alias("issueFinancialValue"),
                F.col("codigo_regime_tributario").alias("regimeTypeCode"),
                F.col("indicador_processamento_cetip").alias("cetipEventAttendedIndicator"),
                F.lit("PRIVADAVERT").alias("cvmRegistrationNumber"),
                F.lit(1).alias("cvmSequentialNumber"),
                F.lit("2026-01-12").alias("cvmRegistrationDate"),
                F.col("indicador_resgate_antecipado_unilateral").alias("unilateralEarlyRedemptionIndicator"),
                F.col("informacoes_adicionais.nome_fiador").alias("guarantorName"),
                F.col("indicador_presenca_agente_fiduciario").alias("fiduciaryAgentIndicator"),
                F.col("nome_sacado").alias("issuerName"),
                F.col("documento_sacado").alias("issuerDocumentNumber"),
                F.col("informacoes_adicionais.codigo_tipo_garantia").cast("int").alias("guaranteeTypeCode"),
                F.struct(
                    F.lpad(F.col("informacoes_adicionais.codigo_metodo_pagamento"), 2, "0").alias("paymentMethodCode"),
                    F.lpad(F.col("informacoes_adicionais.codigo_do_indexador"), 4, "0").alias("indexCode"),
                    F.when(
                        F.col("informacoes_adicionais.codigo_do_indexador").cast("int").isin(20, 99),
                        F.lit(None),
                    ).otherwise(F.col("percentual_taxa_variavel").cast("float")).alias("rateFloatingPercentage"),
                    F.col("informacoes_adicionais.taxa_contrato").cast("int").alias("interestRateSpread"),
                    F.col("codigo_criterio_calculo_juros").alias("interestCalculationCriteriaCode"),
                ).alias("paymentMethod"),
            ).alias("nc")
        ).alias("data"),
    ).alias("payload")


def cessao_registro_lastro_nota_comercial(df_registro: DataFrame) -> Tuple[Any, Any]:
    """Monta o payload de registro de Nota Comercial.

    Recebe o df já filtrado e enriquecido com colunas da remessa
    (join feito pela conf cessoes_nota_comercial.json).
    """
    try:
        window_contrato = Window.partitionBy("numero_contrato")
        window_max_vencimento = Window.partitionBy("id_cessao", "numero_contrato")

        df_parcelas_struct = (
            df_registro
            .select("numero_contrato")
            .groupBy("numero_contrato")
            .agg(
                F.collect_list(
                    F.struct(
                        F.concat(F.col("numero_contrato"), F.lit("1")).alias("codigo_controle_parcela_contrato_if"),
                        F.lit(1).alias("numero_parcela"),
                    )
                ).alias("parcelas")
            )
        )

    except Exception as e:
        raise RuntimeError(
            "Falha na preparação dos dataframes em 'cessao_registro_lastro_nota_comercial()'"
        ) from e

    try:
        tipo_struct = _montar_estrutura_registro_nota_comercial(window_contrato)

        df_registro_struct = (
            df_registro
            .withColumn("uuid_temp", F.expr("uuid()"))
            .withColumn("id_vert", F.first("uuid_temp").over(window_contrato))
            .drop("uuid_temp")
            .withColumn("informacoes_adicionais", F.regexp_replace("informacoes_adicionais", "'", '"'))
            .withColumn("max_data_vencimento_parcela", F.max("data_vencimento_parcela").over(window_max_vencimento))
            .withColumn(
                "informacoes_adicionais",
                F.from_json(
                    "informacoes_adicionais",
                    """
                    STRUCT<
                        numero_da_emissao: STRING,
                        numero_da_serie: STRING,
                        quantidade_emitida: STRING,
                        valor_da_unidade_de_emissao: STRING,
                        nome_fiador: STRING,
                        codigo_tipo_garantia: STRING,
                        codigo_metodo_pagamento: STRING,
                        codigo_do_indexador: STRING,
                        taxa_contrato: STRING
                    >
                    """,
                ),
            )
            .select("id_operacao", "id_vert", "id_cessao", "numero_contrato", tipo_struct)
            .dropDuplicates(["id_cessao", "numero_contrato"])
            .join(df_parcelas_struct, on=["numero_contrato"], how="inner")
            .withColumn("status", F.lit(""))
        )

        df_registro_struct = df_registro_struct.localCheckpoint()
        df_registro_struct.count()

        headers = F.array(
            F.struct(F.lit("type").alias("key"), F.lit("REGISTRO").cast("binary").alias("value")),
            F.struct(F.lit("kind").alias("key"), F.lit("ENVIO").cast("binary").alias("value")),
        )

    except Exception as e:
        raise RuntimeError(
            "Falha na montagem do payload em 'cessao_registro_lastro_nota_comercial()'"
        ) from e

    try:
        df_envio_registro = (
            df_registro_struct
            .withColumn("value", F.to_json("payload"))
            .select("value", headers.alias("headers"))
        )
        return df_envio_registro, df_registro_struct

    except Exception as e:
        raise RuntimeError(
            "Falha no parsing do payload para JSON em 'cessao_registro_lastro_nota_comercial()'"
        ) from e
