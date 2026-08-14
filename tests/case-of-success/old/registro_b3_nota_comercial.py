from typing import Any, Dict, Tuple
from pyspark.sql import functions as f
from pyspark.sql.column import Column
from pyspark.sql.types import DecimalType 
from pyspark.sql.window import Window

def montar_estrutura_registro_nota_comercial(window: Window) -> Column:
    
    registro_struct = f.struct(
        f.col("id_vert").alias("id_externo"),
        f.struct(
            f.struct(
                f.lit("N").alias("ncEmissionTypeCode"), # fixo
                f.lit("55639.40-2").alias("otcIssuerAccountCode"), # fixo
                f.col("codigo_tipo_emissao").alias("issueTypeCode"), # parametrizacao
                f.col("codigo_conta_contabil_otc").alias("otcBookkeeperAccountCode"), # parametrizacao
                f.col("informacoes_adicionais.numero_da_emissao").cast("int").alias("issueNumber"), # cessão
                f.col("informacoes_adicionais.numero_da_serie").alias("serieNumber"), # cessão
                f.col("data_emissao_contrato").alias("issueDate"), # cessão
                f.col("max_data_vencimento_parcela").alias("maturityDate"), # cessão
                f.col("data_referencia").alias("profitabilityStartDate"), # cessão
                f.col("informacoes_adicionais.quantidade_emitida").cast("int").alias("issuedQuantity"), # cessão
                f.col("informacoes_adicionais.valor_da_unidade_de_emissao").cast("double").alias("unitIssueValue"), # cessão
                (f.col("informacoes_adicionais.quantidade_emitida").cast("double") * f.col("informacoes_adicionais.valor_da_unidade_de_emissao").cast("double")).cast("double").alias("issueFinancialValue"), # issuedQuantity * unitIssueValue
                f.col("codigo_regime_tributario").alias("regimeTypeCode"), # parametrizacao
                f.col("indicador_processamento_cetip").alias("cetipEventAttendedIndicator"), # parametrizacao
                f.lit("PRIVADAVERT").alias("cvmRegistrationNumber"), # fixo
                f.lit(1).alias("cvmSequentialNumber"), # fixo
                f.lit("2026-01-12").alias("cvmRegistrationDate"), # fixo
                f.col("indicador_resgate_antecipado_unilateral").alias("unilateralEarlyRedemptionIndicator"), # parametrizacao
                f.col("informacoes_adicionais.nome_fiador").alias("guarantorName"), # cessão
                f.col("indicador_presenca_agente_fiduciario").alias("fiduciaryAgentIndicator"), # parametrização
                f.col("nome_sacado").alias("issuerName"), # cessão
                f.when(
                    f.col("tipo_pessoa_sacado") == 0,
                    f.lpad(f.col("documento_sacado"), 14, "0")
                ).otherwise(
                    f.lpad(f.col("documento_sacado"), 11, "0")
                ).alias("issuerDocumentNumber"), # cessão
                f.col("informacoes_adicionais.codigo_tipo_garantia").cast("int").alias("guaranteeTypeCode"), # cessão
                f.struct(
                    f.lpad(f.col("informacoes_adicionais.codigo_metodo_pagamento"), 2, '0').alias("paymentMethodCode"), # cessão
                    f.lpad(f.col("informacoes_adicionais.codigo_do_indexador"), 4, '0').alias("indexCode"), # cessão
                    f.when(
                        (f.col("informacoes_adicionais.codigo_do_indexador").cast("int").isin(20, 99)), 
                        f.lit(None)
                    ).otherwise(f.col("percentual_taxa_variavel").cast("float")).alias("rateFloatingPercentage"), # parametrizacao percentual_taxa_variavel
                    f.col("informacoes_adicionais.taxa_contrato").cast("int").alias("interestRateSpread"), # cessao
                    f.col("codigo_criterio_calculo_juros").alias("interestCalculationCriteriaCode") # parametrizacao
                ).alias("paymentMethod")
            ).alias("nc")
        ).alias("data")
    ).alias("payload")

    return registro_struct

def preparar_objeto_cessao_nota_comercial(df_registro: Any) -> CessaoBasesAtivos:

    try:
        df_remessa = spark.table("lastros.bronze_remessa")

        return CessaoBasesAtivos(
            df_cessoes_pendentes_de_registro=df_registro,
            df_remessa=df_remessa,
        )
        
    except Exception as e:
        raise RuntimeError(
            "Falha ao preparar objeto 'CessaoBasesAtivos' em 'preparar_objeto_cessao_nota_comercial()'"
        ) from e

def cessao_registro_lastro_nota_comercial(obj: CessaoBasesAtivos) -> Tuple[Any, Any]:

    try:
        df_cessoes = obj.df_cessoes_pendentes_de_registro
        df_remessa = (
            obj.df_remessa.select("id_cessao", "numero_contrato", "numero_parcela", "nome_sacado", "documento_sacado", "tipo_pessoa_sacado",
                                  "data_emissao_contrato", "data_vencimento_parcela", "informacoes_adicionais"
                                )
            .withColumnRenamed("id_cessao", "id_cessao_remessa")
            .withColumnRenamed("numero_contrato", "numero_contrato_remessa")
        )

        df_contratos = (
            df_cessoes
            .join(
                df_remessa, 
                (f.col("id_cessao") == f.col("id_cessao_remessa")) & (f.col("numero_contrato") == f.col("numero_contrato_remessa"))
            )
            .drop("id_cessao_remessa", "numero_contrato_remessa")
        )

        df_parcelas_struct = (
            df_contratos
            .select("numero_contrato")
            .groupBy("numero_contrato")
            .agg(
                f.collect_list(
                    f.struct(
                        f.concat(f.col("numero_contrato"), f.lit("1")).alias("codigo_controle_parcela_contrato_if"),
                        f.lit(1).alias("numero_parcela"),
                    )
                ).alias("parcelas")
            )
        )
    
    except Exception as e:
        raise RuntimeError(
            "Falha na preparação dos dataframes em 'cessao_registro_lastro_nota_comercial()'"
        )
    
    try:
        window = Window.partitionBy("numero_contrato")
        tipo_struct = montar_estrutura_registro_nota_comercial(window)

        window_max_data_vencimento_parcela = Window.partitionBy("id_cessao", "numero_contrato")

        df_registro_struct = (
            df_contratos
            .withColumn("uuid_temp", f.expr("uuid()"))
            .withColumn("id_vert", f.first("uuid_temp").over(window))
            .drop("uuid_temp")
            .withColumn("informacoes_adicionais", f.regexp_replace("informacoes_adicionais", "'", '"'))
            .withColumn("max_data_vencimento_parcela", f.max("data_vencimento_parcela").over(window_max_data_vencimento_parcela))
            .withColumn(
                "informacoes_adicionais",
                f.from_json(
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
                    """
                )
            )
            .select(
                "id_operacao",
                "id_vert",
                "id_cessao",
                "numero_contrato",
                tipo_struct
            )
            .dropDuplicates(["id_cessao", "numero_contrato"])
            .join(df_parcelas_struct, on=["numero_contrato"], how="inner")
            .withColumn("status", f.lit(""))
        )

        df_registro_struct = df_registro_struct.localCheckpoint()
        df_registro_struct.count()

        headers = f.array(
            f.struct(f.lit("type").alias("key"), f.lit("REGISTRO").cast("binary").alias("value")),
            f.struct(f.lit("kind").alias("key"), f.lit("ENVIO").cast("binary").alias("value")),
        )
        
    except Exception as e:
        raise RuntimeError(
            "Falha na montagem do payload em 'cessao_registro_lastro_nota_comercial()'"
        )
   
    try:     
        df_envio_registro = (
            df_registro_struct
            .withColumn("value", f.to_json("payload"))
            .select("value", headers.alias("headers"))
        )
            
        return df_envio_registro, df_registro_struct

    except Exception as e:
        raise RuntimeError(
            "Falha no parsing do payload para JSON em 'cessao_registro_lastro_duplicata()'"
        )