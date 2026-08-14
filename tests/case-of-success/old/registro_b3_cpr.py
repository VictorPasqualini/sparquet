from typing import Any, Dict, Tuple
from pyspark.sql import functions as f
from pyspark.sql.window import Window

def preparar_objeto_cessao_cpr(df_registro: Any) -> CessaoBasesAtivos:
    
    try:
        df_remessa = spark.table("lastros.bronze_remessa")
        df_dados_auxiliares = spark.table("lastros.silver_dados_auxiliares")

        return CessaoBasesAtivos(
            df_cessoes_pendentes_de_registro=df_registro,
            df_dados_auxiliares=df_dados_auxiliares,
            df_remessa=df_remessa,
        )
    
    except Exception as e:
        raise RuntimeError(
            "Falha ao preparar objeto 'CessaoBasesAtivos' em 'preparar_objeto_cessao_cpr()'"
        ) from e 

def cessao_registro_lastro_cpr(obj: CessaoBasesAtivos) -> Tuple[Any, Any]:
    
    try:
        df_cessoes = obj.df_cessoes_pendentes_de_registro
        df_remessa = obj.df_remessa
        df_dados_auxiliares = obj.df_dados_auxiliares
        
        df_remessa = (
            df_remessa.select(
                "id_cessao",
                "numero_contrato",
                "nome_sacado",
                "documento_sacado",
                "tipo_pessoa_sacado",
                "municipio_sacado",
                "nome_cedente",
                "documento_cedente",
                "total_parcelas_contrato",
                "data_emissao_contrato",
                "data_vencimento_parcela",
                "nome_originador",
                "cnpj_originador",
                "taxa_contrato"
            )
        )
        
        df_dados_auxiliares = (
            df_dados_auxiliares.select(
                "informacoes_adicionais",
                "numero_contrato",
                "data_referencia"
            )
        )

        df_contratos = (
            df_cessoes
            .join(
                df_remessa, 
                on=["id_cessao", "numero_contrato"], 
                how="left"
            )
            .join(
                df_dados_auxiliares, 
                on=["numero_contrato", "data_referencia"], 
                how="left"
            )
        )

        # Define o tipo de cpr para condicionalmente adicionar campos no struct, se não encontra, atribui valor padrão
        tipo_codigo_cpr = df_contratos.select("tipo_codigo_cpr").first()
        tipo_codigo_cpr = tipo_codigo_cpr["tipo_codigo_cpr"] if tipo_codigo_cpr else "F"

        window = Window.partitionBy("numero_contrato")

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
            "Falha na preparação dos dataframes em 'cessao_registro_lastro_cpr()'"
        )

    try:
        instrument_base = [
            f.col("tipo_codigo_cpr").alias("cprTypeCode"),
            f.col("codigo_registro_conta_otc").alias("otcRegisterAccountCode"),
            f.col("codigo_conta_custodiante_otc").alias("otcCustodianAccountCode"),
            f.when(f.col("indicador_emissao_eletronica") == True, f.lit("S"))
            .when(f.col("indicador_emissao_eletronica") == False, f.lit("N"))
            .otherwise(f.lit("N")).alias("electronicEmissionIndicator"),
            f.col("data_emissao_contrato").alias("issueDate"),
            f.col("data_vencimento_parcela").alias("maturityDate"),
            f.col("dados_auxiliares.quantidade_parcela_contratada").cast("string").alias("issueQuantity"),
            # f.col("dados_auxiliares.valor_face_parcela").cast("string").alias("issueValue"),
            # f.col("dados_auxiliares.valor_face_contrato").cast("string").alias("issueFinancialValue"),
            f.lit("10000000").alias("issueValue"),
            f.lit("16000000").alias("issueFinancialValue"),
            f.lit("2026-06-11").alias("profitabilityStartDate"),
            # f.col("data_referencia").alias("profitabilityStartDate"),
            f.when(f.col("indicador_expiracao") == True, f.lit("S"))
            .when(f.col("indicador_expiracao") == False, f.lit("N"))
            .otherwise(f.lit("N")).alias("automaticExpirationIndicator"),

            # collaterals
            f.array_distinct(
                f.collect_list(
                    f.struct(f.col("dados_auxiliares.tipo_garantia").alias("collateralTypeCode"))
                ).over(window)
            ).alias("collaterals"),

            # products
            f.array_distinct(
                f.collect_list(
                    f.struct(
                        f.lit("FEIJAO").alias("cprProductName"),
                        #f.col("dados_auxiliares.nome_produto").alias("cprProductName"),
                        f.col("dados_auxiliares.classe_produto").alias("cprProductClassName"),
                        f.col("dados_auxiliares.safra_produto").alias("cprProductHarvest"),
                        # f.col("dados_auxiliares.descricao_produto").alias("cprProductDescription"),
                        f.lit("16000").alias("cprProductQuantity"),
                        f.lit("FEIJAO").alias("cprProductDescription"),
                        # f.col("dados_auxiliares.quantidade_produto").cast("string").alias("cprProductQuantity"),
                        f.lit("UNIDADE").alias("measureUnitName"),
                        #f.col("dados_auxiliares.unidade_medida").alias("measureUnitName"),
                        f.col("dados_auxiliares.tipo_armazenamento").alias("packagingWayName"),
                        f.col("dados_auxiliares.status_produto").alias("cprProductStatusCode"),
                        f.col("dados_auxiliares.tipo_producao").alias("productionTypeCode")
                    )
                ).over(window)
            ).alias("products"),

            # issuers
            f.array_distinct(
                f.collect_list(
                    f.struct(
                        f.col("nome_sacado").alias("cprIssuerName"),
                        f.when(
                            f.col("tipo_pessoa_sacado") == 0,
                            f.lpad(f.col("documento_sacado"), 14, "0")
                        ).otherwise(
                            f.lpad(f.col("documento_sacado"), 11, "0")
                        ).alias("documentNumber"), # cessão
                        f.when(f.col("tipo_pessoa_sacado") == "0", f.lit("PJ"))
                        .when(f.col("tipo_pessoa_sacado") == "1", f.lit("PF"))
                        .otherwise(f.lit("PF")).alias("personTypeAcronym"),
                        f.col("dados_auxiliares.uf_emissor").alias("stateAcronym"),
                        f.lit("CASCAVEL").alias("cityName"),
                        #f.upper(f.col("municipio_sacado")).alias("cityName"),
                        f.lpad(f.col("dados_auxiliares.natureza_juridica_emissor"), 2, "0").alias("issuerLegalNatureCode"),
                    )
                ).over(window)
            ).alias("issuers"),

            # scr
            f.struct(
                f.col("dados_auxiliares.ativo_informado_scr").alias("scrTypeCode"),
                f.col("numero_contrato").alias("contractCode"),
                f.col("dados_auxiliares.codigo_finalidade").alias("finalityCode")
            ).alias("scr"),

            # creditor
            f.struct(
                f.col("nome_originador").alias("creditorName"),
                f.lpad(f.col("cnpj_originador"), 14, "0").alias("documentNumber")
            ).alias("creditor"),

            # deposit
            f.struct(
                f.col("codigo_conta_favorecida_otc").alias("otcFavoredAccountCode"),
                f.coalesce(f.col("numero_documento_cpr"), f.lit("")).alias("documentNumber"),
                f.coalesce(f.col("tipo_pessoa_sigla_cpr"), f.lit("")).alias("personTypeAcronym"),
                f.col("numero_contrato").alias("selfNumber"),
                f.col("tipo_modalidade_liquidacao").alias("settlementModalityTypeCode"),
                f.col("total_parcelas_contrato").alias("depositQuantity"),
            ).alias("deposit"),

            # productionPlaces
            f.array_distinct(
                f.collect_list(
                    f.struct(
                        f.col("dados_auxiliares.local_producao").alias("productionPlaceName"),
                        f.col("dados_auxiliares.cep_local_producao").cast("string").alias("zipCode")
                    )
                ).over(window)
            ).alias("productionPlaces"),
        ]

        # Adiciona condicionalmente o paymentMethod
        if tipo_codigo_cpr == "F":
            instrument = f.struct(
                *instrument_base,
                f.struct(
                    f.col("codigo_metodo_pagamento_cpr").alias("paymentMethodCode"),
                    f.col("codigo_index_cpr").alias("indexCode"),
                    f.col("taxa_contrato").alias("interestRateSpreadPercentage"),
                    f.col("codigo_tipo_taxa_juros_cpr").alias("interestRateCriteriaTypeCode"),
                ).alias("paymentMethod")
            )
            
        else:
            instrument = f.struct(*instrument_base)

        struct_registro = f.struct(
            f.col("id_vert").alias("id_externo"),
            f.struct(instrument.alias("instrument")).alias("data")
        ).alias("payload")

        df_contratos.display()

        df_registro_struct = (
            df_contratos
            .withColumn("uuid_temp", f.expr("uuid()"))
            .withColumn("id_vert", f.first("uuid_temp").over(window))
            .drop("uuid_temp")
            .withColumn("dados_auxiliares", f.explode("informacoes_adicionais"))
            .select(
                "id_operacao",
                "id_vert",
                "id_cessao",
                "numero_contrato",
                struct_registro
            )
            .dropDuplicates(["id_cessao", "numero_contrato"])
            .join(df_parcelas_struct, on=["numero_contrato"], how="inner")
            .withColumn("status", f.lit(""))
        )

        df_registro_struct.display()

        df_registro_struct = df_registro_struct.localCheckpoint()
        df_registro_struct.count()

        headers = f.array(
            f.struct(f.lit("type").alias("key"), f.lit("REGISTRO").cast("binary").alias("value")),
            f.struct(f.lit("kind").alias("key"), f.lit("ENVIO").cast("binary").alias("value")),
        )
    
    except Exception as e:
        raise RuntimeError(
            "Falha na montagem do payload em 'cessao_registro_lastro_cpr()'"
        )

    try:
        df_envio_registro = (
            df_registro_struct.withColumn("value", f.to_json("payload")).select("value", headers.alias("headers"))
        )

        return df_envio_registro, df_registro_struct
    
    except Exception as e:
        raise RuntimeError(
            "Falha no parsing do payload para JSON em 'cessao_registro_lastro_cpr()'"
        ) from e