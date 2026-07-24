from typing import Any, Dict, Tuple
from pyspark.sql import functions as f
from pyspark.sql.column import Column
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window


def montar_estrutura_emissao_duplicata_vert(window: Window) -> Column:
    
    cedente_dado = f.expr(
        "filter(array(cedentes), x -> x.documento_cedente = documento_cedente)[0]"
    )

    emissao_struct = f.struct(
        f.col("id_vert").alias("id_externo"),
        f.col("documento_participante").alias("participant_document"),
        f.col("id_cessao").alias("envelope_external_reference"),
        f.array_distinct(
            f.collect_list(
                f.struct(
                    f.col("assinantes.email").alias("email"),
                    f.col("assinantes.nome").alias("name"),
                    f.col("assinantes.pessoa_estrangeira").alias("foreignPerson"),
                    f.col("assinantes.documento").alias("document"),
                    f.col("assinantes.tipo_autenticacao").alias("authType")
                )
            ).over(window)
        ).alias("signer_list"),
        f.array_distinct(
            f.collect_list(
                f.struct(
                    f.concat(f.lit("VERT_"), f.col("numero_contrato")).alias("fileExternalReference"),
                    f.col("produto").alias("product"),
                    f.struct(
                        f.col("tipo_template").cast("int").alias("type") ,
                        f.col("referencia_template").alias("externalReference"),
                        f.struct(
                            f.struct(
                                f.col("nome_sacado").alias("value")
                            ).alias("sacado_nome"),
                            f.struct(
                                f.col("numero_contrato").alias("value")
                            ).alias("duplicata_numero"),
                            f.struct(
                                f.col("logradouro_sacado").alias("value")
                            ).alias("sacado_endereco"),
                            f.struct(
                                f.col("municipio").alias("value")
                            ).alias("sacado_municipio"),
                            f.struct(
                                f.col("uf").alias("value")
                            ).alias("sacado_uf"),
                            f.struct(
                                f.col("valor_face_parcela").alias("value")
                            ).alias("duplicata_valor"),
                            f.struct(
                                f.col("documento_sacado").alias("value")
                            ).alias("sacado_cnpj_cpf"),
                            f.struct(
                                f.lit("municipio").alias("value")
                            ).alias("sacado_praca_pagamento"),
                            f.struct(
                                f.col("data_emissao_duplicata").alias("value")
                            ).alias("duplicata_data_emissao"),
                            f.struct(
                                f.col("cep_sacado").alias("value")
                            ).alias("sacado_cep"),
                            f.struct(
                                f.col("data_vencimento_parcela").alias("value")
                            ).alias("duplicata_vencimento"),
                            f.struct(
                                f.col("valor_face_parcela").alias("value")
                            ).alias("fatura_valor_por_extenso"),
                            f.struct(
                                f.col("cnpj_fundo").alias("value")
                            ).alias("endossatario_cpf_cnpj"),
                            f.struct(
                                f.col("valor_face_parcela").alias("value")
                            ).alias("fatura_valor"),
                            f.struct(
                                f.col("numero_nf_duplicata").alias("value")
                            ).alias("fatura_numero"),
                            f.struct(
                                f.col("nome_fundo").alias("value")
                            ).alias("endossatario_nome"),
                            f.struct(
                                f.lpad(f.col("documento_cedente").cast("string"), 14, "0").alias("value")
                            ).alias("cedente_cnpj_cpf"),
                            f.struct(
                                f.col("nome_cedente").alias("value")
                            ).alias("cedente_nome"),

                            # CEDENTE (AGORA DO ARRAY FILTRADO)
                            f.struct(
                                cedente_dado.cedente_municipio.alias("value")
                            ).alias("cedente_municipio"),
                            f.struct(
                                cedente_dado.cedente_rua.alias("value")
                            ).alias("cedente_rua"),
                            f.struct(
                                cedente_dado.cedente_agencia.alias("value")
                            ).alias("cedente_agencia"),

                            f.struct(
                                cedente_dado.cedente_conta.alias("value")
                            ).alias("cedente_conta"),

                            f.struct(
                                cedente_dado.cedente_bairro.alias("value")
                            ).alias("cedente_bairro"),

                            f.struct(
                                cedente_dado.cedente_inscricao_estadual.alias("value")
                            ).alias("cedente_inscricao_estadual"),

                            f.struct(
                                cedente_dado.cedente_banco.alias("value")
                            ).alias("cedente_banco"),

                            f.struct(
                                cedente_dado.cedente_telefone.alias("value")
                            ).alias("cedente_telefone"),

                            f.struct(
                                cedente_dado.cedente_uf.alias("value")
                            ).alias("cedente_uf"),

                            f.struct(
                                cedente_dado.cedente_cep.alias("value")
                            ).alias("cedente_cep"),

                            f.struct(
                                cedente_dado.cedente_inscricao_municipal.alias("value")
                            ).alias("cedente_inscricao_municipal"),
                        ).alias("inputs")
                    ).alias("template")
                )
            ).over(window)
        ).alias("documents")
    ).alias("payload")

    return emissao_struct

def preparar_objeto_cessao_duplicata_vert(df_registro: Any) -> CessaoBasesAtivos:

    try:
        df_remessa = spark.table("lastros.bronze_remessa")
        df_lastros_relacionamento = spark.table("lastros.silver_lastros_relacionamento")
        df_parcelas = spark.table("lastros.silver_parcela")

        return CessaoBasesAtivos(
            df_cessoes_pendentes_de_registro=df_registro,
            df_remessa=df_remessa,
            df_lastros_relacionamento=df_lastros_relacionamento,
            df_parcelas=df_parcelas
        )
        
    except Exception as e:
        raise RuntimeError(
            "Falha ao preparar objeto 'CessaoBasesAtivos' em 'preparar_objeto_cessao_duplicata_vert()'"
        ) from e

def cessao_registro_lastro_duplicata_vert(obj: CessaoBasesAtivos) -> Tuple[Any, Any]:

    try:
        df_cessoes = obj.df_cessoes_pendentes_de_registro
        df_parcelas = obj.df_parcelas
        df_remessa = (
            obj.df_remessa.select("id_cessao", "numero_contrato", "nome_sacado", "documento_sacado", "tipo_pessoa_sacado",
                                  "municipio_sacado", "nome_cedente", "documento_cedente", "total_parcelas_contrato",
                                  "data_emissao_contrato", "data_vencimento_parcela", "nome_originador", "cnpj_originador",
                                  "informacoes_adicionais", "valor_face_parcela", "tipo_pessoa_cedente", "coobrigacao", "uf_sacado",
                                  "logradouro_sacado", "valor_aquisicao_parcela", "numero_nf_duplicata", "cep_sacado"
                                  )
            .withColumnRenamed("id_cessao", "id_cessao_remessa")
            .withColumnRenamed("numero_contrato", "numero_contrato_remessa")
        )

        df_lastros_relacionamento = obj.df_lastros_relacionamento.select("documento_entidade_relacionada", "municipio", "uf")

        df_contratos = (
            df_cessoes
            .join(df_remessa, (f.col("id_cessao") == f.col("id_cessao_remessa")) & (f.col("numero_contrato") == f.col("numero_contrato_remessa")))
            .join(df_lastros_relacionamento, f.col("documento_entidade_relacionada") == f.col("documento_sacado"), "left")
            .drop("id_cessao_remessa", "numero_contrato_remessa")
        )

        df_parcelas_struct = (
            df_parcelas
            .join(
                df_cessoes
                .select("id_cessao", "numero_contrato")
                .withColumnRenamed("id_cessao", "id_cessao_cessoes")
                .withColumnRenamed("numero_contrato", "numero_contrato_cessoes"),
                (f.col("id_cessao") == f.col("id_cessao_cessoes")) & (f.col("numero_contrato") == f.col("numero_contrato_cessoes")),
                "inner",
                )
            .drop("id_cessao_cessoes", "numero_contrato_cessoes")
            .orderBy("numero_contrato", "numero_parcela")
            .groupBy("id_cessao", "numero_contrato")
            .agg(
                f.collect_list(
                    f.struct(
                        f.col("identificador_parcela").alias("codigo_controle_parcela_contrato_if"),
                        f.col("data_vencimento_parcela").alias("data_vencimento_parcela"),
                        f.col("valor_face_parcela").cast("decimal(15,2)").alias("valor_parcela"),
                        f.col("valor_aquisicao_parcela")
                        .cast("decimal(15, 2)")
                        .alias("valor_principal_parcela"),
                        f.col("numero_parcela").alias("numero_parcela"),
                        )
                ).alias("parcelas")
            )
        )
    
    except Exception as e:
        raise RuntimeError(
            "Falha na preparação dos dataframes em 'cessao_registro_lastro_duplicata_vert()'"
        )
    
    try:

        contrato_rank_window = Window.partitionBy("id_cessao").orderBy("numero_contrato")

        df_contratos = (
            df_contratos
            .withColumn("assinantes", f.explode("assinantes"))
            .withColumn("cedentes", f.explode("cedentes"))
            .withColumn("contrato_rank", f.dense_rank().over(contrato_rank_window))
            .withColumn("lote_index", ((f.col("contrato_rank") - 1) / REGISTROS_POR_GRUPO).cast("int"))
        )

        window = Window.partitionBy("id_cessao", "lote_index")
        tipo_struct = montar_estrutura_emissao_duplicata_vert(window)


        df_registro_struct = (
            df_contratos
            .withColumn("uuid_temp", f.expr("uuid()"))
            .withColumn("id_vert", f.first("uuid_temp").over(window))
            .drop("uuid_temp")
            .withColumn("informacoes_adicionais", f.regexp_replace("informacoes_adicionais", "'", '"'))
            .withColumn("data_emissao_duplicata", f.current_date())
            .withColumn(
                "informacoes_adicionais",
                f.from_json(
                    "informacoes_adicionais",
                    """
                    STRUCT<
                        codigo_m_oper_cred: INT,
                        codigo_s_m_oper_cred: INT,
                        chave_unica_da_duplicata: STRING,
                        especie_da_duplicata: STRING,
                        data_emissao_da_duplicata: STRING,
                        natureza_da_operacao: STRING
                    >
                    """
                )
            )
            .select(
                "id_operacao",
                "id_vert",
                "id_cessao",
                "numero_contrato",
                "lote_index",
                tipo_struct
            )
        )

        df_registro_struct = df_registro_struct.dropDuplicates(["id_cessao", "lote_index"])

        df_registro_struct = (
            df_registro_struct
            .join(df_parcelas_struct.select("id_cessao", "numero_contrato", "parcelas"), on=["id_cessao", "numero_contrato"], how="inner")
            .withColumn("status", f.lit(""))
        )

        df_registro_struct.count()
        df_registro_struct = df_registro_struct.localCheckpoint()

        headers = f.array(
            f.struct(f.lit("type").alias("key"), f.lit("REGISTRO").cast("binary").alias("value")),
            f.struct(f.lit("kind").alias("key"), f.lit("ENVIO").cast("binary").alias("value")),
        )
        
    except Exception as e:
        raise RuntimeError(
            "Falha na montagem do payload em 'cessao_registro_lastro_duplicata()'"
        )
   
    try:     
        df_envio_registro = (
            df_registro_struct
            .withColumn("value", f.to_json("payload"))
            .select("value", headers.alias("headers"))
        )

        if fluxo_operacao != "REGISTRO":
            df_registro_struct = (
                df_registro_struct
                .drop("numero_contrato")
                .join(
                    df_remessa
                    .withColumnRenamed("id_cessao_remessa", "id_cessao")
                    .withColumnRenamed("numero_contrato_remessa", "numero_contrato")
                    .select("id_cessao", "numero_contrato"),
                    on=["id_cessao"],
                    how="left"
                )
            )
            
        return df_envio_registro, df_registro_struct

    except Exception as e:
        raise RuntimeError(
            "Falha no parsing do payload para JSON em 'cessao_registro_lastro_duplicata()'"
        )