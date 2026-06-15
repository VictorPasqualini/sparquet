
from pyspark.sql import functions as f
from pyspark.sql.functions import col
import traceback
import time
    
try:
    processar_somente_cessoes_pendentes = True

    param_lista_cessoes   = validar_param_lista("cessoes_lista_string", str)
    param_lista_operacoes = validar_param_lista("operacoes_lista_int", int)
    param_tipo_ativo      = validar_param_unico("tipo_ativo_string", str)
    param_registradora    = validar_param_unico("registradora_string", str)
    param_lista_contratos = validar_param_lista("contratos_lista_string", str)

    if param_lista_cessoes or param_lista_contratos:
        processar_somente_cessoes_pendentes = False

    if param_tipo_ativo and param_registradora:
        try: 
            fluxos_operacoes_processamento = {(param_tipo_ativo, param_registradora): FLUXOS_OPERACOES[(param_tipo_ativo, param_registradora)]}
            print(f"✅ TIPO DE ATIVO E REGISTRADORA FILTRADOS: {param_tipo_ativo}-{param_registradora}")
        
        except Exception as e:
            raise ValueError(f'ERRO: NÃO EXISTEM FLUXOS DE OPERAÇÕES MAPEADOS PARA {param_tipo_ativo}-{param_registradora}') 
        
    else:
        fluxos_operacoes_processamento = FLUXOS_OPERACOES
        print("ℹ️ NÃO EXISTEM FILTROS PARA TIPO DE ATIVO E REGISTRADORA")

except Exception as e:
    detalhes_erro = traceback.format_exc()
    raise RuntimeError(f"ERRO ETAPA 1 - AO PROCESSAR PARAMETROS DO JOB PIPELINE \n{detalhes_erro}") from e


try:
    df_cessoes_aprovadas_registro = (
        spark.table("lastros.silver_cessao")
        .select("id_cessao", "id_operacao")
        .join(
            spark.table("lastros.silver_cessoes_status")
            .filter(col("status").isin(1, 10, 11))
            .select("id_cessao")
            .distinct(),
            on=["id_cessao"],
            how="inner",
        )
        .join(
            spark.table("lastros.silver_parametrizacao_registro_lastro"), 
            on=["id_operacao"], 
            how="inner"
        )
    )

except Exception as e:
    detalhes_erro = traceback.format_exc()
    raise RuntimeError(f"ERRO ETAPA 2 - AO MONTAR DATAFRAME DE CESSOES APROVADAS DE REGISTRO \n{detalhes_erro}") from e


try: 
    if param_lista_cessoes:
        print("✅ CESSOES FILTRADAS:", param_lista_cessoes)
        df_cessoes_aprovadas_registro = df_cessoes_aprovadas_registro.filter(
            f.col("id_cessao").isin(param_lista_cessoes)
        )

    if param_lista_operacoes:
        print("✅ OPERACOES FILTRADAS:", param_lista_operacoes)
        df_cessoes_aprovadas_registro = df_cessoes_aprovadas_registro.filter(
            f.col("id_operacao").isin(param_lista_operacoes)
        )

    if param_tipo_ativo:
        print("✅ TIPOS ATIVO FILTRADOS:", param_tipo_ativo)
        df_cessoes_aprovadas_registro = df_cessoes_aprovadas_registro.filter(
            f.col("tipo_ativo") == param_tipo_ativo
        )

    if param_registradora:
        print("✅ REGISTRADORAS FILTRADAS:", param_registradora)
        df_cessoes_aprovadas_registro = df_cessoes_aprovadas_registro.filter(
            f.col("registradora") == param_registradora
        )

except Exception as e:
    detalhes_erro = traceback.format_exc()
    raise RuntimeError(f"ERRO ETAPA 3 - AO PROCESSAR FILTRO DOS PARAMETROS DO JOB PIPELINE \n{detalhes_erro}") from e


try:
    if processar_somente_cessoes_pendentes:
        df_cessoes_aprovadas_registro = df_cessoes_aprovadas_registro.join(
            spark.table("lastros.silver_controle_registro_cessoes")
            .select("id_cessao")
            .distinct(),
            on=["id_cessao"],
            how="leftanti",
        )

    df_cessoes_aprovadas_registro = df_cessoes_aprovadas_registro.localCheckpoint()

    lista_cessoes_pendentes = [
        r.id_cessao
        for r in df_cessoes_aprovadas_registro.select("id_cessao").distinct().collect()
    ]

    if len(lista_cessoes_pendentes) == 0:
        print(f"ℹ️ NÃO EXISTEM CESSÕES PENDENTES DE REGISTRO - ENCERRANDO PIPELINE...")
        dbutils.notebook.exit("SEM_DADOS")
        
except Exception as e:
    detalhes_erro = traceback.format_exc()
    raise RuntimeError(f"ERRO ETAPA 4 - AO MONTAR DATAFRAME E LISTA DE CESSOES PENDENTES DE REGISTRO \n{detalhes_erro}") from e


try:
    df_cessoes_aprovadas_parcialmente = (
        spark.table("lastros.silver_contrato")
        .filter(f.col("id_cessao").isin(lista_cessoes_pendentes))
        .select("id_cessao", "id_operacao", "numero_contrato", "data_referencia")
        .join(
            spark.table("lastros.silver_status_criterios_contratos")
            .filter(f.col("id_cessao").isin(lista_cessoes_pendentes))
            .select("id_cessao", "id_operacao", "numero_contrato", "status_criterio"),
            on=["id_cessao", "id_operacao", "numero_contrato"],
            how="left",
        )
        .withColumn(
            "status_criterio_contrato",
            f.when(f.col("status_criterio").isNull(), f.lit(True))
            .otherwise(f.col("status_criterio")),
        )
        .filter(f.col("status_criterio_contrato") == True)
        .select("id_cessao", "numero_contrato", "data_referencia")
        .distinct()
    )

    df_cessoes_aprovadas_parcialmente = df_cessoes_aprovadas_parcialmente.localCheckpoint()

except Exception as e:
    detalhes_erro = traceback.format_exc()
    raise RuntimeError(f"ERRO ETAPA 5 - AO MONTAR CESSOES APROVADAS PARCIALMENTE \n{detalhes_erro}") from e


try:
    df_tipo_contrato = (
        spark.table("lastros.bronze_remessa")
        .filter(f.col("id_cessao").isin(lista_cessoes_pendentes))
        .select("id_cessao", "numero_contrato", "tipo_contrato")
        .groupBy("id_cessao", "numero_contrato")
        .agg(f.first("tipo_contrato").alias("tipo_contrato"))
    )

    df_multi_ativos = (
        df_cessoes_aprovadas_registro
        .select("id_operacao", "tipo_ativo", "registradora")
        .distinct()
        .groupBy("id_operacao")
        .agg(
            (f.countDistinct(f.struct("tipo_ativo", "registradora")) > 1).alias("multi_ativos")
        )
    )

    df_cessoes_pendentes_de_registro = (
        df_cessoes_aprovadas_registro
        .join(
            df_cessoes_aprovadas_parcialmente.select("id_cessao", "numero_contrato", "data_referencia"),
            on=["id_cessao"],
            how="inner",
        )
        .join(
            df_tipo_contrato.select("id_cessao", "numero_contrato", "tipo_contrato"),
            on=["id_cessao", "numero_contrato"],
            how="left",
        )
        .join(
            df_multi_ativos, 
            on=["id_operacao"], 
            how="left"
        )
    )

    df_cessoes_pendentes_de_registro = df_cessoes_pendentes_de_registro.localCheckpoint()

    df_cessoes_nao_enviadas_para_registro = df_cessoes_pendentes_de_registro

except Exception as e:
    detalhes_erro = traceback.format_exc()
    raise RuntimeError(f"ERRO ETAPA 6 - AO MONTAR DATAFRAME DE CESSOES PENDENTES DE REGISTRO \n{detalhes_erro}") from e