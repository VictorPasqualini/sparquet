# =============================================================================
# SparkFramework — Teste de POC no Databricks (Unity Catalog + Delta Lake)
#
# Como usar:
#   1. Instale a lib no cluster Databricks:
#        %pip install spark-framework==0.2.0
#      Ou diretamente do repositório:
#        %pip install git+https://github.com/sua-org/sparquet.git
#
#   2. Ajuste CATALOG, SCHEMA e as tabelas abaixo para o seu Unity Catalog.
#
#   3. Cole este script em um notebook Databricks e execute célula a célula,
#      ou suba como job via Databricks Workflows.
#
# O framework detecta automaticamente o ambiente Databricks e reutiliza a
# SparkSession existente — não é necessário nenhuma configuração extra.
# =============================================================================

# -----------------------------------------------------------------------------
# Célula 1 — importações
# -----------------------------------------------------------------------------
from spark_framework import SparkFramework

# No Databricks não passe spark={} com master/app_name — o framework ignora
# esses campos e usa a sessão existente do runtime.
fw = SparkFramework()

print(f"Ambiente detectado: {fw._spark_config}")

# -----------------------------------------------------------------------------
# Célula 2 — Pipeline: leitura Delta + transformação + escrita Delta (overwrite)
# -----------------------------------------------------------------------------
# Ajuste os nomes de catálogo/esquema/tabela para o seu ambiente:
CATALOG = "meu_catalog"
SCHEMA  = "meu_schema"

resultado = fw.run_from_dict({
    "name": "poc_clientes_delta",
    "description": "Lê tabela de clientes do Unity Catalog, aplica limpeza e regrava",

    "input": {
        "format": "delta",
        "path": f"{CATALOG}.{SCHEMA}.clientes_raw"
        # Sem 'path' físico: o framework detecta que é nome de tabela pelo '.'
    },

    # ingestion_ts é adicionado automaticamente pelo framework após a leitura.
    # Não é necessário declarar with_column para isso.

    "transformations": [
        # Filtra apenas clientes ativos
        {"type": "filter", "condition": "status = 'ativo'"},

        # Padroniza o email (expressão SQL nativa do Spark)
        {"type": "with_column", "name": "email", "expression": "lower(trim(email))"},

        # Renomeia coluna para padrão snake_case
        {"type": "rename", "mappings": {"clienteId": "cliente_id"}},

        # Remove duplicatas pela chave de negócio
        {"type": "drop_duplicates", "columns": ["cliente_id"]}
    ],

    "validations": {
        "on_failure": "warn",
        "rules": [
            {"type": "not_null", "columns": ["cliente_id", "email"]},
            {"type": "unique",   "columns": ["cliente_id"]},
            {"type": "row_count", "min": 1}
        ]
    },

    "output": {
        "format": "delta",
        "path": f"{CATALOG}.{SCHEMA}.clientes_curated",
        "mode": "overwrite"
    }
})

print(resultado.summary())
if resultado.validation_results:
    for r in resultado.validation_results:
        print(f"  {r}")

# -----------------------------------------------------------------------------
# Célula 3 — Pipeline com MERGE INTO (upsert)
# -----------------------------------------------------------------------------
resultado_merge = fw.run_from_dict({
    "name": "poc_pedidos_upsert",
    "description": "Upsert de pedidos na tabela Delta do Unity Catalog",

    "input": {
        "format": "delta",
        "path": f"{CATALOG}.{SCHEMA}.pedidos_stage"
    },

    "transformations": [
        {"type": "cast", "columns": {"valor_total": "double", "quantidade": "int"}},
        {"type": "filter", "condition": "valor_total > 0"}
    ],

    "output": {
        "format": "delta",
        "path": f"{CATALOG}.{SCHEMA}.pedidos",
        "mode": "merge",
        "options": {
            "merge_keys": ["pedido_id"]
        }
    }
})

print(resultado_merge.summary())

# -----------------------------------------------------------------------------
# Célula 4 — Pipeline com join entre duas tabelas Delta + múltiplos outputs
# -----------------------------------------------------------------------------
resultado_join = fw.run_from_dict({
    "name": "poc_pedidos_enriquecidos",

    "input": {
        "format": "delta",
        "path": f"{CATALOG}.{SCHEMA}.pedidos"
    },

    "transformations": [
        {
            "type": "join",
            "with": {"format": "delta", "path": f"{CATALOG}.{SCHEMA}.produtos"},
            "on":   "produto_id",
            "how":  "left"
        },
        {
            "type": "with_column",
            "name": "receita_total",
            "expression": "quantidade * preco_unitario"
        }
    ],

    # Dois outputs com projeção de colunas diferentes
    "outputs": [
        {
            "format": "delta",
            "path": f"{CATALOG}.{SCHEMA}.pedidos_enriquecidos",
            "mode": "overwrite"
        },
        {
            "format": "csv",
            "path": "/dbfs/tmp/export/pedidos_report",
            "mode": "overwrite",
            "columns": ["pedido_id", "produto_nome", "receita_total", "ingestion_ts"]
        }
    ]
})

print(resultado_join.summary())

# -----------------------------------------------------------------------------
# Célula 5 — Encerramento (opcional — no Databricks não para a sessão)
# -----------------------------------------------------------------------------
# fw.stop() é seguro de chamar: em Databricks ele NÃO para a SparkSession,
# apenas limpa a referência interna do framework.
fw.stop()
print("Framework finalizado.")
