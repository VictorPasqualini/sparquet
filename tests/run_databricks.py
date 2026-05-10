# =============================================================================
# SparkFramework — POC Databricks: Processamento de Cessões
#
# Fluxo:
#   1. Lê a tabela principal de cessões e faz os joins necessários
#   2. Armazena o resultado em uma temp view cacheada (ViewWriter)
#   3. Para cada tipo de ativo, executa um pipeline que:
#        a. Lê da temp view (ViewReader — sem reler o Delta)
#        b. Filtra pelo tipo de ativo
#        c. Gera as colunas 'payload' (JSON) e 'header' (chave Kafka)
#        d. Escreve no tópico Kafka (payload + header)
#        e. Escreve na tabela Delta de monitoramento (contexto + payload)
#
# Instalação no cluster Databricks:
#   %pip install git+https://github.com/VictorPasqualini/sparquet.git
#
# Dependência Kafka (adicionar nas libs do cluster ou via init script):
#   org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1
#
# Ajuste as constantes na seção CONFIG antes de executar.
# =============================================================================

# =============================================================================
# CÉLULA 1 — Configuração
# =============================================================================

from spark_framework import SparkFramework

# ---------------------------------------------------------------------------
# Ajuste aqui para o seu ambiente
# ---------------------------------------------------------------------------
CATALOG = "meu_catalog"
SCHEMA  = "meu_schema"

KAFKA_BOOTSTRAP = "broker1:9092,broker2:9092"   # bootstrap servers do seu cluster
KAFKA_TOPIC     = "cessoes-processadas"          # tópico de destino

# Tabelas de origem (Unity Catalog)
TBL_CESSOES       = f"{CATALOG}.{SCHEMA}.cessoes"
TBL_OPERACOES     = f"{CATALOG}.{SCHEMA}.operacoes"
TBL_CEDENTES      = f"{CATALOG}.{SCHEMA}.cedentes"
TBL_SACADOS       = f"{CATALOG}.{SCHEMA}.sacados"

# Destinos
TBL_MONITORAMENTO = f"{CATALOG}.{SCHEMA}.cessoes_monitoramento"

# Tipos de ativo que serão processados nesta execução
TIPOS_ATIVO = ["DUPLICATA", "NOTA_COMERCIAL", "CCB", "CPR"]

# Nome interno da temp view cacheada entre os pipelines do loop
VIEW_CESSOES = "cessoes_para_processar"

# ---------------------------------------------------------------------------
fw = SparkFramework()
# No Databricks: usa automaticamente a SparkSession do runtime

print("=" * 60)
print("Iniciando processamento de cessões")
print(f"  tipos: {TIPOS_ATIVO}")
print(f"  tópico: {KAFKA_TOPIC}")
print("=" * 60)


# =============================================================================
# CÉLULA 2 — Etapa 1: Join das tabelas e cache como temp view
#
# Executa UMA VEZ e armazena o resultado em memória.
# Os pipelines do loop reutilizam esse cache sem reler o Delta.
# =============================================================================

resultado_join = fw.run_from_dict({
    "name": "cessoes_join_e_cache",
    "description": "Joins das tabelas de cessões, operações, cedentes e sacados",

    "input": {
        "format": "delta",
        "path": TBL_CESSOES
    },

    # ingestion_ts é adicionado automaticamente pelo framework após a leitura.

    "transformations": [
        # Filtra apenas cessões pendentes de processamento
        {
            "type": "filter",
            "condition": "status_processamento = 'PENDENTE'"
        },

        # Join com operações para trazer dados financeiros da operação
        {
            "type": "join",
            "with": {"format": "delta", "path": TBL_OPERACOES},
            "on":   "operacao_id",
            "how":  "inner"
        },

        # Join com cedentes
        {
            "type": "join",
            "with": {"format": "delta", "path": TBL_CEDENTES},
            "on":   "cedente_id",
            "how":  "left"
        },

        # Join com sacados
        {
            "type": "join",
            "with": {"format": "delta", "path": TBL_SACADOS},
            "on":   "sacado_id",
            "how":  "left"
        },

        # Padronizações comuns a todos os tipos
        {"type": "cast", "columns": {"valor_face": "double", "valor_presente": "double"}},
        {"type": "drop_duplicates", "columns": ["cessao_id"]}
    ],

    "validations": {
        "on_failure": "warn",
        "rules": [
            {"type": "not_null",  "columns": ["cessao_id", "tipo_ativo", "operacao_id"]},
            {"type": "row_count", "min": 1}
        ]
    },

    # ViewWriter: salva o resultado como temp view cacheada.
    # Não escreve em disco — vive apenas na SparkSession.
    "output": {
        "format": "view",
        "path":   VIEW_CESSOES,
        "mode":   "overwrite",
        "options": {"cache": "true"}
    }
})

print(resultado_join.summary())
if not resultado_join.success:
    raise RuntimeError(f"Falha no join: {resultado_join.error}")

print(f"Cessões carregadas e cacheadas: {resultado_join.rows_read}")


# =============================================================================
# CÉLULA 3 — Configurações por tipo de ativo
#
# Cada tipo define:
#   payload_cols  – colunas que compõem o payload JSON (negócio)
#   monitor_cols  – colunas extras na tabela de monitoramento
#   header_expr   – expressão SQL para a chave Kafka (particionamento)
#
# Ajuste as listas de colunas de acordo com o schema real das suas tabelas.
# =============================================================================

TIPO_CONFIG = {
    "DUPLICATA": {
        "payload_cols": [
            "cessao_id", "operacao_id", "cedente_cnpj", "sacado_cpf_cnpj",
            "valor_face", "valor_presente", "data_vencimento",
            "numero_nota_fiscal", "serie_nota_fiscal", "chave_nfe"
        ],
        "monitor_cols": [
            "cessao_id", "operacao_id", "tipo_ativo", "cedente_nome",
            "sacado_nome", "valor_face", "data_vencimento"
        ],
        "header_expr": "concat('DUPLICATA-', cessao_id)"
    },
    "NOTA_COMERCIAL": {
        "payload_cols": [
            "cessao_id", "operacao_id", "cedente_cnpj",
            "valor_face", "valor_presente", "data_emissao", "data_vencimento",
            "numero_emissao", "serie_emissao", "codigo_isin"
        ],
        "monitor_cols": [
            "cessao_id", "operacao_id", "tipo_ativo", "cedente_nome",
            "valor_face", "data_emissao", "data_vencimento"
        ],
        "header_expr": "concat('NC-', cessao_id)"
    },
    "CCB": {
        "payload_cols": [
            "cessao_id", "operacao_id", "cedente_cnpj", "sacado_cpf_cnpj",
            "valor_face", "valor_presente", "taxa_juros", "indice_correcao",
            "data_emissao", "data_vencimento", "numero_ccb"
        ],
        "monitor_cols": [
            "cessao_id", "operacao_id", "tipo_ativo", "cedente_nome",
            "sacado_nome", "valor_face", "taxa_juros", "data_vencimento"
        ],
        "header_expr": "concat('CCB-', cessao_id)"
    },
    "CPR": {
        "payload_cols": [
            "cessao_id", "operacao_id", "cedente_cnpj",
            "valor_face", "valor_presente", "produto_agricola",
            "quantidade_produto", "unidade_medida",
            "data_emissao", "data_vencimento", "codigo_cpr"
        ],
        "monitor_cols": [
            "cessao_id", "operacao_id", "tipo_ativo", "cedente_nome",
            "produto_agricola", "valor_face", "data_vencimento"
        ],
        "header_expr": "concat('CPR-', cessao_id)"
    },
}


# =============================================================================
# CÉLULA 4 — Loop: processa cada tipo de ativo
# =============================================================================

resultados = {}

for tipo in TIPOS_ATIVO:
    cfg = TIPO_CONFIG.get(tipo)
    if cfg is None:
        print(f"[AVISO] Tipo '{tipo}' sem configuração — ignorado.")
        continue

    print(f"\n--- Processando {tipo} ---")

    payload_struct = ", ".join(cfg["payload_cols"])

    resultado = fw.run_from_dict({
        "name": f"cessoes_{tipo}",

        # Lê da temp view cacheada — NÃO relê o Delta
        "input": {
            "format": "view",
            "path":   VIEW_CESSOES
        },

        "transformations": [
            # Isola apenas o tipo de ativo desta iteração
            {
                "type":      "filter",
                "condition": f"tipo_ativo = '{tipo}'"
            },

            # Gera o payload JSON a partir das colunas de negócio do tipo
            # to_json(struct(...)) é uma função nativa do Spark
            {
                "type":       "with_column",
                "name":       "payload",
                "expression": f"to_json(struct({payload_struct}))"
            },

            # Chave Kafka: identifica univocamente a mensagem e define partição
            {
                "type":       "with_column",
                "name":       "header",
                "expression": cfg["header_expr"]
            }
        ],

        "validations": {
            "on_failure": "warn",
            "rules": [
                {"type": "not_null", "columns": ["cessao_id", "payload", "header"]}
            ]
        },

        "outputs": [
            # --- Saída 1: Kafka ---
            # Publica apenas as colunas que o tópico precisa
            {
                "format":  "kafka",
                "path":    KAFKA_TOPIC,
                "mode":    "append",
                "columns": ["header", "payload"],
                "options": {
                    "bootstrap_servers": KAFKA_BOOTSTRAP,
                    "value_column":      "payload",
                    "key_column":        "header"
                }
            },

            # --- Saída 2: Delta de monitoramento ---
            # Inclui contexto de negócio + payload para rastreabilidade
            {
                "format":  "delta",
                "path":    TBL_MONITORAMENTO,
                "mode":    "append",
                "columns": cfg["monitor_cols"] + ["payload", "ingestion_ts"]
            }
        ]
    })

    resultados[tipo] = resultado
    print(resultado.summary())
    for r in resultado.validation_results:
        print(f"  {r}")


# =============================================================================
# CÉLULA 5 — Resumo final
# =============================================================================

print("\n" + "=" * 60)
print("RESUMO")
print("=" * 60)

total_kafka   = 0
total_monitor = 0
falhas        = []

for tipo, res in resultados.items():
    status = "OK" if res.success else "FALHOU"
    print(f"  [{status}] {tipo}: {res.rows_written} registros")
    if res.success:
        total_kafka   += res.rows_written
        total_monitor += res.rows_written
    else:
        falhas.append(tipo)

print(f"\nKafka   → {total_kafka} mensagens publicadas")
print(f"Delta   → {total_monitor} registros gravados em monitoramento")

if falhas:
    print(f"\nAtenção: tipos com falha → {falhas}")
else:
    print("\nTodos os tipos processados com sucesso.")

# fw.stop() é seguro no Databricks: não encerra a SparkSession do runtime
fw.stop()
