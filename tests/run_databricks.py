# =============================================================================
# SparkFramework — Orquestrador de cessões no Databricks
#
# Instalação no cluster:
#   %pip install git+https://github.com/VictorPasqualini/sparquet.git
#
# Dependência Kafka (libs do cluster ou init script):
#   org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1
#
# Antes de executar:
#   1. Edite os arquivos JSON em tests/databricks/ e substitua:
#      - MEU_CATALOG, MEU_SCHEMA  → seu Unity Catalog
#      - BROKER1:9092             → bootstrap servers do seu Kafka
#   2. Ajuste BASE_PATH abaixo para o caminho dos JSONs no workspace.
# =============================================================================

from spark_framework import SparkFramework

# Caminho da pasta com os JSONs (ajuste para o seu workspace Databricks)
# Exemplo Repos:  "/Workspace/Repos/victor@empresa.com/sparquet/tests/databricks"
# Exemplo DBFS:   "/dbfs/FileStore/spark_framework/configs"
BASE_PATH = "/Workspace/Repos/SEU_USUARIO/sparquet/tests/databricks"

fw = SparkFramework()
# No Databricks o framework detecta automaticamente a SparkSession do runtime.

print("=" * 60)
print("Iniciando processamento de cessoes")
print("=" * 60)

# ---------------------------------------------------------------------------
# Etapa 1: join das tabelas e cache como temp view
# Lê o Delta UMA ÚNICA VEZ e armazena em memória para o loop abaixo.
# ---------------------------------------------------------------------------
r_join = fw.run(f"{BASE_PATH}/01_cessoes_join.json")
print(r_join.summary())
if not r_join.success:
    raise RuntimeError(f"Etapa de join falhou: {r_join.error}")

# ---------------------------------------------------------------------------
# Etapa 2: loop por tipo de ativo
# Cada iteracao le da temp view (sem reler o Delta), gera o payload e escreve
# no Kafka e na tabela de monitoramento.
# ---------------------------------------------------------------------------
TIPOS_ATIVO = ["DUPLICATA", "NOTA_COMERCIAL", "CCB", "CPR"]

resultados = {}
for tipo in TIPOS_ATIVO:
    print(f"\n--- {tipo} ---")
    resultado = fw.run(f"{BASE_PATH}/02_cessoes_{tipo}.json")
    resultados[tipo] = resultado
    print(resultado.summary())
    for v in resultado.validation_results:
        print(f"  {v}")

# ---------------------------------------------------------------------------
# Resumo final
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("RESUMO")
print("=" * 60)
falhas = [t for t, r in resultados.items() if not r.success]
for tipo, res in resultados.items():
    status = "OK" if res.success else "FALHOU"
    print(f"  [{status}] {tipo}: {res.rows_written} registros")

if falhas:
    print(f"\nTipos com falha: {falhas}")
else:
    print("\nTodos os tipos processados com sucesso.")

fw.stop()
