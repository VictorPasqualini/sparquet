"""
Exemplo de uso do SparkFramework como biblioteca.

Executa a ingestão de clientes CSV → Parquet e imprime o resultado.

Como rodar:
    cd framework-spark
    python tests/run_ingestion.py
"""
import sys
import os

os.environ["HADOOP_HOME"] = r"E:\hadoop"
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["PATH"]        = (
    r"E:\hadoop\bin;"
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot\bin;"
    + os.environ["PATH"]
)

# Garante que o pacote seja encontrado ao rodar direto da pasta tests/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from spark_framework import SparkFramework

fw = SparkFramework(
    spark={
        "app_name": "IngestaoClientes",
        "master": "local[*]",
    }
)

result = fw.run("tests/ingestion_csv_to_parquet.json")

print("\n" + "=" * 55)
print(result.summary())
print("=" * 55)

if result.validation_results:
    print("\nResultado das validacoes:")
    for r in result.validation_results:
        print(f"  {r}")

fw.stop()

if not result.success:
    sys.exit(1)
