"""
Exemplo de JOIN entre dois CSVs resultando em múltiplos Parquets.

Como rodar:
    cd framework-spark
    python tests/run_join.py
"""
import sys
import os

os.environ["HADOOP_HOME"] = r"E:\hadoop"
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["PATH"] = (
    r"E:\hadoop\bin;"
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot\bin;"
    + os.environ["PATH"]
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from spark_framework import SparkFramework

fw = SparkFramework(spark={"app_name": "JoinOrdersProducts"})

result = fw.run("tests/join_orders_products.json")

print("\n" + "=" * 60)
print(result.summary())
print("=" * 60)

if result.validation_results:
    print("\nResultado das validacoes:")
    for r in result.validation_results:
        print(f"  {r}")

print("\nOutputs gerados em tests/parquet/:")
print("  orders_full/        -> todas as colunas")
print("  orders_analytics/   -> colunas para BI (sem PII)")
print("  orders_export/      -> CSV enxuto para exportacao")

fw.stop()

if not result.success:
    sys.exit(1)
