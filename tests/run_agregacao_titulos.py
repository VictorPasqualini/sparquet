"""Executa as confs de agregação de títulos (visão sacado + cedente unificada).

    python tests/run_agregacao_titulos.py            # roda a versão com join
    python tests/run_agregacao_titulos.py sem_join   # roda a versão sem join

Ajuste os paths de input/output nos JSONs (lastros.titulos → sua tabela Iceberg,
lastros.titulos_metricas_entidade → seu destino).
"""
import sys

from spark_framework import SparkFramework

conf = (
    "tests/agregacao_titulos_sem_join.json"
    if len(sys.argv) > 1 and sys.argv[1] == "sem_join"
    else "tests/agregacao_titulos.json"
)

fw = SparkFramework(spark={"app_name": "agregacao_titulos"})

result = fw.run(conf)

print(result.summary())
if not result.success:
    print("ERRO:", result.error)

fw.stop()
