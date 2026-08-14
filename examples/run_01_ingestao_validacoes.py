#!/usr/bin/env python3
"""Executa localmente examples/01_ingestao_validacoes.json (CSV -> CSV) e mostra
o resultado do pipeline + o relatorio de validacoes gerado (validations.report).

Pre-requisitos: Java instalado e `pip install pyspark`. Nao precisa de
`pip install -e .` — este script adiciona a raiz do repo ao sys.path.

Uso (da raiz do repo):
    python examples/run_01_ingestao_validacoes.py
"""
from __future__ import annotations

import os
import sys

# Permite rodar sem instalar o pacote: poe a raiz do repo no sys.path.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from pyspark.sql import SparkSession  # noqa: E402

from sparquet import SparkFramework  # noqa: E402

CONF = os.path.join(REPO_ROOT, "examples", "01_ingestao_validacoes.json")
REPORT_PATH = os.path.join(REPO_ROOT, "examples", "output", "01_validation_report")
OUTPUT_PATH = os.path.join(REPO_ROOT, "examples", "output", "01_customers")


def main() -> int:
    fw = SparkFramework(spark={"app_name": "exemplo-01-ingestao"})
    try:
        result = fw.run(CONF)

        print("\n" + "=" * 64)
        print(result.summary())
        print("=" * 64)

        print("\nValidacoes (PipelineResult.validation_results):")
        for v in result.validation_results:
            print(f"  {v}   [failed_count={v.failed_count}]")

        if not result.success:
            print(f"\nERRO: {result.error}")
            return 1

        spark = SparkSession.builder.getOrCreate()

        print(f"\nRelatorio de validacoes ({REPORT_PATH}):")
        spark.read.option("header", "true").csv(REPORT_PATH).show(truncate=False)

        print(f"Dados processados ({OUTPUT_PATH}):")
        spark.read.option("header", "true").csv(OUTPUT_PATH).show(truncate=False)

        return 0
    finally:
        fw.stop()


if __name__ == "__main__":
    raise SystemExit(main())
