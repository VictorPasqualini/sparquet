#!/usr/bin/env python3
"""Cria um warehouse Iceberg de exemplo para abrir no SQL editor do Studio.

Gera duas tabelas — `sales.orders` e `sales.order_items` — em um catalogo Hadoop,
ou seja, diretorios comuns no disco. Assim as tabelas sao lidas das duas formas:

    local.sales.orders            (pelo catalogo, como num metastore)
    <warehouse>/sales/orders      (pelo caminho, que e o que o SQL editor manda)

Os `customer_id` batem com a tabela Delta de clientes do exemplo, entao da para
testar join entre Iceberg e Delta na mesma query.

Pre-requisitos: Java instalado e `pip install pyspark`. O jar do Iceberg e
baixado do Maven na primeira execucao.

Uso (da raiz do repo):
    python examples/seed_iceberg_demo.py                  # warehouse padrao
    python examples/seed_iceberg_demo.py E:/data/iceberg  # outro diretorio
"""
from __future__ import annotations

import csv
import datetime as dt
import os
import random
import shutil
import sys
import tempfile

from pyspark.sql import SparkSession

#: Onde as tabelas ficam quando nenhum caminho e passado. E o mesmo lugar em que
#: os demais exemplos de lake deste repo escrevem.
DEFAULT_WAREHOUSE = "/data/iceberg"

#: O runtime do Iceberg para a linha do Spark 4. Em Spark 3.5 troque por
#: `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0`.
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"

CHANNELS = ["web", "store", "partner", "phone"]
STATUS = ["placed", "shipped", "delivered", "returned", "cancelled"]
FIRST_DAY = dt.date(2025, 1, 1)


def _write_csv(path: str, header: list, rows: list) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def _orders(count: int = 600) -> list:
    return [
        [
            1000 + i,
            random.randint(1, 40),
            (FIRST_DAY + dt.timedelta(days=random.randint(0, 240))).isoformat(),
            random.choice(CHANNELS),
            random.choices(STATUS, weights=[10, 20, 55, 8, 7])[0],
            random.randint(1, 9),
            round(random.uniform(18.5, 1450.0), 2),
        ]
        for i in range(count)
    ]


def _order_items(order_count: int = 600) -> list:
    rows = []
    for i in range(order_count * 2):
        rows.append([
            1000 + (i // 2),
            (i % 2) + 1,
            f"SKU-{random.randint(100, 260)}",
            random.randint(1, 5),
            round(random.uniform(9.9, 320.0), 2),
        ])
    return rows


def main(warehouse: str) -> None:
    random.seed(7)
    # Os dados nascem em CSV e sobem pelo `spark.read.csv` de proposito: em
    # master local o `createDataFrame` a partir de linhas do driver cria um
    # worker Python, e e ali que o ambiente costuma quebrar. Ler CSV e caminho
    # so de JVM.
    staging = tempfile.mkdtemp(prefix="iceberg-seed-")
    try:
        _write_csv(
            os.path.join(staging, "orders.csv"),
            ["order_id", "customer_id", "order_date", "channel", "status", "items", "amount"],
            _orders(),
        )
        _write_csv(
            os.path.join(staging, "order_items.csv"),
            ["order_id", "line_no", "sku", "quantity", "unit_price"],
            _order_items(),
        )

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("seed-iceberg-demo")
            .config("spark.jars.packages", ICEBERG_PACKAGE)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        for name, source in (
            ("orders", "orders.csv"),
            ("order_items", "order_items.csv"),
        ):
            frame = spark.read.option("header", True).option("inferSchema", True).csv(
                os.path.join(staging, source).replace("\\", "/")
            )
            frame.writeTo(f"local.sales.{name}").createOrReplace()
            print(f"local.sales.{name}: {spark.table(f'local.sales.{name}').count()} linhas")

        # A leitura que o SQL editor faz: pelo caminho, sem catalogo nenhum.
        path = f"{warehouse.rstrip('/')}/sales/orders"
        print(f"{path}: {spark.read.format('iceberg').load(path).count()} linhas pelo caminho")
        spark.stop()
    finally:
        shutil.rmtree(staging, ignore_errors=True)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_WAREHOUSE)
