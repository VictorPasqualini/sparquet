"""Quanto custa contar as linhas gravadas, dos dois jeitos.

Não é um teste (o nome não começa com `test_`, então `unittest discover` o ignora):
é a medição que justifica `sparquet/core/write_metrics.py`. Compara

- **count** — o comportamento antigo: `output_df.count()` por destino, uma action
  a mais que reexecuta `read → transformations → projeção`;
- **write_metrics** — o atual: o contador que o job da escrita já apura, lido do
  `SQLAppStatusStore`, sem action nenhuma.

Mede também `write` puro (sem contagem alguma), que é o piso: nenhuma estratégia de
contagem pode ficar abaixo dele.

    PYTHONPATH=. python tests/bench_write_metrics.py [linhas] [repetições] [destinos]

Cada combinação roda `repetições` vezes e reporta o **mínimo** — a medida menos
contaminada por GC, JIT e vizinhança. Cada repetição usa um diretório de saída
próprio, para nenhuma medir a limpeza da anterior.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402

from sparquet.core.pipeline import Pipeline  # noqa: E402
from sparquet.core.write_metrics import WriteMetrics  # noqa: E402

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 6_000_000
REPEATS = int(sys.argv[2]) if len(sys.argv) > 2 else 4
DESTINATIONS = int(sys.argv[3]) if len(sys.argv) > 3 else 1


def session() -> SparkSession:
    return (
        SparkSession.builder.master("local[4]")
        .appName("sparquet-bench-write-metrics")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def make_source(spark: SparkSession, root: str) -> str:
    """Uma fonte CSV de verdade: parsing de CSV é o que faz a releitura doer."""
    path = os.path.join(root, "source")
    (
        spark.range(0, ROWS)
        .selectExpr(
            "id",
            "concat('name-', id) as name",
            "cast(id % 97 as int) as bucket",
            "cast(id * 1.5 as double) as amount",
        )
        .write.mode("overwrite")
        .option("header", "true")
        .csv(path)
    )
    return path


def config(source: str, root: str, tag: str) -> dict:
    return {
        "name": f"bench-{tag}",
        "input": {"format": "csv", "path": source, "options": {"header": "true", "inferSchema": "false"}},
        "transformations": [
            {"type": "filter", "condition": "bucket < 90"},
            {"type": "with_column", "column": "flag", "expression": "case when bucket < 10 then 'low' else 'high' end"},
        ],
        "outputs": [
            {"format": "parquet", "path": os.path.join(root, f"{tag}-{index}"), "mode": "overwrite"}
            for index in range(DESTINATIONS)
        ],
    }


def timed(source: str, root: str, tag: str) -> tuple[float, int, str]:
    started = time.perf_counter()
    result = Pipeline.from_dict(config(source, root, tag)).run()
    elapsed = time.perf_counter() - started
    if not result.success:
        raise SystemExit(f"pipeline falhou: {result.error}")
    origin = ",".join(sorted({m.rows_from for m in result.output_metrics}))
    return elapsed, result.rows_written, origin


def bench(name: str, source: str, root: str, disable_metrics: bool) -> float:
    """Roda a mesma carga N vezes e devolve o menor tempo."""
    original = WriteMetrics.available
    if disable_metrics:
        # Desliga a leitura da métrica sem tocar no pipeline: é exatamente o
        # caminho de fallback, que é `count()` — o comportamento antigo.
        WriteMetrics.available = property(lambda self: False)
    try:
        times = []
        for repeat in range(REPEATS):
            elapsed, rows, origin = timed(source, root, f"{name}-{repeat}")
            times.append(elapsed)
            print(f"  {name} #{repeat + 1}: {elapsed:6.2f}s  rows={rows}  from={origin}")
        return min(times)
    finally:
        WriteMetrics.available = original


def bench_write_only(spark: SparkSession, source: str, root: str) -> float:
    """O piso: escrever sem contar nada."""
    times = []
    for repeat in range(REPEATS):
        df = (
            spark.read.option("header", "true").csv(source)
            .filter("bucket < 90")
            .selectExpr("*", "case when bucket < 10 then 'low' else 'high' end as flag")
        )
        target = os.path.join(root, f"floor-{repeat}")
        started = time.perf_counter()
        for index in range(DESTINATIONS):
            df.write.mode("overwrite").parquet(f"{target}-{index}")
        times.append(time.perf_counter() - started)
        print(f"  write-only #{repeat + 1}: {times[-1]:6.2f}s")
    return min(times)


def main() -> None:
    root = tempfile.mkdtemp(prefix="sparquet-bench-")
    spark = session()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        print(f"{ROWS:,} linhas · {DESTINATIONS} destino(s) · {REPEATS} repetições · local[4]")
        source = make_source(spark, root)

        floor = bench_write_only(spark, source, root)
        with_metrics = bench("write_metrics", source, root, disable_metrics=False)
        with_count = bench("count", source, root, disable_metrics=True)

        print()
        print(f"write puro (piso)     {floor:6.2f}s")
        print(f"write_metrics         {with_metrics:6.2f}s   (+{with_metrics - floor:.2f}s sobre o piso)")
        print(f"count (antes)         {with_count:6.2f}s   (+{with_count - floor:.2f}s sobre o piso)")
        gain = (with_count - with_metrics) / with_count * 100
        print(f"ganho                 {with_count - with_metrics:6.2f}s   ({gain:.1f}%)")
    finally:
        spark.stop()
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
