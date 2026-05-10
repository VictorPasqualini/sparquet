"""CLI entry point para o SparkFramework (spark-framework <config.json>)."""

import argparse
import sys

from spark_framework import SparkFramework


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="spark-framework",
        description="SparkFramework — Motor de pipelines Spark orientado a JSON",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  spark-framework examples/basic_parquet.json
  spark-framework examples/iceberg_upsert.json --stop-spark
  spark-framework tests/ingestion_csv_to_parquet.json
        """,
    )
    parser.add_argument("config", help="Caminho para o arquivo JSON de configuracao")
    parser.add_argument(
        "--stop-spark",
        action="store_true",
        help="Encerra a SparkSession ao final",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    fw = SparkFramework()
    result = fw.run(args.config)

    print(result.summary())

    if result.validation_results:
        print("\nValidacoes:")
        for r in result.validation_results:
            print(f"  {r}")

    if args.stop_spark:
        fw.stop()

    if not result.success:
        sys.exit(1)


if __name__ == "__main__":
    main()
