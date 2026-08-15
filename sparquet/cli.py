"""CLI entry point para o Sparquet (sparquet <config.json>)."""

import argparse
import sys

from sparquet import Sparquet


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sparquet",
        description="Sparquet — Motor de pipelines Spark orientado a JSON",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  sparquet examples/basic_parquet.json
  sparquet examples/iceberg_upsert.json --stop-spark
  sparquet tests/ingestion_csv_to_parquet.json
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

    fw = Sparquet()
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
