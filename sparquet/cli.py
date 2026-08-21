"""CLI entry point for Sparquet (sparquet <config.json>)."""

import argparse
import sys

from sparquet import Sparquet


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sparquet",
        description="Sparquet - JSON-driven Spark pipeline engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  sparquet examples/01_ingestao_validacoes.json
  sparquet examples/04_merge_delta.json --stop-spark
        """,
    )
    parser.add_argument("config", help="Path to the pipeline JSON config")
    parser.add_argument(
        "--stop-spark",
        action="store_true",
        help="Stop the SparkSession when the run finishes",
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
