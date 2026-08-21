"""O CSV que o framework escreve tem de ser legivel por qualquer leitor de CSV.

O `validations.report` e o produto de DQ que mais se le fora do Spark — pandas,
Excel, `csv` do Python. A coluna `rule_params` guarda um JSON, cheio de aspas, e o
default do Spark escapa aspas com `\\"` em vez de dobra-las (`""`), como manda o
RFC 4180. O arquivo voltava partido no meio do campo em todas essas ferramentas: o
`regex` do relatorio aparecia como varias colunas deslocadas.

Este arquivo trava as duas pontas: o que sai do writer e o que o proprio reader do
framework le de volta. Roda com `python tests/test_csv_dialect_spark.py` (ou pytest).
Sem pyspark ou sem Java, a classe e **pulada** — nunca falha.
"""
from __future__ import annotations

import csv
import glob
import io
import os
import shutil
import tempfile
import unittest

try:  # pyspark pode nao estar instalado no ambiente de testes puros
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - ambiente sem pyspark
    SparkSession = None  # type: ignore[assignment]

from sparquet.core.config import InputConfig, OutputConfig
from sparquet.io.csv import CsvReader, CsvWriter

#: Um valor com aspas E virgula — as duas coisas que o dialeto tem de sobreviver.
VALUE = '{"pattern": "^[A-Z]{2}$", "code": "X"}'


class TestCsvDialect(unittest.TestCase):
    spark = None
    tmp = None

    @classmethod
    def setUpClass(cls) -> None:
        if SparkSession is None:
            raise unittest.SkipTest("pyspark nao instalado")
        try:
            cls.spark = (
                SparkSession.builder
                .master("local[1]")
                .appName("sparquet-csv-dialect-tests")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
            )
            cls.spark.createDataFrame([(1,)], "probe int").count()
        except Exception as exc:  # pragma: no cover - ambiente sem Java/Spark
            cls.spark = None
            raise unittest.SkipTest(f"Spark/Java indisponivel: {exc}")
        cls.tmp = tempfile.mkdtemp(prefix="sparquet-csv-")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.spark is not None:
            cls.spark.stop()
        if cls.tmp:
            shutil.rmtree(cls.tmp, ignore_errors=True)

    def _write(self, value: str = VALUE) -> str:
        path = os.path.join(self.tmp, "out")
        shutil.rmtree(path, ignore_errors=True)
        df = self.spark.createDataFrame([(1, value)], "id int, rule_params string")
        CsvWriter(
            self.spark, OutputConfig(format="csv", path=path, mode="overwrite")
        ).write(df.coalesce(1))
        return path

    def test_quotes_are_doubled_not_backslash_escaped(self):
        with io.open(glob.glob(self._write() + "/*.csv")[0], encoding="utf-8") as handle:
            raw = handle.read()
        self.assertIn('""pattern""', raw, "aspas internas deveriam sair dobradas")
        self.assertNotIn('\\"', raw, "escape com barra invertida nao e RFC 4180")

    def test_a_plain_csv_reader_gets_the_value_back_whole(self):
        # O ponto do arquivo: pandas/Excel/csv leem a mesma coisa que o Spark gravou.
        with io.open(glob.glob(self._write() + "/*.csv")[0], encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(rows[0]["rule_params"], VALUE)

    def test_the_framework_reads_back_what_it_wrote(self):
        # Simetria: leitor e escritor no mesmo dialeto. Sem isto, consertar o lado de
        # fora quebraria o de dentro.
        path = self._write()
        back = CsvReader(self.spark, InputConfig(format="csv", path=path)).read().collect()[0]
        self.assertEqual(back["rule_params"], VALUE)

    def test_the_legacy_dialect_is_still_reachable_by_option(self):
        # Arquivos escritos pelo default antigo do Spark (`\\"`) continuam legiveis
        # declarando o escape — a mudanca de default nao fecha a porta.
        path = os.path.join(self.tmp, "legacy")
        shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path)
        with io.open(
            os.path.join(path, "part.csv"), "w", encoding="utf-8", newline="\n"
        ) as handle:
            handle.write('id,rule_params\n1,"{\\"pattern\\": \\"^[A-Z]{2}$\\"}"\n')
        legacy = '{"pattern": "^[A-Z]{2}$"}'
        got = (
            CsvReader(
                self.spark,
                InputConfig(format="csv", path=path, options={"escape": "\\"}),
            )
            .read()
            .collect()[0]["rule_params"]
        )
        self.assertEqual(got, legacy)


if __name__ == "__main__":
    unittest.main(verbosity=2)
