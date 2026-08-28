"""Round-trip dos formatos nativos: o que o writer grava, o reader le de volta.

Sao os seis formatos que rodam sem jar extra nem servico externo — `parquet`,
`orc`, `json`, `csv`, `txt` e `view`. Os demais conectores (delta, iceberg, jdbc,
bigquery, kafka, ...) tem teste de *montagem de opcoes*, porque exigem dependencia
ou rede; estes aqui sao os unicos em que da para exercitar o caminho inteiro —
escrever de verdade e reler de verdade — dentro da suite.

O que cada teste trava e a promessa que o usuario faz ao escolher o formato:

  parquet/orc  guardam o schema junto com os dados: volta igual, tipo por tipo.
  json/csv     sao texto e perdem tipo; a promessa e o *valor*, nao o tipo, e a
               comparacao aqui e feita como texto de proposito.
  txt          uma coluna de string, uma linha por registro, na ordem gravada.
  view         nao toca disco: registra na sessao e volta identico.

O DataFrame de teste nasce de `spark.sql(... VALUES ...)`, nao de
`createDataFrame`: o segundo sobe um worker Python, e em master local com o
`PYSPARK_PYTHON` desalinhado o arquivo inteiro morreria por um motivo que nao tem
nada a ver com formato de arquivo. Aqui tudo o que roda e JVM.

Roda com `PYTHONPATH=. python tests/test_formats_roundtrip_spark.py` (ou pytest). Sem pyspark
ou sem Java a classe e **pulada** — nunca falha.
"""
from __future__ import annotations

import datetime
import os
import shutil
import sys
import tempfile
import unittest

try:  # pyspark pode nao estar instalado no ambiente de testes puros
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - ambiente sem pyspark
    SparkSession = None  # type: ignore[assignment]

from sparquet.core.config import InputConfig, OutputConfig
from sparquet.io.factory import ReaderFactory, WriterFactory

# Mesma razao de `sparquet/core/context.py`: em master local o worker precisa ser
# o mesmo Python do driver.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

#: Nome e tipo de cada coluna — o tipo tambem serve para escrever o NULL tipado.
COLUMNS = (
    ("id", "INT"),
    ("nome", "STRING"),
    ("valor", "DOUBLE"),
    ("ativo", "BOOLEAN"),
    ("dia", "DATE"),
)

#: Uma linha comum, uma cheia de caractere que quebra texto delimitado, e uma
#: quase toda nula — o nulo e o que mais some numa ida e volta por texto.
ROWS = (
    (1, "alpha", 1.5, True, datetime.date(2024, 1, 31)),
    (2, 'com "aspas", virgula e acento: cessao', -0.25, False, datetime.date(2024, 2, 29)),
    (3, None, None, None, None),
)

FILE_FORMATS = ("parquet", "orc", "json", "csv")

TXT_LINES = ("primeira linha", "segunda, com virgula", 'terceira com "aspas"')


def sql_literal(value, tipo: str) -> str:
    if value is None:
        return f"CAST(NULL AS {tipo})"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime.date):
        return f"DATE'{value.isoformat()}'"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    return f"CAST({value} AS {tipo})"


def fixture_sql() -> str:
    """As linhas de ROWS como uma relacao literal, tipada, sem worker Python."""
    tuples = ", ".join(
        "(" + ", ".join(sql_literal(value, tipo) for value, (_, tipo) in zip(row, COLUMNS)) + ")"
        for row in ROWS
    )
    names = ", ".join(name for name, _ in COLUMNS)
    return f"SELECT * FROM VALUES {tuples} AS t({names})"


def as_text(value) -> str:
    """Valor comparavel entre formatos que guardam tipo e formatos que nao guardam."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class TestNativeFormatsRoundTrip(unittest.TestCase):
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
                .appName("sparquet-format-roundtrip-tests")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
            )
            cls.spark.sql("SELECT 1").count()
        except Exception as exc:  # pragma: no cover - ambiente sem Java/Spark
            cls.spark = None
            raise unittest.SkipTest(f"Spark/Java indisponivel: {exc}")
        cls.tmp = tempfile.mkdtemp(prefix="sparquet-roundtrip-")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.spark is not None:
            cls.spark.stop()
        if cls.tmp:
            shutil.rmtree(cls.tmp, ignore_errors=True)

    # ---- infra

    def source(self):
        return self.spark.sql(fixture_sql())

    def path_for(self, name: str) -> str:
        path = os.path.join(self.tmp, name)
        shutil.rmtree(path, ignore_errors=True)
        return path

    def roundtrip(self, fmt, df, write_options=None, read_options=None, partition_by=None):
        """Grava pela factory e le pela factory — o mesmo caminho do pipeline."""
        path = self.path_for(fmt if partition_by is None else f"{fmt}-part")
        WriterFactory.create(
            self.spark,
            OutputConfig(
                format=fmt,
                path=path,
                mode="overwrite",
                options=write_options or {},
                partition_by=list(partition_by or []),
            ),
        ).write(df.coalesce(1))
        return ReaderFactory.create(
            self.spark, InputConfig(format=fmt, path=path, options=read_options or {})
        ).read()

    def assert_same_values(self, back):
        """Compara como texto, por nome de coluna, sem depender da ordem das linhas."""
        names = [name for name, _ in COLUMNS]
        self.assertEqual(set(back.columns), set(names))
        got = sorted(tuple(as_text(row[name]) for name in names) for row in back.collect())
        want = sorted(tuple(as_text(value) for value in row) for row in ROWS)
        self.assertEqual(got, want)

    # ---- colunares: o tipo tambem volta

    def test_parquet_preserva_schema_e_linhas(self):
        source = self.source()
        back = self.roundtrip("parquet", source)
        self.assertEqual(back.schema.simpleString(), source.schema.simpleString())
        self.assertEqual(sorted(back.collect()), sorted(source.collect()))

    def test_orc_preserva_schema_e_linhas(self):
        source = self.source()
        back = self.roundtrip("orc", source)
        self.assertEqual(back.schema.simpleString(), source.schema.simpleString())
        self.assertEqual(sorted(back.collect()), sorted(source.collect()))

    def test_parquet_particionado_devolve_todas_as_linhas_com_a_coluna(self):
        # A coluna de particao sai dos dados e vira diretorio; o reader tem de
        # recoloca-la, senao um `partition_by` inocente perde uma coluna.
        back = self.roundtrip("parquet", self.source(), partition_by=["ativo"])
        self.assertIn("ativo", back.columns)
        self.assertEqual(back.count(), len(ROWS))
        self.assertEqual(sorted(row["id"] for row in back.collect()), [row[0] for row in ROWS])

    # ---- texto: o valor volta, o tipo nao e promessa

    def test_json_preserva_os_valores_e_todas_as_colunas(self):
        # O leitor de JSON devolve as colunas em ordem alfabetica: comparar por nome.
        self.assert_same_values(self.roundtrip("json", self.source()))

    def test_json_nao_inventa_valor_para_o_que_era_nulo(self):
        # O writer omite o campo nulo; o risco e o leitor devolver "" ou "null".
        back = self.roundtrip("json", self.source())
        vazia = [row for row in back.collect() if row["id"] == 3][0]
        for coluna in ("nome", "valor", "ativo", "dia"):
            self.assertIsNone(vazia[coluna], f"{coluna} deveria voltar nulo")

    def test_csv_com_header_preserva_os_valores(self):
        back = self.roundtrip(
            "csv",
            self.source(),
            write_options={"header": "true"},
            read_options={"header": "true", "inferSchema": "true"},
        )
        self.assert_same_values(back)

    def test_csv_devolve_inteiro_o_campo_com_aspas_e_virgula(self):
        # O dialeto RFC 4180 vive em test_csv_dialect_spark.py; o que se trava aqui
        # e que a ida e volta pela factory nao parte o campo em duas colunas.
        back = self.roundtrip(
            "csv",
            self.source(),
            write_options={"header": "true"},
            read_options={"header": "true", "inferSchema": "true"},
        )
        linha = [row for row in back.collect() if row["id"] == 2][0]
        self.assertEqual(linha["nome"], ROWS[1][1])

    def test_txt_devolve_as_linhas_na_ordem_gravada(self):
        literais = ", ".join(f"({sql_literal(linha, 'STRING')})" for linha in TXT_LINES)
        df = self.spark.sql(f"SELECT * FROM VALUES {literais} AS t(value)")
        back = self.roundtrip("txt", df)
        self.assertEqual(back.columns, ["value"])
        self.assertEqual([row["value"] for row in back.collect()], list(TXT_LINES))

    # ---- memoria

    def test_view_devolve_o_dataframe_identico(self):
        source = self.source()
        WriterFactory.create(
            self.spark, OutputConfig(format="view", path="roundtrip_sessao")
        ).write(source)
        back = ReaderFactory.create(
            self.spark, InputConfig(format="view", path="roundtrip_sessao")
        ).read()
        self.assertEqual(back.schema.simpleString(), source.schema.simpleString())
        self.assertEqual(sorted(back.collect()), sorted(source.collect()))

    def test_view_global_e_lida_sem_o_prefixo_global_temp(self):
        # O reader poe `global_temp.` quando o nome nao tem ponto — sem isso o JSON
        # precisaria conhecer um detalhe interno do Spark.
        source = self.source()
        WriterFactory.create(
            self.spark,
            OutputConfig(format="view", path="roundtrip_global", options={"scope": "global"}),
        ).write(source)
        back = ReaderFactory.create(
            self.spark,
            InputConfig(format="view", path="roundtrip_global", options={"scope": "global"}),
        ).read()
        self.assertEqual(sorted(back.collect()), sorted(source.collect()))
        # E o caminho explicito continua valendo.
        pelo_prefixo = ReaderFactory.create(
            self.spark, InputConfig(format="view", path="global_temp.roundtrip_global")
        ).read()
        self.assertEqual(pelo_prefixo.count(), len(ROWS))

    # ---- o conjunto

    def test_todos_os_formatos_de_arquivo_conservam_a_contagem(self):
        # Varredura barata: um formato novo que entre na factory sem reler o que
        # escreveu cai aqui, mesmo sem ninguem escrever o teste dedicado.
        for fmt in FILE_FORMATS:
            with self.subTest(formato=fmt):
                options = {"header": "true"} if fmt == "csv" else {}
                back = self.roundtrip(
                    fmt, self.source(), write_options=options, read_options=options
                )
                self.assertEqual(back.count(), len(ROWS))


if __name__ == "__main__":
    unittest.main(verbosity=2)
