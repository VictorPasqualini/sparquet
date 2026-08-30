"""Modos de escrita e compressao nos formatos nativos.

`tests/test_formats_roundtrip_spark.py` prova que o dado volta igual; este prova
o que acontece **quando o destino ja existe** e o que sai quando o JSON pede
compressao. Sao as duas coisas que o teste de montagem nao alcanca: `mode` e
`compression` viram comportamento so na hora da escrita, e a diferenca entre
`append` e `overwrite` e justamente a que apaga dado de producao quando esta
errada.

O que cada teste trava:

  overwrite     substitui o conteudo — a segunda escrita nao soma.
  append        soma sem apagar o que estava.
  error         o destino existente e um erro, nao um silencio.
  ignore        o destino existente e mantido intocado, sem erro.
  compression   o arquivo sai comprimido (a extensao muda) e volta legivel.
  partition_by  vira diretorio `coluna=valor`, e a leitura devolve a coluna.

Sem jar nem servico: tudo aqui vem no proprio pyspark. Sem Java a classe e
**pulada** — nunca falha.

    PYTHONPATH=. python tests/test_write_modes_spark.py
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:  # pyspark pode nao estar instalado no ambiente de testes puros
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - ambiente sem pyspark
    SparkSession = None  # type: ignore[assignment]

from sparquet.core.config import InputConfig, OutputConfig  # noqa: E402
from sparquet.io.factory import ReaderFactory, WriterFactory  # noqa: E402

from test_formats_roundtrip_spark import fixture_sql  # noqa: E402

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

#: (formato, valor de `compression`, sufixo que o arquivo passa a ter).
#: O sufixo e a prova objetiva: sem ele, "comprimiu" seria so uma opcao aceita e
#: ignorada — que e exatamente como uma opcao errada se comporta.
COMPRESSOES = (
    ("parquet", "gzip", ".gz.parquet"),
    ("orc", "zlib", ".zlib.orc"),
    ("json", "gzip", ".json.gz"),
    ("csv", "gzip", ".csv.gz"),
)


class TestWriteModes(unittest.TestCase):
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
                .appName("sparquet-write-modes-tests")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
            )
            cls.spark.sql("SELECT 1").count()
        except Exception as exc:  # pragma: no cover - ambiente sem Java/Spark
            cls.spark = None
            raise unittest.SkipTest(f"Spark/Java indisponivel: {exc}")
        cls.tmp = tempfile.mkdtemp(prefix="sparquet-modes-")

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

    def write(self, fmt, path, mode, df=None, options=None, partition_by=None):
        WriterFactory.create(
            self.spark,
            OutputConfig(
                format=fmt,
                path=path,
                mode=mode,
                options=options or {},
                partition_by=list(partition_by or []),
            ),
        ).write((df if df is not None else self.source()).coalesce(1))

    def read(self, fmt, path, options=None):
        return ReaderFactory.create(
            self.spark, InputConfig(format=fmt, path=path, options=options or {})
        ).read()

    # ---- modos

    def test_overwrite_substitui_em_vez_de_somar(self):
        path = self.path_for("modo-overwrite")
        self.write("parquet", path, "overwrite")
        self.write("parquet", path, "overwrite")
        self.assertEqual(self.read("parquet", path).count(), 3)

    def test_append_soma_sem_apagar_o_que_estava(self):
        path = self.path_for("modo-append")
        self.write("parquet", path, "overwrite")
        self.write("parquet", path, "append")
        self.assertEqual(self.read("parquet", path).count(), 6)

    def test_error_falha_quando_o_destino_ja_existe(self):
        path = self.path_for("modo-error")
        self.write("parquet", path, "overwrite")
        with self.assertRaises(Exception) as capturado:
            self.write("parquet", path, "error")
        # A mensagem tem que dizer QUAL caminho ja existe: e o unico dado que faz
        # o erro ser acionavel para quem le o log de um pipeline agendado.
        self.assertIn("modo-error", str(capturado.exception))
        self.assertEqual(self.read("parquet", path).count(), 3)

    def test_ignore_mantem_o_que_estava_e_nao_falha(self):
        path = self.path_for("modo-ignore")
        self.write("parquet", path, "overwrite")
        # Segunda escrita com o dobro das linhas: se `ignore` escrevesse, a
        # contagem mudaria.
        self.write("parquet", path, "ignore", df=self.source().union(self.source()))
        self.assertEqual(self.read("parquet", path).count(), 3)

    # ---- compressao

    def test_compression_muda_o_arquivo_e_o_dado_volta(self):
        for fmt, codec, sufixo in COMPRESSOES:
            with self.subTest(formato=fmt, compression=codec):
                path = self.path_for(f"compress-{fmt}")
                opcoes = {"compression": codec}
                if fmt == "csv":
                    opcoes["header"] = "true"
                self.write(fmt, path, "overwrite", options=opcoes)

                arquivos = [
                    nome
                    for nome in os.listdir(path)
                    if not nome.startswith((".", "_"))
                ]
                self.assertTrue(arquivos, f"{fmt}: nada foi escrito")
                self.assertTrue(
                    all(nome.endswith(sufixo) for nome in arquivos),
                    f"{fmt}: esperava {sufixo}, veio {arquivos}",
                )

                leitura = {"header": "true"} if fmt == "csv" else {}
                self.assertEqual(self.read(fmt, path, leitura).count(), 3)

    def test_compression_invalida_falha_em_vez_de_gravar_sem_comprimir(self):
        """Codec errado tem que quebrar. Gravar sem comprimir porque o nome nao
        existe e o pior dos dois mundos: passa no teste e estoura o storage."""
        path = self.path_for("compress-invalida")
        with self.assertRaises(Exception):
            self.write(
                "parquet", path, "overwrite", options={"compression": "nao-existe"}
            )

    # ---- particionamento

    def test_partition_by_vira_diretorio_e_a_coluna_volta(self):
        for fmt in ("parquet", "json", "csv"):
            with self.subTest(formato=fmt):
                path = self.path_for(f"particao-{fmt}")
                opcoes = {"header": "true"} if fmt == "csv" else {}
                self.write(
                    fmt, path, "overwrite", options=opcoes, partition_by=["ativo"]
                )

                diretorios = sorted(
                    nome for nome in os.listdir(path) if nome.startswith("ativo=")
                )
                # true, false e o nulo, que o Spark grava no diretorio reservado.
                self.assertEqual(
                    diretorios,
                    ["ativo=__HIVE_DEFAULT_PARTITION__", "ativo=false", "ativo=true"],
                )

                back = self.read(fmt, path, opcoes)
                self.assertIn("ativo", back.columns)
                self.assertEqual(back.count(), 3)

    def test_leitura_de_uma_particao_devolve_so_ela(self):
        """O ganho real de particionar: apontar para o diretorio da particao le
        menos arquivo. A coluna some do schema porque virou o caminho."""
        path = self.path_for("particao-filtro")
        self.write("parquet", path, "overwrite", partition_by=["ativo"])

        back = self.read("parquet", os.path.join(path, "ativo=true"))
        self.assertEqual(back.count(), 1)
        self.assertNotIn("ativo", back.columns)


if __name__ == "__main__":
    unittest.main(verbosity=2)
