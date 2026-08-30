"""Integração dos formatos de arquivo que faltavam: `avro`, `xml`, `binary`.

`parquet`, `orc`, `json`, `csv`, `txt` e `view` já rodam de verdade em
`tests/test_formats_roundtrip_spark.py`, porque não pedem nada além do pyspark.
Estes três ficavam de fora: `avro` precisa de um jar, `xml` precisava até o Spark
3.x, e `binary` é só leitura — não dá para testá-lo com o par write/read dos
outros.

O que cada teste trava é a promessa do formato:

  avro    guarda o schema junto com o dado; volta igual, com nulo intacto.
  xml     texto hierárquico; o `rowTag` é o que define o que é uma linha, e sem
          ele não há registro nenhum — é a opção que mais gera chamado.
  binary  devolve o arquivo inteiro em `content`, com `path` e `length`; serve
          para PDF/imagem/blob, e o teste prova que o byte volta byte.

    SPARQUET_IT=1 python tests/io/integration/test_files_spark.py
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness  # noqa: E402


@harness.requires_integration
class AvroTest(unittest.TestCase):
    def test_ida_e_volta_preserva_valores_e_nulos(self) -> None:
        directory = (harness.WORK / "avro").as_posix()

        written, read_back = harness.round_trip(
            "avro",
            {"format": "avro", "path": directory, "mode": "overwrite"},
            {"format": "avro", "path": directory},
        )

        self.assertTrue(written.success, msg=written.error)
        self.assertTrue(read_back.success, msg=read_back.error)
        self.assertEqual(written.rows_written, len(harness.SEED_ROWS))
        self.assertEqual(read_back.rows_read, len(harness.SEED_ROWS))

        por_id = {linha["id"]: linha for linha in harness.rows_back("avro")}
        self.assertEqual(por_id["1"]["nome"], "alpha")
        self.assertEqual(por_id["2"]["nome"], harness.SEED_ROWS[1][1])
        # Nulo é o que mais se perde; avro guarda o schema, então continua nulo.
        self.assertEqual(por_id["3"]["nome"], "")
        self.assertEqual(por_id["3"]["valor"], "")

    def test_compressao_é_uma_opcao_do_json_e_nao_muda_o_que_volta(self) -> None:
        directory = (harness.WORK / "avro-deflate").as_posix()

        harness.run(
            {
                "name": "it-avro-deflate",
                "input": harness.seed_input(),
                "output": {
                    "format": "avro",
                    "path": directory,
                    "mode": "overwrite",
                    "options": {"compression": "deflate"},
                },
            }
        )
        lido = harness.run(
            {
                "name": "it-avro-deflate-leitura",
                "input": {"format": "avro", "path": directory},
                "output": {"format": "view", "path": "it_avro_deflate"},
            }
        )

        self.assertTrue(lido.success, msg=lido.error)
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))

    def test_particionar_devolve_todas_as_linhas_com_a_coluna(self) -> None:
        """A coluna de partição sai do dado e vira diretório — e volta no read."""
        directory = (harness.WORK / "avro-particionado").as_posix()

        harness.run(
            {
                "name": "it-avro-particionado",
                "input": harness.seed_input(),
                "output": {
                    "format": "avro",
                    "path": directory,
                    "mode": "overwrite",
                    "partition_by": ["id"],
                },
            }
        )
        lido = harness.run(
            {
                "name": "it-avro-particionado-leitura",
                "input": {"format": "avro", "path": directory},
                "output": {
                    "format": "csv",
                    "path": (harness.WORK / "back-avro-particionado").as_posix(),
                    "mode": "overwrite",
                    "options": {"header": "true"},
                },
            }
        )

        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))
        linhas = harness.rows_back("avro-particionado")
        self.assertEqual({linha["id"] for linha in linhas}, {"1", "2", "3"})


@harness.requires_integration
class XmlTest(unittest.TestCase):
    """No Spark 4 o datasource `xml` é nativo; até o 3.x exigia `spark-xml`."""

    def test_ida_e_volta_com_row_tag(self) -> None:
        directory = (harness.WORK / "xml").as_posix()

        written, read_back = harness.round_trip(
            "xml",
            {
                "format": "xml",
                "path": directory,
                "mode": "overwrite",
                "options": {"rowTag": "titulo", "rootTag": "titulos"},
            },
            {"format": "xml", "path": directory, "options": {"rowTag": "titulo"}},
        )

        self.assertTrue(written.success, msg=written.error)
        self.assertTrue(read_back.success, msg=read_back.error)
        self.assertEqual(read_back.rows_read, len(harness.SEED_ROWS))

        por_id = {linha["id"]: linha for linha in harness.rows_back("xml")}
        self.assertEqual(por_id["1"]["nome"], "alpha")
        # Aspas e vírgula não são especiais em XML, mas o escape do writer é —
        # se ele quebrasse a tag, o valor voltaria partido ou o parse falharia.
        self.assertEqual(por_id["2"]["nome"], harness.SEED_ROWS[1][1])

    def test_row_tag_errado_nao_encontra_registro_nenhum(self) -> None:
        """O erro mais comum do formato, e ele é silencioso: zero linha, sem falha."""
        directory = (harness.WORK / "xml-tag").as_posix()

        harness.run(
            {
                "name": "it-xml-tag",
                "input": harness.seed_input(),
                "output": {
                    "format": "xml",
                    "path": directory,
                    "mode": "overwrite",
                    "options": {"rowTag": "titulo"},
                },
            }
        )
        lido = harness.run(
            {
                "name": "it-xml-tag-leitura",
                "input": {
                    "format": "xml",
                    "path": directory,
                    "options": {"rowTag": "outra_coisa"},
                },
                "output": {"format": "view", "path": "it_xml_tag"},
            }
        )

        self.assertTrue(lido.success, msg=lido.error)
        self.assertEqual(lido.rows_read, 0)

    def test_append_soma_no_mesmo_diretorio(self) -> None:
        """`append` em XML não gera um arquivo inválido: cada escrita põe outro
        arquivo com o mesmo `rootTag`, e a leitura soma os dois."""
        directory = (harness.WORK / "xml-append").as_posix()
        escrita = {
            "format": "xml",
            "path": directory,
            "mode": "overwrite",
            "options": {"rowTag": "registro", "rootTag": "registros"},
        }

        harness.run(
            {"name": "it-xml-append-1", "input": harness.seed_input(), "output": escrita}
        )
        harness.run(
            {
                "name": "it-xml-append-2",
                "input": harness.seed_input(),
                "output": {**escrita, "mode": "append"},
            }
        )
        lido = harness.run(
            {
                "name": "it-xml-append-leitura",
                "input": {
                    "format": "xml",
                    "path": directory,
                    "options": {"rowTag": "registro"},
                },
                "output": {"format": "view", "path": "it_xml_append"},
            }
        )

        self.assertTrue(lido.success, msg=lido.error)
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS) * 2)

    def test_compressao_comprime_o_arquivo_e_a_leitura_descomprime_sozinha(self) -> None:
        """XML comprimido é o caso normal em ingestão de terceiro: o arquivo sai
        `.gz` e o reader não precisa de opção nenhuma para abrir."""
        directory = harness.WORK / "xml-gzip"

        gravado = harness.run(
            {
                "name": "it-xml-gzip",
                "input": harness.seed_input(),
                "output": {
                    "format": "xml",
                    "path": directory.as_posix(),
                    "mode": "overwrite",
                    "options": {
                        "rowTag": "registro",
                        "rootTag": "registros",
                        "compression": "gzip",
                    },
                },
            }
        )
        self.assertTrue(gravado.success, msg=gravado.error)

        partes = [
            arquivo.name
            for arquivo in directory.iterdir()
            if not arquivo.name.startswith((".", "_"))
        ]
        self.assertTrue(partes, "nada foi escrito")
        self.assertTrue(
            all(nome.endswith(".gz") for nome in partes),
            f"esperava arquivos .gz, veio {partes}",
        )

        lido = harness.run(
            {
                "name": "it-xml-gzip-leitura",
                "input": {
                    "format": "xml",
                    "path": directory.as_posix(),
                    "options": {"rowTag": "registro"},
                },
                "output": {"format": "view", "path": "it_xml_gzip"},
            }
        )
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))


@harness.requires_integration
class BinaryTest(unittest.TestCase):
    """Só leitura: o Spark não escreve `binaryFile` — não existe BinaryWriter."""

    def setUp(self) -> None:
        self.directory = harness.work_dir("blobs")
        (self.directory / "nota.pdf").write_bytes(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3 fim")
        (self.directory / "leia-me.txt").write_text("texto", encoding="utf-8")

    def test_le_o_arquivo_inteiro_com_caminho_e_tamanho(self) -> None:
        resultado = harness.run(
            {
                "name": "it-binary",
                "input": {"format": "binary", "path": self.directory.as_posix()},
                "transformations": [{"type": "select", "columns": ["path", "length"]}],
                "output": {
                    "format": "csv",
                    "path": (harness.WORK / "back-binary").as_posix(),
                    "mode": "overwrite",
                    "options": {"header": "true"},
                },
            }
        )

        self.assertTrue(resultado.success, msg=resultado.error)
        self.assertEqual(resultado.rows_read, 2)
        nomes = {Path(linha["path"]).name for linha in harness.rows_back("binary")}
        self.assertEqual(nomes, {"nota.pdf", "leia-me.txt"})

    def test_path_glob_filter_escolhe_o_que_entra(self) -> None:
        """É a opção que faz o formato ser usável: um diretório tem de tudo."""
        resultado = harness.run(
            {
                "name": "it-binary-glob",
                "input": {
                    "format": "binary",
                    "path": self.directory.as_posix(),
                    "options": {"pathGlobFilter": "*.pdf"},
                },
                "output": {"format": "view", "path": "it_binary_glob"},
            }
        )

        self.assertEqual(resultado.rows_read, 1)

    def test_o_conteudo_volta_byte_a_byte(self) -> None:
        """Escrito em parquet, que é como se persiste `content` — não há writer."""
        destino = (harness.WORK / "binary-parquet").as_posix()

        harness.run(
            {
                "name": "it-binary-content",
                "input": {
                    "format": "binary",
                    "path": self.directory.as_posix(),
                    "options": {"pathGlobFilter": "*.pdf"},
                },
                "output": {"format": "parquet", "path": destino, "mode": "overwrite"},
            }
        )
        lido = harness.run(
            {
                "name": "it-binary-content-leitura",
                "input": {"format": "parquet", "path": destino},
                "output": {"format": "view", "path": "it_binary_content"},
            }
        )

        self.assertTrue(lido.success, msg=lido.error)
        conteudo = lido.output_df.select("content").collect()[0][0]
        self.assertEqual(bytes(conteudo), (self.directory / "nota.pdf").read_bytes())


if __name__ == "__main__":
    unittest.main(verbosity=2)
