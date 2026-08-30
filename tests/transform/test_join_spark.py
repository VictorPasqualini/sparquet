"""O join: a fonte do lado direito e o que sai quando os dois lados tem os
mesmos nomes de coluna.

Um join entre fontes que compartilham nomes — o caso normal num self join —
devolvia duas colunas `nome`, e a transformacao seguinte que citasse `nome`
morria com `AMBIGUOUS_REFERENCE`. Nao e um erro que apareca na montagem: o JSON
esta certo, o join roda, e quebra tres etapas depois. `JoinTransformation`
renomeia as repetidas do lado direito (`nome_r`), e isso e o que este arquivo
trava.

O que cada teste cobre:

  input            a chave da fonte direita. `with`, o nome antigo, e recusado
                   com um erro que manda renomear — ignorado em silencio, o join
                   rodaria sem a segunda fonte.
  chave por nome   `on: "id"` — o Spark ja funde a chave numa coluna so, e ela
                   NAO pode ser renomeada.
  homonimas        as demais colunas repetidas viram `nome_r`, e o valor de cada
                   lado vai para a coluna certa.
  colisao          se `nome_r` ja existir no lado esquerdo, sai `nome_r2`.
  `on` SQL         com a condicao escrita a mao a chave nao e fundida, entao ela
                   tambem entra na renomeacao — e a condicao continua valendo,
                   porque a projecao e montada depois do join.
  sem repetidas    o DataFrame volta como estava, sem projecao no plano.
  semi/anti        so o lado esquerdo sai; nada a renomear.

Sem jar nem servico: tudo aqui vem no proprio pyspark. Sem Java a classe e
**pulada** — nunca falha.

    PYTHONPATH=. python tests/transform/test_join_spark.py
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

try:  # pyspark pode nao estar instalado no ambiente de testes puros
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - ambiente sem pyspark
    SparkSession = None  # type: ignore[assignment]

from sparquet.core.config import TransformationConfig  # noqa: E402
from sparquet.transform.engine import TransformationEngine  # noqa: E402

# Sem isto o worker sobe com outro Python e morre com "Python worker exited
# unexpectedly" na primeira etapa que cria worker — aqui, o createDataFrame das
# fixtures. Vale so em master local.
for _var in ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"):
    os.environ.setdefault(_var, sys.executable)


class TestJoin(unittest.TestCase):
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
                .appName("sparquet-join-tests")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
            )
            cls.spark.sql("SELECT 1").count()
        except Exception as exc:  # pragma: no cover - ambiente sem Java/Spark
            cls.spark = None
            raise unittest.SkipTest(f"Spark/Java indisponivel: {exc}")
        cls.tmp = tempfile.mkdtemp(prefix="sparquet-join-")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.spark is not None:
            cls.spark.stop()
        if cls.tmp:
            shutil.rmtree(cls.tmp, ignore_errors=True)

    # ---- infra

    def relacao(self, valores: str, colunas: str):
        """Um DataFrame literal montado no proprio SQL.

        `createDataFrame` sobre uma lista Python precisa de worker, e em master
        local no Windows o worker e a parte fragil da montagem — uma relacao
        `VALUES` fica so na JVM. Mesmo motivo do `fixture_sql` em
        `tests/test_formats_roundtrip_spark.py`.
        """
        return self.spark.sql(f"SELECT * FROM VALUES {valores} AS t({colunas})")

    def gravar(self, nome: str, valores: str, colunas: str) -> str:
        """Grava um parquet e devolve o caminho, para servir de fonte do join."""
        caminho = str(Path(self.tmp) / nome)
        self.relacao(valores, colunas).write.mode("overwrite").parquet(caminho)
        return caminho

    def juntar(self, esquerda, params):
        engine = TransformationEngine()
        cfg = TransformationConfig.from_dict({"type": "join", **params})
        return engine.apply(esquerda, [cfg])

    def esquerda(self):
        return self.relacao("(1, 'alpha', 1.5), (2, 'beta', 2.5)", "id, nome, valor")

    # ---- a fonte do lado direito

    def test_input_e_a_chave_da_fonte(self) -> None:
        caminho = self.gravar("dim.parquet", "(1, 'x')", "id, extra")
        juntado = self.juntar(
            self.esquerda(),
            {"input": {"format": "parquet", "path": caminho}, "on": "id"},
        )
        self.assertEqual(juntado.count(), 1)
        self.assertIn("extra", juntado.columns)

    def test_with_e_recusado_com_o_nome_novo_no_erro(self) -> None:
        """A chave antiga nao pode ser ignorada: sem a fonte o join nao existe."""
        caminho = self.gravar("dim2.parquet", "(1, 'x')", "id, extra")
        with self.assertRaises(ValueError) as capturado:
            self.juntar(
                self.esquerda(),
                {"with": {"format": "parquet", "path": caminho}, "on": "id"},
            )
        mensagem = str(capturado.exception)
        self.assertIn("'with' nao existe mais", mensagem)
        self.assertIn("'input'", mensagem)

    def test_sem_input_o_erro_diz_qual_chave_falta(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            self.juntar(self.esquerda(), {"on": "id"})
        self.assertIn("input", str(capturado.exception))

    # ---- nomes repetidos

    def test_self_join_por_nome_de_chave_nao_repete_coluna(self) -> None:
        caminho = self.gravar(
            "self.parquet", "(1, 'pai', 9.0), (2, 'mae', 8.0)", "id, nome, valor"
        )
        juntado = self.juntar(
            self.esquerda(),
            {"input": {"format": "parquet", "path": caminho}, "on": "id"},
        )

        # Nenhum nome sai duas vezes, e a chave continua com o nome original.
        self.assertEqual(len(juntado.columns), len(set(juntado.columns)))
        self.assertEqual(
            juntado.columns, ["id", "nome", "valor", "nome_r", "valor_r"]
        )

        # E a citacao que antes era ambigua agora resolve.
        linha = juntado.orderBy("id").select("nome", "nome_r").first()
        self.assertEqual((linha["nome"], linha["nome_r"]), ("alpha", "pai"))

    def test_o_lado_direito_e_que_muda_de_nome(self) -> None:
        """A escolha importa: quem escreve o pipeline pensa nas colunas do fluxo
        principal, e sao elas que continuam com o nome de sempre."""
        caminho = self.gravar("lado.parquet", "(1, 'direita')", "id, nome")
        juntado = self.juntar(
            self.esquerda(),
            {"input": {"format": "parquet", "path": caminho}, "on": "id"},
        )
        linha = juntado.first()
        self.assertEqual(linha["nome"], "alpha")
        self.assertEqual(linha["nome_r"], "direita")

    def test_quando_o_sufixo_ja_existe_o_proximo_e_numerado(self) -> None:
        caminho = self.gravar("num.parquet", "(1, 'direita')", "id, nome")
        esquerda = self.relacao("(1, 'alpha', 'ja_ocupado')", "id, nome, nome_r")
        juntado = self.juntar(
            esquerda, {"input": {"format": "parquet", "path": caminho}, "on": "id"}
        )
        self.assertEqual(juntado.columns, ["id", "nome", "nome_r", "nome_r2"])
        self.assertEqual(juntado.first()["nome_r2"], "direita")

    def test_com_on_em_sql_a_chave_tambem_e_desambiguada(self) -> None:
        """Sem `USING` o Spark nao funde a chave: `id` vinha duas vezes. E a
        condicao segue valendo, porque a renomeacao acontece depois do join."""
        caminho = self.gravar("sql.parquet", "(1, 'pai')", "id, nome")
        juntado = self.juntar(
            self.esquerda(),
            {
                "input": {"format": "parquet", "path": caminho},
                "on": "l.id = r.id",
            },
        )
        self.assertEqual(juntado.columns, ["id", "nome", "valor", "id_r", "nome_r"])
        linha = juntado.first()
        self.assertEqual((linha["id"], linha["id_r"]), (1, 1))

    def test_sem_nome_repetido_nada_muda(self) -> None:
        caminho = self.gravar("livre.parquet", "(1, 'x')", "id, extra")
        juntado = self.juntar(
            self.esquerda(),
            {"input": {"format": "parquet", "path": caminho}, "on": "id"},
        )
        self.assertEqual(juntado.columns, ["id", "nome", "valor", "extra"])

    def test_semi_e_anti_saem_so_com_o_lado_esquerdo(self) -> None:
        caminho = self.gravar("semi.parquet", "(1, 'pai', 9.0)", "id, nome, valor")
        for how, esperado in (("leftsemi", 1), ("leftanti", 1)):
            with self.subTest(how=how):
                juntado = self.juntar(
                    self.esquerda(),
                    {
                        "input": {"format": "parquet", "path": caminho},
                        "on": "id",
                        "how": how,
                    },
                )
                self.assertEqual(juntado.columns, ["id", "nome", "valor"])
                self.assertEqual(juntado.count(), esperado)

    def test_with_transformations_valem_antes_da_desambiguacao(self) -> None:
        """Renomear a mao continua sendo a saida quando o sufixo automatico nao
        e o nome que se quer — e o `select` de dentro do join tambem corta o que
        nao vai ser usado."""
        caminho = self.gravar("trans.parquet", "(1, 'pai', 9.0)", "id, nome, valor")
        juntado = self.juntar(
            self.esquerda(),
            {
                "input": {"format": "parquet", "path": caminho},
                "with_transformations": [
                    {"type": "select", "columns": ["id", "nome AS nome_pai"]}
                ],
                "on": "id",
            },
        )
        self.assertEqual(juntado.columns, ["id", "nome", "valor", "nome_pai"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
