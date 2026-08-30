"""Integração do caminho JDBC — leitura e escrita contra um banco de verdade.

O banco é o **H2**, que roda dentro da própria JVM do Spark: um arquivo, nenhum
serviço, nenhum container. É o único jeito de executar o caminho JDBC inteiro
numa suíte que precisa continuar offline e curta.

O formato usado é `postgresql` com `url` e `driver` explícitos. Não é disfarce: os
cinco conectores de banco (`postgresql`, `mysql`, `mariadb`, `sqlserver`,
`oracle`) são a MESMA `JdbcReader`/`JdbcWriter` mais um dialeto que só decide
driver default, porta default e formato da url. O que este arquivo prova — que a
tabela é criada, que os valores voltam, que `append` soma, que `query` substitui
`dbtable`, que `truncate` preserva a tabela e que a url ausente diz o que falta —
é código compartilhado, idêntico nos cinco. O que ele NÃO prova é o dialeto de
cada banco; isso continua em `tests/io/test_connectors.py`, por montagem, e
depende de serviço para ir além (ver `services.py`).

Um efeito colateral útil: `url` + `driver` explícitos são um recurso documentado
em `JdbcReader` que nunca havia sido executado. Aqui ele é a própria premissa.

    SPARQUET_IT=1 python tests/io/integration/test_jdbc_spark.py
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness  # noqa: E402


def _url(nome: str) -> str:
    """Banco H2 em arquivo, um por teste.

    `DB_CLOSE_DELAY=-1` mantém o banco vivo enquanto a JVM viver: o Spark abre e
    fecha uma conexão por tarefa, e sem isso a primeira que fechasse apagaria a
    tabela que a próxima ia ler.
    """
    arquivo = harness.work_dir(f"h2-{nome}") / "banco"
    return f"jdbc:h2:file:{arquivo.as_posix()};DB_CLOSE_DELAY=-1"


def _conexao(nome: str) -> dict:
    return {"url": _url(nome), "driver": "org.h2.Driver", "user": "sa", "password": ""}


@harness.requires_integration
class JdbcTest(unittest.TestCase):
    def test_ida_e_volta_cria_a_tabela_e_devolve_os_valores(self) -> None:
        conexao = _conexao("round-trip")

        written, read_back = harness.round_trip(
            "jdbc",
            {
                "format": "postgresql",
                "path": "semente",
                "mode": "overwrite",
                "options": conexao,
            },
            {"format": "postgresql", "path": "semente", "options": conexao},
        )

        self.assertTrue(written.success, msg=written.error)
        self.assertTrue(read_back.success, msg=read_back.error)
        self.assertEqual(read_back.rows_read, len(harness.SEED_ROWS))

        por_id = {linha["id"]: linha for linha in harness.rows_back("jdbc")}
        self.assertEqual(por_id["2"]["nome"], harness.SEED_ROWS[1][1])
        # O nulo continua nulo depois de passar por uma coluna de banco.
        self.assertEqual(por_id["3"]["nome"], "")
        self.assertEqual(por_id["3"]["valor"], "")

    def test_append_soma_sem_apagar_o_que_estava(self) -> None:
        conexao = _conexao("append")
        escrita = {
            "format": "postgresql",
            "path": "acumula",
            "mode": "overwrite",
            "options": conexao,
        }

        harness.run(
            {"name": "it-jdbc-append-1", "input": harness.seed_input(), "output": escrita}
        )
        harness.run(
            {
                "name": "it-jdbc-append-2",
                "input": harness.seed_input(),
                "output": {**escrita, "mode": "append"},
            }
        )
        lido = harness.run(
            {
                "name": "it-jdbc-append-leitura",
                "input": {"format": "postgresql", "path": "acumula", "options": conexao},
                "output": {"format": "view", "path": "it_jdbc_append"},
            }
        )

        self.assertTrue(lido.success, msg=lido.error)
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS) * 2)

    def test_overwrite_substitui_em_vez_de_somar(self) -> None:
        """A diferença que separa os dois modos, num banco onde a tabela persiste."""
        conexao = _conexao("overwrite")
        escrita = {
            "format": "postgresql",
            "path": "substitui",
            "mode": "overwrite",
            "options": conexao,
        }

        harness.run(
            {"name": "it-jdbc-over-1", "input": harness.seed_input(), "output": escrita}
        )
        harness.run(
            {"name": "it-jdbc-over-2", "input": harness.seed_input(), "output": escrita}
        )
        lido = harness.run(
            {
                "name": "it-jdbc-over-leitura",
                "input": {"format": "postgresql", "path": "substitui", "options": conexao},
                "output": {"format": "view", "path": "it_jdbc_over"},
            }
        )

        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))

    def test_truncate_preserva_a_tabela_e_troca_o_conteudo(self) -> None:
        """`truncate: true` faz TRUNCATE em vez de DROP/CREATE — mesmo resultado
        visível, motivo diferente: a definição da tabela (tipos, grants) sobrevive."""
        conexao = _conexao("truncate")
        escrita = {
            "format": "postgresql",
            "path": "trunca",
            "mode": "overwrite",
            "options": conexao,
        }

        harness.run(
            {"name": "it-jdbc-trunc-1", "input": harness.seed_input(), "output": escrita}
        )
        resultado = harness.run(
            {
                "name": "it-jdbc-trunc-2",
                "input": harness.seed_input(),
                "output": {**escrita, "options": {**conexao, "truncate": "true"}},
            }
        )
        self.assertTrue(resultado.success, msg=resultado.error)

        lido = harness.run(
            {
                "name": "it-jdbc-trunc-leitura",
                "input": {"format": "postgresql", "path": "trunca", "options": conexao},
                "output": {"format": "view", "path": "it_jdbc_trunc"},
            }
        )
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))

    def test_query_substitui_a_tabela_na_leitura(self) -> None:
        conexao = _conexao("query")
        harness.run(
            {
                "name": "it-jdbc-query-base",
                "input": harness.seed_input(),
                "output": {
                    "format": "postgresql",
                    "path": "consulta",
                    "mode": "overwrite",
                    "options": conexao,
                },
            }
        )

        lido = harness.run(
            {
                "name": "it-jdbc-query",
                "input": {
                    "format": "postgresql",
                    # `path` é ignorado quando há `query` — daí o nome inexistente.
                    "path": "tabela_que_nao_existe",
                    "options": {
                        **conexao,
                        # Duas convenções na mesma linha, por conta do H2: a
                        # TABELA nasceu maiúscula (`CONSULTA`), as COLUNAS
                        # nasceram citadas em minúsculo (`"id"`). Sem aspas o H2
                        # maiusculiza, e `id` viraria `ID`, que não existe.
                        "query": (
                            'SELECT "id", "nome" FROM CONSULTA WHERE "id" = 2'
                        ),
                    },
                },
                "output": {"format": "view", "path": "it_jdbc_query"},
            }
        )

        self.assertTrue(lido.success, msg=lido.error)
        self.assertEqual(lido.rows_read, 1)

    def test_sem_url_e_sem_host_diz_o_que_falta(self) -> None:
        resultado = harness.run(
            {
                "name": "it-jdbc-sem-url",
                "input": harness.seed_input(),
                "output": {
                    "format": "postgresql",
                    "path": "qualquer",
                    "mode": "overwrite",
                    "options": {"user": "sa"},
                },
            }
        )

        self.assertFalse(resultado.success)
        self.assertIn("'url'", resultado.error or "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
