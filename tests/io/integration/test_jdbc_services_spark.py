"""Integração JDBC contra os bancos de verdade — o que o H2 não pode provar.

`test_jdbc_spark.py` executa o caminho JDBC inteiro com H2 dentro da própria JVM,
com `url` e `driver` explícitos. Isso cobre o código compartilhado
(`JdbcReader`/`JdbcWriter`) e deixa de fora exatamente uma coisa: **o dialeto**.
Driver default, porta default e formato da url são o que cada um dos cinco
conectores (`postgresql`, `mysql`, `mariadb`, `sqlserver`, `oracle`) adiciona, e
nenhum teste de montagem sabe se a url montada é aceita pelo banco.

Por isso aqui NENHUM teste informa `url` nem `driver`: só `host`, `database`,
`user` e `password`. Se a conexão abre, a url que o dialeto montou está certa
contra o servidor real — e o driver default é uma classe que existe no jar
declarado em `services.py`. Um teste ainda omite o `port`, e aí a prova é a porta
default do dialeto bater com a porta em que o banco escuta.

Cada banco só roda se a porta dele responder; sem container, tudo se pula dizendo
o que subir. Um por vez é o uso normal — os cinco juntos são ~4 GB de imagem:

    docker compose -f tests/io/integration/docker-compose.yml up -d postgres
    SPARQUET_IT=1 python tests/io/integration/test_jdbc_services_spark.py
    docker compose -f tests/io/integration/docker-compose.yml down -v

Credenciais saem do `docker-compose.yml` ao lado e são sobrescrevíveis por
ambiente (`SPARQUET_IT_POSTGRES_USER`, `..._PASSWORD`, `..._DATABASE`), que é como
apontar a suíte para um banco já existente sem editar teste.
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness  # noqa: E402
import services  # noqa: E402


def _env(servico: str, sufixo: str, default: str) -> str:
    return os.environ.get(f"SPARQUET_IT_{servico.upper()}_{sufixo}", default)


class _DialetoTests:
    """O corpo comum. Não herda de `TestCase` de propósito: quem herda dos dois é
    cada banco lá embaixo, e uma base coletável rodaria sem serviço nenhum."""

    #: Nome em `services.SERVICES` e nas variáveis de ambiente.
    servico = ""
    #: Nome do formato no JSON — é o dialeto sob teste.
    formato = ""
    #: Banco/serviço criado pela imagem do compose.
    database = ""
    user = ""
    password = ""
    #: Porta que o dialeto usa quando o JSON não informa `port`. O teste que a
    #: exercita se pula quando o servidor não está nela (MariaDB fica na 3307 no
    #: host para não brigar com o MySQL).
    porta_default = 0
    #: Como o banco cita identificador: `"` em quase todos, backtick no MySQL.
    citacao = '"'
    #: Propriedades que o driver exige para abrir a conexão neste container
    #: (TLS, sobretudo). Vão em `options` e o framework as repassa ao driver —
    #: o que também prova o passthrough chegando no lugar certo.
    extra: dict = {}

    def conexao(self, com_porta: bool = True) -> dict:
        opcoes = {
            "host": services.host_of(self.servico),
            "database": _env(self.servico, "DATABASE", self.database),
            "user": _env(self.servico, "USER", self.user),
            "password": _env(self.servico, "PASSWORD", self.password),
            **self.extra,
        }
        if com_porta:
            opcoes["port"] = str(services.port_of(self.servico))
        return opcoes

    def tabela(self, sufixo: str) -> str:
        return f"sparquet_it_{sufixo}"

    def escrita(self, tabela: str, modo: str = "overwrite", **opcoes) -> dict:
        return {
            "format": self.formato,
            "path": tabela,
            "mode": modo,
            "options": {**self.conexao(), **opcoes},
        }

    def leitura(self, tabela: str, **opcoes) -> dict:
        return {
            "format": self.formato,
            "path": tabela,
            "options": {**self.conexao(), **opcoes},
        }

    def cite(self, identificador: str) -> str:
        return f"{self.citacao}{identificador}{self.citacao}"

    # ------------------------------------------------------------------ testes

    def test_a_url_montada_pelo_dialeto_abre_e_os_valores_voltam(self) -> None:
        """`host` + `database` + `port`, sem `url` nem `driver`: o dialeto monta a
        url e escolhe o driver, e o banco real aceita os dois ou nada funciona."""
        tabela = self.tabela("ida_e_volta")
        rotulo = f"jdbc-{self.servico}"

        written, read_back = harness.round_trip(
            rotulo, self.escrita(tabela), self.leitura(tabela)
        )

        self.assertTrue(written.success, msg=written.error)
        self.assertTrue(read_back.success, msg=read_back.error)
        self.assertEqual(read_back.rows_read, len(harness.SEED_ROWS))

        por_id = {linha["id"]: linha for linha in harness.rows_back(rotulo)}
        # A linha que quebra texto delimitado: aspas, vírgula e dois-pontos
        # atravessaram uma coluna de banco de verdade sem perder caractere.
        self.assertEqual(por_id["2"]["nome"], harness.SEED_ROWS[1][1])
        # E o nulo continua nulo. É o que mais se perde numa ida e volta.
        self.assertEqual(por_id["3"]["nome"], "")
        self.assertEqual(por_id["3"]["valor"], "")

    def test_sem_port_o_dialeto_usa_a_porta_padrao_do_banco(self) -> None:
        """A porta default do dialeto é um número escrito no código do sparquet.
        Ou é a porta em que o banco escuta, ou a conexão não abre."""
        porta = services.port_of(self.servico)
        if porta != self.porta_default:
            self.skipTest(
                f"{self.servico} responde na porta {porta}, e não na "
                f"{self.porta_default} que o dialeto usa por default — nada a provar"
            )

        tabela = self.tabela("porta_default")
        opcoes = {**self.conexao(com_porta=False)}
        self.assertNotIn("port", opcoes)

        escrita = {
            "format": self.formato,
            "path": tabela,
            "mode": "overwrite",
            "options": opcoes,
        }
        resultado = harness.run(
            {
                "name": f"it-{self.servico}-porta-default",
                "input": harness.seed_input(),
                "output": escrita,
            }
        )

        self.assertTrue(resultado.success, msg=resultado.error)

    def test_append_soma_no_banco_em_vez_de_substituir(self) -> None:
        tabela = self.tabela("append")

        harness.run(
            {
                "name": f"it-{self.servico}-append-1",
                "input": harness.seed_input(),
                "output": self.escrita(tabela),
            }
        )
        segunda = harness.run(
            {
                "name": f"it-{self.servico}-append-2",
                "input": harness.seed_input(),
                "output": self.escrita(tabela, modo="append"),
            }
        )
        self.assertTrue(segunda.success, msg=segunda.error)

        lido = harness.run(
            {
                "name": f"it-{self.servico}-append-leitura",
                "input": self.leitura(tabela),
                "output": {"format": "view", "path": f"it_{self.servico}_append"},
            }
        )
        self.assertTrue(lido.success, msg=lido.error)
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS) * 2)

    def test_query_substitui_a_tabela_na_leitura(self) -> None:
        """`query` é repassada crua ao banco, então é o único ponto da suíte onde
        a sintaxe é do dialeto e não do Spark — daí a citação por banco."""
        tabela = self.tabela("consulta")
        harness.run(
            {
                "name": f"it-{self.servico}-query-base",
                "input": harness.seed_input(),
                "output": self.escrita(tabela),
            }
        )

        lido = harness.run(
            {
                "name": f"it-{self.servico}-query",
                "input": {
                    "format": self.formato,
                    # Ignorado quando há `query` — daí um nome que não existe.
                    "path": "tabela_que_nao_existe",
                    "options": {
                        **self.conexao(),
                        "query": (
                            f"SELECT {self.cite('id')}, {self.cite('nome')} "
                            f"FROM {tabela} WHERE {self.cite('id')} = 2"
                        ),
                    },
                },
                "output": {"format": "view", "path": f"it_{self.servico}_query"},
            }
        )

        self.assertTrue(lido.success, msg=lido.error)
        self.assertEqual(lido.rows_read, 1)

    def test_host_ausente_diz_o_que_falta_antes_de_tentar_conectar(self) -> None:
        """Sem `url` e sem `host` não há url para montar, e o erro precisa dizer
        isso — não estourar um timeout de conexão em `jdbc://:5432/`."""
        resultado = harness.run(
            {
                "name": f"it-{self.servico}-sem-host",
                "input": harness.seed_input(),
                "output": {
                    "format": self.formato,
                    "path": self.tabela("nunca_criada"),
                    "mode": "overwrite",
                    "options": {"user": self.user},
                },
            }
        )

        self.assertFalse(resultado.success)
        self.assertIn("'url'", resultado.error or "")


@harness.requires_integration
@services.requires_service("postgres")
class PostgresTest(_DialetoTests, unittest.TestCase):
    servico = "postgres"
    formato = "postgresql"
    database = "sparquet"
    user = "sparquet"
    password = "sparquet"
    porta_default = 5432


@harness.requires_integration
@services.requires_service("mysql")
class MySqlTest(_DialetoTests, unittest.TestCase):
    servico = "mysql"
    formato = "mysql"
    database = "sparquet"
    user = "root"
    password = "sparquet"
    porta_default = 3306
    citacao = "`"
    # MySQL 8 autentica com `caching_sha2_password`, que numa conexão sem TLS
    # exige buscar a chave pública do servidor. Sem estas duas o driver recusa
    # antes de qualquer SQL, com "Public Key Retrieval is not allowed".
    extra = {"sslMode": "DISABLED", "allowPublicKeyRetrieval": "true"}


@harness.requires_integration
@services.requires_service("mariadb")
class MariaDbTest(_DialetoTests, unittest.TestCase):
    servico = "mariadb"
    formato = "mariadb"
    database = "sparquet"
    user = "root"
    password = "sparquet"
    # O container publica a 3307 no host de propósito, então o teste da porta
    # default se pula aqui. O número continua declarado: é o do dialeto.
    porta_default = 3306
    # O dialeto monta `jdbc:mysql://` e o servidor é MariaDB: quem cita é o
    # MySQLDialect, com backtick.
    citacao = "`"
    # ACHADO desta camada: o Spark 4.1 **não tem dialeto MariaDB**. Só existe
    # `MySQLDialect`, e ele só reconhece url que comece com `jdbc:mysql`; uma url
    # `jdbc:mariadb://` cai no dialeto default, que cita identificador com `"`. O
    # MariaDB recusa isso e o CREATE TABLE morre em `You have an error in your SQL
    # syntax ... near '"id" INTEGER'` — leitura inclusive, porque o SELECT montado
    # pelo Spark cita as colunas do mesmo jeito.
    #
    # As duas saídas foram executadas contra o container e as duas funcionam:
    # `sessionVariables: sql_mode='ANSI_QUOTES'` (o MariaDB passa a aceitar `"`,
    # mas na mesma sessão `"..."` deixa de ser literal de string — importa para
    # quem usa `query`), ou o Connector/J com url de MySQL, que traz o
    # MySQLDialect de verdade. O dialeto adotou a segunda; a primeira continua
    # disponível informando `url`/`driver` em options, e o
    # `MariaDbUrlExplicitaTest` abaixo a exercita.
    extra = {"sslMode": "DISABLED", "allowPublicKeyRetrieval": "true"}


@harness.requires_integration
@services.requires_service("mariadb")
class MariaDbUrlExplicitaTest(unittest.TestCase):
    """A rota alternativa do MariaDB: url `jdbc:mariadb://` informada à mão.

    O dialeto default monta url de MySQL. Quem precisa do driver do MariaDB
    (recurso específico dele, política de jar) informa `url` em options — e aí
    o driver acompanha a url sozinho, sem precisar de `driver`. O preço é o
    `sql_mode='ANSI_QUOTES'`, sem o qual o servidor recusa a citação com aspas
    duplas do dialeto default do Spark; o framework avisa quando ele falta.
    """

    def opcoes(self) -> dict:
        host = services.host_of("mariadb")
        porta = services.port_of("mariadb")
        return {
            "url": f"jdbc:mariadb://{host}:{porta}/sparquet",
            "user": "root",
            "password": "sparquet",
            "sessionVariables": "sql_mode='ANSI_QUOTES'",
        }

    def test_url_de_mariadb_com_ansi_quotes_faz_a_ida_e_volta(self) -> None:
        tabela = "sparquet_it_maria_url"
        rotulo = "jdbc-mariadb-url"
        opcoes = self.opcoes()

        written, read_back = harness.round_trip(
            rotulo,
            {
                "format": "mariadb",
                "path": tabela,
                "mode": "overwrite",
                "options": opcoes,
            },
            {"format": "mariadb", "path": tabela, "options": opcoes},
        )

        self.assertTrue(written.success, msg=written.error)
        self.assertTrue(read_back.success, msg=read_back.error)
        self.assertEqual(read_back.rows_read, len(harness.SEED_ROWS))

@harness.requires_integration
@services.requires_service("sqlserver")
class SqlServerTest(_DialetoTests, unittest.TestCase):
    servico = "sqlserver"
    formato = "sqlserver"
    # A imagem não cria banco nenhum; `master` é o que existe.
    database = "master"
    user = "sa"
    password = "Sparquet!2024"
    porta_default = 1433
    # O mssql-jdbc 12 liga `encrypt=true` por default e o container tem
    # certificado autoassinado: sem isto a conexão morre na validação do
    # certificado, antes de existir sessão.
    extra = {"encrypt": "false", "trustServerCertificate": "true"}


@harness.requires_integration
@services.requires_service("oracle")
class OracleTest(_DialetoTests, unittest.TestCase):
    servico = "oracle"
    formato = "oracle"
    # No Oracle o `database` do JSON é o *service name*, não um catálogo:
    # a url do dialeto é `//host:port/service`. FREEPDB1 é o PDB da imagem.
    database = "FREEPDB1"
    user = "system"
    password = "sparquet"
    porta_default = 1521


if __name__ == "__main__":
    unittest.main(verbosity=2)
