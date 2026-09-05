"""Integração dos conectores NoSQL e de busca: Mongo, Cassandra, Elasticsearch, OpenSearch.

Estes quatro são os únicos da suíte cujo jar traz **código Spark dentro** — não é
um driver neutro como o JDBC, é um datasource compilado contra uma linha do Spark.
Então o primeiro resultado que este arquivo produz não é "o conector funciona", é
*qual versão do conector serve ao Spark instalado* — e o lugar de registrar isso é
a coordenada em `services.py`. Executado contra os containers no Spark 4.1.1:

  **mongodb**       passa. Prova `path` virando `collection`, e `_id` sendo o
                    único enfeite que o conector acrescenta na volta.
  **cassandra**     passa no caminho de dados. Prova o `path`
                    `"keyspace.tabela"` sendo partido em duas opções, e o modo
                    `append` sendo upsert por chave (gravar duas vezes não
                    duplica, ao contrário de todo o resto da suíte). O
                    `CassandraCatalog` do mesmo jar **não** sobe no Spark 4 — ver
                    `CassandraTest`.
  **opensearch**    passa com `org.opensearch.client:opensearch-spark-40_2.13:2.0.0`,
                    que é build de Spark 4 de verdade.
  **elasticsearch** pula: nenhuma versão publicada do `elasticsearch-spark`
                    roda em Spark 4 (`Dataset.sqlContext()` saiu da API), e o
                    motivo exato está no campo `incompativel` da coordenada. O
                    dia em que sair um build, apagar aquele campo faz este teste
                    rodar sem mais nenhuma mudança. Enquanto não sai, quem tem um
                    servidor Elasticsearch chega nele pelo conector do OpenSearch
                    — e é isso que `ElasticsearchViaOpenSearchTest` mede.

  Os dois formatos são conectores DISTINTOS, com prefixos de opção diferentes
  (`es.*` e `opensearch.*`), e são o par mais fácil de confundir no catálogo.

Um serviço por vez; sem container, cada classe se pula dizendo o que subir:

    docker compose -f tests/io/integration/docker-compose.yml up -d mongodb
    SPARQUET_IT=1 python tests/io/integration/test_nosql_services_spark.py
    docker compose -f tests/io/integration/docker-compose.yml down -v
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness  # noqa: E402
import services  # noqa: E402

# Nenhum teste deste arquivo lê parquet, delta, iceberg, avro ou H2, e o jar do
# Cassandra não convive com os de delta e iceberg no mesmo `spark.jars.packages`
# (ver `harness.USE_BASE_PACKAGES` para o erro exato). Desligar aqui é o que
# torna a sessão deste arquivo possível — e mais barata, de passagem.
harness.USE_BASE_PACKAGES = False

#: Banco/keyspace/índice de teste. Um nome só, minúsculo: índice do
#: Elasticsearch não aceita maiúscula.
_ESPACO = "sparquet_it"

#: O pipeline acrescenta `ingestion_ts` depois do reader (`core/pipeline.py`), então
#: todo destino recebe QUATRO colunas — e um schema declarado à mão precisa dizer
#: isso, ou o conector recusa a coluna que não existe na tabela.
_COLUNAS = ("id", "nome", "valor", "ingestion_ts")


def _por_id(rotulo: str) -> dict:
    return {linha["id"]: linha for linha in harness.rows_back(rotulo)}


class _ConferaAVolta:
    """As três asserções que valem para qualquer destino: a contagem, a linha que
    quebra texto delimitado e o nulo que costuma voltar como string vazia."""

    def confira(self, rotulo: str) -> None:
        por_id = _por_id(rotulo)
        self.assertEqual(len(por_id), len(harness.SEED_ROWS))
        self.assertEqual(por_id["2"]["nome"], harness.SEED_ROWS[1][1])
        self.assertEqual(por_id["3"]["nome"], "")
        self.assertEqual(por_id["3"]["valor"], "")


# --------------------------------------------------------------------- MongoDB


@harness.requires_integration
@services.requires_service("mongodb")
class MongoTest(_ConferaAVolta, unittest.TestCase):
    def conexao(self) -> dict:
        host = services.host_of("mongodb")
        porta = services.port_of("mongodb")
        return {
            "connection.uri": f"mongodb://{host}:{porta}",
            "database": _ESPACO,
        }

    def test_ida_e_volta_pela_colecao_do_path(self) -> None:
        """`path` é a coleção: nenhuma opção `collection` no JSON."""
        written, read_back = harness.round_trip(
            "mongodb",
            {
                "format": "mongodb",
                "path": "semente",
                "mode": "overwrite",
                "options": self.conexao(),
            },
            {"format": "mongodb", "path": "semente", "options": self.conexao()},
            # `_id` é um struct e o CSV não escreve struct. Tirá-lo é o preço de
            # ler um documento com um schema tabular — e a prova de que o
            # conector devolve o resto exatamente como entrou.
            read_transformations=[{"type": "drop", "columns": ["_id"]}],
        )

        self.assertTrue(written.success, msg=written.error)
        self.assertTrue(read_back.success, msg=read_back.error)
        self.assertEqual(read_back.rows_read, len(harness.SEED_ROWS))
        self.confira("mongodb")

    def test_overwrite_recria_a_colecao_em_vez_de_somar(self) -> None:
        saida = {
            "format": "mongodb",
            "path": "substitui",
            "mode": "overwrite",
            "options": self.conexao(),
        }
        harness.run(
            {"name": "it-mongo-over-1", "input": harness.seed_input(), "output": saida}
        )
        harness.run(
            {"name": "it-mongo-over-2", "input": harness.seed_input(), "output": saida}
        )

        lido = harness.run(
            {
                "name": "it-mongo-over-leitura",
                "input": {
                    "format": "mongodb",
                    "path": "substitui",
                    "options": self.conexao(),
                },
                "output": {"format": "view", "path": "it_mongo_over"},
            }
        )
        self.assertTrue(lido.success, msg=lido.error)
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))

    def test_collection_em_options_sobrepoe_o_path(self) -> None:
        gravado = harness.run(
            {
                "name": "it-mongo-collection",
                "input": harness.seed_input(),
                "output": {
                    "format": "mongodb",
                    "path": "ignorada",
                    "mode": "overwrite",
                    "options": {**self.conexao(), "collection": "escolhida"},
                },
            }
        )
        self.assertTrue(gravado.success, msg=gravado.error)

        lido = harness.run(
            {
                "name": "it-mongo-collection-leitura",
                "input": {
                    "format": "mongodb",
                    "path": "escolhida",
                    "options": self.conexao(),
                },
                "output": {"format": "view", "path": "it_mongo_collection"},
            }
        )
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))


# -------------------------------------------------------------------- Cassandra


@harness.requires_integration
@services.requires_service("cassandra")
class CassandraTest(_ConferaAVolta, unittest.TestCase):
    """O conector NÃO cria schema: keyspace e tabela precisam existir antes.

    O DDL **não** vai pelo `CassandraCatalog`. Ele está compilado contra a API do
    Spark 3.5 e o primeiro `CREATE` morre com
    `java.lang.NoClassDefFoundError: org/apache/spark/sql/catalyst/analysis/
    NoSuchNamespaceException$` — classe que saiu no Spark 4. O caminho de dados
    do mesmo jar (`format("cassandra")`, ida e volta) funciona: o que quebrou é
    só o catálogo, e é por isso que estes testes existem apesar dele.

    Então o CQL vai pelo `CassandraConnector` do próprio jar, chamado por py4j
    com uma `SparkConf` montada na hora. Duas razões para a conf ser separada e
    não a da sessão: host e porta continuam sendo provados como *opção* de
    reader/writer, que é o que o usuário escreve no JSON, e o DDL não passa a
    depender de configuração global que os testes deveriam estar exercitando.
    """

    @classmethod
    def setUpClass(cls) -> None:
        jvm = harness.session().sparkContext._jvm
        conf = (
            jvm.org.apache.spark.SparkConf(False)
            .set("spark.cassandra.connection.host", services.host_of("cassandra"))
            .set("spark.cassandra.connection.port", str(services.port_of("cassandra")))
        )
        sessao = jvm.com.datastax.spark.connector.cql.CassandraConnector.apply(
            conf
        ).openSession()
        try:
            sessao.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {_ESPACO} WITH replication = "
                "{'class': 'SimpleStrategy', 'replication_factor': 1}"
            )
            for tabela in ("semente", "upsert"):
                # `ingestion_ts` entra porque o pipeline a acrescenta depois do
                # reader (`pipeline.py`): a tabela recebe quatro colunas, não três.
                sessao.execute(
                    f"CREATE TABLE IF NOT EXISTS {_ESPACO}.{tabela} "
                    "(id int PRIMARY KEY, nome text, valor double, "
                    "ingestion_ts timestamp)"
                )
        finally:
            sessao.close()

    def conexao(self) -> dict:
        return {
            "spark.cassandra.connection.host": services.host_of("cassandra"),
            "spark.cassandra.connection.port": str(services.port_of("cassandra")),
        }

    def test_ida_e_volta_com_keyspace_e_tabela_no_path(self) -> None:
        """`"keyspace.tabela"` num campo só, partido pelo conector em duas opções."""
        written, read_back = harness.round_trip(
            "cassandra",
            {
                "format": "cassandra",
                "path": f"{_ESPACO}.semente",
                "mode": "append",
                "options": self.conexao(),
            },
            {
                "format": "cassandra",
                "path": f"{_ESPACO}.semente",
                "options": self.conexao(),
            },
        )

        self.assertTrue(written.success, msg=written.error)
        self.assertTrue(read_back.success, msg=read_back.error)
        self.assertEqual(read_back.rows_read, len(harness.SEED_ROWS))
        self.confira("cassandra")

    def test_append_no_cassandra_e_upsert_pela_chave(self) -> None:
        """A exceção da suíte: `append` aqui não soma linha, sobrescreve a que tem
        a mesma partition key. Gravar duas vezes deixa três linhas, não seis."""
        saida = {
            "format": "cassandra",
            "path": f"{_ESPACO}.upsert",
            "mode": "append",
            "options": self.conexao(),
        }
        harness.run(
            {"name": "it-cass-upsert-1", "input": harness.seed_input(), "output": saida}
        )
        segunda = harness.run(
            {"name": "it-cass-upsert-2", "input": harness.seed_input(), "output": saida}
        )
        self.assertTrue(segunda.success, msg=segunda.error)

        lido = harness.run(
            {
                "name": "it-cass-upsert-leitura",
                "input": {
                    "format": "cassandra",
                    "path": f"{_ESPACO}.upsert",
                    "options": self.conexao(),
                },
                "output": {"format": "view", "path": "it_cass_upsert"},
            }
        )
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))

    def test_keyspace_em_options_com_o_path_sendo_so_a_tabela(self) -> None:
        lido = harness.run(
            {
                "name": "it-cass-keyspace-option",
                "input": {
                    "format": "cassandra",
                    "path": "semente",
                    "options": {**self.conexao(), "keyspace": _ESPACO},
                },
                "output": {"format": "view", "path": "it_cass_keyspace"},
            }
        )
        self.assertTrue(lido.success, msg=lido.error)
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))


# ---------------------------------------------------- Elasticsearch / OpenSearch


class _BuscaTests(_ConferaAVolta):
    """Corpo comum dos dois conectores de busca. O que muda entre eles é só o
    prefixo das opções — e é justamente por isso que são conectores separados."""

    #: Nome em `services.SERVICES`.
    servico = ""
    #: Formato no JSON.
    formato = ""
    #: `es` ou `opensearch` — o prefixo de TODAS as opções do conector.
    prefixo = ""

    def conexao(self) -> dict:
        return {
            f"{self.prefixo}.nodes": services.host_of(self.servico),
            f"{self.prefixo}.port": str(services.port_of(self.servico)),
            # Sem isto o conector pede a lista de nós ao cluster e recebe o IP
            # interno do container (172.x), inalcançável do host: a escrita
            # começa, o bulk vai para um endereço que não existe e o teste morre
            # em timeout sem dizer por quê.
            f"{self.prefixo}.nodes.wan.only": "true",
        }

    def indice(self, sufixo: str) -> str:
        return f"{_ESPACO.replace('_', '-')}-{sufixo}"

    def test_ida_e_volta_pelo_indice_do_path(self) -> None:
        indice = self.indice("semente")
        written, read_back = harness.round_trip(
            self.servico,
            {
                "format": self.formato,
                "path": indice,
                "mode": "overwrite",
                "options": self.conexao(),
            },
            {"format": self.formato, "path": indice, "options": self.conexao()},
        )

        self.assertTrue(written.success, msg=written.error)
        self.assertTrue(read_back.success, msg=read_back.error)
        self.assertEqual(read_back.rows_read, len(harness.SEED_ROWS))
        self.confira(self.servico)

    def test_mapping_id_usa_a_coluna_como_id_do_documento(self) -> None:
        """Com `_id` vindo de uma coluna, regravar o mesmo dado não duplica —
        que é a única forma de idempotência que estes conectores oferecem."""
        indice = self.indice("mapping-id")
        saida = {
            "format": self.formato,
            "path": indice,
            "mode": "append",
            "options": {**self.conexao(), f"{self.prefixo}.mapping.id": "id"},
        }

        harness.run(
            {"name": f"it-{self.servico}-id-1", "input": harness.seed_input(), "output": saida}
        )
        segunda = harness.run(
            {"name": f"it-{self.servico}-id-2", "input": harness.seed_input(), "output": saida}
        )
        self.assertTrue(segunda.success, msg=segunda.error)

        lido = harness.run(
            {
                "name": f"it-{self.servico}-id-leitura",
                "input": {
                    "format": self.formato,
                    "path": indice,
                    "options": self.conexao(),
                },
                "output": {"format": "view", "path": f"it_{self.servico}_id"},
            }
        )
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))


@harness.requires_integration
@services.requires_service("elasticsearch")
class ElasticsearchTest(_BuscaTests, unittest.TestCase):
    servico = "elasticsearch"
    formato = "elasticsearch"
    prefixo = "es"


@harness.requires_integration
@services.requires_service("opensearch")
class OpenSearchTest(_BuscaTests, unittest.TestCase):
    servico = "opensearch"
    formato = "opensearch"
    prefixo = "opensearch"


def _razao_da_rota_alternativa() -> Optional[str]:
    """Por que a rota "Elasticsearch pelo conector do OpenSearch" não roda.

    Ela precisa de dois containers, e por motivos diferentes: o **elasticsearch**
    porque é o servidor sob teste, e o **opensearch** porque é de quem está de pé
    que a sessão tira o jar (`services.packages_for_reachable`). Note que aqui o
    `incompativel` do elasticsearch não vale: ele fala do jar do
    `elasticsearch-spark`, que esta rota justamente não usa.
    """
    do_jar = services.skip_reason("opensearch")
    if do_jar:
        return f"o jar vem do opensearch, e {do_jar}"
    if not services.reachable("elasticsearch"):
        return (
            f"elasticsearch não responde em {services.host_of('elasticsearch')}:"
            f"{services.port_of('elasticsearch')} — "
            f"`docker compose -f {services.COMPOSE.name} up -d elasticsearch`"
        )
    return None


_SEM_ROTA = _razao_da_rota_alternativa()


@harness.requires_integration
@unittest.skipIf(_SEM_ROTA is not None, _SEM_ROTA or "")
class ElasticsearchViaOpenSearchTest(_BuscaTests, unittest.TestCase):
    """A saída para Elasticsearch em Spark 4: o conector do **OpenSearch**
    apontado para um servidor **Elasticsearch**.

    O `opensearch-spark` é fork do `elasticsearch-hadoop` e continua falando a
    API REST de índice e busca, então um Elasticsearch responde a ele. Medido
    contra o container 8.16.1: escrita, leitura e `mapping.id` passam, e os
    documentos aparecem no `_search` do próprio ES.

    O que **não** funciona, e por isso a migração de um pipeline custa duas
    edições no JSON em vez de zero:

      * `format: "elasticsearch"` com o jar do OpenSearch — o jar não registra o
        nome antigo: `[DATA_SOURCE_NOT_FOUND] Failed to find the data source: es`.
      * opções `es.*` no formato `opensearch` — o prefixo antigo é ignorado e a
        escrita morre em `OpenSearchHadoopIllegalArgumentException`.

    Ou seja: trocar `format` para `opensearch` e renomear `es.*` para
    `opensearch.*`. Nada mais muda — nem o servidor, nem o índice, nem o schema.
    """

    #: O servidor sob teste é o Elasticsearch: é dele que saem host e porta.
    servico = "elasticsearch"
    #: O conector é o do OpenSearch, com o prefixo de opção dele.
    formato = "opensearch"
    prefixo = "opensearch"

    def indice(self, sufixo: str) -> str:
        # Índice próprio: no dia em que sair um build de Spark 4 do
        # `elasticsearch-spark`, `ElasticsearchTest` volta a rodar contra o mesmo
        # servidor, e duas classes gravando no mesmo índice esconderiam falha.
        return super().indice(f"via-opensearch-{sufixo}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
