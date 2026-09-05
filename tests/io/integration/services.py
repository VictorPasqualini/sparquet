"""O terceiro grupo de conectores: os que precisam de um serviço de pé.

`avro`/`delta`/`iceberg` precisam só de um jar, e `xml`/`binary` nem isso — esses
rodam em qualquer máquina, e o `harness` cuida deles. Postgres, MySQL, MariaDB,
SQL Server, Oracle, Kafka, MongoDB, Cassandra, Elasticsearch e OpenSearch
precisam de um servidor escutando numa porta. Este módulo é o que decide, na hora
do teste, se esse servidor existe.

A regra é a mesma do resto da suíte: **nunca falhar por ausência de ambiente**. Um
teste de serviço só roda se a porta responder; se não responder, ele é pulado com
a razão dizendo exatamente o que subir. Ninguém precisa de Docker para rodar os
testes do sparquet, e quem tem ganha cobertura a mais sem mudar nada.

    docker compose -f tests/io/integration/docker-compose.yml up -d postgres
    SPARQUET_IT=1 python tests/io/integration/test_jdbc_services_spark.py

O jar de cada serviço entra na sessão **só quando aquele serviço responde**
(`packages_for_reachable`). Isso não é economia de disco: o `spark.jars.packages`
resolve no Maven na criação da sessão, e listar um conector que ninguém vai usar
faria toda execução pagar o download — e falhar sem rede.

Endereço e porta saem do ambiente quando preciso, sem editar teste:

    SPARQUET_IT_POSTGRES_HOST=10.0.0.5 SPARQUET_IT_POSTGRES_PORT=6432 ...

**Compatibilidade com o Spark 4 medida contra os containers**, no 4.1.1: cada linha
da tabela `SERVICES` é ou uma coordenada que passou, ou um campo `incompativel`
dizendo o que se descobriu executando. Os drivers JDBC são independentes da versão
do Spark; é nos conectores com código Spark dentro que mora a divergência — o
`elasticsearch-spark` não tem build de Spark 4 e o `opensearch-spark-40` tem, e o do
Cassandra passa no caminho de dados mas não registra catálogo. Ao trocar uma
coordenada, medir de novo: o pom é a evidência mais barata, o container é a
definitiva.

**Coordenada por linha do Spark.** Um jar de conector é publicado por linha e por
binário do Scala (2.13 no Spark 4.x, 2.12 no 3.5), então `package` é um molde com
`{scala}` e `{spark}` — `harness.scala_binary()` e `harness.spark_version()`
preenchem. Quando muda o nome do artefato, e não só o sufixo, a linha entra em
`por_linha`; é o caso do OpenSearch (`-spark-40` no Spark 4, `-spark-30` no 3.5).
`incompativel_em` diz em quais linhas a incompatibilidade vale: o
`elasticsearch-spark` não roda no Spark 4 e roda no 3.5, que é justamente a saída
para quem precisa do conector nativo.
"""
from __future__ import annotations

import os
import socket
import unittest
from pathlib import Path
from typing import Dict, NamedTuple, Optional

#: Onde fica o compose que sobe tudo isto.
COMPOSE = Path(__file__).resolve().parent / "docker-compose.yml"


class Service(NamedTuple):
    """Um serviço externo e o que é preciso para falar com ele."""

    #: Nome do serviço no `docker-compose.yml` — é o que a razão do skip manda subir.
    compose: str
    #: Porta padrão, sobrescrevível por ambiente.
    port: int
    #: Coordenada Maven do conector, ou `None` quando o jar já vem no pyspark.
    #: Aceita `{scala}` (o binário da linha) e `{spark}` (a versão exata do
    #: pyspark): os conectores publicados pelo próprio Spark seguem a versão dele,
    #: os de terceiros seguem só o binário do Scala.
    package: Optional[str]
    #: `grupo:artefato` que a árvore do conector traz e a sessão não deve
    #: carregar. Entra em `spark.jars.excludes`; aceita `{scala}`.
    exclui: tuple = ()
    #: Coordenada inteira para uma linha do Spark específica, quando o **nome** do
    #: artefato muda e não só o sufixo: `{"3.5": "grupo:artefato_2.12:versao"}`.
    por_linha: Dict[str, str] = {}
    #: Preenchido quando **nenhuma** versão publicada do conector serve ao Spark
    #: desta máquina. O serviço pode estar de pé; o teste pula assim mesmo, e a
    #: razão é o que se descobriu executando. Some no dia em que sair um build.
    incompativel: str = ""
    #: Linhas do Spark em que `incompativel` vale. Vazio = todas. Existe porque a
    #: incompatibilidade costuma ser de uma linha só — o mesmo jar que não roda no
    #: Spark 4 roda no 3.5.
    incompativel_em: tuple = ()


#: Chave = nome usado em `requires_service("postgres")` e nas variáveis de
#: ambiente (`SPARQUET_IT_POSTGRES_HOST`).
SERVICES: Dict[str, Service] = {
    "postgres": Service("postgres", 5432, "org.postgresql:postgresql:42.7.4"),
    "mysql": Service("mysql", 3306, "com.mysql:mysql-connector-j:9.1.0"),
    # Porta trocada de propósito: MariaDB e MySQL sobem juntos e brigariam pela 3306.
    # Dois drivers: o dialeto monta url de MySQL (é o que traz o MySQLDialect), e o
    # driver do MariaDB é o da rota alternativa, com url `jdbc:mariadb://` informada
    # à mão — `MariaDbUrlExplicitaTest` exercita essa.
    "mariadb": Service(
        "mariadb",
        3307,
        "com.mysql:mysql-connector-j:9.1.0,org.mariadb.jdbc:mariadb-java-client:3.5.1",
        # O `mariadb-java-client` puxa `waffle-jna` (login integrado do Windows,
        # que nenhum teste usa) e com ele `jna`, `jna-platform` e `caffeine`:
        # cinco jars a mais no `spark.jars`, um deles carregando biblioteca
        # nativa, num classpath onde ninguém os chama. E não é só peso morto —
        # medido no Windows, com eles a criação da sessão entrou no mesmo laço de
        # `BlockManagerId ... idWithoutTopologyInfo is null` descrito em
        # `harness.USE_BASE_PACKAGES`, sem nenhum teste progredir; com o exclude,
        # zero ocorrências e a suíte passa. O laço é intermitente (há execução
        # que se recupera dele), então vale como indício, não como prova: o que
        # se sabe é que jar que ninguém usa no `spark.jars` só tem a perder.
        exclui=("com.github.waffle:waffle-jna",),
    ),
    "sqlserver": Service(
        "sqlserver", 1433, "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"
    ),
    "oracle": Service(
        "oracle", 1521, "com.oracle.database.jdbc:ojdbc11:23.6.0.24.10"
    ),
    # Publicado pelo próprio Spark: a coordenada é a versão exata do pyspark
    # instalado, não a linha. Um `spark-sql-kafka` de outro patch resolve, mas
    # é classe do Spark duplicada no classpath sem nenhum motivo.
    "kafka": Service(
        "kafka", 9092, "org.apache.spark:spark-sql-kafka-0-10_{scala}:{spark}"
    ),
    # Executada contra o container: lê e escreve no Spark 4.1.1 sem opção extra.
    "mongodb": Service(
        "mongodb", 27017, "org.mongodb.spark:mongo-spark-connector_{scala}:10.4.1"
    ),
    # Executada contra o container: leitura e escrita passam no Spark 4.1.1, mas
    # o `CassandraCatalog` deste jar não sobe (é compilado contra a API do Spark
    # 3.5 — ver `CassandraTest`). É a última versão publicada; não existe build
    # para Spark 4. Quem precisa de DDL usa o `CassandraConnector` direto.
    "cassandra": Service(
        "cassandra", 9042, "com.datastax.spark:spark-cassandra-connector_{scala}:3.5.1"
    ),
    # Não existe artefato para Spark 4: `-spark-30` continua sendo o último nome
    # publicado (até 9.5.3, e o pom dela ainda declara spark-core/sql/catalyst
    # 3.4.3; `elasticsearch-spark-40_2.13` devolve 404 no Maven Central). A árvore
    # dele pede
    # `org.apache.spark:spark-yarn_2.13:3.4.3` — jar de Spark 3.4 no classpath de
    # um Spark 4.1. Excluído junto com `commons-logging`, cuja versão pedida
    # (1.1.1) o ivy resolve pelo cache local do m2 e não encontra o jar, o que
    # sozinho derruba a criação da sessão com `[JAVA_GATEWAY_EXITED] Java gateway
    # process exited before sending its port number.` Com os dois excluídos a
    # sessão sobe e a escrita morre no que não tem contorno.
    "elasticsearch": Service(
        "elasticsearch",
        9200,
        "org.elasticsearch:elasticsearch-spark-30_{scala}:8.16.1",
        exclui=("org.apache.spark:spark-yarn_{scala}", "commons-logging:commons-logging"),
        # A incompatibilidade é da linha 4.x: o `-spark-30` é build de Spark 3 e
        # roda no 3.5, que é a saída de quem precisa do conector nativo.
        incompativel_em=("4.1",),
        incompativel=(
            "elasticsearch-spark-30 (8.16.1 e 9.0.3, executadas; 9.5.3 é a última "
            "publicada e ainda compila contra Spark 3.4.3) chama "
            "Dataset.sqlContext(), removido no Spark 4: "
            "java.lang.NoSuchMethodError: 'org.apache.spark.sql.SQLContext "
            "org.apache.spark.sql.Dataset.sqlContext()'. Não há artefato "
            "elasticsearch-spark-40 no Maven Central. A rota que funciona é o "
            "conector do OpenSearch apontado para o servidor do Elasticsearch — "
            "ver ElasticsearchViaOpenSearchTest"
        ),
    ),
    # O fork do elasticsearch-hadoop foi o que saiu na frente: o
    # `opensearch-spark-40_2.13:2.0.0` é build de Spark 4 de verdade (o pom declara
    # spark-core/spark-sql 4.1.1 e Scala 2.13.16), enquanto o `-spark-30` 1.3.0 que
    # estava aqui chamava `Dataset.sqlContext()`, removido no Spark 4, e falhava com
    # `java.lang.NoSuchMethodError: 'org.apache.spark.sql.SQLContext
    # org.apache.spark.sql.Dataset.sqlContext()'` contra o container. Este é hoje o
    # único conector de busca com caminho no Spark 4 — para Elasticsearch não há.
    #
    # As dependências de Spark do jar entram em `exclui` porque `spark.jars.packages`
    # resolve a árvore inteira: sem isso o ivy baixa `spark-core`, `spark-sql`,
    # `spark-sql-api`, `spark-common-utils`, `spark-streaming` e `spark-yarn` 4.1.1 e
    # os põe em `spark.jars` — um segundo jogo de classes do Spark no classpath do
    # driver, mais alguns minutos de download na primeira sessão. Medido nas duas
    # configurações: a suíte passa com e sem os excludes (o pyspark instalado ganha do
    # `spark.jars` na ordem do classpath), então isto não é correção de quebra — é
    # não deixar a sessão de teste depender dessa ordem no dia em que a versão do
    # pyspark e a que o conector declara divergirem.
    "opensearch": Service(
        "opensearch",
        9201,
        "org.opensearch.client:opensearch-spark-40_2.13:2.0.0",
        exclui=(
            "org.apache.spark:spark-core_{scala}",
            "org.apache.spark:spark-sql_{scala}",
            "org.apache.spark:spark-sql-api_{scala}",
            "org.apache.spark:spark-common-utils_{scala}",
            "org.apache.spark:spark-streaming_{scala}",
            "org.apache.spark:spark-yarn_{scala}",
        ),
        # No Spark 3.5 o artefato é outro, não só o sufixo: o `-spark-40` é
        # build de Spark 4 (o pom declara spark-sql 4.1.1) e o `-spark-30`
        # 1.3.0 é o último publicado para a linha 3.x — é o mesmo jar que
        # falhava aqui no Spark 4 chamando `Dataset.sqlContext()`.
        por_linha={"3.5": "org.opensearch.client:opensearch-spark-30_2.12:1.3.0"},
    ),
}


def _variavel(nome: str, sufixo: str) -> str:
    return f"SPARQUET_IT_{nome.upper()}_{sufixo}"


def host_of(nome: str) -> str:
    return os.environ.get(_variavel(nome, "HOST"), "127.0.0.1")


def port_of(nome: str) -> int:
    bruto = os.environ.get(_variavel(nome, "PORT"))
    return int(bruto) if bruto else SERVICES[nome].port


def _linha_do_spark() -> str:
    """A linha do pyspark instalado, pelo harness — importado aqui dentro porque
    é ele quem importa este módulo, não o contrário."""
    import harness

    return harness.spark_line()


def _molde(coordenada: str) -> str:
    """Preenche `{scala}` e `{spark}` da coordenada com o que esta linha usa."""
    import harness

    return coordenada.format(
        scala=harness.scala_binary(), spark=harness.spark_version()
    )


def package_of(nome: str) -> Optional[str]:
    """Coordenada do conector nesta linha do Spark, sobrescrevível por
    `SPARQUET_IT_<SERVICO>_PACKAGE` — é assim que se testa outra versão sem
    editar o teste, e é o caminho para acertar as marcadas como a verificar."""
    override = os.environ.get(_variavel(nome, "PACKAGE"))
    if override is not None:
        return override or None
    servico = SERVICES[nome]
    coordenada = servico.por_linha.get(_linha_do_spark(), servico.package)
    return _molde(coordenada) if coordenada else None


def incompativel_aqui(nome: str) -> str:
    """A incompatibilidade registrada, quando ela vale nesta linha do Spark.

    `incompativel_em` vazio quer dizer "em todas": é o caso de quem não tem build
    em lugar nenhum. Com linhas listadas, o mesmo jar pode estar fora numa e
    dentro na outra — `elasticsearch-spark` não roda no Spark 4 e roda no 3.5.
    """
    servico = SERVICES[nome]
    if not servico.incompativel:
        return ""
    if servico.incompativel_em and _linha_do_spark() not in servico.incompativel_em:
        return ""
    return servico.incompativel


#: Uma porta ou responde rápido ou não está lá. Um timeout generoso aqui só faria
#: a suíte inteira esperar por cada serviço ausente.
_TIMEOUT = 1.0

_cache: Dict[str, bool] = {}


def reachable(nome: str) -> bool:
    """A porta aceita conexão? Resultado memorizado — a resposta não muda no meio
    de uma execução, e sondar de novo a cada teste custaria segundos."""
    if nome in _cache:
        return _cache[nome]
    try:
        with socket.create_connection((host_of(nome), port_of(nome)), _TIMEOUT):
            resposta = True
    except OSError:
        resposta = False
    _cache[nome] = resposta
    return resposta


def skip_reason(nome: str) -> Optional[str]:
    """Por que o teste deste serviço não roda — `None` quando ele roda.

    Dois motivos, e o segundo não se resolve subindo container: quando não
    existe versão do conector que funcione neste Spark, o teste pula com o que
    se descobriu executando. Apagar o `incompativel` da tabela é o que basta
    para ele voltar a rodar no dia em que sair um build.
    """
    servico = SERVICES[nome]
    incompativel = incompativel_aqui(nome)
    if incompativel:
        return f"{nome}: {incompativel}"
    if reachable(nome):
        return None
    return (
        f"{nome} não responde em {host_of(nome)}:{port_of(nome)} — "
        f"`docker compose -f {COMPOSE.name} up -d {servico.compose}`"
    )


def requires_service(nome: str):
    """Decorador de classe/método: pula quando o serviço não está de pé."""
    razao = skip_reason(nome)
    return unittest.skipIf(razao is not None, razao or "")


def excludes_for_reachable() -> list:
    """`grupo:artefato` que os serviços de pé pedem e a sessão não deve carregar.

    Conector de datasource costuma declarar dependência de Spark inteira, e um
    jar de outra linha do Spark no classpath quebra coisa que não tem nada a ver
    com o conector. O que cada um precisa excluir está na tabela `SERVICES`.
    """
    excluidos = []
    for nome, servico in SERVICES.items():
        if reachable(nome) and not incompativel_aqui(nome):
            excluidos.extend(_molde(item) for item in servico.exclui)
    return excluidos


def packages_for_reachable() -> list:
    """Coordenadas dos serviços que estão de pé — o que a sessão deve carregar.

    Só os alcançáveis: um pacote listado é um download na criação da sessão, e
    quem não subiu serviço nenhum não deve pagar por nenhum. Marcado como
    `incompativel` também fica fora: o teste vai pular, então o jar seria
    resolução paga e classpath sujo sem nenhum teste em troca.
    """
    coordenadas = []
    for nome in SERVICES:
        if not reachable(nome) or incompativel_aqui(nome):
            continue
        pacote = package_of(nome)
        if pacote:
            coordenadas.append(pacote)
    return coordenadas
