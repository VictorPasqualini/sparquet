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

**Compatibilidade com o Spark 4 ainda não medida** para os conectores marcados
abaixo. Os drivers JDBC são independentes de versão do Spark e devem funcionar;
os conectores com código Spark dentro (Cassandra, Elasticsearch, OpenSearch e o
Mongo) foram publicados contra o Spark 3.x, e a versão que serve ao Spark 4 é a
primeira coisa a descobrir quando esta camada for exercitada. Enquanto não for,
a coordenada aqui é um ponto de partida, não um fato verificado.
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
    package: Optional[str]
    #: `grupo:artefato` que a árvore do conector traz e a sessão não deve
    #: carregar. Entra em `spark.jars.excludes`.
    exclui: tuple = ()
    #: Preenchido quando **nenhuma** versão publicada do conector serve ao Spark
    #: desta máquina. O serviço pode estar de pé; o teste pula assim mesmo, e a
    #: razão é o que se descobriu executando. Some no dia em que sair um build.
    incompativel: str = ""


#: Chave = nome usado em `requires_service("postgres")` e nas variáveis de
#: ambiente (`SPARQUET_IT_POSTGRES_HOST`).
SERVICES: Dict[str, Service] = {
    "postgres": Service("postgres", 5432, "org.postgresql:postgresql:42.7.4"),
    "mysql": Service("mysql", 3306, "com.mysql:mysql-connector-j:9.1.0"),
    # Porta trocada de propósito: MariaDB e MySQL sobem juntos e brigariam pela 3306.
    "mariadb": Service("mariadb", 3307, "org.mariadb.jdbc:mariadb-java-client:3.5.1"),
    "sqlserver": Service(
        "sqlserver", 1433, "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"
    ),
    "oracle": Service(
        "oracle", 1521, "com.oracle.database.jdbc:ojdbc11:23.6.0.24.10"
    ),
    "kafka": Service("kafka", 9092, "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"),
    # Executada contra o container: lê e escreve no Spark 4.1.1 sem opção extra.
    "mongodb": Service(
        "mongodb", 27017, "org.mongodb.spark:mongo-spark-connector_2.13:10.4.1"
    ),
    # Executada contra o container: leitura e escrita passam no Spark 4.1.1, mas
    # o `CassandraCatalog` deste jar não sobe (é compilado contra a API do Spark
    # 3.5 — ver `CassandraTest`). É a última versão publicada; não existe build
    # para Spark 4. Quem precisa de DDL usa o `CassandraConnector` direto.
    "cassandra": Service(
        "cassandra", 9042, "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1"
    ),
    # Não existe artefato para Spark 4: o `-spark-30` é o último nome publicado
    # (8.16.1 até 9.0.3), e a árvore dele pede
    # `org.apache.spark:spark-yarn_2.13:3.4.3` — jar de Spark 3.4 no classpath de
    # um Spark 4.1. Excluído junto com `commons-logging`, cuja versão pedida
    # (1.1.1) o ivy resolve pelo cache local do m2 e não encontra o jar, o que
    # sozinho derruba a criação da sessão com `[JAVA_GATEWAY_EXITED] Java gateway
    # process exited before sending its port number.` Com os dois excluídos a
    # sessão sobe e a escrita morre no que não tem contorno.
    "elasticsearch": Service(
        "elasticsearch",
        9200,
        "org.elasticsearch:elasticsearch-spark-30_2.13:8.16.1",
        exclui=("org.apache.spark:spark-yarn_2.13", "commons-logging:commons-logging"),
        incompativel=(
            "elasticsearch-spark-30 (8.16.1 e 9.0.3, as últimas publicadas) chama "
            "Dataset.sqlContext(), removido no Spark 4: "
            "java.lang.NoSuchMethodError: 'org.apache.spark.sql.SQLContext "
            "org.apache.spark.sql.Dataset.sqlContext()'. Não há artefato "
            "elasticsearch-spark-40 no Maven Central"
        ),
    ),
    # Fork do elasticsearch-hadoop, e herdou o mesmo bloqueio — 1.3.0 é a última
    # publicada e falha igual, executada contra o container.
    "opensearch": Service(
        "opensearch",
        9201,
        "org.opensearch.client:opensearch-spark-30_2.13:1.3.0",
        incompativel=(
            "opensearch-spark-30 1.3.0 (a última publicada) chama "
            "Dataset.sqlContext(), removido no Spark 4: "
            "java.lang.NoSuchMethodError: 'org.apache.spark.sql.SQLContext "
            "org.apache.spark.sql.Dataset.sqlContext()'"
        ),
    ),
}


def _variavel(nome: str, sufixo: str) -> str:
    return f"SPARQUET_IT_{nome.upper()}_{sufixo}"


def host_of(nome: str) -> str:
    return os.environ.get(_variavel(nome, "HOST"), "127.0.0.1")


def port_of(nome: str) -> int:
    bruto = os.environ.get(_variavel(nome, "PORT"))
    return int(bruto) if bruto else SERVICES[nome].port


def package_of(nome: str) -> Optional[str]:
    """Coordenada do conector, sobrescrevível por
    `SPARQUET_IT_<SERVICO>_PACKAGE` — é assim que se testa outra versão sem
    editar o teste, e é o caminho para acertar as marcadas como a verificar."""
    override = os.environ.get(_variavel(nome, "PACKAGE"))
    if override is not None:
        return override or None
    return SERVICES[nome].package


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
    if servico.incompativel:
        return f"{nome}: {servico.incompativel}"
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
        if reachable(nome) and not servico.incompativel:
            excluidos.extend(servico.exclui)
    return excluidos


def packages_for_reachable() -> list:
    """Coordenadas dos serviços que estão de pé — o que a sessão deve carregar.

    Só os alcançáveis: um pacote listado é um download na criação da sessão, e
    quem não subiu serviço nenhum não deve pagar por nenhum. Marcado como
    `incompativel` também fica fora: o teste vai pular, então o jar seria
    resolução paga e classpath sujo sem nenhum teste em troca.
    """
    coordenadas = []
    for nome, servico in SERVICES.items():
        if not reachable(nome) or servico.incompativel:
            continue
        pacote = package_of(nome)
        if pacote:
            coordenadas.append(pacote)
    return coordenadas
