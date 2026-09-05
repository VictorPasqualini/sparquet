from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter
from sparquet.utils.logger import defer_warning

# Chaves consumidas pelo framework para montar a conexão — não são repassadas ao
# Spark como opções soltas (url/driver/dbtable/query são aplicadas explicitamente;
# host/port/database servem só para montar a url quando 'url' não é informada).
_CONN_KEYS = {"url", "driver", "host", "port", "database", "dbtable", "query"}


# Quarteto de leitura paralela do Spark JDBC: `partitionColumn` só funciona com os
# outros três. O Spark exige "all or none" e falha com uma mensagem genérica; as
# outras três sem `partitionColumn` são IGNORADAS em silêncio e a leitura cai para
# uma única task/conexão.
_PARTITION_KEYS = ("partitionColumn", "lowerBound", "upperBound", "numPartitions")


def _validate_read_partitioning(opts: dict, who: str) -> None:
    """Recusa (ou avisa sobre) um quarteto de leitura paralela incompleto."""
    present = [key for key in _PARTITION_KEYS if opts.get(key) not in (None, "")]
    if not present:
        return

    if "partitionColumn" in present:
        missing = [key for key in _PARTITION_KEYS if key not in present]
        if missing:
            raise ValueError(
                f"{who}: 'partitionColumn' exige também {', '.join(missing)} — o Spark "
                f"pede os quatro juntos ({', '.join(_PARTITION_KEYS)}). Informe os "
                f"limites (ex: SELECT min/max da coluna) ou remova 'partitionColumn'."
            )
        return

    defer_warning(
        f"{who}: {', '.join(present)} sem 'partitionColumn' — o Spark IGNORA essas "
        f"opções na leitura e a tabela inteira vem numa única task/conexão. "
        f"Informe 'partitionColumn' (coluna numérica/data) com lowerBound, "
        f"upperBound e numPartitions para ler em paralelo.",
        options=present,
    )


class _JdbcDialect:
    """Presets por banco: driver JDBC, porta padrão e formato da url.

    Subclasses de reader/writer herdam de um dialeto + da base JDBC.
    """

    _DRIVER: str | None = None
    _DEFAULT_PORT: str | None = None

    @staticmethod
    def _format_url(host: str, port: str, database: str) -> str:  # pragma: no cover
        raise NotImplementedError

    @classmethod
    def _resolve_url(cls, opts: dict) -> str:
        url = opts.get("url")
        if url:
            return url
        host = opts.get("host")
        if not host:
            raise ValueError(
                f"{cls.__name__}: informe 'url' em options, ou 'host' "
                f"(+ 'database'/'port') para montar a url JDBC."
            )
        port = str(opts.get("port", cls._DEFAULT_PORT or ""))
        database = opts.get("database", "")
        return cls._format_url(host, port, database)

    @classmethod
    def _resolve_driver(cls, opts: dict) -> str | None:
        """Classe do driver: a informada em options, senão o default do dialeto."""
        return opts.get("driver") or cls._DRIVER

    def _passthrough(self, opts: dict) -> dict:
        return {k: v for k, v in opts.items() if k not in _CONN_KEYS}


class JdbcReader(BaseReader, _JdbcDialect):
    """Leitura via Spark JDBC.

    Requer o driver JDBC do banco no classpath (spark.jars / spark.jars.packages).

    path   = nome da tabela lida (dbtable). Ignorado quando 'query' é informado.

    Opções (options):
      url        – url JDBC completa (tem precedência). Ex: jdbc:postgresql://h:5432/db
      host/port/database – montam a url quando 'url' é omitida (port usa o default do banco)
      driver     – classe do driver (default por banco)
      user/password – credenciais
      query      – SELECT usado no lugar de 'dbtable' (não pode coexistir com dbtable)
      dbtable    – sobrepõe 'path' como tabela/subquery
      partitionColumn/lowerBound/upperBound/numPartitions – leitura paralela
                   (os QUATRO juntos; ver _validate_read_partitioning)
      fetchsize  – linhas por round-trip
      pushDownPredicate / pushDownAggregate / pushDownLimit / pushDownOffset /
      pushDownTableSample – empurram filtro, agregação, LIMIT, OFFSET e TABLESAMPLE
                   para o banco (passam direto ao Spark). Só valem no caminho v2
                   (leitura por tabela); com 'query' o SELECT já é o recorte.
      sessionInitStatement / queryTimeout / customSchema – idem

    Estratégia de leitura: sem 'partitionColumn' a tabela inteira vem numa única
    conexão e numa única task — todo o volume passa por um executor. Filtrar no
    banco (via 'query', ou 'dbtable' com subquery) é o que evita trazer o que
    não será usado; 'partitionColumn' é o que paraleliza o que sobra.
    """

    def read(self) -> DataFrame:
        opts = dict(self.config.options)
        query = opts.get("query")

        if query and opts.get("dbtable"):
            raise ValueError(
                f"{type(self).__name__}: 'query' e 'dbtable' são exclusivos — o Spark "
                f"recusa os dois juntos. Deixe só 'query' (SELECT completo), ou só "
                f"'dbtable' (tabela, ou subquery com alias: \"(SELECT ...) t\")."
            )
        if query and opts.get("partitionColumn"):
            raise ValueError(
                f"{type(self).__name__}: 'query' e 'partitionColumn' são exclusivos no "
                f"Spark. Para ler em paralelo um recorte da tabela, mova o SELECT para "
                f"'dbtable' como subquery com alias — \"(SELECT ... WHERE ...) t\" — e "
                f"mantenha partitionColumn/lowerBound/upperBound/numPartitions."
            )
        _validate_read_partitioning(opts, type(self).__name__)

        reader = self.spark.read.format("jdbc").option("url", self._resolve_url(opts))

        driver = self._resolve_driver(opts)
        if driver:
            reader = reader.option("driver", driver)

        if query:
            reader = reader.option("query", query)
        else:
            reader = reader.option("dbtable", opts.get("dbtable") or self.config.path)

        return reader.options(**self._passthrough(opts)).load()


class JdbcWriter(BaseWriter, _JdbcDialect):
    """Escrita via Spark JDBC.

    Modos: 'append' (INSERT) e 'overwrite' (recria/trunca a tabela). 'merge' não é
    suportado pelo JDBC nativo do Spark — use append/overwrite (ou 'truncate': 'true'
    em options no overwrite para preservar a tabela).

    path   = tabela de destino (dbtable).

    Opções (options):
      url, host/port/database, driver, user/password – conexão (ver JdbcReader)
      truncate       – 'true' faz TRUNCATE em vez de DROP/CREATE no overwrite
      batchsize      – linhas por batch de INSERT
      isolationLevel – nível de isolamento da transação
      createTableColumnTypes / createTableOptions – DDL na criação da tabela
    """

    def write(self, df: DataFrame) -> None:
        opts = dict(self.config.options)
        writer = (
            df.write.format("jdbc")
            .mode(self.config.mode)
            .option("url", self._resolve_url(opts))
            .option("dbtable", opts.get("dbtable") or self.config.path)
        )

        driver = self._resolve_driver(opts)
        if driver:
            writer = writer.option("driver", driver)

        writer.options(**self._passthrough(opts)).save()


# --------------------------------------------------------------------- dialetos


class _PostgresDialect(_JdbcDialect):
    _DRIVER = "org.postgresql.Driver"
    _DEFAULT_PORT = "5432"

    @staticmethod
    def _format_url(host: str, port: str, database: str) -> str:
        return f"jdbc:postgresql://{host}:{port}/{database}"


class _MySqlDialect(_JdbcDialect):
    _DRIVER = "com.mysql.cj.jdbc.Driver"
    _DEFAULT_PORT = "3306"

    @staticmethod
    def _format_url(host: str, port: str, database: str) -> str:
        return f"jdbc:mysql://{host}:{port}/{database}"


class _MariaDbDialect(_JdbcDialect):
    """MariaDB pela url do MySQL — é o único caminho com dialeto no Spark.

    O Spark 4.1 não tem dialeto MariaDB (a lista é DB2, Databricks, Derby, H2,
    MsSqlServer, MySQL, Oracle, Postgres, Snowflake, Teradata), e o `MySQLDialect`
    só reconhece url que comece com `jdbc:mysql`. Com `jdbc:mariadb://` cai-se no
    dialeto default, que cita identificador com `"` — o MariaDB recusa, e não é só
    a escrita: o SELECT que o Spark monta cita as colunas do mesmo jeito, então a
    leitura morre em `You have an error in your SQL syntax ... near '"id" INTEGER'`.

    O MariaDB fala o protocolo do MySQL, então o Connector/J com `jdbc:mysql://`
    conversa com o servidor MariaDB e traz o dialeto junto: citação com crase,
    mapeamento de tipos do MySQL e SQL correto no pushdown de agregação/LIMIT.
    Por isso o default deste formato é a url e o driver do MySQL.

    Para usar o driver do MariaDB assim mesmo (recurso específico dele, ou por
    política de jar), informe `url` e `driver` em options — o driver acompanha a
    url automaticamente — e junte `sessionVariables: sql_mode='ANSI_QUOTES'`, que
    faz o MariaDB aceitar `"` como citação de identificador. O preço é que, na
    mesma sessão, `"..."` deixa de ser literal de string: importa para quem usa
    `query`.
    """

    _DRIVER = "com.mysql.cj.jdbc.Driver"
    _MARIADB_DRIVER = "org.mariadb.jdbc.Driver"
    _DEFAULT_PORT = "3306"

    @staticmethod
    def _format_url(host: str, port: str, database: str) -> str:
        return f"jdbc:mysql://{host}:{port}/{database}"

    @classmethod
    def _resolve_driver(cls, opts: dict) -> str | None:
        driver = opts.get("driver")
        if driver:
            return driver
        # Url de MariaDB informada à mão: o Connector/J recusa uma url que não
        # comece com `jdbc:mysql`, então o driver acompanha a url.
        if str(opts.get("url", "")).startswith("jdbc:mariadb:"):
            return cls._MARIADB_DRIVER
        return cls._DRIVER

    @classmethod
    def _resolve_url(cls, opts: dict) -> str:
        url = super()._resolve_url(opts)
        variaveis = str(opts.get("sessionVariables", ""))
        if url.startswith("jdbc:mariadb:") and "ANSI_QUOTES" not in variaveis:
            defer_warning(
                "url jdbc:mariadb:// sem ANSI_QUOTES — o Spark 4 não tem dialeto "
                "MariaDB e cita identificador com aspas duplas, que o servidor "
                "recusa (leitura inclusive). Use a url jdbc:mysql:// (o default "
                "deste formato) ou acrescente "
                "sessionVariables: sql_mode='ANSI_QUOTES'.",
                url=url,
            )
        return url


class _SqlServerDialect(_JdbcDialect):
    _DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    _DEFAULT_PORT = "1433"

    @staticmethod
    def _format_url(host: str, port: str, database: str) -> str:
        return f"jdbc:sqlserver://{host}:{port};databaseName={database}"


class _OracleDialect(_JdbcDialect):
    _DRIVER = "oracle.jdbc.OracleDriver"
    _DEFAULT_PORT = "1521"

    @staticmethod
    def _format_url(host: str, port: str, database: str) -> str:
        # 'database' é interpretado como service name (forma //host:port/service).
        return f"jdbc:oracle:thin:@//{host}:{port}/{database}"


class PostgresReader(_PostgresDialect, JdbcReader):
    pass


class PostgresWriter(_PostgresDialect, JdbcWriter):
    pass


class MySqlReader(_MySqlDialect, JdbcReader):
    pass


class MySqlWriter(_MySqlDialect, JdbcWriter):
    pass


class MariaDbReader(_MariaDbDialect, JdbcReader):
    pass


class MariaDbWriter(_MariaDbDialect, JdbcWriter):
    pass


class SqlServerReader(_SqlServerDialect, JdbcReader):
    pass


class SqlServerWriter(_SqlServerDialect, JdbcWriter):
    pass


class OracleReader(_OracleDialect, JdbcReader):
    pass


class OracleWriter(_OracleDialect, JdbcWriter):
    pass
