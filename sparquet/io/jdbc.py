from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter

# Chaves consumidas pelo framework para montar a conexão — não são repassadas ao
# Spark como opções soltas (url/driver/dbtable/query são aplicadas explicitamente;
# host/port/database servem só para montar a url quando 'url' não é informada).
_CONN_KEYS = {"url", "driver", "host", "port", "database", "dbtable", "query"}


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
      fetchsize  – linhas por round-trip
    """

    def read(self) -> DataFrame:
        opts = dict(self.config.options)
        reader = self.spark.read.format("jdbc").option("url", self._resolve_url(opts))

        driver = opts.get("driver", self._DRIVER)
        if driver:
            reader = reader.option("driver", driver)

        query = opts.get("query")
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

        driver = opts.get("driver", self._DRIVER)
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
    _DRIVER = "org.mariadb.jdbc.Driver"
    _DEFAULT_PORT = "3306"

    @staticmethod
    def _format_url(host: str, port: str, database: str) -> str:
        return f"jdbc:mariadb://{host}:{port}/{database}"


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
