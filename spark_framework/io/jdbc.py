"""Conectores JDBC — bancos relacionais como fonte e destino do pipeline.

Um unico formato (`jdbc`) cobre qualquer banco com driver JDBC, e os apelidos
`postgres`, `mysql`, `sqlserver` e `oracle` preenchem driver, porta e formato de
URL para os casos comuns:

    { "format": "postgres", "path": "public.clientes",
      "options": { "host": "db.interno", "database": "vendas",
                   "user": "app", "password_env": "PG_PASSWORD" } }

O JAR do driver precisa estar no classpath. Fora do Databricks o caminho mais
simples e declarar o pacote no proprio JSON:

    { "spark": { "configs": { "spark.jars.packages": "org.postgresql:postgresql:42.7.4" } } }

CREDENCIAIS
    Prefira `user_env` / `password_env`, que leem variaveis de ambiente e mantem
    a senha fora do JSON versionado. `user` / `password` literais funcionam (e
    aceitam `{param}` de template), mas o conf passa a ser um segredo.

LEITURA
    path            – tabela (`schema.tabela`); ignorado quando `query` e usada
    options.query   – SQL livre executado no banco (pushdown manual)
    particionamento – partition_column + lower_bound + upper_bound + num_partitions
                      fazem o Spark abrir N conexoes paralelas; sem eles a leitura
                      inteira passa por uma unica conexao.

ESCRITA
    modes overwrite | append | ignore | error usam o writer JDBC do Spark.
    mode merge faz upsert: grava um staging temporario e roda o SQL de upsert do
    banco (ON CONFLICT no Postgres, ON DUPLICATE KEY no MySQL, MERGE no SQL Server
    e Oracle), exigindo `merge_keys` e uma constraint unica sobre essas colunas.
"""

from __future__ import annotations

import os
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame

from spark_framework.io.base import BaseReader, BaseWriter
from spark_framework.utils.logger import logger

# format do JSON → driver, prefixo de URL e porta default do banco.
VENDORS: Dict[str, Dict[str, Any]] = {
    "postgres": {
        "driver": "org.postgresql.Driver",
        "port": 5432,
        "package": "org.postgresql:postgresql:42.7.4",
        "quote": '"',
    },
    "mysql": {
        "driver": "com.mysql.cj.jdbc.Driver",
        "port": 3306,
        "package": "com.mysql:mysql-connector-j:9.1.0",
        "quote": "`",
    },
    "sqlserver": {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "port": 1433,
        "package": "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11",
        "quote": "[]",
    },
    "oracle": {
        "driver": "oracle.jdbc.OracleDriver",
        "port": 1521,
        "package": "com.oracle.database.jdbc:ojdbc11:23.6.0.24.10",
        "quote": '"',
    },
    # Generico: exige url e driver explicitos.
    "jdbc": {"driver": None, "port": None, "package": None, "quote": '"'},
}

# Apelidos aceitos no campo "format" alem do nome canonico.
VENDOR_ALIASES = {
    "postgresql": "postgres",
    "mssql": "sqlserver",
}

# snake_case amigavel → nome da opcao no conector JDBC do Spark.
_OPTION_ALIASES = {
    "partition_column": "partitionColumn",
    "lower_bound": "lowerBound",
    "upper_bound": "upperBound",
    "num_partitions": "numPartitions",
    "fetch_size": "fetchsize",
    "batch_size": "batchsize",
    "isolation_level": "isolationLevel",
    "session_init_statement": "sessionInitStatement",
    "push_down_predicate": "pushDownPredicate",
    "create_table_options": "createTableOptions",
    "create_table_column_types": "createTableColumnTypes",
    "query_timeout": "queryTimeout",
    "truncate_table": "truncate",
}

# Chaves consumidas pelo conector do framework, nunca repassadas ao Spark.
_FRAMEWORK_KEYS = {
    "host",
    "port",
    "database",
    "url",
    "user",
    "user_env",
    "password",
    "password_env",
    "query",
    "merge_keys",
    "merge_update_columns",
    "staging_table",
}

_TABLE_PATTERN = re.compile(r"^[A-Za-z_][\w$]*(\.[A-Za-z_][\w$]*){0,2}$")


def canonical_vendor(format_name: str) -> str:
    """Normaliza o `format` do JSON para uma chave de VENDORS."""
    name = (format_name or "").strip().lower()
    return VENDOR_ALIASES.get(name, name)


def build_url(vendor: str, options: Dict[str, Any]) -> str:
    """Monta a URL JDBC a partir de host/port/database, ou usa `url` se informada."""
    explicit = str(options.get("url") or "").strip()
    if explicit:
        return explicit

    if vendor == "jdbc":
        raise ValueError(
            "jdbc: informe 'url' em options (ex: "
            "\"jdbc:postgresql://host:5432/base\"), ou use um dos formatos "
            "'postgres', 'mysql', 'sqlserver', 'oracle'."
        )

    host = str(options.get("host") or "").strip()
    database = str(options.get("database") or "").strip()
    if not host:
        raise ValueError(f"{vendor}: informe 'host' (ou uma 'url' completa) em options.")
    if not database and vendor != "oracle":
        raise ValueError(f"{vendor}: informe 'database' (ou uma 'url' completa) em options.")

    port = options.get("port") or VENDORS[vendor]["port"]

    if vendor == "postgres":
        return f"jdbc:postgresql://{host}:{port}/{database}"
    if vendor == "mysql":
        return f"jdbc:mysql://{host}:{port}/{database}"
    if vendor == "sqlserver":
        return f"jdbc:sqlserver://{host}:{port};databaseName={database}"
    if vendor == "oracle":
        # `database` e o service name; sem ele o usuario deve passar a url pronta.
        if not database:
            raise ValueError(
                "oracle: informe 'database' (service name) ou uma 'url' completa em options."
            )
        return f"jdbc:oracle:thin:@//{host}:{port}/{database}"

    raise ValueError(f"Formato JDBC nao suportado: '{vendor}'.")


def resolve_secret(options: Dict[str, Any], key: str) -> Optional[str]:
    """Le `<key>` literal ou `<key>_env` (nome de variavel de ambiente)."""
    env_key = options.get(f"{key}_env")
    if env_key:
        value = os.environ.get(str(env_key))
        if value is None:
            raise ValueError(
                f"jdbc: variavel de ambiente '{env_key}' (referida em '{key}_env') "
                "nao esta definida no ambiente de execucao."
            )
        return value

    literal = options.get(key)
    return None if literal is None else str(literal)


def spark_options(vendor: str, options: Dict[str, Any]) -> Dict[str, str]:
    """Opcoes repassadas ao conector JDBC do Spark, com aliases resolvidos."""
    resolved: Dict[str, str] = {}

    for key, value in options.items():
        if key in _FRAMEWORK_KEYS:
            continue
        resolved[_OPTION_ALIASES.get(key, key)] = str(value)

    resolved["url"] = build_url(vendor, options)

    driver = options.get("driver") or VENDORS[vendor]["driver"]
    if driver:
        resolved["driver"] = str(driver)
    elif "driver" not in resolved:
        raise ValueError(
            "jdbc: informe 'driver' em options (ex: \"org.postgresql.Driver\") "
            "quando usar o formato generico 'jdbc'."
        )

    user = resolve_secret(options, "user")
    password = resolve_secret(options, "password")
    if user is not None:
        resolved["user"] = user
    if password is not None:
        resolved["password"] = password

    return resolved


def quote_identifier(vendor: str, name: str) -> str:
    """Cita um identificador com o delimitador do banco, preservando `schema.tabela`."""
    quote = VENDORS.get(vendor, VENDORS["jdbc"])["quote"]
    parts = name.split(".")

    def wrap(part: str) -> str:
        if quote == "[]":
            return f"[{part.replace(']', ']]')}]"
        return f"{quote}{part.replace(quote, quote * 2)}{quote}"

    return ".".join(wrap(part) for part in parts)


def staging_name(table: str) -> str:
    """Nome do staging do upsert, no mesmo schema da tabela destino."""
    parts = table.split(".")
    suffix = uuid.uuid4().hex[:8]
    parts[-1] = f"{parts[-1]}_sparquet_stg_{suffix}"
    return ".".join(parts)


def upsert_sql(
    vendor: str,
    target: str,
    staging: str,
    columns: List[str],
    keys: List[str],
    update_columns: Optional[List[str]] = None,
) -> str:
    """SQL de upsert do banco, lendo do staging e gravando no destino.

    Postgres e MySQL usam a sintaxe nativa de INSERT ... ON CONFLICT /
    ON DUPLICATE KEY; SQL Server e Oracle usam MERGE ANSI. Em todos os casos as
    colunas de `keys` precisam de uma constraint unica no destino.
    """
    missing = [key for key in keys if key not in columns]
    if missing:
        raise ValueError(
            f"jdbc merge: merge_keys {missing} nao existem no DataFrame. "
            f"Colunas disponiveis: {columns}"
        )

    updatable = update_columns if update_columns is not None else [
        column for column in columns if column not in keys
    ]

    q = lambda name: quote_identifier(vendor, name)  # noqa: E731
    target_sql = q(target)
    staging_sql = q(staging)
    column_list = ", ".join(q(column) for column in columns)
    select_list = ", ".join(f"S.{q(column)}" for column in columns)

    if vendor == "postgres":
        if not updatable:
            return (
                f"INSERT INTO {target_sql} ({column_list}) "
                f"SELECT {select_list} FROM {staging_sql} S "
                f"ON CONFLICT ({', '.join(q(key) for key in keys)}) DO NOTHING"
            )
        assignments = ", ".join(f"{q(column)} = EXCLUDED.{q(column)}" for column in updatable)
        return (
            f"INSERT INTO {target_sql} ({column_list}) "
            f"SELECT {select_list} FROM {staging_sql} S "
            f"ON CONFLICT ({', '.join(q(key) for key in keys)}) DO UPDATE SET {assignments}"
        )

    if vendor == "mysql":
        if not updatable:
            return (
                f"INSERT IGNORE INTO {target_sql} ({column_list}) "
                f"SELECT {select_list} FROM {staging_sql} S"
            )
        assignments = ", ".join(f"{q(column)} = VALUES({q(column)})" for column in updatable)
        return (
            f"INSERT INTO {target_sql} ({column_list}) "
            f"SELECT {select_list} FROM {staging_sql} S "
            f"ON DUPLICATE KEY UPDATE {assignments}"
        )

    # SQL Server e Oracle: MERGE ANSI.
    on_clause = " AND ".join(f"T.{q(key)} = S.{q(key)}" for key in keys)
    matched = (
        " WHEN MATCHED THEN UPDATE SET "
        + ", ".join(f"T.{q(column)} = S.{q(column)}" for column in updatable)
        if updatable
        else ""
    )
    statement = (
        f"MERGE INTO {target_sql} T USING {staging_sql} S ON ({on_clause})"
        f"{matched} "
        f"WHEN NOT MATCHED THEN INSERT ({column_list}) VALUES ({select_list})"
    )
    # O SQL Server exige o ponto e virgula final no MERGE; o Oracle o rejeita via JDBC.
    return f"{statement};" if vendor == "sqlserver" else statement


class JdbcReader(BaseReader):
    """Le uma tabela ou uma query de um banco relacional via JDBC.

    Exemplos de JSON:
      { "format": "postgres", "path": "public.clientes",
        "options": { "host": "db.interno", "database": "vendas",
                     "user": "app", "password_env": "PG_PASSWORD" } }

      { "format": "mysql", "path": "ignorado",
        "options": { "host": "db", "database": "loja", "user": "app",
                     "password_env": "MYSQL_PASSWORD",
                     "query": "SELECT id, total FROM pedidos WHERE dt >= '2026-01-01'" } }

      { "format": "postgres", "path": "public.eventos",
        "options": { "url": "jdbc:postgresql://db:5432/analytics",
                     "user_env": "PG_USER", "password_env": "PG_PASSWORD",
                     "partition_column": "id", "lower_bound": 1,
                     "upper_bound": 5000000, "num_partitions": 8 } }
    """

    def read(self) -> DataFrame:
        vendor = canonical_vendor(self.config.format)
        options = dict(self.config.options)
        query = str(options.get("query") or "").strip()

        resolved = spark_options(vendor, options)

        if query:
            resolved["query"] = query
        else:
            table = (self.config.path or "").strip()
            if not table:
                raise ValueError(
                    f"{vendor}: informe a tabela em 'path' (ex: \"public.clientes\") "
                    "ou uma consulta em options.query."
                )
            resolved["dbtable"] = table

        partitioned = "partitionColumn" in resolved
        if partitioned:
            faltando = [
                key
                for key in ("lowerBound", "upperBound", "numPartitions")
                if key not in resolved
            ]
            if faltando:
                raise ValueError(
                    f"{vendor}: leitura particionada exige {faltando} junto de "
                    "'partition_column'."
                )

        logger.info(
            "Lendo via JDBC",
            vendor=vendor,
            source=resolved.get("query", resolved.get("dbtable")),
            partitions=resolved.get("numPartitions", 1),
        )
        return self.spark.read.format("jdbc").options(**resolved).load()


class JdbcWriter(BaseWriter):
    """Grava um DataFrame em uma tabela relacional via JDBC.

    Modos:
      append    – insere linhas (default do dia a dia)
      overwrite – recria a tabela; use options.truncate_table=true para preservar
                  a estrutura (e as constraints) e apagar so as linhas
      ignore    – nao escreve se a tabela ja existir
      error     – falha se a tabela ja existir
      merge     – upsert por merge_keys, via staging temporario + SQL do banco

    Exemplos de JSON:
      { "format": "postgres", "path": "public.clientes", "mode": "append",
        "options": { "host": "db", "database": "vendas", "user": "app",
                     "password_env": "PG_PASSWORD", "batch_size": 5000 } }

      { "format": "postgres", "path": "public.clientes", "mode": "merge",
        "options": { "host": "db", "database": "vendas", "user": "app",
                     "password_env": "PG_PASSWORD", "merge_keys": ["id"] } }
    """

    def write(self, df: DataFrame) -> None:
        vendor = canonical_vendor(self.config.format)
        options = dict(self.config.options)
        table = (self.config.path or "").strip()
        mode = (self.config.mode or "append").lower()

        if not table:
            raise ValueError(f"{vendor}: informe a tabela destino em 'path'.")
        if not _TABLE_PATTERN.match(table):
            raise ValueError(
                f"{vendor}: 'path' deve ser um nome de tabela como \"schema.tabela\" "
                f"(recebido: {table!r})."
            )
        if self.config.partition_by:
            logger.warning(
                "partition_by nao se aplica a destinos JDBC e sera ignorado",
                table=table,
                partition_by=self.config.partition_by,
            )

        resolved = spark_options(vendor, options)

        if mode == "merge":
            self._merge(df, vendor, table, options, resolved)
            return

        writer = df.write.format("jdbc").options(**resolved).option("dbtable", table)
        writer.mode(mode).save()
        logger.info("Escrita JDBC concluida", vendor=vendor, table=table, mode=mode)

    def _merge(
        self,
        df: DataFrame,
        vendor: str,
        table: str,
        options: Dict[str, Any],
        resolved: Dict[str, str],
    ) -> None:
        keys = options.get("merge_keys") or []
        if isinstance(keys, str):
            keys = [keys]
        if not keys:
            raise ValueError(
                f"{vendor}: mode 'merge' exige options.merge_keys "
                '(ex: { "merge_keys": ["id"] }).'
            )

        update_columns = options.get("merge_update_columns")
        if isinstance(update_columns, str):
            update_columns = [update_columns]

        staging = str(options.get("staging_table") or staging_name(table))
        statement = upsert_sql(
            vendor=vendor,
            target=table,
            staging=staging,
            columns=list(df.columns),
            keys=list(keys),
            update_columns=update_columns,
        )

        df.write.format("jdbc").options(**resolved).option("dbtable", staging).mode(
            "overwrite"
        ).save()

        try:
            self._execute(resolved, statement)
            logger.info(
                "Upsert JDBC concluido", vendor=vendor, table=table, merge_keys=keys
            )
        finally:
            self._execute(
                resolved,
                f"DROP TABLE {quote_identifier(vendor, staging)}",
                ignore_errors=True,
            )

    def _execute(
        self, resolved: Dict[str, str], statement: str, ignore_errors: bool = False
    ) -> None:
        """Roda um comando SQL na conexao JDBC, pela JVM da sessao Spark."""
        jvm = getattr(self.spark, "_jvm", None)
        if jvm is None:
            raise RuntimeError(
                "jdbc merge: a sessao Spark nao expoe a JVM (ex: Spark Connect), "
                "entao o upsert nao pode rodar. Use mode 'append' e faca o upsert "
                "no banco, ou execute o pipeline em uma sessao Spark classica."
            )

        properties = jvm.java.util.Properties()
        for key in ("user", "password"):
            if key in resolved:
                properties.setProperty(key, resolved[key])
        if "driver" in resolved:
            properties.setProperty("driver", resolved["driver"])
            jvm.java.lang.Class.forName(resolved["driver"])

        connection = jvm.java.sql.DriverManager.getConnection(resolved["url"], properties)
        try:
            cursor = connection.createStatement()
            try:
                cursor.execute(statement)
            finally:
                cursor.close()
        except Exception:
            if not ignore_errors:
                raise
        finally:
            connection.close()


def vendor_hint(vendor: str) -> Tuple[Optional[str], Optional[str]]:
    """Driver e coordenada Maven sugeridos para o banco — usado em mensagens de erro."""
    entry = VENDORS.get(canonical_vendor(vendor))
    if not entry:
        return None, None
    return entry["driver"], entry["package"]
