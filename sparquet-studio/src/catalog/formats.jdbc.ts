/**
 * Relational database connectors.
 *
 * Source of truth: spark_framework/io/jdbc.py. One reader/writer pair serves the
 * generic `jdbc` format and every vendor alias; the alias only decides the driver,
 * the default port, the URL shape and the upsert dialect. The entries below are
 * generated from that same table so the forms cannot drift per vendor.
 */

import type { FieldOption, FieldSpec, FormatDef } from '@/catalog/types'

interface JdbcVendor {
  id: string
  label: string
  /** Extra `format` strings the factory accepts for this connector. */
  aliases: string[]
  driver: string | null
  port: number | null
  /** Maven coordinate for `spark.jars.packages`. */
  pkg: string | null
  urlExample: string
  tableExample: string
  /** Statement the writer runs for `mode: merge`. */
  upsert: string
  databaseLabel: string
  databaseHelp: string
}

const VENDORS: JdbcVendor[] = [
  {
    id: 'postgres',
    label: 'PostgreSQL',
    aliases: ['postgresql'],
    driver: 'org.postgresql.Driver',
    port: 5432,
    pkg: 'org.postgresql:postgresql:42.7.4',
    urlExample: 'jdbc:postgresql://db.internal:5432/sales',
    tableExample: 'public.orders',
    upsert: 'INSERT … ON CONFLICT (keys) DO UPDATE',
    databaseLabel: 'Database',
    databaseHelp: 'Database name on the server, e.g. `sales`.',
  },
  {
    id: 'mysql',
    label: 'MySQL',
    aliases: [],
    driver: 'com.mysql.cj.jdbc.Driver',
    port: 3306,
    pkg: 'com.mysql:mysql-connector-j:9.1.0',
    urlExample: 'jdbc:mysql://db.internal:3306/shop',
    tableExample: 'shop.orders',
    upsert: 'INSERT … ON DUPLICATE KEY UPDATE',
    databaseLabel: 'Database',
    databaseHelp: 'Schema name, which MySQL treats as the database, e.g. `shop`.',
  },
  {
    id: 'sqlserver',
    label: 'SQL Server',
    aliases: ['mssql'],
    driver: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
    port: 1433,
    pkg: 'com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11',
    urlExample: 'jdbc:sqlserver://db.internal:1433;databaseName=dw',
    tableExample: 'dbo.orders',
    upsert: 'MERGE INTO … WHEN MATCHED / WHEN NOT MATCHED',
    databaseLabel: 'Database',
    databaseHelp: 'Written into the URL as `databaseName=<value>`.',
  },
  {
    id: 'oracle',
    label: 'Oracle',
    aliases: [],
    driver: 'oracle.jdbc.OracleDriver',
    port: 1521,
    pkg: 'com.oracle.database.jdbc:ojdbc11:23.6.0.24.10',
    urlExample: 'jdbc:oracle:thin:@//db.internal:1521/ORCLPDB1',
    tableExample: 'SALES.ORDERS',
    upsert: 'MERGE INTO … WHEN MATCHED / WHEN NOT MATCHED',
    databaseLabel: 'Service name',
    databaseHelp: 'Oracle service name, used as `@//host:port/<value>`.',
  },
  {
    id: 'jdbc',
    label: 'JDBC (any database)',
    aliases: [],
    driver: null,
    port: null,
    pkg: null,
    urlExample: 'jdbc:db2://db.internal:50000/warehouse',
    tableExample: 'schema.table',
    upsert: 'MERGE INTO … WHEN MATCHED / WHEN NOT MATCHED',
    databaseLabel: 'Database',
    databaseHelp: 'Only used when the URL is built from host and port; most drivers want the full URL instead.',
  },
]

const ICONS: Record<string, string> = {
  postgres: 'Database',
  mysql: 'Database',
  sqlserver: 'Database',
  oracle: 'Database',
  jdbc: 'DatabaseZap',
}

const text = (value: unknown): string => (typeof value === 'string' ? value.trim() : '')

const hasUrl = (options: Record<string, unknown>): boolean => text(options.url) !== ''

const isMergeMode = (options: Record<string, unknown>): boolean =>
  String(options.mode ?? '').toLowerCase() === 'merge'

/** Option predicates receive the options object; the inspector injects `mode` next to it. */
const visibleInMerge = (options: Record<string, unknown>): boolean =>
  !('mode' in options) || isMergeMode(options)

const BOOL_OPTIONS: FieldOption[] = [
  { value: 'true', label: 'true' },
  { value: 'false', label: 'false' },
]

const ISOLATION_OPTIONS: FieldOption[] = [
  { value: 'NONE', label: 'NONE' },
  { value: 'READ_UNCOMMITTED', label: 'READ_UNCOMMITTED' },
  { value: 'READ_COMMITTED', label: 'READ_COMMITTED', hint: 'Spark default' },
  { value: 'REPEATABLE_READ', label: 'REPEATABLE_READ' },
  { value: 'SERIALIZABLE', label: 'SERIALIZABLE' },
]

function connectionFields(vendor: JdbcVendor): FieldSpec[] {
  const generic = vendor.id === 'jdbc'

  return [
    {
      key: 'url',
      label: 'JDBC URL',
      type: 'text',
      required: generic,
      placeholder: vendor.urlExample,
      help: generic
        ? 'Full JDBC URL. Required for the generic connector.'
        : 'Full JDBC URL. Set this OR host + database — when both are present, the URL wins.',
      docs: `The connector builds the URL from host, port and ${vendor.databaseLabel.toLowerCase()} when this is empty. Anything the driver supports can be appended here (SSL, timeouts, failover hosts), which is why it takes precedence.`,
      validate: (value, options) =>
        text(value) === '' && text(options.host) === ''
          ? generic
            ? 'The generic JDBC connector needs a full URL.'
            : 'Set a JDBC URL, or a host and database.'
          : null,
    },
    {
      key: 'host',
      label: 'Host',
      type: 'text',
      placeholder: 'db.internal',
      help: 'Server hostname. Ignored when a URL is set.',
      visibleWhen: (options) => !hasUrl(options),
    },
    {
      key: 'port',
      label: 'Port',
      type: 'number',
      placeholder: vendor.port ? String(vendor.port) : '5432',
      help: vendor.port
        ? `Defaults to ${vendor.port} when omitted.`
        : 'Required when building the URL from a host.',
      visibleWhen: (options) => !hasUrl(options),
    },
    {
      key: 'database',
      label: vendor.databaseLabel,
      type: 'text',
      placeholder: vendor.id === 'oracle' ? 'ORCLPDB1' : 'sales',
      help: vendor.databaseHelp,
      visibleWhen: (options) => !hasUrl(options),
      validate: (value, options) =>
        !generic && !hasUrl(options) && text(options.host) !== '' && text(value) === ''
          ? `${vendor.label} needs a ${vendor.databaseLabel.toLowerCase()} when the URL is built from a host.`
          : null,
    },
    {
      key: 'user',
      label: 'User',
      type: 'text',
      placeholder: 'app',
      help: 'Database user. Prefer the environment-variable form below on shared repositories.',
    },
    {
      key: 'user_env',
      label: 'User from environment variable',
      type: 'text',
      placeholder: 'PG_USER',
      help: 'Name of the environment variable holding the user. Wins over the literal above.',
      group: 'advanced',
    },
    {
      key: 'password',
      label: 'Password',
      type: 'text',
      placeholder: '••••••',
      help: 'Stored in the pipeline JSON as plain text — use the environment-variable form for anything shared.',
      docs: 'The value is written into the config file exactly as typed, so a committed pipeline becomes a leaked credential. `password_env` keeps the secret in the execution environment instead; `{param}` also works, moving the secret to the caller.',
    },
    {
      key: 'password_env',
      label: 'Password from environment variable',
      type: 'text',
      placeholder: 'PG_PASSWORD',
      help: 'Name of the environment variable holding the password. Wins over the literal above.',
      docs: 'The run fails with a clear ValueError when the variable is not set in the executing environment, rather than attempting an anonymous connection.',
    },
    {
      key: 'driver',
      label: 'Driver class',
      type: 'text',
      required: generic,
      placeholder: vendor.driver ?? 'com.ibm.db2.jcc.DB2Driver',
      help: vendor.driver
        ? `Defaults to ${vendor.driver}. Override only for a forked or shaded driver.`
        : 'JDBC driver class name. Required for the generic connector.',
      group: generic ? 'main' : 'advanced',
      docs: vendor.pkg
        ? `The JAR must be on the cluster classpath. Outside Databricks the simplest route is \`"spark": { "configs": { "spark.jars.packages": "${vendor.pkg}" } }\` in this pipeline.`
        : 'The JAR must be on the cluster classpath, e.g. through `spark.jars.packages` in the pipeline `spark.configs`.',
    },
  ]
}

function readFields(vendor: JdbcVendor): FieldSpec[] {
  return [
    ...connectionFields(vendor),
    {
      key: 'query',
      label: 'Query',
      type: 'sql',
      rows: 4,
      placeholder: `SELECT id, total FROM ${vendor.tableExample} WHERE created_at >= '2026-01-01'`,
      help: 'SQL executed by the database instead of reading the table. When set, the node path is ignored.',
      docs: 'Runs as a subquery on the server, so filters, joins and aggregations happen before any data crosses the network. This is the manual form of predicate pushdown; use it whenever the table is much bigger than what the pipeline needs.',
      supportsRuntimeVars: true,
    },
    {
      key: 'partition_column',
      label: 'Partition column',
      type: 'text',
      placeholder: 'id',
      help: 'Numeric, date or timestamp column Spark slices to read in parallel.',
      docs: 'Without it the whole table is read through a single connection on one executor. With it, Spark issues `num_partitions` queries, each bounded by a range between the lower and upper bound — so the column should be indexed and reasonably uniform.',
      validate: (value, options) => {
        if (text(value) === '') return null
        const missing = ['lower_bound', 'upper_bound', 'num_partitions'].filter(
          (key) => options[key] === undefined || options[key] === '',
        )
        return missing.length > 0
          ? `Parallel reads also need ${missing.join(', ')}. The connector raises a ValueError otherwise.`
          : null
      },
    },
    {
      key: 'lower_bound',
      label: 'Lower bound',
      type: 'text',
      placeholder: '1',
      help: 'Start of the partitioning range. Rows below it are still read, in the first partition.',
      visibleWhen: (options) => text(options.partition_column) !== '',
    },
    {
      key: 'upper_bound',
      label: 'Upper bound',
      type: 'text',
      placeholder: '5000000',
      help: 'End of the partitioning range. Rows above it are still read, in the last partition.',
      visibleWhen: (options) => text(options.partition_column) !== '',
    },
    {
      key: 'num_partitions',
      label: 'Partitions',
      type: 'number',
      placeholder: '8',
      help: 'Concurrent connections opened against the database. Keep it under the connection pool limit.',
      visibleWhen: (options) => text(options.partition_column) !== '',
    },
    {
      key: 'fetch_size',
      label: 'Fetch size',
      type: 'number',
      placeholder: '10000',
      help: 'Rows the JDBC driver pulls per round trip. Raising it trades driver memory for fewer round trips.',
      group: 'advanced',
    },
    {
      key: 'session_init_statement',
      label: 'Session init statement',
      type: 'sql',
      placeholder: "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
      help: 'SQL executed once per connection before reading.',
      group: 'advanced',
    },
    {
      key: 'push_down_predicate',
      label: 'Push down predicates',
      type: 'select',
      options: BOOL_OPTIONS,
      help: 'Set to false to keep filters in Spark instead of sending them to the database.',
      group: 'advanced',
    },
  ]
}

function writeFields(vendor: JdbcVendor): FieldSpec[] {
  return [
    ...connectionFields(vendor),
    {
      key: 'merge_keys',
      label: 'Merge keys',
      type: 'string-list',
      placeholder: 'id',
      help: `Columns identifying a row. Required in merge mode, and the database needs a unique constraint on them.`,
      docs: `Merge mode writes a temporary staging table next to the target and then runs \`${vendor.upsert}\`. Without a unique constraint over these columns the database inserts duplicates instead of updating.`,
      visibleWhen: visibleInMerge,
      validate: (value, options) =>
        isMergeMode(options) && (!Array.isArray(value) || value.length === 0)
          ? 'Merge mode requires at least one merge key.'
          : null,
    },
    {
      key: 'merge_update_columns',
      label: 'Columns to update',
      type: 'string-list',
      placeholder: 'total',
      help: 'Restricts what the UPDATE branch touches. Defaults to every non-key column.',
      docs: 'Useful to protect columns the destination owns — an audit timestamp, a manually curated flag — from being overwritten by every load.',
      visibleWhen: visibleInMerge,
      group: 'advanced',
    },
    {
      key: 'staging_table',
      label: 'Staging table',
      type: 'text',
      placeholder: `${vendor.tableExample}_stg`,
      help: 'Overrides the generated staging table name used by merge mode.',
      docs: 'By default the connector creates `<table>_sparquet_stg_<hash>` in the target schema and drops it afterwards. Set this when the writing user may only create tables in a specific schema.',
      visibleWhen: visibleInMerge,
      group: 'advanced',
    },
    {
      key: 'batch_size',
      label: 'Batch size',
      type: 'number',
      placeholder: '5000',
      help: 'Rows per JDBC batch insert. Spark defaults to 1000.',
    },
    {
      key: 'truncate_table',
      label: 'Truncate on overwrite',
      type: 'select',
      options: BOOL_OPTIONS,
      help: 'Overwrite mode only: TRUNCATE the table instead of dropping and recreating it.',
      docs: 'Dropping the table also drops its indexes, constraints and grants, and lets Spark pick the column types. Truncating keeps the schema the DBA created — usually what you want against a real database.',
      group: 'advanced',
    },
    {
      key: 'isolation_level',
      label: 'Isolation level',
      type: 'select',
      options: ISOLATION_OPTIONS,
      help: 'Transaction isolation used by the write.',
      group: 'advanced',
    },
    {
      key: 'create_table_options',
      label: 'Create table options',
      type: 'text',
      placeholder: 'ENGINE=InnoDB',
      help: 'Appended to the CREATE TABLE statement when Spark creates the table.',
      group: 'advanced',
    },
    {
      key: 'create_table_column_types',
      label: 'Create table column types',
      type: 'text',
      placeholder: 'name VARCHAR(120), total DECIMAL(18,2)',
      help: 'Overrides the column types Spark would infer when creating the table.',
      group: 'advanced',
    },
  ]
}

function jdbcFormat(vendor: JdbcVendor): FormatDef {
  const aliasNote =
    vendor.aliases.length > 0
      ? ` The format string \`${vendor.aliases.join('`, `')}\` is accepted as an alias.`
      : ''

  const packageNote = vendor.pkg
    ? `\n\nThe driver JAR must be on the classpath: \`"spark": { "configs": { "spark.jars.packages": "${vendor.pkg}" } }\` is the simplest route outside Databricks.`
    : '\n\nSet both the URL and the driver class, and put the driver JAR on the classpath through `spark.jars.packages`.'

  return {
    id: vendor.id,
    label: vendor.label,
    icon: ICONS[vendor.id] ?? 'Database',
    canRead: true,
    canWrite: true,
    summary:
      vendor.id === 'jdbc'
        ? 'Any database with a JDBC driver, as a source or a destination.'
        : `Read from and write to ${vendor.label} over JDBC.`,
    description: `Reads a table or a SQL query from ${vendor.label} and writes DataFrames back to it, over the Spark JDBC connector.${aliasNote}\n\nCredentials belong in \`user_env\` / \`password_env\` (environment variable names) rather than in the pipeline file. Reads run through a single connection unless a partition column is configured.${packageNote}`,
    pathLabel: 'Table',
    pathPlaceholder: vendor.tableExample,
    pathHelp:
      'Schema-qualified table name, not a filesystem path. On reads it is ignored when a query is set; on writes it is the destination table.',
    modes: ['append', 'overwrite', 'merge', 'ignore', 'error'],
    supportsPartitioning: false,
    supportsMerge: true,
    readOptions: readFields(vendor),
    writeOptions: writeFields(vendor),
    gotchas: [
      'Put the driver JAR on the classpath — a missing driver fails at connection time, not at parse time.',
      'Keep secrets out of the file: user_env and password_env read environment variables at run time.',
      'A read without a partition column uses one connection and one executor, however large the table is.',
      'partition_column only works alongside lower_bound, upper_bound and num_partitions.',
      'Merge mode needs a unique constraint on the merge keys, and a classic Spark session — it uses the JVM, so Spark Connect is out.',
      'Overwrite drops and recreates the table by default; set truncate_table to keep indexes, constraints and grants.',
      'partition_by has no meaning for a database destination and is ignored with a warning.',
    ],
    examples: [
      {
        title: `Read a table from ${vendor.label} in parallel`,
        json: `{
  "format": "${vendor.id}",
  "path": "${vendor.tableExample}",
  "options": {
${
  vendor.id === 'jdbc'
    ? `    "url": "${vendor.urlExample}",
    "driver": "com.ibm.db2.jcc.DB2Driver",`
    : `    "host": "db.internal",
    "database": "${vendor.id === 'oracle' ? 'ORCLPDB1' : 'sales'}",`
}
    "user": "app",
    "password_env": "DB_PASSWORD",
    "partition_column": "id",
    "lower_bound": 1,
    "upper_bound": 5000000,
    "num_partitions": 8
  }
}`,
      },
      {
        title: 'Upsert an aggregate back into the database',
        json: `{
  "format": "${vendor.id}",
  "path": "${vendor.tableExample}",
  "mode": "merge",
  "options": {
${
  vendor.id === 'jdbc'
    ? `    "url": "${vendor.urlExample}",
    "driver": "com.ibm.db2.jcc.DB2Driver",`
    : `    "host": "db.internal",
    "database": "${vendor.id === 'oracle' ? 'ORCLPDB1' : 'sales'}",`
}
    "user": "app",
    "password_env": "DB_PASSWORD",
    "merge_keys": ["id"],
    "batch_size": 5000
  }
}`,
      },
    ],
  }
}

export const JDBC_FORMATS: FormatDef[] = VENDORS.map(jdbcFormat)

/** Format ids the framework routes to the JDBC connector, aliases included. */
export const JDBC_FORMAT_IDS: string[] = VENDORS.flatMap((vendor) => [
  vendor.id,
  ...vendor.aliases,
])
