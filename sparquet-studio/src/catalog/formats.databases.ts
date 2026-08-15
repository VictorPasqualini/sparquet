/**
 * IO format catalog — database, warehouse and NoSQL/search connectors.
 *
 * Source of truth: sparquet/io/factory.py (registries) plus each io/*.py module
 * (jdbc, bigquery, snowflake, redshift, mongodb, dynamodb, cassandra,
 * elasticsearch). Every one of these connectors needs its provider JAR on the
 * Spark classpath at runtime — the framework only builds the `.format(...).options(...)`
 * call; it never bundles the driver.
 */

import type { FieldOption, FieldSpec, FormatDef } from '@/catalog/types'

const BOOL_OPTIONS: FieldOption[] = [
  { value: 'true', label: 'true' },
  { value: 'false', label: 'false' },
]

const PLAINTEXT_SECRET =
  'Stored in plaintext inside the pipeline JSON — prefer a value injected at runtime (params) over a hard-coded secret.'

/* --------------------------------------------------------------------- JDBC */

const jdbcConnectionFields = (driver: string, port: string): FieldSpec[] => [
  {
    key: 'url',
    label: 'JDBC URL',
    type: 'text',
    placeholder: `jdbc:...:${port}/db`,
    help: 'Full JDBC URL. Takes precedence over host/port/database when set.',
    supportsRuntimeVars: true,
  },
  {
    key: 'host',
    label: 'Host',
    type: 'text',
    placeholder: 'db.internal',
    help: `Used to build the URL when 'url' is omitted (port defaults to ${port}).`,
  },
  { key: 'port', label: 'Port', type: 'text', placeholder: port },
  { key: 'database', label: 'Database', type: 'text', placeholder: 'app' },
  { key: 'user', label: 'User', type: 'text', placeholder: 'sparquet' },
  { key: 'password', label: 'Password', type: 'text', docs: PLAINTEXT_SECRET },
  {
    key: 'driver',
    label: 'Driver class',
    type: 'text',
    placeholder: driver,
    help: `Default: ${driver}. The matching driver JAR must be on the classpath.`,
    group: 'advanced',
  },
]

const jdbcReadOptions = (driver: string, port: string): FieldSpec[] => [
  ...jdbcConnectionFields(driver, port),
  {
    key: 'query',
    label: 'Query',
    type: 'sql',
    rows: 3,
    placeholder: 'SELECT id, nome FROM clientes WHERE ativo = true',
    help: 'Reads the result of this SQL instead of the table in the path. Mutually exclusive with a table.',
    group: 'advanced',
  },
  {
    key: 'partitionColumn',
    label: 'Partition column',
    type: 'text',
    help: 'Numeric/date/timestamp column for parallel reads. Requires lowerBound, upperBound and numPartitions.',
    group: 'advanced',
  },
  { key: 'numPartitions', label: 'Num partitions', type: 'number', placeholder: '8', group: 'advanced' },
  { key: 'lowerBound', label: 'Lower bound', type: 'text', group: 'advanced' },
  { key: 'upperBound', label: 'Upper bound', type: 'text', group: 'advanced' },
  { key: 'fetchsize', label: 'Fetch size', type: 'number', placeholder: '1000', group: 'advanced' },
]

const jdbcWriteOptions = (driver: string, port: string): FieldSpec[] => [
  ...jdbcConnectionFields(driver, port),
  {
    key: 'truncate',
    label: 'Truncate on overwrite',
    type: 'select',
    options: BOOL_OPTIONS,
    help: 'true keeps the table and TRUNCATEs it on overwrite instead of DROP + CREATE (preserves schema/grants).',
    group: 'advanced',
  },
  { key: 'batchsize', label: 'Batch size', type: 'number', placeholder: '1000', group: 'advanced' },
  {
    key: 'isolationLevel',
    label: 'Isolation level',
    type: 'select',
    options: [
      { value: 'READ_UNCOMMITTED', label: 'READ_UNCOMMITTED' },
      { value: 'READ_COMMITTED', label: 'READ_COMMITTED' },
      { value: 'REPEATABLE_READ', label: 'REPEATABLE_READ' },
      { value: 'SERIALIZABLE', label: 'SERIALIZABLE' },
      { value: 'NONE', label: 'NONE' },
    ],
    group: 'advanced',
  },
  {
    key: 'createTableColumnTypes',
    label: 'Create-table column types',
    type: 'text',
    placeholder: 'id BIGINT, nome VARCHAR(255)',
    help: 'DDL column types used when the table is created on overwrite.',
    group: 'advanced',
  },
]

const jdbcFormat = (opts: {
  id: string
  label: string
  driver: string
  port: string
  urlExample: string
}): FormatDef => ({
  id: opts.id,
  label: opts.label,
  icon: 'Database',
  canRead: true,
  canWrite: true,
  summary: `${opts.label} over Spark JDBC — table by name, with parallel reads and batched writes.`,
  description:
    `Reads and writes ${opts.label} through Spark's JDBC data source. The path is the table name (dbtable); connection details go in options.\n\n` +
    'Give a full `url`, or `host` + `database` to have the URL built for you. The driver JAR must be on the Spark classpath.',
  pathLabel: 'Table',
  pathPlaceholder: 'schema.tabela',
  pathHelp:
    'Table name written to the JDBC `dbtable` option. On read, a `query` option replaces it with an arbitrary SELECT.',
  modes: ['overwrite', 'append'],
  supportsPartitioning: false,
  supportsMerge: false,
  readOptions: jdbcReadOptions(opts.driver, opts.port),
  writeOptions: jdbcWriteOptions(opts.driver, opts.port),
  gotchas: [
    `Requires the ${opts.label} JDBC driver on the classpath (spark.jars / spark.jars.packages); otherwise the run fails with "No suitable driver".`,
    'Provide `url`, or `host` + `database` to build it — without either, the read/write raises a ValueError before Spark is touched.',
    'mode "merge" is NOT supported by Spark JDBC — only overwrite/append. overwrite DROPs and recreates the table unless `truncate` is "true".',
    'A large write is row-by-row INSERT batches on the driver connection — tune `batchsize`, and use partitionColumn/numPartitions to parallelize reads.',
    `Credentials in user/password are plaintext in the JSON — ${PLAINTEXT_SECRET.toLowerCase()}`,
  ],
  examples: [
    {
      title: `Read a ${opts.label} table with parallel partitions`,
      json: `{
  "format": "${opts.id}",
  "path": "public.clientes",
  "options": {
    "url": "${opts.urlExample}",
    "user": "sparquet",
    "password": "{db_password}",
    "partitionColumn": "id",
    "lowerBound": "1",
    "upperBound": "1000000",
    "numPartitions": "8"
  }
}`,
    },
    {
      title: `Append into a ${opts.label} table`,
      json: `{
  "format": "${opts.id}",
  "path": "public.saida",
  "mode": "append",
  "options": {
    "host": "db.internal",
    "database": "app",
    "user": "sparquet",
    "password": "{db_password}",
    "batchsize": "5000"
  }
}`,
    },
  ],
})

/* ----------------------------------------------------------- warehouses etc */

const bigquery: FormatDef = {
  id: 'bigquery',
  label: 'BigQuery',
  icon: 'Cloud',
  canRead: true,
  canWrite: true,
  summary: 'Google BigQuery tables via the spark-bigquery-connector.',
  description:
    'Reads and writes BigQuery tables. The path is `project.dataset.table` (or `dataset.table` with a default project). Writes stage through a GCS bucket (indirect) or the Storage Write API (direct).',
  pathLabel: 'Table',
  pathPlaceholder: 'projeto.dataset.tabela',
  pathHelp: 'Fully-qualified `project.dataset.table`. On read a `query` option can replace it.',
  modes: ['overwrite', 'append'],
  supportsPartitioning: false,
  supportsMerge: false,
  readOptions: [
    { key: 'parentProject', label: 'Billing project', type: 'text', placeholder: 'meu-projeto', help: 'Project charged for the read when it differs from the table project.' },
    { key: 'credentialsFile', label: 'Credentials file', type: 'text', placeholder: '/secrets/sa.json', help: 'Service-account key path. Or use `credentials` (base64 JSON).' },
    { key: 'query', label: 'Query', type: 'sql', rows: 3, placeholder: 'SELECT ...', help: 'Requires viewsEnabled=true and a materialization dataset.', group: 'advanced' },
    { key: 'viewsEnabled', label: 'Views enabled', type: 'select', options: BOOL_OPTIONS, help: 'Needed to read views or a `query`.', group: 'advanced' },
    { key: 'filter', label: 'Filter', type: 'sql', rows: 2, placeholder: "dt = '2025-01-01'", help: 'Predicate pushed down to BigQuery.', group: 'advanced' },
    { key: 'maxParallelism', label: 'Max parallelism', type: 'number', group: 'advanced' },
  ],
  writeOptions: [
    { key: 'temporaryGcsBucket', label: 'Temporary GCS bucket', type: 'text', placeholder: 'meu-bucket-staging', help: 'Staging bucket for the default (indirect) write method.' },
    { key: 'writeMethod', label: 'Write method', type: 'select', options: [{ value: 'indirect', label: 'indirect (via GCS)' }, { value: 'direct', label: 'direct (Storage Write API)' }], help: 'direct skips the GCS staging bucket.', group: 'advanced' },
    { key: 'credentialsFile', label: 'Credentials file', type: 'text', placeholder: '/secrets/sa.json' },
    { key: 'partitionField', label: 'Partition field', type: 'text', help: 'Native BigQuery time/range partitioning column.', group: 'advanced' },
    { key: 'clusteredFields', label: 'Clustered fields', type: 'text', placeholder: 'regiao,produto', group: 'advanced' },
  ],
  gotchas: [
    'Requires the spark-bigquery-connector JAR on the classpath.',
    'The default (indirect) write needs `temporaryGcsBucket`; without it use writeMethod "direct".',
    'Authentication falls back to Application Default Credentials when no credentials option is set.',
    'Partitioning is native (partitionField/clusteredFields), NOT Spark partition_by.',
  ],
  examples: [
    {
      title: 'Read a table filtered',
      json: `{
  "format": "bigquery",
  "path": "meu-projeto.vendas.pedidos",
  "options": { "parentProject": "meu-projeto", "filter": "dt_ref = '2025-01-01'" }
}`,
    },
    {
      title: 'Write via a staging bucket',
      json: `{
  "format": "bigquery",
  "path": "meu-projeto.dw.pedidos",
  "mode": "append",
  "options": { "temporaryGcsBucket": "meu-bucket-staging" }
}`,
    },
  ],
}

const snowflake: FormatDef = {
  id: 'snowflake',
  label: 'Snowflake',
  icon: 'Snowflake',
  canRead: true,
  canWrite: true,
  summary: 'Snowflake tables via the spark-snowflake connector.',
  description:
    'Reads and writes Snowflake. The path is the table (dbtable); the sfXxx options carry the connection. Reads accept a `query` in place of the table.',
  pathLabel: 'Table',
  pathPlaceholder: 'ANALYTICS.PUBLIC.PEDIDOS',
  pathHelp: 'Table written to the connector `dbtable` option. A `query` option replaces it on read.',
  modes: ['overwrite', 'append'],
  supportsPartitioning: false,
  supportsMerge: false,
  readOptions: [
    { key: 'sfUrl', label: 'Account URL', type: 'text', placeholder: 'org-conta.snowflakecomputing.com' },
    { key: 'sfUser', label: 'User', type: 'text' },
    { key: 'sfPassword', label: 'Password', type: 'text', docs: PLAINTEXT_SECRET },
    { key: 'sfDatabase', label: 'Database', type: 'text' },
    { key: 'sfSchema', label: 'Schema', type: 'text', placeholder: 'PUBLIC' },
    { key: 'sfWarehouse', label: 'Warehouse', type: 'text' },
    { key: 'sfRole', label: 'Role', type: 'text', group: 'advanced' },
    { key: 'query', label: 'Query', type: 'sql', rows: 3, placeholder: 'SELECT ...', group: 'advanced' },
  ],
  writeOptions: [
    { key: 'sfUrl', label: 'Account URL', type: 'text', placeholder: 'org-conta.snowflakecomputing.com' },
    { key: 'sfUser', label: 'User', type: 'text' },
    { key: 'sfPassword', label: 'Password', type: 'text', docs: PLAINTEXT_SECRET },
    { key: 'sfDatabase', label: 'Database', type: 'text' },
    { key: 'sfSchema', label: 'Schema', type: 'text', placeholder: 'PUBLIC' },
    { key: 'sfWarehouse', label: 'Warehouse', type: 'text' },
    { key: 'sfRole', label: 'Role', type: 'text', group: 'advanced' },
    { key: 'truncate_table', label: 'Truncate table', type: 'select', options: BOOL_OPTIONS, help: 'ON keeps the table schema on overwrite.', group: 'advanced' },
  ],
  gotchas: [
    'Requires the spark-snowflake connector AND the snowflake-jdbc JAR on the classpath.',
    'Key-pair auth uses `pem_private_key` instead of sfPassword.',
    'mode merge is not supported — overwrite/append only.',
  ],
  examples: [
    {
      title: 'Read a table',
      json: `{
  "format": "snowflake",
  "path": "ANALYTICS.PUBLIC.PEDIDOS",
  "options": {
    "sfUrl": "org-conta.snowflakecomputing.com",
    "sfUser": "SPARQUET", "sfPassword": "{sf_password}",
    "sfDatabase": "ANALYTICS", "sfSchema": "PUBLIC", "sfWarehouse": "WH_ETL"
  }
}`,
    },
  ],
}

const redshift: FormatDef = {
  id: 'redshift',
  label: 'Amazon Redshift',
  icon: 'Warehouse',
  canRead: true,
  canWrite: true,
  summary: 'Amazon Redshift via spark-redshift — UNLOAD/COPY staged through S3.',
  description:
    'Reads and writes Redshift using the community spark-redshift connector, which stages data in an S3 bucket (`tempdir`) and drives UNLOAD (read) / COPY (write). The path is the table (dbtable).',
  pathLabel: 'Table',
  pathPlaceholder: 'public.vendas',
  pathHelp: 'Table written to the connector `dbtable` option. A `query` option replaces it on read.',
  modes: ['overwrite', 'append'],
  supportsPartitioning: false,
  supportsMerge: false,
  readOptions: [
    { key: 'url', label: 'JDBC URL', type: 'text', placeholder: 'jdbc:redshift://host:5439/db' },
    { key: 'tempdir', label: 'S3 tempdir', type: 'text', placeholder: 's3://bucket/staging', help: 'S3 prefix used to stage UNLOAD/COPY files. Required.' },
    { key: 'user', label: 'User', type: 'text' },
    { key: 'password', label: 'Password', type: 'text', docs: PLAINTEXT_SECRET },
    { key: 'aws_iam_role', label: 'IAM role ARN', type: 'text', placeholder: 'arn:aws:iam::123:role/redshift', help: 'Alternative to S3 keys for UNLOAD/COPY.', group: 'advanced' },
    { key: 'forward_spark_s3_credentials', label: 'Forward Spark S3 creds', type: 'select', options: BOOL_OPTIONS, group: 'advanced' },
    { key: 'query', label: 'Query', type: 'sql', rows: 3, placeholder: 'SELECT ...', group: 'advanced' },
  ],
  writeOptions: [
    { key: 'url', label: 'JDBC URL', type: 'text', placeholder: 'jdbc:redshift://host:5439/db' },
    { key: 'tempdir', label: 'S3 tempdir', type: 'text', placeholder: 's3://bucket/staging', help: 'S3 prefix used to stage the COPY. Required.' },
    { key: 'user', label: 'User', type: 'text' },
    { key: 'password', label: 'Password', type: 'text', docs: PLAINTEXT_SECRET },
    { key: 'aws_iam_role', label: 'IAM role ARN', type: 'text', group: 'advanced' },
    { key: 'diststyle', label: 'Dist style', type: 'select', options: [{ value: 'EVEN', label: 'EVEN' }, { value: 'KEY', label: 'KEY' }, { value: 'ALL', label: 'ALL' }], group: 'advanced' },
    { key: 'distkey', label: 'Dist key', type: 'text', group: 'advanced' },
  ],
  gotchas: [
    'Requires the spark-redshift connector, the Redshift JDBC driver AND S3 access — reads/writes always round-trip through `tempdir`.',
    '`url` and `tempdir` (S3) are both mandatory.',
    'S3 credentials come from aws_iam_role or forward_spark_s3_credentials=true — one of them must resolve.',
    'mode merge is not supported.',
  ],
  examples: [
    {
      title: 'Write staged via S3',
      json: `{
  "format": "redshift",
  "path": "public.vendas",
  "mode": "append",
  "options": {
    "url": "jdbc:redshift://cluster:5439/dw",
    "tempdir": "s3://meu-bucket/redshift-staging",
    "aws_iam_role": "arn:aws:iam::123456789:role/redshift-copy"
  }
}`,
    },
  ],
}

/* ---------------------------------------------------------- NoSQL / search */

const mongoReadOptions: FieldSpec[] = [
  { key: 'connection.uri', label: 'Connection URI', type: 'text', placeholder: 'mongodb://user:pass@host:27017', docs: PLAINTEXT_SECRET, supportsRuntimeVars: true },
  { key: 'database', label: 'Database', type: 'text', placeholder: 'app' },
  { key: 'collection', label: 'Collection', type: 'text', help: 'Overrides the path.', group: 'advanced' },
  { key: 'aggregation.pipeline', label: 'Aggregation pipeline', type: 'json', rows: 4, placeholder: '[ { "$match": { "ativo": true } } ]', group: 'advanced' },
]

const mongoWriteOptions: FieldSpec[] = [
  { key: 'connection.uri', label: 'Connection URI', type: 'text', placeholder: 'mongodb://user:pass@host:27017', docs: PLAINTEXT_SECRET, supportsRuntimeVars: true },
  { key: 'database', label: 'Database', type: 'text', placeholder: 'app' },
  { key: 'collection', label: 'Collection', type: 'text', help: 'Overrides the path.', group: 'advanced' },
  { key: 'operationType', label: 'Operation', type: 'select', options: [{ value: 'insert', label: 'insert' }, { value: 'replace', label: 'replace' }, { value: 'update', label: 'update' }], group: 'advanced' },
  { key: 'idFieldList', label: 'Id fields', type: 'text', placeholder: '_id', help: 'Key fields for replace/update.', group: 'advanced' },
]

const mongodb: FormatDef = {
  id: 'mongodb',
  label: 'MongoDB',
  icon: 'Leaf',
  canRead: true,
  canWrite: true,
  summary: 'MongoDB collections via the MongoDB Spark Connector v10.',
  description:
    'Reads and writes MongoDB collections. The path is the collection; `connection.uri` and `database` go in options. Pushes an optional aggregation pipeline on read.',
  pathLabel: 'Collection',
  pathPlaceholder: 'clientes',
  pathHelp: 'Collection name (set as the `collection` option). The database is a separate option.',
  modes: ['overwrite', 'append'],
  supportsPartitioning: false,
  supportsMerge: false,
  readOptions: mongoReadOptions,
  writeOptions: mongoWriteOptions,
  gotchas: [
    'Requires the mongo-spark-connector JAR (v10+, format "mongodb") on the classpath.',
    '`connection.uri` and `database` are mandatory; the path only names the collection.',
    'overwrite drops and recreates the collection — append keeps existing documents.',
    'replace/update need idFieldList, otherwise every write is an insert.',
  ],
  examples: [
    {
      title: 'Read a collection',
      json: `{
  "format": "mongodb",
  "path": "clientes",
  "options": { "connection.uri": "mongodb://host:27017", "database": "app" }
}`,
    },
    {
      title: 'Upsert by _id',
      json: `{
  "format": "mongodb",
  "path": "clientes",
  "mode": "append",
  "options": {
    "connection.uri": "mongodb://host:27017", "database": "app",
    "operationType": "update", "idFieldList": "_id"
  }
}`,
    },
  ],
}

const documentdb: FormatDef = {
  id: 'documentdb',
  label: 'DocumentDB',
  icon: 'FileJson',
  canRead: true,
  canWrite: true,
  summary: 'Amazon DocumentDB through the MongoDB Spark Connector (wire-compatible).',
  description:
    'Amazon DocumentDB speaks the MongoDB protocol, so it uses the same connector. The only difference is the connection URI: point it at the DocumentDB cluster with TLS and retryWrites disabled.',
  pathLabel: 'Collection',
  pathPlaceholder: 'pedidos',
  pathHelp: 'Collection name (set as the `collection` option).',
  modes: ['overwrite', 'append'],
  supportsPartitioning: false,
  supportsMerge: false,
  readOptions: mongoReadOptions,
  writeOptions: mongoWriteOptions,
  gotchas: [
    'Uses the mongo-spark-connector — same JAR and options as MongoDB.',
    'The DocumentDB URI needs ?tls=true&retryWrites=false (retryable writes are unsupported).',
    'TLS to DocumentDB requires the Amazon RDS CA bundle in the JVM truststore.',
    'The cluster is only reachable from inside its VPC — run the job with network access to it.',
  ],
  examples: [
    {
      title: 'Read from a DocumentDB cluster',
      json: `{
  "format": "documentdb",
  "path": "pedidos",
  "options": {
    "connection.uri": "mongodb://user:{docdb_pass}@docdb.cluster.region.docdb.amazonaws.com:27017/?tls=true&retryWrites=false",
    "database": "app"
  }
}`,
    },
  ],
}

const dynamodb: FormatDef = {
  id: 'dynamodb',
  label: 'DynamoDB',
  icon: 'Boxes',
  canRead: true,
  canWrite: true,
  summary: 'Amazon DynamoDB tables via the spark-dynamodb connector.',
  description:
    'Reads a DynamoDB table into a DataFrame (parallel scan) and writes rows back as PutItem batches. The path is the table name; the write is always an upsert by primary key.',
  pathLabel: 'Table',
  pathPlaceholder: 'Orders',
  pathHelp: 'DynamoDB table name (set as the `tableName` option).',
  modes: ['append'],
  supportsPartitioning: false,
  supportsMerge: false,
  readOptions: [
    { key: 'region', label: 'Region', type: 'text', placeholder: 'us-east-1' },
    { key: 'roleArn', label: 'Role ARN', type: 'text', placeholder: 'arn:aws:iam::123:role/reader', group: 'advanced' },
    { key: 'endpoint', label: 'Endpoint', type: 'text', placeholder: 'http://localhost:8000', help: 'Override for DynamoDB local / VPC endpoints.', group: 'advanced' },
    { key: 'stronglyConsistentReads', label: 'Strongly consistent reads', type: 'select', options: BOOL_OPTIONS, group: 'advanced' },
    { key: 'throughput', label: 'Throughput (RCU)', type: 'number', help: 'Caps read capacity consumed by the scan.', group: 'advanced' },
  ],
  writeOptions: [
    { key: 'region', label: 'Region', type: 'text', placeholder: 'us-east-1' },
    { key: 'roleArn', label: 'Role ARN', type: 'text', group: 'advanced' },
    { key: 'endpoint', label: 'Endpoint', type: 'text', group: 'advanced' },
    { key: 'writeBatchSize', label: 'Write batch size', type: 'number', placeholder: '25', help: 'Items per BatchWriteItem (max 25).', group: 'advanced' },
    { key: 'throughput', label: 'Throughput (WCU)', type: 'number', group: 'advanced' },
  ],
  gotchas: [
    'Requires the spark-dynamodb connector JAR on the classpath.',
    'Write is PutItem per row (upsert by primary key) — there is no table "overwrite"; use mode append.',
    'The DataFrame columns must match the table key schema, and every row needs the partition (and sort) key.',
    'A full scan consumes read capacity — cap it with throughput to avoid throttling production tables.',
  ],
  examples: [
    {
      title: 'Scan a table',
      json: `{
  "format": "dynamodb",
  "path": "Orders",
  "options": { "region": "us-east-1", "stronglyConsistentReads": "false" }
}`,
    },
  ],
}

const cassandra: FormatDef = {
  id: 'cassandra',
  label: 'Cassandra',
  icon: 'Columns3',
  canRead: true,
  canWrite: true,
  summary: 'Apache Cassandra / ScyllaDB via the spark-cassandra-connector.',
  description:
    'Reads and writes Cassandra (and ScyllaDB) tables. The path is `keyspace.table`; the cluster contact and credentials go in options. Partition-key filters are pushed down.',
  pathLabel: 'Keyspace.table',
  pathPlaceholder: 'loja.pedidos',
  pathHelp: 'Written as the connector `keyspace`/`table` options. A bare name uses the `keyspace` option instead.',
  modes: ['append'],
  supportsPartitioning: false,
  supportsMerge: false,
  readOptions: [
    { key: 'spark.cassandra.connection.host', label: 'Contact points', type: 'text', placeholder: 'node1,node2' },
    { key: 'spark.cassandra.connection.port', label: 'Port', type: 'text', placeholder: '9042', group: 'advanced' },
    { key: 'spark.cassandra.auth.username', label: 'Username', type: 'text', group: 'advanced' },
    { key: 'spark.cassandra.auth.password', label: 'Password', type: 'text', docs: PLAINTEXT_SECRET, group: 'advanced' },
    { key: 'keyspace', label: 'Keyspace', type: 'text', help: 'Overrides the keyspace parsed from the path.', group: 'advanced' },
  ],
  writeOptions: [
    { key: 'spark.cassandra.connection.host', label: 'Contact points', type: 'text', placeholder: 'node1,node2' },
    { key: 'spark.cassandra.connection.port', label: 'Port', type: 'text', placeholder: '9042', group: 'advanced' },
    { key: 'spark.cassandra.auth.username', label: 'Username', type: 'text', group: 'advanced' },
    { key: 'spark.cassandra.auth.password', label: 'Password', type: 'text', docs: PLAINTEXT_SECRET, group: 'advanced' },
    { key: 'spark.cassandra.output.consistency.level', label: 'Consistency level', type: 'select', options: [{ value: 'ONE', label: 'ONE' }, { value: 'QUORUM', label: 'QUORUM' }, { value: 'LOCAL_QUORUM', label: 'LOCAL_QUORUM' }, { value: 'ALL', label: 'ALL' }], group: 'advanced' },
  ],
  gotchas: [
    'Requires the spark-cassandra-connector JAR matching your Spark version.',
    'The target table must already exist — the connector writes rows, it does not create schema.',
    'Write is an upsert by primary key (Cassandra has no INSERT-vs-UPDATE distinction); mode append.',
    'Only partition-key predicates are pushed down; other filters scan and filter in Spark.',
  ],
  examples: [
    {
      title: 'Read a table',
      json: `{
  "format": "cassandra",
  "path": "loja.pedidos",
  "options": { "spark.cassandra.connection.host": "node1,node2" }
}`,
    },
  ],
}

const elasticsearch: FormatDef = {
  id: 'elasticsearch',
  label: 'Elasticsearch',
  icon: 'Search',
  canRead: true,
  canWrite: true,
  summary: 'Elasticsearch / OpenSearch indices via elasticsearch-hadoop.',
  description:
    'Reads and writes Elasticsearch (and OpenSearch) indices. The path is the index/resource; cluster contact and auth go in options. A query DSL can be pushed on read, and a column can become the document _id on write.',
  pathLabel: 'Index',
  pathPlaceholder: 'clientes',
  pathHelp: 'Index/resource passed to load()/save() (e.g. "clientes" or "clientes/_doc").',
  modes: ['overwrite', 'append'],
  supportsPartitioning: false,
  supportsMerge: false,
  readOptions: [
    { key: 'es.nodes', label: 'Nodes', type: 'text', placeholder: 'es.internal' },
    { key: 'es.port', label: 'Port', type: 'text', placeholder: '9200' },
    { key: 'es.net.http.auth.user', label: 'User', type: 'text', group: 'advanced' },
    { key: 'es.net.http.auth.pass', label: 'Password', type: 'text', docs: PLAINTEXT_SECRET, group: 'advanced' },
    { key: 'es.nodes.wan.only', label: 'WAN only', type: 'select', options: BOOL_OPTIONS, help: 'true for managed/cloud clusters behind a proxy.', group: 'advanced' },
    { key: 'es.query', label: 'Query DSL', type: 'json', rows: 4, placeholder: '{ "query": { "match_all": {} } }', group: 'advanced' },
  ],
  writeOptions: [
    { key: 'es.nodes', label: 'Nodes', type: 'text', placeholder: 'es.internal' },
    { key: 'es.port', label: 'Port', type: 'text', placeholder: '9200' },
    { key: 'es.net.http.auth.user', label: 'User', type: 'text', group: 'advanced' },
    { key: 'es.net.http.auth.pass', label: 'Password', type: 'text', docs: PLAINTEXT_SECRET, group: 'advanced' },
    { key: 'es.nodes.wan.only', label: 'WAN only', type: 'select', options: BOOL_OPTIONS, group: 'advanced' },
    { key: 'es.mapping.id', label: 'Id column', type: 'text', help: 'DataFrame column used as the document _id.', group: 'advanced' },
    { key: 'es.write.operation', label: 'Write operation', type: 'select', options: [{ value: 'index', label: 'index' }, { value: 'create', label: 'create' }, { value: 'update', label: 'update' }, { value: 'upsert', label: 'upsert' }], group: 'advanced' },
  ],
  gotchas: [
    'Requires the elasticsearch-spark (es-hadoop) JAR matching your Spark/Scala version.',
    'update/upsert need es.mapping.id — without an id column every write creates a new document.',
    'Managed/cloud clusters usually need es.nodes.wan.only=true.',
    'OpenSearch works through the same connector; very new ES/OS majors may need a compatibility header.',
  ],
  examples: [
    {
      title: 'Index a DataFrame keyed by id',
      json: `{
  "format": "elasticsearch",
  "path": "clientes",
  "mode": "append",
  "options": { "es.nodes": "es.internal", "es.port": "9200", "es.mapping.id": "id" }
}`,
    },
  ],
}

export const DATABASE_FORMATS: FormatDef[] = [
  jdbcFormat({ id: 'postgresql', label: 'PostgreSQL', driver: 'org.postgresql.Driver', port: '5432', urlExample: 'jdbc:postgresql://db:5432/app' }),
  jdbcFormat({ id: 'mysql', label: 'MySQL', driver: 'com.mysql.cj.jdbc.Driver', port: '3306', urlExample: 'jdbc:mysql://db:3306/app' }),
  jdbcFormat({ id: 'mariadb', label: 'MariaDB', driver: 'org.mariadb.jdbc.Driver', port: '3306', urlExample: 'jdbc:mariadb://db:3306/app' }),
  jdbcFormat({ id: 'sqlserver', label: 'SQL Server', driver: 'com.microsoft.sqlserver.jdbc.SQLServerDriver', port: '1433', urlExample: 'jdbc:sqlserver://db:1433;databaseName=app' }),
  jdbcFormat({ id: 'oracle', label: 'Oracle', driver: 'oracle.jdbc.OracleDriver', port: '1521', urlExample: 'jdbc:oracle:thin:@//db:1521/ORCLPDB1' }),
  bigquery,
  snowflake,
  redshift,
  mongodb,
  documentdb,
  dynamodb,
  cassandra,
  elasticsearch,
]
