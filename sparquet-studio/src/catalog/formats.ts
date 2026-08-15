/**
 * IO format catalog — one entry per key of the reader/writer registries.
 *
 * Source of truth: sparquet/io/factory.py (registries) plus each io/*.py
 * module. Only the option keys the framework itself interprets carry a `default`
 * here; everything else is opaque pass-through to Spark and is listed because the
 * connector reads it, not the framework.
 */

import type { FieldOption, FieldSpec, FormatDef } from '@/catalog/types'
import { DATABASE_FORMATS } from './formats.databases'

/**
 * The framework never coerces option values and PySpark stringifies whatever it
 * receives, so boolean-ish options are emitted as the strings Spark expects.
 * `view.options.cache` makes this mandatory: a JSON boolean reaches `.lower()` and
 * crashes with AttributeError (io/view.py:43).
 */
const BOOL_OPTIONS: FieldOption[] = [
  { value: 'true', label: 'true' },
  { value: 'false', label: 'false' },
]

const TEXT_COMPRESSION_OPTIONS: FieldOption[] = [
  { value: 'none', label: 'none' },
  { value: 'gzip', label: 'gzip' },
  { value: 'bzip2', label: 'bzip2' },
  { value: 'lz4', label: 'lz4' },
  { value: 'snappy', label: 'snappy' },
  { value: 'deflate', label: 'deflate' },
]

const isMergeMode = (options: Record<string, unknown>): boolean =>
  String(options.mode ?? '').toLowerCase() === 'merge'

/**
 * Option-level predicates receive the OPTIONS object, which carries no `mode` key of
 * its own — the inspector injects the output's `mode` next to the options when it has
 * one. Without that context the field stays visible instead of silently disappearing.
 */
const visibleInMerge = (options: Record<string, unknown>): boolean =>
  !('mode' in options) || isMergeMode(options)

const mergeKeysField = (formatLabel: string): FieldSpec => ({
  key: 'merge_keys',
  label: 'Merge keys',
  type: 'string-list',
  required: true,
  placeholder: 'id',
  help: `Builds the ON clause as T.<key> = S.<key>, AND-joined. ${formatLabel} raises a ValueError when merge mode has no keys.`,
  visibleWhen: visibleInMerge,
  validate: (value, options) =>
    isMergeMode(options) && (!Array.isArray(value) || value.length === 0)
      ? 'Merge mode requires at least one merge key.'
      : null,
})

const mergeConditionField: FieldSpec = {
  key: 'merge_condition',
  label: 'Extra merge condition',
  type: 'sql',
  rows: 2,
  placeholder: 'T.deleted = false',
  help: 'ANDed into the ON clause. Use the aliases T. (target table) and S. (source DataFrame).',
  docs: 'Injected raw into the generated MERGE statement — no quoting or escaping is applied, so only well-formed SQL predicates over T./S. columns are safe here.',
  visibleWhen: visibleInMerge,
}

export const FORMATS: FormatDef[] = [
  {
    id: 'parquet',
    label: 'Parquet',
    icon: 'Database',
    canRead: true,
    canWrite: true,
    summary: 'Native Spark Parquet files on a filesystem path.',
    description:
      'Reads and writes Parquet directories, passing every configured option straight through to the Spark reader/writer. The framework interprets no option key by name.\n\nThe path is always a filesystem location — there is no table-name resolution, so a dotted value is just a literal path.',
    pathLabel: 'Path',
    pathPlaceholder: '/data/bronze/clientes',
    pathHelp:
      'Filesystem path or URI to a directory. Passed verbatim to load()/save(); never resolved as a catalog table.',
    modes: ['overwrite', 'append', 'error', 'ignore'],
    supportsPartitioning: true,
    supportsMerge: false,
    readOptions: [
      {
        key: 'mergeSchema',
        label: 'Merge schema',
        type: 'select',
        options: BOOL_OPTIONS,
        help: 'Unions the schemas of all part files instead of picking one summary file.',
      },
      {
        key: 'recursiveFileLookup',
        label: 'Recursive file lookup',
        type: 'select',
        options: BOOL_OPTIONS,
        help: 'Reads nested directories and ignores partition discovery.',
        group: 'advanced',
      },
      {
        key: 'pathGlobFilter',
        label: 'Path glob filter',
        type: 'text',
        placeholder: '*.parquet',
        group: 'advanced',
      },
      {
        key: 'modifiedAfter',
        label: 'Modified after',
        type: 'text',
        placeholder: '2025-01-01T00:00:00',
        help: 'Only files modified after this timestamp are read.',
        group: 'advanced',
      },
      {
        key: 'modifiedBefore',
        label: 'Modified before',
        type: 'text',
        placeholder: '2025-12-31T23:59:59',
        group: 'advanced',
      },
    ],
    writeOptions: [
      {
        key: 'compression',
        label: 'Compression',
        type: 'select',
        options: [
          { value: 'snappy', label: 'snappy' },
          { value: 'gzip', label: 'gzip' },
          { value: 'zstd', label: 'zstd' },
          { value: 'lz4', label: 'lz4' },
          { value: 'brotli', label: 'brotli' },
          { value: 'uncompressed', label: 'uncompressed' },
          { value: 'none', label: 'none' },
        ],
        help: 'Spark default is snappy when the key is omitted.',
      },
      {
        key: 'maxRecordsPerFile',
        label: 'Max records per file',
        type: 'number',
        placeholder: '1000000',
        help: 'Caps part-file size by row count. 0 disables the limit.',
        group: 'advanced',
      },
    ],
    gotchas: [
      'mode "merge" is not supported: it reaches df.write.mode("merge") and Spark raises "Unknown save mode".',
      'No table-name semantics — unlike Delta, a dotted value such as "schema.table" is treated as a literal path.',
      'Option values are never type-checked; PySpark stringifies them, so a list value silently becomes its Python str() form.',
      'Every input read gains an automatic ingestion_ts column, which lands in the written schema unless excluded via the output columns.',
    ],
    examples: [
      {
        title: 'Read a partitioned Parquet dataset',
        json: `{
  "format": "parquet",
  "path": "/data/bronze/clientes",
  "options": { "mergeSchema": "true" }
}`,
      },
      {
        title: 'Write partitioned by dt_ref',
        json: `{
  "format": "parquet",
  "path": "/data/silver/clientes",
  "mode": "overwrite",
  "partition_by": ["dt_ref"],
  "options": { "compression": "snappy" }
}`,
      },
    ],
  },

  {
    id: 'delta',
    label: 'Delta Lake',
    icon: 'Layers',
    canRead: true,
    canWrite: true,
    summary: 'Delta tables by catalog name or path, with time travel and MERGE upserts.',
    description:
      'Reads and writes Delta Lake, auto-detecting whether the path is a catalog table name or a physical location, and implements upserts through a generated MERGE INTO statement.\n\nThe merge is a blind upsert: UPDATE on match, INSERT otherwise. There is no delete branch and no source de-duplication.',
    pathLabel: 'Table or path',
    pathPlaceholder: 'catalog.schema.pedidos',
    pathHelp:
      'Treated as a physical path when it starts with /, s3://, gs://, abfss://, wasbs://, hdfs://, dbfs:/ or file: — otherwise any value containing a dot is treated as a catalog table name.',
    modes: ['overwrite', 'append', 'merge'],
    supportsPartitioning: true,
    supportsMerge: true,
    readOptions: [
      {
        key: 'versionAsOf',
        label: 'Version as of',
        type: 'text',
        placeholder: '12',
        help: 'Time travel by table version.',
      },
      {
        key: 'timestampAsOf',
        label: 'Timestamp as of',
        type: 'text',
        placeholder: '2025-05-10T10:00:00Z',
        help: 'Time travel by commit timestamp. Mutually exclusive with the version.',
      },
    ],
    writeOptions: [
      mergeKeysField('DeltaWriter'),
      mergeConditionField,
      {
        key: 'mergeSchema',
        label: 'Merge schema',
        type: 'select',
        options: BOOL_OPTIONS,
        help: 'Evolves the target schema with new columns coming from the DataFrame.',
        group: 'advanced',
      },
      {
        key: 'overwriteSchema',
        label: 'Overwrite schema',
        type: 'select',
        options: BOOL_OPTIONS,
        help: 'Replaces the target schema on an overwrite write.',
        group: 'advanced',
      },
      {
        key: 'replaceWhere',
        label: 'Replace where',
        type: 'sql',
        rows: 2,
        placeholder: "dt_ref = '2025-01-01'",
        help: 'Selective overwrite of the rows matching this predicate.',
        group: 'advanced',
      },
      {
        key: 'userMetadata',
        label: 'User metadata',
        type: 'text',
        placeholder: 'run=daily-load',
        help: 'Free-form string recorded in the Delta commit history.',
        group: 'advanced',
      },
      {
        key: 'maxRecordsPerFile',
        label: 'Max records per file',
        type: 'number',
        placeholder: '1000000',
        group: 'advanced',
      },
    ],
    gotchas: [
      'Merge mode requires merge_keys — an empty or absent list raises a ValueError before any Spark call.',
      'The table-name heuristic is a trap: "output/tabela.delta", "./out.delta" and "C:/data/my.delta" are all read as catalog table names. Force a leading / or a supported scheme for physical paths.',
      'Schemes outside the whitelist (s3a://, abfs://, wasb://, adl://, viewfs://, oss://) fall through to the dot test and can be misclassified as tables.',
      'partition_by is ignored entirely on the merge path.',
      'The generated UPDATE SET covers every column except the merge keys — a DataFrame containing only key columns produces invalid MERGE SQL.',
      'No delete branch and no source de-duplication: duplicated merge keys in the source raise Delta\'s "multiple source rows matched" error at runtime.',
      'The merge registers the temp view _spark_fw_merge_src, the same name the Iceberg writer uses — two merge outputs in one pipeline share it.',
      'merge_condition is interpolated raw into the SQL statement; validate it as an expression over T./S. columns.',
      'The merge runs through spark.sql(), so the Delta SQL extensions must be enabled on the session.',
      'mode is lower-cased before dispatch, so "MERGE" also works here (unlike Iceberg).',
    ],
    examples: [
      {
        title: 'Read a table at a past version',
        json: `{
  "format": "delta",
  "path": "catalog.schema.pedidos",
  "options": { "versionAsOf": "12" }
}`,
      },
      {
        title: 'Upsert with MERGE',
        json: `{
  "format": "delta",
  "path": "catalog.schema.pedidos",
  "mode": "merge",
  "options": {
    "merge_keys": ["pedido_id"],
    "merge_condition": "T.deleted = false"
  }
}`,
      },
    ],
  },

  {
    id: 'iceberg',
    label: 'Iceberg',
    icon: 'Mountain',
    canRead: true,
    canWrite: true,
    summary: 'Apache Iceberg tables through the Spark DSv2 connector, with MERGE upserts.',
    description:
      'Reads and writes Iceberg tables, with an upsert path built on a generated MERGE INTO ... UPDATE SET * / INSERT *.\n\nUnlike Delta there is no path-vs-table heuristic: the value is handed to the connector as-is, and on merge it is interpolated straight into the SQL statement.',
    pathLabel: 'Table identifier',
    pathPlaceholder: 'catalog.db.pedidos',
    pathHelp:
      'Catalog identifier resolved by the Iceberg connector. On merge it must be a valid SQL identifier (catalog.db.table) — a filesystem path produces invalid MERGE SQL.',
    modes: ['overwrite', 'append', 'merge'],
    supportsPartitioning: true,
    supportsMerge: true,
    readOptions: [
      {
        key: 'snapshot-id',
        label: 'Snapshot id',
        type: 'text',
        placeholder: '3821550127947089987',
        help: 'Reads the table at a specific snapshot.',
      },
      {
        key: 'as-of-timestamp',
        label: 'As of timestamp',
        type: 'text',
        placeholder: '1717200000000',
        help: 'Epoch milliseconds; reads the snapshot current at that instant.',
      },
      {
        key: 'branch',
        label: 'Branch',
        type: 'text',
        placeholder: 'audit',
        group: 'advanced',
      },
      {
        key: 'tag',
        label: 'Tag',
        type: 'text',
        placeholder: 'end-of-month',
        group: 'advanced',
      },
    ],
    writeOptions: [
      mergeKeysField('IcebergWriter'),
      mergeConditionField,
      {
        key: 'write-format',
        label: 'Write format',
        type: 'select',
        options: [
          { value: 'parquet', label: 'parquet' },
          { value: 'avro', label: 'avro' },
          { value: 'orc', label: 'orc' },
        ],
        help: 'File format of the data files written into the table.',
        group: 'advanced',
      },
      {
        key: 'target-file-size-bytes',
        label: 'Target file size (bytes)',
        type: 'number',
        placeholder: '536870912',
        group: 'advanced',
      },
      {
        key: 'fanout-enabled',
        label: 'Fanout enabled',
        type: 'select',
        options: BOOL_OPTIONS,
        help: 'Allows writing unsorted data to partitioned tables at the cost of more open files.',
        group: 'advanced',
      },
    ],
    gotchas: [
      'Merge mode requires merge_keys — an empty or absent list raises a ValueError.',
      'The mode is compared case-sensitively against "merge": "MERGE" skips the merge path and reaches df.write.mode("MERGE"), where Spark raises "Unknown save mode" — nothing is written and the run reports success=false (Delta lower-cases first). Always emit it lowercase.',
      'On non-merge writes merge_keys and merge_condition are NOT stripped — they are forwarded to Spark as writer options, silently but wrongly.',
      'The merge target is string-interpolated with no quoting and no path fallback, so a filesystem path yields invalid MERGE SQL.',
      'MERGE uses UPDATE SET * / INSERT *, so source and target schemas must line up — the auto-injected ingestion_ts column must exist in the target table.',
      'No delete branch and no source de-duplication: duplicated merge keys raise a runtime error.',
      'Registers the temp view _spark_fw_merge_src, the same name the Delta writer uses.',
      'Runs through spark.sql(), so the Iceberg session extensions and catalog must be configured.',
      'partition_by is ignored on merge, and on save it is only honored by the connector at table-creation time.',
    ],
    examples: [
      {
        title: 'Read a snapshot by timestamp',
        json: `{
  "format": "iceberg",
  "path": "catalog.db.pedidos",
  "options": { "as-of-timestamp": "1717200000000" }
}`,
      },
      {
        title: 'Upsert with MERGE',
        json: `{
  "format": "iceberg",
  "path": "catalog.db.pedidos",
  "mode": "merge",
  "options": { "merge_keys": ["id"] }
}`,
      },
    ],
  },

  {
    id: 'csv',
    label: 'CSV',
    icon: 'FileSpreadsheet',
    canRead: true,
    canWrite: true,
    summary: 'Delimited text with framework defaults for header, inferSchema and encoding.',
    description:
      'Reads and writes delimited files. The framework supplies its own defaults (header and encoding on both sides, inferSchema on read only) and merges the configured options over them, so any of them can be overridden.\n\nWriting produces a directory of part files, never a single .csv file.',
    pathLabel: 'Path',
    pathPlaceholder: '/landing/vendas',
    pathHelp:
      'Filesystem path or URI. On write it is a directory that receives the part files, never a file name.',
    modes: ['overwrite', 'append', 'error', 'ignore'],
    supportsPartitioning: true,
    supportsMerge: false,
    readOptions: [
      {
        key: 'header',
        label: 'Header',
        type: 'select',
        options: BOOL_OPTIONS,
        default: 'true',
        help: 'Framework default: true. Uses the first line as column names.',
      },
      {
        key: 'inferSchema',
        label: 'Infer schema',
        type: 'select',
        options: BOOL_OPTIONS,
        default: 'true',
        help: 'Framework default: true. Costs an extra pass over the file and can change types between runs.',
      },
      {
        key: 'encoding',
        label: 'Encoding',
        type: 'text',
        default: 'UTF-8',
        placeholder: 'UTF-8',
        help: 'Framework default: UTF-8.',
      },
      {
        key: 'sep',
        label: 'Separator',
        type: 'text',
        placeholder: ',',
        help: 'Spark default is a comma when the key is omitted.',
      },
      {
        key: 'quote',
        label: 'Quote character',
        type: 'text',
        placeholder: '"',
        group: 'advanced',
      },
      {
        key: 'escape',
        label: 'Escape character',
        type: 'text',
        placeholder: '\\',
        group: 'advanced',
      },
      {
        key: 'multiLine',
        label: 'Multi-line records',
        type: 'select',
        options: BOOL_OPTIONS,
        help: 'Allows quoted values to span several lines.',
        group: 'advanced',
      },
      {
        key: 'nullValue',
        label: 'Null literal',
        type: 'text',
        placeholder: 'NULL',
        group: 'advanced',
      },
      {
        key: 'dateFormat',
        label: 'Date format',
        type: 'text',
        placeholder: 'yyyy-MM-dd',
        group: 'advanced',
      },
      {
        key: 'timestampFormat',
        label: 'Timestamp format',
        type: 'text',
        placeholder: "yyyy-MM-dd'T'HH:mm:ss",
        group: 'advanced',
      },
      {
        key: 'mode',
        label: 'Parser mode',
        type: 'select',
        options: [
          { value: 'PERMISSIVE', label: 'PERMISSIVE' },
          { value: 'DROPMALFORMED', label: 'DROPMALFORMED' },
          { value: 'FAILFAST', label: 'FAILFAST' },
        ],
        help: 'CSV parser behavior for corrupt records. Unrelated to the write mode.',
        group: 'advanced',
      },
    ],
    writeOptions: [
      {
        key: 'header',
        label: 'Header',
        type: 'select',
        options: BOOL_OPTIONS,
        default: 'true',
        help: 'Framework default: true. Writes the column names as the first line.',
      },
      {
        key: 'encoding',
        label: 'Encoding',
        type: 'text',
        default: 'UTF-8',
        placeholder: 'UTF-8',
        help: 'Framework default: UTF-8.',
      },
      {
        key: 'sep',
        label: 'Separator',
        type: 'text',
        placeholder: ',',
        help: 'Spark default is a comma when the key is omitted.',
      },
      {
        key: 'quote',
        label: 'Quote character',
        type: 'text',
        placeholder: '"',
        group: 'advanced',
      },
      {
        key: 'quoteAll',
        label: 'Quote all values',
        type: 'select',
        options: BOOL_OPTIONS,
        group: 'advanced',
      },
      {
        key: 'escape',
        label: 'Escape character',
        type: 'text',
        placeholder: '\\',
        group: 'advanced',
      },
      {
        key: 'nullValue',
        label: 'Null literal',
        type: 'text',
        placeholder: '',
        group: 'advanced',
      },
      {
        key: 'emptyValue',
        label: 'Empty-string literal',
        type: 'text',
        placeholder: '',
        group: 'advanced',
      },
      {
        key: 'dateFormat',
        label: 'Date format',
        type: 'text',
        placeholder: 'yyyy-MM-dd',
        group: 'advanced',
      },
      {
        key: 'timestampFormat',
        label: 'Timestamp format',
        type: 'text',
        placeholder: "yyyy-MM-dd'T'HH:mm:ss",
        group: 'advanced',
      },
      {
        key: 'compression',
        label: 'Compression',
        type: 'select',
        options: TEXT_COMPRESSION_OPTIONS,
        group: 'advanced',
      },
    ],
    gotchas: [
      'mode "merge" is invalid: it reaches df.write.mode("merge") and fails in Spark.',
      'The defaults merge is a case-sensitive dict merge — override with exactly "header", "inferSchema" and "encoding"; a differently-cased key is sent to Spark alongside the default.',
      'inferSchema on read costs an extra pass over the file and can silently change column types between runs.',
      'Writing produces a directory of part files, so the path is a folder and not a file name.',
      'inferSchema is deliberately absent from the write defaults — it means nothing on write.',
      'The auto-injected ingestion_ts column is written unless excluded by the output columns.',
    ],
    examples: [
      {
        title: 'Read a semicolon-delimited file without inference',
        json: `{
  "format": "csv",
  "path": "/landing/vendas",
  "options": { "sep": ";", "header": "true", "inferSchema": "false" }
}`,
      },
      {
        title: 'Export two columns',
        json: `{
  "format": "csv",
  "path": "/export/relatorio",
  "mode": "overwrite",
  "columns": ["id", "total"],
  "options": { "sep": ";", "encoding": "UTF-8" }
}`,
      },
    ],
  },

  {
    id: 'txt',
    label: 'Text',
    icon: 'FileText',
    canRead: true,
    canWrite: true,
    summary: 'Plain text — one row per line in a single `value` column.',
    description:
      'Reads plain-text files into a single string column named `value`, and writes a single-string-column DataFrame back out as text.\n\nThe writer accepts exactly one string column, so any text output needs a column projection (or a concat_ws/to_json transformation) that reduces the DataFrame to one column.',
    pathLabel: 'Path',
    pathPlaceholder: '/landing/logs',
    pathHelp:
      'Filesystem path or URI. On write it is a directory that receives the part files.',
    modes: ['overwrite', 'append', 'error', 'ignore'],
    supportsPartitioning: true,
    supportsMerge: false,
    readOptions: [
      {
        key: 'wholetext',
        label: 'Whole text',
        type: 'select',
        options: BOOL_OPTIONS,
        help: 'true reads each file as one single string instead of one row per line. Spark default: false.',
      },
      {
        key: 'lineSep',
        label: 'Line separator',
        type: 'text',
        placeholder: '\\n',
        help: 'Spark default is the newline character when the key is omitted.',
        group: 'advanced',
      },
      {
        key: 'pathGlobFilter',
        label: 'Path glob filter',
        type: 'text',
        placeholder: '*.log',
        group: 'advanced',
      },
      {
        key: 'recursiveFileLookup',
        label: 'Recursive file lookup',
        type: 'select',
        options: BOOL_OPTIONS,
        group: 'advanced',
      },
    ],
    writeOptions: [
      {
        key: 'compression',
        label: 'Compression',
        type: 'select',
        options: TEXT_COMPRESSION_OPTIONS,
      },
      {
        key: 'lineSep',
        label: 'Line separator',
        type: 'text',
        placeholder: '\\n',
        help: 'Spark default is the newline character when the key is omitted.',
        group: 'advanced',
      },
    ],
    gotchas: [
      'The write requires exactly ONE string column — Spark raises "text data source supports only a single column" otherwise.',
      'Reading always yields the fixed column name `value`; downstream transformations must use that name.',
      'The auto-injected ingestion_ts column makes a freshly-read DataFrame two columns wide, so a naive read-text → write-text pipeline fails until it is projected away.',
      'mode "merge" is invalid: it reaches df.write.mode("merge") and fails in Spark.',
      'Partition columns are excluded from the written data schema, so partition_by plus one remaining string column is valid.',
    ],
    examples: [
      {
        title: 'Read log lines',
        json: `{
  "format": "txt",
  "path": "/landing/logs",
  "options": { "wholetext": "false", "pathGlobFilter": "*.log" }
}`,
      },
      {
        title: 'Write a single column, gzipped',
        json: `{
  "format": "txt",
  "path": "/export/linhas",
  "mode": "overwrite",
  "columns": ["value"],
  "options": { "compression": "gzip" }
}`,
      },
    ],
  },

  {
    id: 'view',
    label: 'Temp view',
    icon: 'Table2',
    canRead: true,
    canWrite: true,
    summary: 'Session temp views — hand a DataFrame to the next pipeline without re-reading.',
    description:
      'Writes a DataFrame as a session-scoped temp view (cached by default) and reads any temp view or catalog table by name.\n\nThe write is always createOrReplaceTempView, so mode and partition_by are accepted by the schema but never used.',
    pathLabel: 'View name',
    pathPlaceholder: 'cessoes_para_processar',
    pathHelp:
      'On read, resolved via spark.table() — a temp view or a catalog table (catalog.schema.table). On write it must be a bare unqualified identifier; a dotted name raises a parse error.',
    modes: ['overwrite'],
    supportsPartitioning: false,
    supportsMerge: false,
    readOptions: [],
    writeOptions: [
      {
        key: 'cache',
        label: 'Cache',
        type: 'select',
        options: BOOL_OPTIONS,
        default: 'true',
        help: 'Framework default: true. Caching also forces an eager count() to materialize the view.',
        docs: 'Must be the STRING "false" to disable. The writer calls `.lower()` on this value, so a JSON boolean crashes with AttributeError.',
        validate: (value) =>
          typeof value === 'boolean'
            ? 'cache must be the string "true" or "false", not a JSON boolean.'
            : null,
      },
    ],
    gotchas: [
      'options.cache must be a quoted string — a JSON boolean false crashes the writer with AttributeError.',
      'mode is completely ignored: "append" does not append, it replaces the view.',
      'partition_by is ignored.',
      'Caching is on by default and forces an eager count(), a materialization cost that is invisible in the config.',
      'The view is session-scoped: it lives only while the SparkSession is alive and is invisible to other jobs.',
      'The write name must be a bare identifier, even though the read side accepts dotted catalog names.',
      'The reader ignores options entirely, so anything set on a view input is silently dropped.',
    ],
    examples: [
      {
        title: 'Read a staged view',
        json: `{
  "format": "view",
  "path": "cessoes_para_processar"
}`,
      },
      {
        title: 'Publish a staging view without caching',
        json: `{
  "format": "view",
  "path": "view_registro_staging",
  "options": { "cache": "false" }
}`,
      },
    ],
  },

  {
    id: 'kafka',
    label: 'Kafka',
    icon: 'Radio',
    canRead: true,
    canWrite: true,
    summary: 'Batch read and write for Kafka topics (Amazon MSK via SASL/IAM options).',
    description:
      'Reads a Kafka topic in batch and publishes a DataFrame back to a topic.\n\nRead: subscribes to the path topic and returns the raw Kafka schema (key/value as binary, topic, partition, offset, timestamp) — cast value downstream. Without explicit offsets a batch read consumes the whole topic (startingOffsets=earliest, endingOffsets=latest).\n\nWrite: the value/key columns are renamed to `value` and `key`, and every column outside {key, value, topic, partition, timestamp, headers} is dropped before publishing.\n\nAmazon MSK is the same connector with SASL/IAM options.',
    pathLabel: 'Topic',
    pathPlaceholder: 'registro-lastros',
    pathHelp:
      'The Kafka topic name — not a filesystem path. It is applied as the writer option `topic`, so an option with that same key would override it.',
    modes: ['append'],
    supportsPartitioning: false,
    supportsMerge: false,
    readOptions: [
      {
        key: 'bootstrap_servers',
        label: 'Bootstrap servers',
        type: 'text',
        placeholder: 'broker1:9092,broker2:9092',
        help: 'Comma-separated broker list. Friendly alias for kafka.bootstrap.servers — set one or the other.',
      },
      {
        key: 'startingOffsets',
        label: 'Starting offsets',
        type: 'text',
        default: 'earliest',
        placeholder: 'earliest',
        help: 'Batch default: earliest (reads the whole topic). Also "latest" or a JSON offsets map.',
      },
      {
        key: 'endingOffsets',
        label: 'Ending offsets',
        type: 'text',
        default: 'latest',
        placeholder: 'latest',
        help: 'Batch default: latest.',
      },
      {
        key: 'kafka.bootstrap.servers',
        label: 'Bootstrap servers (canonical key)',
        type: 'text',
        placeholder: 'broker1:9092',
        group: 'advanced',
      },
      {
        key: 'assign',
        label: 'Assign',
        type: 'text',
        placeholder: '{"meu-topico":[0,1]}',
        help: 'Explicit topic-partitions to read instead of subscribing to the path topic.',
        group: 'advanced',
      },
      {
        key: 'subscribePattern',
        label: 'Subscribe pattern',
        type: 'text',
        placeholder: 'eventos-.*',
        help: 'Regex of topics to read instead of the single path topic.',
        group: 'advanced',
      },
      {
        key: 'kafka.security.protocol',
        label: 'Security protocol',
        type: 'select',
        options: [
          { value: 'PLAINTEXT', label: 'PLAINTEXT' },
          { value: 'SSL', label: 'SSL' },
          { value: 'SASL_PLAINTEXT', label: 'SASL_PLAINTEXT' },
          { value: 'SASL_SSL', label: 'SASL_SSL' },
        ],
        group: 'advanced',
      },
      {
        key: 'kafka.sasl.mechanism',
        label: 'SASL mechanism',
        type: 'text',
        placeholder: 'AWS_MSK_IAM',
        help: 'e.g. PLAIN, SCRAM-SHA-512, or AWS_MSK_IAM for Amazon MSK.',
        group: 'advanced',
      },
    ],
    writeOptions: [
      {
        key: 'bootstrap_servers',
        // Required as a PAIR, never alone: the writer accepts either key, so `required`
        // here would report the canonical spelling as a missing value (lint.ts:347
        // reports and skips the field's own validate).
        label: 'Bootstrap servers',
        type: 'text',
        placeholder: 'broker1:9092,broker2:9092',
        help: 'Comma-separated broker list. Renamed internally to kafka.bootstrap.servers — set this one OR the canonical key, not both.',
        docs: 'The writer raises a ValueError when neither `bootstrap_servers` nor the canonical `kafka.bootstrap.servers` is present, before any Spark call. When both are set, this one is popped last and overwrites the canonical value.',
        validate: (value, options) => {
          const alias = typeof value === 'string' ? value.trim() : ''
          const canonical = options['kafka.bootstrap.servers']
          const spark = typeof canonical === 'string' ? canonical.trim() : ''
          return alias === '' && spark === ''
            ? 'Set bootstrap_servers, or the canonical kafka.bootstrap.servers under advanced options.'
            : null
        },
      },
      {
        key: 'value_column',
        label: 'Value column',
        type: 'text',
        default: 'payload',
        placeholder: 'payload',
        help: 'Column renamed to `value` before writing. Code default is payload (the docs claiming "value" are wrong).',
        docs: 'The write fails with a ValueError when the DataFrame has no `value` column after the rename. If a column named `value` already exists, no rename happens and that column wins.',
      },
      {
        key: 'key_column',
        label: 'Key column',
        type: 'text',
        default: 'header',
        placeholder: 'header',
        help: 'Column renamed to `key`. Code default is header; leave empty to publish without a key.',
      },
      {
        key: 'kafka.bootstrap.servers',
        label: 'Bootstrap servers (canonical key)',
        type: 'text',
        placeholder: 'broker1:9092',
        help: 'Alternative to bootstrap_servers, passed straight to the connector. Set only one of the two.',
        group: 'advanced',
      },
      {
        key: 'kafka.security.protocol',
        label: 'Security protocol',
        type: 'select',
        options: [
          { value: 'PLAINTEXT', label: 'PLAINTEXT' },
          { value: 'SSL', label: 'SSL' },
          { value: 'SASL_PLAINTEXT', label: 'SASL_PLAINTEXT' },
          { value: 'SASL_SSL', label: 'SASL_SSL' },
        ],
        group: 'advanced',
      },
      {
        key: 'kafka.sasl.mechanism',
        label: 'SASL mechanism',
        type: 'select',
        options: [
          { value: 'PLAIN', label: 'PLAIN' },
          { value: 'SCRAM-SHA-256', label: 'SCRAM-SHA-256' },
          { value: 'SCRAM-SHA-512', label: 'SCRAM-SHA-512' },
        ],
        group: 'advanced',
      },
      {
        key: 'kafka.sasl.jaas.config',
        label: 'SASL JAAS config',
        type: 'textarea',
        rows: 3,
        placeholder:
          'org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="...";',
        help: 'Stored in plaintext inside the pipeline JSON — prefer a secret reference resolved at runtime.',
        group: 'advanced',
      },
    ],
    gotchas: [
      'Batch read: without explicit offsets the framework applies startingOffsets=earliest / endingOffsets=latest, so a read consumes the whole topic once.',
      'The read returns the raw Kafka schema (key/value as binary) — add a CAST(value AS STRING) (or from_json) transformation right after the source.',
      'Amazon MSK: set kafka.security.protocol=SASL_SSL and kafka.sasl.mechanism=AWS_MSK_IAM, with the aws-msk-iam-auth JAR on the classpath.',
      'bootstrap_servers (or kafka.bootstrap.servers) is mandatory on both read and write, and validated before any Spark call.',
      'A `value` column must exist after the rename, otherwise the write raises a ValueError listing the available columns.',
      'The code defaults are value_column=payload and key_column=header, contradicting the documented value/key.',
      'Only {key, value, topic, partition, timestamp, headers} survive — every other column is dropped silently, including the auto-injected ingestion_ts.',
      'mode is never read: batch Kafka writes are append-only, so overwrite and merge mean nothing here.',
      'partition_by is ignored, but a DataFrame column literally named `partition` is forwarded as the target partition.',
      'Requires the spark-sql-kafka-0-10 connector on the cluster classpath; otherwise the failure is a class-not-found at write time.',
      'kafka.sasl.jaas.config puts credentials in plaintext inside the pipeline JSON.',
    ],
    examples: [
      {
        title: 'Publish payload with a key column',
        json: `{
  "format": "kafka",
  "path": "registro-lastros",
  "columns": ["header", "payload"],
  "options": {
    "bootstrap_servers": "broker1:9092,broker2:9092",
    "value_column": "payload",
    "key_column": "header"
  }
}`,
      },
      {
        title: 'Publish keyless over SASL_SSL',
        json: `{
  "format": "kafka",
  "path": "registro-lastros",
  "options": {
    "bootstrap_servers": "broker1:9093",
    "value_column": "payload",
    "key_column": "",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN"
  }
}`,
      },
    ],
  },

  ...DATABASE_FORMATS,
]
