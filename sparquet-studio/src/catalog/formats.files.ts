/**
 * IO format catalog — extra file/lakehouse formats.
 *
 * Source of truth: sparquet/io/factory.py plus io/{json,orc,avro,xml,binary,hudi}.py.
 * json/orc are native Spark; avro/xml/hudi need their connector JAR on the classpath.
 */

import type { FieldOption, FieldSpec, FormatDef } from '@/catalog/types'

const BOOL_OPTIONS: FieldOption[] = [
  { value: 'true', label: 'true' },
  { value: 'false', label: 'false' },
]

const COMPRESSION: FieldOption[] = [
  { value: 'snappy', label: 'snappy' },
  { value: 'gzip', label: 'gzip' },
  { value: 'zstd', label: 'zstd' },
  { value: 'lz4', label: 'lz4' },
  { value: 'none', label: 'none' },
]

export const basePathOption = (example: string): FieldSpec => ({
  key: 'basePath',
  label: 'Base path',
  type: 'text',
  placeholder: example,
  help: 'Root of the partitioned dataset. Needed when the path points INSIDE a partition directory.',
  docs: [
    'Spark derives the partition columns from the directories **below** the path it is given. Reading',
    '`' + example + '/dt=2026-09-01` directly is the cheapest possible read — no listing of the other',
    'partitions at all — but there is nothing left below it to derive from, so `dt` is simply absent',
    'from the DataFrame, and a downstream `select`/`filter` on it fails at runtime.',
    '',
    'Setting `basePath` to the dataset root (`' + example + '`) restores the column while keeping the',
    'narrow read. Same for a glob (`dt=2026-09-0*`).',
  ].join('\n'),
  group: 'advanced',
})

const json: FormatDef = {
  id: 'json',
  label: 'JSON',
  icon: 'FileJson',
  canRead: true,
  canWrite: true,
  summary: 'Native Spark JSON files on a filesystem path.',
  description:
    'Reads and writes JSON (one object per line by default). Native to Spark — no extra JAR. The path is a directory; writing produces part files.',
  pathLabel: 'Path',
  pathPlaceholder: '/data/landing/eventos',
  pathHelp: 'Filesystem path or URI to a directory of JSON files.',
  modes: ['overwrite', 'append', 'error', 'ignore'],
  supportsPartitioning: true,
  supportsMerge: false,
  readOptions: [
    basePathOption('/data/landing/eventos'),
    { key: 'multiLine', label: 'Multi-line', type: 'select', options: BOOL_OPTIONS, help: 'true reads each file as one JSON document instead of one object per line.' },
    { key: 'primitivesAsString', label: 'Primitives as string', type: 'select', options: BOOL_OPTIONS, group: 'advanced' },
    { key: 'dateFormat', label: 'Date format', type: 'text', placeholder: 'yyyy-MM-dd', group: 'advanced' },
    { key: 'timestampFormat', label: 'Timestamp format', type: 'text', group: 'advanced' },
    { key: 'mode', label: 'Parser mode', type: 'select', options: [{ value: 'PERMISSIVE', label: 'PERMISSIVE' }, { value: 'DROPMALFORMED', label: 'DROPMALFORMED' }, { value: 'FAILFAST', label: 'FAILFAST' }], group: 'advanced' },
  ],
  writeOptions: [
    { key: 'compression', label: 'Compression', type: 'select', options: COMPRESSION, group: 'advanced' },
    { key: 'dateFormat', label: 'Date format', type: 'text', group: 'advanced' },
    { key: 'timestampFormat', label: 'Timestamp format', type: 'text', group: 'advanced' },
    { key: 'ignoreNullFields', label: 'Ignore null fields', type: 'select', options: BOOL_OPTIONS, group: 'advanced' },
  ],
  gotchas: [
    'Default is one JSON object per line (JSON Lines); set multiLine=true for a single pretty-printed document per file.',
    'Writing produces a directory of part files, never a single .json.',
    'mode "merge" is invalid for JSON.',
  ],
  examples: [
    { title: 'Read newline-delimited JSON', json: `{
  "format": "json",
  "path": "/data/landing/eventos"
}` },
    { title: 'Read one document per file', json: `{
  "format": "json",
  "path": "/data/payloads",
  "options": { "multiLine": "true" }
}` },
  ],
}

const orc: FormatDef = {
  id: 'orc',
  label: 'ORC',
  icon: 'Database',
  canRead: true,
  canWrite: true,
  summary: 'Native Spark ORC columnar files.',
  description:
    'Reads and writes ORC, a columnar format like Parquet. Native to Spark — no extra JAR. Good compression and predicate pushdown.',
  pathLabel: 'Path',
  pathPlaceholder: '/data/curated/pedidos',
  pathHelp: 'Filesystem path or URI to an ORC directory.',
  modes: ['overwrite', 'append', 'error', 'ignore'],
  supportsPartitioning: true,
  supportsMerge: false,
  readOptions: [
    basePathOption('/data/silver/vendas'),
    { key: 'mergeSchema', label: 'Merge schema', type: 'select', options: BOOL_OPTIONS, group: 'advanced' },
  ],
  writeOptions: [
    { key: 'compression', label: 'Compression', type: 'select', options: [{ value: 'zlib', label: 'zlib' }, { value: 'snappy', label: 'snappy' }, { value: 'lz4', label: 'lz4' }, { value: 'zstd', label: 'zstd' }, { value: 'none', label: 'none' }], help: 'ORC default is zlib.' },
  ],
  gotchas: ['mode "merge" is invalid for ORC.', 'Writing produces a directory of part files.'],
  examples: [
    { title: 'Write ORC partitioned', json: `{
  "format": "orc",
  "path": "/data/curated/pedidos",
  "mode": "overwrite",
  "partition_by": ["dt_ref"]
}` },
  ],
}

const avro: FormatDef = {
  id: 'avro',
  label: 'Avro',
  icon: 'FileCode2',
  canRead: true,
  canWrite: true,
  summary: 'Apache Avro row files (needs spark-avro).',
  description:
    'Reads and writes Avro. Requires the `org.apache.spark:spark-avro` package on the classpath (spark.jars.packages). Row-oriented — good for record streams and schema evolution.',
  pathLabel: 'Path',
  pathPlaceholder: '/data/raw/eventos',
  pathHelp: 'Filesystem path or URI to an Avro directory.',
  modes: ['overwrite', 'append', 'error', 'ignore'],
  supportsPartitioning: true,
  supportsMerge: false,
  readOptions: [
    basePathOption('/data/bronze/eventos'),
    { key: 'avroSchema', label: 'Avro schema', type: 'json', rows: 4, help: 'Explicit reader schema (JSON).', group: 'advanced' },
    { key: 'ignoreExtension', label: 'Ignore extension', type: 'select', options: BOOL_OPTIONS, group: 'advanced' },
  ],
  writeOptions: [
    { key: 'compression', label: 'Compression', type: 'select', options: [{ value: 'snappy', label: 'snappy' }, { value: 'deflate', label: 'deflate' }, { value: 'bzip2', label: 'bzip2' }, { value: 'xz', label: 'xz' }] },
    { key: 'recordName', label: 'Record name', type: 'text', group: 'advanced' },
    { key: 'recordNamespace', label: 'Record namespace', type: 'text', group: 'advanced' },
  ],
  gotchas: ['Requires the spark-avro package on the classpath.', 'mode "merge" is invalid.'],
  examples: [
    { title: 'Read Avro', json: `{ "format": "avro", "path": "/data/raw/eventos" }` },
  ],
}

const xml: FormatDef = {
  id: 'xml',
  label: 'XML',
  icon: 'FileCode2',
  canRead: true,
  canWrite: true,
  summary: 'XML files via spark-xml (rowTag required).',
  description:
    'Reads and writes XML. Requires the `com.databricks:spark-xml` package on the classpath (it registers the "xml" format). Each record is delimited by `rowTag`.',
  pathLabel: 'Path',
  pathPlaceholder: '/data/raw/catalogo',
  pathHelp: 'Filesystem path or URI to an XML directory.',
  modes: ['overwrite', 'append', 'error', 'ignore'],
  supportsPartitioning: true,
  supportsMerge: false,
  readOptions: [
    basePathOption('/data/raw/catalogo'),
    { key: 'rowTag', label: 'Row tag', type: 'text', required: true, placeholder: 'book', help: 'Required: the tag that delimits each record (row).', validate: (v) => (typeof v === 'string' && v.trim() !== '' ? null : 'rowTag is required for XML.') },
    { key: 'attributePrefix', label: 'Attribute prefix', type: 'text', placeholder: '_', group: 'advanced' },
    { key: 'valueTag', label: 'Value tag', type: 'text', placeholder: '_VALUE', group: 'advanced' },
    { key: 'mode', label: 'Parser mode', type: 'select', options: [{ value: 'PERMISSIVE', label: 'PERMISSIVE' }, { value: 'DROPMALFORMED', label: 'DROPMALFORMED' }, { value: 'FAILFAST', label: 'FAILFAST' }], group: 'advanced' },
  ],
  writeOptions: [
    { key: 'rowTag', label: 'Row tag', type: 'text', required: true, placeholder: 'book', validate: (v) => (typeof v === 'string' && v.trim() !== '' ? null : 'rowTag is required for XML.') },
    { key: 'rootTag', label: 'Root tag', type: 'text', placeholder: 'books', help: 'Default: "rows".', group: 'advanced' },
  ],
  gotchas: [
    'Requires the spark-xml package on the classpath.',
    'rowTag is mandatory on both read and write.',
    'mode "merge" is invalid.',
  ],
  examples: [
    { title: 'Read XML books', json: `{
  "format": "xml",
  "path": "/data/raw/catalogo",
  "options": { "rowTag": "book" }
}` },
  ],
}

const binary: FormatDef = {
  id: 'binary',
  label: 'Binary files',
  icon: 'Box',
  canRead: true,
  canWrite: false,
  summary: 'Read-only: whole files as binary (binaryFile).',
  description:
    'Reads entire files as binary — columns `path`, `modificationTime`, `length`, `content`. Native to Spark. Useful for images, PDFs and blobs.\n\nRead-only: Spark cannot write this format, so there is no binary writer — persist the `content` column via parquet/delta instead.',
  pathLabel: 'Path',
  pathPlaceholder: '/data/raw/documentos',
  pathHelp: 'Filesystem path or URI to a directory of files.',
  modes: [],
  supportsPartitioning: false,
  supportsMerge: false,
  readOptions: [
    { key: 'pathGlobFilter', label: 'Path glob filter', type: 'text', placeholder: '*.pdf' },
    { key: 'recursiveFileLookup', label: 'Recursive file lookup', type: 'select', options: BOOL_OPTIONS, group: 'advanced' },
    { key: 'modifiedAfter', label: 'Modified after', type: 'text', group: 'advanced' },
    { key: 'modifiedBefore', label: 'Modified before', type: 'text', group: 'advanced' },
  ],
  writeOptions: [],
  gotchas: [
    'READ-ONLY: there is no binary writer (Spark cannot write binaryFile) — using it as an output raises a ValueError listing the writable formats.',
    'Every file is loaded whole into the `content` column; a folder of large files can blow up memory.',
  ],
  examples: [
    { title: 'Read PDFs', json: `{
  "format": "binary",
  "path": "/data/raw/documentos",
  "options": { "pathGlobFilter": "*.pdf" }
}` },
  ],
}

const hudi: FormatDef = {
  id: 'hudi',
  label: 'Apache Hudi',
  icon: 'Layers',
  canRead: true,
  canWrite: true,
  summary: 'Apache Hudi lakehouse tables (upsert via hoodie options).',
  description:
    'Reads and writes Apache Hudi. Requires the `hudi-spark-bundle` JAR and the Hudi session extensions. The path is the table base path. Partitioning and upsert are controlled by `hoodie.*` options — the framework `partition_by` is NOT used here.',
  pathLabel: 'Base path',
  pathPlaceholder: 's3://lake/hudi/pedidos',
  pathHelp: 'Base path of the Hudi table (a filesystem/object-store path).',
  modes: ['append', 'overwrite'],
  supportsPartitioning: false,
  supportsMerge: false,
  readOptions: [
    { key: 'hoodie.datasource.query.type', label: 'Query type', type: 'select', options: [{ value: 'snapshot', label: 'snapshot' }, { value: 'incremental', label: 'incremental' }, { value: 'read_optimized', label: 'read_optimized' }] },
    { key: 'hoodie.datasource.read.begin.instanttime', label: 'Begin instant', type: 'text', help: 'Start commit for an incremental read.', group: 'advanced' },
  ],
  writeOptions: [
    { key: 'hoodie.table.name', label: 'Table name', type: 'text', required: true, validate: (v) => (typeof v === 'string' && v.trim() !== '' ? null : 'hoodie.table.name is required.') },
    { key: 'hoodie.datasource.write.recordkey.field', label: 'Record key field', type: 'text', help: 'Primary key for upsert.' },
    { key: 'hoodie.datasource.write.precombine.field', label: 'Precombine field', type: 'text', help: 'Tie-breaker column — latest wins.' },
    { key: 'hoodie.datasource.write.partitionpath.field', label: 'Partition path field', type: 'text', group: 'advanced' },
    { key: 'hoodie.datasource.write.operation', label: 'Write operation', type: 'select', options: [{ value: 'upsert', label: 'upsert' }, { value: 'insert', label: 'insert' }, { value: 'bulk_insert', label: 'bulk_insert' }, { value: 'delete', label: 'delete' }] },
    { key: 'hoodie.datasource.write.table.type', label: 'Table type', type: 'select', options: [{ value: 'COPY_ON_WRITE', label: 'COPY_ON_WRITE' }, { value: 'MERGE_ON_READ', label: 'MERGE_ON_READ' }], group: 'advanced' },
  ],
  gotchas: [
    'Requires the hudi-spark bundle JAR and the Hudi session extensions on the Spark session.',
    'Upsert is via hoodie.datasource.write.operation=upsert (with recordkey + precombine) — not the framework `merge` mode.',
    'partition_by is ignored — use hoodie.datasource.write.partitionpath.field.',
  ],
  examples: [
    { title: 'Upsert into a Hudi table', json: `{
  "format": "hudi",
  "path": "s3://lake/hudi/pedidos",
  "mode": "append",
  "options": {
    "hoodie.table.name": "pedidos",
    "hoodie.datasource.write.recordkey.field": "pedido_id",
    "hoodie.datasource.write.precombine.field": "atualizado_em",
    "hoodie.datasource.write.operation": "upsert"
  }
}` },
  ],
}

export const FILE_FORMATS: FormatDef[] = [json, orc, avro, xml, binary, hudi]
