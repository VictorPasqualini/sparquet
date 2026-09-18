/**
 * Where a dataset sits, said the way a metastore says it.
 *
 * Glue, Hive and Unity Catalog all show the same two shapes: a TABLE, which has a
 * name inside a database inside a catalog, and a LOCATION, which is a directory
 * inside a bucket. Sparquet has no metastore — a Job names an address and the
 * engine resolves it — so the hierarchy is read back out of the address instead of
 * looked up. `analytics.gold.revenue` is three levels of catalog;
 * `/lake/gold/revenue` is a bucket and a folder. Both end at one leaf, and the leaf
 * is what a person documents.
 *
 * What decides which is which:
 *   - the FORMAT says what kind of thing it is (delta and iceberg are tables even
 *     when they live on a path; parquet and csv are directories even when they
 *     hold a single file);
 *   - the ADDRESS says where it hangs.
 *
 * Deliberately NOT a metastore. Nothing here resolves an address for the engine or
 * decides who may read it — Spark, Glue or Unity Catalog still do that at runtime.
 * This only groups rows on a screen.
 */

/** What the thing is, once the format is known. */
export type AssetKind = 'table' | 'view' | 'directory' | 'topic' | 'index' | 'collection'

/** What the top of the tree is. Decides the icon and how the label reads. */
export type RootKind = 'bucket' | 'catalog' | 'stream' | 'session'

export interface CatalogAsset {
  /** The dataset key it was derived from — the join back to the lineage. */
  key: string
  kind: AssetKind
  root: string
  rootLabel: string
  rootKind: RootKind
  /** URI scheme when the address had one (`s3`, `gs`, `abfss`, `jdbc:postgresql`). */
  scheme?: string
  /** Levels between the root and the leaf: databases, schemas, folders. */
  namespace: string[]
  /** The leaf: the table, the topic, the directory a person would name. */
  name: string
}

/**
 * Format to kind. A format missing from here is a directory, which is the right
 * guess for a file format nobody has classified yet.
 */
const KIND_BY_FORMAT: Record<string, AssetKind> = {
  delta: 'table',
  iceberg: 'table',
  hudi: 'table',
  bigquery: 'table',
  snowflake: 'table',
  redshift: 'table',
  dynamodb: 'table',
  cassandra: 'table',
  jdbc: 'table',
  postgresql: 'table',
  mysql: 'table',
  mariadb: 'table',
  sqlserver: 'table',
  oracle: 'table',
  view: 'view',
  kafka: 'topic',
  elasticsearch: 'index',
  opensearch: 'index',
  mongodb: 'collection',
  documentdb: 'collection',
}

/**
 * When one dataset is named by two formats, the more specific one wins. A Delta
 * table read back as parquet is still a table — the parquet reader is only walking
 * its files.
 */
const KIND_RANK: Record<AssetKind, number> = {
  table: 5,
  view: 4,
  topic: 3,
  index: 2,
  collection: 2,
  directory: 1,
}

export function kindOfFormat(format: string): AssetKind {
  return KIND_BY_FORMAT[format] ?? 'directory'
}

export function kindOfFormats(formats: readonly string[]): AssetKind {
  let winner: AssetKind = 'directory'
  for (const format of formats) {
    const kind = kindOfFormat(format)
    if (KIND_RANK[kind] > KIND_RANK[winner]) winner = kind
  }
  return winner
}

/** Schemes whose root is a bucket or a filesystem, not a server. */
const OBJECT_STORE_SCHEMES = new Set([
  'abfs',
  'abfss',
  'adl',
  'file',
  'gs',
  'hdfs',
  'oss',
  's3',
  's3a',
  's3n',
  'wasb',
  'wasbs',
])

const URI = /^([a-z][a-z0-9+.-]*):\/\/([^/]*)(?:\/(.*))?$/i

const JDBC_PREFIX = 'jdbc:'

function segmentsOf(rest: string): string[] {
  return rest.split('/').filter((segment) => segment !== '')
}

function split(segments: string[]): { namespace: string[]; name: string } {
  if (segments.length === 0) return { namespace: [], name: '' }
  return { namespace: segments.slice(0, -1), name: segments[segments.length - 1] }
}

/** Everything about an asset except what the dataset itself supplies. */
type Placement = Omit<CatalogAsset, 'key' | 'kind'>

/**
 * `s3://warehouse/bronze/orders` and `jdbc:postgresql://db:5432/app`.
 *
 * The JDBC prefix is peeled first: `jdbc:postgresql://…` is two schemes stacked,
 * which no URI parser accepts, and the half that names the driver belongs with the
 * root rather than with the table.
 */
function fromUri(address: string): Placement | null {
  const jdbc = address.startsWith(JDBC_PREFIX)
  const match = URI.exec(jdbc ? address.slice(JDBC_PREFIX.length) : address)
  if (!match) return null

  const [, scheme, authority, rest = ''] = match
  const prefix = jdbc ? `${JDBC_PREFIX}${scheme}` : scheme
  const segments = segmentsOf(rest)
  // A bare `s3://bucket` names the bucket itself: there is no leaf below it.
  const leaf = segments.length === 0 ? { namespace: [], name: authority } : split(segments)

  return {
    root: `${prefix}://${authority}`,
    rootLabel: authority || prefix,
    rootKind: !jdbc && OBJECT_STORE_SCHEMES.has(scheme.toLowerCase()) ? 'bucket' : 'catalog',
    scheme: prefix,
    namespace: leaf.namespace,
    name: leaf.name,
  }
}

/** `analytics.gold.revenue`, `sales.orders`, `orders`. */
function fromIdentifier(address: string): Placement {
  const parts = address.split('.').filter((part) => part !== '')
  if (parts.length < 2) {
    // A table named with nothing in front of it is what every metastore calls
    // `default`. Saying so puts it in the tree instead of loose at the top.
    return {
      root: 'default',
      rootLabel: 'default',
      rootKind: 'catalog',
      namespace: [],
      name: parts[0] ?? address,
    }
  }
  const leaf = split(parts.slice(1))
  return {
    root: parts[0],
    rootLabel: parts[0],
    rootKind: 'catalog',
    namespace: leaf.namespace,
    name: leaf.name,
  }
}

/** `/lake/gold/revenue`, `data/orders`. */
function fromPath(address: string): Placement {
  const segments = segmentsOf(address)
  const absolute = address.startsWith('/')
  if (segments.length === 0) {
    return { root: '/', rootLabel: '/', rootKind: 'bucket', namespace: [], name: address }
  }
  if (segments.length === 1) {
    // `/orders` — the leaf hangs straight off the filesystem root.
    return {
      root: absolute ? '/' : '.',
      rootLabel: absolute ? '/' : '.',
      rootKind: 'bucket',
      namespace: [],
      name: segments[0],
    }
  }
  const leaf = split(segments.slice(1))
  return {
    root: absolute ? `/${segments[0]}` : segments[0],
    rootLabel: segments[0],
    rootKind: 'bucket',
    namespace: leaf.namespace,
    name: leaf.name,
  }
}

/**
 * Reads one address into a place in the tree.
 *
 * A dotted address is only read as `catalog.database.table` when the format says
 * table. `orders.csv` is a file, not a table called `csv` in a database called
 * `orders`, and nothing in the string alone tells the two apart.
 */
export function describeAsset(key: string, formats: readonly string[]): CatalogAsset {
  const kind = kindOfFormats(formats)
  const address = key.trim()

  if (kind === 'view') {
    return {
      key,
      kind,
      root: 'session',
      rootLabel: 'session',
      rootKind: 'session',
      namespace: [],
      name: address,
    }
  }

  if (kind === 'topic' && !address.includes('/')) {
    // Dots belong to the topic name (`orders.v1`); they are never a hierarchy.
    return {
      key,
      kind,
      root: 'kafka',
      rootLabel: 'kafka',
      rootKind: 'stream',
      namespace: [],
      name: address,
    }
  }

  const uri = fromUri(address)
  if (uri) return { key, kind, ...uri }

  if (kind === 'table' && !address.includes('/')) {
    return { key, kind, ...fromIdentifier(address) }
  }

  return { key, kind, ...fromPath(address) }
}

/**
 * Which of the three tiers a node sits in.
 *
 * Databricks says catalog / schema / table and Athena says catalog / database /
 * table; they are the same three levels with two names for the middle one. A
 * lake has the same shape under different words — a bucket holds top-level
 * prefixes that hold datasets — so the tiers are assigned by DEPTH and the words
 * change with the root: a bucket is the catalog tier, its first prefix is the
 * schema tier, and anything deeper is a plain folder that no metastore would
 * have a name for.
 */
export type NodeTier = 'catalog' | 'schema' | 'folder'

/** One level of the browser tree: a catalog, a schema/database, a folder. */
export interface CatalogNode {
  /** Full path from the root, joined — stable across renders, unique in the tree. */
  id: string
  label: string
  /** Set on the top level only; the levels below a root are plain namespaces. */
  rootKind?: RootKind
  /** Which of the three metastore tiers this level is. */
  tier: NodeTier
  /** 0 for a root, 1 for a schema, and up from there. */
  depth: number
  children: CatalogNode[]
  /** Assets sitting at this level, sorted by name. */
  assets: CatalogAsset[]
  /** Assets in this node and everything under it. */
  count: number
}

function tierOf(depth: number): NodeTier {
  if (depth === 0) return 'catalog'
  if (depth === 1) return 'schema'
  return 'folder'
}

function emptyNode(id: string, label: string, depth: number, rootKind?: RootKind): CatalogNode {
  return {
    id,
    label,
    rootKind,
    tier: tierOf(depth),
    depth,
    children: [],
    assets: [],
    count: 0,
  }
}

function childOf(parent: CatalogNode, label: string): CatalogNode {
  const id = `${parent.id}/${label}`
  const existing = parent.children.find((child) => child.id === id)
  if (existing) return existing
  const created = emptyNode(id, label, parent.depth + 1)
  parent.children.push(created)
  return created
}

function sortNode(node: CatalogNode): CatalogNode {
  node.children.sort((a, b) => a.label.localeCompare(b.label))
  node.children.forEach(sortNode)
  node.assets.sort((a, b) => a.name.localeCompare(b.name))
  node.count = node.assets.length + node.children.reduce((total, child) => total + child.count, 0)
  return node
}

/** Buckets first, then catalogs, then the streams and the views nobody stores. */
const ROOT_ORDER: Record<RootKind, number> = { bucket: 0, catalog: 1, stream: 2, session: 3 }

/**
 * Groups assets into the tree the browser draws: roots at the top, namespaces
 * under them, leaves at the bottom. Roots of the same kind stay together, so a
 * lake and a warehouse do not interleave alphabetically.
 */
export function buildNamespaceTree(assets: readonly CatalogAsset[]): CatalogNode[] {
  const roots = new Map<string, CatalogNode>()

  for (const asset of assets) {
    let node = roots.get(asset.root)
    if (!node) {
      node = emptyNode(asset.root, asset.rootLabel, 0, asset.rootKind)
      roots.set(asset.root, node)
    }
    for (const segment of asset.namespace) node = childOf(node, segment)
    node.assets.push(asset)
  }

  return [...roots.values()].map(sortNode).sort((a, b) => {
    const order = ROOT_ORDER[a.rootKind ?? 'bucket'] - ROOT_ORDER[b.rootKind ?? 'bucket']
    return order !== 0 ? order : a.label.localeCompare(b.label)
  })
}

/** Every node id from the root down to the node holding this asset. */
export function ancestorIds(asset: CatalogAsset): string[] {
  const ids: string[] = [asset.root]
  let current = asset.root
  for (const segment of asset.namespace) {
    current = `${current}/${segment}`
    ids.push(current)
  }
  return ids
}
