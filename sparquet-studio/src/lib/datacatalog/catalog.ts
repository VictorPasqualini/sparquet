/**
 * The data catalog: what a dataset IS, on top of what lineage already knows.
 *
 * Lineage is derived — addresses, formats, who writes, who reads — and it costs
 * nothing because every Job already declares it. None of it says what the data
 * means, who to ask about it, or whether it may leave the company. That part
 * cannot be derived from any pipeline, so it is the only part stored: one
 * annotation per dataset address, keyed by the SAME normalized address lineage
 * joins Jobs on (`datasetKey`). The address is the join key; there is no id to
 * keep in sync, and a dataset that no Job mentions any more keeps its annotation
 * until someone deletes it — a rename should not silently drop the description.
 *
 * Not to be confused with `src/catalog/`, which is the Studio's node palette
 * (transformations, formats, validators). This one is about the user's data.
 */

import type { LineageDataset, LineageIndex } from '@/lib/lineage'
import { normalizeTags } from '@/lib/tags'

/** How far the data may travel. Ordered from the least to the most restricted. */
export const CLASSIFICATIONS = ['public', 'internal', 'confidential', 'restricted'] as const

export type DataClassification = (typeof CLASSIFICATIONS)[number]

/** How restricted each classification is. Higher wins when two meet. */
export const CLASSIFICATION_RANK: Record<DataClassification, number> = {
  public: 1,
  internal: 2,
  confidential: 3,
  restricted: 4,
}

/**
 * What one column IS, on top of what the schema already says about it.
 *
 * A column is the unit sensitivity actually has. A table is called
 * `restricted` because one column in it holds a document number, and then the
 * whole table is closed to everybody who only ever needed the order date. Saying
 * it column by column is what lets a rule open the table and still close the
 * column — and it is the same shape the annotation already has for a dataset, so
 * it inherits the storage, the sanitizing and the `tag:` scopes without a second
 * mechanism.
 *
 * Only what cannot be derived is stored. The name, the type and where the value
 * came from are already in the schema and in the column graph; this record adds
 * the meaning, the sensitivity and the tags, which no pipeline can state.
 */
export interface ColumnAnnotation {
  /** The column name as the schema spells it. The key within the dataset. */
  column: string
  description: string
  /** Empty means nobody has classified it, which is not the same as public. */
  classification: DataClassification | ''
  tags: string[]
}

/** Column annotations of one dataset, by column name. */
export type ColumnAnnotations = Record<string, ColumnAnnotation>

export interface DatasetAnnotation {
  /** Normalized dataset address — the join key with lineage. */
  key: string
  /** What the dataset is, in the words of whoever knows. */
  description: string
  /** Who to ask. A name, a team, an email — the Studio does not resolve it. */
  owner: string
  /** Business domain: `finance`, `sales`, `crm`. Groups the catalog. */
  domain: string
  /** Empty means nobody has classified it yet, which is not the same as public. */
  classification: DataClassification | ''
  tags: string[]
  /**
   * The connection secret that opens this dataset, by name.
   *
   * A name and nothing else: the fields and their values stay on the runner, and
   * this annotation travels to the browser, to `.studio/meta.json` and into what
   * the AI is shown. What it buys is the answer to "which credential does this
   * table need", which today is only discoverable by reading every Job that
   * touches it. Empty means nobody has said — not that the dataset needs none.
   */
  connection: string
  /**
   * What each column is, for the columns somebody has said something about.
   *
   * Sparse on purpose: a dataset with four hundred columns and one document
   * number holds one entry here. The columns themselves come from the schema,
   * which is derived; this map only carries what a person wrote.
   */
  columns: ColumnAnnotations
  updatedAt: number
}

/** Everything the user has written down, addressed by dataset key. */
export type CatalogAnnotations = Record<string, DatasetAnnotation>

export interface CatalogEntry {
  dataset: LineageDataset
  annotation: DatasetAnnotation | null
  /** A description is the minimum that makes a catalog worth reading. */
  documented: boolean
  /** Somebody to ask when it breaks. */
  owned: boolean
}

export interface CatalogStats {
  total: number
  documented: number
  owned: number
  classified: number
  /** Columns anybody has classified, across every dataset in use. */
  columnsClassified: number
  /** 0–1. What fraction of the datasets in use has a description. */
  coverage: number
}

export const MAX_DESCRIPTION = 500
export const MAX_OWNER = 120
export const MAX_DOMAIN = 60
/** A secret name, which the runner bounds the same way. */
export const MAX_CONNECTION = 120
export const MAX_COLUMN_DESCRIPTION = 300
/** A column name, bounded the way the annotation's other free text is. */
export const MAX_COLUMN_NAME = 200

export function emptyAnnotation(key: string): DatasetAnnotation {
  return {
    key,
    description: '',
    owner: '',
    domain: '',
    classification: '',
    tags: [],
    connection: '',
    columns: {},
    updatedAt: 0,
  }
}

export function emptyColumnAnnotation(column: string): ColumnAnnotation {
  return { column, description: '', classification: '', tags: [] }
}

/**
 * True when a column annotation says nothing.
 *
 * Emptied by hand it is deleted rather than kept blank, for the reason the
 * dataset annotation is: a map full of empty records counts as documented in
 * every statistic that only checks whether a key exists.
 */
export function isColumnBlank(annotation: ColumnAnnotation): boolean {
  return (
    annotation.description.trim() === '' &&
    annotation.classification === '' &&
    annotation.tags.length === 0
  )
}

/**
 * True when the annotation says nothing at all.
 *
 * An entry emptied by hand is deleted rather than stored blank: a catalog full
 * of empty records reads as "documented" in every count that only checks whether
 * a row exists.
 */
export function isBlank(annotation: DatasetAnnotation): boolean {
  return (
    annotation.description.trim() === '' &&
    annotation.owner.trim() === '' &&
    annotation.domain.trim() === '' &&
    annotation.classification === '' &&
    annotation.tags.length === 0 &&
    annotation.connection.trim() === '' &&
    Object.keys(annotation.columns).length === 0
  )
}

function clamp(value: string, max: number): string {
  return value.length > max ? value.slice(0, max) : value
}

/** One column annotation, trimmed and bounded, whatever the caller passed in. */
export function normalizeColumnAnnotation(
  column: string,
  patch: Partial<ColumnAnnotation>,
  base: ColumnAnnotation = emptyColumnAnnotation(column),
): ColumnAnnotation {
  const merged = { ...base, ...patch, column }
  return {
    column: clamp(String(merged.column ?? '').trim(), MAX_COLUMN_NAME),
    description: clamp(String(merged.description ?? '').trim(), MAX_COLUMN_DESCRIPTION),
    classification: isClassification(merged.classification) ? merged.classification : '',
    tags: normalizeTags(merged.tags),
  }
}

/**
 * A map of column annotations, whatever came back from storage.
 *
 * Keyed by the column name lower-cased, because a schema that calls it
 * `CustomerId` and a rule written against `customerid` are talking about the
 * same column, and an access decision that misses on a capital letter fails
 * open. The spelling somebody typed is kept inside the record.
 */
export function normalizeColumns(value: unknown): ColumnAnnotations {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return {}
  const out: ColumnAnnotations = {}
  for (const [name, entry] of Object.entries(value as Record<string, unknown>)) {
    if (typeof entry !== 'object' || entry === null || Array.isArray(entry)) continue
    const spelled = String((entry as { column?: unknown }).column ?? name)
    const annotation = normalizeColumnAnnotation(spelled, entry as Partial<ColumnAnnotation>)
    if (!annotation.column || isColumnBlank(annotation)) continue
    out[columnKey(annotation.column)] = annotation
  }
  return out
}

/** The one spelling of a column name every lookup and every rule agrees on. */
export function columnKey(column: string): string {
  return (column ?? '').trim().toLowerCase()
}

/** One annotation, trimmed and bounded, whatever the caller passed in. */
export function normalizeAnnotation(
  key: string,
  patch: Partial<DatasetAnnotation>,
  base: DatasetAnnotation = emptyAnnotation(key),
): DatasetAnnotation {
  const merged = { ...base, ...patch, key }
  return {
    key,
    description: clamp(String(merged.description ?? '').trim(), MAX_DESCRIPTION),
    owner: clamp(String(merged.owner ?? '').trim(), MAX_OWNER),
    domain: clamp(String(merged.domain ?? '').trim(), MAX_DOMAIN),
    classification: isClassification(merged.classification) ? merged.classification : '',
    tags: normalizeTags(merged.tags),
    connection: clamp(String(merged.connection ?? '').trim(), MAX_CONNECTION),
    columns: normalizeColumns(merged.columns),
    updatedAt: Date.now(),
  }
}

function isClassification(value: unknown): value is DataClassification {
  return CLASSIFICATIONS.includes(value as DataClassification)
}

/**
 * The map with one dataset's annotation written, or removed when it goes blank.
 *
 * Returns a new map; the caller decides when to persist it.
 */
export function withAnnotation(
  annotations: CatalogAnnotations,
  key: string,
  patch: Partial<DatasetAnnotation>,
): CatalogAnnotations {
  const next = normalizeAnnotation(key, patch, annotations[key] ?? emptyAnnotation(key))
  const copy = { ...annotations }
  if (isBlank(next)) delete copy[key]
  else copy[key] = next
  return copy
}

/**
 * The map with one column of one dataset written, or removed when it goes blank.
 *
 * The column lives inside its dataset's annotation rather than in a map of its
 * own, because the dataset address is the join key the whole catalog is built
 * on. A column has no address that survives its table.
 */
export function withColumnAnnotation(
  annotations: CatalogAnnotations,
  key: string,
  column: string,
  patch: Partial<ColumnAnnotation>,
): CatalogAnnotations {
  const name = columnKey(column)
  if (!name) return annotations
  const base = annotations[key] ?? emptyAnnotation(key)
  const next = normalizeColumnAnnotation(
    (patch.column ?? base.columns[name]?.column ?? column).trim(),
    patch,
    base.columns[name] ?? emptyColumnAnnotation(column),
  )
  const columns = { ...base.columns }
  if (isColumnBlank(next)) delete columns[name]
  else columns[name] = next
  return withAnnotation(annotations, key, { columns })
}

/** What has been written about one column, or null. Case does not matter. */
export function columnAnnotationOf(
  annotation: DatasetAnnotation | null | undefined,
  column: string,
): ColumnAnnotation | null {
  return annotation?.columns[columnKey(column)] ?? null
}

/** Every annotated column of a dataset, in the order a table reads. */
export function columnAnnotationsOf(
  annotation: DatasetAnnotation | null | undefined,
): ColumnAnnotation[] {
  return Object.values(annotation?.columns ?? {}).sort((a, b) =>
    a.column.localeCompare(b.column),
  )
}

/**
 * The classification the dataset actually has, columns included.
 *
 * A table is exactly as restricted as its most restricted column. Leaving the
 * two numbers to be reconciled by whoever reads the screen is how a table with a
 * document number in it ends up filed as `internal` — so the roll-up is computed
 * here, once, and every surface asks this rather than reading the field.
 *
 * It never lowers what somebody wrote on the dataset itself: a table can be
 * restricted for a reason no single column explains.
 */
export function effectiveClassification(
  annotation: DatasetAnnotation | null | undefined,
): DataClassification | '' {
  let best: DataClassification | '' = annotation?.classification ?? ''
  for (const column of Object.values(annotation?.columns ?? {})) {
    if (!column.classification) continue
    if (!best || CLASSIFICATION_RANK[column.classification] > CLASSIFICATION_RANK[best]) {
      best = column.classification
    }
  }
  return best
}

/**
 * True when a column is more restricted than the table it sits in.
 *
 * The one case worth pointing at on screen: the dataset says `internal`, the
 * column says `restricted`, and anybody reading the dataset's badge alone would
 * get it wrong.
 */
export function columnRaisesClassification(
  annotation: DatasetAnnotation | null | undefined,
  column: ColumnAnnotation,
): boolean {
  if (!column.classification) return false
  const dataset = annotation?.classification ?? ''
  if (!dataset) return true
  return CLASSIFICATION_RANK[column.classification] > CLASSIFICATION_RANK[dataset]
}

export function withoutAnnotation(
  annotations: CatalogAnnotations,
  key: string,
): CatalogAnnotations {
  if (!(key in annotations)) return annotations
  const copy = { ...annotations }
  delete copy[key]
  return copy
}

/**
 * Whatever came back from storage, as annotations.
 *
 * Storage is a file a user can edit and a browser database that survives
 * upgrades, so nothing here may assume the shape it wrote is the shape it reads.
 */
export function sanitizeAnnotations(value: unknown): CatalogAnnotations {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return {}
  const result: CatalogAnnotations = {}
  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    if (typeof entry !== 'object' || entry === null || Array.isArray(entry)) continue
    const annotation = normalizeAnnotation(key, entry as Partial<DatasetAnnotation>)
    if (isBlank(annotation)) continue
    const stamped = (entry as { updatedAt?: unknown }).updatedAt
    result[key] = { ...annotation, updatedAt: typeof stamped === 'number' ? stamped : 0 }
  }
  return result
}

/** The datasets in use, each with whatever has been written about it. */
export function buildCatalog(
  index: LineageIndex,
  annotations: CatalogAnnotations,
): CatalogEntry[] {
  return index.datasets.map((dataset) => {
    const annotation = annotations[dataset.key] ?? null
    return {
      dataset,
      annotation,
      documented: (annotation?.description ?? '') !== '',
      owned: (annotation?.owner ?? '') !== '',
    }
  })
}

export function catalogStats(entries: readonly CatalogEntry[]): CatalogStats {
  const documented = entries.filter((entry) => entry.documented).length
  const owned = entries.filter((entry) => entry.owned).length
  const classified = entries.filter(
    (entry) => (entry.annotation?.classification ?? '') !== '',
  ).length
  const columnsClassified = entries.reduce(
    (count, entry) =>
      count +
      Object.values(entry.annotation?.columns ?? {}).filter((column) => column.classification)
        .length,
    0,
  )
  return {
    total: entries.length,
    documented,
    owned,
    classified,
    columnsClassified,
    coverage: entries.length === 0 ? 0 : documented / entries.length,
  }
}

/**
 * Annotations whose dataset no Job mentions any more.
 *
 * Kept, not deleted: the usual cause is an address that changed, and the text is
 * worth more than the tidiness. Surfaced so somebody can move or drop it.
 */
export function orphanAnnotations(
  index: LineageIndex,
  annotations: CatalogAnnotations,
): DatasetAnnotation[] {
  const known = new Set(index.datasets.map((dataset) => dataset.key))
  return Object.values(annotations)
    .filter((annotation) => !known.has(annotation.key))
    .sort((a, b) => a.key.localeCompare(b.key))
}

/** Every domain already in use, for the picker that offers them. */
export function knownDomains(annotations: CatalogAnnotations): string[] {
  const domains = new Set<string>()
  for (const annotation of Object.values(annotations)) {
    if (annotation.domain) domains.add(annotation.domain)
  }
  return [...domains].sort()
}
