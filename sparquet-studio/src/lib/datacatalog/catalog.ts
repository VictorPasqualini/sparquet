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
  /** 0–1. What fraction of the datasets in use has a description. */
  coverage: number
}

export const MAX_DESCRIPTION = 500
export const MAX_OWNER = 120
export const MAX_DOMAIN = 60

export function emptyAnnotation(key: string): DatasetAnnotation {
  return {
    key,
    description: '',
    owner: '',
    domain: '',
    classification: '',
    tags: [],
    updatedAt: 0,
  }
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
    annotation.tags.length === 0
  )
}

function clamp(value: string, max: number): string {
  return value.length > max ? value.slice(0, max) : value
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
  return {
    total: entries.length,
    documented,
    owned,
    classified,
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
