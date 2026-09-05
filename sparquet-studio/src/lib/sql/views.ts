/**
 * Catalog addresses turned into SQL view names.
 *
 * The SQL editor never writes a path: the runner opens each dataset with the
 * framework's own reader and registers it as a temp view, so the query names an
 * alias. The alias has to be derived from the address — the same address every
 * time, so a saved query keeps working — short enough to type, and unique inside
 * one query even when two buckets end with the same folder name.
 */

/** Everything Spark accepts in an unquoted view name is `[A-Za-z0-9_]`. */
const NOT_NAME = /[^A-Za-z0-9_]+/g

/** A scheme (`s3:`, `abfss:`) or a drive letter (`E:`) names no folder. */
const SCHEME = /^[A-Za-z][A-Za-z0-9+.-]*:$/

/** Spark's own limit is longer, but a name nobody can read is not an alias. */
const MAX_ALIAS = 48

/**
 * The last two segments of an address, joined: `/lake/silver/orders` becomes
 * `silver_orders`. Two segments rather than one because `orders` alone collides
 * between layers, and the layer is what the reader is looking at.
 */
export function viewAlias(key: string, taken: ReadonlySet<string> = new Set()): string {
  const segments = key
    .split(/[/\\]/)
    .map((segment) => segment.trim())
    .filter((segment) => segment.length > 0 && !SCHEME.test(segment))

  const tail = segments.slice(-2).join('_') || key
  let base = tail.replace(NOT_NAME, '_').replace(/^_+|_+$/g, '').toLowerCase()
  if (base.length === 0) base = 'dataset'
  if (/^[0-9]/.test(base)) base = `_${base}`
  base = base.slice(0, MAX_ALIAS)

  if (!taken.has(base)) return base
  for (let suffix = 2; suffix < 1000; suffix += 1) {
    const candidate = `${base.slice(0, MAX_ALIAS - 4)}_${suffix}`
    if (!taken.has(candidate)) return candidate
  }
  return `${base.slice(0, MAX_ALIAS - 8)}_${Date.now().toString(36)}`
}

/**
 * Which of `aliases` the SQL actually names.
 *
 * Only those datasets are opened. Attaching the whole catalog to every query
 * would make the runner read dozens of tables to answer a query about one, and
 * a query that names a dataset the runner cannot open would fail for a reason
 * that has nothing to do with what was asked.
 */
export function mentionedAliases(sql: string, aliases: readonly string[]): string[] {
  if (!sql.trim()) return []
  return aliases.filter((alias) => new RegExp(`\\b${alias}\\b`, 'i').test(sql))
}
