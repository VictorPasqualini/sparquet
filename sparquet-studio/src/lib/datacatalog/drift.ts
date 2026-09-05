/**
 * What the canvas claims about a dataset against what the storage actually has.
 *
 * The derived schema is a reading of the Jobs: it says what a chain writes, or
 * what a chain demands of what it reads. The probed schema comes from the runner
 * opening the dataset and asking Spark. They disagree for real reasons — a Job
 * changed and never ran, a column was dropped by hand, a cast never landed — and
 * naming the disagreement is the whole point of this module.
 *
 * Types are compared after normalization, because Spark answers with one spelling
 * (`bigint`) while a pipeline is usually written with another (`long`). Only
 * spellings of the same type are folded together; `int` and `long` stay
 * different, since that difference is exactly the kind of drift worth seeing.
 */

import type { CatalogField, DatasetSchema } from './schema'

/** One Spark type written two ways: the same type, and it must not read as drift. */
const TYPE_ALIASES: Record<string, string> = {
  bigint: 'long',
  integer: 'int',
  smallint: 'short',
  tinyint: 'byte',
  real: 'float',
  numeric: 'decimal',
  bool: 'boolean',
  str: 'string',
  text: 'string',
  datetime: 'timestamp',
}

export type DriftStatus =
  /** Same name, same type. */
  | 'match'
  /** Same name, different type. */
  | 'type'
  /** The canvas states a column the dataset does not have. */
  | 'missing'
  /** The dataset has a column the canvas never mentions. */
  | 'extra'
  /** Same name, but the canvas never stated a type, so there is nothing to compare. */
  | 'unstated'

export interface DriftRow {
  name: string
  /** The type the canvas states, or null when it states none. */
  derived: string | null
  /** The type the runner read, or null when the dataset has no such column. */
  actual: string | null
  status: DriftStatus
  /** Nullability, as the storage reports it. */
  nullable: boolean | null
  /** How the canvas came to state the column, when it does. */
  origin: CatalogField['origin'] | null
}

export interface SchemaDrift {
  /** Every column of either side: the canvas order first, then what only exists. */
  rows: DriftRow[]
  match: number
  type: number
  missing: number
  extra: number
  unstated: number
  /**
   * True when the derived schema is the whole schema. While it is not, an `extra`
   * column is the expected state of the world rather than a finding.
   */
  complete: boolean
  /** True when something worth acting on was found. */
  drifted: boolean
}

export interface ProbedField {
  name: string
  type: string
  nullable: boolean
}

/**
 * One type reduced to the shape a comparison can use: lowercase, no whitespace,
 * and every alias spelled the one way.
 *
 * Parameters are kept — `decimal(18,2)` and `decimal(10,0)` are different types
 * and a pipeline that swaps one for the other has drifted.
 */
export function normalizeType(type: string): string {
  const trimmed = type.trim().toLowerCase().replace(/\s+/g, '')
  const head = trimmed.replace(/[(<].*$/, '')
  const alias = TYPE_ALIASES[head]
  return alias ? alias + trimmed.slice(head.length) : trimmed
}

function sameType(derived: string, actual: string): boolean {
  return normalizeType(derived) === normalizeType(actual)
}

/**
 * Compares the derived schema with a probed one, column by column.
 *
 * Matching is by name and case-insensitive: Spark itself resolves columns
 * case-insensitively by default, so treating `Amount` and `amount` as two
 * columns would report drift nobody has.
 */
export function compareSchema(
  derived: DatasetSchema | null,
  probed: readonly ProbedField[],
): SchemaDrift {
  const actual = new Map<string, ProbedField>()
  for (const field of probed) actual.set(field.name.toLowerCase(), field)

  const rows: DriftRow[] = []
  const claimed = new Set<string>()

  for (const field of derived?.fields ?? []) {
    const key = field.name.toLowerCase()
    if (claimed.has(key)) continue
    claimed.add(key)
    const found = actual.get(key)
    const status: DriftStatus = !found
      ? 'missing'
      : !field.type
        ? 'unstated'
        : sameType(field.type, found.type)
          ? 'match'
          : 'type'
    rows.push({
      name: field.name,
      derived: field.type,
      actual: found?.type ?? null,
      status,
      nullable: found ? found.nullable : null,
      origin: field.origin,
    })
  }

  for (const field of probed) {
    if (claimed.has(field.name.toLowerCase())) continue
    claimed.add(field.name.toLowerCase())
    rows.push({
      name: field.name,
      derived: null,
      actual: field.type,
      status: 'extra',
      nullable: field.nullable,
      origin: null,
    })
  }

  const count = (status: DriftStatus) => rows.filter((row) => row.status === status).length
  const complete = derived?.confidence === 'complete'
  const extra = count('extra')
  const mismatch = count('type')
  const missing = count('missing')

  return {
    rows,
    match: count('match'),
    type: mismatch,
    missing,
    extra,
    unstated: count('unstated'),
    complete,
    // An extra column only means drift when the canvas claimed to know the whole
    // schema; on a partial schema it is simply a column no Job talks about.
    drifted: mismatch > 0 || missing > 0 || (complete && extra > 0),
  }
}
