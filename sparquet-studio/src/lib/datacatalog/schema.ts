/**
 * The columns of a dataset, derived from the canvas.
 *
 * There is no metastore to ask, and the Studio never opens the data, so the only
 * honest source is what the Jobs themselves state: a `cast` names a column AND its
 * type, a `with_column` creates one, a `group_by` produces exactly its keys plus
 * its aggregates, a destination with a column list projects the final shape. Put
 * end to end along a chain, that is a schema — sometimes the whole one, more often
 * a part of it, and the difference is reported rather than hidden.
 *
 * Three confidences, and they mean different things to whoever reads the screen:
 *   complete — the chain proves the full list, in order. Trust it as the schema.
 *   partial  — these columns provably exist; the reader supplies the rest at
 *              runtime (a CSV with `inferSchema`, a Parquet folder, a table).
 *   unknown  — nothing in the canvas says anything about columns.
 *
 * A dataset that is WRITTEN is described by the chain that writes it — that chain
 * decides its shape. A dataset that is only READ is described by what the Jobs
 * demand of it: every column a cast, a select, a join key or a validation rule
 * names must be in there, or the run fails. Written beats read, complete beats
 * partial, and a dataset touched by several Jobs keeps the strongest description.
 *
 * The same walk also carries PROVENANCE: while it tracks names it keeps, for each
 * column in flight, the source columns it came from. At the destination that turns
 * into column-level lineage — `revenue` came from `/lake/silver/orders.amount` —
 * and into the list of steps that name a column, which is what impact analysis
 * reads. An expression is opaque Spark SQL, so the identifiers inside it are
 * matched by shape and the result is an approximation that says so: see
 * `identifiers`.
 */

import { chainToSink, isCompilable, sideParent } from '@/lib/compiler'
import { datasetKey } from '@/lib/lineage'
import type { Job, StudioGraph, StudioNode } from '@/types/studio'

/** Why the canvas is sure the column is there. */
export type FieldOrigin =
  /** Named by a `select`, or by the column list of a destination. */
  | 'projected'
  /** A `cast` gave it a type — which also proves it already existed. */
  | 'cast'
  /** Created here: `with_column`, `struct`. */
  | 'computed'
  /** A `group_by` key. */
  | 'grouped'
  /** A `group_by` aggregate. */
  | 'aggregated'
  /** Came in from the right side of a join. */
  | 'joined'
  /** Added by the validations block — the quarantine's `annotate` column. */
  | 'quality'
  /** Something demands it exists: a join key, a fill_na, a rule. */
  | 'required'

export interface CatalogField {
  name: string
  /** The type the canvas states. `null` when no Job ever says it. */
  type: string | null
  origin: FieldOrigin
  /** The expression, the aggregate, or what asked for it. */
  note?: string
}

export type SchemaConfidence = 'complete' | 'partial' | 'unknown'

export interface SchemaSource {
  jobId: string
  jobName: string
  /** The canvas node the schema was read off, so the screen can point at it. */
  nodeId: string
  /** Whether the Job writes this dataset or only reads it. */
  side: 'written' | 'read'
}

/** One column of one dataset — the address the catalog uses, plus the name. */
export interface ColumnRef {
  key: string
  column: string
}

/** How the value got from the source column to the one the chain writes. */
export type ColumnLinkKind =
  /** Carried through unchanged. */
  | 'copy'
  /** Same column, new type. */
  | 'cast'
  /** Computed by an expression that reads it. */
  | 'expression'
  /** Summed, counted, averaged. */
  | 'aggregate'
  /** A `group_by` key. */
  | 'group key'
  /** Brought in by the right side of a join. */
  | 'join'
  /** Written by the validations block. */
  | 'quality'

/** One edge of the column graph: a source column feeding a written column. */
export interface ColumnLink {
  from: ColumnRef
  to: ColumnRef
  jobId: string
  jobName: string
  /** The destination node that writes `to`, so the screen can point at it. */
  nodeId: string
  kind: ColumnLinkKind
  /** The expression or aggregate, when there was one. */
  note?: string
}

/** A step that names a column of a dataset it reads — what breaks if it changes. */
export interface ColumnUse {
  key: string
  column: string
  jobId: string
  jobName: string
  nodeId: string
  /** The transform type, the validator name, or the destination projection. */
  step: string
  role: FieldOrigin
}

export interface DatasetSchema {
  /** Normalized address — the same key lineage and the annotations use. */
  key: string
  fields: CatalogField[]
  confidence: SchemaConfidence
  source: SchemaSource | null
}

const LINK_KIND: Record<FieldOrigin, ColumnLinkKind> = {
  projected: 'copy',
  cast: 'cast',
  computed: 'expression',
  grouped: 'group key',
  aggregated: 'aggregate',
  joined: 'join',
  quality: 'quality',
  required: 'copy',
}

/* ------------------------------------------------------------------ ranking */

const CONFIDENCE_RANK: Record<SchemaConfidence, number> = {
  complete: 2,
  partial: 1,
  unknown: 0,
}

/**
 * A description that says more replaces one that says less. Written outranks read
 * at equal confidence: the writing chain decides the shape, the reading one only
 * proves that some of it is there.
 */
function score(schema: DatasetSchema): number {
  const side = schema.source?.side === 'written' ? 1 : 0
  return CONFIDENCE_RANK[schema.confidence] * 10 + side * 2 + (schema.fields.length > 0 ? 1 : 0)
}

/* -------------------------------------------------------------- small parse */

function text(value: unknown): string {
  return typeof value === 'string' ? value.trim() : ''
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function names(value: unknown): string[] {
  if (typeof value === 'string') return value.trim() === '' ? [] : [value.trim()]
  if (!Array.isArray(value)) return []
  return value.filter((entry): entry is string => typeof entry === 'string' && entry.trim() !== '')
    .map((entry) => entry.trim())
}

/**
 * Output name of one `select` / `agg` entry, or null when it cannot be derived.
 * `sum(amount) as revenue` is `revenue`; a bare `country` is itself; anything
 * else — a function call with no alias — has a name only Spark knows.
 */
function outputName(entry: string): string | null {
  const value = entry.trim()
  if (!value) return null
  const aliased = /\s+as\s+(`[^`]+`|[A-Za-z_]\w*)$/i.exec(value)
  if (aliased) return aliased[1].replace(/`/g, '')
  if (/^[A-Za-z_]\w*$/.test(value)) return value
  const quoted = /^`([^`]+)`$/.exec(value)
  if (quoted) return quoted[1]
  return null
}

/**
 * The entry without its `as alias` tail — the part that actually reads columns.
 * Without this, `sum(amount) as revenue` looks like it reads a column called
 * revenue, and the alias turns into a source of itself.
 */
function expressionPart(entry: string): string {
  return entry.replace(/\s+as\s+(`[^`]+`|[A-Za-z_]\w*)$/i, '')
}

/** A `select` entry that is nothing but a column name reads an existing column. */
function referencedName(entry: string): string | null {
  const value = entry.trim()
  if (/^[A-Za-z_]\w*$/.test(value)) return value
  const quoted = /^`([^`]+)`$/.exec(value)
  return quoted ? quoted[1] : null
}

/**
 * Column names an expression appears to read.
 *
 * Nothing here parses Spark SQL — it matches identifiers by shape and throws away
 * what cannot be a column: a function call (`sum(`), a keyword, a type name, and
 * anything inside a string literal. That is enough for the expressions people
 * actually write (`amount * rate`, `sum(amount) as revenue`, `coalesce(a, b)`),
 * and it is why every edge derived from an expression is labelled as such rather
 * than presented as a parse.
 */
const NOT_A_COLUMN = new Set([
  'and', 'or', 'not', 'is', 'null', 'true', 'false', 'case', 'when', 'then',
  'else', 'end', 'as', 'cast', 'distinct', 'select', 'from', 'where', 'in',
  'like', 'rlike', 'between', 'on', 'over', 'partition', 'by', 'order', 'asc',
  'desc', 'interval', 'current_date', 'current_timestamp', 'string', 'int',
  'integer', 'bigint', 'long', 'smallint', 'short', 'tinyint', 'byte', 'double',
  'float', 'real', 'decimal', 'numeric', 'boolean', 'date', 'timestamp',
  'binary', 'array', 'map', 'struct', 'year', 'month', 'day', 'hour', 'minute',
  'second', 'week', 'quarter',
])

function identifiers(expression: string): string[] {
  const withoutLiterals = expression.replace(/'[^']*'/g, ' ').replace(/"[^"]*"/g, ' ')
  const out: string[] = []
  const pattern = /`([^`]+)`|([A-Za-z_]\w*)\s*(\()?/g
  let match = pattern.exec(withoutLiterals)
  while (match !== null) {
    const quoted = match[1]
    const bare = match[2]
    if (quoted) out.push(quoted)
    // `sum(` is a function, `sum` alone is a column called sum.
    else if (bare && !match[3] && !NOT_A_COLUMN.has(bare.toLowerCase())) out.push(bare)
    match = pattern.exec(withoutLiterals)
  }
  return out
}

/** Every string value of a nested field map, which is where struct hides its SQL. */
function expressionsIn(value: unknown): string[] {
  if (typeof value === 'string') return value.trim() ? [value] : []
  if (Array.isArray(value)) return value.flatMap(expressionsIn)
  if (isRecord(value)) return Object.values(value).flatMap(expressionsIn)
  return []
}

/* ----------------------------------------------------------------- tracking */

/**
 * What one chain knows, from its head to wherever the walk stopped.
 *
 * `ordered` is the full schema when the chain proves it and null the moment it
 * stops being provable; `present` survives that loss, because a `cast` still
 * proves its column is in the frame even when nobody knows what else is. They are
 * kept apart on purpose: a partial list rendered as if it were the whole schema
 * is worse than no list at all.
 */
interface Chain {
  ordered: CatalogField[] | null
  present: Map<string, CatalogField>
  /** Types seen anywhere in the chain, so a later projection can carry them. */
  types: Map<string, string>
  /** Columns this chain created — they are NOT in the dataset it reads from. */
  created: Set<string>
  /** Columns the chain demands of its head dataset. */
  upstream: Map<string, CatalogField>
  /** The address this chain reads from: where an untouched column comes from. */
  head: string
  /**
   * Addresses a union stacked onto the head. A union lines up columns by
   * position, so an untouched column comes from every side at once, and naming
   * only the head would hide half of where the values came from.
   */
  merged: string[]
  /** Column in flight -> the source columns behind it, encoded by `ref`. */
  roots: Map<string, Set<string>>
  /**
   * True once a step reshapes the frame in a way nobody here can follow (`sql`).
   * After that a column name no longer proves where its values came from, so the
   * chain stops guessing instead of inventing an edge.
   */
  opaque: boolean
  /** The walk this chain belongs to — a use is recorded where it happens. */
  walk: Walk
  /** The node being applied right now, for the same reason. */
  at: StudioNode | null
}

function newChain(walk: Walk, head: string): Chain {
  return {
    ordered: null,
    present: new Map(),
    types: new Map(),
    created: new Set(),
    upstream: new Map(),
    head,
    merged: [],
    roots: new Map(),
    opaque: false,
    walk,
    at: null,
  }
}

/** Separator no address or column name can contain. */
const REF = '\u0000'

function ref(key: string, column: string): string {
  return `${key}${REF}${column}`
}

function unref(encoded: string): ColumnRef {
  const at = encoded.indexOf(REF)
  return { key: encoded.slice(0, at), column: encoded.slice(at + 1) }
}

/**
 * The source columns behind one column in flight.
 *
 * A column nobody touched still has a root: it came from the head dataset under
 * its own name. That default is exactly what stops holding once a step is opaque.
 */
function rootsOf(chain: Chain, name: string): Set<string> {
  const known = chain.roots.get(name)
  if (known) return known
  if (chain.opaque || !chain.head || chain.created.has(name)) return new Set()
  return new Set([chain.head, ...chain.merged].map((key) => ref(key, name)))
}

function rootsOfExpression(chain: Chain, expression: string): Set<string> {
  const out = new Set<string>()
  for (const name of identifiers(expression)) {
    for (const entry of rootsOf(chain, name)) out.add(entry)
  }
  return out
}

/** What the step being applied is called, in the words of the canvas. */
function stepName(node: StudioNode): string {
  if (node.data.kind === 'transform') return node.data.transform
  if (node.data.kind === 'validation') return node.data.validator
  if (node.data.kind === 'sink') return 'output'
  return node.data.kind
}

/** Records that a step names a column of the dataset this chain reads. */
function noteUse(chain: Chain, column: string, role: FieldOrigin): void {
  if (!chain.head || !chain.at || !column || chain.created.has(column)) return
  chain.walk.uses.push({
    key: chain.head,
    column,
    jobId: chain.walk.job.id,
    jobName: chain.walk.job.name,
    nodeId: chain.at.id,
    step: stepName(chain.at),
    role,
  })
}

/**
 * Every column an expression appears to read is a reference to the head dataset.
 *
 * Recorded as a USE and not as a demand: an identifier matched by shape is good
 * enough to warn whoever changes the column, and not good enough to claim the
 * column is part of the source schema.
 */
function reads(chain: Chain, expression: string, role: FieldOrigin): void {
  for (const name of identifiers(expression)) noteUse(chain, name, role)
}

function field(
  chain: Chain,
  name: string,
  origin: FieldOrigin,
  type?: string | null,
  note?: string,
): CatalogField {
  const known = type ?? chain.types.get(name) ?? null
  return note ? { name, type: known, origin, note } : { name, type: known, origin }
}

/** Records that the head dataset must already contain this column. */
function demand(chain: Chain, name: string, origin: FieldOrigin, note?: string): void {
  if (!name || chain.created.has(name)) return
  // Every mention is a use, even the second one: they are different steps, and
  // each of them breaks on its own when the column changes.
  noteUse(chain, name, origin)
  if (chain.upstream.has(name)) return
  chain.upstream.set(name, field(chain, name, origin, undefined, note))
}

function produce(chain: Chain, entry: CatalogField): void {
  chain.present.set(entry.name, entry)
  if (entry.type) chain.types.set(entry.name, entry.type)
}

/* --------------------------------------------------------------- the walker */

const PASSTHROUGH = new Set([
  'filter',
  'sort',
  'distinct',
  'drop_duplicates',
  'checkpoint',
  'collect',
  'stop_if_empty',
  'debug',
  'sample',
  'limit',
  'repartition',
  'coalesce',
  'cache',
  'persist',
])

/**
 * What a walk carries: the graph it runs on, the Job it belongs to, and the
 * schemas found so far — a join's right-hand source is a dataset of its own, and
 * the demands the sub-chain makes of it belong in the catalog too.
 */
interface Walk {
  graph: StudioGraph
  job: Job
  found: DatasetSchema[]
  /** Column-level edges, filled in at every destination the walk reaches. */
  links: ColumnLink[]
  /** Every step that names a column of a dataset it reads. */
  uses: ColumnUse[]
  depth: number
}

function applyTransform(chain: Chain, node: StudioNode, walk: Walk): void {
  if (node.data.kind !== 'transform') return
  const { transform, params } = node.data

  switch (transform) {
    case 'select': {
      const entries = names(params.columns)
      if (entries.length === 0) {
        chain.ordered = null
        return
      }
      const projected: CatalogField[] = []
      for (const entry of entries) {
        const referenced = referencedName(entry)
        if (referenced) demand(chain, referenced, 'projected')
        const name = outputName(entry)
        if (name === null) {
          // One entry nobody can name makes the whole projection unknowable.
          chain.ordered = null
          return
        }
        if (!referenced) {
          // `amount * rate as total`: a new column, fed by what the entry reads.
          const source = expressionPart(entry)
          reads(chain, source, 'projected')
          chain.roots.set(name, rootsOfExpression(chain, source))
          chain.created.add(name)
        }
        projected.push(field(chain, name, 'projected', undefined, referenced ? undefined : entry))
      }
      chain.ordered = projected
      chain.present = new Map(projected.map((entry) => [entry.name, entry]))
      return
    }

    case 'cast': {
      const map = params.columns
      if (!isRecord(map)) return
      for (const [name, type] of Object.entries(map)) {
        const spark = text(type)
        if (!name || !spark) continue
        chain.types.set(name, spark)
        demand(chain, name, 'cast')
        const entry = field(chain, name, chain.created.has(name) ? 'computed' : 'cast', spark)
        produce(chain, entry)
        if (chain.ordered) {
          chain.ordered = chain.ordered.map((current) =>
            current.name === name ? { ...current, type: spark } : current,
          )
        }
      }
      return
    }

    case 'with_column': {
      const map = params.columns
      // The engine branches on the KEY being present, so an empty map adds nothing.
      if (isRecord(map)) {
        for (const [name, expression] of Object.entries(map)) {
          if (!name) continue
          // Resolved before the name is marked as created, so `x = x + 1` still
          // points at the x that came in.
          const source = text(expression)
          reads(chain, source, 'computed')
          chain.roots.set(name, rootsOfExpression(chain, source))
          chain.created.add(name)
          const entry = field(chain, name, 'computed', null, source || undefined)
          produce(chain, entry)
          if (chain.ordered && !chain.ordered.some((current) => current.name === name)) {
            chain.ordered = [...chain.ordered, entry]
          }
        }
        return
      }
      const single = text(params.column) || text(params.name)
      if (!single) {
        chain.ordered = null
        return
      }
      const source = text(params.expression)
      reads(chain, source, 'computed')
      chain.roots.set(single, rootsOfExpression(chain, source))
      chain.created.add(single)
      const entry = field(chain, single, 'computed', null, source || undefined)
      produce(chain, entry)
      if (chain.ordered && !chain.ordered.some((current) => current.name === single)) {
        chain.ordered = [...chain.ordered, entry]
      }
      return
    }

    case 'struct': {
      const single = text(params.column) || text(params.name)
      if (!single) {
        chain.ordered = null
        return
      }
      const roots = new Set<string>()
      for (const expression of expressionsIn(params.fields)) {
        reads(chain, expression, 'computed')
        for (const root of rootsOfExpression(chain, expression)) roots.add(root)
      }
      chain.roots.set(single, roots)
      chain.created.add(single)
      const entry = field(chain, single, 'computed', 'struct', 'struct(...)')
      produce(chain, entry)
      if (chain.ordered && !chain.ordered.some((current) => current.name === single)) {
        chain.ordered = [...chain.ordered, entry]
      }
      return
    }

    case 'rename': {
      const mappings = params.mappings
      if (!isRecord(mappings)) {
        chain.ordered = null
        return
      }
      for (const [from, to] of Object.entries(mappings)) {
        const target = text(to)
        if (!from || !target) continue
        demand(chain, from, 'projected')
        const current = chain.present.get(from)
        chain.present.delete(from)
        chain.roots.set(target, rootsOf(chain, from))
        chain.roots.delete(from)
        chain.created.add(target)
        produce(chain, { ...(current ?? field(chain, target, 'projected')), name: target })
        const type = chain.types.get(from)
        if (type) chain.types.set(target, type)
        if (chain.ordered) {
          chain.ordered = chain.ordered.map((entry) =>
            entry.name === from ? { ...entry, name: target } : entry,
          )
        }
      }
      return
    }

    case 'drop': {
      for (const name of names(params.columns)) {
        demand(chain, name, 'projected')
        chain.present.delete(name)
        chain.roots.delete(name)
        if (chain.ordered) chain.ordered = chain.ordered.filter((entry) => entry.name !== name)
      }
      return
    }

    case 'fill_na': {
      for (const name of names(params.columns)) demand(chain, name, 'required', 'fill_na')
      return
    }

    case 'group_by': {
      // The one transformation that makes an unknown schema knowable: the output
      // is exactly the keys plus the aggregates, in that order.
      const keys = names(params.by)
      const aggregates = names(params.agg)
      const built: CatalogField[] = []
      for (const key of keys) {
        demand(chain, key, 'grouped')
        built.push(field(chain, key, 'grouped'))
      }
      let knowable = keys.length > 0
      for (const entry of aggregates) {
        const name = outputName(entry)
        if (name === null) {
          knowable = false
          continue
        }
        const source = expressionPart(entry)
        reads(chain, source, 'aggregated')
        chain.roots.set(name, rootsOfExpression(chain, source))
        chain.created.add(name)
        built.push({ name, type: null, origin: 'aggregated', note: entry })
      }
      chain.ordered = knowable ? built : null
      chain.present = new Map(built.map((entry) => [entry.name, entry]))
      return
    }

    case 'join': {
      const keys = names(params.on)
      for (const key of keys) demand(chain, key, 'required', 'join key')
      const right = walkSide(walk, node.id)
      if (right) {
        for (const entry of right.ordered ?? right.present.values()) {
          if (keys.includes(entry.name)) continue
          const joined: CatalogField = { ...entry, origin: 'joined' }
          chain.roots.set(entry.name, rootsOf(right, entry.name))
          produce(chain, joined)
          if (chain.ordered && right.ordered) chain.ordered = [...chain.ordered, joined]
        }
      }
      // A join only keeps the order provable when BOTH sides are.
      if (!right || right.ordered === null) chain.ordered = null
      return
    }

    case 'union': {
      // Positional union: the left side keeps deciding the names. Provenance is
      // matched BY NAME here, which is what the two sides mean when they line up
      // and the closest honest guess when they do not.
      const other = walkSide(walk, node.id)
      if (!other) return
      // Names either side already tracks explicitly get both sets of roots.
      const shared = new Set([
        ...(other.ordered ?? []).map((entry) => entry.name),
        ...other.present.keys(),
        ...other.roots.keys(),
        ...(chain.ordered ?? []).map((entry) => entry.name),
        ...chain.present.keys(),
        ...chain.roots.keys(),
      ])
      for (const name of shared) {
        const roots = new Set(rootsOf(chain, name))
        for (const root of rootsOf(other, name)) roots.add(root)
        chain.roots.set(name, roots)
      }
      // Everything else the chain carries is untouched on both sides: the other
      // side's head now answers for those columns too.
      if (!other.opaque && other.head) chain.merged.push(other.head, ...other.merged)
      return
    }

    default:
      if (PASSTHROUGH.has(transform)) return
      // sql, $include and anything custom reshape the frame in ways only Spark knows.
      chain.ordered = null
      chain.opaque = true
      chain.roots.clear()
  }
}

/** Validation rules name columns that must be there, or the run fails. */
function applyValidation(chain: Chain, node: StudioNode): void {
  if (node.data.kind !== 'validation') return
  const { validator, params } = node.data
  const referenced = [...names(params.columns), ...names(params.column)]
  for (const name of referenced) {
    demand(chain, name, 'required', validator)
    if (!chain.present.has(name)) {
      chain.present.set(name, field(chain, name, 'required', undefined, validator))
    }
  }
}

/**
 * Walks the right-hand sub-chain of a join or a union. Depth-bounded because a
 * canvas can nest joins, and a malformed graph should not cost a stack.
 */
function walkSide(walk: Walk, nodeId: string): Chain | null {
  if (walk.depth <= 0) return null
  const parent = sideParent(walk.graph, nodeId)
  if (!parent) return null
  const { nodes, problem } = chainToSink(walk.graph, parent.id)
  if (problem || nodes.length === 0) return null
  const chain = walkChain({ ...walk, depth: walk.depth - 1 }, nodes)
  recordHead(walk, nodes[0], chain)
  return chain
}

/** Runs the tracker over one chain, head first. */
function walkChain(walk: Walk, nodes: readonly StudioNode[]): Chain {
  const chain = newChain(walk, addressOfNode(nodes[0]))
  for (const node of nodes) {
    if (!isCompilable(node)) continue
    chain.at = node
    if (node.data.kind === 'transform') applyTransform(chain, node, walk)
    else if (node.data.kind === 'validation') applyValidation(chain, node)
  }
  return chain
}

/** The head of a chain is the dataset every demand the chain made was made of. */
function recordHead(walk: Walk, head: StudioNode, chain: Chain): void {
  if (head.data.kind !== 'source') return
  walk.found.push(
    finish(addressOfNode(head), [...chain.upstream.values()], false, {
      jobId: walk.job.id,
      jobName: walk.job.name,
      nodeId: head.id,
      side: 'read',
    }),
  )
}

/* ------------------------------------------------------------------ per job */

function addressOfNode(node: StudioNode): string {
  if (node.data.kind === 'source' || node.data.kind === 'sink') {
    const path = text(node.data.path)
    if (path) return datasetKey(path)
  }
  return ''
}

function finish(
  key: string,
  fields: CatalogField[],
  ordered: boolean,
  source: SchemaSource,
): DatasetSchema {
  if (fields.length === 0) return { key, fields: [], confidence: 'unknown', source }
  return { key, fields, confidence: ordered ? 'complete' : 'partial', source }
}

function offer(out: Map<string, DatasetSchema>, schema: DatasetSchema): void {
  if (schema.key === '' || schema.confidence === 'unknown') return
  const current = out.get(schema.key)
  if (!current || score(schema) > score(current)) out.set(schema.key, schema)
}

const MAX_SIDE_DEPTH = 6

/** What one Job says about columns: their shape, where they came from, who reads them. */
export interface JobColumns {
  schemas: DatasetSchema[]
  links: ColumnLink[]
  uses: ColumnUse[]
}

/**
 * Every dataset this Job says something about: the destinations it writes, and
 * the sources it reads, each with whatever the chain proves — plus the column
 * edges and references the same walk collected on the way.
 */
export function analyzeJob(job: Job): JobColumns {
  const graph = job.graph
  const found: DatasetSchema[] = []
  const walk: Walk = { graph, job, found, links: [], uses: [], depth: MAX_SIDE_DEPTH }

  for (const node of graph.nodes) {
    if (node.data.kind !== 'sink' || !isCompilable(node)) continue
    // The validation report has a schema of its own, written by the DQ engine and
    // named nowhere on the canvas — describing it from this chain would be a lie.
    if (node.data.dqRole === 'report') continue

    const { nodes, problem } = chainToSink(graph, node.id)
    if (problem || nodes.length === 0) continue
    const chain = walkChain(walk, nodes)
    chain.at = node

    // The destination's own column list is the last word on what gets written.
    const projection = node.data.columns
    if (Array.isArray(projection) && projection.length > 0) {
      for (const name of projection) noteUse(chain, name, 'projected')
      const projected = projection.map((name) => field(chain, name, 'projected'))
      chain.ordered = projected
      chain.present = new Map(projected.map((entry) => [entry.name, entry]))
    }
    if (node.data.dqRole === 'invalid') {
      const annotate = text(node.data.annotate)
      if (annotate) {
        const entry: CatalogField = {
          name: annotate,
          type: 'array<string>',
          origin: 'quality',
          note: 'codes of the rules that rejected the row',
        }
        produce(chain, entry)
        if (chain.ordered) chain.ordered = [...chain.ordered, entry]
      }
    }

    const written = chain.ordered ?? [...chain.present.values()]
    const address = addressOfNode(node)
    found.push(
      finish(address, written, chain.ordered !== null, {
        jobId: job.id,
        jobName: job.name,
        nodeId: node.id,
        side: 'written',
      }),
    )

    if (address) {
      for (const entry of written) {
        for (const encoded of rootsOf(chain, entry.name)) {
          const from = unref(encoded)
          // A Job that rewrites the same address in place says nothing new about
          // where the column came from.
          if (from.key === address && from.column === entry.name) continue
          walk.links.push({
            from,
            to: { key: address, column: entry.name },
            jobId: job.id,
            jobName: job.name,
            nodeId: node.id,
            kind: LINK_KIND[entry.origin],
            ...(entry.note ? { note: entry.note } : {}),
          })
        }
      }
    }

    recordHead(walk, nodes[0], chain)
  }

  return { schemas: found, links: walk.links, uses: walk.uses }
}

/** The schemas alone — what the catalog list and the dataset sheet ask for. */
export function schemasOfJob(job: Job): DatasetSchema[] {
  return analyzeJob(job).schemas
}

/**
 * One schema per dataset address, across every Job in the library — the strongest
 * description each address has anywhere.
 */
export function deriveSchemas(jobs: readonly Job[]): Map<string, DatasetSchema> {
  const out = new Map<string, DatasetSchema>()
  for (const job of jobs) {
    for (const schema of schemasOfJob(job)) offer(out, schema)
  }
  return out
}
