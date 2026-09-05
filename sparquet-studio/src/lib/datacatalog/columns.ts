/**
 * Column-level lineage, and the impact of changing one column.
 *
 * The Jobs already state, column by column, where a written value came from —
 * `schema.ts` collects those edges while it derives the schema. This module turns
 * the pile into a graph: follow it forward to answer "what breaks if I change
 * this", follow it back to answer "where does this number come from".
 *
 * Two different questions live here, and they are answered from two different
 * records. An EDGE means a value flows: `/lake/silver/orders.amount` feeds
 * `/lake/gold/revenue_by_country.revenue`. A USE means a step merely names the
 * column — a filter, a join key, a validation rule. A rename breaks both, so
 * impact analysis reports both, kept apart so the reader can tell a data path
 * from a mention.
 *
 * Every edge is as good as what the canvas says, no better: an expression is
 * opaque Spark SQL and the identifiers inside it are matched by shape. Treat a
 * long trail as a lead, not as a proof.
 */

import { analyzeJob, type ColumnLink, type ColumnRef, type ColumnUse } from './schema'
import type { Job } from '@/types/studio'

/** How deep a trail is followed before the answer is called long enough. */
const MAX_DEPTH = 8

export interface ColumnGraph {
  links: ColumnLink[]
  uses: ColumnUse[]
  /** Edges leaving a column, by `refKey`. */
  out: Map<string, ColumnLink[]>
  /** Edges arriving at a column, by `refKey`. */
  into: Map<string, ColumnLink[]>
  /** Steps naming a column, by `refKey`. */
  mentions: Map<string, ColumnUse[]>
}

/** One hop of a trail, with how far from the column the walk had gone. */
export interface ColumnStep {
  link: ColumnLink
  depth: number
}

export interface ColumnImpact {
  /** Columns fed by this one, nearest first. */
  downstream: ColumnStep[]
  /** Where this column is read: filters, join keys, rules, projections. */
  uses: ColumnUse[]
  /** True when the walk stopped at the depth cap instead of at a leaf. */
  truncated: boolean
}

export function refKey(ref: ColumnRef): string {
  return `${ref.key} ${ref.column}`
}

function push<T>(index: Map<string, T[]>, key: string, value: T): void {
  const current = index.get(key)
  if (current) current.push(value)
  else index.set(key, [value])
}

/** Same edge stated by two Jobs is two edges; stated twice by one, only one. */
function linkId(link: ColumnLink): string {
  return [link.jobId, link.nodeId, link.kind, refKey(link.from), refKey(link.to)].join(' ')
}

/**
 * The column graph of a library.
 *
 * Built from the same walk the schemas come from, so a dataset described in the
 * catalog and a dataset in this graph are the same dataset, at the same address.
 */
export function buildColumnGraph(jobs: readonly Job[]): ColumnGraph {
  const links: ColumnLink[] = []
  const uses: ColumnUse[] = []
  const seen = new Set<string>()

  for (const job of jobs) {
    const analysis = analyzeJob(job)
    for (const link of analysis.links) {
      const id = linkId(link)
      if (seen.has(id)) continue
      seen.add(id)
      links.push(link)
    }
    uses.push(...analysis.uses)
  }

  const out = new Map<string, ColumnLink[]>()
  const into = new Map<string, ColumnLink[]>()
  const mentions = new Map<string, ColumnUse[]>()

  for (const link of links) {
    push(out, refKey(link.from), link)
    push(into, refKey(link.to), link)
  }
  for (const use of uses) push(mentions, refKey({ key: use.key, column: use.column }), use)

  return { links, uses, out, into, mentions }
}

/** Breadth-first walk of one direction, cycle-safe and depth-bounded. */
function walk(
  index: Map<string, ColumnLink[]>,
  start: ColumnRef,
  next: (link: ColumnLink) => ColumnRef,
  maxDepth: number,
): { steps: ColumnStep[]; truncated: boolean } {
  const steps: ColumnStep[] = []
  const visited = new Set<string>([refKey(start)])
  let frontier: ColumnRef[] = [start]
  let truncated = false

  for (let depth = 1; depth <= maxDepth && frontier.length > 0; depth += 1) {
    const following: ColumnRef[] = []
    for (const ref of frontier) {
      for (const link of index.get(refKey(ref)) ?? []) {
        steps.push({ link, depth })
        const target = next(link)
        const id = refKey(target)
        if (visited.has(id)) continue
        visited.add(id)
        following.push(target)
      }
    }
    frontier = following
    // One more level exists but the cap was reached: say so rather than let the
    // trail look finished.
    if (depth === maxDepth && frontier.some((ref) => (index.get(refKey(ref)) ?? []).length > 0)) {
      truncated = true
    }
  }

  return { steps, truncated }
}

/**
 * What a change to this column reaches: the columns it feeds, and every step
 * that names it. Both are needed — dropping a column that only ever appears in a
 * `filter` breaks the Job without feeding anything downstream.
 */
export function impactOf(
  graph: ColumnGraph,
  ref: ColumnRef,
  options: { maxDepth?: number } = {},
): ColumnImpact {
  const { steps, truncated } = walk(
    graph.out,
    ref,
    (link) => link.to,
    options.maxDepth ?? MAX_DEPTH,
  )
  return {
    downstream: steps,
    uses: graph.mentions.get(refKey(ref)) ?? [],
    truncated,
  }
}

/** Where the values in this column came from, nearest first. */
export function originsOf(
  graph: ColumnGraph,
  ref: ColumnRef,
  options: { maxDepth?: number } = {},
): ColumnStep[] {
  return walk(graph.into, ref, (link) => link.from, options.maxDepth ?? MAX_DEPTH).steps
}

/**
 * The same answer stated twice is noise, not evidence.
 *
 * The graph keeps every record because they are different facts about the canvas:
 * a `select` and the output projection each copy `amount`, and a `group_by` names
 * a column once per aggregate that reads it. A reader wants the distinct answers,
 * nearest first — the walk is breadth-first, so the first occurrence is the
 * shortest trail and the one worth keeping.
 */
export function dedupeSteps(
  steps: readonly ColumnStep[],
  endOf: (step: ColumnStep) => ColumnRef,
): ColumnStep[] {
  const seen = new Set<string>()
  const out: ColumnStep[] = []
  for (const step of steps) {
    const id = [refKey(endOf(step)), step.link.kind, step.link.jobId].join(' ')
    if (seen.has(id)) continue
    seen.add(id)
    out.push(step)
  }
  return out
}

/** The same step naming a column several times is one mention, not several. */
export function dedupeUses(uses: readonly ColumnUse[]): ColumnUse[] {
  const seen = new Set<string>()
  const out: ColumnUse[] = []
  for (const use of uses) {
    const id = [use.jobId, use.nodeId, use.step, use.role].join(' ')
    if (seen.has(id)) continue
    seen.add(id)
    out.push(use)
  }
  return out
}

/** Every column of one dataset the graph knows anything about, in first-seen order. */
export function columnsOf(graph: ColumnGraph, key: string): string[] {
  const out: string[] = []
  const seen = new Set<string>()
  const add = (ref: ColumnRef) => {
    if (ref.key !== key || seen.has(ref.column)) return
    seen.add(ref.column)
    out.push(ref.column)
  }
  for (const link of graph.links) {
    add(link.to)
    add(link.from)
  }
  for (const use of graph.uses) add({ key: use.key, column: use.column })
  return out
}
