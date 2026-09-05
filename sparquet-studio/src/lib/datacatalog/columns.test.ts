import { describe, expect, it } from 'vitest'

import { buildColumnGraph, columnsOf, impactOf, originsOf } from './columns'
import { pipelineToGraph } from '@/lib/compiler/toGraph'
import { TEMPLATES } from '@/data/templates'
import type { ColumnStep } from './columns'
import type { Job } from '@/types/studio'

/* ---------------------------------------------------------------- helpers */

let counter = 0

const jobOf = (pipeline: unknown, name = `job-${(counter += 1)}`): Job => {
  const { graph, settings } = pipelineToGraph(pipeline)
  return {
    id: name,
    workflowId: 'w1',
    name,
    description: '',
    tags: [],
    settings,
    graph,
    params: [],
    createdAt: 0,
    updatedAt: 0,
    revision: 1,
  }
}

const templateOf = (id: string): unknown => {
  const template = TEMPLATES.find((entry) => entry.id === id)
  if (!template) throw new Error(`template ${id} not found`)
  return template.pipeline
}

const medallion = () => [
  jobOf(templateOf('medallion-bronze'), 'bronze'),
  jobOf(templateOf('medallion-silver'), 'silver'),
  jobOf(templateOf('medallion-gold'), 'gold'),
]

/** `/lake/gold/x.revenue <- /lake/silver/orders.amount`, as one comparable string. */
const trail = (steps: readonly ColumnStep[]): string[] =>
  steps.map(
    ({ link }) => `${link.from.key}.${link.from.column} -> ${link.to.key}.${link.to.column}`,
  )

/* ------------------------------------------------------------------ tests */

describe('buildColumnGraph', () => {
  it('links an aggregate back to the column it sums', () => {
    const graph = buildColumnGraph(medallion())
    const revenue = graph.into.get('/lake/gold/revenue_by_country revenue') ?? []

    expect(revenue.map((link) => `${link.from.key}.${link.from.column}`).sort()).toEqual([
      '/lake/silver/orders.amount',
      '/lake/silver/orders_backfill.amount',
    ])
    expect(revenue[0].kind).toBe('aggregate')
    expect(revenue[0].note).toBe('sum(amount) as revenue')
  })

  it('carries a group key through as a key, not as a copy', () => {
    const graph = buildColumnGraph(medallion())
    const country = graph.into.get('/lake/gold/revenue_by_country country') ?? []

    expect(country.map((link) => link.kind)).toContain('group key')
  })

  it('invents no edge for a column a computed expression never reads', () => {
    // computed_at is current_timestamp(): it comes from nowhere, and saying it
    // came from the head dataset would be a lie the screen would repeat.
    const graph = buildColumnGraph(medallion())

    expect(graph.into.get('/lake/gold/revenue_by_country computed_at')).toBeUndefined()
  })

  it('takes the joined columns from the dataset the join reads', () => {
    const graph = buildColumnGraph(medallion())
    const segment = graph.into.get('/lake/silver/orders segment') ?? []

    expect(segment.map((link) => `${link.from.key}.${link.from.column}`)).toEqual([
      '/lake/raw/customers.segment',
    ])
    expect(segment[0].kind).toBe('join')
  })

  it('stops guessing after a sql step reshapes the frame', () => {
    const job = jobOf({
      name: 'opaque',
      input: { format: 'parquet', path: '/in' },
      transformations: [
        { type: 'select', columns: ['a', 'b'] },
        { type: 'sql', query: 'SELECT * FROM {df}' },
      ],
      output: { format: 'parquet', path: '/out', mode: 'overwrite' },
    })

    expect(buildColumnGraph([job]).links).toEqual([])
  })

  it('follows a rename to the name the destination writes', () => {
    const job = jobOf({
      name: 'renamed',
      input: { format: 'parquet', path: '/in' },
      transformations: [
        { type: 'select', columns: ['id', 'valor'] },
        { type: 'rename', mappings: { valor: 'amount' } },
      ],
      output: { format: 'parquet', path: '/out', mode: 'overwrite' },
    })
    const graph = buildColumnGraph([job])

    expect(trail(originsOf(graph, { key: '/out', column: 'amount' }))).toEqual([
      '/in.valor -> /out.amount',
    ])
  })
})

describe('impactOf', () => {
  it('follows one column across three Jobs, nearest first', () => {
    const graph = buildColumnGraph(medallion())
    const impact = impactOf(graph, { key: '/lake/landing/orders', column: 'amount' })

    expect(trail(impact.downstream).sort()).toEqual([
      '/lake/bronze/orders.amount -> /lake/quarantine/orders.amount',
      '/lake/bronze/orders.amount -> /lake/silver/orders.amount',
      '/lake/landing/orders.amount -> /lake/bronze/orders.amount',
      '/lake/silver/orders.amount -> /lake/exports/revenue_by_country.avg_ticket',
      '/lake/silver/orders.amount -> /lake/exports/revenue_by_country.revenue',
      '/lake/silver/orders.amount -> /lake/gold/revenue_by_country.avg_ticket',
      '/lake/silver/orders.amount -> /lake/gold/revenue_by_country.revenue',
    ])
    expect(impact.downstream[0].link.kind).toBe('cast')
    expect(impact.truncated).toBe(false)
  })

  it('reports the steps that only name the column', () => {
    const graph = buildColumnGraph(medallion())
    const impact = impactOf(graph, { key: '/lake/silver/orders', column: 'amount' })

    expect(impact.uses.map((use) => `${use.jobName}:${use.step}`)).toContain('gold:group_by')
  })

  it('counts a validation rule as something that breaks', () => {
    const graph = buildColumnGraph(medallion())
    const impact = impactOf(graph, { key: '/lake/bronze/orders', column: 'order_id' })

    expect(impact.uses.map((use) => use.step)).toEqual(
      expect.arrayContaining(['not_null', 'unique']),
    )
  })

  it('ends on a cycle instead of walking it forever', () => {
    const back = jobOf(
      {
        name: 'back',
        input: { format: 'parquet', path: '/b' },
        transformations: [{ type: 'select', columns: ['id'] }],
        output: { format: 'parquet', path: '/a', mode: 'overwrite' },
      },
      'back',
    )
    const forth = jobOf(
      {
        name: 'forth',
        input: { format: 'parquet', path: '/a' },
        transformations: [{ type: 'select', columns: ['id'] }],
        output: { format: 'parquet', path: '/b', mode: 'overwrite' },
      },
      'forth',
    )
    const graph = buildColumnGraph([back, forth])

    expect(trail(impactOf(graph, { key: '/a', column: 'id' }).downstream)).toEqual([
      '/a.id -> /b.id',
      '/b.id -> /a.id',
    ])
  })

  it('says when it stopped at the depth cap rather than at the end', () => {
    const graph = buildColumnGraph(medallion())
    const impact = impactOf(
      graph,
      { key: '/lake/landing/orders', column: 'amount' },
      { maxDepth: 1 },
    )

    expect(trail(impact.downstream)).toEqual([
      '/lake/landing/orders.amount -> /lake/bronze/orders.amount',
    ])
    expect(impact.truncated).toBe(true)
  })
})

describe('columnsOf', () => {
  it('lists what the canvas knows about one address', () => {
    const graph = buildColumnGraph(medallion())

    expect(columnsOf(graph, '/lake/gold/revenue_by_country')).toEqual([
      'country',
      'revenue',
      'orders',
      'avg_ticket',
    ])
  })
})
