import { describe, expect, it } from 'vitest'

import { deriveSchemas, schemasOfJob } from './schema'
import { pipelineToGraph } from '@/lib/compiler/toGraph'
import { TEMPLATES } from '@/data/templates'
import type { Job } from '@/types/studio'

/* ---------------------------------------------------------------- helpers */

let counter = 0

/** A Job built from a pipeline JSON, which is how every real canvas starts. */
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

const fieldNames = (fields: readonly { name: string }[]) => fields.map((field) => field.name)

/* ------------------------------------------------------------------ tests */

describe('schemasOfJob', () => {
  it('reads the types a cast states, and the column a with_column creates', () => {
    const job = jobOf(templateOf('medallion-bronze'))
    const written = schemasOfJob(job).find((schema) => schema.key === '/lake/bronze/orders')

    // The CSV reader supplies the rest of the columns at runtime, so this is only
    // part of the schema — and it says so.
    expect(written?.confidence).toBe('partial')
    expect(written?.fields).toContainEqual({ name: 'amount', type: 'decimal(18,2)', origin: 'cast' })
    expect(written?.fields).toContainEqual({
      name: 'ordered_at',
      type: 'timestamp',
      origin: 'cast',
    })
    expect(written?.fields).toContainEqual({
      name: 'ingested_at',
      type: null,
      origin: 'computed',
      note: 'current_timestamp()',
    })
  })

  it('describes a dataset it only reads by what the Job demands of it', () => {
    const job = jobOf(templateOf('medallion-bronze'))
    const read = schemasOfJob(job).find((schema) => schema.key === '/lake/landing/orders')

    expect(read?.source?.side).toBe('read')
    expect(read?.confidence).toBe('partial')
    // Every cast key must already be there — cast uses F.col, not F.expr.
    expect(fieldNames(read?.fields ?? [])).toEqual([
      'order_id',
      'customer_id',
      'amount',
      'ordered_at',
    ])
    // ingested_at is created by this Job; it is NOT in the landing drop.
    expect(fieldNames(read?.fields ?? [])).not.toContain('ingested_at')
  })

  it('takes the right side of a join as its own dataset, with the columns the join selects', () => {
    const job = jobOf(templateOf('medallion-silver'))
    const schemas = schemasOfJob(job)
    const customers = schemas.find((schema) => schema.key === '/lake/raw/customers')

    expect(fieldNames(customers?.fields ?? [])).toEqual([
      'customer_id',
      'customer_name',
      'segment',
    ])
  })

  it('carries the joined columns into the dataset the chain writes', () => {
    const job = jobOf(templateOf('medallion-silver'))
    const silver = schemasOfJob(job).find((schema) => schema.key === '/lake/silver/orders')

    expect(fieldNames(silver?.fields ?? [])).toContain('customer_name')
    expect(silver?.fields.find((field) => field.name === 'segment')?.origin).toBe('joined')
  })

  it('proves the whole schema of a group_by output, in order', () => {
    const gold = schemasOfJob(jobOf(templateOf('medallion-gold'))).find(
      (schema) => schema.key === '/lake/gold/revenue_by_country',
    )

    // A group_by output IS its keys plus its aggregates — nothing else survives.
    expect(gold?.confidence).toBe('complete')
    expect(fieldNames(gold?.fields ?? [])).toEqual([
      'country',
      'revenue',
      'orders',
      'avg_ticket',
      'computed_at',
    ])
    expect(gold?.fields[1]).toEqual({
      name: 'revenue',
      type: null,
      origin: 'aggregated',
      note: 'sum(amount) as revenue',
    })
  })

  it('adds the annotate column to the quarantine and nothing else', () => {
    const job = jobOf(templateOf('medallion-silver'))
    const quarantine = schemasOfJob(job).find(
      (schema) => schema.key === '/lake/quarantine/orders',
    )

    expect(quarantine?.fields).toContainEqual({
      name: 'dq_failures',
      type: 'array<string>',
      origin: 'quality',
      note: 'codes of the rules that rejected the row',
    })
  })

  it('says nothing about the validation report, whose schema the DQ engine owns', () => {
    const keys = schemasOfJob(jobOf(templateOf('medallion-silver'))).map((schema) => schema.key)
    expect(keys).not.toContain('/lake/quality/orders_report')
  })

  it('takes a destination column list as the last word on what is written', () => {
    const job = jobOf({
      name: 'projected',
      input: { format: 'parquet', path: '/in' },
      transformations: [
        { type: 'cast', columns: { id: 'long', amount: 'double' } },
        { type: 'with_column', column: 'ingested_at', expression: 'current_timestamp()' },
      ],
      output: { format: 'parquet', path: '/out', mode: 'overwrite', columns: ['id', 'amount'] },
    })
    const written = schemasOfJob(job).find((schema) => schema.key === '/out')

    expect(written?.confidence).toBe('complete')
    expect(written?.fields).toEqual([
      { name: 'id', type: 'long', origin: 'projected' },
      { name: 'amount', type: 'double', origin: 'projected' },
    ])
  })

  it('counts a validation rule as proof that its column exists', () => {
    const job = jobOf({
      name: 'checked',
      input: { format: 'parquet', path: '/in' },
      validations: {
        on_failure: 'warn',
        rules: [{ type: 'not_null', columns: ['order_id'] }],
      },
      output: { format: 'parquet', path: '/out', mode: 'overwrite' },
    })
    const read = schemasOfJob(job).find((schema) => schema.key === '/in')

    expect(read?.fields).toEqual([
      { name: 'order_id', type: null, origin: 'required', note: 'not_null' },
    ])
  })

  it('stops claiming an order once a sql step reshapes the frame', () => {
    const job = jobOf({
      name: 'opaque',
      input: { format: 'parquet', path: '/in' },
      transformations: [
        { type: 'select', columns: ['a', 'b'] },
        { type: 'sql', query: 'SELECT * FROM {df}' },
      ],
      output: { format: 'parquet', path: '/out', mode: 'overwrite' },
    })
    const written = schemasOfJob(job).find((schema) => schema.key === '/out')

    expect(written?.confidence).toBe('partial')
  })

  it('renames a column instead of inventing a second one', () => {
    const job = jobOf({
      name: 'renamed',
      input: { format: 'parquet', path: '/in' },
      transformations: [
        { type: 'select', columns: ['id', 'valor'] },
        { type: 'cast', columns: { valor: 'decimal(18,2)' } },
        { type: 'rename', mappings: { valor: 'amount' } },
      ],
      output: { format: 'parquet', path: '/out', mode: 'overwrite' },
    })
    const written = schemasOfJob(job).find((schema) => schema.key === '/out')

    expect(written?.fields).toEqual([
      { name: 'id', type: null, origin: 'projected' },
      { name: 'amount', type: 'decimal(18,2)', origin: 'projected' },
    ])
  })
})

describe('deriveSchemas', () => {
  it('keeps the description that says the most about each address', () => {
    // The bronze Job writes /lake/bronze/orders; the silver Job only reads it.
    const jobs = [jobOf(templateOf('medallion-silver')), jobOf(templateOf('medallion-bronze'))]
    const bronze = deriveSchemas(jobs).get('/lake/bronze/orders')

    expect(bronze?.source?.side).toBe('written')
    expect(fieldNames(bronze?.fields ?? [])).toContain('ingested_at')
  })

  it('drops the addresses nothing says anything about', () => {
    const job = jobOf({
      name: 'silent',
      input: { format: 'parquet', path: '/in' },
      transformations: [{ type: 'filter', condition: 'x > 1' }],
      output: { format: 'parquet', path: '/out', mode: 'overwrite' },
    })

    expect(deriveSchemas([job]).size).toBe(0)
  })
})
