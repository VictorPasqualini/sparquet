/**
 * The bridge from an exploration to a Job: what has to survive the crossing is
 * the statement itself, and what must NOT be invented is the destination.
 */

import { describe, expect, it } from 'vitest'

import { pipelineFromQuery } from '@/lib/sql/toJob'

const ORDERS = { key: 's3://lake/silver/orders', alias: 'silver_orders', format: 'delta' }
const CUSTOMERS = { key: 's3://lake/silver/customers', alias: 'silver_customers', format: 'delta' }

describe('pipelineFromQuery', () => {
  it('registers the input under the alias the query already uses', () => {
    const { pipeline, attached } = pipelineFromQuery({
      name: 'Orders by month',
      sql: 'SELECT month, count(*) FROM silver_orders GROUP BY month',
      datasets: [ORDERS],
    })

    expect(pipeline.input).toEqual({ format: 'delta', path: ORDERS.key })
    expect(pipeline.transformations).toEqual([
      {
        type: 'sql',
        query: 'SELECT month, count(*) FROM silver_orders GROUP BY month',
        // The alias, not `_df` — this is what lets the statement travel verbatim.
        view_name: 'silver_orders',
      },
    ])
    expect(attached).toBe(ORDERS)
  })

  it('leaves the destination blank rather than guessing a write', () => {
    const { pipeline } = pipelineFromQuery({
      name: 'Orders',
      sql: 'SELECT * FROM silver_orders',
      datasets: [ORDERS],
    })

    expect(pipeline.output).toEqual({ format: '', path: '', mode: 'overwrite' })
  })

  it('drops a trailing semicolon, which is an editor habit and a spark.sql error', () => {
    const { pipeline } = pipelineFromQuery({
      name: 'Orders',
      sql: '  SELECT * FROM silver_orders ;  ',
      datasets: [ORDERS],
    })

    expect((pipeline.transformations as { query: string }[])[0].query).toBe(
      'SELECT * FROM silver_orders',
    )
  })

  it('hands back the datasets a pipeline cannot take, because it has one input', () => {
    const { attached, unattached } = pipelineFromQuery({
      name: 'Joined',
      sql: 'SELECT * FROM silver_orders JOIN silver_customers USING (customer_id)',
      datasets: [ORDERS, CUSTOMERS],
    })

    expect(attached).toBe(ORDERS)
    expect(unattached).toEqual([CUSTOMERS])
  })

  it('still produces a job when the query names no catalog dataset', () => {
    const { pipeline, attached } = pipelineFromQuery({
      name: 'Literal',
      sql: 'SELECT 1',
      datasets: [],
    })

    expect(attached).toBeNull()
    expect(pipeline.input).toEqual({ format: '', path: '' })
    // No `view_name`: there is nothing to register it under yet.
    expect(pipeline.transformations).toEqual([{ type: 'sql', query: 'SELECT 1' }])
  })

  it('carries the spark block only when the editor resolved one', () => {
    const bare = pipelineFromQuery({ name: 'A', sql: 'SELECT 1', datasets: [ORDERS] })
    expect(bare.pipeline.spark).toBeUndefined()

    const withConfigs = pipelineFromQuery({
      name: 'A',
      sql: 'SELECT 1',
      datasets: [ORDERS],
      spark: { configs: { 'spark.jars.packages': 'io.delta:delta-spark_2.13:4.0.0' } },
    })
    expect(withConfigs.pipeline.spark).toEqual({
      configs: { 'spark.jars.packages': 'io.delta:delta-spark_2.13:4.0.0' },
    })
  })
})
