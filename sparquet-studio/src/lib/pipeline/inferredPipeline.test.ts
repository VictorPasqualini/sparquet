import { describe, expect, it } from 'vitest'

import { pipelineToGraph } from '@/lib/compiler'
import { deriveInferredPipeline, type JobLink } from '@/lib/pipeline'
import type { Job } from '@/types/studio'

/* ---------------------------------------------------------------- helpers */

/**
 * Builds a job the way the library store does — through the importer — so
 * the test exercises the same graph the editor stores and compiles back.
 */
function job(id: string, name: string, pipeline: unknown): Job {
  const imported = pipelineToGraph(pipeline)
  return {
    id,
    workflowId: 'workflow',
    name,
    description: '',
    tags: [],
    settings: imported.settings,
    graph: imported.graph,
    params: [],
    createdAt: 0,
    updatedAt: 0,
    revision: 1,
  }
}

const pipe = (name: string, input: unknown, output: unknown, extra: object = {}) => ({
  name,
  input,
  ...extra,
  output,
})

const linkOf = (edges: readonly JobLink[], source: string, target: string) =>
  edges.find((edge) => edge.source === source && edge.target === target)

/* ------------------------------------------------------------------ tests */

describe('deriveInferredPipeline — storage links', () => {
  it('links an output path to the input that reads it back', () => {
    const pipeline = deriveInferredPipeline([
      job(
        'a',
        'Bronze',
        pipe(
          'bronze',
          { format: 'csv', path: '/raw/orders' },
          {
            format: 'parquet',
            path: '/lake/bronze/orders',
            mode: 'overwrite',
          },
        ),
      ),
      job(
        'b',
        'Silver',
        pipe(
          'silver',
          { format: 'parquet', path: '/lake/bronze/orders' },
          {
            format: 'delta',
            path: '/lake/silver/orders',
          },
        ),
      ),
    ])

    expect(pipeline.edges).toHaveLength(1)
    expect(pipeline.edges[0]).toMatchObject({
      source: 'a',
      target: 'b',
      via: 'storage',
      locations: ['/lake/bronze/orders'],
    })
    // Topological order: the writer is read first.
    expect(pipeline.nodes.map((node) => [node.name, node.order])).toEqual([
      ['Bronze', 1],
      ['Silver', 2],
    ])
  })

  it('ignores trailing slashes, backslashes and case when comparing paths', () => {
    const pipeline = deriveInferredPipeline([
      job(
        'a',
        'Writer',
        pipe(
          'writer',
          { format: 'csv', path: '/raw' },
          {
            format: 'parquet',
            path: 'C:\\Lake\\Bronze\\Orders\\',
          },
        ),
      ),
      job(
        'b',
        'Reader',
        pipe(
          'reader',
          { format: 'parquet', path: 'c:/lake/bronze/orders' },
          {
            format: 'csv',
            path: '/out',
          },
        ),
      ),
    ])

    expect(pipeline.edges).toHaveLength(1)
    expect(pipeline.edges[0].via).toBe('storage')
  })

  it('does not link different paths, or the same path on unrelated connectors', () => {
    const pipeline = deriveInferredPipeline([
      job(
        'a',
        'Writer',
        pipe(
          'writer',
          { format: 'csv', path: '/raw' },
          {
            format: 'parquet',
            path: '/lake/bronze/orders',
          },
        ),
      ),
      job(
        'b',
        'Elsewhere',
        pipe(
          'elsewhere',
          { format: 'parquet', path: '/lake/bronze/customers' },
          {
            format: 'csv',
            path: '/out',
          },
        ),
      ),
      job(
        'c',
        'Database',
        // Same string, but a Postgres table is not the parquet folder above.
        pipe(
          'database',
          { format: 'postgresql', path: '/lake/bronze/orders' },
          {
            format: 'csv',
            path: '/out-db',
          },
        ),
      ),
    ])

    expect(pipeline.edges).toEqual([])
  })

  it('links a join `with` source, not only the main input', () => {
    const pipeline = deriveInferredPipeline([
      job(
        'a',
        'Dimension',
        pipe(
          'dimension',
          { format: 'csv', path: '/raw/customers' },
          {
            format: 'parquet',
            path: '/lake/dim/customers',
          },
        ),
      ),
      job(
        'b',
        'Facts',
        pipe(
          'facts',
          { format: 'parquet', path: '/lake/bronze/orders' },
          { format: 'delta', path: '/lake/gold/orders' },
          {
            transformations: [
              {
                type: 'join',
                with: { format: 'parquet', path: '/lake/dim/customers' },
                on: 'customer_id',
                how: 'left',
              },
            ],
          },
        ),
      ),
    ])

    expect(linkOf(pipeline.edges, 'a', 'b')).toMatchObject({ via: 'storage' })
  })

  it('fans one file out to every file reading one of its outputs', () => {
    const pipeline = deriveInferredPipeline([
      job('a', 'Split', {
        name: 'split',
        input: { format: 'csv', path: '/raw/orders' },
        outputs: [
          { format: 'parquet', path: '/lake/ok', mode: 'overwrite' },
          { format: 'parquet', path: '/lake/rejected', mode: 'overwrite' },
        ],
      }),
      job(
        'b',
        'Curate ok',
        pipe(
          'ok',
          { format: 'parquet', path: '/lake/ok' },
          {
            format: 'delta',
            path: '/lake/gold/ok',
          },
        ),
      ),
      job(
        'c',
        'Reprocess',
        pipe(
          'rejected',
          { format: 'parquet', path: '/lake/rejected' },
          {
            format: 'delta',
            path: '/lake/gold/retry',
          },
        ),
      ),
    ])

    expect(pipeline.edges).toHaveLength(2)
    expect(linkOf(pipeline.edges, 'a', 'b')?.locations).toEqual(['/lake/ok'])
    expect(linkOf(pipeline.edges, 'a', 'c')?.locations).toEqual(['/lake/rejected'])
    expect(pipeline.nodes.find((node) => node.jobId === 'a')?.outputs).toHaveLength(2)
  })

  it('merges several shared locations into one edge between the same pair', () => {
    const pipeline = deriveInferredPipeline([
      job('a', 'Split', {
        name: 'split',
        input: { format: 'csv', path: '/raw/orders' },
        outputs: [
          { format: 'parquet', path: '/lake/ok' },
          { format: 'parquet', path: '/lake/rejected' },
        ],
      }),
      job(
        'b',
        'Merge back',
        pipe(
          'merge',
          { format: 'parquet', path: '/lake/ok' },
          { format: 'delta', path: '/lake/gold/all' },
          {
            transformations: [
              { type: 'union', with: { format: 'parquet', path: '/lake/rejected' } },
            ],
          },
        ),
      ),
    ])

    expect(pipeline.edges).toHaveLength(1)
    expect(pipeline.edges[0].locations).toEqual(['/lake/ok', '/lake/rejected'])
  })
})

describe('deriveInferredPipeline — temp view links', () => {
  it('links a temp view written by one file to the file reading it', () => {
    const pipeline = deriveInferredPipeline([
      job(
        'a',
        'Stage',
        pipe(
          'stage',
          { format: 'csv', path: '/raw/orders' },
          {
            format: 'view',
            path: 'staging_orders',
          },
        ),
      ),
      job(
        'b',
        'Commit',
        pipe(
          'commit',
          { format: 'view', path: 'staging_orders' },
          {
            format: 'delta',
            path: '/lake/gold/orders',
          },
        ),
      ),
    ])

    expect(pipeline.edges).toHaveLength(1)
    expect(pipeline.edges[0]).toMatchObject({ source: 'a', target: 'b', via: 'view' })
  })

  it('matches a global temp view against its bare name, in both directions', () => {
    const pipeline = deriveInferredPipeline([
      job(
        'a',
        'Stage',
        pipe(
          'stage',
          { format: 'csv', path: '/raw/orders' },
          {
            format: 'view',
            path: 'global_temp.staging_orders',
          },
        ),
      ),
      job(
        'b',
        'Commit',
        pipe(
          'commit',
          { format: 'view', path: 'staging_orders' },
          {
            format: 'view',
            path: 'GLOBAL_TEMP.committed',
          },
        ),
      ),
      job(
        'c',
        'Report',
        pipe(
          'report',
          { format: 'view', path: 'committed' },
          {
            format: 'csv',
            path: '/out/report',
          },
        ),
      ),
    ])

    expect(linkOf(pipeline.edges, 'a', 'b')?.via).toBe('view')
    expect(linkOf(pipeline.edges, 'b', 'c')?.via).toBe('view')
    expect(pipeline.nodes.map((node) => node.order)).toEqual([1, 2, 3])
  })

  it('never links a view to a path that merely shares its name', () => {
    const pipeline = deriveInferredPipeline([
      job(
        'a',
        'Stage',
        pipe('stage', { format: 'csv', path: '/raw' }, { format: 'view', path: 'orders' }),
      ),
      job(
        'b',
        'Read table',
        pipe(
          'table',
          { format: 'delta', path: 'orders' },
          {
            format: 'csv',
            path: '/out',
          },
        ),
      ),
    ])

    expect(pipeline.edges).toEqual([])
  })
})

describe('deriveInferredPipeline — node contents', () => {
  it('lists the steps in execution order with counts and endpoints', () => {
    const pipeline = deriveInferredPipeline([
      job('a', 'Curate', {
        name: 'curate',
        input: { format: 'parquet', path: '/lake/bronze/orders' },
        transformations: [
          { type: 'filter', condition: 'status = 1' },
          { type: 'select', columns: ['id', 'total'] },
          {
            type: 'join',
            with: { format: 'delta', path: '/lake/dim/customers' },
            on: 'id',
            how: 'left',
          },
        ],
        validations: {
          on_failure: 'warn',
          rules: [
            { type: 'not_null', columns: ['id'] },
            { type: 'row_count', min: 1 },
          ],
        },
        output: {
          format: 'delta',
          path: '/lake/gold/orders',
          mode: 'append',
          transformations: [
            { type: 'with_column', column: 'ingested_at', expression: 'now()' },
          ],
        },
      }),
    ])

    const [node] = pipeline.nodes
    expect(node.order).toBe(1)
    expect(node.compiled).toBe(true)
    expect(node.input).toEqual({ format: 'parquet', path: '/lake/bronze/orders' })
    expect(node.outputs).toEqual([
      { format: 'delta', path: '/lake/gold/orders', mode: 'append' },
    ])
    expect(node.transformationCount).toBe(4)
    expect(node.hasValidations).toBe(true)

    expect(node.steps.map((step) => [step.kind, step.type])).toEqual([
      ['input', 'parquet'],
      ['transformation', 'filter'],
      ['transformation', 'select'],
      ['transformation', 'join'],
      ['validations', 'validations'],
      ['transformation', 'with_column'],
      ['output', 'delta'],
    ])
    expect(node.steps[1].detail).toBe('status = 1')
    expect(node.steps[5].detail).toBe('ingested_at = now()')
    expect(node.steps[3].detail).toBe('left · /lake/dim/customers')
    expect(node.steps[4].detail).toBe('2 rules · on failure: warn')
    expect(node.steps[6].detail).toBe('/lake/gold/orders')
    // Labels come from the catalog so the box reads like the palette.
    expect(node.steps[1].label).toBe('Filter')
  })

  it('still describes a job that does not compile', () => {
    const orphan = job(
      'a',
      'Draft',
      pipe('draft', { format: 'csv', path: '/raw' }, { format: 'csv', path: '/out' }),
    )
    // Drop every destination: the pipeline no longer compiles.
    orphan.graph = {
      nodes: orphan.graph.nodes.filter((node) => node.data.kind !== 'sink'),
      edges: [],
    }

    const [node] = deriveInferredPipeline([orphan]).nodes
    expect(node.compiled).toBe(false)
    expect(node.input).toEqual({ format: 'csv', path: '/raw' })
    expect(node.outputs).toEqual([])
    expect(node.order).toBe(1)
  })

  it('leaves unrelated files unconnected and orders them by name', () => {
    const pipeline = deriveInferredPipeline([
      job(
        'b',
        'Zulu',
        pipe('zulu', { format: 'csv', path: '/raw/z' }, { format: 'csv', path: '/out/z' }),
      ),
      job(
        'a',
        'Alpha',
        pipe('alpha', { format: 'csv', path: '/raw/a' }, { format: 'csv', path: '/out/a' }),
      ),
    ])

    expect(pipeline.edges).toEqual([])
    expect(pipeline.nodes.map((node) => node.name)).toEqual(['Alpha', 'Zulu'])
  })
})
