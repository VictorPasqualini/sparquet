import { describe, expect, it } from 'vitest'

import { pipelineToGraph } from '@/lib/compiler'
import { describeJob, topologicalOrder } from '@/lib/pipeline/describe'
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

/* ------------------------------------------------------------------ tests */

describe('describeJob', () => {
  it('lists the steps in execution order with counts and endpoints', () => {
    const described = describeJob(
      job('a', 'Curate', {
        name: 'curate',
        input: { format: 'parquet', path: '/lake/bronze/orders' },
        transformations: [
          { type: 'filter', condition: 'status = 1' },
          { type: 'select', columns: ['id', 'total'] },
          {
            type: 'join',
            input: { format: 'delta', path: '/lake/dim/customers' },
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
    )

    expect(described.compiled).toBe(true)
    expect(described.input).toEqual({ format: 'parquet', path: '/lake/bronze/orders' })
    expect(described.outputs).toEqual([
      { format: 'delta', path: '/lake/gold/orders', mode: 'append' },
    ])
    expect(described.transformationCount).toBe(4)
    expect(described.hasValidations).toBe(true)
    expect(described.validationRuleCount).toBe(2)

    expect(described.steps.map((step) => [step.kind, step.type])).toEqual([
      ['input', 'parquet'],
      ['transformation', 'filter'],
      ['transformation', 'select'],
      ['transformation', 'join'],
      ['validations', 'validations'],
      ['transformation', 'with_column'],
      ['output', 'delta'],
    ])
    expect(described.steps[1].detail).toBe('status = 1')
    expect(described.steps[5].detail).toBe('ingested_at = now()')
    expect(described.steps[3].detail).toBe('left · /lake/dim/customers')
    expect(described.steps[4].detail).toBe('2 rules · on failure: warn')
    expect(described.steps[6].detail).toBe('/lake/gold/orders')
    // Labels come from the catalog so the box reads like the palette.
    expect(described.steps[1].label).toBe('Filter')
  })

  it('describes every destination of a multi-output job', () => {
    const described = describeJob(
      job('a', 'Split', {
        name: 'split',
        input: { format: 'csv', path: '/raw/orders' },
        outputs: [
          { format: 'parquet', path: '/lake/ok', mode: 'overwrite' },
          { format: 'parquet', path: '/lake/rejected', mode: 'overwrite' },
        ],
      }),
    )

    expect(described.outputs).toEqual([
      { format: 'parquet', path: '/lake/ok', mode: 'overwrite' },
      { format: 'parquet', path: '/lake/rejected', mode: 'overwrite' },
    ])
    expect(described.steps.filter((step) => step.kind === 'output')).toHaveLength(2)
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

    const described = describeJob(orphan)
    expect(described.compiled).toBe(false)
    expect(described.input).toEqual({ format: 'csv', path: '/raw' })
    expect(described.outputs).toEqual([])
  })
})

describe('topologicalOrder', () => {
  const names = (entries: [string, string][]) => new Map(entries)

  it('follows the links, tie-breaking by name', () => {
    const { ordered, cyclic } = topologicalOrder(
      ['c', 'a', 'b'],
      names([
        ['a', 'Alpha'],
        ['b', 'Bravo'],
        ['c', 'Charlie'],
      ]),
      [
        { source: 'b', target: 'c' },
        { source: 'a', target: 'b' },
      ],
    )

    expect(ordered).toEqual(['a', 'b', 'c'])
    expect(cyclic).toEqual([])
  })

  it('orders unlinked ids by name', () => {
    const { ordered } = topologicalOrder(
      ['z', 'a'],
      names([
        ['z', 'Zulu'],
        ['a', 'Alpha'],
      ]),
      [],
    )

    expect(ordered).toEqual(['a', 'z'])
  })

  it('emits whatever a cycle traps last, and reports it', () => {
    const { ordered, cyclic } = topologicalOrder(
      ['a', 'b', 'c'],
      names([
        ['a', 'Alpha'],
        ['b', 'Bravo'],
        ['c', 'Charlie'],
      ]),
      [
        { source: 'b', target: 'c' },
        { source: 'c', target: 'b' },
      ],
    )

    expect(ordered).toEqual(['a', 'b', 'c'])
    expect(cyclic).toEqual(['b', 'c'])
  })

  it('ignores a self-link rather than deadlocking on it', () => {
    const { ordered, cyclic } = topologicalOrder(
      ['a'],
      names([['a', 'Alpha']]),
      [{ source: 'a', target: 'a' }],
    )

    expect(ordered).toEqual(['a'])
    expect(cyclic).toEqual([])
  })
})
