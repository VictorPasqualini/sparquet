import { describe, expect, it } from 'vitest'

import type { Job, StudioGraph, StudioNode, ValidationNodeData } from '@/types/studio'

import { upgradeJob, upgradeValidations } from './migrations'

/** The pre-split node: every rule and the block policy on one box. */
const legacyNode = (id: string, data: Record<string, unknown>): StudioNode =>
  ({
    id,
    type: 'validations',
    position: { x: 200, y: 40 },
    data: { kind: 'validations', ...data },
  }) as unknown as StudioNode

const node = (id: string, kind: 'source' | 'sink'): StudioNode =>
  ({
    id,
    type: kind,
    position: { x: 0, y: 0 },
    data:
      kind === 'source'
        ? { kind, format: 'csv', path: '/in', options: {} }
        : { kind, format: 'csv', path: '/out', mode: 'overwrite', partitionBy: [], columns: null, options: {} },
  }) as StudioNode

const edge = (source: string, target: string) => ({ id: `e-${source}-${target}`, source, target })

const settings = { pipelineName: 'test', description: '', spark: {} }

const validatorsOf = (graph: StudioGraph): string[] =>
  graph.nodes
    .filter((item): item is StudioNode & { data: ValidationNodeData } => item.data.kind === 'validation')
    .map((item) => item.data.validator)

describe('upgradeValidations', () => {
  it('splits the rules into a chained run and lifts the policy into the settings', () => {
    const graph: StudioGraph = {
      nodes: [
        node('src', 'source'),
        legacyNode('checks', {
          onFailure: 'warn',
          rules: [
            { type: 'not_null', columns: ['id'] },
            { type: 'range', column: 'age', min: 0, max: 150 },
          ],
          report: { format: 'csv', path: '/dq', mode: 'overwrite' },
          outputs: { invalid: { format: 'delta', path: 'dq.bad', mode: 'overwrite' } },
        }),
        node('out', 'sink'),
      ],
      edges: [edge('src', 'checks'), edge('checks', 'out')],
    }

    const upgraded = upgradeValidations(graph, settings)

    expect(upgraded.changed).toBe(true)
    expect(validatorsOf(upgraded.graph)).toEqual(['not_null', 'range'])
    expect(upgraded.settings.validations).toEqual({
      onFailure: 'warn',
      report: { format: 'csv', path: '/dq', mode: 'overwrite' },
      outputs: { invalid: { format: 'delta', path: 'dq.bad', mode: 'overwrite' } },
    })

    // The chain still runs src → rule → rule → out, in the original order.
    const [first, second] = upgraded.graph.nodes.filter((item) => item.data.kind === 'validation')
    expect(upgraded.graph.edges).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ source: 'src', target: first.id }),
        expect.objectContaining({ source: first.id, target: second.id }),
        expect.objectContaining({ source: second.id, target: 'out' }),
      ]),
    )
    expect(upgraded.graph.edges).toHaveLength(3)
    // The rules keep every parameter the old block carried.
    const range = second.data.kind === 'validation' ? second.data.params : {}
    expect(range).toEqual({ column: 'age', min: 0, max: 150 })
  })

  it('drops an empty block and closes the chain back up', () => {
    const graph: StudioGraph = {
      nodes: [node('src', 'source'), legacyNode('checks', { onFailure: 'skip' }), node('out', 'sink')],
      edges: [edge('src', 'checks'), edge('checks', 'out')],
    }

    const upgraded = upgradeValidations(graph, settings)

    expect(upgraded.graph.nodes.map((item) => item.id)).toEqual(['src', 'out'])
    expect(upgraded.graph.edges).toEqual([
      expect.objectContaining({ source: 'src', target: 'out' }),
    ])
    expect(upgraded.settings.validations).toEqual({ onFailure: 'skip' })
  })

  it('keeps a graph without a legacy node byte-identical', () => {
    const graph: StudioGraph = {
      nodes: [node('src', 'source'), node('out', 'sink')],
      edges: [edge('src', 'out')],
    }

    const upgraded = upgradeValidations(graph, settings)

    expect(upgraded.changed).toBe(false)
    expect(upgraded.graph).toBe(graph)
    expect(upgraded.settings).toBe(settings)
  })

  it('is idempotent: a second pass changes nothing', () => {
    const graph: StudioGraph = {
      nodes: [
        node('src', 'source'),
        legacyNode('checks', { onFailure: 'fail', rules: [{ type: 'unique', columns: ['id'] }] }),
        node('out', 'sink'),
      ],
      edges: [edge('src', 'checks'), edge('checks', 'out')],
    }

    const once = upgradeValidations(graph, settings)
    const twice = upgradeValidations(once.graph, once.settings)

    expect(twice.changed).toBe(false)
    expect(twice.graph).toBe(once.graph)
  })
})

describe('upgradeJob', () => {
  const job: Job = {
    id: 'w1',
    workflowId: 'p1',
    name: 'Clients',
    description: '',
    tags: [],
    settings,
    graph: { nodes: [node('src', 'source'), node('out', 'sink')], edges: [edge('src', 'out')] },
    params: [],
    createdAt: 1,
    updatedAt: 1,
    revision: 1,
  }

  it('returns the same record when there is nothing to upgrade', () => {
    // Identity is what lets the storage migration skip the write.
    expect(upgradeJob(job)).toBe(job)
  })

  it('rewrites only the graph and the settings', () => {
    const legacy: Job = {
      ...job,
      graph: {
        nodes: [legacyNode('checks', { onFailure: 'warn', rules: [{ type: 'row_count', min: 1 }] })],
        edges: [],
      },
    }

    const upgraded = upgradeJob(legacy)

    expect(upgraded).not.toBe(legacy)
    expect(upgraded.revision).toBe(legacy.revision)
    expect(validatorsOf(upgraded.graph)).toEqual(['row_count'])
    expect(upgraded.settings.validations?.onFailure).toBe('warn')
  })
})
