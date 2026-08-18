import { describe, expect, it } from 'vitest'

import type { Job, StudioGraph, StudioNode, ValidationNodeData } from '@/types/studio'
import { HANDLE } from '@/types/studio'

import { upgradeJob, upgradeValidations, upgradeValidationSinks } from './migrations'

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
    // v3 only lifts the block off the node; `upgradeValidationSinks` (v4) is what
    // turns the two datasets into canvas nodes.
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

describe('upgradeValidationSinks', () => {
  /** A v3 record: rules already split into nodes, datasets still in the settings. */
  const ruleNode = (id: string, validator = 'not_null'): StudioNode =>
    ({
      id,
      type: 'validation',
      position: { x: 200, y: 0 },
      data: { kind: 'validation', validator, params: { columns: ['id'] } },
    }) as StudioNode

  const chain = (): StudioGraph => ({
    nodes: [node('src', 'source'), ruleNode('v1'), ruleNode('v2', 'unique'), node('out', 'sink')],
    edges: [edge('src', 'v1'), edge('v1', 'v2'), edge('v2', 'out')],
  })

  const withPolicy = (validations: Record<string, unknown>) =>
    ({ ...settings, validations }) as unknown as typeof settings

  it('hangs each dataset off the last rule, through its own handle', () => {
    const upgraded = upgradeValidationSinks(
      chain(),
      withPolicy({
        onFailure: 'warn',
        report: { format: 'csv', path: '/dq', mode: 'overwrite' },
        outputs: {
          valid: { format: 'delta', path: 'silver.ok', mode: 'overwrite' },
          invalid: { format: 'delta', path: 'silver.bad', mode: 'append' },
        },
      }),
    )

    expect(upgraded.changed).toBe(true)
    // Only the run policy is left behind.
    expect(upgraded.settings.validations).toEqual({ onFailure: 'warn' })

    const sideEdges = upgraded.graph.edges.filter(
      (item) => item.sourceHandle !== undefined && item.sourceHandle !== HANDLE.out,
    )
    expect(sideEdges.map((item) => item.sourceHandle)).toEqual([
      HANDLE.outReport,
      HANDLE.outValid,
      HANDLE.outInvalid,
    ])
    // "After all rules ran" is the last rule, not the first.
    expect(sideEdges.every((item) => item.source === 'v2')).toBe(true)

    const sinks = upgraded.graph.nodes.filter((item) => item.data.kind === 'sink')
    expect(sinks.map((item) => item.id)).toEqual(['out', 'v2-dq-report', 'v2-dq-valid', 'v2-dq-invalid'])
    // The job's own destination is untouched: the side outputs are drawn beside it.
    expect(upgraded.graph.edges).toEqual(
      expect.arrayContaining([expect.objectContaining({ source: 'v2', target: 'out' })]),
    )

    const invalid = sinks.find((item) => item.id === 'v2-dq-invalid')
    expect(invalid?.data.kind === 'sink' && invalid.data.mode).toBe('append')
  })

  it('drops the datasets when there is no rule to attach them to', () => {
    const graph: StudioGraph = {
      nodes: [node('src', 'source'), node('out', 'sink')],
      edges: [edge('src', 'out')],
    }

    const upgraded = upgradeValidationSinks(
      graph,
      withPolicy({ onFailure: 'warn', report: { format: 'csv', path: '/dq' } }),
    )

    expect(upgraded.changed).toBe(true)
    expect(upgraded.settings.validations).toEqual({ onFailure: 'warn' })
    expect(upgraded.graph.nodes).toHaveLength(2)
  })

  it('leaves a record that has only a run policy untouched', () => {
    const graph = chain()
    const stored = withPolicy({ onFailure: 'skip' })
    const upgraded = upgradeValidationSinks(graph, stored)

    expect(upgraded.changed).toBe(false)
    expect(upgraded.graph).toBe(graph)
    expect(upgraded.settings).toBe(stored)
  })

  it('is idempotent: a second pass changes nothing', () => {
    const once = upgradeValidationSinks(
      chain(),
      withPolicy({ onFailure: 'warn', report: { format: 'csv', path: '/dq' } }),
    )
    const twice = upgradeValidationSinks(once.graph, once.settings)

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

  it('runs both hops: the rules split AND the datasets become nodes', () => {
    const legacy: Job = {
      ...job,
      graph: {
        nodes: [
          node('src', 'source'),
          legacyNode('checks', {
            onFailure: 'warn',
            rules: [{ type: 'not_null', columns: ['id'] }],
            report: { format: 'csv', path: '/dq', mode: 'overwrite' },
          }),
          node('out', 'sink'),
        ],
        edges: [edge('src', 'checks'), edge('checks', 'out')],
      },
    }

    const upgraded = upgradeJob(legacy)

    expect(upgraded.settings.validations).toEqual({ onFailure: 'warn' })
    const reportEdge = upgraded.graph.edges.find(
      (item) => item.sourceHandle === HANDLE.outReport,
    )
    expect(reportEdge).toBeDefined()
    const sink = upgraded.graph.nodes.find((item) => item.id === reportEdge?.target)
    expect(sink?.data.kind === 'sink' && sink.data.path).toBe('/dq')

    // Running it again is a no-op, so the storage migration can skip the write.
    expect(upgradeJob(upgraded)).toBe(upgraded)
  })
})
