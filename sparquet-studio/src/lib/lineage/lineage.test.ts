import { describe, expect, it } from 'vitest'

import { buildLineage, datasetKey, datasetsOfJob, endpointsOfGraph, lineageOfJob } from './lineage'
import type {
  Job,
  JobSettings,
  SinkNode,
  SourceNode,
  StudioEdge,
  StudioGraph,
  TransformNode,
  ValidationSinkRole,
} from '@/types/studio'
import { HANDLE } from '@/types/studio'

/* ---------------------------------------------------------------- helpers */

const SETTINGS: JobSettings = { pipelineName: 'test', description: '', spark: {} }

const sourceNode = (
  id: string,
  format = 'csv',
  path = '/in',
  options: Record<string, unknown> = {},
): SourceNode => ({
  id,
  type: 'source',
  position: { x: 0, y: 0 },
  data: { kind: 'source', format, path, options },
})

const transformNode = (
  id: string,
  transform: string,
  disabled?: boolean,
): TransformNode => ({
  id,
  type: 'transform',
  position: { x: 0, y: 0 },
  data: { kind: 'transform', transform, params: {}, ...(disabled ? { disabled } : {}) },
})

const sinkNode = (
  id: string,
  format = 'parquet',
  path = '/out',
  dqRole?: ValidationSinkRole,
  options: Record<string, unknown> = {},
): SinkNode => ({
  id,
  type: 'sink',
  position: { x: 0, y: 0 },
  data: {
    kind: 'sink',
    format,
    path,
    mode: 'overwrite',
    partitionBy: [],
    columns: null,
    options,
    ...(dqRole ? { dqRole } : {}),
  },
})

const edge = (source: string, target: string, targetHandle: string = HANDLE.in): StudioEdge => ({
  id: `${source}-${target}-${targetHandle}`,
  source,
  target,
  sourceHandle: HANDLE.out,
  targetHandle,
})

const job = (id: string, name: string, graph: StudioGraph, workflowId = 'wf'): Job => ({
  id,
  workflowId,
  name,
  description: '',
  tags: [],
  settings: SETTINGS,
  graph,
  params: [],
  createdAt: 0,
  updatedAt: 0,
  revision: 1,
})

/* ------------------------------------------------------------ endpoints */

describe('endpointsOfGraph', () => {
  it('reads a source and writes a sink', () => {
    const graph: StudioGraph = {
      nodes: [sourceNode('s'), sinkNode('o')],
      edges: [edge('s', 'o')],
    }
    expect(endpointsOfGraph(graph)).toEqual([
      { role: 'input', format: 'csv', address: '/in', nodeId: 's', label: undefined },
      {
        role: 'output',
        format: 'parquet',
        address: '/out',
        mode: 'overwrite',
        nodeId: 'o',
        label: undefined,
      },
    ])
  })

  it('calls the second source of a join a join, not another input', () => {
    const graph: StudioGraph = {
      nodes: [sourceNode('left'), sourceNode('right', 'parquet', '/dim'), transformNode('j', 'join')],
      edges: [edge('left', 'j'), edge('right', 'j', HANDLE.inRight)],
    }
    const roles = endpointsOfGraph(graph).map((endpoint) => [endpoint.address, endpoint.role])
    expect(roles).toEqual([
      ['/in', 'input'],
      ['/dim', 'join'],
    ])
  })

  it('tells a union apart from a join, because the framework reads both sides either way', () => {
    const graph: StudioGraph = {
      nodes: [sourceNode('left'), sourceNode('right', 'parquet', '/more'), transformNode('u', 'union')],
      edges: [edge('left', 'u'), edge('right', 'u', HANDLE.inRight)],
    }
    expect(endpointsOfGraph(graph)[1].role).toBe('union')
  })

  it('finds the side input through the sub-chain a with_transformations builds', () => {
    // source → select → join(in-right): what the compiler draws for a join whose
    // right side is filtered before the join runs.
    const graph: StudioGraph = {
      nodes: [
        sourceNode('left'),
        sourceNode('right', 'parquet', '/dim'),
        transformNode('sel', 'select'),
        transformNode('j', 'join'),
      ],
      edges: [
        edge('left', 'j'),
        edge('right', 'sel'),
        edge('sel', 'j', HANDLE.inRight),
      ],
    }
    expect(endpointsOfGraph(graph).map((endpoint) => [endpoint.address, endpoint.role])).toEqual([
      ['/in', 'input'],
      ['/dim', 'join'],
    ])
  })

  it('drops the side source of a disabled join — a muted step reads nothing', () => {
    const graph: StudioGraph = {
      nodes: [sourceNode('left'), sourceNode('right', 'parquet', '/dim'), transformNode('j', 'join', true)],
      edges: [edge('left', 'j'), edge('right', 'j', HANDLE.inRight)],
    }
    expect(endpointsOfGraph(graph).map((endpoint) => endpoint.address)).toEqual(['/in'])
  })

  it('keeps a source nobody wired up — an address is declared even before the chain is', () => {
    const graph: StudioGraph = { nodes: [sourceNode('s')], edges: [] }
    expect(endpointsOfGraph(graph)[0].role).toBe('input')
  })

  it('maps a quality destination to the dataset the validations block writes', () => {
    const graph: StudioGraph = {
      nodes: [
        sinkNode('r', 'json', '/dq/report', 'report'),
        sinkNode('v', 'parquet', '/dq/valid', 'valid'),
        sinkNode('i', 'parquet', '/dq/invalid', 'invalid'),
      ],
      edges: [],
    }
    expect(endpointsOfGraph(graph).map((endpoint) => endpoint.role)).toEqual([
      'validation:report',
      'validation:valid',
      'validation:invalid',
    ])
  })

  it('falls through to options for the formats that name the dataset there', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('k', 'kafka', '', { topic: 'orders' }),
        sinkNode('j', 'jdbc', '', undefined, { dbtable: 'public.orders' }),
      ],
      edges: [],
    }
    expect(endpointsOfGraph(graph).map((endpoint) => endpoint.address)).toEqual([
      'orders',
      'public.orders',
    ])
  })

  it('ignores a node with no address at all — there is nothing to trace', () => {
    const graph: StudioGraph = { nodes: [sourceNode('s', 'csv', '   '), sinkNode('o', 'parquet', '')], edges: [] }
    expect(endpointsOfGraph(graph)).toEqual([])
  })
})

/* ------------------------------------------------------------- identity */

describe('datasetKey', () => {
  it('treats a trailing slash as the same directory', () => {
    expect(datasetKey('/lake/silver/orders/')).toBe(datasetKey('/lake/silver/orders'))
  })

  it('keeps case, because storage does', () => {
    expect(datasetKey('/lake/Orders')).not.toBe(datasetKey('/lake/orders'))
  })
})

/* ------------------------------------------------------------- job view */

describe('lineageOfJob', () => {
  it('splits what the job reads from what it writes', () => {
    const result = lineageOfJob(
      job('j1', 'bronze to silver', {
        nodes: [sourceNode('s', 'csv', '/bronze'), sinkNode('o', 'parquet', '/silver')],
        edges: [edge('s', 'o')],
      }),
    )
    expect(result.reads.map((endpoint) => endpoint.address)).toEqual(['/bronze'])
    expect(result.writes.map((endpoint) => endpoint.address)).toEqual(['/silver'])
  })
})

/* --------------------------------------------------------------- index */

describe('buildLineage', () => {
  it('names every workflow that touches a dataset', () => {
    const writer = job(
      'w',
      'writes it',
      { nodes: [sourceNode('s'), sinkNode('o', 'parquet', '/shared')], edges: [edge('s', 'o')] },
      'wf-a',
    )
    const reader = job(
      'r',
      'reads it',
      {
        nodes: [sourceNode('s', 'parquet', '/shared'), sinkNode('o', 'parquet', '/out')],
        edges: [edge('s', 'o')],
      },
      'wf-b',
    )

    const index = buildLineage([writer, reader])
    const shared = index.datasets.find((dataset) => dataset.key === '/shared')

    // The handoff crosses a workflow boundary: neither side can see it alone.
    expect(shared?.workflowIds).toEqual(['wf-a', 'wf-b'])
    expect(shared?.place).toBe('intermediate')
    expect(
      index.datasets.find((dataset) => dataset.key === '/out')?.workflowIds,
    ).toEqual(['wf-b'])
  })

  const bronzeToSilver = job('j1', 'bronze to silver', {
    nodes: [sourceNode('s', 'csv', '/bronze'), sinkNode('o', 'parquet', '/silver')],
    edges: [edge('s', 'o')],
  })
  const silverToGold = job('j2', 'silver to gold', {
    nodes: [sourceNode('s', 'parquet', '/silver/'), sinkNode('o', 'delta', '/gold')],
    edges: [edge('s', 'o')],
  })

  it('joins two jobs on the address one writes and the other reads', () => {
    const index = buildLineage([bronzeToSilver, silverToGold])
    const silver = index.datasets.find((dataset) => dataset.key === '/silver')

    expect(silver?.place).toBe('intermediate')
    expect(silver?.producers.map((mention) => mention.jobId)).toEqual(['j1'])
    expect(silver?.consumers.map((mention) => mention.jobId)).toEqual(['j2'])
    expect(index.edges).toEqual([{ from: 'j1', to: 'j2', datasetKey: '/silver' }])
  })

  it('places the ends of the chain: read-only is external, write-only is terminal', () => {
    const index = buildLineage([bronzeToSilver, silverToGold])
    const place = (key: string) => index.datasets.find((dataset) => dataset.key === key)?.place
    expect(place('/bronze')).toBe('external')
    expect(place('/gold')).toBe('terminal')
  })

  it('records every format an address was named with', () => {
    const index = buildLineage([bronzeToSilver, silverToGold])
    expect(index.datasets.find((dataset) => dataset.key === '/silver')?.formats).toEqual([
      'parquet',
    ])
  })

  it('flags a handoff made of temporary views: it only holds inside one run', () => {
    const writer = job('a', 'writes a view', {
      nodes: [sourceNode('s', 'csv', '/in'), sinkNode('o', 'view', 'staging')],
      edges: [edge('s', 'o')],
    })
    const reader = job('b', 'reads the view', {
      nodes: [sourceNode('s', 'view', 'staging'), sinkNode('o', 'parquet', '/out')],
      edges: [edge('s', 'o')],
    })
    const index = buildLineage([writer, reader])
    expect(index.datasets.find((dataset) => dataset.key === 'staging')?.sessionScoped).toBe(true)
    expect(index.datasets.find((dataset) => dataset.key === '/out')?.sessionScoped).toBe(false)
  })

  it('does not make a job depend on itself when it reads back what it wrote', () => {
    const cycle = job('j', 'reprocess in place', {
      nodes: [sourceNode('s', 'delta', '/table'), sinkNode('o', 'delta', '/table')],
      edges: [edge('s', 'o')],
    })
    const index = buildLineage([cycle])
    expect(index.edges).toEqual([])
    expect(index.datasets[0].place).toBe('intermediate')
  })

  it('sorts datasets by address so the screen does not shuffle between renders', () => {
    const index = buildLineage([silverToGold, bronzeToSilver])
    expect(index.datasets.map((dataset) => dataset.key)).toEqual([
      '/bronze',
      '/gold',
      '/silver',
    ])
  })

  it('has nothing to say about an empty library', () => {
    expect(buildLineage([])).toEqual({ jobs: [], datasets: [], edges: [] })
  })
})

describe('datasetsOfJob', () => {
  it('returns what one job touches, on either side', () => {
    const index = buildLineage([
      job('j1', 'one', {
        nodes: [sourceNode('s', 'csv', '/bronze'), sinkNode('o', 'parquet', '/silver')],
        edges: [edge('s', 'o')],
      }),
      job('j2', 'two', {
        nodes: [sourceNode('s', 'parquet', '/other'), sinkNode('o', 'delta', '/gold')],
        edges: [edge('s', 'o')],
      }),
    ])
    expect(datasetsOfJob(index, 'j1').map((dataset) => dataset.key)).toEqual(['/bronze', '/silver'])
  })
})
