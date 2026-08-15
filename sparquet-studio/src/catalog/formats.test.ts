import { describe, expect, it } from 'vitest'

import { getFormat } from '@/catalog'
import { lintWorkflow } from '@/lib/validation/lint'
import { HANDLE } from '@/types/studio'
import type { SinkNodeData, StudioEdge, StudioNode, WorkflowSettings } from '@/types/studio'

const SETTINGS: WorkflowSettings = { pipelineName: 'test', description: '', spark: {} }

const source = (id: string): StudioNode => ({
  id,
  type: 'source',
  position: { x: 0, y: 0 },
  data: { kind: 'source', format: 'parquet', path: '/in', options: {} },
})

const sink = (id: string, patch: Partial<SinkNodeData> = {}): StudioNode => ({
  id,
  type: 'sink',
  position: { x: 0, y: 0 },
  data: {
    kind: 'sink',
    format: 'parquet',
    path: '/out',
    mode: 'overwrite',
    partitionBy: [],
    columns: null,
    options: {},
    ...patch,
  },
})

const link = (from: string, to: string): StudioEdge => ({
  id: `${from}->${to}`,
  source: from,
  target: to,
  sourceHandle: HANDLE.out,
  targetHandle: HANDLE.in,
})

const kafkaSink = (options: Record<string, unknown>): StudioNode =>
  sink('out', { format: 'kafka', path: 'registro-lastros', mode: 'append', options })

const bootstrapIssues = (options: Record<string, unknown>): string[] =>
  lintWorkflow(
    { nodes: [source('src'), kafkaSink(options)], edges: [link('src', 'out')] },
    SETTINGS,
    [],
  )
    .filter((issue) => issue.field === 'options.bootstrap_servers')
    .map((issue) => issue.message)

describe('kafka bootstrap servers', () => {
  it('accepts the canonical kafka.bootstrap.servers option on its own', () => {
    expect(
      bootstrapIssues({ 'kafka.bootstrap.servers': 'broker1:9092', value_column: 'payload' }),
    ).toEqual([])
  })

  it('accepts the friendly alias on its own', () => {
    expect(bootstrapIssues({ bootstrap_servers: 'broker1:9092' })).toEqual([])
  })

  it('reports the pair when neither key is set', () => {
    expect(bootstrapIssues({ value_column: 'payload' })).toHaveLength(1)
  })

  it('reports the pair when both keys are blank', () => {
    expect(
      bootstrapIssues({ bootstrap_servers: '   ', 'kafka.bootstrap.servers': '' }),
    ).toHaveLength(1)
  })
})

describe('iceberg gotchas', () => {
  it('states that a mis-cased MERGE fails the write instead of falling back to one', () => {
    const gotcha = getFormat('iceberg')?.gotchas.find((entry) => entry.includes('"MERGE"'))
    expect(gotcha).toBeDefined()
    expect(gotcha).toContain('Unknown save mode')
    expect(gotcha).not.toMatch(/silently becomes an ordinary write/)
  })
})

describe('database connectors', () => {
  const IDS = ['jdbc', 'postgres', 'mysql', 'sqlserver', 'oracle']

  it('registers every vendor as readable and writable', () => {
    for (const id of IDS) {
      const def = getFormat(id)
      expect(def, id).toBeDefined()
      expect(def?.canRead, id).toBe(true)
      expect(def?.canWrite, id).toBe(true)
      expect(def?.supportsMerge, id).toBe(true)
      // partition_by is a filesystem concept the JDBC writer ignores.
      expect(def?.supportsPartitioning, id).toBe(false)
    }
  })

  it('offers the write modes the connector implements', () => {
    expect(getFormat('postgres')?.modes).toEqual([
      'append',
      'overwrite',
      'merge',
      'ignore',
      'error',
    ])
  })

  it('names the table, not a path', () => {
    expect(getFormat('mysql')?.pathLabel).toBe('Table')
    expect(getFormat('mysql')?.pathPlaceholder).toContain('.')
  })

  it('requires a URL only on the generic connector', () => {
    const generic = getFormat('jdbc')?.readOptions.find((field) => field.key === 'url')
    const postgres = getFormat('postgres')?.readOptions.find((field) => field.key === 'url')
    expect(generic?.required).toBe(true)
    expect(postgres?.required).toBeFalsy()
  })

  it('accepts host + database instead of a URL', () => {
    const url = getFormat('postgres')?.readOptions.find((field) => field.key === 'url')
    expect(url?.validate?.('', { host: 'db', database: 'sales' })).toBeNull()
    expect(url?.validate?.('', {})).toMatch(/URL, or a host/)
  })

  it('requires the full set of parallel-read bounds', () => {
    const field = getFormat('postgres')?.readOptions.find(
      (entry) => entry.key === 'partition_column',
    )
    expect(field?.validate?.('id', { lower_bound: 1, upper_bound: 10, num_partitions: 4 })).toBeNull()
    expect(field?.validate?.('id', { lower_bound: 1 })).toMatch(/upper_bound/)
    // No partition column at all is the normal single-connection read.
    expect(field?.validate?.('', {})).toBeNull()
  })

  it('requires merge keys only in merge mode', () => {
    const field = getFormat('postgres')?.writeOptions.find((entry) => entry.key === 'merge_keys')
    expect(field?.validate?.([], { mode: 'append' })).toBeNull()
    expect(field?.validate?.([], { mode: 'merge' })).toMatch(/merge key/)
    expect(field?.validate?.(['id'], { mode: 'merge' })).toBeNull()
  })

  it('fills the vendor driver in the field placeholder', () => {
    const driver = getFormat('mysql')?.writeOptions.find((field) => field.key === 'driver')
    expect(driver?.placeholder).toBe('com.mysql.cj.jdbc.Driver')
    expect(driver?.required).toBeFalsy()
    expect(getFormat('jdbc')?.writeOptions.find((f) => f.key === 'driver')?.required).toBe(true)
  })

  it('warns about credentials living in the file', () => {
    const password = getFormat('postgres')?.readOptions.find((field) => field.key === 'password')
    expect(password?.help).toMatch(/environment-variable/i)
    expect(getFormat('postgres')?.gotchas.join(' ')).toMatch(/password_env/)
  })
})
