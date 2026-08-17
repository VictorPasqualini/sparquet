import { describe, expect, it } from 'vitest'

import { getFormat } from '@/catalog'
import { lintJob } from '@/lib/validation/lint'
import { HANDLE } from '@/types/studio'
import type { SinkNodeData, StudioEdge, StudioNode, JobSettings } from '@/types/studio'

const SETTINGS: JobSettings = { pipelineName: 'test', description: '', spark: {} }

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
  lintJob(
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
