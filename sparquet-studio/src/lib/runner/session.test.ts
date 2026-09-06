import { describe, expect, it } from 'vitest'

import { sparkForDatasets } from './session'
import type { Job, JobSettings, SinkNode, SourceNode, StudioGraph } from '@/types/studio'
import { HANDLE } from '@/types/studio'

const DELTA = {
  'spark.jars.packages': 'io.delta:delta-spark_2.13:4.3.1',
  'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
  'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
}

const settings = (configs: Record<string, string> = {}): JobSettings => ({
  pipelineName: 'test',
  description: '',
  spark: { configs },
})

const graph = (inPath: string, outPath: string, format = 'delta'): StudioGraph => {
  const source: SourceNode = {
    id: 's',
    type: 'source',
    position: { x: 0, y: 0 },
    data: { kind: 'source', format, path: inPath, options: {} },
  }
  const sink: SinkNode = {
    id: 'o',
    type: 'sink',
    position: { x: 0, y: 0 },
    data: {
      kind: 'sink',
      format,
      path: outPath,
      mode: 'overwrite',
      partitionBy: [],
      columns: null,
      options: {},
    },
  }
  return {
    nodes: [source, sink],
    edges: [{ id: 'e', source: 's', target: 'o', sourceHandle: HANDLE.out, targetHandle: HANDLE.in }],
  }
}

const job = (id: string, spark: Record<string, string>, from: string, to: string): Job => ({
  id,
  workflowId: 'wf',
  name: id,
  description: '',
  tags: [],
  settings: settings(spark),
  graph: graph(from, to),
  params: [],
  createdAt: 0,
  updatedAt: 0,
  revision: 1,
})

describe('sparkForDatasets', () => {
  it('takes the configs from the Job that reads the dataset', () => {
    const jobs = [job('bronze', DELTA, '/raw/orders', '/bronze/orders')]
    expect(sparkForDatasets(jobs, ['/bronze/orders'])).toEqual({ configs: DELTA })
  })

  it('says nothing when no Job touching the dataset declares any config', () => {
    const jobs = [job('plain', {}, '/raw/orders', '/bronze/orders')]
    // `undefined` and not `{}`: an empty block would ask the runner to consider
    // restarting a session for nothing.
    expect(sparkForDatasets(jobs, ['/bronze/orders'])).toBeUndefined()
  })

  it('leaves out a Job that touches something else entirely', () => {
    const jobs = [job('other', DELTA, '/raw/people', '/bronze/people')]
    expect(sparkForDatasets(jobs, ['/bronze/orders'])).toBeUndefined()
  })

  it('unions what several Jobs declare about the datasets in one query', () => {
    const jobs = [
      job('delta', DELTA, '/raw/orders', '/bronze/orders'),
      job('iceberg', { 'spark.sql.catalog.ice': 'org.apache.iceberg.spark.SparkCatalog' },
        '/raw/people', '/bronze/people'),
    ]
    expect(sparkForDatasets(jobs, ['/bronze/orders', '/bronze/people'])).toEqual({
      configs: { ...DELTA, 'spark.sql.catalog.ice': 'org.apache.iceberg.spark.SparkCatalog' },
    })
  })

  it('matches a dataset whose address carries a trailing slash', () => {
    const jobs = [job('bronze', DELTA, '/raw/orders', '/bronze/orders/')]
    expect(sparkForDatasets(jobs, ['/bronze/orders'])).toEqual({ configs: DELTA })
  })

  it('sends nothing when the query names no dataset at all', () => {
    expect(sparkForDatasets([job('bronze', DELTA, '/a', '/b')], [])).toBeUndefined()
  })

  it('carries only configs, never the master or the app name', () => {
    const jobs: Job[] = [
      {
        ...job('bronze', DELTA, '/raw/orders', '/bronze/orders'),
        settings: {
          pipelineName: 'test',
          description: '',
          // A query has no opinion on where the cluster is; sending one would let
          // an ad hoc SELECT move somebody else's session.
          spark: { app_name: 'bronze', master: 'local[2]', configs: DELTA },
        },
      },
    ]
    expect(sparkForDatasets(jobs, ['/bronze/orders'])).toEqual({ configs: DELTA })
  })
})
