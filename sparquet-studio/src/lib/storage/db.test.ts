import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { Pipeline, Workflow, Job } from '@/types/studio'

const { memory, failWrites } = vi.hoisted(() => ({
  memory: new Map<string, unknown>(),
  failWrites: new Set<string>(),
}))

vi.mock('idb-keyval', () => ({
  createStore: () => ({}),
  get: async (key: string) => memory.get(key),
  set: async (key: string, value: unknown) => {
    if (failWrites.has(key)) throw new Error(`storage write failed for ${key}`)
    memory.set(key, value)
  },
  del: async (key: string) => {
    memory.delete(key)
  },
  keys: async () => [...memory.keys()],
}))

type Db = typeof import('@/lib/storage/db')

let db: Db

beforeEach(async () => {
  memory.clear()
  failWrites.clear()
  vi.resetModules()
  db = await import('@/lib/storage/db')
})

function makeWorkflow(overrides: Partial<Workflow> = {}): Workflow {
  return {
    id: 'p1',
    name: 'Ingestion',
    description: 'Daily loads',
    accent: 'amber',
    createdAt: 1_000,
    updatedAt: 1_000,
    ...overrides,
  }
}

function makeJob(workflowId: string, overrides: Partial<Job> = {}): Job {
  return {
    id: 'w1',
    workflowId,
    name: 'Clients',
    description: 'Reads clients and writes the silver table',
    tags: ['bronze'],
    settings: {
      pipelineName: 'clients',
      description: '',
      spark: { app_name: 'clients' },
    },
    graph: {
      nodes: [
        {
          id: 'n1',
          type: 'source',
          position: { x: 0, y: 0 },
          data: { kind: 'source', format: 'csv', path: '/data/in', options: {} },
        },
      ],
      edges: [{ id: 'e1', source: 'n1', target: 'n2' }],
    },
    params: [{ id: 'pa1', key: 'dt_ref', type: 'string', value: '2025-01-01' }],
    createdAt: 2_000,
    updatedAt: 2_000,
    revision: 1,
    ...overrides,
  }
}

function makePipeline(workflowId: string, overrides: Partial<Pipeline> = {}): Pipeline {
  return {
    id: 'f1',
    workflowId,
    name: 'Nightly load',
    description: 'Bronze then silver',
    stages: [
      { id: 's1', jobId: 'w1', position: { x: 0, y: 0 } },
      { id: 's2', jobId: 'w2', position: { x: 380, y: 0 } },
    ],
    links: [{ id: 'l1', source: 's1', target: 's2' }],
    createdAt: 4_000,
    updatedAt: 4_000,
    revision: 1,
    ...overrides,
  }
}

function bundle(extra: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    app: 'sparquet-studio',
    version: db.STORAGE_VERSION,
    exportedAt: Date.now(),
    workflows: [],
    jobs: [],
    ...extra,
  }
}

const byId = <T extends { id: string }>(items: T[]): T[] =>
  [...items].sort((a, b) => a.id.localeCompare(b.id))

describe('workflows and jobs', () => {
  it('round-trips a saved workflow and job', async () => {
    const workflow = makeWorkflow()
    const job = makeJob(workflow.id)

    await db.saveWorkflow(workflow)
    await db.saveJob(job)

    expect(await db.listWorkflows()).toEqual([workflow])
    expect(await db.getJob(job.id)).toEqual(job)
    expect(await db.listJobs(workflow.id)).toEqual([job])
    expect(await db.listJobs('other')).toEqual([])
  })

  it('stamps the storage version on first use', async () => {
    await db.saveWorkflow(makeWorkflow())
    expect(memory.get('sparquet-studio:db:meta:version')).toBe(db.STORAGE_VERSION)
  })

  it('keys every record under the sparquet-studio prefix', async () => {
    await db.saveWorkflow(makeWorkflow())
    await db.saveJob(makeJob('p1'))
    expect([...memory.keys()].every((key) => key.startsWith('sparquet-studio:'))).toBe(true)
  })

  it('updates a job in place instead of appending a copy', async () => {
    const job = makeJob('p1')
    await db.saveJob(job)
    await db.saveJob({ ...job, name: 'Clients v2', revision: 2, updatedAt: 3_000 })

    const stored = await db.listJobs()
    expect(stored).toHaveLength(1)
    expect(stored[0]?.name).toBe('Clients v2')
    expect(stored[0]?.revision).toBe(2)
  })

  it('returns null for a missing job', async () => {
    expect(await db.getJob('nope')).toBeNull()
  })
})

describe('pipelines', () => {
  it('round-trips a saved pipeline and scopes the listing to its workflow', async () => {
    const pipeline = makePipeline('p1')
    await db.savePipeline(pipeline)
    await db.savePipeline(makePipeline('p2', { id: 'f2', name: 'Reports refresh' }))

    expect(await db.getPipeline('f1')).toEqual(pipeline)
    expect((await db.listPipelines('p1')).map((f) => f.id)).toEqual(['f1'])
    expect((await db.listPipelines()).map((f) => f.id).sort()).toEqual(['f1', 'f2'])
    expect(await db.getPipeline('nope')).toBeNull()
  })

  it('keeps a stage whose job was deleted, so the canvas can flag it', async () => {
    await db.saveJob(makeJob('p1', { id: 'w1' }))
    await db.savePipeline(makePipeline('p1'))

    await db.deleteJob('w1')

    const stored = await db.getPipeline('f1')
    expect(stored?.stages.map((stage) => stage.jobId)).toEqual(['w1', 'w2'])
  })

  it('exports and imports pipelines alongside workflows and jobs', async () => {
    await db.saveWorkflow(makeWorkflow({ id: 'p1' }))
    await db.saveJob(makeJob('p1', { id: 'w1' }))
    const pipeline = makePipeline('p1')
    await db.savePipeline(pipeline)

    const exported = await db.exportAll()
    expect(exported.pipelines).toEqual([pipeline])

    const wire: unknown = JSON.parse(JSON.stringify(exported))
    await db.clearAll()
    expect(await db.listPipelines()).toEqual([])

    const summary = await db.importAll(wire)
    expect(summary).toEqual({ workflows: 1, jobs: 1, pipelines: 1, skipped: 0, merged: true })
    expect(await db.listPipelines()).toEqual([pipeline])
  })

  it('imports a bundle that predates pipelines', async () => {
    const summary = await db.importAll({
      app: 'sparquet-studio',
      version: 1,
      exportedAt: Date.now(),
      workflows: [makeWorkflow({ id: 'p1' })],
      jobs: [makeJob('p1', { id: 'w1' })],
    })

    expect(summary.pipelines).toBe(0)
    expect(await db.listPipelines()).toEqual([])
  })

  it('drops pipeline records the canvas could not walk', async () => {
    const base = makePipeline('p1')
    const summary = await db.importAll(
      bundle({
        workflows: [makeWorkflow({ id: 'p1' })],
        pipelines: [
          { ...base, id: 'f1', stages: [{ id: 's1' }] },
          { ...base, id: 'f2', stages: [{ id: 's1', jobId: 'w1', position: { x: 0 } }] },
          { ...base, id: 'f3', links: [{ source: 's1', target: 's2' }] },
          { ...base, id: 'f4', revision: undefined },
          { ...base, id: 'f9' },
        ],
      }),
    )

    expect(summary.pipelines).toBe(1)
    expect(summary.skipped).toBe(4)
    expect((await db.listPipelines()).map((f) => f.id)).toEqual(['f9'])
  })
})

describe('deleteWorkflow', () => {
  it('cascades to the jobs and pipelines of that workflow only', async () => {
    await db.saveWorkflow(makeWorkflow({ id: 'p1' }))
    await db.saveWorkflow(makeWorkflow({ id: 'p2', name: 'Reports' }))
    await db.saveJob(makeJob('p1', { id: 'w1' }))
    await db.saveJob(makeJob('p1', { id: 'w2' }))
    await db.saveJob(makeJob('p2', { id: 'w3' }))
    await db.savePipeline(makePipeline('p1', { id: 'f1' }))
    await db.savePipeline(makePipeline('p2', { id: 'f2' }))

    await db.deleteWorkflow('p1')

    expect((await db.listWorkflows()).map((p) => p.id)).toEqual(['p2'])
    expect((await db.listJobs()).map((w) => w.id)).toEqual(['w3'])
    expect((await db.listPipelines()).map((f) => f.id)).toEqual(['f2'])
  })
})

describe('duplicateJob', () => {
  it('numbers copies once the requested name is taken', async () => {
    const job = makeJob('p1', { name: 'Clients' })
    await db.saveJob(job)

    const first = await db.duplicateJob(job.id, 'Copy of Clients')
    const second = await db.duplicateJob(job.id, 'Copy of Clients')
    const third = await db.duplicateJob(job.id, 'Copy of Clients')

    expect(first?.name).toBe('Copy of Clients')
    expect(second?.name).toBe('Copy of Clients (2)')
    expect(third?.name).toBe('Copy of Clients (3)')
  })

  it('copies the graph, keeps the workflow and gets a fresh id', async () => {
    const job = makeJob('p1')
    await db.saveJob(job)

    const copy = await db.duplicateJob(job.id, 'Copy of Clients')
    expect(copy).not.toBeNull()
    expect(copy?.id).not.toBe(job.id)
    expect(copy?.workflowId).toBe('p1')
    expect(copy?.revision).toBe(1)
    expect(copy?.graph).toEqual(job.graph)
    expect(await db.listJobs()).toHaveLength(2)
  })

  it('returns null when the source is gone', async () => {
    expect(await db.duplicateJob('missing', 'Copy of missing')).toBeNull()
  })
})

describe('exportAll / importAll', () => {
  it('round-trips a bundle without losing records', async () => {
    const workflow = makeWorkflow()
    const job = makeJob(workflow.id)
    await db.saveWorkflow(workflow)
    await db.saveJob(job)

    const bundle = await db.exportAll()
    expect(bundle.app).toBe('sparquet-studio')
    expect(bundle.version).toBe(db.STORAGE_VERSION)
    expect(bundle.workflows).toEqual([workflow])
    expect(bundle.jobs).toEqual([job])

    // Survives a JSON hop, which is how the bundle actually travels.
    const wire: unknown = JSON.parse(JSON.stringify(bundle))

    await db.clearAll()
    expect(await db.listWorkflows()).toEqual([])

    const summary = await db.importAll(wire)
    expect(summary).toEqual({ workflows: 1, jobs: 1, pipelines: 0, skipped: 0, merged: true })
    expect(await db.listWorkflows()).toEqual([workflow])
    expect(await db.listJobs()).toEqual([job])
  })

  it('merges by id and keeps records missing from the bundle', async () => {
    await db.saveWorkflow(makeWorkflow({ id: 'p1' }))
    await db.saveJob(makeJob('p1', { id: 'w1' }))

    const incoming = {
      app: 'sparquet-studio',
      version: db.STORAGE_VERSION,
      exportedAt: Date.now(),
      workflows: [makeWorkflow({ id: 'p2', name: 'Reports' })],
      jobs: [makeJob('p1', { id: 'w1', name: 'Clients (imported)' })],
    }

    await db.importAll(incoming, { merge: true })

    expect(byId(await db.listWorkflows()).map((p) => p.id)).toEqual(['p1', 'p2'])
    const jobs = await db.listJobs()
    expect(jobs).toHaveLength(1)
    expect(jobs[0]?.name).toBe('Clients (imported)')
  })

  it('replaces everything when merge is false', async () => {
    await db.saveWorkflow(makeWorkflow({ id: 'p1' }))
    await db.saveJob(makeJob('p1', { id: 'w1' }))
    await db.saveJob(makeJob('p1', { id: 'w2' }))

    const replacement = makeWorkflow({ id: 'p9', name: 'Fresh start' })
    const summary = await db.importAll(
      {
        app: 'sparquet-studio',
        version: db.STORAGE_VERSION,
        exportedAt: Date.now(),
        workflows: [replacement],
        jobs: [],
      },
      { merge: false },
    )

    expect(summary.merged).toBe(false)
    expect(await db.listWorkflows()).toEqual([replacement])
    expect(await db.listJobs()).toEqual([])
  })

  it('drops malformed records and reports them as skipped', async () => {
    const summary = await db.importAll({
      app: 'sparquet-studio',
      version: db.STORAGE_VERSION,
      exportedAt: Date.now(),
      workflows: [makeWorkflow(), { id: 'broken' }],
      jobs: [{ id: 'w0', workflowId: 'p1' }],
    })

    expect(summary).toEqual({ workflows: 1, jobs: 0, pipelines: 0, skipped: 2, merged: true })
    expect(await db.listJobs()).toEqual([])
  })

  it('rejects a bundle from a newer Studio build', async () => {
    await expect(
      db.importAll({ version: db.STORAGE_VERSION + 1, workflows: [], jobs: [] }),
    ).rejects.toThrow(/newer version/i)
  })

  it('rejects a value that is not a bundle', async () => {
    await expect(db.importAll('nope')).rejects.toThrow(/invalid bundle/i)
  })

  it('keeps the library when a replace bundle has an unusable version', async () => {
    await db.saveWorkflow(makeWorkflow({ id: 'p1' }))
    await db.saveJob(makeJob('p1', { id: 'w1' }))

    for (const version of [-1, 0.5]) {
      await expect(
        db.importAll(bundle({ version, workflows: [], jobs: [] }), { merge: false }),
      ).rejects.toThrow(/whole number/i)
    }

    expect((await db.listWorkflows()).map((p) => p.id)).toEqual(['p1'])
    expect((await db.listJobs()).map((w) => w.id)).toEqual(['w1'])
  })

  it('restores the library when a replace import fails mid-write', async () => {
    await db.saveWorkflow(makeWorkflow({ id: 'p1' }))
    await db.saveJob(makeJob('p1', { id: 'w1' }))
    // Physical keys keep their pre-rename names so an existing workspace still
    // loads: a Workflow is stored under `db:project:`, a Job under `db:workflow:`,
    // a Pipeline under `db:flow:`. See the key map at the top of db.ts.
    failWrites.add('sparquet-studio:db:project:p9')

    await expect(
      db.importAll(bundle({ workflows: [makeWorkflow({ id: 'p9' })] }), { merge: false }),
    ).rejects.toThrow(/restored/i)

    expect((await db.listWorkflows()).map((p) => p.id)).toEqual(['p1'])
    expect((await db.listJobs()).map((w) => w.id)).toEqual(['w1'])
  })

  it('drops jobs whose graph nodes or edges are malformed', async () => {
    const base = makeJob('p1')
    const summary = await db.importAll(
      bundle({
        workflows: [makeWorkflow({ id: 'p1' })],
        jobs: [
          { ...base, id: 'w1', graph: { nodes: [{ id: 'n1' }], edges: [] } },
          { ...base, id: 'w2', graph: { nodes: [null], edges: [] } },
          { ...base, id: 'w3', graph: { nodes: [], edges: [{ source: 'a', target: 'b' }] } },
          { ...base, id: 'w4' },
        ],
      }),
    )

    expect(summary.jobs).toBe(1)
    expect(summary.skipped).toBe(3)
    expect((await db.listJobs()).map((w) => w.id)).toEqual(['w4'])
  })

  it('drops jobs missing the fields the screens read', async () => {
    const base = makeJob('p1')
    const without = (id: string, field: keyof Job): unknown => {
      const record: Record<string, unknown> = { ...base, id }
      delete record[field]
      return record
    }

    const summary = await db.importAll(
      bundle({
        workflows: [makeWorkflow({ id: 'p1' })],
        jobs: [
          without('w1', 'description'),
          without('w2', 'tags'),
          without('w3', 'settings'),
          without('w4', 'params'),
          makeJob('p1', { id: 'w9' }),
        ],
      }),
    )

    expect(summary.jobs).toBe(1)
    expect(summary.skipped).toBe(4)
    expect((await db.listJobs()).map((w) => w.id)).toEqual(['w9'])
  })
})

describe('corrupt records already in storage', () => {
  it('ignores them instead of handing a broken job to the screens', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await db.saveJob(makeJob('p1', { id: 'w1' }))
    // A Job lives under the legacy `db:workflow:` prefix (see db.ts key map).
    memory.set('sparquet-studio:db:workflow:bad', {
      ...makeJob('p1', { id: 'bad' }),
      graph: { nodes: [{ id: 'n1' }], edges: [] },
    })

    expect((await db.listJobs()).map((w) => w.id)).toEqual(['w1'])
    expect(await db.getJob('bad')).toBeNull()
    expect(warn).toHaveBeenCalledOnce()
    warn.mockRestore()
  })
})

describe('migrate', () => {
  it('is a no-op once the store is at the current version', async () => {
    await db.saveWorkflow(makeWorkflow())
    const before = [...memory.entries()]
    expect(await db.migrate()).toBe(db.STORAGE_VERSION)
    expect([...memory.entries()]).toEqual(before)
    expect(memory.get('sparquet-studio:db:backup')).toBeUndefined()
  })

  it('backs the previous bundle up before upgrading unversioned data', async () => {
    const workflow = makeWorkflow()
    // A Workflow lives under the legacy `db:project:` prefix (see db.ts key map).
    memory.set('sparquet-studio:db:project:p1', workflow)

    expect(await db.migrate()).toBe(db.STORAGE_VERSION)

    const backup = memory.get('sparquet-studio:db:backup')
    expect(backup).toMatchObject({ version: 0, workflows: [workflow] })
    expect(await db.listWorkflows()).toEqual([workflow])
  })

  it('rewrites a v2 job whose validations still live in one node', async () => {
    memory.set('sparquet-studio:db:meta:version', 2)
    memory.set('sparquet-studio:db:project:p1', makeWorkflow())
    memory.set(
      'sparquet-studio:db:workflow:w1',
      makeJob('p1', {
        graph: {
          nodes: [
            {
              id: 'checks',
              type: 'validations',
              position: { x: 0, y: 0 },
              data: {
                kind: 'validations',
                onFailure: 'warn',
                rules: [{ type: 'not_null', columns: ['id'] }],
                report: null,
              },
            },
          ],
          edges: [],
        } as unknown as Job['graph'],
      }),
    )

    expect(await db.migrate()).toBe(db.STORAGE_VERSION)

    const job = await db.getJob('w1')
    expect(job?.graph.nodes.map((node) => node.data.kind)).toEqual(['validation'])
    expect(job?.settings.validations).toEqual({ onFailure: 'warn' })
  })
})

describe('clearAll', () => {
  it('removes records but keeps the seeded marker', async () => {
    await db.saveWorkflow(makeWorkflow())
    await db.saveJob(makeJob('p1'))
    await db.markSeeded()

    await db.clearAll()

    expect(await db.listWorkflows()).toEqual([])
    expect(await db.listJobs()).toEqual([])
    expect(await db.isSeeded()).toBe(true)
  })
})
