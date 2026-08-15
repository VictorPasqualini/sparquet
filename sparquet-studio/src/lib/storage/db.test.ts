import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { Project, Workflow } from '@/types/studio'

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

function makeProject(overrides: Partial<Project> = {}): Project {
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

function makeWorkflow(projectId: string, overrides: Partial<Workflow> = {}): Workflow {
  return {
    id: 'w1',
    projectId,
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

function bundle(extra: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    app: 'sparquet-studio',
    version: db.STORAGE_VERSION,
    exportedAt: Date.now(),
    projects: [],
    workflows: [],
    ...extra,
  }
}

const byId = <T extends { id: string }>(items: T[]): T[] =>
  [...items].sort((a, b) => a.id.localeCompare(b.id))

describe('projects and workflows', () => {
  it('round-trips a saved project and workflow', async () => {
    const project = makeProject()
    const workflow = makeWorkflow(project.id)

    await db.saveProject(project)
    await db.saveWorkflow(workflow)

    expect(await db.listProjects()).toEqual([project])
    expect(await db.getWorkflow(workflow.id)).toEqual(workflow)
    expect(await db.listWorkflows(project.id)).toEqual([workflow])
    expect(await db.listWorkflows('other')).toEqual([])
  })

  it('stamps the storage version on first use', async () => {
    await db.saveProject(makeProject())
    expect(memory.get('sparquet-studio:db:meta:version')).toBe(db.STORAGE_VERSION)
  })

  it('keys every record under the sparquet-studio prefix', async () => {
    await db.saveProject(makeProject())
    await db.saveWorkflow(makeWorkflow('p1'))
    expect([...memory.keys()].every((key) => key.startsWith('sparquet-studio:'))).toBe(true)
  })

  it('updates a workflow in place instead of appending a copy', async () => {
    const workflow = makeWorkflow('p1')
    await db.saveWorkflow(workflow)
    await db.saveWorkflow({ ...workflow, name: 'Clients v2', revision: 2, updatedAt: 3_000 })

    const stored = await db.listWorkflows()
    expect(stored).toHaveLength(1)
    expect(stored[0]?.name).toBe('Clients v2')
    expect(stored[0]?.revision).toBe(2)
  })

  it('returns null for a missing workflow', async () => {
    expect(await db.getWorkflow('nope')).toBeNull()
  })
})

describe('deleteProject', () => {
  it('cascades to the workflows of that project only', async () => {
    await db.saveProject(makeProject({ id: 'p1' }))
    await db.saveProject(makeProject({ id: 'p2', name: 'Reports' }))
    await db.saveWorkflow(makeWorkflow('p1', { id: 'w1' }))
    await db.saveWorkflow(makeWorkflow('p1', { id: 'w2' }))
    await db.saveWorkflow(makeWorkflow('p2', { id: 'w3' }))

    await db.deleteProject('p1')

    expect((await db.listProjects()).map((p) => p.id)).toEqual(['p2'])
    expect((await db.listWorkflows()).map((w) => w.id)).toEqual(['w3'])
  })
})

describe('duplicateWorkflow', () => {
  it('numbers copies once the requested name is taken', async () => {
    const workflow = makeWorkflow('p1', { name: 'Clients' })
    await db.saveWorkflow(workflow)

    const first = await db.duplicateWorkflow(workflow.id, 'Copy of Clients')
    const second = await db.duplicateWorkflow(workflow.id, 'Copy of Clients')
    const third = await db.duplicateWorkflow(workflow.id, 'Copy of Clients')

    expect(first?.name).toBe('Copy of Clients')
    expect(second?.name).toBe('Copy of Clients (2)')
    expect(third?.name).toBe('Copy of Clients (3)')
  })

  it('copies the graph, keeps the project and gets a fresh id', async () => {
    const workflow = makeWorkflow('p1')
    await db.saveWorkflow(workflow)

    const copy = await db.duplicateWorkflow(workflow.id, 'Copy of Clients')
    expect(copy).not.toBeNull()
    expect(copy?.id).not.toBe(workflow.id)
    expect(copy?.projectId).toBe('p1')
    expect(copy?.revision).toBe(1)
    expect(copy?.graph).toEqual(workflow.graph)
    expect(await db.listWorkflows()).toHaveLength(2)
  })

  it('returns null when the source is gone', async () => {
    expect(await db.duplicateWorkflow('missing', 'Copy of missing')).toBeNull()
  })
})

describe('exportAll / importAll', () => {
  it('round-trips a bundle without losing records', async () => {
    const project = makeProject()
    const workflow = makeWorkflow(project.id)
    await db.saveProject(project)
    await db.saveWorkflow(workflow)

    const bundle = await db.exportAll()
    expect(bundle.app).toBe('sparquet-studio')
    expect(bundle.version).toBe(db.STORAGE_VERSION)
    expect(bundle.projects).toEqual([project])
    expect(bundle.workflows).toEqual([workflow])

    // Survives a JSON hop, which is how the bundle actually travels.
    const wire: unknown = JSON.parse(JSON.stringify(bundle))

    await db.clearAll()
    expect(await db.listProjects()).toEqual([])

    const summary = await db.importAll(wire)
    expect(summary).toEqual({ projects: 1, workflows: 1, skipped: 0, merged: true })
    expect(await db.listProjects()).toEqual([project])
    expect(await db.listWorkflows()).toEqual([workflow])
  })

  it('merges by id and keeps records missing from the bundle', async () => {
    await db.saveProject(makeProject({ id: 'p1' }))
    await db.saveWorkflow(makeWorkflow('p1', { id: 'w1' }))

    const incoming = {
      app: 'sparquet-studio',
      version: db.STORAGE_VERSION,
      exportedAt: Date.now(),
      projects: [makeProject({ id: 'p2', name: 'Reports' })],
      workflows: [makeWorkflow('p1', { id: 'w1', name: 'Clients (imported)' })],
    }

    await db.importAll(incoming, { merge: true })

    expect(byId(await db.listProjects()).map((p) => p.id)).toEqual(['p1', 'p2'])
    const workflows = await db.listWorkflows()
    expect(workflows).toHaveLength(1)
    expect(workflows[0]?.name).toBe('Clients (imported)')
  })

  it('replaces everything when merge is false', async () => {
    await db.saveProject(makeProject({ id: 'p1' }))
    await db.saveWorkflow(makeWorkflow('p1', { id: 'w1' }))
    await db.saveWorkflow(makeWorkflow('p1', { id: 'w2' }))

    const replacement = makeProject({ id: 'p9', name: 'Fresh start' })
    const summary = await db.importAll(
      {
        app: 'sparquet-studio',
        version: db.STORAGE_VERSION,
        exportedAt: Date.now(),
        projects: [replacement],
        workflows: [],
      },
      { merge: false },
    )

    expect(summary.merged).toBe(false)
    expect(await db.listProjects()).toEqual([replacement])
    expect(await db.listWorkflows()).toEqual([])
  })

  it('drops malformed records and reports them as skipped', async () => {
    const summary = await db.importAll({
      app: 'sparquet-studio',
      version: db.STORAGE_VERSION,
      exportedAt: Date.now(),
      projects: [makeProject(), { id: 'broken' }],
      workflows: [{ id: 'w0', projectId: 'p1' }],
    })

    expect(summary).toEqual({ projects: 1, workflows: 0, skipped: 2, merged: true })
    expect(await db.listWorkflows()).toEqual([])
  })

  it('rejects a bundle from a newer Studio build', async () => {
    await expect(
      db.importAll({ version: db.STORAGE_VERSION + 1, projects: [], workflows: [] }),
    ).rejects.toThrow(/newer version/i)
  })

  it('rejects a value that is not a bundle', async () => {
    await expect(db.importAll('nope')).rejects.toThrow(/invalid bundle/i)
  })

  it('keeps the library when a replace bundle has an unusable version', async () => {
    await db.saveProject(makeProject({ id: 'p1' }))
    await db.saveWorkflow(makeWorkflow('p1', { id: 'w1' }))

    for (const version of [-1, 0.5]) {
      await expect(
        db.importAll(bundle({ version, projects: [], workflows: [] }), { merge: false }),
      ).rejects.toThrow(/whole number/i)
    }

    expect((await db.listProjects()).map((p) => p.id)).toEqual(['p1'])
    expect((await db.listWorkflows()).map((w) => w.id)).toEqual(['w1'])
  })

  it('restores the library when a replace import fails mid-write', async () => {
    await db.saveProject(makeProject({ id: 'p1' }))
    await db.saveWorkflow(makeWorkflow('p1', { id: 'w1' }))
    failWrites.add('sparquet-studio:db:project:p9')

    await expect(
      db.importAll(bundle({ projects: [makeProject({ id: 'p9' })] }), { merge: false }),
    ).rejects.toThrow(/restored/i)

    expect((await db.listProjects()).map((p) => p.id)).toEqual(['p1'])
    expect((await db.listWorkflows()).map((w) => w.id)).toEqual(['w1'])
  })

  it('drops workflows whose graph nodes or edges are malformed', async () => {
    const base = makeWorkflow('p1')
    const summary = await db.importAll(
      bundle({
        projects: [makeProject({ id: 'p1' })],
        workflows: [
          { ...base, id: 'w1', graph: { nodes: [{ id: 'n1' }], edges: [] } },
          { ...base, id: 'w2', graph: { nodes: [null], edges: [] } },
          { ...base, id: 'w3', graph: { nodes: [], edges: [{ source: 'a', target: 'b' }] } },
          { ...base, id: 'w4' },
        ],
      }),
    )

    expect(summary.workflows).toBe(1)
    expect(summary.skipped).toBe(3)
    expect((await db.listWorkflows()).map((w) => w.id)).toEqual(['w4'])
  })

  it('drops workflows missing the fields the screens read', async () => {
    const base = makeWorkflow('p1')
    const without = (id: string, field: keyof Workflow): unknown => {
      const record: Record<string, unknown> = { ...base, id }
      delete record[field]
      return record
    }

    const summary = await db.importAll(
      bundle({
        projects: [makeProject({ id: 'p1' })],
        workflows: [
          without('w1', 'description'),
          without('w2', 'tags'),
          without('w3', 'settings'),
          without('w4', 'params'),
          makeWorkflow('p1', { id: 'w9' }),
        ],
      }),
    )

    expect(summary.workflows).toBe(1)
    expect(summary.skipped).toBe(4)
    expect((await db.listWorkflows()).map((w) => w.id)).toEqual(['w9'])
  })
})

describe('corrupt records already in storage', () => {
  it('ignores them instead of handing a broken workflow to the screens', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    await db.saveWorkflow(makeWorkflow('p1', { id: 'w1' }))
    memory.set('sparquet-studio:db:workflow:bad', {
      ...makeWorkflow('p1', { id: 'bad' }),
      graph: { nodes: [{ id: 'n1' }], edges: [] },
    })

    expect((await db.listWorkflows()).map((w) => w.id)).toEqual(['w1'])
    expect(await db.getWorkflow('bad')).toBeNull()
    expect(warn).toHaveBeenCalledOnce()
    warn.mockRestore()
  })
})

describe('migrate', () => {
  it('is a no-op once the store is at the current version', async () => {
    await db.saveProject(makeProject())
    const before = [...memory.entries()]
    expect(await db.migrate()).toBe(db.STORAGE_VERSION)
    expect([...memory.entries()]).toEqual(before)
    expect(memory.get('sparquet-studio:db:backup')).toBeUndefined()
  })

  it('backs the previous bundle up before upgrading unversioned data', async () => {
    const project = makeProject()
    memory.set('sparquet-studio:db:project:p1', project)

    expect(await db.migrate()).toBe(db.STORAGE_VERSION)

    const backup = memory.get('sparquet-studio:db:backup')
    expect(backup).toMatchObject({ version: 0, projects: [project] })
    expect(await db.listProjects()).toEqual([project])
  })
})

describe('clearAll', () => {
  it('removes records but keeps the seeded marker', async () => {
    await db.saveProject(makeProject())
    await db.saveWorkflow(makeWorkflow('p1'))
    await db.markSeeded()

    await db.clearAll()

    expect(await db.listProjects()).toEqual([])
    expect(await db.listWorkflows()).toEqual([])
    expect(await db.isSeeded()).toBe(true)
  })
})
