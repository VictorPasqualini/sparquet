import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { StudioEdge, StudioNode, Job } from '@/types/studio'

import { useEditorStore } from './editor'
import { useSettingsStore } from './settings'

const dbState = vi.hoisted(() => ({
  records: new Map<string, unknown>(),
  /** When set, a write parks here until the test resolves it. */
  gate: null as Promise<void> | null,
}))

vi.mock('@/lib/storage/db', () => ({
  getJob: async (id: string) => dbState.records.get(id) ?? null,
  saveJob: async (job: Job) => {
    if (dbState.gate) await dbState.gate
    dbState.records.set(job.id, JSON.parse(JSON.stringify(job)) as Job)
    return job
  },
}))

function stored(id: string): Job {
  const record = dbState.records.get(id)
  if (!record) throw new Error(`no stored record for ${id}`)
  return record as Job
}

function sourceNode(id: string, path: string): StudioNode {
  return {
    id,
    type: 'source',
    position: { x: 0, y: 0 },
    data: { kind: 'source', format: 'parquet', path, options: {} },
  }
}

function sinkNode(id: string): StudioNode {
  return {
    id,
    type: 'sink',
    position: { x: 200, y: 0 },
    data: {
      kind: 'sink',
      format: 'parquet',
      path: '/out',
      mode: 'overwrite',
      partitionBy: [],
      columns: null,
      options: {},
    },
  }
}

const edge: StudioEdge = { id: 'e1', source: 'a', target: 'b', type: 'pipeline' }

let seq = 0

/** Each test uses its own id: the store keeps module-level state between tests. */
function makeJob(patch: Partial<Job> = {}): Job {
  seq += 1
  const job: Job = {
    id: `wf-${seq}`,
    workflowId: 'p1',
    name: `Job ${seq}`,
    description: '',
    tags: [],
    settings: { pipelineName: 'pipeline', description: '', spark: {} },
    graph: { nodes: [sourceNode('a', '/in'), sinkNode('b')], edges: [edge] },
    params: [],
    createdAt: 1,
    updatedAt: 1,
    revision: 3,
    ...patch,
  }
  dbState.records.set(job.id, JSON.parse(JSON.stringify(job)) as Job)
  return job
}

/** Lets queued microtasks run, which is what separates two user gestures. */
const nextTurn = () => Promise.resolve()

beforeEach(() => {
  vi.useFakeTimers()
  dbState.records.clear()
  dbState.gate = null
})

afterEach(async () => {
  useEditorStore.getState().close()
  await useEditorStore.getState().save()
  vi.useRealTimers()
})

describe('autosave', () => {
  it('flushes the pending write when the editor closes', async () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)

    editor.updateNodeData('a', { path: '/edited' })
    expect(useEditorStore.getState().dirty).toBe(true)

    // Navigating away before the 700 ms debounce elapsed must not drop the edit.
    editor.close()
    await editor.save()

    const nodes = stored(job.id).graph.nodes as StudioNode[]
    expect(nodes[0].data).toMatchObject({ path: '/edited' })
    expect(stored(job.id).revision).toBe(4)
    // The completed write must not resurrect the closed editor.
    expect(useEditorStore.getState().job).toBeNull()
    expect(useEditorStore.getState().dirty).toBe(false)
  })

  it('flushes the pending write when another job is opened', async () => {
    const first = makeJob()
    const second = makeJob()
    const editor = useEditorStore.getState()

    editor.open(first)
    editor.updateNodeData('a', { path: '/first' })
    editor.open(second)
    await editor.save()

    const nodes = stored(first.id).graph.nodes as StudioNode[]
    expect(nodes[0].data).toMatchObject({ path: '/first' })
    expect(useEditorStore.getState().job?.id).toBe(second.id)
  })

  it('keeps dirty for edits that land while the write is in flight', async () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)

    let release!: () => void
    dbState.gate = new Promise<void>((resolve) => {
      release = resolve
    })

    editor.updateNodeData('a', { path: '/one' })
    const saving = editor.save()
    await nextTurn()

    // Arrives after the snapshot the write is persisting.
    editor.updateNodeData('a', { path: '/two' })

    dbState.gate = null
    release()
    await saving

    expect(useEditorStore.getState().dirty).toBe(true)
    const afterFirst = stored(job.id).graph.nodes as StudioNode[]
    expect(afterFirst[0].data).toMatchObject({ path: '/one' })

    await vi.advanceTimersByTimeAsync(1000)
    const afterSecond = stored(job.id).graph.nodes as StudioNode[]
    expect(afterSecond[0].data).toMatchObject({ path: '/two' })
    expect(useEditorStore.getState().dirty).toBe(false)
  })

  it('does not clobber a revision written by another tab', async () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)

    // Another tab saved this job while it was open here.
    dbState.records.set(job.id, { ...job, revision: 9, name: 'renamed elsewhere' })

    editor.updateNodeData('a', { path: '/mine' })
    await editor.save()

    const conflict = useEditorStore.getState().conflict
    expect(conflict?.revision).toBe(9)
    expect(conflict?.name).toBe('renamed elsewhere')
    // The local work is still persisted, on top of the newer stored revision.
    const nodes = stored(job.id).graph.nodes as StudioNode[]
    expect(nodes[0].data).toMatchObject({ path: '/mine' })
    expect(stored(job.id).revision).toBe(10)
  })

  it('rebases a queued write on the revision it just wrote itself', async () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)

    let release!: () => void
    dbState.gate = new Promise<void>((resolve) => {
      release = resolve
    })

    editor.updateNodeData('a', { path: '/one' })
    const first = editor.save()
    await nextTurn()
    // Queued while the first write holds the same base revision in memory.
    editor.updateNodeData('a', { path: '/two' })
    const second = editor.save()

    dbState.gate = null
    release()
    await Promise.all([first, second])

    expect(useEditorStore.getState().conflict).toBeNull()
    expect(stored(job.id).revision).toBe(5)
    const nodes = stored(job.id).graph.nodes as StudioNode[]
    expect(nodes[0].data).toMatchObject({ path: '/two' })
    expect(useEditorStore.getState().dirty).toBe(false)
  })

  it('reports no conflict for its own consecutive writes', async () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)

    editor.updateNodeData('a', { path: '/one' })
    await editor.save()
    await nextTurn()
    editor.updateNodeData('a', { path: '/two' })
    await editor.save()

    expect(useEditorStore.getState().conflict).toBeNull()
    expect(stored(job.id).revision).toBe(5)
  })
})

describe('history', () => {
  it('ignores edge selection changes', () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)

    editor.onEdgesChange([{ id: 'e1', type: 'select', selected: true }])

    const state = useEditorStore.getState()
    expect(state.past).toHaveLength(0)
    expect(state.dirty).toBe(false)
    expect(state.edges[0].selected).toBe(true)
  })

  it('ignores node selection changes', () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)

    editor.onNodesChange([{ id: 'a', type: 'select', selected: true }])

    const state = useEditorStore.getState()
    expect(state.past).toHaveLength(0)
    expect(state.dirty).toBe(false)
  })

  it('records a canvas delete of nodes and their edges as one entry', () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)

    // React Flow's deleteElements dispatches the edges and the nodes in two
    // callbacks inside the same turn.
    editor.onEdgesChange([{ id: 'e1', type: 'remove' }])
    editor.onNodesChange([
      { id: 'a', type: 'remove' },
      { id: 'b', type: 'remove' },
    ])

    expect(useEditorStore.getState().past).toHaveLength(1)
    expect(useEditorStore.getState().nodes).toHaveLength(0)

    editor.undo()
    const state = useEditorStore.getState()
    expect(state.nodes).toHaveLength(2)
    expect(state.edges).toHaveLength(1)
    expect(state.past).toHaveLength(0)
  })

  it('keeps separate gestures in separate entries', async () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)

    editor.onEdgesChange([{ id: 'e1', type: 'remove' }])
    await nextTurn()
    editor.onNodesChange([{ id: 'a', type: 'remove' }])

    expect(useEditorStore.getState().past).toHaveLength(2)
  })

  it('coalesces successive edits to the same field into one entry', async () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)

    for (const path of ['/d', '/da', '/dat', '/data']) {
      editor.updateNodeData('a', { path })
      await vi.advanceTimersByTimeAsync(60)
    }

    expect(useEditorStore.getState().past).toHaveLength(1)

    editor.undo()
    const nodes = useEditorStore.getState().nodes
    expect(nodes[0].data).toMatchObject({ path: '/in' })
  })

  it('starts a new entry after the coalescing window and for another field', async () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)

    editor.updateNodeData('a', { path: '/one' })
    await vi.advanceTimersByTimeAsync(600)
    editor.updateNodeData('a', { path: '/two' })
    await nextTurn()
    editor.updateNodeData('a', { format: 'csv' })

    expect(useEditorStore.getState().past).toHaveLength(3)
  })
})

describe('live linting', () => {
  afterEach(() => {
    useSettingsStore.getState().setCanvas({ liveLint: true })
  })

  it('skips scheduled linting when the preference is off', async () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)
    useSettingsStore.getState().setCanvas({ liveLint: false })

    const before = useEditorStore.getState().issues
    editor.updateNodeData('a', { path: '' })
    await vi.advanceTimersByTimeAsync(400)

    // lint() always publishes a fresh array, so identity proves it did not run.
    expect(useEditorStore.getState().issues).toBe(before)

    editor.lint()
    expect(useEditorStore.getState().issues).not.toBe(before)
  })

  it('lints on a change when the preference is on', async () => {
    const job = makeJob()
    const editor = useEditorStore.getState()
    editor.open(job)

    const before = useEditorStore.getState().issues
    editor.updateNodeData('a', { path: '' })
    await vi.advanceTimersByTimeAsync(400)

    expect(useEditorStore.getState().issues).not.toBe(before)
  })
})

describe('run step status', () => {
  function transformNode(id: string, transform: string, disabled = false): StudioNode {
    return {
      id,
      type: 'transform',
      position: { x: 0, y: 0 },
      data: { kind: 'transform', transform, params: {}, disabled },
    }
  }

  function link(source: string, target: string): StudioEdge {
    return { id: `e-${source}-${target}`, source, target, type: 'pipeline' }
  }

  /** src → t1 → muted → t2 → validations → (t3 → out1 | t4 → out2). */
  function branchedJob(): Job {
    return makeJob({
      graph: {
        nodes: [
          sourceNode('src', '/in'),
          transformNode('t1', 'filter'),
          transformNode('muted', 'distinct', true),
          transformNode('t2', 'select'),
          {
            id: 'checks',
            type: 'validations',
            position: { x: 0, y: 0 },
            data: {
              kind: 'validations',
              onFailure: 'fail',
              rules: [{ type: 'not_null', columns: ['id'] }],
              report: null,
            },
          },
          transformNode('t3', 'sort'),
          transformNode('t4', 'drop'),
          sinkNode('out1'),
          sinkNode('out2'),
        ],
        edges: [
          link('src', 't1'),
          link('t1', 'muted'),
          link('muted', 't2'),
          link('t2', 'checks'),
          link('checks', 't3'),
          link('t3', 'out1'),
          link('checks', 't4'),
          link('t4', 'out2'),
        ],
      },
    })
  }

  it('maps a transformation index to the node the compiler emitted it from', () => {
    const editor = useEditorStore.getState()
    editor.open(branchedJob())

    const ids = useEditorStore.getState().transformNodeIdsInOrder()
    const { pipeline } = useEditorStore.getState().compile()
    const nodes = useEditorStore.getState().nodes

    // Muted nodes and per-destination transformations never reach the main array.
    expect(ids).toEqual(['t1', 't2'])
    expect(pipeline?.transformations).toHaveLength(ids.length)
    for (const [index, id] of ids.entries()) {
      const node = nodes.find((candidate) => candidate.id === id)
      const spec = pipeline?.transformations?.[index]
      const emitted = spec && 'type' in spec ? spec.type : undefined
      expect(node?.data.kind === 'transform' && node.data.transform).toBe(emitted)
    }
  })

  it('sets, replaces and clears node statuses, and forgets them on close', () => {
    const editor = useEditorStore.getState()
    editor.open(branchedJob())

    editor.setStepStatuses({ t1: 'pending', t2: 'pending' })
    editor.setStepStatus('t1', 'running', 'filter')
    expect(useEditorStore.getState().stepStatus).toEqual({ t1: 'running', t2: 'pending' })

    editor.clearStepStatus()
    expect(useEditorStore.getState().stepStatus).toEqual({})

    editor.setStepStatus('t1', 'error')
    editor.close()
    expect(useEditorStore.getState().stepStatus).toEqual({})
  })
})
