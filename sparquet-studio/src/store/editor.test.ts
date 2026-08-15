import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { StudioEdge, StudioNode, Workflow } from '@/types/studio'

import { useEditorStore } from './editor'
import { useSettingsStore } from './settings'

const dbState = vi.hoisted(() => ({
  records: new Map<string, unknown>(),
  /** When set, a write parks here until the test resolves it. */
  gate: null as Promise<void> | null,
}))

vi.mock('@/lib/storage/db', () => ({
  getWorkflow: async (id: string) => dbState.records.get(id) ?? null,
  saveWorkflow: async (workflow: Workflow) => {
    if (dbState.gate) await dbState.gate
    dbState.records.set(workflow.id, JSON.parse(JSON.stringify(workflow)) as Workflow)
    return workflow
  },
}))

function stored(id: string): Workflow {
  const record = dbState.records.get(id)
  if (!record) throw new Error(`no stored record for ${id}`)
  return record as Workflow
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
function makeWorkflow(patch: Partial<Workflow> = {}): Workflow {
  seq += 1
  const workflow: Workflow = {
    id: `wf-${seq}`,
    projectId: 'p1',
    name: `Workflow ${seq}`,
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
  dbState.records.set(workflow.id, JSON.parse(JSON.stringify(workflow)) as Workflow)
  return workflow
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
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)

    editor.updateNodeData('a', { path: '/edited' })
    expect(useEditorStore.getState().dirty).toBe(true)

    // Navigating away before the 700 ms debounce elapsed must not drop the edit.
    editor.close()
    await editor.save()

    const nodes = stored(workflow.id).graph.nodes as StudioNode[]
    expect(nodes[0].data).toMatchObject({ path: '/edited' })
    expect(stored(workflow.id).revision).toBe(4)
    // The completed write must not resurrect the closed editor.
    expect(useEditorStore.getState().workflow).toBeNull()
    expect(useEditorStore.getState().dirty).toBe(false)
  })

  it('flushes the pending write when another workflow is opened', async () => {
    const first = makeWorkflow()
    const second = makeWorkflow()
    const editor = useEditorStore.getState()

    editor.open(first)
    editor.updateNodeData('a', { path: '/first' })
    editor.open(second)
    await editor.save()

    const nodes = stored(first.id).graph.nodes as StudioNode[]
    expect(nodes[0].data).toMatchObject({ path: '/first' })
    expect(useEditorStore.getState().workflow?.id).toBe(second.id)
  })

  it('keeps dirty for edits that land while the write is in flight', async () => {
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)

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
    const afterFirst = stored(workflow.id).graph.nodes as StudioNode[]
    expect(afterFirst[0].data).toMatchObject({ path: '/one' })

    await vi.advanceTimersByTimeAsync(1000)
    const afterSecond = stored(workflow.id).graph.nodes as StudioNode[]
    expect(afterSecond[0].data).toMatchObject({ path: '/two' })
    expect(useEditorStore.getState().dirty).toBe(false)
  })

  it('does not clobber a revision written by another tab', async () => {
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)

    // Another tab saved this workflow while it was open here.
    dbState.records.set(workflow.id, { ...workflow, revision: 9, name: 'renamed elsewhere' })

    editor.updateNodeData('a', { path: '/mine' })
    await editor.save()

    const conflict = useEditorStore.getState().conflict
    expect(conflict?.revision).toBe(9)
    expect(conflict?.name).toBe('renamed elsewhere')
    // The local work is still persisted, on top of the newer stored revision.
    const nodes = stored(workflow.id).graph.nodes as StudioNode[]
    expect(nodes[0].data).toMatchObject({ path: '/mine' })
    expect(stored(workflow.id).revision).toBe(10)
  })

  it('rebases a queued write on the revision it just wrote itself', async () => {
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)

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
    expect(stored(workflow.id).revision).toBe(5)
    const nodes = stored(workflow.id).graph.nodes as StudioNode[]
    expect(nodes[0].data).toMatchObject({ path: '/two' })
    expect(useEditorStore.getState().dirty).toBe(false)
  })

  it('reports no conflict for its own consecutive writes', async () => {
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)

    editor.updateNodeData('a', { path: '/one' })
    await editor.save()
    await nextTurn()
    editor.updateNodeData('a', { path: '/two' })
    await editor.save()

    expect(useEditorStore.getState().conflict).toBeNull()
    expect(stored(workflow.id).revision).toBe(5)
  })
})

describe('history', () => {
  it('ignores edge selection changes', () => {
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)

    editor.onEdgesChange([{ id: 'e1', type: 'select', selected: true }])

    const state = useEditorStore.getState()
    expect(state.past).toHaveLength(0)
    expect(state.dirty).toBe(false)
    expect(state.edges[0].selected).toBe(true)
  })

  it('ignores node selection changes', () => {
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)

    editor.onNodesChange([{ id: 'a', type: 'select', selected: true }])

    const state = useEditorStore.getState()
    expect(state.past).toHaveLength(0)
    expect(state.dirty).toBe(false)
  })

  it('records a canvas delete of nodes and their edges as one entry', () => {
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)

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
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)

    editor.onEdgesChange([{ id: 'e1', type: 'remove' }])
    await nextTurn()
    editor.onNodesChange([{ id: 'a', type: 'remove' }])

    expect(useEditorStore.getState().past).toHaveLength(2)
  })

  it('coalesces successive edits to the same field into one entry', async () => {
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)

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
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)

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
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)
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
    const workflow = makeWorkflow()
    const editor = useEditorStore.getState()
    editor.open(workflow)

    const before = useEditorStore.getState().issues
    editor.updateNodeData('a', { path: '' })
    await vi.advanceTimersByTimeAsync(400)

    expect(useEditorStore.getState().issues).not.toBe(before)
  })
})
