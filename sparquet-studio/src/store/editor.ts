import {
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  type Connection,
  type EdgeChange,
  type NodeChange,
  type XYPosition,
} from '@xyflow/react'
import { nanoid } from 'nanoid'
import { create } from 'zustand'

import { defaultsFor, getFormat, getTransformation, getValidator } from '@/catalog'
import {
  autoLayout,
  chainToSink,
  compileGraph,
  isCompilable,
  isDisabled,
  isSinkNode,
  isSourceNode,
  isTransformNode,
  isValidationNode,
  isValidationSink,
  longestCommonPrefix,
  NODE_RENDER_SIZE,
  NOTE_RENDER_SIZE,
  pipelineToGraph,
  serializePipeline,
  validationSinkLink,
} from '@/lib/compiler'
import { mergeParams } from '@/lib/params'
import * as db from '@/lib/storage/db'
import { upgradeJob } from '@/lib/storage/migrations'
import { lintJob } from '@/lib/validation/lint'
import type { PipelineSpec } from '@/types/pipeline'
import { HANDLE, validationSinkRoleOfHandle } from '@/types/studio'
import type {
  ParamDefinition,
  RunResult,
  StepStatus,
  StudioEdge,
  StudioGraph,
  StudioNode,
  StudioNodeData,
  ValidationIssue,
  Job,
  JobSettings,
} from '@/types/studio'

import { useLibraryStore } from './library'
import { useSettingsStore } from './settings'

const AUTOSAVE_DELAY = 700
const HISTORY_LIMIT = 60
/** Successive edits to the same field within this window share one undo entry. */
const COALESCE_WINDOW = 500

interface Snapshot {
  nodes: StudioNode[]
  edges: StudioEdge[]
  params: ParamDefinition[]
  settings: JobSettings
}

/** Everything a write persists, captured synchronously so `close()` can flush it. */
interface PendingWrite {
  job: Job
  nodes: StudioNode[]
  edges: StudioEdge[]
  params: ParamDefinition[]
  settings: JobSettings
}

export type PanelId = 'inspector' | 'settings' | 'json' | 'ai' | 'run' | 'issues'

interface EditorState {
  job: Job | null
  nodes: StudioNode[]
  edges: StudioEdge[]
  params: ParamDefinition[]
  settings: JobSettings

  selectedNodeId: string | null
  issues: ValidationIssue[]
  dirty: boolean
  saving: boolean
  lastSavedAt: number | null
  /** The stored record when another tab saved this job first. */
  conflict: Job | null

  past: Snapshot[]
  future: Snapshot[]

  run: RunResult | null
  running: boolean
  /** Live status of each node the current run has reached, keyed by node id. */
  stepStatus: Record<string, StepStatus>

  /** The right-hand panel is a single tabbed surface; null means collapsed. */
  activePanel: PanelId | null
  panelWidth: number

  /* lifecycle */
  open: (job: Job) => void
  close: () => void
  save: () => Promise<void>
  dismissConflict: () => void

  /* graph */
  onNodesChange: (changes: NodeChange<StudioNode>[]) => void
  onEdgesChange: (changes: EdgeChange<StudioEdge>[]) => void
  onConnect: (connection: Connection) => void
  addNode: (node: { data: StudioNodeData; position: XYPosition }) => string
  addTransform: (type: string, position: XYPosition) => string
  addSource: (position: XYPosition, format?: string) => string
  addSink: (position: XYPosition, format?: string) => string
  /** One `validations.rules` entry, e.g. `not_null`. */
  addValidation: (type: string, position: XYPosition) => string
  addNote: (position: XYPosition) => string
  updateNodeData: (id: string, patch: Partial<StudioNodeData> | Record<string, unknown>) => void
  /** Writes one key of a transformation's or a validation rule's `params`. */
  updateNodeParam: (id: string, key: string, value: unknown) => void
  removeNodes: (ids: string[]) => void
  duplicateNode: (id: string) => void
  toggleDisabled: (id: string) => void
  select: (id: string | null) => void
  replaceGraph: (graph: StudioGraph, options?: { layout?: boolean }) => void
  applyPipeline: (pipeline: unknown, options?: { keepSettings?: boolean }) => ValidationIssue[]
  layout: () => void

  /* metadata */
  setSettings: (patch: Partial<JobSettings>) => void
  setParams: (params: ParamDefinition[]) => void

  /* history */
  undo: () => void
  redo: () => void
  canUndo: () => boolean
  canRedo: () => boolean

  /* derived */
  compile: () => { pipeline: PipelineSpec | null; issues: ValidationIssue[] }
  pipelineJson: () => string
  lint: () => void

  /* run */
  setRun: (run: RunResult | null) => void
  setRunning: (running: boolean) => void
  /**
   * Marks one node. `type` is the transformation type the runner reported; it is
   * accepted so callers can forward the event as-is, but the node already knows
   * its own type, so nothing is stored.
   */
  setStepStatus: (nodeId: string, status: StepStatus, type?: string) => void
  /** Replaces the whole map — how a run start seeds every node to `pending`. */
  setStepStatuses: (statuses: Record<string, StepStatus>) => void
  clearStepStatus: () => void
  /** Node ids behind the compiled `transformations`, in the runner's index order. */
  transformNodeIdsInOrder: () => string[]

  /* panels */
  togglePanel: (panel: PanelId, open?: boolean) => void
  setPanelWidth: (width: number) => void
}

export const PANEL_MIN_WIDTH = 320
export const PANEL_MAX_WIDTH = 720

function clampPanelWidth(width: number): number {
  return Math.min(PANEL_MAX_WIDTH, Math.max(PANEL_MIN_WIDTH, Math.round(width)))
}

const EMPTY_SETTINGS: JobSettings = {
  pipelineName: 'pipeline',
  description: '',
  spark: {},
}

let autosaveTimer: ReturnType<typeof setTimeout> | null = null
let lintTimer: ReturnType<typeof setTimeout> | null = null
/** Writes run one at a time so two saves never derive a revision from the same base. */
let writeChain: Promise<void> = Promise.resolve()
/** What this tab wrote last; a stored revision it does not match came from elsewhere. */
let lastWrite: { id: string; revision: number } | null = null
/** Commits raised in the same event-loop turn belong to one user action. */
let sameTurn = false
let lastCommitKey: string | null = null
let lastCommitAt = 0

export const useEditorStore = create<EditorState>((set, get) => {
  /** Captures the current graph for the undo stack. */
  const snapshot = (): Snapshot => {
    const { nodes, edges, params, settings } = get()
    return { nodes, edges, params, settings }
  }

  const liveLint = (): boolean => useSettingsStore.getState().canvas.liveLint

  const forgetCoalescing = () => {
    sameTurn = false
    lastCommitKey = null
  }

  /**
   * Records history and schedules persistence + linting.
   *
   * One user gesture must leave exactly one undo entry, so a commit reuses the
   * entry pushed by the previous one when it belongs to the same action: the same
   * event-loop turn (React Flow splits a canvas delete into an edge callback and a
   * node callback) or the same field within `COALESCE_WINDOW` (one change event per
   * keystroke). `key` identifies the mutation site.
   */
  const commit = (
    updater: () => void,
    options: { history?: boolean; lint?: boolean; key?: string } = {},
  ) => {
    const { history = true, lint = true, key } = options
    const now = Date.now()
    const coalesce =
      sameTurn ||
      (key !== undefined && key === lastCommitKey && now - lastCommitAt < COALESCE_WINDOW)
    const before = history && !coalesce ? snapshot() : null
    updater()
    if (before) {
      set((state) => ({
        past: [...state.past, before].slice(-HISTORY_LIMIT),
        future: [],
      }))
    }
    if (history) {
      lastCommitKey = key ?? null
      lastCommitAt = now
      if (!sameTurn) {
        sameTurn = true
        queueMicrotask(() => {
          sameTurn = false
        })
      }
    }
    set({ dirty: true })
    scheduleSave()
    if (lint && liveLint()) scheduleLint()
  }

  const scheduleSave = () => {
    if (autosaveTimer) clearTimeout(autosaveTimer)
    autosaveTimer = setTimeout(() => {
      autosaveTimer = null
      void get().save()
    }, AUTOSAVE_DELAY)
  }

  const scheduleLint = () => {
    if (lintTimer) clearTimeout(lintTimer)
    lintTimer = setTimeout(() => get().lint(), 250)
  }

  /** Snapshots what a write must persist; null when there is nothing pending. */
  const capture = (): PendingWrite | null => {
    const { job, nodes, edges, params, settings, dirty } = get()
    if (!job || !dirty) return null
    return { job, nodes, edges, params, settings }
  }

  const write = async ({ job, nodes, edges, params, settings }: PendingWrite) => {
    set({ saving: true })
    const stored = await db.getJob(job.id)
    // A stored revision this tab did not write means someone else saved first.
    const ours = lastWrite?.id === job.id && lastWrite.revision === stored?.revision
    const conflict = stored && stored.revision !== job.revision && !ours ? stored : null
    const next: Job = {
      ...job,
      graph: { nodes, edges },
      params,
      settings,
      updatedAt: Date.now(),
      revision: Math.max(job.revision, stored?.revision ?? 0) + 1,
    }
    await db.saveJob(next)
    lastWrite = { id: next.id, revision: next.revision }
    useLibraryStore.getState().upsertJob(next)
    set((state) => {
      // The editor moved on (closed or switched job): only the write mattered.
      if (state.job?.id !== job.id) return { saving: false }
      // Edits that landed while the write was in flight are not part of `next`;
      // keep `dirty` so the save they scheduled still persists them.
      const settled =
        state.nodes === nodes &&
        state.edges === edges &&
        state.params === params &&
        state.settings === settings
      return {
        job: next,
        saving: false,
        lastSavedAt: next.updatedAt,
        dirty: settled ? false : state.dirty,
        conflict: conflict ?? state.conflict,
      }
    })
  }

  /** Queues the pending edit for persistence; safe to call right before a reset. */
  const flush = (): Promise<void> => {
    const pending = capture()
    if (!pending) return writeChain
    writeChain = writeChain.then(async () => {
      try {
        await write(pending)
      } catch {
        set({ saving: false })
      }
    })
    return writeChain
  }

  const insert = (data: StudioNodeData, position: XYPosition): string => {
    const id = `${data.kind}-${nanoid(6)}`
    // Seeded dimensions let React Flow draw the node and its edges on the first
    // frame; the ResizeObserver refines them right after.
    const measured = data.kind === 'note' ? { ...NOTE_RENDER_SIZE } : { ...NODE_RENDER_SIZE }
    const node = { id, type: data.kind, position, data, measured } as StudioNode
    commit(() => set((state) => ({ nodes: [...state.nodes, node], selectedNodeId: id })))
    return id
  }

  return {
    job: null,
    nodes: [],
    edges: [],
    params: [],
    settings: EMPTY_SETTINGS,

    selectedNodeId: null,
    issues: [],
    dirty: false,
    saving: false,
    lastSavedAt: null,
    conflict: null,

    past: [],
    future: [],

    run: null,
    running: false,
    stepStatus: {},

    activePanel: 'inspector',
    panelWidth: 400,

    open: (job) => {
      const current = get()
      // Re-opening the same job would swap in node objects that React Flow
      // has not measured yet. Because the DOM elements keep their size, no
      // ResizeObserver fires, the nodes stay hidden and no edge is ever drawn.
      if (current.job?.id === job.id) return

      if (autosaveTimer) {
        clearTimeout(autosaveTimer)
        autosaveTimer = null
      }
      // Whatever the previous job still owed the disk must land before the
      // state that holds it is replaced.
      void flush()
      forgetCoalescing()
      // Storage migrates records on boot, but a job can also arrive from a merged
      // bundle or an older backup: upgrading here means the editor never opens a
      // shape it cannot draw. The record is rewritten by the next save.
      const migrated = upgradeJob(job)
      set({
        job: migrated,
        nodes: migrated.graph.nodes,
        edges: migrated.graph.edges,
        params: migrated.params,
        settings: migrated.settings,
        selectedNodeId: null,
        issues: [],
        dirty: false,
        saving: false,
        conflict: null,
        past: [],
        future: [],
        run: null,
        running: false,
        stepStatus: {},
        lastSavedAt: job.updatedAt,
      })
      get().lint()
    },

    close: () => {
      if (autosaveTimer) {
        clearTimeout(autosaveTimer)
        autosaveTimer = null
      }
      if (lintTimer) {
        clearTimeout(lintTimer)
        lintTimer = null
      }
      // The editor unmounts on every navigation: flush the debounced autosave
      // instead of dropping it, or the last edits never reach storage.
      void flush()
      forgetCoalescing()
      set({
        job: null,
        nodes: [],
        edges: [],
        params: [],
        settings: EMPTY_SETTINGS,
        selectedNodeId: null,
        issues: [],
        dirty: false,
        saving: false,
        conflict: null,
        past: [],
        future: [],
        run: null,
        stepStatus: {},
      })
    },

    save: () => flush(),

    dismissConflict: () => set({ conflict: null }),

    onNodesChange: (changes) => {
      // Drags fire continuously; only the final position is worth an undo entry.
      const structural = changes.some(
        (change) =>
          change.type === 'remove' ||
          change.type === 'add' ||
          (change.type === 'position' && change.dragging === false),
      )
      const apply = () =>
        set((state) => ({ nodes: applyNodeChanges(changes, state.nodes) as StudioNode[] }))

      if (structural) {
        commit(apply, { lint: changes.some((c) => c.type !== 'position') })
      } else {
        apply()
        const positional = changes.some((c) => c.type === 'position' || c.type === 'dimensions')
        if (positional) {
          set({ dirty: true })
          scheduleSave()
        }
      }

      const removed = changes.filter((c) => c.type === 'remove').map((c) => c.id)
      if (removed.length && removed.includes(get().selectedNodeId ?? '')) {
        set({ selectedNodeId: null })
      }
    },

    onEdgesChange: (changes) => {
      // Clicking an edge is not an edit: selection never enters history nor dirties
      // the job. Mirrors the `select` handling in onNodesChange.
      const structural = changes.some((change) => change.type !== 'select')
      const apply = () =>
        set((state) => ({ edges: applyEdgeChanges(changes, state.edges) as StudioEdge[] }))

      if (structural) commit(apply)
      else apply()
    },

    onConnect: (connection) => {
      const { nodes, edges } = get()
      if (!connection.source || !connection.target) return
      if (connection.source === connection.target) return

      const targetHandle = connection.targetHandle ?? HANDLE.in
      const target = nodes.find((n) => n.id === connection.target)
      const source = nodes.find((n) => n.id === connection.source)
      if (!target || !source) return
      if (target.data.kind === 'note' || source.data.kind === 'note') return
      if (target.data.kind === 'source') return
      // A validation side output compiles into `validations.report` / `.outputs.*`:
      // it is a written dataset, so only a destination node can close that link.
      if (
        validationSinkRoleOfHandle(connection.sourceHandle) !== null &&
        (target.data.kind !== 'sink' || source.data.kind !== 'validation')
      ) {
        return
      }
      if (createsCycle(edges, connection.source, connection.target)) return

      // One connection per input handle: a new link replaces the old one.
      const cleaned = edges.filter(
        (edge) =>
          !(
            edge.target === connection.target &&
            (edge.targetHandle ?? HANDLE.in) === targetHandle
          ),
      )

      commit(() =>
        set({
          edges: addEdge(
            {
              ...connection,
              targetHandle,
              sourceHandle: connection.sourceHandle ?? HANDLE.out,
              id: `e-${nanoid(6)}`,
              type: 'pipeline',
            },
            cleaned,
          ) as StudioEdge[],
        }),
      )
    },

    addNode: ({ data, position }) => insert(data, position),

    addTransform: (type, position) => {
      const def = getTransformation(type)
      return insert(
        {
          kind: 'transform',
          transform: type,
          params: def ? defaultsFor(def.fields) : {},
          label: def?.label,
        },
        position,
      )
    },

    addSource: (position, format = 'parquet') =>
      insert({ kind: 'source', format, path: '', options: {} }, position),

    addSink: (position, format = 'parquet') => {
      const def = getFormat(format)
      return insert(
        {
          kind: 'sink',
          format,
          path: '',
          mode: def?.modes[0] ?? 'overwrite',
          partitionBy: [],
          columns: null,
          options: {},
        },
        position,
      )
    },

    addValidation: (type, position) => {
      const def = getValidator(type)
      return insert(
        {
          kind: 'validation',
          validator: type,
          params: def ? defaultsFor(def.fields) : {},
          label: def?.label,
        },
        position,
      )
    },

    addNote: (position) =>
      insert({ kind: 'note', text: 'Double-click to edit', tone: 'brand' }, position),

    updateNodeData: (id, patch) => {
      commit(
        () =>
          set((state) => ({
            nodes: state.nodes.map((node) =>
              node.id === id
                ? ({ ...node, data: { ...node.data, ...patch } } as StudioNode)
                : node,
            ),
          })),
        { key: `data:${id}:${Object.keys(patch).join(',')}` },
      )
    },

    updateNodeParam: (id, key, value) => {
      commit(
        () =>
          set((state) => ({
            nodes: state.nodes.map((node) => {
              if (node.id !== id) return node
              if (node.data.kind !== 'transform' && node.data.kind !== 'validation') return node
              const params = { ...node.data.params }
              if (value === undefined || value === '' || value === null) delete params[key]
              else params[key] = value
              return { ...node, data: { ...node.data, params } } as StudioNode
            }),
          })),
        { key: `param:${id}:${key}` },
      )
    },

    removeNodes: (ids) => {
      const set_ = new Set(ids)
      commit(() =>
        set((state) => ({
          nodes: state.nodes.filter((node) => !set_.has(node.id)),
          edges: state.edges.filter((edge) => !set_.has(edge.source) && !set_.has(edge.target)),
          selectedNodeId: set_.has(state.selectedNodeId ?? '') ? null : state.selectedNodeId,
        })),
      )
    },

    duplicateNode: (id) => {
      const node = get().nodes.find((n) => n.id === id)
      if (!node) return
      insert(structuredClone(node.data), {
        x: node.position.x + 60,
        y: node.position.y + 60,
      })
    },

    toggleDisabled: (id) => {
      const node = get().nodes.find((n) => n.id === id)
      if (!node) return
      if (node.data.kind !== 'transform' && node.data.kind !== 'validation') return
      get().updateNodeData(id, { disabled: !node.data.disabled })
    },

    select: (selectedNodeId) => set({ selectedNodeId }),

    replaceGraph: (graph, options) => {
      const next = options?.layout === false ? graph : autoLayout(graph)
      commit(() => set({ nodes: next.nodes, edges: next.edges, selectedNodeId: null }))
    },

    applyPipeline: (pipeline, options) => {
      const result = pipelineToGraph(pipeline)
      const laid = autoLayout(result.graph)
      commit(() =>
        set((state) => ({
          nodes: laid.nodes,
          edges: laid.edges,
          selectedNodeId: null,
          settings: options?.keepSettings ? state.settings : result.settings,
          // An imported pipeline can introduce new {param} placeholders; values
          // already typed for existing keys survive.
          params: mergeParams(state.params, pipeline),
        })),
      )
      return result.issues
    },

    layout: () => {
      const { nodes, edges } = get()
      const laid = autoLayout({ nodes, edges })
      commit(() => set({ nodes: laid.nodes, edges: laid.edges }))
    },

    setSettings: (patch) => {
      commit(() => set((state) => ({ settings: { ...state.settings, ...patch } })), {
        key: `settings:${Object.keys(patch).join(',')}`,
      })
    },

    setParams: (params) => {
      commit(() => set({ params }), { key: 'params' })
    },

    undo: () => {
      const { past } = get()
      const previous = past[past.length - 1]
      if (!previous) return
      forgetCoalescing()
      set((state) => ({
        past: state.past.slice(0, -1),
        future: [snapshot(), ...state.future].slice(0, HISTORY_LIMIT),
        nodes: previous.nodes,
        edges: previous.edges,
        params: previous.params,
        settings: previous.settings,
        dirty: true,
      }))
      scheduleSave()
      if (liveLint()) scheduleLint()
    },

    redo: () => {
      const { future } = get()
      const next = future[0]
      if (!next) return
      forgetCoalescing()
      set((state) => ({
        past: [...state.past, snapshot()].slice(-HISTORY_LIMIT),
        future: state.future.slice(1),
        nodes: next.nodes,
        edges: next.edges,
        params: next.params,
        settings: next.settings,
        dirty: true,
      }))
      scheduleSave()
      if (liveLint()) scheduleLint()
    },

    canUndo: () => get().past.length > 0,
    canRedo: () => get().future.length > 0,

    compile: () => {
      const { nodes, edges, settings } = get()
      return compileGraph({ nodes, edges }, settings)
    },

    pipelineJson: () => {
      const { pipeline } = get().compile()
      return pipeline ? serializePipeline(pipeline) : ''
    },

    lint: () => {
      const { nodes, edges, settings, params } = get()
      const compileIssues = compileGraph({ nodes, edges }, settings).issues
      const lintIssues = lintJob({ nodes, edges }, settings, params)
      const seen = new Set<string>()
      const issues = [...compileIssues, ...lintIssues].filter((issue) => {
        if (seen.has(issue.id)) return false
        seen.add(issue.id)
        return true
      })
      set({ issues })
    },

    setRun: (run) => set({ run }),
    setRunning: (running) => set({ running }),

    setStepStatus: (nodeId, status) =>
      set((state) => ({ stepStatus: { ...state.stepStatus, [nodeId]: status } })),
    setStepStatuses: (stepStatus) => set({ stepStatus }),
    clearStepStatus: () => set({ stepStatus: {} }),
    transformNodeIdsInOrder: () => mainChainTransformNodeIds(get()),

    togglePanel: (panel, open) =>
      set((state) => {
        if (open === true) return { activePanel: panel }
        if (open === false) {
          return { activePanel: state.activePanel === panel ? null : state.activePanel }
        }
        return { activePanel: state.activePanel === panel ? null : panel }
      }),

    setPanelWidth: (width) => set({ panelWidth: clampPanelWidth(width) }),
  }
})

/**
 * Node ids behind the main `transformations` array, in the exact order the
 * compiler emits them — the runner reports each step by its index into that
 * array, so a mismatch would light up the wrong node.
 *
 * The selection mirrors `compileGraph()` (`lib/compiler/toJson.ts`) and reuses the
 * same primitives: walk back from every destination, keep the prefix all of them
 * share, and stop at the LAST validation rule of the run, because the framework
 * runs the main transformations, then the validations block, then each output's own
 * transformations. Chains the compiler rejects are skipped here too — a graph that
 * does not compile has no steps to report.
 *
 * Returns a fresh array, so read it through `getState()` or memoise it rather
 * than passing it straight to `useEditorStore` as a selector.
 */
export function mainChainTransformNodeIds(state: StudioGraph): string[] {
  const graph: StudioGraph = { nodes: state.nodes, edges: state.edges }
  const middles: StudioNode[][] = []

  for (const sink of mainSinksOf(graph)) {
    const walk = chainToSink(graph, sink.id)
    if (walk.problem) continue
    const chain = walk.nodes.filter(isCompilable)
    const head = chain[0]
    if (!head || !isSourceNode(head)) continue
    if (chain.slice(1).some(isSourceNode)) continue
    middles.push(chain.slice(1, chain.length - 1))
  }

  if (middles.length === 0) return []
  const prefix = longestCommonPrefix(middles, (a, b) => a.id === b.id)
  return sharedChainOf(prefix)
    .filter(isTransformNode)
    .map((node) => node.id)
}

/**
 * The destinations that compile into `outputs[]`.
 *
 * The validation side outputs (report, quarantine) are excluded: they are written
 * from the same DataFrame the rules saw, not from a branch of the chain, and
 * folding them into the shared-prefix computation would cut the main chain short at
 * the rule they hang off.
 */
function mainSinksOf(graph: StudioGraph): StudioNode[] {
  return graph.nodes.filter(
    (node) => isSinkNode(node) && !isDisabled(node) && !isValidationSink(graph, node.id),
  )
}

/**
 * The part of a shared prefix that runs before the outputs branch: everything up to
 * and including the last validation rule, or the whole prefix when there is none.
 */
function sharedChainOf(prefix: StudioNode[]): StudioNode[] {
  for (let index = prefix.length - 1; index >= 0; index -= 1) {
    if (isValidationNode(prefix[index])) return prefix.slice(0, index + 1)
  }
  return prefix
}

/**
 * The source node and the sink nodes a run touches, in the order the compiler
 * emits them — the source feeds `input`, and the sinks line up index for index
 * with the compiled `outputs` array, so the runner's `scope: 'input' | 'output'`
 * step markers can be mapped back onto the canvas.
 *
 * Mirrors `mainChainTransformNodeIds`: same chain walk, same rejections.
 */
export function runtimeEndpointNodeIds(state: StudioGraph): {
  sourceId: string | null
  sinkIds: string[]
} {
  const graph: StudioGraph = { nodes: state.nodes, edges: state.edges }
  const sinkIds: string[] = []
  let sourceId: string | null = null

  for (const sink of mainSinksOf(graph)) {
    const walk = chainToSink(graph, sink.id)
    if (walk.problem) continue
    const chain = walk.nodes.filter(isCompilable)
    const head = chain[0]
    if (!head || !isSourceNode(head)) continue
    if (chain.slice(1).some(isSourceNode)) continue
    sourceId ??= head.id
    sinkIds.push(sink.id)
  }

  return { sourceId, sinkIds }
}

/**
 * A 1-based number for every node a run touches, in execution order: the source,
 * the shared transformations, every validation rule, then each destination's own
 * branch followed by the destination itself.
 *
 * It gives every box on the canvas a stable handle ("step 3") that matches the
 * order the pipeline actually runs, so the logs and the canvas can be read side
 * by side. Nodes outside any compilable chain (notes, orphans) get no number.
 */
export function nodeOrdinals(state: StudioGraph): Record<string, number> {
  const graph: StudioGraph = { nodes: state.nodes, edges: state.edges }
  const ordered: string[] = []
  const seen = new Set<string>()
  const push = (id: string) => {
    if (seen.has(id)) return
    seen.add(id)
    ordered.push(id)
  }

  const chains: { sink: StudioNode; middle: StudioNode[]; head: StudioNode }[] = []
  for (const sink of mainSinksOf(graph)) {
    const walk = chainToSink(graph, sink.id)
    if (walk.problem) continue
    const chain = walk.nodes.filter(isCompilable)
    const head = chain[0]
    if (!head || !isSourceNode(head)) continue
    if (chain.slice(1).some(isSourceNode)) continue
    chains.push({ sink, head, middle: chain.slice(1, chain.length - 1) })
  }
  if (chains.length === 0) return {}

  const prefix = longestCommonPrefix(
    chains.map((entry) => entry.middle),
    (a, b) => a.id === b.id,
  )
  const mainPrefix = sharedChainOf(prefix)

  const first = chains[0]
  if (first) push(first.head.id)
  for (const node of mainPrefix) push(node.id)
  // `_write_validation_report` and `_write_validation_outputs` both run BEFORE
  // `_write_outputs`, so the side outputs are numbered right after the rules and
  // ahead of the job's own destinations — the order a log reader sees.
  const rules = new Set(mainPrefix.filter(isValidationNode).map((node) => node.id))
  for (const sink of graph.nodes.filter(isSinkNode)) {
    if (isDisabled(sink)) continue
    const link = validationSinkLink(graph, sink.id)
    if (link && rules.has(link.parent.id)) push(sink.id)
  }
  for (const entry of chains) {
    for (const node of entry.middle.slice(mainPrefix.length)) push(node.id)
    push(entry.sink.id)
  }

  return Object.fromEntries(ordered.map((id, index) => [id, index + 1]))
}

/** Would adding source→target close a loop? */
function createsCycle(edges: StudioEdge[], source: string, target: string): boolean {
  const adjacency = new Map<string, string[]>()
  for (const edge of edges) {
    const list = adjacency.get(edge.source) ?? []
    list.push(edge.target)
    adjacency.set(edge.source, list)
  }
  const stack = [target]
  const seen = new Set<string>()
  while (stack.length) {
    const current = stack.pop()
    if (!current) break
    if (current === source) return true
    if (seen.has(current)) continue
    seen.add(current)
    stack.push(...(adjacency.get(current) ?? []))
  }
  return false
}
