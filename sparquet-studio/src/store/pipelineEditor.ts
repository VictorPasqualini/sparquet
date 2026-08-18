/**
 * State of the pipeline editor: the stages on the canvas, the links that
 * order them, and the live progress of a sequential run.
 *
 * Deliberately separate from `store/editor.ts`. That store owns a single
 * pipeline's graph — nodes, params, compile, lint — and a pipeline shares none of
 * it: a stage is a reference to a job, not a node with fields. Sharing one
 * store would mean two half-populated shapes and a lint pass that means nothing
 * here. What IS shared is the persistence rhythm (debounced autosave, undo
 * history, flush on close), mirrored below so both editors behave alike.
 */

import { create } from 'zustand'

import { linkRejection, newLink, newStage, type LinkRejection } from '@/lib/pipeline'
import * as db from '@/lib/storage/db'
import type {
  Pipeline,
  PipelineLink,
  PipelineRunResult,
  PipelineStage,
  PipelineStageResult,
  RunLogLine,
  StepStatus,
} from '@/types/studio'

import { useLibraryStore } from './library'

const AUTOSAVE_DELAY = 700
const HISTORY_LIMIT = 60

interface Snapshot {
  stages: PipelineStage[]
  links: PipelineLink[]
}

interface PipelineEditorState {
  pipeline: Pipeline | null
  stages: PipelineStage[]
  links: PipelineLink[]
  selectedStageId: string | null

  dirty: boolean
  saving: boolean
  lastSavedAt: number | null

  past: Snapshot[]
  future: Snapshot[]

  running: boolean
  run: PipelineRunResult | null
  /** Live status per stage id; seeded to `pending` for every stage on run start. */
  stageStatus: Record<string, StepStatus>
  /** Settled outcome per stage id, filled as `stage_result` events arrive. */
  stageResults: Record<string, PipelineStageResult>
  /** Lines streamed by the current run, in arrival order. */
  logs: RunLogLine[]

  /* lifecycle */
  open: (pipeline: Pipeline) => void
  close: () => void
  save: () => Promise<void>

  /* graph */
  addStage: (jobId: string, position: { x: number; y: number }) => string
  moveStage: (id: string, position: { x: number; y: number }, settled: boolean) => void
  removeStages: (ids: string[]) => void
  /** Adds an order link. Returns why it was refused, or `null` on success. */
  connect: (source: string, target: string) => LinkRejection | null
  removeLinks: (ids: string[]) => void
  select: (id: string | null) => void

  /* history */
  undo: () => void
  redo: () => void

  /* run */
  startRun: (stageIds: string[]) => void
  appendLog: (line: RunLogLine) => void
  markStage: (id: string, status: StepStatus) => void
  setStageResult: (result: PipelineStageResult) => void
  finishRun: (result: PipelineRunResult) => void
  clearRun: () => void
}

let autosaveTimer: ReturnType<typeof setTimeout> | null = null
/** Writes run one at a time so two saves never derive a revision from the same base. */
let writeChain: Promise<void> = Promise.resolve()

export const usePipelineEditorStore = create<PipelineEditorState>((set, get) => {
  const snapshot = (): Snapshot => {
    const { stages, links } = get()
    return { stages, links }
  }

  const scheduleSave = () => {
    if (autosaveTimer) clearTimeout(autosaveTimer)
    autosaveTimer = setTimeout(() => {
      autosaveTimer = null
      void get().save()
    }, AUTOSAVE_DELAY)
  }

  /** Records history, marks the pipeline dirty and schedules the write. */
  const commit = (updater: () => void, options: { history?: boolean } = {}) => {
    const before = options.history === false ? null : snapshot()
    updater()
    if (before) {
      set((state) => ({
        past: [...state.past, before].slice(-HISTORY_LIMIT),
        future: [],
      }))
    }
    set({ dirty: true })
    scheduleSave()
  }

  const write = async (pipeline: Pipeline, stages: PipelineStage[], links: PipelineLink[]) => {
    set({ saving: true })
    const stored = await db.getPipeline(pipeline.id)
    const next: Pipeline = {
      ...pipeline,
      stages,
      links,
      updatedAt: Date.now(),
      revision: Math.max(pipeline.revision, stored?.revision ?? 0) + 1,
    }
    await db.savePipeline(next)
    useLibraryStore.getState().upsertPipeline(next)
    set((state) => {
      // The editor moved on (closed or switched pipeline): only the write mattered.
      if (state.pipeline?.id !== pipeline.id) return { saving: false }
      // Edits that landed while the write was in flight are not part of `next`;
      // keep `dirty` so the save they scheduled still persists them.
      const settled = state.stages === stages && state.links === links
      return {
        pipeline: next,
        saving: false,
        lastSavedAt: next.updatedAt,
        dirty: settled ? false : state.dirty,
      }
    })
  }

  /** Queues the pending edit for persistence; safe to call right before a reset. */
  const flush = (): Promise<void> => {
    const { pipeline, stages, links, dirty } = get()
    if (!pipeline || !dirty) return writeChain
    writeChain = writeChain.then(async () => {
      try {
        await write(pipeline, stages, links)
      } catch {
        set({ saving: false })
      }
    })
    return writeChain
  }

  return {
    pipeline: null,
    stages: [],
    links: [],
    selectedStageId: null,

    dirty: false,
    saving: false,
    lastSavedAt: null,

    past: [],
    future: [],

    running: false,
    run: null,
    stageStatus: {},
    stageResults: {},
    logs: [],

    open: (pipeline) => {
      if (get().pipeline?.id === pipeline.id) return
      if (autosaveTimer) {
        clearTimeout(autosaveTimer)
        autosaveTimer = null
      }
      // Whatever the previous pipeline still owed the disk must land before the state
      // that holds it is replaced.
      void flush()
      set({
        pipeline,
        stages: pipeline.stages,
        links: pipeline.links,
        selectedStageId: null,
        dirty: false,
        saving: false,
        lastSavedAt: pipeline.updatedAt,
        past: [],
        future: [],
        running: false,
        run: null,
        stageStatus: {},
        stageResults: {},
        logs: [],
      })
    },

    close: () => {
      if (autosaveTimer) {
        clearTimeout(autosaveTimer)
        autosaveTimer = null
      }
      // The screen unmounts on every navigation: flush the debounced autosave
      // instead of dropping it, or the last edits never reach storage.
      void flush()
      set({
        pipeline: null,
        stages: [],
        links: [],
        selectedStageId: null,
        dirty: false,
        saving: false,
        past: [],
        future: [],
        running: false,
        run: null,
        stageStatus: {},
        stageResults: {},
        logs: [],
      })
    },

    save: () => flush(),

    addStage: (jobId, position) => {
      const stage = newStage(jobId, position)
      commit(() =>
        set((state) => ({
          stages: [...state.stages, stage],
          selectedStageId: stage.id,
        })),
      )
      return stage.id
    },

    moveStage: (id, position, settled) => {
      const apply = () =>
        set((state) => ({
          stages: state.stages.map((stage) =>
            stage.id === id ? { ...stage, position } : stage,
          ),
        }))
      // Drags fire continuously; only the drop is worth an undo entry.
      if (settled) commit(apply)
      else {
        apply()
        set({ dirty: true })
        scheduleSave()
      }
    },

    removeStages: (ids) => {
      const removed = new Set(ids)
      commit(() =>
        set((state) => ({
          stages: state.stages.filter((stage) => !removed.has(stage.id)),
          links: state.links.filter(
            (link) => !removed.has(link.source) && !removed.has(link.target),
          ),
          selectedStageId: removed.has(state.selectedStageId ?? '')
            ? null
            : state.selectedStageId,
        })),
      )
    },

    connect: (source, target) => {
      const { links } = get()
      const rejection = linkRejection(links, source, target)
      if (rejection) return rejection
      const link = newLink(source, target)
      commit(() => set((state) => ({ links: [...state.links, link] })))
      return null
    },

    removeLinks: (ids) => {
      const removed = new Set(ids)
      commit(() => set((state) => ({ links: state.links.filter((l) => !removed.has(l.id)) })))
    },

    select: (selectedStageId) => set({ selectedStageId }),

    undo: () => {
      const previous = get().past.at(-1)
      if (!previous) return
      set((state) => ({
        past: state.past.slice(0, -1),
        future: [snapshot(), ...state.future].slice(0, HISTORY_LIMIT),
        stages: previous.stages,
        links: previous.links,
        dirty: true,
      }))
      scheduleSave()
    },

    redo: () => {
      const next = get().future[0]
      if (!next) return
      set((state) => ({
        past: [...state.past, snapshot()].slice(-HISTORY_LIMIT),
        future: state.future.slice(1),
        stages: next.stages,
        links: next.links,
        dirty: true,
      }))
      scheduleSave()
    },

    startRun: (stageIds) => {
      set({
        running: true,
        run: null,
        // Every stage starts as `pending`: the boxes must show the whole plan,
        // not only the stage the runner happens to have reached.
        stageStatus: Object.fromEntries(stageIds.map((id) => [id, 'pending' as const])),
        stageResults: {},
        logs: [],
      })
    },

    appendLog: (line) => set((state) => ({ logs: [...state.logs, line] })),

    markStage: (id, status) =>
      set((state) => ({ stageStatus: { ...state.stageStatus, [id]: status } })),

    setStageResult: (result) =>
      set((state) => ({
        stageStatus: { ...state.stageStatus, [result.id]: result.status },
        stageResults: { ...state.stageResults, [result.id]: result },
      })),

    finishRun: (result) => set({ running: false, run: result }),

    clearRun: () =>
      set({ running: false, run: null, stageStatus: {}, stageResults: {}, logs: [] }),
  }
})
