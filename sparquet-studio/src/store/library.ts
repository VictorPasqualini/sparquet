import { nanoid } from 'nanoid'
import { create } from 'zustand'
import { useShallow } from 'zustand/react/shallow'

import { autoLayout, pipelineToGraph } from '@/lib/compiler'
import { newPipeline, newLink, newStage, stageRowPosition } from '@/lib/pipeline'
import { inferParams } from '@/lib/params'
import { collectTags } from '@/lib/tags'
import * as db from '@/lib/storage/db'
import type {
  Pipeline,
  ParamDefinition,
  Workflow,
  WorkflowAccent,
  Job,
} from '@/types/studio'

interface LibraryState {
  workflows: Workflow[]
  jobs: Job[]
  /** Pipelines: ordered sequences of jobs, one record per pipeline. */
  pipelines: Pipeline[]
  loading: boolean
  error: string | null

  load: () => Promise<void>
  createWorkflow: (input: {
    name: string
    description?: string
    accent?: WorkflowAccent
  }) => Promise<Workflow>
  updateWorkflow: (
    id: string,
    patch: Partial<Omit<Workflow, 'id' | 'createdAt'>>,
  ) => Promise<void>
  deleteWorkflow: (id: string) => Promise<void>

  createJob: (input: {
    workflowId: string
    name: string
    description?: string
    /** Seed the graph from an existing pipeline JSON (template, import, AI). */
    pipeline?: unknown
  }) => Promise<Job>
  updateJobMeta: (
    id: string,
    patch: Partial<Pick<Job, 'name' | 'description' | 'tags' | 'workflowId'>>,
  ) => Promise<void>
  deleteJob: (id: string) => Promise<void>
  duplicateJob: (id: string) => Promise<Job | null>
  /** Called by the editor after a save so lists stay fresh without a reload. */
  upsertJob: (job: Job) => void

  createPipeline: (input: {
    workflowId: string
    name: string
    description?: string
    /** Stage every job in this order, already linked head to tail. */
    jobIds?: string[]
  }) => Promise<Pipeline>
  updatePipelineMeta: (
    id: string,
    patch: Partial<Pick<Pipeline, 'name' | 'description' | 'tags' | 'workflowId'>>,
  ) => Promise<void>
  deletePipeline: (id: string) => Promise<void>
  /** Called by the pipeline editor after a save, mirroring `upsertJob`. */
  upsertPipeline: (pipeline: Pipeline) => void
}

const emptyGraph = { nodes: [], edges: [] }

export const useLibraryStore = create<LibraryState>((set, get) => ({
  workflows: [],
  jobs: [],
  pipelines: [],
  loading: false,
  error: null,

  load: async () => {
    set({ loading: true, error: null })
    try {
      const [workflows, jobs, pipelines] = await Promise.all([
        db.listWorkflows(),
        db.listJobs(),
        db.listPipelines(),
      ])
      set({ workflows, jobs, pipelines, loading: false })
    } catch (error) {
      set({ loading: false, error: error instanceof Error ? error.message : String(error) })
    }
  },

  createWorkflow: async ({ name, description = '', accent = 'amber' }) => {
    const now = Date.now()
    const workflow: Workflow = {
      id: nanoid(10),
      name,
      description,
      accent,
      tags: [],
      createdAt: now,
      updatedAt: now,
    }
    await db.saveWorkflow(workflow)
    set((state) => ({ workflows: [...state.workflows, workflow] }))
    return workflow
  },

  updateWorkflow: async (id, patch) => {
    const current = get().workflows.find((p) => p.id === id)
    if (!current) return
    const next: Workflow = { ...current, ...patch, updatedAt: Date.now() }
    await db.saveWorkflow(next)
    set((state) => ({ workflows: state.workflows.map((p) => (p.id === id ? next : p)) }))
  },

  deleteWorkflow: async (id) => {
    await db.deleteWorkflow(id)
    set((state) => ({
      workflows: state.workflows.filter((p) => p.id !== id),
      jobs: state.jobs.filter((w) => w.workflowId !== id),
      pipelines: state.pipelines.filter((f) => f.workflowId !== id),
    }))
  },

  createJob: async ({ workflowId, name, description = '', pipeline }) => {
    const now = Date.now()
    let graph = emptyGraph as Job['graph']
    let settings: Job['settings'] = {
      pipelineName: slugify(name),
      description,
      spark: {},
    }

    let params: ParamDefinition[] = []

    if (pipeline) {
      const imported = pipelineToGraph(pipeline)
      graph = autoLayout(imported.graph)
      settings = imported.settings
      // Any imported pipeline may carry {param} placeholders; surface them as
      // editable inputs instead of leaving them literal at run time.
      params = inferParams(pipeline)
    }

    const job: Job = {
      id: nanoid(10),
      workflowId,
      name,
      description,
      tags: [],
      settings,
      graph,
      params,
      createdAt: now,
      updatedAt: now,
      revision: 1,
    }
    await db.saveJob(job)
    set((state) => ({ jobs: [...state.jobs, job] }))
    return job
  },

  updateJobMeta: async (id, patch) => {
    const current = get().jobs.find((w) => w.id === id)
    if (!current) return
    const next: Job = {
      ...current,
      ...patch,
      updatedAt: Date.now(),
      revision: current.revision + 1,
    }
    await db.saveJob(next)
    set((state) => ({ jobs: state.jobs.map((w) => (w.id === id ? next : w)) }))
  },

  deleteJob: async (id) => {
    await db.deleteJob(id)
    // Pipelines keep their reference on purpose: a stage pointing at a
    // deleted job is drawn as broken, which is information. Rewriting other
    // records here would hide the mistake instead of surfacing it.
    set((state) => ({ jobs: state.jobs.filter((w) => w.id !== id) }))
  },

  duplicateJob: async (id) => {
    const current = get().jobs.find((w) => w.id === id)
    if (!current) return null
    const copy = await db.duplicateJob(id, `Copy of ${current.name}`)
    if (copy) set((state) => ({ jobs: [...state.jobs, copy] }))
    return copy
  },

  upsertJob: (job) =>
    set((state) => ({
      jobs: state.jobs.some((w) => w.id === job.id)
        ? state.jobs.map((w) => (w.id === job.id ? job : w))
        : [...state.jobs, job],
    })),

  createPipeline: async ({ workflowId, name, description = '', jobIds = [] }) => {
    const stages = jobIds.map((jobId, index) =>
      newStage(jobId, stageRowPosition(index)),
    )
    // Seeded stages are chained head to tail: the order the user picked them in
    // is the order they meant, and an unlinked pile would say nothing.
    const links = stages.slice(1).map((stage, index) => newLink(stages[index].id, stage.id))

    const pipeline = newPipeline({ workflowId, name, description, stages, links })
    await db.savePipeline(pipeline)
    set((state) => ({ pipelines: [...state.pipelines, pipeline] }))
    return pipeline
  },

  updatePipelineMeta: async (id, patch) => {
    const current = get().pipelines.find((f) => f.id === id)
    if (!current) return
    const next: Pipeline = {
      ...current,
      ...patch,
      updatedAt: Date.now(),
      revision: current.revision + 1,
    }
    await db.savePipeline(next)
    set((state) => ({ pipelines: state.pipelines.map((f) => (f.id === id ? next : f)) }))
  },

  deletePipeline: async (id) => {
    await db.deletePipeline(id)
    set((state) => ({ pipelines: state.pipelines.filter((f) => f.id !== id) }))
  },

  upsertPipeline: (pipeline) =>
    set((state) => ({
      pipelines: state.pipelines.some((f) => f.id === pipeline.id)
        ? state.pipelines.map((f) => (f.id === pipeline.id ? pipeline : f))
        : [...state.pipelines, pipeline],
    })),
}))

/**
 * Every tag already in use, most used first — what the tag pickers offer.
 *
 * The selector derives a brand-new array on every call, and a plain selector is
 * compared by identity: React would see a different snapshot on each render and
 * loop until it gives up with "Maximum update depth exceeded". `useShallow`
 * compares the entries instead, so the hook only re-renders when the tags
 * actually change. It lives here, next to the store, because all three screens
 * that offer tags need the same list.
 */
export function useKnownTags(): string[] {
  return useLibraryStore(
    useShallow((state) => collectTags([...state.jobs, ...state.pipelines, ...state.workflows])),
  )
}

/** Turns a display name into a safe `name` value for the pipeline JSON. */
export function slugify(value: string): string {
  return (
    value
      .normalize('NFD')
      .replace(/\p{Diacritic}/gu, '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '') || 'pipeline'
  )
}
