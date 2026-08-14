import { nanoid } from 'nanoid'
import { create } from 'zustand'

import { autoLayout, pipelineToGraph } from '@/lib/compiler'
import { inferParams } from '@/lib/params'
import * as db from '@/lib/storage/db'
import type { ParamDefinition, Project, ProjectAccent, Workflow } from '@/types/studio'

interface LibraryState {
  projects: Project[]
  workflows: Workflow[]
  loading: boolean
  error: string | null

  load: () => Promise<void>
  createProject: (input: { name: string; description?: string; accent?: ProjectAccent }) => Promise<Project>
  updateProject: (id: string, patch: Partial<Omit<Project, 'id' | 'createdAt'>>) => Promise<void>
  deleteProject: (id: string) => Promise<void>

  createWorkflow: (input: {
    projectId: string
    name: string
    description?: string
    /** Seed the graph from an existing pipeline JSON (template, import, AI). */
    pipeline?: unknown
  }) => Promise<Workflow>
  updateWorkflowMeta: (
    id: string,
    patch: Partial<Pick<Workflow, 'name' | 'description' | 'tags' | 'projectId'>>,
  ) => Promise<void>
  deleteWorkflow: (id: string) => Promise<void>
  duplicateWorkflow: (id: string) => Promise<Workflow | null>
  /** Called by the editor after a save so lists stay fresh without a reload. */
  upsertWorkflow: (workflow: Workflow) => void
}

const emptyGraph = { nodes: [], edges: [] }

export const useLibraryStore = create<LibraryState>((set, get) => ({
  projects: [],
  workflows: [],
  loading: false,
  error: null,

  load: async () => {
    set({ loading: true, error: null })
    try {
      const [projects, workflows] = await Promise.all([db.listProjects(), db.listWorkflows()])
      set({ projects, workflows, loading: false })
    } catch (error) {
      set({ loading: false, error: error instanceof Error ? error.message : String(error) })
    }
  },

  createProject: async ({ name, description = '', accent = 'amber' }) => {
    const now = Date.now()
    const project: Project = {
      id: nanoid(10),
      name,
      description,
      accent,
      createdAt: now,
      updatedAt: now,
    }
    await db.saveProject(project)
    set((state) => ({ projects: [...state.projects, project] }))
    return project
  },

  updateProject: async (id, patch) => {
    const current = get().projects.find((p) => p.id === id)
    if (!current) return
    const next: Project = { ...current, ...patch, updatedAt: Date.now() }
    await db.saveProject(next)
    set((state) => ({ projects: state.projects.map((p) => (p.id === id ? next : p)) }))
  },

  deleteProject: async (id) => {
    await db.deleteProject(id)
    set((state) => ({
      projects: state.projects.filter((p) => p.id !== id),
      workflows: state.workflows.filter((w) => w.projectId !== id),
    }))
  },

  createWorkflow: async ({ projectId, name, description = '', pipeline }) => {
    const now = Date.now()
    let graph = emptyGraph as Workflow['graph']
    let settings: Workflow['settings'] = {
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

    const workflow: Workflow = {
      id: nanoid(10),
      projectId,
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
    await db.saveWorkflow(workflow)
    set((state) => ({ workflows: [...state.workflows, workflow] }))
    return workflow
  },

  updateWorkflowMeta: async (id, patch) => {
    const current = get().workflows.find((w) => w.id === id)
    if (!current) return
    const next: Workflow = {
      ...current,
      ...patch,
      updatedAt: Date.now(),
      revision: current.revision + 1,
    }
    await db.saveWorkflow(next)
    set((state) => ({ workflows: state.workflows.map((w) => (w.id === id ? next : w)) }))
  },

  deleteWorkflow: async (id) => {
    await db.deleteWorkflow(id)
    set((state) => ({ workflows: state.workflows.filter((w) => w.id !== id) }))
  },

  duplicateWorkflow: async (id) => {
    const current = get().workflows.find((w) => w.id === id)
    if (!current) return null
    const copy = await db.duplicateWorkflow(id, `Copy of ${current.name}`)
    if (copy) set((state) => ({ workflows: [...state.workflows, copy] }))
    return copy
  },

  upsertWorkflow: (workflow) =>
    set((state) => ({
      workflows: state.workflows.some((w) => w.id === workflow.id)
        ? state.workflows.map((w) => (w.id === workflow.id ? workflow : w))
        : [...state.workflows, workflow],
    })),
}))

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
