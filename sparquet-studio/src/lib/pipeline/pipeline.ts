/**
 * Pipeline — several pipeline FILES wired into one sequence.
 *
 * Each stage is one JSON: it has its own input, transformations and outputs, and
 * Sparquet runs it on its own. A pipeline adds the one thing a single JSON
 * cannot express — the ORDER several of them run in — and the runner executes
 * that order stage by stage.
 *
 * The order comes from links the author DRAWS. Nothing is inferred from paths:
 * two stages can share no path at all and still have to run in a fixed order
 * (e.g. a truncate before a load), so guessing would be the wrong answer.
 *
 * A stage stores only a `jobId`. The pipeline JSON is compiled from the
 * referenced job at run time, so a stage can never drift from the file it
 * points at — and a deleted job leaves a stage this module reports as
 * broken instead of a copy of stale JSON that still runs.
 *
 * Pure: no React, no store, no IO.
 */

import { nanoid } from 'nanoid'

import { compileGraph } from '@/lib/compiler'
import {
  describeJob,
  topologicalOrder,
  type JobDescription,
} from '@/lib/pipeline/describe'
import { paramValues } from '@/lib/params'
import type { PipelineSpec } from '@/types/pipeline'
import type {
  Pipeline,
  PipelineLink,
  PipelineStage,
  ValidationIssue,
  Job,
} from '@/types/studio'

/* ------------------------------------------------------------------- types */

export interface ResolvedStage {
  /** `PipelineStage.id` — the canvas node id and the id the runner reports back. */
  id: string
  jobId: string
  position: { x: number; y: number }
  /** 1-based execution order. Stages in a cycle are numbered last. */
  order: number
  /** `null` when the referenced job no longer exists. */
  job: Job | null
  /** The job name, or a placeholder when the reference is broken. */
  name: string
  /** What the stage reads, writes and does. `null` for a broken reference. */
  description: JobDescription | null
  /** The pipeline this stage would run. `null` when it does not compile. */
  pipeline: PipelineSpec | null
}

export interface ResolvedPipeline {
  /** Stages in execution order. */
  stages: ResolvedStage[]
  links: PipelineLink[]
  /**
   * Problems that stop the pipeline from running, plus warnings worth showing.
   * `nodeId` is the stage id, so the canvas can point at the box.
   */
  issues: ValidationIssue[]
}

/** Why a link was refused, or `null` when it is allowed. */
export type LinkRejection = 'self' | 'duplicate' | 'cycle'

/* ------------------------------------------------------------------ factory */

/**
 * Pitch between stages placed automatically, wide enough for the box plus the
 * link label. Shared by the canvas and by a pipeline seeded from a list of files.
 */
export const STAGE_COLUMN_GAP = 420

/** Left-to-right row position for the nth stage, so a seeded pipeline reads as a line. */
export function stageRowPosition(index: number): { x: number; y: number } {
  return { x: index * STAGE_COLUMN_GAP, y: 0 }
}

export function newStage(jobId: string, position: { x: number; y: number }): PipelineStage {
  return { id: `stage-${nanoid(6)}`, jobId, position }
}

export function newLink(source: string, target: string): PipelineLink {
  return { id: `link-${nanoid(6)}`, source, target }
}

export function newPipeline(input: {
  workflowId: string
  name: string
  description?: string
  stages?: PipelineStage[]
  links?: PipelineLink[]
}): Pipeline {
  const now = Date.now()
  return {
    id: nanoid(10),
    workflowId: input.workflowId,
    name: input.name,
    description: input.description ?? '',
    stages: input.stages ?? [],
    links: input.links ?? [],
    tags: [],
    createdAt: now,
    updatedAt: now,
    revision: 1,
  }
}

/* -------------------------------------------------------------- link rules */

/**
 * The same rules the pipeline canvas enforces: no self-link, no duplicate, and
 * never a cycle — a sequence with a loop has no first stage, so it could not be
 * run at all.
 */
export function linkRejection(
  links: readonly PipelineLink[],
  source: string,
  target: string,
): LinkRejection | null {
  if (source === target) return 'self'
  if (links.some((link) => link.source === source && link.target === target)) return 'duplicate'
  return wouldCreateCycle(links, source, target) ? 'cycle' : null
}

/** Would adding `source → target` close a loop? */
export function wouldCreateCycle(
  links: readonly PipelineLink[],
  source: string,
  target: string,
): boolean {
  if (source === target) return true
  return reaches(links, target, source)
}

/** Stable tie-break: by name, then by id, so numbering never flickers. */
function byNameThenId(a: string, b: string, nameById: ReadonlyMap<string, string>): number {
  const key = (id: string) => `${(nameById.get(id) ?? '').toLowerCase()} ${id}`
  return key(a).localeCompare(key(b))
}

/** Can `from` already reach `goal` by following links forwards? */
function reaches(links: readonly PipelineLink[], from: string, goal: string): boolean {
  const adjacency = new Map<string, string[]>()
  for (const link of links) {
    adjacency.set(link.source, [...(adjacency.get(link.source) ?? []), link.target])
  }

  const stack = [from]
  const seen = new Set<string>()
  while (stack.length > 0) {
    const current = stack.pop()
    if (current === undefined) break
    if (current === goal) return true
    if (seen.has(current)) continue
    seen.add(current)
    stack.push(...(adjacency.get(current) ?? []))
  }
  return false
}

/* ----------------------------------------------------------------- resolve */

const MISSING_STAGE_NAME = 'Missing job'

interface StageSource {
  description: JobDescription
  pipeline: PipelineSpec | null
}

/**
 * Describing a stage means compiling its job, which is far too expensive to
 * redo while a box is being dragged — every pointer move resolves the pipeline again.
 *
 * Job records are immutable (the stores replace them on every save), so
 * identity is a sound cache key: an edited job is a new object and misses.
 * A WeakMap keeps nothing alive on its own, so deleted jobs simply vanish.
 */
const sourceCache = new WeakMap<Job, StageSource>()

function stageSource(job: Job): StageSource {
  const cached = sourceCache.get(job)
  if (cached) return cached
  const { pipeline } = compileGraph(job.graph, job.settings, job.params)
  const source: StageSource = { description: describeJob(job), pipeline }
  sourceCache.set(job, source)
  return source
}

/**
 * Turns a stored pipeline plus the workflow's workflows into what the canvas draws
 * and what the runner is sent: stages in execution order, each carrying the
 * pipeline it would run, plus every issue that blocks the run.
 *
 * Never throws and never drops a stage: a broken reference, a job that does
 * not compile and even a cycle (only reachable through an imported record, since
 * `linkRejection` refuses to draw one) all resolve to a stage plus an issue.
 */
export function resolvePipeline(
  pipeline: Pipeline,
  jobs: readonly Job[],
): ResolvedPipeline {
  const byId = new Map(jobs.map((job) => [job.id, job]))
  const issues: ValidationIssue[] = []

  const described = pipeline.stages.map((stage) => {
    const job = byId.get(stage.jobId) ?? null
    if (!job) {
      return { stage, job, name: MISSING_STAGE_NAME, description: null, pipeline: null }
    }
    // Compiling is what keeps a stage honest: `pipeline` is the very JSON Sparquet
    // would execute, read out of the job's canvas as it stands right now.
    const source = stageSource(job)
    return {
      stage,
      job,
      name: job.name,
      description: source.description,
      pipeline: source.pipeline,
    }
  })

  // Links pointing at a stage that is no longer on the canvas would silently
  // change the order; drop them here rather than persisting a repair.
  const stageIds = new Set(pipeline.stages.map((stage) => stage.id))
  const links = pipeline.links.filter(
    (link) => stageIds.has(link.source) && stageIds.has(link.target),
  )

  const nameById = new Map(described.map((entry) => [entry.stage.id, entry.name]))

  // Wired stages are ordered by their links; stages nothing links to run LAST.
  // Kahn would otherwise treat an unwired stage as a root and number it first,
  // so dropping a new box on the canvas would renumber the whole sequence.
  const linked = new Set(links.flatMap((link) => [link.source, link.target]))
  const wired = [...stageIds].filter((id) => linked.has(id))
  const orphans = [...stageIds]
    .filter((id) => !linked.has(id))
    .sort((a, b) => byNameThenId(a, b, nameById))

  const { ordered, cyclic } = topologicalOrder(wired, nameById, links)
  const orderById = new Map([...ordered, ...orphans].map((id, index) => [id, index + 1]))
  const inCycle = new Set(cyclic)

  const stages: ResolvedStage[] = described
    .map((entry) => ({
      id: entry.stage.id,
      jobId: entry.stage.jobId,
      position: entry.stage.position,
      order: orderById.get(entry.stage.id) ?? 0,
      job: entry.job,
      name: entry.name,
      description: entry.description,
      pipeline: entry.pipeline,
    }))
    .sort((a, b) => a.order - b.order)

  if (stages.length === 0) {
    issues.push({
      id: 'pipeline-empty',
      severity: 'info',
      message: 'This pipeline has no stages yet.',
      hint: 'Add a pipeline from the panel on the left, then link the boxes to set the order.',
    })
  }

  for (const stage of stages) {
    if (!stage.job) {
      issues.push({
        id: `pipeline-missing-${stage.id}`,
        severity: 'error',
        nodeId: stage.id,
        message: 'This stage points at a job that no longer exists.',
        hint: 'Delete the stage, or recreate the job it referenced.',
      })
      continue
    }
    if (!stage.pipeline) {
      issues.push({
        id: `pipeline-uncompilable-${stage.id}`,
        severity: 'error',
        nodeId: stage.id,
        message: `"${stage.name}" does not compile into a pipeline yet.`,
        hint: 'Open the stage and fix the blocking issues on its canvas.',
      })
    }
    if (inCycle.has(stage.id)) {
      issues.push({
        id: `pipeline-cycle-${stage.id}`,
        severity: 'error',
        nodeId: stage.id,
        message: `"${stage.name}" is part of a loop, so it has no place in the order.`,
        hint: 'Remove one of the links that closes the loop.',
      })
    }
  }

  // A stage nothing links to still runs, at the end, which is rarely deliberate.
  if (stages.length > 1) {
    for (const stage of stages) {
      if (linked.has(stage.id) || inCycle.has(stage.id)) continue
      issues.push({
        id: `pipeline-unlinked-${stage.id}`,
        severity: 'warning',
        nodeId: stage.id,
        message: `"${stage.name}" is not linked to any other stage.`,
        hint: 'Connect it so its position in the sequence is deliberate.',
      })
    }
  }

  return { stages, links, issues }
}

/* --------------------------------------------------------------- run plan */

/** One stage as the runner wants it: an id, a name and the compiled pipeline. */
export interface PipelineRunStage {
  id: string
  name: string
  pipeline: PipelineSpec
  params?: Record<string, string | number | boolean | string[]>
  /** The Studio job this stage runs, so the persisted execution history can link back to it. */
  jobId: string
}

export interface PipelineRunPlan {
  /** Stages in execution order, ready to POST. Empty when anything blocks. */
  stages: PipelineRunStage[]
  /** Errors that must be fixed before the pipeline can run. */
  blockers: ValidationIssue[]
}

/**
 * Builds the request body for a pipeline run, or the reasons it cannot be built.
 *
 * All-or-nothing on purpose: a sequence is only meaningful whole, so one broken
 * stage stops the run instead of quietly executing the rest in a shortened order.
 */
export function planPipelineRun(resolved: ResolvedPipeline): PipelineRunPlan {
  const blockers = resolved.issues.filter((issue) => issue.severity === 'error')

  if (resolved.stages.length === 0) {
    return {
      stages: [],
      blockers: [
        {
          id: 'pipeline-run-empty',
          severity: 'error',
          message: 'Add at least one stage before running the pipeline.',
        },
      ],
    }
  }
  if (blockers.length > 0) return { stages: [], blockers }

  const stages: PipelineRunStage[] = []
  for (const stage of resolved.stages) {
    // `blockers` already guarantees both, but the compiler cannot know that.
    if (!stage.pipeline || !stage.job) continue
    const values = paramValues(stage.job.params)
    stages.push({
      id: stage.id,
      name: stage.name,
      pipeline: stage.pipeline,
      jobId: stage.job.id,
      ...(Object.keys(values).length > 0 ? { params: values } : {}),
    })
  }

  return { stages, blockers: [] }
}
