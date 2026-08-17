/**
 * Studio domain model — workflows, jobs and the visual graph.
 *
 * The GRAPH is the source of truth while editing. Pipeline JSON is compiled from
 * it (`lib/compiler/toJson.ts`) and imported into it (`lib/compiler/toGraph.ts`),
 * so what the canvas shows and what Sparquet executes can never drift.
 */

import type { Edge, Node } from '@xyflow/react'

import type {
  OnFailureMode,
  OutputSpec,
  SparkSettings,
  ValidationRuleSpec,
  WriteMode,
} from './pipeline'

/* ------------------------------------------------------------------ nodes */

export type NodeKind = 'source' | 'transform' | 'validations' | 'sink' | 'note'

/** A data source: the pipeline `input`, or the right-hand side of a join/union. */
export type SourceNodeData = {
  kind: 'source'
  label?: string
  format: string
  path: string
  options: Record<string, unknown>
  /** Free-form note shown in the inspector; never compiled into JSON. */
  comment?: string
}

/** One entry of a `transformations` array. */
export type TransformNodeData = {
  kind: 'transform'
  label?: string
  /** Registry key, e.g. `filter`, `with_column`, `join`. */
  transform: string
  /** Every JSON key of the transformation except `type` / `skip_if_false`. */
  params: Record<string, unknown>
  /** `skip_if_false` guard, kept separate because the engine treats it specially. */
  skipIfFalse?: string
  /** Muted nodes stay on the canvas but are omitted from the compiled JSON. */
  disabled?: boolean
  comment?: string
}

/** The single `validations` block. Runs after transformations, before writes. */
export type ValidationsNodeData = {
  kind: 'validations'
  label?: string
  onFailure: OnFailureMode
  rules: ValidationRuleSpec[]
  /** Optional data-quality report sink. */
  report?: OutputSpec | null
  /** Optional row-routing (quarantine): keys `valid` / `invalid` → an output sink. */
  outputs?: Record<string, OutputSpec> | null
  comment?: string
}

/** One entry of `outputs` (or the single `output`). */
export type SinkNodeData = {
  kind: 'sink'
  label?: string
  format: string
  path: string
  mode: WriteMode | string
  partitionBy: string[]
  /** `null` means "write every column". */
  columns: string[] | null
  options: Record<string, unknown>
  comment?: string
}

/** Canvas-only sticky note. Never compiled. */
export type NoteNodeData = {
  kind: 'note'
  label?: string
  text: string
  tone: 'brand' | 'neutral' | 'info' | 'warning'
}

export type StudioNodeData =
  SourceNodeData | TransformNodeData | ValidationsNodeData | SinkNodeData | NoteNodeData

export type SourceNode = Node<SourceNodeData, 'source'>
export type TransformNode = Node<TransformNodeData, 'transform'>
export type ValidationsNode = Node<ValidationsNodeData, 'validations'>
export type SinkNode = Node<SinkNodeData, 'sink'>
export type NoteNode = Node<NoteNodeData, 'note'>

export type StudioNode = SourceNode | TransformNode | ValidationsNode | SinkNode | NoteNode

/** Handle ids used across the canvas. */
export const HANDLE = {
  /** Single incoming handle of every node. */
  in: 'in',
  /** Secondary incoming handle — the right side of `join` / `union`. */
  inRight: 'in-right',
  /** Single outgoing handle. */
  out: 'out',
} as const

export type StudioEdge = Edge

export interface StudioGraph {
  nodes: StudioNode[]
  edges: StudioEdge[]
}

/* -------------------------------------------------------------- jobs */

/** Types a `{param}` template variable can take, mirroring utils/template.py. */
export type ParamType = 'string' | 'number' | 'boolean' | 'list'

export interface ParamDefinition {
  id: string
  key: string
  type: ParamType
  value: string | number | boolean | string[]
  description?: string
}

export interface JobSettings {
  /** `name` inside the compiled pipeline JSON. */
  pipelineName: string
  description: string
  spark: SparkSettings
}

export interface Job {
  id: string
  workflowId: string
  name: string
  description: string
  tags: string[]
  settings: JobSettings
  graph: StudioGraph
  params: ParamDefinition[]
  createdAt: number
  updatedAt: number
  /** Bumped by every persisted mutation; used for optimistic-concurrency checks. */
  revision: number
}

export interface Workflow {
  id: string
  name: string
  description: string
  /** Tailwind-free accent token, resolved in the UI. */
  accent: WorkflowAccent
  createdAt: number
  updatedAt: number
}

/* --------------------------------------------------------- pipelines */

/**
 * One stage of a pipeline.
 *
 * A stage REFERENCES a job by id — it never copies its pipeline JSON — so
 * editing the job immediately changes what the stage runs, and a deleted
 * job leaves a reference the canvas reports as broken instead of silently
 * running stale JSON.
 */
export interface PipelineStage {
  /** Stable stage id: the React Flow node id, and the stage id sent to the runner. */
  id: string
  jobId: string
  /**
   * Canvas position. Kept as a plain pair rather than React Flow's `XYPosition`
   * so the persisted record never depends on the canvas library.
   */
  position: { x: number; y: number }
}

/**
 * A manual execution-order link: `source` runs before `target`. Drawn by the
 * author — never inferred from paths, which is what the read-only inferred pipeline
 * (`lib/pipeline/inferredPipeline.ts`) does instead.
 */
export interface PipelineLink {
  id: string
  /** Stage id that runs first. */
  source: string
  /** Stage id that runs after. */
  target: string
}

/** Several pipeline files wired into one sequence the runner executes in order. */
export interface Pipeline {
  id: string
  workflowId: string
  name: string
  description: string
  stages: PipelineStage[]
  links: PipelineLink[]
  createdAt: number
  updatedAt: number
  /** Bumped by every persisted mutation, like `Job.revision`. */
  revision: number
}

export const WORKFLOW_ACCENTS = ['amber', 'sky', 'violet', 'emerald', 'rose', 'slate'] as const

export type WorkflowAccent = (typeof WORKFLOW_ACCENTS)[number]

/* ------------------------------------------------------------- validation */

export type IssueSeverity = 'error' | 'warning' | 'info'

export interface ValidationIssue {
  id: string
  severity: IssueSeverity
  message: string
  /** Node the issue belongs to, when it is node-scoped. */
  nodeId?: string
  /** Field key inside the node, when the issue is field-scoped. */
  field?: string
  /** Short actionable hint shown under the message. */
  hint?: string
}

/* --------------------------------------------------------------- runtime */

export type RunStatus = 'idle' | 'connecting' | 'running' | 'success' | 'error' | 'skipped'

export interface RunLogLine {
  ts: number
  level: 'debug' | 'info' | 'warning' | 'error'
  message: string
  /** Where the line came from. Only the streaming endpoint labels its logs. */
  source?: 'pipeline' | 'stdout' | 'spark'
  context?: Record<string, unknown>
  /** Stage that emitted the line, on a pipeline run only. */
  stageId?: string
}

/** Lifecycle of a single transformation while the pipeline streams. */
export type StepStatus = 'pending' | 'running' | 'success' | 'error' | 'skipped'

/**
 * Per-transformation progress, keyed by the transformation's 0-based index in
 * the pipeline. `type` mirrors the transformation type reported by the runner.
 */
export type StepState = Record<number, { status: StepStatus; type?: string }>

export interface RunResult {
  status: RunStatus
  pipelineName?: string
  rowsRead?: number
  rowsWritten?: number
  durationMs?: number
  skipped?: boolean
  error?: string
  validations?: {
    type: string
    passed: boolean
    message?: string
    failedCount?: number
  }[]
  /** Rows written per destination (counted on each output's final df before write). */
  outputMetrics?: {
    format: string
    path: string
    mode?: string
    rowsWritten?: number
  }[]
  preview?: {
    columns: string[]
    rows: unknown[][]
    truncated: boolean
  }
  logs: RunLogLine[]
}

/** How one stage of a pipeline run ended. */
export type PipelineStageOutcome = Extract<StepStatus, 'success' | 'error' | 'skipped'>

export interface PipelineStageResult {
  /** 0-based position in the sequence the runner was given. */
  index: number
  /** `PipelineStage.id`, so the canvas can find the box this belongs to. */
  id: string
  name?: string
  status: PipelineStageOutcome
  rowsRead?: number
  rowsWritten?: number
  durationMs?: number
  error?: string
  validations?: RunResult['validations']
  outputMetrics?: RunResult['outputMetrics']
}

export interface PipelineRunResult {
  status: RunStatus
  durationMs?: number
  /** Every stage the runner reached, in execution order. */
  stages: PipelineStageResult[]
  /** Preview of the LAST stage only — where the pipeline ends is what is worth showing. */
  preview?: RunResult['preview']
  error?: string
  logs: RunLogLine[]
}

/* ------------------------------------------------------------ templates */

export interface JobTemplate {
  id: string
  name: string
  summary: string
  /** What a newcomer learns by opening it. */
  highlights: string[]
  level: 'starter' | 'intermediate' | 'advanced'
  tags: string[]
  /** Raw pipeline JSON, imported through the compiler on use. */
  pipeline: unknown
}
