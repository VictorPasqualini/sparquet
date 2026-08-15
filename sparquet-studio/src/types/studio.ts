/**
 * Studio domain model — projects, workflows and the visual graph.
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
  | SourceNodeData
  | TransformNodeData
  | ValidationsNodeData
  | SinkNodeData
  | NoteNodeData

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

/* -------------------------------------------------------------- workflows */

/** Types a `{param}` template variable can take, mirroring utils/template.py. */
export type ParamType = 'string' | 'number' | 'boolean' | 'list'

export interface ParamDefinition {
  id: string
  key: string
  type: ParamType
  value: string | number | boolean | string[]
  description?: string
}

export interface WorkflowSettings {
  /** `name` inside the compiled pipeline JSON. */
  pipelineName: string
  description: string
  spark: SparkSettings
}

export interface Workflow {
  id: string
  projectId: string
  name: string
  description: string
  tags: string[]
  settings: WorkflowSettings
  graph: StudioGraph
  params: ParamDefinition[]
  createdAt: number
  updatedAt: number
  /** Bumped by every persisted mutation; used for optimistic-concurrency checks. */
  revision: number
}

export interface Project {
  id: string
  name: string
  description: string
  /** Tailwind-free accent token, resolved in the UI. */
  accent: ProjectAccent
  createdAt: number
  updatedAt: number
}

export const PROJECT_ACCENTS = [
  'amber',
  'sky',
  'violet',
  'emerald',
  'rose',
  'slate',
] as const

export type ProjectAccent = (typeof PROJECT_ACCENTS)[number]

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
  context?: Record<string, unknown>
}

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

/* ------------------------------------------------------------ templates */

export interface WorkflowTemplate {
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
