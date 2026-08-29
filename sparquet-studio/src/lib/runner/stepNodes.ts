/**
 * Which box on the canvas a run step belongs to.
 *
 * The runner never names a node: it reports a step by the position it occupies in
 * the compiled JSON, or — for the quality datasets — by the ROLE it fills. Turning
 * that back into a node id is what lets the canvas paint per-box status, both from
 * the live stream (`RunPanel`) and from a run read back out of the runner's
 * database (`showRunView` in `store/editor.ts`).
 *
 * The lanes are passed in rather than derived here: the walks that produce them
 * (`runtimeEndpointNodeIds`, `mainChainTransformNodeIds`, …) live in
 * `store/editor.ts` next to the graph they read, and importing them here would
 * make the store and this module depend on each other.
 */

import { getFormat, getTransformation, getValidator } from '@/catalog'
import type { ExecutionStatus, JobRunRecord, StepRunRecord } from '@/types/history'
import { isValidationSinkRole, type StepStatus, type ValidationSinkRole } from '@/types/studio'

/** The node ids a run can touch, each lane in the order the compiler emits it. */
export interface StepNodeLanes {
  /** Feeds `input`. */
  sourceId: string | null
  /** One entry per compiled `transformations` step, in index order. */
  transformIds: string[]
  /**
   * One entry per RULE the framework runs — a node with several `targets`
   * repeats, because the runner counts targets, not boxes.
   */
  validationIds: string[]
  /** Index-aligned with the compiled `outputs` array. */
  sinkIds: string[]
  /** The quality datasets, keyed by role: they sit in no lane. */
  dqSinkIds: Partial<Record<ValidationSinkRole, string>>
}

/** A step marker, however it arrived: live SSE event or persisted row. */
export interface StepAddress {
  scope: string
  /** Absent on a role-keyed step. */
  index?: number
  /** Absent on an index-keyed step. */
  role?: string | null
}

/**
 * The live stream defaults a scope-less marker to `transformation`; an older
 * runner persisted the same lane as `transform`. One name from here on.
 */
export function normalizeStepScope(scope: string): string {
  return scope === 'transform' ? 'transformation' : scope
}

/**
 * Which box a marker belongs to. Each index-keyed scope counts in its own lane,
 * so the same index means a different node in each of them; the quality datasets
 * are keyed by ROLE instead, because they sit in no lane — they are standalone
 * declarations with no incoming link, so there is no position for an index to
 * refer to.
 */
export function nodeIdForStep(lanes: StepNodeLanes, step: StepAddress): string | undefined {
  const scope = normalizeStepScope(step.scope)
  if (scope === 'validation_sink') {
    return isValidationSinkRole(step.role) ? lanes.dqSinkIds[step.role] : undefined
  }
  if (step.index === undefined) return undefined
  if (scope === 'input') return lanes.sourceId ?? undefined
  if (scope === 'output') return lanes.sinkIds[step.index]
  if (scope === 'validation') return lanes.validationIds[step.index]
  return lanes.transformIds[step.index]
}

/** Every node the lanes cover, seeded to `pending` — the shape a run starts from. */
export function pendingStatuses(lanes: StepNodeLanes): Record<string, StepStatus> {
  const ids = [
    ...(lanes.sourceId ? [lanes.sourceId] : []),
    ...lanes.transformIds,
    ...lanes.validationIds,
    ...Object.values(lanes.dqSinkIds),
    ...lanes.sinkIds,
  ]
  return Object.fromEntries(ids.map((id) => [id, 'pending' as const]))
}

/** `failed` is the server's word for what the canvas calls `error`. */
export function toStepStatus(status: ExecutionStatus): StepStatus {
  return status === 'failed' ? 'error' : status
}

/**
 * Worst-first, so a node that carries several steps (a validation rule with
 * several `targets`) reports the one that matters: a failure is never hidden
 * behind a sibling that succeeded.
 */
const SEVERITY: Record<StepStatus, number> = {
  error: 5,
  // Above `running`: the run is over, so a box still holding `running` from the
  // step the stop interrupted would keep spinning forever.
  cancelled: 4,
  running: 3,
  success: 2,
  skipped: 1,
  pending: 0,
}

export interface RunViewStatuses {
  /** Per node id, the status the canvas paints. */
  status: Record<string, StepStatus>
  /** Per node id, the summed wall-clock of its steps. */
  duration: Record<string, number>
  /**
   * Per node id, the steps behind that status, in execution order — what the
   * Inspector shows when a box is opened from a past run.
   */
  steps: Record<string, StepRunRecord[]>
  /** Steps whose box is gone: the graph was edited after the run. */
  unmatched: StepRunRecord[]
}

/**
 * A past job execution projected onto the canvas.
 *
 * Nodes the run never reached keep `pending`, which the canvas draws as dimmed —
 * on a finished run that reads as "this box did not run", the same way it reads
 * as "not yet" while a run is in flight.
 *
 * A step whose node is gone (the graph was edited since) lands in `unmatched`
 * rather than being dropped, so the panel can say so instead of silently showing
 * a run with holes in it.
 */
export function runViewStatuses(lanes: StepNodeLanes, job: JobRunRecord): RunViewStatuses {
  const status = pendingStatuses(lanes)
  const duration: Record<string, number> = {}
  const steps: Record<string, StepRunRecord[]> = {}
  const unmatched: StepRunRecord[] = []

  for (const step of job.steps) {
    const nodeId = nodeIdForStep(lanes, {
      scope: step.scope,
      index: step.stepIndex,
      role: step.role,
    })
    if (nodeId === undefined) {
      unmatched.push(step)
      continue
    }
    const next = toStepStatus(step.status)
    const current = status[nodeId]
    if (current === undefined || SEVERITY[next] > SEVERITY[current]) status[nodeId] = next
    if (step.durationMs !== null) duration[nodeId] = (duration[nodeId] ?? 0) + step.durationMs
    steps[nodeId] = [...(steps[nodeId] ?? []), step]
  }

  return { status, duration, steps, unmatched }
}

/** The `details` JSON of a step, as an object. Empty when absent or malformed. */
export function stepDetails(step: StepRunRecord): Record<string, unknown> {
  if (!step.details) return {}
  try {
    const parsed: unknown = JSON.parse(step.details)
    return typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : {}
  } catch {
    return {}
  }
}

/**
 * The name a person would call the step: `Filter rows`, `Parquet`, `Not null`.
 *
 * The runner records the framework's own identifiers (`filter`, `parquet`,
 * `not_null`) because that is what the JSON says, and the catalog is what turns
 * them back into the words the palette used. An identifier the catalog does not
 * know falls through unchanged rather than being hidden — a step nobody can name
 * still has to be readable in a list of what ran.
 */
export function stepName(step: StepRunRecord): string {
  const scope = normalizeStepScope(step.scope)
  if (scope === 'validation_sink') {
    return step.role ? `${step.role[0]?.toUpperCase()}${step.role.slice(1)} dataset` : 'Dataset'
  }
  if (scope === 'transformation') return getTransformation(step.type)?.label ?? step.type
  if (scope === 'validation') return getValidator(step.type)?.label ?? step.type
  if (scope === 'input' || scope === 'output') return getFormat(step.type)?.label ?? step.type
  return step.role ?? step.type ?? scope
}

/** How a step reads in a list: `validation · not_null`, `validation_sink · report`. */
export function stepLabel(step: StepRunRecord): string {
  const scope = normalizeStepScope(step.scope)
  const detail = step.role ?? step.type
  return detail ? `${scope} · ${detail}` : scope
}
