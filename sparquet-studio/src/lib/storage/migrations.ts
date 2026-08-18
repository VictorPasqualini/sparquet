/**
 * Record upgrades applied to stored jobs.
 *
 * `db.migrate()` runs these once per storage version bump and rewrites the
 * records, but the same functions also run when a job reaches the editor, so a
 * record that arrives from anywhere else (an older exported bundle merged in at
 * runtime, a hand-edited backup) is still editable.
 *
 * Everything here is PURE and idempotent: it returns the input untouched when
 * there is nothing to upgrade, which is what lets callers skip the write.
 */

import { isLastValidationOfRun, isValidationNode, makeEdge } from '@/lib/compiler/graph'
import type { OnFailureMode } from '@/types/pipeline'
import { ON_FAILURE_MODES } from '@/types/pipeline'
import type {
  Job,
  JobSettings,
  SinkNode,
  StudioEdge,
  StudioGraph,
  StudioNode,
  ValidationNode,
  ValidationPolicy,
  ValidationSinkRole,
} from '@/types/studio'
import {
  DEFAULT_VALIDATION_POLICY,
  HANDLE,
  VALIDATION_SINK_HANDLES,
  VALIDATION_SINK_ROLES,
} from '@/types/studio'

/**
 * The pre-split shape: ONE node carrying every rule plus the block-level policy.
 * Kept as a local type — the domain model no longer describes it.
 */
interface LegacyValidationsData {
  kind: 'validations'
  label?: string
  comment?: string
  rules?: unknown
  onFailure?: unknown
  report?: unknown
  outputs?: unknown
}

type JsonRecord = Record<string, unknown>

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function kindOf(node: StudioNode): string {
  return (node.data as { kind: string }).kind
}

function isLegacyValidationsNode(node: StudioNode): boolean {
  return kindOf(node) === 'validations'
}

function legacyDataOf(node: StudioNode): LegacyValidationsData {
  return node.data as unknown as LegacyValidationsData
}

/** Rules that carry a usable `type`; anything else never compiled anyway. */
function legacyRules(data: LegacyValidationsData): JsonRecord[] {
  if (!Array.isArray(data.rules)) return []
  return data.rules.filter(
    (rule): rule is JsonRecord =>
      isRecord(rule) && typeof rule.type === 'string' && rule.type.trim() !== '',
  )
}

/**
 * The policy shape of storage v3, when the three written datasets were still
 * settings rather than nodes. `upgradeValidations` may still produce it (it reads
 * even older records); `upgradeValidationSinks` is what clears it.
 */
type LegacyValidationPolicy = ValidationPolicy & { report?: unknown; outputs?: unknown }

function legacyPolicy(data: LegacyValidationsData): LegacyValidationPolicy {
  const raw = typeof data.onFailure === 'string' ? data.onFailure : ''
  const onFailure: OnFailureMode = ON_FAILURE_MODES.includes(raw as OnFailureMode)
    ? (raw as OnFailureMode)
    : DEFAULT_VALIDATION_POLICY.onFailure

  const policy: LegacyValidationPolicy = { onFailure }
  if (isRecord(data.report)) policy.report = data.report
  if (isRecord(data.outputs)) policy.outputs = data.outputs
  return policy
}

/** Deterministic ids keep the upgrade stable if it ever runs twice on a copy. */
function ruleNodeOf(node: StudioNode, rule: JsonRecord, index: number): ValidationNode {
  const params: JsonRecord = {}
  for (const [key, value] of Object.entries(rule)) {
    if (key === 'type') continue
    params[key] = value
  }
  return {
    id: `${node.id}-r${index + 1}`,
    type: 'validation',
    position: { x: node.position.x + index * 300, y: node.position.y },
    data: { kind: 'validation', validator: String(rule.type), params },
  }
}

const handleOf = (edge: StudioEdge): string => edge.targetHandle ?? HANDLE.in

/**
 * Splits every legacy validations node into one node per rule, chained in place,
 * and lifts the block-level policy into the job settings.
 *
 * A node with no rules is dropped and its chain closed back up, so the graph never
 * loses the connection between what came before it and what came after.
 */
export function upgradeValidations(
  graph: StudioGraph,
  settings: JobSettings,
): { graph: StudioGraph; settings: JobSettings; changed: boolean } {
  const legacy = graph.nodes.filter(isLegacyValidationsNode)
  if (legacy.length === 0) return { graph, settings, changed: false }

  let nodes = [...graph.nodes]
  let edges = [...graph.edges]
  // The first block that carries a policy wins: a graph with several of them never
  // compiled, and its rules are merged into one run here anyway.
  let policy: ValidationPolicy | undefined = settings.validations

  for (const node of legacy) {
    const data = legacyDataOf(node)
    policy ??= legacyPolicy(data)

    const rules = legacyRules(data).map((rule, index) => ruleNodeOf(node, rule, index))
    const position = nodes.findIndex((candidate) => candidate.id === node.id)

    if (rules.length === 0) {
      const parent = edges.find((edge) => edge.target === node.id)?.source
      const children = edges.filter((edge) => edge.source === node.id)
      edges = edges.filter((edge) => edge.source !== node.id && edge.target !== node.id)
      if (parent !== undefined) {
        for (const child of children) {
          const bridged = makeEdge(parent, child.target, handleOf(child))
          if (edges.some((edge) => edge.id === bridged.id)) continue
          edges.push(bridged)
        }
      }
      nodes = nodes.filter((candidate) => candidate.id !== node.id)
      continue
    }

    const first = rules[0]
    const last = rules[rules.length - 1]
    edges = edges.map((edge) => {
      if (edge.target === node.id) return { ...edge, target: first.id }
      if (edge.source === node.id) return { ...edge, source: last.id }
      return edge
    })
    for (let index = 1; index < rules.length; index += 1) {
      edges.push(makeEdge(rules[index - 1].id, rules[index].id))
    }
    // The label of the old block is meaningless on a single rule, so it is dropped;
    // its note is kept on the first rule, where the author will look for it.
    if (data.comment) first.data.comment = data.comment
    nodes.splice(position, 1, ...rules)
  }

  return {
    graph: { nodes, edges },
    settings: policy ? { ...settings, validations: policy } : settings,
    changed: true,
  }
}

/* ------------------------------------------- v4: policy sinks become nodes */

/** `partition_by` / `columns` survive only when they are lists of strings. */
function stringList(value: unknown): string[] | null {
  if (!Array.isArray(value)) return null
  return value.filter((entry): entry is string => typeof entry === 'string')
}

function sinkNodeOf(
  id: string,
  spec: JsonRecord,
  position: { x: number; y: number },
): SinkNode {
  return {
    id,
    type: 'sink',
    position,
    data: {
      kind: 'sink',
      format: typeof spec.format === 'string' ? spec.format : '',
      path: typeof spec.path === 'string' ? spec.path : '',
      mode: typeof spec.mode === 'string' ? spec.mode : '',
      partitionBy: stringList(spec.partition_by) ?? [],
      columns: stringList(spec.columns),
      options: isRecord(spec.options) ? { ...spec.options } : {},
    },
  }
}

/** The stored policy's three datasets, in the order they are written and drawn. */
function legacySinkSpecs(policy: LegacyValidationPolicy): [ValidationSinkRole, JsonRecord][] {
  const specs: [ValidationSinkRole, JsonRecord][] = []
  if (isRecord(policy.report)) specs.push(['report', policy.report])
  if (isRecord(policy.outputs)) {
    for (const role of VALIDATION_SINK_ROLES) {
      if (role === 'report') continue
      const entry = policy.outputs[role]
      if (isRecord(entry)) specs.push([role, entry])
    }
  }
  return specs
}

/**
 * Moves `validations.report` and `validations.outputs` out of the job settings and
 * onto the canvas, as destination nodes hanging off the LAST rule of the run — the
 * point where "every rule has run" is true, which is when the framework writes them.
 *
 * `on_failure` stays behind: it is run policy, not a dataset.
 *
 * A job that configured them without ever having a rule compiled no `validations`
 * block, so nothing was written for it either — those specs are dropped with a
 * console warning rather than left dangling off nothing.
 */
export function upgradeValidationSinks(
  graph: StudioGraph,
  settings: JobSettings,
): { graph: StudioGraph; settings: JobSettings; changed: boolean } {
  const policy = settings.validations as LegacyValidationPolicy | undefined
  if (!policy) return { graph, settings, changed: false }

  const specs = legacySinkSpecs(policy)
  const stripped: ValidationPolicy = { onFailure: policy.onFailure }
  if (specs.length === 0) {
    // Still rewrite when the legacy keys were present but empty, so the record
    // stops carrying shapes the model no longer describes.
    const hadKeys = 'report' in policy || 'outputs' in policy
    return hadKeys
      ? { graph, settings: { ...settings, validations: stripped }, changed: true }
      : { graph, settings, changed: false }
  }

  const rules = graph.nodes.filter(isValidationNode)
  const anchor = rules.find((node) => isLastValidationOfRun(graph, node.id)) ?? null

  if (!anchor) {
    console.warn(
      `Sparquet Studio: dropped ${specs.length} validation destination(s) from a job with no validation rule — ` +
        'without a rule no `validations` block was ever compiled, so nothing was written to them.',
    )
    return { graph, settings: { ...settings, validations: stripped }, changed: true }
  }

  const nodes = [...graph.nodes]
  const edges = [...graph.edges]
  specs.forEach(([role, spec], index) => {
    // Deterministic id and position: running the upgrade twice on a copy of the
    // same record has to produce the same graph.
    const id = `${anchor.id}-dq-${role}`
    if (nodes.some((node) => node.id === id)) return
    nodes.push(
      sinkNodeOf(id, spec, {
        x: anchor.position.x + index * 300,
        y: anchor.position.y + 220,
      }),
    )
    edges.push(makeEdge(anchor.id, id, HANDLE.in, VALIDATION_SINK_HANDLES[role]))
  })

  return {
    graph: { nodes, edges },
    settings: { ...settings, validations: stripped },
    changed: true,
  }
}

/** Brings one stored job up to the current shape. Returns it as-is when already current. */
export function upgradeJob(job: Job): Job {
  const split = upgradeValidations(job.graph, job.settings)
  const sinks = upgradeValidationSinks(split.graph, split.settings)
  if (!split.changed && !sinks.changed) return job
  return { ...job, graph: sinks.graph, settings: sinks.settings }
}
