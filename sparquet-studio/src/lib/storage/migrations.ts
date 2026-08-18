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

import { makeEdge } from '@/lib/compiler/graph'
import type { OnFailureMode, OutputSpec } from '@/types/pipeline'
import { ON_FAILURE_MODES } from '@/types/pipeline'
import type {
  Job,
  JobSettings,
  StudioEdge,
  StudioGraph,
  StudioNode,
  ValidationNode,
  ValidationPolicy,
} from '@/types/studio'
import { DEFAULT_VALIDATION_POLICY, HANDLE } from '@/types/studio'

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

function legacyPolicy(data: LegacyValidationsData): ValidationPolicy {
  const raw = typeof data.onFailure === 'string' ? data.onFailure : ''
  const onFailure: OnFailureMode = ON_FAILURE_MODES.includes(raw as OnFailureMode)
    ? (raw as OnFailureMode)
    : DEFAULT_VALIDATION_POLICY.onFailure

  const policy: ValidationPolicy = { onFailure }
  if (isRecord(data.report)) policy.report = data.report as unknown as OutputSpec
  if (isRecord(data.outputs)) {
    policy.outputs = data.outputs as unknown as Record<string, OutputSpec>
  }
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

/** Brings one stored job up to the current shape. Returns it as-is when already current. */
export function upgradeJob(job: Job): Job {
  const upgraded = upgradeValidations(job.graph, job.settings)
  if (!upgraded.changed) return job
  return { ...job, graph: upgraded.graph, settings: upgraded.settings }
}
