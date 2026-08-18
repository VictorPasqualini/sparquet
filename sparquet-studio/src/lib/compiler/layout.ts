/**
 * Automatic canvas layout.
 *
 * Dagre ranks nodes left to right so a pipeline reads like the JSON it compiles
 * to: source on the left, destinations on the right, join sub-chains stacked
 * under the branch they feed.
 */

import { graphlib, layout } from '@dagrejs/dagre'

import type { StudioGraph, StudioNode, ValidationSinkRole } from '@/types/studio'
import { VALIDATION_SINK_ROLES } from '@/types/studio'
import { isValidationNode, validationSinkRoleOf } from '@/lib/compiler/graph'

export interface LayoutOptions {
  direction?: 'LR' | 'TB'
  /** Distance between ranks (columns in `LR`). */
  rankSep?: number
  /** Distance between siblings inside a rank. */
  nodeSep?: number
  nodeWidth?: number
  nodeHeight?: number
  noteWidth?: number
  noteHeight?: number
  marginX?: number
  marginY?: number
}

/** Actual rendered size of the node chrome (see components/canvas/NodeShell). */
export const NODE_RENDER_SIZE = { width: 264, height: 112 }
export const NOTE_RENDER_SIZE = { width: 240, height: 140 }

const DEFAULTS = {
  direction: 'LR',
  rankSep: 120,
  nodeSep: 70,
  nodeWidth: 280,
  nodeHeight: 110,
  noteWidth: 240,
  noteHeight: 140,
  marginX: 0,
  marginY: 0,
} as const satisfies Required<LayoutOptions>

/** Returns a copy of `graph` with fresh positions; the input is never touched. */
export function autoLayout(graph: StudioGraph, options: LayoutOptions = {}): StudioGraph {
  const settings = { ...DEFAULTS, ...options }
  const edges = graph.edges.map((edge) => ({ ...edge }))

  const sizeOf = (node: StudioNode) =>
    node.data.kind === 'note'
      ? { width: settings.noteWidth, height: settings.noteHeight }
      : { width: settings.nodeWidth, height: settings.nodeHeight }

  /**
   * The quality destinations are kept OUT of the ranking. They have no edges at
   * all, so dagre would drop them into the first column, right where the input
   * belongs — exactly the wrong story: they are not a stage the data passes
   * through, they are datasets the validations block writes while the chain carries
   * on to the right. They are placed by hand below, under the rules.
   */
  const side = new Map<string, ValidationSinkRole>()
  for (const node of graph.nodes) {
    const role = validationSinkRoleOf(node)
    if (role) side.set(node.id, role)
  }

  const dag = new graphlib.Graph({ multigraph: false, compound: false })
  dag.setGraph({
    rankdir: settings.direction,
    ranksep: settings.rankSep,
    nodesep: settings.nodeSep,
    marginx: settings.marginX,
    marginy: settings.marginY,
  })
  dag.setDefaultEdgeLabel(() => ({}))

  for (const node of graph.nodes) {
    if (side.has(node.id)) continue
    dag.setNode(node.id, sizeOf(node))
  }

  const known = new Set(graph.nodes.filter((node) => !side.has(node.id)).map((node) => node.id))
  for (const edge of graph.edges) {
    if (edge.source === edge.target) continue
    if (!known.has(edge.source) || !known.has(edge.target)) continue
    dag.setEdge(edge.source, edge.target)
  }

  try {
    layout(dag)
  } catch {
    // A layout failure must never cost the user their graph.
    return { nodes: graph.nodes.map((node) => ({ ...node })), edges }
  }

  const measuredOf = (node: StudioNode) =>
    // Seed `measured`: React Flow keeps a node invisible (and skips its edges)
    // until it has dimensions, which otherwise arrive only after a ResizeObserver
    // frame. Real measurements overwrite these on the first paint.
    node.measured ??
    (node.data.kind === 'note' ? { ...NOTE_RENDER_SIZE } : { ...NODE_RENDER_SIZE })

  const placedAt = new Map<string, { x: number; y: number }>()
  let bottom = Number.NEGATIVE_INFINITY
  for (const node of graph.nodes) {
    if (side.has(node.id)) continue
    const placed = dag.node(node.id)
    if (!placed) continue
    const { width, height } = sizeOf(node)
    const position = { x: placed.x - width / 2, y: placed.y - height / 2 }
    placedAt.set(node.id, position)
    bottom = Math.max(bottom, position.y + height)
  }
  if (!Number.isFinite(bottom)) bottom = 0

  /**
   * One row under the whole diagram, starting at the last rule's column: near the
   * rules that fill them, and clear of the row the data actually flows along.
   * Falls back to the leftmost placed node when the graph has no rule to sit under.
   */
  const ruleColumns = graph.nodes
    .filter((node) => !side.has(node.id) && isValidationNode(node))
    .map((node) => placedAt.get(node.id)?.x)
    .filter((x): x is number => x !== undefined)
  const anchorX =
    ruleColumns.length > 0
      ? Math.max(...ruleColumns)
      : Math.min(0, ...[...placedAt.values()].map((position) => position.x))

  const ordered = [...side].sort(
    (a, b) => VALIDATION_SINK_ROLES.indexOf(a[1]) - VALIDATION_SINK_ROLES.indexOf(b[1]),
  )
  ordered.forEach(([nodeId], column) => {
    placedAt.set(nodeId, {
      x: anchorX + column * (settings.nodeWidth + settings.nodeSep),
      y: bottom + settings.rankSep,
    })
  })

  const nodes = graph.nodes.map((node) => {
    const position = placedAt.get(node.id)
    const measured = measuredOf(node)
    if (!position) return { ...node, measured }
    return { ...node, measured, position }
  })

  return { nodes, edges }
}
