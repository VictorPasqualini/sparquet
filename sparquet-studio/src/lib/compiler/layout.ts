/**
 * Automatic canvas layout.
 *
 * Dagre ranks nodes left to right so a pipeline reads like the JSON it compiles
 * to: source on the left, destinations on the right, join sub-chains stacked
 * under the branch they feed.
 */

import { graphlib, layout } from '@dagrejs/dagre'

import type { StudioGraph, StudioNode } from '@/types/studio'

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

  const dag = new graphlib.Graph({ multigraph: false, compound: false })
  dag.setGraph({
    rankdir: settings.direction,
    ranksep: settings.rankSep,
    nodesep: settings.nodeSep,
    marginx: settings.marginX,
    marginy: settings.marginY,
  })
  dag.setDefaultEdgeLabel(() => ({}))

  for (const node of graph.nodes) dag.setNode(node.id, sizeOf(node))

  const known = new Set(graph.nodes.map((node) => node.id))
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

  const nodes = graph.nodes.map((node) => {
    const placed = dag.node(node.id)
    const { width, height } = sizeOf(node)
    // Seed `measured`: React Flow keeps a node invisible (and skips its edges)
    // until it has dimensions, which otherwise arrive only after a ResizeObserver
    // frame. Real measurements overwrite these on the first paint.
    const measured =
      node.measured ??
      (node.data.kind === 'note' ? { ...NOTE_RENDER_SIZE } : { ...NODE_RENDER_SIZE })
    if (!placed) return { ...node, measured }
    return {
      ...node,
      measured,
      position: { x: placed.x - width / 2, y: placed.y - height / 2 },
    }
  })

  return { nodes, edges }
}
