/**
 * The lineage index as a graph React Flow can draw.
 *
 * BIPARTITE on purpose: a dataset node, then the Job that reads it, then the
 * dataset that Job writes. Drawing Job → Job directly would be half the boxes,
 * but the arrow would say "B runs after A" — which is not what the JSON says. It
 * says B reads the path A wrote, and the path is the thing an operator wants to
 * see, click and search for.
 *
 * Positions come from dagre, left to right, the same direction and the same
 * library the job canvas uses (`lib/compiler/layout.ts`), so both graphs in the
 * app read the same way.
 */

import { graphlib, layout } from '@dagrejs/dagre'
import type { Edge, Node } from '@xyflow/react'

import type { CatalogAnnotations, DatasetAnnotation } from '@/lib/datacatalog'

import {
  datasetKey,
  datasetNodeId,
  jobNodeId,
  type JobLineage,
  type LineageDataset,
  type LineageEndpoint,
  type LineageIndex,
} from './lineage'

export type DatasetNodeData = {
  dataset: LineageDataset
  /** What the catalog says about it, or null while nobody has said anything. */
  annotation: DatasetAnnotation | null
}
export type JobNodeData = { job: JobLineage }

export type LineageFlowNode = Node<DatasetNodeData, 'dataset'> | Node<JobNodeData, 'job'>

/** Rendered size of each box — dagre needs it, and the components pin it. */
export const DATASET_SIZE = { width: 268, height: 90 }
export const JOB_SIZE = { width: 220, height: 62 }

export interface LineageFlow {
  nodes: LineageFlowNode[]
  edges: Edge[]
}

/** Label for a read edge. Plain inputs stay unlabelled: the arrow already says it. */
function readLabel(endpoint: LineageEndpoint): string | undefined {
  if (endpoint.role === 'join') return 'join'
  if (endpoint.role === 'union') return 'union'
  return undefined
}

/** Label for a write edge — the write mode, or which quality dataset it is. */
function writeLabel(endpoint: LineageEndpoint): string | undefined {
  if (endpoint.role === 'validation:report') return 'report'
  if (endpoint.role === 'validation:valid') return 'valid'
  if (endpoint.role === 'validation:invalid') return 'quarantine'
  return endpoint.mode
}

export function toFlow(
  index: LineageIndex,
  annotations: CatalogAnnotations = {},
): LineageFlow {
  const nodes: LineageFlowNode[] = index.datasets.map((dataset) => ({
    id: datasetNodeId(dataset.key),
    type: 'dataset',
    position: { x: 0, y: 0 },
    data: { dataset, annotation: annotations[dataset.key] ?? null },
    draggable: false,
    connectable: false,
  }))

  for (const job of index.jobs) {
    // A job that names no dataset at all has nothing to sit between.
    if (job.reads.length === 0 && job.writes.length === 0) continue
    nodes.push({
      id: jobNodeId(job.jobId),
      type: 'job',
      position: { x: 0, y: 0 },
      data: { job },
      draggable: false,
      connectable: false,
    })
  }

  const edges: Edge[] = []
  for (const job of index.jobs) {
    const jobId = jobNodeId(job.jobId)
    for (const endpoint of job.reads) {
      const source = datasetNodeId(datasetKey(endpoint.address))
      edges.push({
        id: `${source}->${jobId}:${endpoint.nodeId}`,
        source,
        target: jobId,
        label: readLabel(endpoint),
      })
    }
    for (const endpoint of job.writes) {
      const target = datasetNodeId(datasetKey(endpoint.address))
      edges.push({
        id: `${jobId}->${target}:${endpoint.nodeId}`,
        source: jobId,
        target,
        label: writeLabel(endpoint),
      })
    }
  }

  return { nodes: position(nodes, edges), edges }
}

/** Fresh positions from dagre; disconnected pieces are laid out beside each other. */
function position(nodes: LineageFlowNode[], edges: Edge[]): LineageFlowNode[] {
  const graph = new graphlib.Graph()
  graph.setGraph({ rankdir: 'LR', ranksep: 110, nodesep: 28, marginx: 24, marginy: 24 })
  graph.setDefaultEdgeLabel(() => ({}))

  for (const node of nodes) {
    const size = node.type === 'dataset' ? DATASET_SIZE : JOB_SIZE
    graph.setNode(node.id, { width: size.width, height: size.height })
  }
  for (const edge of edges) graph.setEdge(edge.source, edge.target)

  layout(graph)

  return nodes.map((node) => {
    const placed = graph.node(node.id)
    if (!placed) return node
    const size = node.type === 'dataset' ? DATASET_SIZE : JOB_SIZE
    // dagre positions by center; React Flow positions by top-left corner.
    return {
      ...node,
      position: { x: placed.x - size.width / 2, y: placed.y - size.height / 2 },
    }
  })
}
