/**
 * The inferred pipeline surface: one box per pipeline file, one link per place where
 * a file writes what another reads.
 *
 * Read-only on purpose — Sparquet has no orchestrator, so nothing here can be
 * connected, moved or run. Every file is still executed on its own from its
 * editor, which is what "Open" leads to.
 */

import {
  Background,
  BackgroundVariant,
  Controls,
  MarkerType,
  Panel,
  ReactFlow,
  type DefaultEdgeOptions,
} from '@xyflow/react'
import { Eye, HardDrive } from 'lucide-react'
import { useCallback, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import type { InferredPipeline } from '@/lib/pipeline'

import { JobLinkEdge, type JobLinkRfEdge } from './JobLinkEdge'
import { JOB_NODE_WIDTH, JobNode, type JobRfNode } from './JobNode'

/** React Flow re-mounts every node when these identities change. */
const nodeTypes = { file: JobNode }
const edgeTypes = { fileLink: JobLinkEdge }

const PRO_OPTIONS = { hideAttribution: false }
/**
 * `minZoom` keeps the boxes readable: fitting a long chain into the viewport
 * would otherwise shrink the paths to an unreadable map. The user pans from there.
 */
const FIT_VIEW_OPTIONS = { padding: 0.15, maxZoom: 1, minZoom: 0.55 }
const DEFAULT_EDGE_OPTIONS: DefaultEdgeOptions = {
  type: 'fileLink',
  markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 },
}

/** Column pitch leaves room for the link label; row pitch, for an expanded box. */
const COLUMN_GAP = JOB_NODE_WIDTH + 240
const ROW_GAP = 400

export function InferredPipelineCanvas({ pipeline }: { pipeline: InferredPipeline }) {
  const navigate = useNavigate()
  const [expanded, setExpanded] = useState<ReadonlySet<string>>(() => new Set())

  const positions = useMemo(() => layoutFiles(pipeline), [pipeline])

  const onToggle = useCallback((jobId: string) => {
    setExpanded((current) => {
      const next = new Set(current)
      if (!next.delete(jobId)) next.add(jobId)
      return next
    })
  }, [])

  const onOpen = useCallback(
    (jobId: string) => navigate(`/jobs/${jobId}`),
    [navigate],
  )

  const nodes = useMemo<JobRfNode[]>(
    () =>
      pipeline.nodes.map((file) => ({
        id: file.jobId,
        type: 'file',
        position: positions.get(file.jobId) ?? { x: 0, y: 0 },
        data: { file, expanded: expanded.has(file.jobId), onToggle, onOpen },
      })),
    [expanded, pipeline.nodes, onOpen, onToggle, positions],
  )

  const edges = useMemo<JobLinkRfEdge[]>(
    () =>
      pipeline.edges.map((edge) => ({
        id: edge.id,
        source: edge.source,
        target: edge.target,
        type: 'fileLink',
        data: { via: edge.via, locations: edge.locations },
      })),
    [pipeline.edges],
  )

  return (
    <ReactFlow<JobRfNode, JobLinkRfEdge>
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      edgeTypes={edgeTypes}
      defaultEdgeOptions={DEFAULT_EDGE_OPTIONS}
      proOptions={PRO_OPTIONS}
      fitView
      fitViewOptions={FIT_VIEW_OPTIONS}
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable={false}
      minZoom={0.2}
      maxZoom={1.5}
      className="bg-canvas"
      aria-label="Inferred pipeline: how the pipeline files chain into each other"
    >
      <Background
        variant={BackgroundVariant.Dots}
        gap={20}
        size={1.4}
        color="rgb(var(--grid-dot))"
      />
      <Controls showInteractive={false} className="rounded-lg border border-line shadow-card" />
      <Panel position="top-left">
        <Legend />
      </Panel>
    </ReactFlow>
  )
}

/* ------------------------------------------------------------------ legend */

function Legend() {
  return (
    <ul className="flex flex-wrap items-center gap-3 rounded-lg border border-line bg-surface/95 px-2.5 py-1.5 text-2xs text-content-muted shadow-card">
      <li className="flex items-center gap-1.5">
        <HardDrive className="h-3 w-3 text-node-output" aria-hidden />
        <svg width="22" height="6" viewBox="0 0 22 6" aria-hidden className="text-node-output">
          <line x1="0" y1="3" x2="22" y2="3" stroke="currentColor" strokeWidth="1.75" />
        </svg>
        Written path
      </li>
      <li className="flex items-center gap-1.5">
        <Eye className="h-3 w-3 text-node-combine" aria-hidden />
        <svg width="22" height="6" viewBox="0 0 22 6" aria-hidden className="text-node-combine">
          <line
            x1="0"
            y1="3"
            x2="22"
            y2="3"
            stroke="currentColor"
            strokeWidth="1.75"
            strokeDasharray="6 4"
          />
        </svg>
        Shared temp view
      </li>
    </ul>
  )
}

/* ------------------------------------------------------------------ layout */

/**
 * Layered left to right: a file sits one column right of the last file feeding
 * it, so the map reads in the same direction as a pipeline canvas. Rows follow
 * the derived order, which keeps positions stable between renders.
 */
function layoutFiles(pipeline: InferredPipeline): Map<string, { x: number; y: number }> {
  const depth = new Map(pipeline.nodes.map((node) => [node.jobId, 0]))
  const incoming = new Map<string, string[]>()
  for (const edge of pipeline.edges) {
    if (edge.source === edge.target) continue
    incoming.set(edge.target, [...(incoming.get(edge.target) ?? []), edge.source])
  }

  // `pipeline.nodes` is already topological, so one pass settles every depth —
  // except inside a cycle, where the fallback of 0 keeps the layout finite.
  for (const node of pipeline.nodes) {
    const parents = incoming.get(node.jobId) ?? []
    const deepest = parents.reduce(
      (max, parent) => Math.max(max, (depth.get(parent) ?? 0) + 1),
      0,
    )
    depth.set(node.jobId, deepest)
  }

  const rows = new Map<number, number>()
  const positions = new Map<string, { x: number; y: number }>()
  for (const node of pipeline.nodes) {
    const column = depth.get(node.jobId) ?? 0
    const row = rows.get(column) ?? 0
    rows.set(column, row + 1)
    positions.set(node.jobId, { x: column * COLUMN_GAP, y: row * ROW_GAP })
  }
  return positions
}
