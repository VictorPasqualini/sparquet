/**
 * The lineage as a canvas: dataset, Job, dataset, left to right.
 *
 * Clicking a box TRACES it — everything that feeds it and everything it feeds,
 * however many Jobs away, stays lit while the rest dims. That is the question
 * this view exists to answer ("where did this come from, who breaks if I change
 * it"), and it is the one thing a static picture of the whole library cannot say.
 */

import {
  Background,
  BackgroundVariant,
  Controls,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Edge,
  type NodeProps,
  type NodeTypes,
} from '@xyflow/react'
import { Database, Workflow as JobIcon, Share2 } from 'lucide-react'
import { memo, useCallback, useEffect, useMemo, useState } from 'react'

import { cn } from '@/lib/utils/cn'
import {
  DATASET_SIZE,
  JOB_SIZE,
  toFlow,
  type DatasetNodeData,
  type JobNodeData,
  type LineageFlowNode,
} from '@/lib/lineage/flow'
import { datasetNodeId, jobNodeId, traceFrom, type DatasetPlace, type LineageIndex } from '@/lib/lineage'
import type { CatalogAnnotations } from '@/lib/datacatalog'

/** Same colours the list view uses for the four placements. */
const PLACE_ACCENT: Record<DatasetPlace, string> = {
  external: 'border-l-node-input',
  intermediate: 'border-l-node-combine',
  terminal: 'border-l-node-output',
  isolated: 'border-l-state-warning',
}

const PLACE_LABEL: Record<DatasetPlace, string> = {
  external: 'external',
  intermediate: 'handoff',
  terminal: 'terminal',
  isolated: 'isolated',
}

/** Handles exist so edges have somewhere to land; nothing here is connectable. */
function EdgeAnchors() {
  return (
    <>
      <Handle type="target" position={Position.Left} className="!h-1 !w-1 !border-0 !bg-line" />
      <Handle type="source" position={Position.Right} className="!h-1 !w-1 !border-0 !bg-line" />
    </>
  )
}

const DatasetNode = memo(function DatasetNode({
  data,
}: NodeProps<LineageFlowNode & { type: 'dataset'; data: DatasetNodeData }>) {
  const { dataset, annotation } = data
  const shared = dataset.workflowIds.length > 1
  return (
    <div
      className={cn(
        'flex flex-col justify-center gap-1 rounded-lg border border-l-4 border-line bg-surface',
        'px-3 py-2 shadow-sm',
        PLACE_ACCENT[dataset.place],
      )}
      style={{ width: DATASET_SIZE.width, height: DATASET_SIZE.height }}
      title={annotation?.description || 'Double-click to describe this dataset'}
    >
      <div className="flex items-center gap-1.5">
        <Database className="h-3 w-3 shrink-0 text-content-subtle" />
        <code className="truncate font-mono text-2xs text-content">{dataset.key}</code>
        {shared ? (
          <Share2 className="ml-auto h-3 w-3 shrink-0 text-state-info" aria-label="More than one workflow touches it" />
        ) : null}
      </div>
      <div className="flex items-center gap-1.5 text-2xs text-content-subtle">
        <span>{PLACE_LABEL[dataset.place]}</span>
        <span aria-hidden>·</span>
        <span className="truncate">{dataset.formats.join(', ')}</span>
        {dataset.sessionScoped ? <span className="shrink-0 text-state-warning">view</span> : null}
      </div>
      <div className="flex items-center gap-1 text-2xs">
        {annotation ? (
          <span className="truncate text-content-muted">
            {annotation.owner || annotation.domain || annotation.description}
          </span>
        ) : (
          <span className="text-content-subtle/70">no catalog entry</span>
        )}
        {annotation?.classification ? (
          <span className="ml-auto shrink-0 uppercase tracking-wide text-content-subtle">
            {annotation.classification}
          </span>
        ) : null}
      </div>
      <EdgeAnchors />
    </div>
  )
})

const JobNode = memo(function JobNode({
  data,
}: NodeProps<LineageFlowNode & { type: 'job'; data: JobNodeData }>) {
  const { job } = data
  return (
    <div
      className={cn(
        'flex flex-col justify-center gap-0.5 rounded-md border border-line bg-surface-raised',
        'px-3 py-2 shadow-sm',
      )}
      style={{ width: JOB_SIZE.width, height: JOB_SIZE.height }}
      title="Double-click to open this Job"
    >
      <div className="flex items-center gap-1.5">
        <JobIcon className="h-3 w-3 shrink-0 text-brand-500" />
        <span className="truncate text-xs font-medium text-content">{job.jobName}</span>
      </div>
      <span className="text-2xs text-content-subtle">
        {job.reads.length} in · {job.writes.length} out
      </span>
      <EdgeAnchors />
    </div>
  )
})

const nodeTypes: NodeTypes = { dataset: DatasetNode, job: JobNode }

const PRO_OPTIONS = { hideAttribution: true }
const FIT_VIEW_OPTIONS = { padding: 0.15, maxZoom: 1 }

const DEFAULT_EDGE_OPTIONS = {
  type: 'smoothstep',
  markerEnd: { type: MarkerType.ArrowClosed, width: 14, height: 14 },
  style: { strokeWidth: 1.4 },
  labelBgPadding: [4, 2] as [number, number],
  labelBgBorderRadius: 4,
}

export interface LineageGraphProps {
  index: LineageIndex
  /** The catalog, so a box can show who owns what it names. */
  annotations?: CatalogAnnotations
  /** Search text from the screen; matching boxes get a ring. */
  query?: string
  onOpenJob: (jobId: string) => void
  /** Double-clicking a dataset opens its catalog entry. */
  onOpenDataset?: (key: string) => void
}

function Graph({
  index,
  annotations,
  query = '',
  onOpenJob,
  onOpenDataset,
}: LineageGraphProps) {
  const [selected, setSelected] = useState<string | null>(null)
  const { fitView } = useReactFlow()

  const flow = useMemo(() => toFlow(index, annotations), [index, annotations])

  // A different library is a different picture: frame it again.
  useEffect(() => {
    const id = window.setTimeout(() => void fitView(FIT_VIEW_OPTIONS), 0)
    return () => window.clearTimeout(id)
  }, [flow, fitView])

  // A selection that no longer exists would dim the whole graph forever.
  useEffect(() => {
    if (selected && !flow.nodes.some((node) => node.id === selected)) setSelected(null)
  }, [flow, selected])

  const trace = useMemo(
    () => (selected ? traceFrom(index, selected) : null),
    [index, selected],
  )

  const hits = useMemo(() => {
    const needle = query.trim().toLowerCase()
    if (!needle) return null
    const matched = new Set<string>()
    for (const dataset of index.datasets) {
      if (
        dataset.key.toLowerCase().includes(needle) ||
        dataset.formats.some((format) => format.includes(needle))
      ) {
        matched.add(datasetNodeId(dataset.key))
      }
    }
    for (const job of index.jobs) {
      if (job.jobName.toLowerCase().includes(needle)) matched.add(jobNodeId(job.jobId))
    }
    return matched
  }, [index, query])

  const nodes: LineageFlowNode[] = useMemo(
    () =>
      flow.nodes.map((node) => {
        const dimmed = trace !== null && !trace.all.has(node.id)
        const isSeed = selected === node.id
        return {
          ...node,
          className: cn(
            'transition-opacity',
            dimmed && 'opacity-20',
            isSeed && '[&>div]:ring-2 [&>div]:ring-brand-500',
            !isSeed && hits?.has(node.id) && '[&>div]:ring-2 [&>div]:ring-gold',
          ),
        }
      }),
    [flow.nodes, trace, selected, hits],
  )

  const edges: Edge[] = useMemo(
    () =>
      flow.edges.map((edge) => {
        const inTrace =
          trace !== null && trace.all.has(edge.source) && trace.all.has(edge.target)
        return {
          ...edge,
          animated: inTrace,
          className: cn('transition-opacity', trace !== null && !inTrace && 'opacity-10'),
        }
      }),
    [flow.edges, trace],
  )

  const handleNodeClick = useCallback(
    (_: unknown, node: { id: string }) => {
      setSelected((current) => (current === node.id ? null : node.id))
    },
    [],
  )

  const handleNodeDoubleClick = useCallback(
    (_: unknown, node: LineageFlowNode) => {
      if (node.type === 'job') {
        onOpenJob((node.data as JobNodeData).job.jobId)
        return
      }
      onOpenDataset?.((node.data as DatasetNodeData).dataset.key)
    },
    [onOpenJob, onOpenDataset],
  )

  return (
    <ReactFlow<LineageFlowNode>
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      proOptions={PRO_OPTIONS}
      fitView
      fitViewOptions={FIT_VIEW_OPTIONS}
      defaultEdgeOptions={DEFAULT_EDGE_OPTIONS}
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable
      onNodeClick={handleNodeClick}
      onNodeDoubleClick={handleNodeDoubleClick}
      onPaneClick={() => setSelected(null)}
      minZoom={0.15}
      className="bg-canvas"
    >
      <Background variant={BackgroundVariant.Dots} gap={18} size={1} className="opacity-60" />
      <Controls showInteractive={false} position="bottom-right" />
    </ReactFlow>
  )
}

export function LineageGraph(props: LineageGraphProps) {
  return (
    <ReactFlowProvider>
      <Graph {...props} />
    </ReactFlowProvider>
  )
}
