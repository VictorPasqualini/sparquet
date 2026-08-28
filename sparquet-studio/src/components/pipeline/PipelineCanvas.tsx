/**
 * The pipeline editing surface.
 *
 * Same shape as the pipeline canvas (`canvas/JobCanvas.tsx`): the store owns the
 * graph, this component owns pointer affordances — drag & drop from the picker,
 * connecting, deleting — and the canvas chrome. The difference is what a box is:
 * here every box is a whole pipeline file, and every edge is the order two of
 * them run in.
 */

import {
  Background,
  BackgroundVariant,
  ConnectionLineType,
  Controls,
  MarkerType,
  ReactFlow,
  useReactFlow,
  type Connection,
  type DefaultEdgeOptions,
  type Edge,
  type EdgeChange,
  type NodeChange,
} from '@xyflow/react'
import { Link2, Share2 } from 'lucide-react'
import { useCallback, useEffect, useMemo, type DragEvent, type KeyboardEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import { toast } from 'sonner'

import { cancelConnect, startConnect, useConnectSource } from '@/components/canvas/NodeShell'
import { EmptyState, Kbd } from '@/components/ui'
import { wouldCreateCycle, type LinkRejection, type ResolvedPipeline } from '@/lib/pipeline'
import { usePipelineEditorStore } from '@/store/pipelineEditor'
import { useSettingsStore } from '@/store/settings'

import { StageLinkEdge } from './StageLinkEdge'
import { StageNode, STAGE_NODE_WIDTH, type StageRfNode } from './StageNode'

/** Payload key the stage picker writes into `dataTransfer`. */
export const STAGE_DND_MIME = 'application/sparquet-pipeline-stage'

/** React Flow re-mounts every node when these identities change. */
const nodeTypes = { stage: StageNode }
const edgeTypes = { stageLink: StageLinkEdge }

const PRO_OPTIONS = { hideAttribution: false }
const FIT_VIEW_OPTIONS = { padding: 0.2, maxZoom: 1, minZoom: 0.5 }
const DEFAULT_EDGE_OPTIONS: DefaultEdgeOptions = {
  type: 'stageLink',
  markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 },
}
/** Half a box, so a drop lands centred under the pointer. */
const DROP_OFFSET = { x: STAGE_NODE_WIDTH / 2, y: 40 }

/** Why a link was refused, in words the author can act on. */
const REJECTION_MESSAGE: Record<LinkRejection, string> = {
  self: 'A stage cannot run after itself.',
  duplicate: 'These stages are already linked in that order.',
  cycle: 'That link would close a loop, and a loop has no first stage.',
}

export function PipelineCanvas({ resolved }: { resolved: ResolvedPipeline }) {
  const navigate = useNavigate()
  const { screenToFlowPosition } = useReactFlow()

  const canvas = useSettingsStore((state) => state.canvas)
  const selectedStageId = usePipelineEditorStore((state) => state.selectedStageId)
  const addStage = usePipelineEditorStore((state) => state.addStage)
  const moveStage = usePipelineEditorStore((state) => state.moveStage)
  const removeStages = usePipelineEditorStore((state) => state.removeStages)
  const removeLinks = usePipelineEditorStore((state) => state.removeLinks)
  const connect = usePipelineEditorStore((state) => state.connect)
  const select = usePipelineEditorStore((state) => state.select)

  const connectSource = useConnectSource()
  // The pending source lives outside React, so leaving the screen has to clear it.
  useEffect(() => cancelConnect, [])

  /**
   * Drilling into a stage carries the execution the canvas is showing, so the job
   * opens on the same run rather than on its own latest one — the drill-down is
   * only honest if both levels describe the same execution.
   */
  const onOpen = useCallback(
    (jobId: string, stageId: string) => {
      const runView = usePipelineEditorStore.getState().runView
      const jobRunId = runView?.jobRunIds[stageId]
      navigate(
        `/jobs/${jobId}`,
        jobRunId ? { state: { runId: runView?.runId, jobRunId } } : undefined,
      )
    },
    [navigate],
  )

  const issuesByStage = useMemo(() => {
    const map = new Map<string, ResolvedPipeline['issues']>()
    for (const issue of resolved.issues) {
      if (!issue.nodeId) continue
      map.set(issue.nodeId, [...(map.get(issue.nodeId) ?? []), issue])
    }
    return map
  }, [resolved.issues])

  const nodes = useMemo<StageRfNode[]>(
    () =>
      resolved.stages.map((stage) => ({
        id: stage.id,
        type: 'stage' as const,
        position: stage.position,
        selected: stage.id === selectedStageId,
        // React Flow labels a node `Node <id>` by default; spelling the gesture
        // out here is the only place a keyboard user meets it.
        ariaLabel: stage.job
          ? `Stage ${stage.order}: ${stage.name}. Press Enter to open it in the editor.`
          : `Stage ${stage.order}: the job it points at was deleted.`,
        data: { stage, issues: issuesByStage.get(stage.id) ?? [], onOpen },
      })),
    [issuesByStage, onOpen, resolved.stages, selectedStageId],
  )

  const edges = useMemo<Edge[]>(
    () =>
      resolved.links.map((link) => ({
        id: link.id,
        source: link.source,
        target: link.target,
        type: 'stageLink',
        // Screen readers get the relation spelled out; the arrow is only visual.
        ariaLabel: 'runs before',
      })),
    [resolved.links],
  )

  const onNodesChange = useCallback(
    (changes: NodeChange<StageRfNode>[]) => {
      for (const change of changes) {
        if (change.type === 'position' && change.position) {
          moveStage(change.id, change.position, change.dragging === false)
        } else if (change.type === 'remove') {
          removeStages([change.id])
        } else if (change.type === 'select' && change.selected) {
          select(change.id)
        }
      }
    },
    [moveStage, removeStages, select],
  )

  const onEdgesChange = useCallback(
    (changes: EdgeChange<Edge>[]) => {
      const removed = changes.filter((change) => change.type === 'remove').map((c) => c.id)
      if (removed.length > 0) removeLinks(removed)
    },
    [removeLinks],
  )

  const onConnect = useCallback(
    (connection: Connection) => {
      if (!connection.source || !connection.target) return
      const rejection = connect(connection.source, connection.target)
      if (rejection) toast.error(REJECTION_MESSAGE[rejection])
    },
    [connect],
  )

  const isValidConnection = useCallback(
    (connection: Edge | Connection) => {
      const { source, target } = connection
      if (!source || !target || source === target) return false
      return !wouldCreateCycle(resolved.links, source, target)
    },
    [resolved.links],
  )

  /* ------------------------------------------------------------ drag & drop */

  const onDragOver = useCallback((event: DragEvent<HTMLDivElement>) => {
    if (!event.dataTransfer.types.includes(STAGE_DND_MIME)) return
    event.preventDefault()
    // Must stay compatible with the picker's `effectAllowed = 'copy'`, or the
    // browser cancels the drop before `onDrop` ever fires.
    event.dataTransfer.dropEffect = 'copy'
  }, [])

  const onDrop = useCallback(
    (event: DragEvent<HTMLDivElement>) => {
      const jobId = event.dataTransfer.getData(STAGE_DND_MIME)
      if (!jobId) return
      event.preventDefault()
      const point = screenToFlowPosition({ x: event.clientX, y: event.clientY })
      addStage(jobId, { x: point.x - DROP_OFFSET.x, y: point.y - DROP_OFFSET.y })
    },
    [addStage, screenToFlowPosition],
  )

  /* ------------------------------------------------------ keyboard gestures */

  /**
   * Two gestures live on the focused stage. `Enter` opens the job it runs — the
   * keyboard twin of double-clicking the box. `C` is the keyboard equivalent of
   * dragging a handle: press it on the focused stage, tab to the next stage,
   * press it again.
   */
  const onKeyDown = useCallback(
    (event: KeyboardEvent<HTMLDivElement>) => {
      if (event.key === 'Escape' && connectSource !== null) {
        event.preventDefault()
        cancelConnect()
        return
      }

      const focused = event.target as HTMLElement
      const isStage = focused.classList.contains('react-flow__node')

      if (event.key === 'Enter') {
        // Mid-link, Enter would navigate out of a gesture that is still running.
        if (!isStage || connectSource !== null) return
        if (event.metaKey || event.ctrlKey || event.altKey) return
        const stage = resolved.stages.find((candidate) => candidate.id === focused.dataset.id)
        // A broken stage points at a deleted job: nothing to open.
        if (!stage?.job) return
        event.preventDefault()
        onOpen(stage.jobId, stage.id)
        return
      }

      if (event.key.toLowerCase() !== 'c') return
      if (event.metaKey || event.ctrlKey || event.altKey) return

      if (!isStage) return
      const stageId = focused.getAttribute('data-id')
      if (!stageId) return

      event.preventDefault()
      if (connectSource === null) {
        startConnect(stageId)
        return
      }
      if (connectSource === stageId) {
        cancelConnect()
        return
      }
      const rejection = connect(connectSource, stageId)
      if (rejection) toast.error(REJECTION_MESSAGE[rejection])
      cancelConnect()
      select(stageId)
    },
    [connect, connectSource, onOpen, resolved.stages, select],
  )

  return (
    <div
      className="relative h-full w-full"
      onDrop={onDrop}
      onDragOver={onDragOver}
      onKeyDown={onKeyDown}
    >
      <ReactFlow<StageRfNode, Edge>
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        isValidConnection={isValidConnection}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        defaultEdgeOptions={DEFAULT_EDGE_OPTIONS}
        connectionLineType={ConnectionLineType.SmoothStep}
        proOptions={PRO_OPTIONS}
        fitView
        fitViewOptions={FIT_VIEW_OPTIONS}
        onPaneClick={() => select(null)}
        deleteKeyCode={['Backspace', 'Delete']}
        minZoom={0.2}
        maxZoom={1.5}
        className="bg-canvas"
        aria-label="Pipeline: the pipelines of this pipeline and the order they run in"
      >
        {canvas.showGrid && (
          <Background
            variant={BackgroundVariant.Dots}
            gap={20}
            size={1.4}
            color="rgb(var(--grid-dot))"
          />
        )}
        <Controls
          showInteractive={false}
          className="rounded-lg border border-line shadow-card"
        />
      </ReactFlow>

      {resolved.stages.length === 0 && (
        <div className="pointer-events-none absolute inset-0 z-10 flex items-center justify-center p-6">
          <div className="pointer-events-auto w-full max-w-md rounded-2xl border border-line bg-surface/95 shadow-card animate-fade-in">
            <EmptyState
              icon={<Share2 />}
              title="No stages in this pipeline yet"
              description="Click a pipeline in the list on the left to add it — or drag it in. Then link the boxes to say which one runs first."
            />
            <p className="border-t border-line px-6 py-2.5 text-center text-2xs text-content-subtle">
              Focus a stage and press <Kbd>c</Kbd> to link it to another one, or{' '}
              <Kbd>enter</Kbd> to open the job it runs.
            </p>
          </div>
        </div>
      )}

      {connectSource !== null && (
        <div
          role="status"
          className="pointer-events-none absolute inset-x-0 bottom-4 z-20 flex justify-center px-4"
        >
          <p className="flex flex-wrap items-center justify-center gap-1.5 rounded-full border border-brand-500/40 bg-surface-overlay px-3 py-1.5 text-2xs text-content shadow-pop">
            <Link2 className="h-3.5 w-3.5 text-brand-600 dark:text-brand-400" aria-hidden />
            <span>Choose the stage that runs next —</span>
            <Kbd>tab</Kbd>
            to move,
            <Kbd>c</Kbd>
            to link,
            <Kbd>esc</Kbd>
            to cancel
          </p>
        </div>
      )}
    </div>
  )
}
