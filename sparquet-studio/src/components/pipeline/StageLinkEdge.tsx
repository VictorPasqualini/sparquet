/**
 * An execution-order link between two stages: "this one runs, then that one".
 *
 * Same affordance as the pipeline canvas edge (`canvas/edges/PipelineEdge.tsx`):
 * hovering or selecting reveals a button that removes it, so a link can be undone
 * without a modal.
 */

import { BaseEdge, EdgeLabelRenderer, getSmoothStepPath, type EdgeProps } from '@xyflow/react'
import { X } from 'lucide-react'
import { useState } from 'react'

import { IconButton, Tooltip } from '@/components/ui'
import { cn } from '@/lib/utils/cn'
import { usePipelineEditorStore } from '@/store/pipelineEditor'

export function StageLinkEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  markerEnd,
  selected,
}: EdgeProps) {
  const removeLinks = usePipelineEditorStore((state) => state.removeLinks)
  const [hovered, setHovered] = useState(false)

  const [path, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    borderRadius: 12,
  })

  const active = hovered || selected === true

  return (
    <g onMouseEnter={() => setHovered(true)} onMouseLeave={() => setHovered(false)}>
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        className={cn('stroke-line-strong', active && 'stroke-brand-500')}
      />
      <EdgeLabelRenderer>
        <div
          className="absolute"
          style={{ transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)` }}
          onMouseEnter={() => setHovered(true)}
          onMouseLeave={() => setHovered(false)}
        >
          {active ? (
            <Tooltip content="Remove this order link">
              <IconButton
                size="xs"
                variant="secondary"
                label="Remove order link"
                className="nodrag nopan pointer-events-auto rounded-full shadow-card hover:border-state-danger/60 hover:text-state-danger"
                onClick={() => removeLinks([id])}
              >
                <X />
              </IconButton>
            </Tooltip>
          ) : (
            <span className="rounded-full border border-line bg-surface px-1.5 py-px text-2xs text-content-subtle">
              then
            </span>
          )}
        </div>
      </EdgeLabelRenderer>
    </g>
  )
}
