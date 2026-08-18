import { BaseEdge, EdgeLabelRenderer, getSmoothStepPath, type EdgeProps } from '@xyflow/react'
import { X } from 'lucide-react'
import { useState } from 'react'

import { IconButton, Tooltip } from '@/components/ui'
import { cn } from '@/lib/utils/cn'
import { useEditorStore } from '@/store/editor'
import { useSettingsStore } from '@/store/settings'
import { getValidationSink } from '@/catalog'
import { HANDLE, validationSinkRoleOfHandle } from '@/types/studio'

export function PipelineEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  sourceHandleId,
  targetHandleId,
  markerEnd,
  selected,
}: EdgeProps) {
  const animate = useSettingsStore((state) => state.canvas.animateEdges)
  const onEdgesChange = useEditorStore((state) => state.onEdgesChange)
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
  const isSecondary = targetHandleId === HANDLE.inRight
  // A side output is drawn dashed AND labelled: the data leaving here is a copy,
  // and the main chain past the rule still carries every row.
  const sideRole = validationSinkRoleOfHandle(sourceHandleId)
  const sideDef = sideRole ? getValidationSink(sideRole) : null

  return (
    <g onMouseEnter={() => setHovered(true)} onMouseLeave={() => setHovered(false)}>
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        className={cn(
          // `dashdraw` ships with the React Flow base stylesheet.
          animate && '[animation:dashdraw_0.5s_linear_infinite] [stroke-dasharray:5]',
          sideDef && !animate && '[stroke-dasharray:4_4]',
          active && 'stroke-brand-500',
        )}
      />
      <EdgeLabelRenderer>
        <div
          className="absolute"
          style={{ transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)` }}
          onMouseEnter={() => setHovered(true)}
          onMouseLeave={() => setHovered(false)}
        >
          {active ? (
            <Tooltip content="Remove connection">
              <IconButton
                size="xs"
                variant="secondary"
                label="Remove connection"
                className="nodrag nopan pointer-events-auto rounded-full shadow-card hover:border-state-danger/60 hover:text-state-danger"
                onClick={() => onEdgesChange([{ id, type: 'remove' }])}
              >
                <X />
              </IconButton>
            </Tooltip>
          ) : isSecondary ? (
            <span className="rounded-full border border-line bg-surface px-1.5 py-px text-2xs text-content-subtle">
              right
            </span>
          ) : sideDef ? (
            <span
              className="rounded-full border border-line bg-surface px-1.5 py-px text-2xs text-content-subtle"
              title={`${sideDef.label} — a side output. ${sideDef.caveat}`}
            >
              side output
            </span>
          ) : null}
        </div>
      </EdgeLabelRenderer>
    </g>
  )
}
