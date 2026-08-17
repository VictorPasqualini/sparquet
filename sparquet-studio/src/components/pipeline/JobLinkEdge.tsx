/**
 * A link between two files. The two kinds are told apart by stroke pattern AND
 * by a label naming the location, never by colour alone.
 */

import {
  BaseEdge,
  EdgeLabelRenderer,
  getSmoothStepPath,
  type Edge,
  type EdgeProps,
} from '@xyflow/react'
import { Eye, HardDrive } from 'lucide-react'

import type { JobLinkVia } from '@/lib/pipeline'
import { cn } from '@/lib/utils/cn'
import { truncateMiddle } from '@/lib/utils/format'

export type JobLinkEdgeData = {
  via: JobLinkVia
  locations: string[]
}

export type JobLinkRfEdge = Edge<JobLinkEdgeData, 'fileLink'>

export function JobLinkEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  markerEnd,
  data,
}: EdgeProps<JobLinkRfEdge>) {
  const [path, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    borderRadius: 12,
  })

  const via = data?.via ?? 'storage'
  const locations = data?.locations ?? []
  const extra = locations.length - 1
  const title = locations.join('\n')

  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        className={cn(via === 'view' ? 'stroke-node-combine' : 'stroke-node-output')}
        style={via === 'view' ? { strokeDasharray: '6 4' } : undefined}
      />
      <EdgeLabelRenderer>
        <div
          className="absolute flex max-w-[13rem] items-center gap-1 rounded-full border border-line bg-surface px-2 py-0.5 text-2xs text-content-muted shadow-card"
          style={{ transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)` }}
          title={title}
        >
          {via === 'view' ? (
            <Eye className="h-3 w-3 shrink-0 text-node-combine" aria-hidden />
          ) : (
            <HardDrive className="h-3 w-3 shrink-0 text-node-output" aria-hidden />
          )}
          <span className="sr-only">
            {via === 'view' ? 'Shared temp view' : 'Written path'}:{' '}
          </span>
          <span className="truncate font-mono">
            {truncateMiddle(locations[0] ?? (via === 'view' ? 'temp view' : 'path'), 24)}
          </span>
          {extra > 0 && <span className="shrink-0">+{extra}</span>}
        </div>
      </EdgeLabelRenderer>
    </>
  )
}
