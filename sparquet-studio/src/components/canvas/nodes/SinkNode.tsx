import type { NodeProps } from '@xyflow/react'
import { memo } from 'react'

import { getFormat } from '@/catalog'
import { Badge, type BadgeTone } from '@/components/ui'
import type { SinkNode as SinkNodeType } from '@/types/studio'

import { catalogIcon } from '../icons'
import { NodeShell, truncateMiddle, useNodeIssues } from '../NodeShell'

/** Overwrite replaces data and merge rewrites rows — both deserve a louder chip. */
const MODE_TONE: Record<string, BadgeTone> = {
  overwrite: 'warning',
  merge: 'brand',
  append: 'neutral',
  ignore: 'neutral',
  error: 'neutral',
}

/**
 * An empty projection list is not a projection: the compiler leaves `columns` out
 * and the writer gets the whole DataFrame, `ingestion_ts` included.
 */
export function projectionLabel(columns: string[] | null): string | null {
  if (columns === null) return null
  if (columns.length === 0) return 'Writes every column'
  return `Projects ${columns.length} column${columns.length === 1 ? '' : 's'}`
}

export const SinkNode = memo(function SinkNodeRenderer({
  id,
  data,
  selected,
}: NodeProps<SinkNodeType>) {
  const format = getFormat(data.format)
  const issues = useNodeIssues(id)
  const projection = projectionLabel(data.columns)

  return (
    <NodeShell
      nodeId={id}
      accent="output"
      icon={catalogIcon(format?.icon ?? 'Database')}
      title={data.label ?? 'Output'}
      selected={selected}
      issues={issues}
      inputs="single"
      hasOutput={false}
      badges={
        <>
          <Badge tone="info">{format?.label ?? data.format}</Badge>
          <Badge tone={MODE_TONE[data.mode] ?? 'neutral'}>{data.mode}</Badge>
        </>
      }
    >
      {data.path ? (
        <p className="truncate font-mono text-2xs text-content-muted" title={data.path}>
          {truncateMiddle(data.path, 36)}
        </p>
      ) : (
        <p className="text-2xs text-content-subtle">
          No {(format?.pathLabel ?? 'path').toLowerCase()} yet
        </p>
      )}
      {projection && <p className="text-2xs text-content-subtle">{projection}</p>}
    </NodeShell>
  )
})
