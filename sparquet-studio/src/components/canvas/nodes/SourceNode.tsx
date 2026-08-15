import type { NodeProps } from '@xyflow/react'
import { memo } from 'react'

import { getFormat } from '@/catalog'
import { Badge } from '@/components/ui'
import type { SourceNode as SourceNodeType } from '@/types/studio'

import { catalogIcon } from '../icons'
import { NodeShell, truncateMiddle, useNodeIssues } from '../NodeShell'

export const SourceNode = memo(function SourceNodeRenderer({
  id,
  data,
  selected,
}: NodeProps<SourceNodeType>) {
  const format = getFormat(data.format)
  const issues = useNodeIssues(id)

  return (
    <NodeShell
      nodeId={id}
      accent="input"
      icon={catalogIcon(format?.icon ?? 'Database')}
      title={data.label ?? 'Source'}
      selected={selected}
      issues={issues}
      inputs="none"
      hasOutput
      badges={<Badge tone="info">{format?.label ?? data.format}</Badge>}
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
    </NodeShell>
  )
})
