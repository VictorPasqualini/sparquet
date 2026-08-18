import type { NodeProps } from '@xyflow/react'
import { memo, useMemo } from 'react'

import { getFormat, getValidationSink } from '@/catalog'
import { Badge, type BadgeTone } from '@/components/ui'
import { validationSinkLink } from '@/lib/compiler'
import { useEditorStore } from '@/store/editor'
import type { SinkNode as SinkNodeType, ValidationSinkRole } from '@/types/studio'

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

/**
 * Which of the validation step's side outputs this destination is, if any.
 *
 * Read from the graph, not from the node: the link's source handle is what decides,
 * so re-dragging it changes the box immediately with nothing to keep in sync.
 */
export function useValidationSinkRole(nodeId: string): ValidationSinkRole | null {
  const nodes = useEditorStore((state) => state.nodes)
  const edges = useEditorStore((state) => state.edges)
  return useMemo(
    () => validationSinkLink({ nodes, edges }, nodeId)?.role ?? null,
    [edges, nodeId, nodes],
  )
}

export const SinkNode = memo(function SinkNodeRenderer({
  id,
  data,
  selected,
}: NodeProps<SinkNodeType>) {
  const format = getFormat(data.format)
  const issues = useNodeIssues(id)
  const projection = projectionLabel(data.columns)
  const role = useValidationSinkRole(id)
  const sideDef = role ? getValidationSink(role) : null

  return (
    <NodeShell
      nodeId={id}
      accent="output"
      icon={catalogIcon(sideDef?.icon ?? format?.icon ?? 'Database')}
      title={data.label ?? sideDef?.label ?? 'Output'}
      subtitle={sideDef?.subtitle}
      selected={selected}
      issues={issues}
      inputs="single"
      hasOutput={false}
      acceptsSideOutput
      badges={
        <>
          {/* The chip repeats the role in words: the drop-down handle it hangs
              from is a position, and position alone is not an accessible signal. */}
          {sideDef && <Badge tone="brand">side output</Badge>}
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
