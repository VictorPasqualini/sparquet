import type { NodeProps } from '@xyflow/react'
import { memo } from 'react'

import { getFormat, getValidationSink } from '@/catalog'
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
  // The role is stored on the node: the `validations` block is job-scoped, so this
  // destination belongs to the job and hangs off no rule in particular.
  const sideDef = data.dqRole ? getValidationSink(data.dqRole) : null

  return (
    <NodeShell
      nodeId={id}
      accent="output"
      icon={catalogIcon(sideDef?.icon ?? format?.icon ?? 'Database')}
      title={data.label ?? sideDef?.label ?? 'Output'}
      subtitle={sideDef?.subtitle}
      selected={selected}
      issues={issues}
      // A quality destination is a declaration, not a step: the validations block
      // writes it from the DataFrame every rule saw, so there is nothing to wire in
      // and nothing to pass on.
      // The quarantine of rejected rows takes an OPTIONAL input: linking rules into
      // it scopes the split to those rules. Unlinked still means "every row-level
      // rule", so the handle adds a capability without changing the default. The
      // report and the valid side have nothing to scope, so they stay handle-less.
      inputs={!sideDef || data.dqRole === 'invalid' ? 'single' : 'none'}
      hasOutput={false}
      badges={
        <>
          {/* The chip repeats the role in words, so the node says what it is even
              when its title has been renamed. */}
          {sideDef && <Badge tone="brand">quality</Badge>}
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
