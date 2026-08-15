import type { NodeProps } from '@xyflow/react'
import { FileOutput } from 'lucide-react'
import { memo } from 'react'

import { getValidator } from '@/catalog'
import { Badge, type BadgeTone } from '@/components/ui'
import type { OnFailureMode } from '@/types/pipeline'
import type { ValidationsNode as ValidationsNodeType } from '@/types/studio'

import { catalogIcon } from '../icons'
import { NodeShell, truncateMiddle, useNodeIssues } from '../NodeShell'

/** `fail` aborts the run, `warn` only reports — the badge has to say which. */
const FAILURE_TONE: Record<OnFailureMode, BadgeTone> = {
  fail: 'danger',
  warn: 'warning',
  skip: 'neutral',
}

export const ValidationsNode = memo(function ValidationsNodeRenderer({
  id,
  data,
  selected,
}: NodeProps<ValidationsNodeType>) {
  const issues = useNodeIssues(id)
  const rules = data.rules ?? []
  const byType = countByType(rules.map((rule) => rule.type))
  const report = data.report ?? null

  return (
    <NodeShell
      nodeId={id}
      accent="validate"
      icon={catalogIcon('ShieldCheck')}
      title={data.label ?? 'Validations'}
      subtitle={rules.length ? plural(rules.length, 'rule') : 'No rules yet'}
      selected={selected}
      issues={issues}
      inputs="single"
      hasOutput
      badges={
        <>
          <Badge tone={FAILURE_TONE[data.onFailure] ?? 'neutral'}>
            on failure: {data.onFailure}
          </Badge>
          {report && (
            <Badge tone="info" icon={<FileOutput />}>
              report
            </Badge>
          )}
        </>
      }
    >
      {byType.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {byType.map(([type, total]) => (
            <span
              key={type}
              className="rounded-md bg-surface-sunken px-1.5 py-0.5 text-2xs text-content-muted"
            >
              {getValidator(type)?.label ?? type}
              {total > 1 && <span className="text-content-subtle"> ×{total}</span>}
            </span>
          ))}
        </div>
      )}
      {report && (
        <p className="truncate font-mono text-2xs text-content-subtle" title={report.path}>
          {report.format} · {truncateMiddle(report.path, 28)}
        </p>
      )}
    </NodeShell>
  )
})

function countByType(types: string[]): [string, number][] {
  const counts = new Map<string, number>()
  for (const type of types) counts.set(type, (counts.get(type) ?? 0) + 1)
  return [...counts.entries()]
}

function plural(total: number, noun: string): string {
  return `${total} ${noun}${total === 1 ? '' : 's'}`
}
