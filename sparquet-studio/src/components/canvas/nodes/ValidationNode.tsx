import type { NodeProps } from '@xyflow/react'
import { memo } from 'react'

import { getValidator } from '@/catalog'
import { Badge } from '@/components/ui'
import type { ValidationNode as ValidationNodeType, ValidationNodeData } from '@/types/studio'

import { catalogIcon } from '../icons'
import { NodeShell, truncateEnd, useNodeIssues } from '../NodeShell'

export const ValidationNode = memo(function ValidationNodeRenderer({
  id,
  data,
  selected,
}: NodeProps<ValidationNodeType>) {
  const def = getValidator(data.validator)
  const issues = useNodeIssues(id)
  const preview = describeRule(data)

  return (
    <NodeShell
      nodeId={id}
      accent="validate"
      icon={catalogIcon(def?.icon ?? 'ShieldCheck')}
      title={data.label ?? def?.label ?? data.validator}
      subtitle="validation rule"
      selected={selected}
      disabled={data.disabled ?? false}
      issues={issues}
      inputs="single"
      hasOutput
      badges={
        def ? undefined : (
          <Badge key="unknown" tone="danger">
            Unknown rule
          </Badge>
        )
      }
    >
      {preview && (
        <p className="truncate font-mono text-2xs text-content-muted" title={preview}>
          {truncateEnd(preview, 40)}
        </p>
      )}
    </NodeShell>
  )
})

/* ------------------------------------------------------------------ preview */

/** The one thing worth knowing about a rule at a glance: what it measures. */
export function describeRule(data: ValidationNodeData): string | null {
  const params = data.params

  switch (data.validator) {
    case 'not_null':
    case 'unique': {
      const columns = list(params.columns)
      return columns.length > 0 ? columns.join(', ') : null
    }

    case 'range': {
      const column = read(params.column)
      if (!column) return null
      const min = bound(params.min)
      const max = bound(params.max)
      if (min === null && max === null) return column
      return `${column} in [${min ?? '−∞'}, ${max ?? '∞'}]`
    }

    case 'regex': {
      const column = read(params.column)
      const pattern = read(params.pattern)
      if (!column) return pattern || null
      return pattern ? `${column} ~ ${pattern}` : column
    }

    case 'row_count': {
      const min = bound(params.min)
      const max = bound(params.max)
      if (min === null && max === null) return null
      return `rows in [${min ?? 0}, ${max ?? '∞'}]`
    }

    case 'sql': {
      const query = read(params.failed_rows) || read(params.query)
      if (!query) return null
      return (query.split('\n').find((row) => row.trim() !== '') ?? query).trim()
    }

    case 'check': {
      const metric = read(params.metric)
      const column = read(params.column) || list(params.columns).join(', ')
      const threshold = read(params.must_be)
      if (!metric) return threshold || null
      const scope = column ? `${metric}(${column})` : metric
      return threshold ? `${scope} ${threshold}` : scope
    }

    case 'schema': {
      const required = list(params.required_columns).length
      const forbidden = list(params.forbidden_columns).length
      const typed = Object.keys(isRecord(params.column_types) ? params.column_types : {}).length
      const parts = [
        required > 0 ? `${required} required` : '',
        forbidden > 0 ? `${forbidden} forbidden` : '',
        typed > 0 ? `${typed} typed` : '',
      ].filter(Boolean)
      return parts.length > 0 ? parts.join(' · ') : null
    }

    default:
      return null
  }
}

function read(value: unknown): string {
  return typeof value === 'string' ? value.trim() : ''
}

function bound(value: unknown): string | null {
  if (typeof value === 'number') return String(value)
  const text = read(value)
  return text === '' ? null : text
}

function list(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  return value.filter((item) => item !== null && item !== undefined).map((item) => String(item))
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
