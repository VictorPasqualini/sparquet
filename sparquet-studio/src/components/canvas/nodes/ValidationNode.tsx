import type { NodeProps } from '@xyflow/react'
import { memo } from 'react'

import { getValidator } from '@/catalog'
import { targetsOf } from '@/lib/compiler/targets'
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
  const type = data.validator

  // A multi-target rule runs once per target, so what it measures is the list of
  // targets — showing only the shared defaults would describe a rule that never runs
  // on its own.
  const targets = describeTargets(params)
  if (targets) return targets

  switch (type) {
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

    default: {
      // Every metric is a rule type of its own, so this renders them all —
      // `missing_percent(cpf) < 1%` — and an unknown type imported from a
      // hand-written config the same way, instead of showing nothing.
      const column = read(params.column) || list(params.columns).join(', ')
      const threshold = read(params.must_be)
      const scope = column ? `${type}(${column})` : type
      return threshold ? `${scope} ${threshold}` : scope || null
    }
  }
}

/** `2 targets · cpf, cnpj` — the count first, since the columns are truncated. */
function describeTargets(params: Record<string, unknown>): string | null {
  const targets = targetsOf(params)
  if (!targets) return null
  const columns = targets
    .map((target) => read(target.column) || list(target.columns).join(', '))
    .filter((column) => column !== '')
  const label = `${targets.length} target${targets.length === 1 ? '' : 's'}`
  return columns.length > 0 ? `${label} · ${columns.join(', ')}` : label
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
