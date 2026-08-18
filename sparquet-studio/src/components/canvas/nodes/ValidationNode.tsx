import type { NodeProps } from '@xyflow/react'
import { memo, useMemo } from 'react'

import { getValidator, VALIDATION_SINKS } from '@/catalog'
import { Badge } from '@/components/ui'
import { isLastValidationOfRun } from '@/lib/compiler'
import { useEditorStore } from '@/store/editor'
import type { ValidationNode as ValidationNodeType, ValidationNodeData } from '@/types/studio'
import { VALIDATION_SINK_HANDLES, validationSinkRoleOfHandle } from '@/types/studio'

import { catalogIcon } from '../icons'
import { NodeShell, truncateEnd, useNodeIssues, type SideOutput } from '../NodeShell'

export const ValidationNode = memo(function ValidationNodeRenderer({
  id,
  data,
  selected,
}: NodeProps<ValidationNodeType>) {
  const def = getValidator(data.validator)
  const issues = useNodeIssues(id)
  const preview = describeRule(data)
  const sideOutputs = useSideOutputs(id)

  return (
    <NodeShell
      nodeId={id}
      accent="validate"
      icon={catalogIcon(def?.icon ?? 'ShieldCheck')}
      title={data.label ?? def?.label ?? data.validator}
      subtitle={
        sideOutputs.length > 0
          ? 'validation rule · side outputs below, main chain keeps every row'
          : 'validation rule'
      }
      selected={selected}
      disabled={data.disabled ?? false}
      issues={issues}
      inputs="single"
      hasOutput
      sideOutputs={sideOutputs}
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

/* ------------------------------------------------------------- side outputs */

/**
 * The three side-output handles this rule offers.
 *
 * They belong to the END of the run — "after every rule ran" is the moment the
 * framework writes them — so only the last rule shows them. A rule that already has
 * one connected keeps its handle whatever its position, otherwise inserting a rule
 * behind it would leave an edge React Flow cannot draw.
 */
function useSideOutputs(nodeId: string): SideOutput[] {
  const nodes = useEditorStore((state) => state.nodes)
  const edges = useEditorStore((state) => state.edges)

  return useMemo(() => {
    const connected = new Set(
      edges
        .filter((edge) => edge.source === nodeId)
        .map((edge) => validationSinkRoleOfHandle(edge.sourceHandle))
        .filter((role): role is NonNullable<typeof role> => role !== null),
    )
    const isLast = isLastValidationOfRun({ nodes, edges }, nodeId)

    return VALIDATION_SINKS.filter((sink) => isLast || connected.has(sink.role)).map(
      (sink) => ({
        id: VALIDATION_SINK_HANDLES[sink.role],
        label: sink.handleLabel,
        title: `${sink.label} — written beside the job's own destinations, which still receive every row.`,
      }),
    )
  }, [edges, nodeId, nodes])
}

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
