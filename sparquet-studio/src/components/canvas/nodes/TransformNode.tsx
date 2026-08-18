import type { NodeProps } from '@xyflow/react'
import { Flag } from 'lucide-react'
import { memo, type ReactNode } from 'react'

import { getTransformation } from '@/catalog'
import { Badge } from '@/components/ui'
import { cn } from '@/lib/utils/cn'
import type { TransformNode as TransformNodeType, TransformNodeData } from '@/types/studio'

import { catalogIcon } from '../icons'
import { NodeShell, truncateEnd, useNodeIssues } from '../NodeShell'

export const TransformNode = memo(function TransformNodeRenderer({
  id,
  data,
  selected,
}: NodeProps<TransformNodeType>) {
  const def = getTransformation(data.transform)
  const issues = useNodeIssues(id)
  const preview = describeParams(data)
  const runtimeVar = data.transform === 'collect' ? read(data.params.as) : ''

  // Built as a list so an empty result stays undefined — NodeShell then skips
  // the whole body strip instead of drawing an empty one.
  const chips: ReactNode[] = []
  if (!def) {
    chips.push(
      <Badge key="unknown" tone="danger">
        Unknown type
      </Badge>,
    )
  }
  if (runtimeVar) {
    chips.push(<Badge key="runtime" tone="brand">{`{{${runtimeVar}}}`}</Badge>)
  }
  if (data.skipIfFalse) {
    chips.push(
      <Badge key="guard" tone="warning" icon={<Flag />}>
        {truncateEnd(data.skipIfFalse, 20)}
      </Badge>,
    )
  }

  return (
    <NodeShell
      nodeId={id}
      accent={def?.accent ?? 'transform'}
      icon={catalogIcon(def?.icon ?? 'Box')}
      title={data.label ?? def?.label ?? data.transform}
      selected={selected}
      disabled={data.disabled ?? false}
      issues={issues}
      inputs={def?.secondaryInput ? 'dual' : 'single'}
      hasOutput
      badges={chips.length > 0 ? chips : undefined}
    >
      {preview && (
        <p
          className={cn('truncate text-2xs text-content-muted', preview.mono && 'font-mono')}
          title={preview.full}
        >
          {truncateEnd(preview.text, 40)}
        </p>
      )}
    </NodeShell>
  )
})

/* ------------------------------------------------------------------ preview */

interface ParamPreview {
  text: string
  /** Full value for the title attribute when the node clips it. */
  full: string
  mono?: boolean
}

function line(text: string, mono = false): ParamPreview {
  return { text, full: text, mono }
}

/**
 * The single most useful thing to know about a transformation at a glance.
 * Counts stay in prose; anything the user typed is shown verbatim in monospace.
 */
export function describeParams(data: TransformNodeData): ParamPreview | null {
  const params = data.params
  const type = data.transform

  switch (type) {
    case 'filter':
      return maybe(read(params.condition), true)

    case 'select':
    case 'drop': {
      const columns = list(params.columns)
      if (columns.length === 0) return null
      if (columns.length === 1) return line(columns[0], true)
      return line(`${columns.length} columns`)
    }

    case 'rename': {
      const pairs = pairsOf(params.mappings)
      if (pairs.length === 0) return null
      if (pairs.length === 1) return line(`${pairs[0][0]} → ${pairs[0][1]}`, true)
      return line(`${pairs.length} renames`)
    }

    case 'cast': {
      const pairs = pairsOf(params.columns)
      if (pairs.length === 0) return null
      if (pairs.length === 1) return line(`${pairs[0][0]} → ${pairs[0][1]}`, true)
      return line(`${pairs.length} casts`)
    }

    case 'with_column': {
      // A non-empty map makes column/expression dead config in the engine.
      const pairs = pairsOf(params.columns)
      if (pairs.length === 1) return line(`${pairs[0][0]} = ${pairs[0][1]}`, true)
      if (pairs.length > 1) return line(`${pairs.length} columns`)
      const column = read(params.column) || read(params.name)
      const expression = read(params.expression)
      if (column && expression) return line(`${column} = ${expression}`, true)
      return maybe(column, true)
    }

    case 'struct': {
      const column = read(params.column)
      const fields = pairsOf(params.fields).length
      if (!column) return null
      return line(fields ? `${column} · ${count(fields, 'field')}` : column, true)
    }

    case 'drop_duplicates': {
      const columns = list(params.columns)
      return line(
        columns.length ? `by ${columns.join(', ')}` : 'every column',
        columns.length > 0,
      )
    }

    case 'distinct':
      return line('every column')

    case 'sort': {
      const columns = list(params.columns)
      if (columns.length === 0) return null
      const direction = params.ascending === false ? 'desc' : 'asc'
      return line(`${columns.join(', ')} · ${direction}`, true)
    }

    case 'fill_na': {
      const value = params.value === undefined ? '' : String(params.value)
      const columns = list(params.columns)
      if (!value) return null
      return line(
        columns.length ? `${value} in ${columns.join(', ')}` : `${value} everywhere`,
        true,
      )
    }

    case 'sql': {
      const query = read(params.query)
      if (!query) return null
      const firstLine = query.split('\n').find((row) => row.trim() !== '') ?? query
      return { text: firstLine.trim(), full: query, mono: true }
    }

    case 'join': {
      const how = read(params.how) || 'inner'
      const on = Array.isArray(params.on) ? list(params.on).join(', ') : read(params.on)
      return line(on ? `${how} join on ${on}` : `${how} join`, true)
    }

    case 'union':
      return line(
        params.allow_missing_columns === true
          ? 'by name, missing allowed'
          : 'columns must line up',
      )

    case 'group_by': {
      const by = list(params.by)
      const aggregations = list(params.agg).length
      if (by.length === 0 && aggregations === 0) return null
      const grouped = by.length ? `by ${by.join(', ')}` : 'whole DataFrame'
      return line(aggregations ? `${grouped} · ${count(aggregations, 'agg')}` : grouped, true)
    }

    case 'checkpoint': {
      const method = read(params.method) || 'localCheckpoint'
      return line(params.eager === false ? `${method}, lazy` : method)
    }

    case 'stop_if_empty':
      return maybe(read(params.message) || 'halts when the DataFrame is empty')

    case 'collect': {
      const column = read(params.column)
      const name = read(params.as)
      if (!column || !name) return maybe(column || name, true)
      return line(`${column} → {{${name}}}`, true)
    }

    case 'debug': {
      const label = read(params.label)
      const actions = list(params.actions)
      if (label) return line(label)
      return actions.length ? line(actions.join(', ')) : null
    }

    case '$include':
      return maybe(read(params.$include), true)

    default:
      return null
  }
}

function maybe(value: string, mono = false): ParamPreview | null {
  return value ? line(value, mono) : null
}

function count(total: number, noun: string): string {
  return `${total} ${noun}${total === 1 ? '' : 's'}`
}

function read(value: unknown): string {
  return typeof value === 'string' ? value.trim() : ''
}

function list(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  return value.filter((item) => item !== null && item !== undefined).map((item) => String(item))
}

function pairsOf(value: unknown): [string, string][] {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return []
  return Object.entries(value as Record<string, unknown>).map(
    ([key, entry]): [string, string] => [key, String(entry)],
  )
}
