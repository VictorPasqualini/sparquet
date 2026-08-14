import {
  AlertTriangle,
  CheckCircle2,
  Crosshair,
  Info,
  RefreshCw,
  XCircle,
  type LucideIcon,
} from 'lucide-react'
import { useCallback, useMemo } from 'react'

import { getFormat, getTransformation } from '@/catalog'
import { Badge, Button, EmptyState, SectionTitle, type BadgeTone } from '@/components/ui'
import { cn } from '@/lib/utils/cn'
import { useEditorStore } from '@/store/editor'
import type { IssueSeverity, StudioNode, ValidationIssue } from '@/types/studio'

interface SeverityMeta {
  key: IssueSeverity
  /** Group heading. */
  label: string
  /** Header count wording, e.g. `[one, many]`. */
  noun: [string, string]
  icon: LucideIcon
  tone: BadgeTone
  color: string
}

const SEVERITIES: SeverityMeta[] = [
  {
    key: 'error',
    label: 'Errors',
    noun: ['error', 'errors'],
    icon: XCircle,
    tone: 'danger',
    color: 'text-state-danger',
  },
  {
    key: 'warning',
    label: 'Warnings',
    noun: ['warning', 'warnings'],
    icon: AlertTriangle,
    tone: 'warning',
    color: 'text-state-warning',
  },
  {
    key: 'info',
    label: 'Info',
    noun: ['info', 'info'],
    icon: Info,
    tone: 'info',
    color: 'text-state-info',
  },
]

export function IssuesPanel() {
  const issues = useEditorStore((state) => state.issues)
  const nodes = useEditorStore((state) => state.nodes)
  const selectedNodeId = useEditorStore((state) => state.selectedNodeId)
  const select = useEditorStore((state) => state.select)
  const togglePanel = useEditorStore((state) => state.togglePanel)
  const lint = useEditorStore((state) => state.lint)

  const groups = useMemo(() => {
    const grouped: Record<IssueSeverity, ValidationIssue[]> = {
      error: [],
      warning: [],
      info: [],
    }
    for (const issue of issues) grouped[issue.severity].push(issue)
    return grouped
  }, [issues])

  const titles = useMemo(
    () => new Map(nodes.map((node) => [node.id, nodeTitle(node)] as const)),
    [nodes],
  )

  const reveal = useCallback(
    (nodeId: string) => {
      select(nodeId)
      togglePanel('inspector', true)
    },
    [select, togglePanel],
  )

  return (
    <div className="flex h-full min-h-0 flex-col bg-surface">
      <header className="flex items-center gap-1.5 border-b border-line px-2.5 py-2">
        {issues.length === 0 ? (
          <span className="text-2xs text-content-subtle">No problems</span>
        ) : (
          SEVERITIES.map(({ key, tone, icon: Icon, noun }) =>
            groups[key].length > 0 ? (
              <Badge key={key} tone={tone} icon={<Icon aria-hidden />}>
                <span className="tabular-nums">{groups[key].length}</span>
                <span>{groups[key].length === 1 ? noun[0] : noun[1]}</span>
              </Badge>
            ) : null,
          )
        )}
        <Button
          className="ml-auto"
          size="xs"
          variant="ghost"
          icon={<RefreshCw />}
          onClick={lint}
        >
          Re-check
        </Button>
      </header>

      {issues.length === 0 ? (
        <div className="flex-1">
          <EmptyState
            icon={<CheckCircle2 className="text-state-success" />}
            title="No problems detected"
            description="The graph compiles cleanly. Studio re-checks it as you edit."
          />
        </div>
      ) : (
        <div className="scroll-area flex-1 space-y-3 p-2">
          {SEVERITIES.map(({ key, label, icon: Icon, color }) => {
            const list = groups[key]
            if (list.length === 0) return null
            return (
              <section key={key} className="space-y-1">
                <SectionTitle className="px-1">
                  {label}
                  <span className="ml-1.5 tabular-nums text-content-subtle">{list.length}</span>
                </SectionTitle>
                <ul className="space-y-0.5">
                  {list.map((issue) => (
                    <li key={issue.id}>
                      <IssueRow
                        issue={issue}
                        icon={Icon}
                        color={color}
                        title={issue.nodeId ? titles.get(issue.nodeId) : undefined}
                        active={Boolean(issue.nodeId) && issue.nodeId === selectedNodeId}
                        onReveal={reveal}
                      />
                    </li>
                  ))}
                </ul>
              </section>
            )
          })}
        </div>
      )}
    </div>
  )
}

/* --------------------------------------------------------------------- row */

interface IssueRowProps {
  issue: ValidationIssue
  icon: LucideIcon
  color: string
  /** Title of the owning node, when it still exists on the canvas. */
  title?: string
  active: boolean
  onReveal: (nodeId: string) => void
}

function IssueRow({ issue, icon: Icon, color, title, active, onReveal }: IssueRowProps) {
  const body = (
    <>
      <Icon className={cn('mt-0.5 h-3.5 w-3.5 shrink-0', color)} aria-hidden />
      <span className="min-w-0 flex-1">
        <span className="block text-xs leading-snug text-content">{issue.message}</span>
        {issue.hint && (
          <span className="mt-0.5 block text-2xs leading-relaxed text-content-muted">
            {issue.hint}
          </span>
        )}
        {title && (
          <span className="mt-1 flex items-center gap-1 text-2xs text-content-subtle">
            <Crosshair className="h-3 w-3 shrink-0" aria-hidden />
            <span className="truncate">
              {title}
              {issue.field ? ` · ${issue.field}` : ''}
            </span>
          </span>
        )}
      </span>
    </>
  )

  const nodeId = issue.nodeId
  if (!nodeId || !title) {
    return <div className="flex items-start gap-2 rounded-lg px-2 py-1.5">{body}</div>
  }

  return (
    <button
      type="button"
      onClick={() => onReveal(nodeId)}
      aria-current={active || undefined}
      className={cn(
        'flex w-full items-start gap-2 rounded-lg border px-2 py-1.5 text-left transition-colors',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/40',
        active
          ? 'border-line-strong bg-surface-sunken'
          : 'border-transparent hover:border-line hover:bg-surface-sunken',
      )}
    >
      {body}
    </button>
  )
}

/* ----------------------------------------------------------------- helpers */

/** Human label for the node an issue belongs to. */
function nodeTitle(node: StudioNode): string {
  const data = node.data
  if (data.label) return data.label
  switch (data.kind) {
    case 'source':
      return `${getFormat(data.format)?.label ?? data.format} source`
    case 'transform':
      return getTransformation(data.transform)?.label ?? data.transform
    case 'validations':
      return 'Validations'
    case 'sink':
      return `${getFormat(data.format)?.label ?? data.format} output`
    case 'note':
      return 'Note'
  }
}
