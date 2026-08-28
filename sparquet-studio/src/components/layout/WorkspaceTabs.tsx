/**
 * The tab strip over the centre of an editor.
 *
 * The side column is for panels that comment on the graph — the inspector, the
 * linter, the assistant. This strip is for surfaces that ARE the workspace: the
 * flow, the JSON behind it, the executions it has had. They each want the middle
 * of the screen, so they take turns there instead of being squeezed into 380px.
 *
 * A real tablist, not a radiogroup: arrows move the selection, one stop in the
 * tab order, and each panel names the tab that controls it.
 */

import type { LucideIcon } from 'lucide-react'
import { useRef, type KeyboardEvent, type ReactNode } from 'react'

import { cn } from '@/lib/utils/cn'

export interface WorkspaceTab<T extends string> {
  id: T
  label: string
  icon: LucideIcon
  /** A count or dot rendered after the label. */
  badge?: ReactNode
}

export const workspaceTabId = (id: string): string => `workspace-tab-${id}`
export const workspacePanelId = (id: string): string => `workspace-panel-${id}`

export function WorkspaceTabs<T extends string>({
  value,
  onChange,
  tabs,
  ariaLabel,
  actions,
}: {
  value: T
  onChange: (value: T) => void
  tabs: WorkspaceTab<T>[]
  ariaLabel: string
  /** Anything the active surface wants on the right of the strip. */
  actions?: ReactNode
}) {
  const listRef = useRef<HTMLDivElement>(null)

  const onKeyDown = (event: KeyboardEvent<HTMLButtonElement>, index: number) => {
    const delta = event.key === 'ArrowRight' ? 1 : event.key === 'ArrowLeft' ? -1 : 0
    if (delta === 0) return
    event.preventDefault()
    const next = (index + delta + tabs.length) % tabs.length
    onChange(tabs[next].id)
    listRef.current?.querySelectorAll<HTMLButtonElement>('[role="tab"]')[next]?.focus()
  }

  return (
    <div className="flex h-9 shrink-0 items-center gap-2 border-b border-line bg-surface px-2">
      <div ref={listRef} role="tablist" aria-label={ariaLabel} className="flex items-center gap-0.5">
        {tabs.map((tab, index) => {
          const active = tab.id === value
          return (
            <button
              key={tab.id}
              type="button"
              role="tab"
              id={workspaceTabId(tab.id)}
              aria-selected={active}
              aria-controls={workspacePanelId(tab.id)}
              tabIndex={active ? 0 : -1}
              onKeyDown={(event) => onKeyDown(event, index)}
              onClick={() => onChange(tab.id)}
              className={cn(
                'flex items-center gap-1.5 rounded-lg px-2.5 py-1 text-2xs font-medium transition-colors',
                active
                  ? 'bg-brand-500/12 text-brand-600 dark:text-brand-400'
                  : 'text-content-subtle hover:bg-surface-sunken hover:text-content',
              )}
            >
              <tab.icon className="h-3.5 w-3.5 shrink-0" />
              {tab.label}
              {tab.badge}
            </button>
          )
        })}
      </div>
      {actions && <div className="ml-auto flex items-center gap-1.5">{actions}</div>}
    </div>
  )
}
