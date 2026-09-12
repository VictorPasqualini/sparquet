/**
 * The tab strip of the SQL editor, and the list of saved queries behind it.
 *
 * Presentational on purpose: every tab, and the decision about what is dirty,
 * lives in the screen. What this file owns is the shape a SQL console has —
 * several statements open at once, one of them current, each either a file or
 * still a draft — because that is the part worth keeping consistent and the
 * part that would otherwise be two hundred lines in the middle of the screen.
 *
 * A dot rather than a badge marks unsaved work. A tab with no dot is on disk;
 * a tab with one has something the library does not have yet, which is the only
 * distinction a person needs while typing.
 */

import { FilePlus2, FolderOpen, MoreHorizontal, Save, X } from 'lucide-react'

import {
  Button,
  IconButton,
  Menu,
  MenuContent,
  MenuItem,
  MenuLabel,
  MenuSeparator,
  MenuTrigger,
} from '@/components/ui'
import { cn } from '@/lib/utils/cn'
import type { SavedQuery } from '@/types/studio'

export interface QueryTabView {
  tabId: string
  /** The saved query this tab holds, or `null` while it is still a draft. */
  queryId: string | null
  name: string
  dirty: boolean
}

export interface QueryTabsProps {
  tabs: QueryTabView[]
  activeTabId: string
  /** Everything in the library, for the open menu. Already sorted by the store. */
  saved: SavedQuery[]
  onSelect: (tabId: string) => void
  onClose: (tabId: string) => void
  onNew: () => void
  onOpen: (queryId: string) => void
  onSave: () => void
  onRename: () => void
  onDelete: () => void
  saving?: boolean
}

export function QueryTabs({
  tabs,
  activeTabId,
  saved,
  onSelect,
  onClose,
  onNew,
  onOpen,
  onSave,
  onRename,
  onDelete,
  saving = false,
}: QueryTabsProps) {
  const active = tabs.find((tab) => tab.tabId === activeTabId)

  return (
    <div className="flex items-center gap-1 border-b border-line bg-surface-sunken/60 px-1.5 py-1">
      {/* The strip scrolls rather than wrapping: a second row of tabs moves the
          editor down every time somebody opens a query, and an editor whose top
          edge jumps is worse than a strip that has to be scrolled. */}
      <div className="flex min-w-0 flex-1 items-center gap-1 overflow-x-auto">
        {tabs.map((tab) => {
          const current = tab.tabId === activeTabId
          return (
            <div
              key={tab.tabId}
              className={cn(
                'group flex shrink-0 items-center gap-1 rounded-lg border px-2 py-1 text-2xs transition-colors',
                current
                  ? 'border-line-strong bg-surface text-content'
                  : 'border-transparent text-content-subtle hover:bg-surface/60 hover:text-content',
              )}
            >
              <button
                type="button"
                onClick={() => onSelect(tab.tabId)}
                aria-current={current ? 'true' : undefined}
                className="max-w-[12rem] truncate outline-none"
                title={tab.name}
              >
                {tab.name}
              </button>
              {tab.dirty ? (
                <span
                  aria-label="Unsaved changes"
                  title="Unsaved changes"
                  className="h-1.5 w-1.5 shrink-0 rounded-full bg-state-warning"
                />
              ) : null}
              <button
                type="button"
                onClick={() => onClose(tab.tabId)}
                aria-label={`Close ${tab.name}`}
                className="shrink-0 rounded p-0.5 text-content-subtle opacity-0 transition-opacity hover:text-content focus-visible:opacity-100 group-hover:opacity-100"
              >
                <X className="h-3 w-3" />
              </button>
            </div>
          )
        })}
        <IconButton label="New query" size="sm" variant="ghost" onClick={onNew}>
          <FilePlus2 />
        </IconButton>
      </div>

      <div className="flex shrink-0 items-center gap-1">
        <Menu>
          <MenuTrigger asChild>
            <Button size="xs" variant="ghost" icon={<FolderOpen />}>
              Open
            </Button>
          </MenuTrigger>
          <MenuContent align="end" className="max-h-80 overflow-y-auto">
            <MenuLabel>Saved queries</MenuLabel>
            {saved.length === 0 ? (
              <MenuItem disabled>Nothing saved yet</MenuItem>
            ) : (
              saved.map((query) => (
                <MenuItem key={query.id} onSelect={() => onOpen(query.id)}>
                  {query.name}
                </MenuItem>
              ))
            )}
          </MenuContent>
        </Menu>

        <Button
          size="xs"
          variant="ghost"
          icon={<Save />}
          onClick={onSave}
          disabled={saving || !active}
        >
          Save
        </Button>

        <Menu>
          <MenuTrigger asChild>
            <IconButton label="Query actions" size="sm" variant="ghost">
              <MoreHorizontal />
            </IconButton>
          </MenuTrigger>
          <MenuContent align="end">
            <MenuItem onSelect={onRename} disabled={!active}>
              Rename…
            </MenuItem>
            <MenuSeparator />
            <MenuItem danger onSelect={onDelete} disabled={!active?.queryId}>
              Delete query
            </MenuItem>
          </MenuContent>
        </Menu>
      </div>
    </div>
  )
}
