import {
  CornerDownLeft,
  FolderOpen,
  FolderPlus,
  GraduationCap,
  LayoutTemplate,
  Moon,
  Search,
  Settings,
  Sun,
  Workflow as WorkflowIcon,
  type LucideIcon,
} from 'lucide-react'
import {
  forwardRef,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
} from 'react'
import { useLocation, useNavigate } from 'react-router-dom'

import { EmptyState, Input, Kbd, Modal } from '@/components/ui'
import { LESSONS } from '@/data/lessons'
import { cn } from '@/lib/utils/cn'
import { useLibraryStore } from '@/store/library'
import { useSettingsStore } from '@/store/settings'
import type { Workflow } from '@/types/studio'

/**
 * Asks the shell to open its "New project" modal.
 *
 * The palette owns the event name so the shell can import it without the two
 * modules importing each other.
 */
export const NEW_PROJECT_EVENT = 'sparquet-studio:new-project'

export interface CommandPaletteProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

const GROUPS = [
  { id: 'actions', label: 'Actions' },
  { id: 'workflows', label: 'Workflows' },
  { id: 'projects', label: 'Projects' },
  { id: 'lessons', label: 'Learn' },
] as const

type GroupId = (typeof GROUPS)[number]['id']

interface CommandItem {
  id: string
  group: GroupId
  label: string
  hint?: string
  icon: LucideIcon
  /** Extra words the fuzzy matcher can hit, never rendered. */
  keywords: string
  run: () => void
}

/** How many rows a group may contribute, searching versus sitting idle. */
const LIMITS: Record<GroupId, number> = { actions: 6, workflows: 8, projects: 8, lessons: 6 }
const IDLE_LIMITS: Record<GroupId, number> = {
  actions: 6,
  workflows: 5,
  projects: 5,
  lessons: 3,
}

export function CommandPalette({ open, onOpenChange }: CommandPaletteProps) {
  const navigate = useNavigate()
  const location = useLocation()
  const projects = useLibraryStore((state) => state.projects)
  const workflows = useLibraryStore((state) => state.workflows)
  const createWorkflow = useLibraryStore((state) => state.createWorkflow)
  const theme = useSettingsStore((state) => state.theme)
  const toggleTheme = useSettingsStore((state) => state.toggleTheme)

  const [query, setQuery] = useState('')
  const [active, setActive] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)
  const rowRefs = useRef<(HTMLButtonElement | null)[]>([])
  const baseId = useId()
  const listId = `${baseId}-list`

  const activeProjectId = /^\/projects\/([^/]+)/.exec(location.pathname)?.[1]

  const items = useMemo<CommandItem[]>(() => {
    const projectName = new Map(projects.map((project) => [project.id, project.name]))
    const countByProject = new Map<string, number>()
    for (const workflow of workflows) {
      countByProject.set(workflow.projectId, (countByProject.get(workflow.projectId) ?? 0) + 1)
    }

    const newWorkflow = () => {
      const projectId = activeProjectId ?? projects[0]?.id
      // Nothing to hang a workflow on yet — send the user through project creation.
      if (!projectId) {
        window.dispatchEvent(new Event(NEW_PROJECT_EVENT))
        return
      }
      void createWorkflow({ projectId, name: nextWorkflowName(workflows) }).then((workflow) =>
        navigate(`/workflows/${workflow.id}`),
      )
    }

    const actions: CommandItem[] = [
      {
        id: 'action:new-project',
        group: 'actions',
        label: 'New project',
        hint: 'Group related workflows',
        icon: FolderPlus,
        keywords: 'create add folder workspace',
        run: () => window.dispatchEvent(new Event(NEW_PROJECT_EVENT)),
      },
      {
        id: 'action:new-workflow',
        group: 'actions',
        label: 'New workflow',
        hint: 'Start an empty pipeline',
        icon: WorkflowIcon,
        keywords: 'create add pipeline canvas blank',
        run: newWorkflow,
      },
      {
        id: 'action:toggle-theme',
        group: 'actions',
        label: 'Toggle theme',
        hint: theme === 'dark' ? 'Switch to light' : 'Switch to dark',
        icon: theme === 'dark' ? Sun : Moon,
        keywords: 'dark light appearance contrast',
        run: toggleTheme,
      },
      {
        id: 'action:settings',
        group: 'actions',
        label: 'Open settings',
        hint: 'AI, runner, canvas, data',
        icon: Settings,
        keywords: 'preferences api key runner storage',
        run: () => navigate('/settings'),
      },
      {
        id: 'action:templates',
        group: 'actions',
        label: 'Open templates',
        hint: 'Start from a working pipeline',
        icon: LayoutTemplate,
        keywords: 'examples starters gallery samples',
        run: () => navigate('/templates'),
      },
    ]

    const workflowItems: CommandItem[] = [...workflows]
      .sort((a, b) => b.updatedAt - a.updatedAt)
      .map((workflow) => ({
        id: `workflow:${workflow.id}`,
        group: 'workflows',
        label: workflow.name,
        hint: projectName.get(workflow.projectId) ?? 'No project',
        icon: WorkflowIcon,
        keywords: [workflow.description, ...workflow.tags, workflow.settings.pipelineName]
          .filter(Boolean)
          .join(' '),
        run: () => navigate(`/workflows/${workflow.id}`),
      }))

    const projectItems: CommandItem[] = projects.map((project) => ({
      id: `project:${project.id}`,
      group: 'projects',
      label: project.name,
      hint: workflowCountLabel(countByProject.get(project.id) ?? 0),
      icon: FolderOpen,
      keywords: project.description,
      run: () => navigate(`/projects/${project.id}`),
    }))

    const lessonItems: CommandItem[] = LESSONS.map((lesson) => ({
      id: `lesson:${lesson.id}`,
      group: 'lessons',
      label: lesson.title,
      hint: `${lesson.minutes} min · ${lesson.level}`,
      icon: GraduationCap,
      keywords: lesson.summary,
      run: () => navigate(`/learn/${lesson.id}`),
    }))

    return [...actions, ...workflowItems, ...projectItems, ...lessonItems]
  }, [activeProjectId, createWorkflow, navigate, projects, theme, toggleTheme, workflows])

  const groups = useMemo(() => {
    const needle = query.replace(/\s+/g, '').toLowerCase()
    let index = 0

    return GROUPS.map((group) => {
      const scored = items
        .filter((item) => item.group === group.id)
        .map((item) => ({
          item,
          score: fuzzyScore(needle, `${item.label} ${item.hint ?? ''} ${item.keywords}`),
        }))
        .filter((entry) => entry.score >= 0)

      if (needle) scored.sort((a, b) => b.score - a.score)
      const limit = needle ? LIMITS[group.id] : IDLE_LIMITS[group.id]

      return {
        ...group,
        rows: scored.slice(0, limit).map((entry) => ({ item: entry.item, index: index++ })),
      }
    }).filter((group) => group.rows.length > 0)
  }, [items, query])

  const flat = useMemo(
    () => groups.flatMap((group) => group.rows.map((row) => row.item)),
    [groups],
  )

  useEffect(() => {
    if (!open) return
    setQuery('')
    setActive(0)

    // The dialog's focus scope sends focus to its close button on mount, and puts
    // it back once more on the way out of React's double-invoked effects. Claim
    // the caret across that short window — on a timer rather than a frame, which
    // a hidden tab would never run — so the search box always ends up focused.
    let attempts = 0
    const timer = window.setInterval(() => {
      const input = inputRef.current
      if (input && document.activeElement !== input) input.focus()
      attempts += 1
      if (attempts >= 10) window.clearInterval(timer)
    }, 16)

    return () => window.clearInterval(timer)
  }, [open])

  useEffect(() => {
    setActive((current) => (current >= flat.length ? 0 : current))
  }, [flat.length])

  useEffect(() => {
    rowRefs.current[active]?.scrollIntoView({ block: 'nearest' })
  }, [active, flat.length])

  const runItem = (item: CommandItem) => {
    onOpenChange(false)
    item.run()
  }

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (flat.length === 0) return
    if (event.key === 'ArrowDown') {
      event.preventDefault()
      setActive((current) => (current + 1) % flat.length)
    } else if (event.key === 'ArrowUp') {
      event.preventDefault()
      setActive((current) => (current - 1 + flat.length) % flat.length)
    } else if (event.key === 'Home') {
      event.preventDefault()
      setActive(0)
    } else if (event.key === 'End') {
      event.preventDefault()
      setActive(flat.length - 1)
    } else if (event.key === 'Enter') {
      event.preventDefault()
      const item = flat[active]
      if (item) runItem(item)
    }
  }

  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      title="Command palette"
      description="Jump to a workflow, a project or a lesson — or run an action."
      size="lg"
      footer={
        <div className="flex w-full items-center justify-between gap-3 text-2xs text-content-subtle">
          <span className="flex items-center gap-1.5">
            <Kbd>↑</Kbd>
            <Kbd>↓</Kbd>
            <span>move</span>
            <Kbd>↵</Kbd>
            <span>open</span>
            <Kbd>esc</Kbd>
            <span>close</span>
          </span>
          <span>
            {flat.length} {flat.length === 1 ? 'result' : 'results'}
          </span>
        </div>
      }
    >
      <div className="space-y-3">
        <Input
          ref={inputRef}
          value={query}
          onChange={(event) => {
            setQuery(event.target.value)
            setActive(0)
          }}
          onKeyDown={onKeyDown}
          placeholder="Search workflows, projects, lessons…"
          leading={<Search />}
          role="combobox"
          aria-expanded
          aria-controls={listId}
          aria-autocomplete="list"
          aria-activedescendant={flat[active] ? `${baseId}-row-${active}` : undefined}
          aria-label="Search commands"
        />

        {flat.length === 0 ? (
          <EmptyState
            icon={<Search />}
            title="No matches"
            description="Try a shorter term, or the name of a transformation you remember."
          />
        ) : (
          <div
            id={listId}
            role="listbox"
            aria-label="Commands"
            className="scroll-area max-h-[min(58vh,24rem)] space-y-3 pr-0.5"
          >
            {groups.map((group) => (
              <div key={group.id} role="group" aria-labelledby={`${baseId}-${group.id}`}>
                <p
                  id={`${baseId}-${group.id}`}
                  className="px-1 pb-1 text-2xs font-semibold uppercase tracking-wider text-content-subtle"
                >
                  {group.label}
                </p>
                <div className="space-y-0.5">
                  {group.rows.map(({ item, index }) => (
                    <CommandRow
                      key={item.id}
                      ref={(element) => {
                        rowRefs.current[index] = element
                      }}
                      id={`${baseId}-row-${index}`}
                      item={item}
                      active={index === active}
                      onHover={() => setActive(index)}
                      onSelect={() => runItem(item)}
                    />
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </Modal>
  )
}

/* -------------------------------------------------------------------- row */

interface CommandRowProps {
  id: string
  item: CommandItem
  active: boolean
  onHover: () => void
  onSelect: () => void
}

const CommandRow = forwardRef<HTMLButtonElement, CommandRowProps>(function CommandRow(
  { id, item, active, onHover, onSelect },
  ref,
) {
  const Icon = item.icon
  return (
    <button
      ref={ref}
      id={id}
      type="button"
      role="option"
      aria-selected={active}
      tabIndex={-1}
      onMouseMove={onHover}
      onClick={onSelect}
      className={cn(
        'flex w-full items-center gap-2.5 rounded-lg px-2 py-2 text-left transition-colors',
        active ? 'bg-brand-500/12 text-content' : 'text-content-muted hover:bg-surface-sunken',
      )}
    >
      <span
        className={cn(
          'flex h-6 w-6 shrink-0 items-center justify-center rounded-md border',
          active
            ? 'border-brand-500/40 bg-brand-500/15 text-brand-600 dark:text-brand-400'
            : 'border-line bg-surface-sunken text-content-subtle',
        )}
      >
        <Icon className="h-3.5 w-3.5" aria-hidden />
      </span>
      <span className="min-w-0 flex-1">
        <span className="block truncate text-xs font-medium text-content">{item.label}</span>
        {item.hint && (
          <span className="block truncate text-2xs text-content-subtle">{item.hint}</span>
        )}
      </span>
      {active && (
        <CornerDownLeft className="h-3.5 w-3.5 shrink-0 text-content-subtle" aria-hidden />
      )}
    </button>
  )
})

/* ------------------------------------------------------------------ utils */

/**
 * Subsequence match, scored so that contiguous runs and word starts win.
 * Returns -1 when the query cannot be spelled out of the text.
 */
function fuzzyScore(query: string, text: string): number {
  if (!query) return 0
  const haystack = text.toLowerCase()
  let score = 0
  let cursor = 0
  let streak = 0

  for (const char of query) {
    const index = haystack.indexOf(char, cursor)
    if (index === -1) return -1
    const previous = index === 0 ? ' ' : haystack[index - 1]
    const boundary =
      previous === ' ' || previous === '_' || previous === '-' || previous === '/'
    streak = index === cursor ? streak + 1 : 0
    score += 1 + streak * 2 + (boundary ? 3 : 0) - Math.min(index - cursor, 8) * 0.1
    cursor = index + 1
  }

  return Math.max(score, 0)
}

function workflowCountLabel(count: number): string {
  if (count === 0) return 'No workflows'
  return `${count} ${count === 1 ? 'workflow' : 'workflows'}`
}

function nextWorkflowName(workflows: Workflow[]): string {
  const base = 'Untitled workflow'
  const taken = new Set(workflows.map((workflow) => workflow.name))
  if (!taken.has(base)) return base
  let suffix = 2
  while (taken.has(`${base} ${suffix}`)) suffix += 1
  return `${base} ${suffix}`
}
