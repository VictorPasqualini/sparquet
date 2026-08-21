import {
  Github,
  GraduationCap,
  LayoutDashboard,
  LayoutTemplate,
  Moon,
  PanelLeftClose,
  PanelLeftOpen,
  Plus,
  Search,
  Settings as SettingsIcon,
  Sun,
  type LucideIcon,
} from 'lucide-react'
import {
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
} from 'react'
import { Link, NavLink, Outlet, useNavigate } from 'react-router-dom'

import { CommandPalette, NEW_WORKFLOW_EVENT } from '@/components/layout/CommandPalette'
import {
  Button,
  Field,
  IconButton,
  Input,
  Kbd,
  Modal,
  SectionTitle,
  Textarea,
  Tooltip,
} from '@/components/ui'
import lockupLight from '@/assets/lockup-light.png'
import logoMark from '@/assets/logo.png'
import lockup from '@/assets/lockup.png'
import { cn } from '@/lib/utils/cn'
import { useLibraryStore } from '@/store/library'
import { useSettingsStore } from '@/store/settings'
import { WORKFLOW_ACCENTS, type Workflow, type WorkflowAccent } from '@/types/studio'

const SIDEBAR_KEY = 'sparquet-studio:sidebar'
const GITHUB_URL = 'https://github.com/sparquet/sparquet-studio'
/** Kept in step with package.json by hand — the tsconfig has no resolveJsonModule. */
const APP_VERSION = '0.1.0'

const IS_MAC =
  typeof navigator !== 'undefined' && /Mac|iPhone|iPad|iPod/.test(navigator.userAgent)
const PALETTE_HINT = IS_MAC ? '⌘K' : 'Ctrl+K'

interface NavEntry {
  to: string
  label: string
  icon: LucideIcon
  /** Only the overview must match exactly, or every route would light it up. */
  end?: boolean
}

const NAV: NavEntry[] = [
  { to: '/', label: 'Overview', icon: LayoutDashboard, end: true },
  { to: '/templates', label: 'Templates', icon: LayoutTemplate },
  { to: '/learn', label: 'Learn', icon: GraduationCap },
  { to: '/settings', label: 'Settings', icon: SettingsIcon },
]

/** Workflow accents mapped onto the semantic token set — no raw palette colors. */
const ACCENT_DOT: Record<WorkflowAccent, string> = {
  amber: 'bg-gold',
  sky: 'bg-node-input',
  violet: 'bg-node-combine',
  emerald: 'bg-node-output',
  rose: 'bg-state-danger',
  slate: 'bg-node-inspect',
}

/** Same accents, tinted — carries workflow identity into the collapsed rail. */
const ACCENT_SOFT: Record<WorkflowAccent, string> = {
  amber: 'bg-gold/15',
  sky: 'bg-node-input/15',
  violet: 'bg-node-combine/15',
  emerald: 'bg-node-output/15',
  rose: 'bg-state-danger/15',
  slate: 'bg-node-inspect/15',
}

export function AppShell() {
  const [collapsed, setCollapsed] = useState(readCollapsed)
  const [paletteOpen, setPaletteOpen] = useState(false)
  const [newWorkflowOpen, setNewWorkflowOpen] = useState(false)

  useEffect(() => {
    try {
      window.localStorage.setItem(SIDEBAR_KEY, collapsed ? 'collapsed' : 'expanded')
    } catch {
      // Storage can be denied (private mode); the layout still works in memory.
    }
  }, [collapsed])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (
        (event.metaKey || event.ctrlKey) &&
        !event.altKey &&
        event.key.toLowerCase() === 'k'
      ) {
        event.preventDefault()
        setPaletteOpen((open) => !open)
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  useEffect(() => {
    const onRequest = () => setNewWorkflowOpen(true)
    window.addEventListener(NEW_WORKFLOW_EVENT, onRequest)
    return () => window.removeEventListener(NEW_WORKFLOW_EVENT, onRequest)
  }, [])

  return (
    <div className="flex h-full bg-canvas">
      <Sidebar
        collapsed={collapsed}
        onToggleCollapsed={() => setCollapsed((value) => !value)}
        onOpenPalette={() => setPaletteOpen(true)}
        onNewWorkflow={() => setNewWorkflowOpen(true)}
      />

      {/* relative so an absolutely positioned descendant anchors to the scroller
          rather than to the document, which would make the page itself scroll. */}
      <main className="scroll-area relative min-h-0 min-w-0 flex-1">
        <Outlet />
      </main>

      <CommandPalette open={paletteOpen} onOpenChange={setPaletteOpen} />
      <NewWorkflowModal open={newWorkflowOpen} onOpenChange={setNewWorkflowOpen} />
    </div>
  )
}

/* ---------------------------------------------------------------- sidebar */

interface SidebarProps {
  collapsed: boolean
  onToggleCollapsed: () => void
  onOpenPalette: () => void
  onNewWorkflow: () => void
}

function Sidebar({ collapsed, onToggleCollapsed, onOpenPalette, onNewWorkflow }: SidebarProps) {
  const workflows = useLibraryStore((state) => state.workflows)
  const jobs = useLibraryStore((state) => state.jobs)

  const counts = useMemo(() => {
    const map = new Map<string, number>()
    for (const job of jobs) {
      map.set(job.workflowId, (map.get(job.workflowId) ?? 0) + 1)
    }
    return map
  }, [jobs])

  return (
    <aside
      aria-label="Sidebar"
      className={cn(
        'flex h-full shrink-0 flex-col border-r border-line bg-rail transition-[width] duration-200',
        collapsed ? 'w-16' : 'w-60',
      )}
    >
      <div
        className={cn(
          'flex h-16 shrink-0 items-center border-b border-line',
          collapsed ? 'justify-center px-2' : 'px-3',
        )}
      >
        <Link
          to="/"
          aria-label="Sparquet Studio — overview"
          className="flex items-center gap-2.5 rounded-lg py-1 no-drag"
        >
          {collapsed ? (
            <img src={logoMark} alt="" width={28} height={28} className="shrink-0" />
          ) : (
            <span className="flex min-w-0 flex-col gap-1">
              {/* The delivered wordmark is brand blue; the dark rail needs the white cut. */}
              <img src={lockupLight} alt="" className="hidden h-[22px] w-auto dark:block" />
              <img src={lockup} alt="" className="h-[22px] w-auto dark:hidden" />
              <span className="text-2xs font-medium uppercase tracking-[0.22em] text-content-subtle">
                Studio
              </span>
            </span>
          )}
        </Link>
      </div>

      <div className={cn('shrink-0 p-2', collapsed && 'flex justify-center')}>
        {collapsed ? (
          <Tooltip content="Search" shortcut={PALETTE_HINT} side="right">
            <IconButton label="Search" size="sm" onClick={onOpenPalette}>
              <Search />
            </IconButton>
          </Tooltip>
        ) : (
          <button
            type="button"
            onClick={onOpenPalette}
            className={cn(
              'flex w-full items-center gap-2 rounded-lg border border-line bg-rail-sunken px-2.5 py-1.5',
              'text-xs text-content-subtle transition-colors hover:border-line-strong hover:text-content',
            )}
          >
            <Search className="h-3.5 w-3.5" aria-hidden />
            <span className="flex-1 text-left">Search</span>
            <Kbd>{PALETTE_HINT}</Kbd>
          </button>
        )}
      </div>

      <nav
        aria-label="Primary"
        className={cn(
          'shrink-0 space-y-0.5 px-2 pb-2',
          collapsed && 'flex flex-col items-center',
        )}
      >
        {NAV.map((entry) => (
          <SidebarLink
            key={entry.to}
            to={entry.to}
            end={entry.end}
            label={entry.label}
            icon={entry.icon}
            collapsed={collapsed}
          />
        ))}
      </nav>

      <div className={cn('scroll-area min-h-0 flex-1 px-2 pb-2', collapsed && 'px-0')}>
        {collapsed ? (
          <div className="flex flex-col items-center gap-0.5 border-t border-line pt-2">
            {workflows.map((workflow) => (
              <WorkflowLink
                key={workflow.id}
                workflow={workflow}
                count={counts.get(workflow.id) ?? 0}
                collapsed
              />
            ))}
            <Tooltip content="New workflow" side="right">
              <IconButton label="New workflow" size="sm" onClick={onNewWorkflow}>
                <Plus />
              </IconButton>
            </Tooltip>
          </div>
        ) : (
          <>
            <SectionTitle
              className="px-1 pb-1.5 pt-2"
              action={
                <IconButton label="New workflow" size="xs" onClick={onNewWorkflow}>
                  <Plus />
                </IconButton>
              }
            >
              Workflows
            </SectionTitle>
            <div className="space-y-0.5">
              {workflows.map((workflow) => (
                <WorkflowLink
                  key={workflow.id}
                  workflow={workflow}
                  count={counts.get(workflow.id) ?? 0}
                  collapsed={false}
                />
              ))}
            </div>
            {workflows.length === 0 && (
              <p className="px-1 py-1.5 text-2xs leading-relaxed text-content-subtle">
                No workflows yet. Create one to hold your jobs.
              </p>
            )}
          </>
        )}
      </div>

      <SidebarFooter collapsed={collapsed} onToggleCollapsed={onToggleCollapsed} />
    </aside>
  )
}

interface SidebarLinkProps {
  to: string
  label: string
  icon: LucideIcon
  collapsed: boolean
  end?: boolean
}

function SidebarLink({ to, label, icon: Icon, collapsed, end }: SidebarLinkProps) {
  return (
    <Tooltip content={label} side="right" disabled={!collapsed}>
      <NavLink
        to={to}
        end={end}
        aria-label={collapsed ? label : undefined}
        className={({ isActive }) =>
          cn(
            'flex items-center rounded-lg text-xs font-medium transition-colors',
            collapsed ? 'h-8 w-8 justify-center' : 'gap-2.5 px-2.5 py-2',
            isActive
              ? 'bg-brand-500/12 text-brand-600 dark:text-brand-400'
              : 'text-content-muted hover:bg-rail-sunken hover:text-content',
          )
        }
      >
        <Icon className="h-4 w-4 shrink-0" aria-hidden />
        {!collapsed && <span className="truncate">{label}</span>}
      </NavLink>
    </Tooltip>
  )
}

interface WorkflowLinkProps {
  workflow: Workflow
  count: number
  collapsed: boolean
}

function WorkflowLink({ workflow, count, collapsed }: WorkflowLinkProps) {
  const label = `${workflow.name} — ${countLabel(count)}`

  return (
    <Tooltip content={label} side="right" disabled={!collapsed}>
      <NavLink
        to={`/workflows/${workflow.id}`}
        aria-label={collapsed ? label : undefined}
        className={({ isActive }) =>
          cn(
            'flex items-center rounded-lg text-xs transition-colors',
            collapsed ? 'h-8 w-8 justify-center' : 'gap-2.5 px-2.5 py-1.5',
            isActive
              ? 'bg-brand-500/12 text-content'
              : 'text-content-muted hover:bg-rail-sunken hover:text-content',
          )
        }
      >
        {collapsed ? (
          <span
            className={cn(
              'flex h-6 w-6 items-center justify-center rounded-md text-2xs font-semibold uppercase text-content',
              ACCENT_SOFT[workflow.accent],
            )}
          >
            {workflow.name.slice(0, 2)}
          </span>
        ) : (
          <>
            <span
              className={cn('h-2 w-2 shrink-0 rounded-full', ACCENT_DOT[workflow.accent])}
              aria-hidden
            />
            <span className="min-w-0 flex-1 truncate">{workflow.name}</span>
            <span className="shrink-0 text-2xs tabular-nums text-content-subtle">{count}</span>
          </>
        )}
      </NavLink>
    </Tooltip>
  )
}

function SidebarFooter({
  collapsed,
  onToggleCollapsed,
}: {
  collapsed: boolean
  onToggleCollapsed: () => void
}) {
  const theme = useSettingsStore((state) => state.theme)
  const toggleTheme = useSettingsStore((state) => state.toggleTheme)
  const side = collapsed ? 'right' : 'top'
  const themeLabel = theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'

  return (
    <div
      className={cn(
        'shrink-0 border-t border-line p-2',
        collapsed ? 'flex flex-col items-center gap-1' : 'flex items-center gap-1',
      )}
    >
      <Tooltip content={themeLabel} side={side}>
        <IconButton label={themeLabel} size="sm" onClick={toggleTheme}>
          {theme === 'dark' ? <Sun /> : <Moon />}
        </IconButton>
      </Tooltip>

      <Tooltip content="Source on GitHub" side={side}>
        <a
          href={GITHUB_URL}
          target="_blank"
          rel="noreferrer noopener"
          aria-label="Source on GitHub"
          className="inline-flex h-7 w-7 items-center justify-center rounded-md text-content-muted transition-colors hover:bg-rail-sunken hover:text-content"
        >
          <Github className="h-3.5 w-3.5" aria-hidden />
        </a>
      </Tooltip>

      <Tooltip content={collapsed ? 'Expand sidebar' : 'Collapse sidebar'} side={side}>
        <IconButton
          label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          size="sm"
          onClick={onToggleCollapsed}
        >
          {collapsed ? <PanelLeftOpen /> : <PanelLeftClose />}
        </IconButton>
      </Tooltip>

      {!collapsed && (
        <span className="ml-auto pr-1 text-2xs tabular-nums text-content-subtle">
          v{APP_VERSION}
        </span>
      )}
    </div>
  )
}

/* ----------------------------------------------------------- new workflow */

interface NewWorkflowModalProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

function NewWorkflowModal({ open, onOpenChange }: NewWorkflowModalProps) {
  const createWorkflow = useLibraryStore((state) => state.createWorkflow)
  const navigate = useNavigate()
  const formId = useId()

  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [accent, setAccent] = useState<WorkflowAccent>('amber')
  const [busy, setBusy] = useState(false)
  const accentGroupRef = useRef<HTMLDivElement>(null)

  // Radio groups take one tab stop; arrows move the selection and the focus with it.
  const moveAccent = (event: ReactKeyboardEvent<HTMLButtonElement>, index: number) => {
    const delta =
      event.key === 'ArrowRight' || event.key === 'ArrowDown'
        ? 1
        : event.key === 'ArrowLeft' || event.key === 'ArrowUp'
          ? -1
          : 0
    if (delta === 0) return
    event.preventDefault()
    const next = (index + delta + WORKFLOW_ACCENTS.length) % WORKFLOW_ACCENTS.length
    setAccent(WORKFLOW_ACCENTS[next])
    accentGroupRef.current?.querySelectorAll<HTMLButtonElement>('[role="radio"]')[next]?.focus()
  }

  useEffect(() => {
    if (!open) return
    setName('')
    setDescription('')
    setAccent('amber')
    setBusy(false)
  }, [open])

  const submit = async () => {
    const trimmed = name.trim()
    if (!trimmed || busy) return
    setBusy(true)
    const workflow = await createWorkflow({
      name: trimmed,
      description: description.trim(),
      accent,
    })
    onOpenChange(false)
    navigate(`/workflows/${workflow.id}`)
  }

  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      title="New workflow"
      description="A workflow groups the jobs that belong to the same data domain."
      size="md"
      footer={
        <>
          <Button variant="ghost" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            variant="primary"
            type="submit"
            form={formId}
            loading={busy}
            disabled={!name.trim()}
          >
            Create workflow
          </Button>
        </>
      }
    >
      <form
        id={formId}
        className="space-y-4"
        onSubmit={(event) => {
          event.preventDefault()
          void submit()
        }}
      >
        <Field label="Name" required htmlFor={`${formId}-name`}>
          <Input
            id={`${formId}-name`}
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="Billing, Lastros, Marketing…"
            autoFocus
          />
        </Field>

        <Field
          label="Description"
          help="Optional. What the pipelines in here have in common."
          htmlFor={`${formId}-description`}
        >
          <Textarea
            id={`${formId}-description`}
            value={description}
            onChange={(event) => setDescription(event.target.value)}
            rows={2}
            placeholder="Daily ingestion for the billing domain."
          />
        </Field>

        <Field label="Accent" help="Tells workflows apart across the sidebar and the lists.">
          <div
            ref={accentGroupRef}
            role="radiogroup"
            aria-label="Workflow accent"
            className="flex items-center gap-2"
          >
            {WORKFLOW_ACCENTS.map((option, index) => (
              <button
                key={option}
                type="button"
                role="radio"
                aria-checked={option === accent}
                aria-label={`${option} accent`}
                tabIndex={option === accent ? 0 : -1}
                onKeyDown={(event) => moveAccent(event, index)}
                onClick={() => setAccent(option)}
                className={cn(
                  'flex h-7 w-7 items-center justify-center rounded-full border transition-colors',
                  option === accent
                    ? 'border-line-strong bg-surface-sunken'
                    : 'border-transparent hover:border-line',
                )}
              >
                <span className={cn('h-4 w-4 rounded-full', ACCENT_DOT[option])} aria-hidden />
              </button>
            ))}
          </div>
        </Field>
      </form>
    </Modal>
  )
}

/* ------------------------------------------------------------------ utils */

function readCollapsed(): boolean {
  try {
    return window.localStorage.getItem(SIDEBAR_KEY) === 'collapsed'
  } catch {
    return false
  }
}

function countLabel(count: number): string {
  if (count === 0) return 'no jobs'
  return `${count} ${count === 1 ? 'job' : 'jobs'}`
}
