import { ReactFlowProvider } from '@xyflow/react'
import {
  AlertTriangle,
  ArrowLeft,
  Bot,
  Braces,
  CircleCheck,
  LayoutGrid,
  PanelRightClose,
  ListChecks,
  Loader2,
  Play,
  Redo2,
  Save,
  Settings2,
  SlidersHorizontal,
  Undo2,
} from 'lucide-react'
import {
  lazy,
  Suspense,
  useCallback,
  useEffect,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
} from 'react'
import { Link, useLocation, useNavigate, useParams } from 'react-router-dom'
import { toast } from 'sonner'

import { JobCanvas } from '@/components/canvas/JobCanvas'
import { CommandPalette } from '@/components/layout/CommandPalette'
import { AiPanel } from '@/components/panels/AiPanel'
import { Inspector } from '@/components/panels/Inspector'
import { IssuesPanel } from '@/components/panels/IssuesPanel'
import { JobSettingsPanel } from '@/components/panels/JobSettingsPanel'
import { NodePalette } from '@/components/panels/NodePalette'
import { RunPanel } from '@/components/panels/RunPanel'
import { Badge, IconButton, Input, Spinner, Tooltip } from '@/components/ui'
import { relativeTime } from '@/lib/utils/format'
import { cn } from '@/lib/utils/cn'
import { getJob } from '@/lib/storage/db'
import { useEditorStore, type PanelId } from '@/store/editor'
import { useLibraryStore } from '@/store/library'
import type { Job } from '@/types/studio'

/** Monaco is heavy and only the JSON tab needs it. */
const JsonPanel = lazy(() =>
  import('@/components/panels/JsonPanel').then((m) => ({ default: m.JsonPanel })),
)

const SIDE_PANELS: {
  id: PanelId
  label: string
  /** Shorter form for the panel tab strip, which is always tight. */
  tab: string
  icon: typeof Bot
  shortcut: string
}[] = [
  { id: 'inspector', label: 'Inspector', tab: 'Inspector', icon: SlidersHorizontal, shortcut: 'I' },
  { id: 'settings', label: 'Job settings', tab: 'Job', icon: Settings2, shortcut: '⌘,' },
  { id: 'ai', label: 'AI assistant', tab: 'AI', icon: Bot, shortcut: '⌘/' },
  { id: 'json', label: 'JSON', tab: 'JSON', icon: Braces, shortcut: '⌘J' },
  { id: 'run', label: 'Run', tab: 'Run', icon: Play, shortcut: '⌘⏎' },
  { id: 'issues', label: 'Issues', tab: 'Issues', icon: ListChecks, shortcut: '⌘E' },
]

export function JobEditor() {
  const { jobId } = useParams<{ jobId: string }>()
  const navigate = useNavigate()
  const location = useLocation()

  const open = useEditorStore((state) => state.open)
  const close = useEditorStore((state) => state.close)
  const togglePanel = useEditorStore((state) => state.togglePanel)

  const [loading, setLoading] = useState(true)
  const [missing, setMissing] = useState(false)

  // Read once per load: a navigation state object is a fresh identity on every
  // render and must never re-trigger the loader.
  const openAiOnLoad = useRef((location.state as { openAi?: boolean } | null)?.openAi === true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setMissing(false)

    void (async () => {
      if (!jobId) return
      const fromStore = useLibraryStore.getState().jobs.find((w) => w.id === jobId)
      const job: Job | null | undefined = fromStore ?? (await getJob(jobId))
      if (cancelled) return
      if (!job) {
        setMissing(true)
        setLoading(false)
        return
      }
      open(job)
      setLoading(false)
      if (openAiOnLoad.current) togglePanel('ai', true)
    })()

    return () => {
      cancelled = true
      close()
    }
  }, [jobId, open, close, togglePanel])

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center bg-canvas">
        <Spinner className="h-5 w-5" />
      </div>
    )
  }

  if (missing) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-3 bg-canvas text-center">
        <p className="text-sm text-content">This job no longer exists.</p>
        <Link to="/" className="text-xs text-brand-600 hover:underline dark:text-brand-400">
          Back to overview
        </Link>
      </div>
    )
  }

  return (
    <ReactFlowProvider>
      <div className="flex h-full flex-col bg-canvas">
        <EditorTopBar onBack={() => navigate(-1)} />
        <div className="flex min-h-0 flex-1">
          <aside className="w-60 shrink-0 border-r border-line bg-surface">
            <NodePalette />
          </aside>
          <main className="relative min-w-0 flex-1">
            <JobCanvas />
          </main>
          <SidePanel />
        </div>
        <EditorStatusBar />
      </div>
      <EditorShortcuts />
      <EditorCommandPalette />
    </ReactFlowProvider>
  )
}

/** The editor renders outside AppShell, so it mounts its own ⌘K palette. */
function EditorCommandPalette() {
  const [open, setOpen] = useState(false)

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        setOpen((value) => !value)
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  return <CommandPalette open={open} onOpenChange={setOpen} />
}

function EditorTopBar({ onBack }: { onBack: () => void }) {
  const job = useEditorStore((state) => state.job)
  const dirty = useEditorStore((state) => state.dirty)
  const saving = useEditorStore((state) => state.saving)
  const save = useEditorStore((state) => state.save)
  const undo = useEditorStore((state) => state.undo)
  const redo = useEditorStore((state) => state.redo)
  const layout = useEditorStore((state) => state.layout)
  const past = useEditorStore((state) => state.past.length)
  const future = useEditorStore((state) => state.future.length)
  const activePanel = useEditorStore((state) => state.activePanel)
  const togglePanel = useEditorStore((state) => state.togglePanel)
  const updateJobMeta = useLibraryStore((state) => state.updateJobMeta)

  const [name, setName] = useState(job?.name ?? '')
  useEffect(() => setName(job?.name ?? ''), [job?.name])

  const commitName = () => {
    const trimmed = name.trim()
    if (!job || !trimmed || trimmed === job.name) {
      setName(job?.name ?? '')
      return
    }
    void updateJobMeta(job.id, { name: trimmed })
    useEditorStore.setState({ job: { ...job, name: trimmed } })
  }

  return (
    <header className="flex h-14 shrink-0 items-center gap-3 border-b border-line bg-surface px-3">
      <IconButton label="Back" onClick={onBack}>
        <ArrowLeft />
      </IconButton>

      <div className="min-w-0 flex-1">
        <Input
          value={name}
          onChange={(event) => setName(event.target.value)}
          onBlur={commitName}
          onKeyDown={(event) => {
            if (event.key === 'Enter') event.currentTarget.blur()
            if (event.key === 'Escape') {
              setName(job?.name ?? '')
              event.currentTarget.blur()
            }
          }}
          aria-label="Job name"
          className="h-8 max-w-sm border-transparent bg-transparent px-2 text-sm font-medium hover:border-line focus:bg-surface-sunken"
        />
      </div>

      <div className="flex items-center gap-1">
        <Tooltip content="Undo" shortcut="⌘Z">
          <IconButton label="Undo" onClick={undo} disabled={past === 0} size="sm">
            <Undo2 />
          </IconButton>
        </Tooltip>
        <Tooltip content="Redo" shortcut="⇧⌘Z">
          <IconButton label="Redo" onClick={redo} disabled={future === 0} size="sm">
            <Redo2 />
          </IconButton>
        </Tooltip>
        <Tooltip content="Auto-layout" shortcut="⇧⌘L">
          <IconButton label="Auto-layout" onClick={layout} size="sm">
            <LayoutGrid />
          </IconButton>
        </Tooltip>
      </div>

      <div className="mx-1 h-6 w-px bg-line" />

      <div className="flex items-center gap-1">
        {SIDE_PANELS.map((panel) => (
          <Tooltip key={panel.id} content={panel.label} shortcut={panel.shortcut}>
            <IconButton
              label={panel.label}
              size="sm"
              active={activePanel === panel.id}
              onClick={() => togglePanel(panel.id)}
            >
              <panel.icon />
            </IconButton>
          </Tooltip>
        ))}
      </div>

      <div className="mx-1 h-6 w-px bg-line" />

      <button
        type="button"
        onClick={() => void save()}
        className="flex items-center gap-1.5 rounded-lg px-2 py-1 text-2xs text-content-subtle transition-colors hover:bg-surface-sunken"
        title={dirty ? 'Unsaved changes — click to save now' : 'All changes saved'}
      >
        {saving ? (
          <Loader2 className="h-3 w-3 animate-spin" />
        ) : dirty ? (
          <Save className="h-3 w-3" />
        ) : (
          <CircleCheck className="h-3 w-3 text-state-success" />
        )}
        {saving ? 'Saving' : dirty ? 'Unsaved' : 'Saved'}
      </button>
    </header>
  )
}

const tabId = (panel: PanelId): string => `panel-tab-${panel}`
const panelId = (panel: PanelId): string => `panel-body-${panel}`

/**
 * One tabbed surface rather than stacked panels: two panels sharing the column
 * left neither of them usable. The width is drag-resizable and remembered.
 */
function SidePanel() {
  const activePanel = useEditorStore((state) => state.activePanel)
  const width = useEditorStore((state) => state.panelWidth)
  const setPanelWidth = useEditorStore((state) => state.setPanelWidth)
  const togglePanel = useEditorStore((state) => state.togglePanel)
  const issueCount = useEditorStore((state) => state.issues.length)
  const [resizing, setResizing] = useState(false)
  const tabsRef = useRef<HTMLDivElement>(null)

  const onTabKeyDown = (event: ReactKeyboardEvent<HTMLButtonElement>, index: number) => {
    const delta = event.key === 'ArrowRight' ? 1 : event.key === 'ArrowLeft' ? -1 : 0
    if (delta === 0) return
    event.preventDefault()
    const next = (index + delta + SIDE_PANELS.length) % SIDE_PANELS.length
    togglePanel(SIDE_PANELS[next].id, true)
    tabsRef.current?.querySelectorAll<HTMLButtonElement>('[role="tab"]')[next]?.focus()
  }

  useEffect(() => {
    if (!resizing) return
    const onMove = (event: PointerEvent) => setPanelWidth(window.innerWidth - event.clientX)
    const onUp = () => setResizing(false)
    window.addEventListener('pointermove', onMove)
    window.addEventListener('pointerup', onUp)
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'
    return () => {
      window.removeEventListener('pointermove', onMove)
      window.removeEventListener('pointerup', onUp)
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
    }
  }, [resizing, setPanelWidth])

  if (!activePanel) return null

  return (
    <aside
      className="relative flex shrink-0 flex-col border-l border-line bg-surface"
      style={{ width }}
    >
      <div
        role="separator"
        aria-orientation="vertical"
        aria-label="Resize panel"
        onPointerDown={(event) => {
          event.preventDefault()
          setResizing(true)
        }}
        className={cn(
          'absolute -left-1 top-0 z-10 h-full w-2 cursor-col-resize',
          'transition-colors hover:bg-brand-500/40',
          resizing && 'bg-brand-500/60',
        )}
      />

      <div className="flex h-10 shrink-0 items-center gap-0.5 border-b border-line px-1.5">
        <div
          ref={tabsRef}
          role="tablist"
          aria-label="Editor panels"
          className="flex min-w-0 items-center gap-0.5"
        >
          {SIDE_PANELS.map((panel, index) => {
            const active = panel.id === activePanel
            return (
              <button
                key={panel.id}
                type="button"
                role="tab"
                id={tabId(panel.id)}
                aria-selected={active}
                // Only the open panel exists in the DOM, so only its tab may claim it.
                aria-controls={active ? panelId(panel.id) : undefined}
                tabIndex={active ? 0 : -1}
                onKeyDown={(event) => onTabKeyDown(event, index)}
                onClick={() => togglePanel(panel.id, true)}
                title={panel.label}
                className={cn(
                  'flex shrink-0 items-center gap-1.5 whitespace-nowrap rounded-lg px-2 py-1',
                  'text-2xs font-medium transition-colors',
                  active
                    ? 'bg-brand-500/12 text-brand-600 dark:text-brand-400'
                    : 'text-content-subtle hover:bg-surface-sunken hover:text-content',
                )}
              >
                <panel.icon className="h-3.5 w-3.5 shrink-0" />
                {panel.tab}
                {panel.id === 'issues' && issueCount > 0 && (
                  <span className="rounded-full bg-surface-sunken px-1 text-[0.6rem] text-content-muted">
                    {issueCount}
                  </span>
                )}
              </button>
            )
          })}
        </div>
        <IconButton
          label="Close panel"
          size="sm"
          className="ml-auto"
          onClick={() => togglePanel(activePanel, false)}
        >
          <PanelRightClose />
        </IconButton>
      </div>

      <div
        role="tabpanel"
        id={panelId(activePanel)}
        aria-labelledby={tabId(activePanel)}
        tabIndex={0}
        className="flex min-h-0 flex-1 flex-col"
      >
        {activePanel === 'inspector' && <Inspector />}
        {activePanel === 'settings' && <JobSettingsPanel />}
        {activePanel === 'ai' && <AiPanel />}
        {activePanel === 'json' && (
          <Suspense
            fallback={
              <div className="flex flex-1 items-center justify-center">
                <Spinner />
              </div>
            }
          >
            <JsonPanel />
          </Suspense>
        )}
        {activePanel === 'run' && <RunPanel />}
        {activePanel === 'issues' && <IssuesPanel />}
      </div>
    </aside>
  )
}

function EditorStatusBar() {
  const nodes = useEditorStore((state) => state.nodes.length)
  const issues = useEditorStore((state) => state.issues)
  const lastSavedAt = useEditorStore((state) => state.lastSavedAt)
  const togglePanel = useEditorStore((state) => state.togglePanel)

  const errors = issues.filter((issue) => issue.severity === 'error').length
  const warnings = issues.filter((issue) => issue.severity === 'warning').length

  return (
    <footer className="flex h-8 shrink-0 items-center gap-3 border-t border-line bg-surface px-3 text-2xs text-content-subtle">
      <span>{nodes} nodes</span>
      <button
        type="button"
        onClick={() => togglePanel('issues', true)}
        className="flex items-center gap-2 transition-colors hover:text-content"
      >
        {errors > 0 ? (
          <Badge tone="danger" icon={<AlertTriangle />}>
            {errors} errors
          </Badge>
        ) : (
          <Badge tone="success" icon={<CircleCheck />}>
            valid
          </Badge>
        )}
        {warnings > 0 && <Badge tone="warning">{warnings} warnings</Badge>}
      </button>
      <span className="ml-auto">
        {lastSavedAt ? `Saved ${relativeTime(lastSavedAt)}` : 'Not saved yet'}
      </span>
    </footer>
  )
}

/** Global editor shortcuts. Kept in one place so the map stays discoverable. */
function EditorShortcuts() {
  const save = useEditorStore((state) => state.save)
  const undo = useEditorStore((state) => state.undo)
  const redo = useEditorStore((state) => state.redo)
  const layout = useEditorStore((state) => state.layout)
  const togglePanel = useEditorStore((state) => state.togglePanel)
  const removeNodes = useEditorStore((state) => state.removeNodes)
  const duplicateNode = useEditorStore((state) => state.duplicateNode)

  const handler = useCallback(
    (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null
      const typing =
        target?.tagName === 'INPUT' ||
        target?.tagName === 'TEXTAREA' ||
        target?.isContentEditable === true

      const mod = event.metaKey || event.ctrlKey
      const selected = useEditorStore.getState().selectedNodeId

      if (mod && event.key.toLowerCase() === 's') {
        event.preventDefault()
        void save()
        toast.success('Job saved')
        return
      }
      if (mod && event.key.toLowerCase() === 'z') {
        event.preventDefault()
        if (event.shiftKey) redo()
        else undo()
        return
      }
      if (mod && event.shiftKey && event.key.toLowerCase() === 'l') {
        event.preventDefault()
        layout()
        return
      }
      if (mod && event.key === '/') {
        event.preventDefault()
        togglePanel('ai')
        return
      }
      if (mod && event.key.toLowerCase() === 'j') {
        event.preventDefault()
        togglePanel('json')
        return
      }
      if (mod && event.key.toLowerCase() === 'e') {
        event.preventDefault()
        togglePanel('issues')
        return
      }
      if (mod && event.key === ',') {
        event.preventDefault()
        togglePanel('settings')
        return
      }
      if (mod && event.key === 'Enter') {
        event.preventDefault()
        togglePanel('run', true)
        return
      }
      if (typing) return
      if (mod && event.key.toLowerCase() === 'd' && selected) {
        event.preventDefault()
        duplicateNode(selected)
        return
      }
      if ((event.key === 'Delete' || event.key === 'Backspace') && selected) {
        event.preventDefault()
        removeNodes([selected])
      }
    },
    [save, undo, redo, layout, togglePanel, removeNodes, duplicateNode],
  )

  useEffect(() => {
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [handler])

  return null
}
