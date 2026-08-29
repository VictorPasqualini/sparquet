/**
 * The pipeline editor: pipelines of the workflow on the left, the sequence on
 * the canvas, the run stream on the right.
 *
 * It is the sibling of `JobEditor`, one level up: that screen edits what is
 * inside a single JSON, this one edits the ORDER several JSONs run in. Opening a
 * stage lands in that job's editor, which is the drill-down from a box to
 * its transformations.
 */

import { ReactFlowProvider } from '@xyflow/react'
import {
  AlertTriangle,
  ArrowLeft,
  CircleCheck,
  History as HistoryIcon,
  Loader2,
  PanelRightClose,
  PanelRightOpen,
  Redo2,
  Save,
  Undo2,
  Workflow,
} from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { toast } from 'sonner'

import { CreditsBadge } from '@/components/credits/CreditsBadge'
import { PipelineRunViewBanner, showPipelineRun } from '@/components/history/PipelineRunViewBanner'
import { RunsBrowser } from '@/components/history/RunsBrowser'
import {
  WorkspaceTabs,
  workspacePanelId,
  workspaceTabId,
  type WorkspaceTab,
} from '@/components/layout/WorkspaceTabs'
import { PipelineCanvas } from '@/components/pipeline/PipelineCanvas'
import { TagsPopover } from '@/components/library/TagsPopover'
import { PipelineRunPanel } from '@/components/pipeline/PipelineRunPanel'
import { StagePicker } from '@/components/pipeline/StagePicker'
import { Badge, IconButton, Input, Spinner, Tooltip } from '@/components/ui'
import { resolvePipeline, stageRowPosition, type ResolvedPipeline } from '@/lib/pipeline'
import { listLibraryFiles, type LibraryFile } from '@/lib/runner/libraryFiles'
import { getPipeline } from '@/lib/storage/db'
import { collectTags } from '@/lib/tags'
import { cn } from '@/lib/utils/cn'
import { plural, relativeTime } from '@/lib/utils/format'
import {
  usePipelineEditorStore,
  type PipelineWorkspaceView,
} from '@/store/pipelineEditor'
import { useLibraryStore } from '@/store/library'
import { useSettingsStore } from '@/store/settings'
import type { Pipeline, ValidationIssue } from '@/types/studio'

export function PipelineEditor() {
  const { pipelineId } = useParams<{ pipelineId: string }>()

  const open = usePipelineEditorStore((state) => state.open)
  const close = usePipelineEditorStore((state) => state.close)

  const [loading, setLoading] = useState(true)
  const [missing, setMissing] = useState(false)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setMissing(false)

    void (async () => {
      if (!pipelineId) return
      const fromStore = useLibraryStore.getState().pipelines.find((pipeline) => pipeline.id === pipelineId)
      const pipeline: Pipeline | null | undefined = fromStore ?? (await getPipeline(pipelineId))
      if (cancelled) return
      if (!pipeline) {
        setMissing(true)
        setLoading(false)
        return
      }
      open(pipeline)
      setLoading(false)
    })()

    return () => {
      cancelled = true
      close()
    }
  }, [pipelineId, open, close])

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
        <p className="text-sm text-content">This pipeline no longer exists.</p>
        <Link to="/" className="text-xs text-brand-600 hover:underline dark:text-brand-400">
          Back to overview
        </Link>
      </div>
    )
  }

  return <PipelineWorkbench />
}

/**
 * The runnable JSON files in the library, read from the runner.
 *
 * Re-read on demand rather than watched: the list changes when somebody edits
 * the directory, which the browser cannot see, so a refresh button is the honest
 * affordance. `null` means "not read" — no runner, or it did not answer — which
 * is different from an empty library and is shown differently.
 */
function useLibraryFiles(): {
  files: LibraryFile[] | null
  error: string | null
  refresh: () => void
} {
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const [files, setFiles] = useState<LibraryFile[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [nonce, setNonce] = useState(0)

  useEffect(() => {
    const controller = new AbortController()
    void (async () => {
      try {
        const listing = await listLibraryFiles(runnerUrl, runnerToken, controller.signal)
        if (controller.signal.aborted) return
        setFiles(listing.files)
        setError(null)
      } catch (caught) {
        if (controller.signal.aborted) return
        setFiles(null)
        // A runner that is simply not running is the normal case here, not a
        // failure worth a toast: the picker says so in place of the list.
        setError(
          caught instanceof Error && caught.name !== 'TypeError' ? caught.message : null,
        )
      }
    })()
    return () => controller.abort()
  }, [runnerUrl, runnerToken, nonce])

  return { files, error, refresh: () => setNonce((value) => value + 1) }
}

function PipelineWorkbench() {
  const navigate = useNavigate()

  const pipeline = usePipelineEditorStore((state) => state.pipeline)
  const stages = usePipelineEditorStore((state) => state.stages)
  const links = usePipelineEditorStore((state) => state.links)
  const addStage = usePipelineEditorStore((state) => state.addStage)
  const addFileStage = usePipelineEditorStore((state) => state.addFileStage)

  const jobs = useLibraryStore((state) => state.jobs)
  const [showPanel, setShowPanel] = useState(true)
  const libraryFiles = useLibraryFiles()

  const workflowJobs = useMemo(
    () => jobs.filter((job) => job.workflowId === pipeline?.workflowId),
    [pipeline?.workflowId, jobs],
  )

  /**
   * Resolved from the live stores, so a stage always describes the job as it
   * is right now — including one that was renamed, edited or deleted in another tab.
   */
  const resolved = useMemo(() => {
    if (!pipeline) return null
    return resolvePipeline({ ...pipeline, stages, links }, jobs)
  }, [pipeline, links, stages, jobs])

  const usage = useMemo(() => {
    const counts: Record<string, number> = {}
    for (const stage of stages) {
      if (!stage.jobId) continue
      counts[stage.jobId] = (counts[stage.jobId] ?? 0) + 1
    }
    return counts
  }, [stages])

  const fileUsage = useMemo(() => {
    const counts: Record<string, number> = {}
    for (const stage of stages) {
      if (!stage.path) continue
      counts[stage.path] = (counts[stage.path] ?? 0) + 1
    }
    return counts
  }, [stages])

  if (!pipeline || !resolved) return null

  return (
    <ReactFlowProvider>
      <div className="flex h-full flex-col bg-canvas">
        <PipelineTopBar
          onBack={() => navigate(`/workflows/${pipeline.workflowId}`)}
          panelOpen={showPanel}
          onTogglePanel={() => setShowPanel((value) => !value)}
        />
        <div className="flex min-h-0 flex-1">
          <aside className="w-60 shrink-0 border-r border-line bg-surface">
            <StagePicker
              jobs={workflowJobs}
              usage={usage}
              // Appending at the end of the row keeps the picker usable from the
              // keyboard, where there is no drop point to aim at.
              onAdd={(jobId) => addStage(jobId, stageRowPosition(stages.length))}
              files={libraryFiles.files}
              fileUsage={fileUsage}
              onAddFile={(path) => addFileStage(path, stageRowPosition(stages.length))}
              onRefreshFiles={libraryFiles.refresh}
              filesError={libraryFiles.error}
            />
          </aside>
          <PipelineWorkspace resolved={resolved} />
          {showPanel && (
            <aside className="flex w-[380px] shrink-0 flex-col border-l border-line bg-surface">
              <PipelineRunPanel resolved={resolved} />
            </aside>
          )}
        </div>
        <PipelineStatusBar stageCount={stages.length} issues={resolved.issues} />
      </div>
      <PipelineShortcuts />
    </ReactFlowProvider>
  )
}

const PIPELINE_TABS: WorkspaceTab<PipelineWorkspaceView>[] = [
  { id: 'flow', label: 'Flow', icon: Workflow },
  { id: 'runs', label: 'Runs', icon: HistoryIcon },
]

/**
 * The middle of the pipeline editor: the flow, and the executions it has had.
 *
 * Opening a pipeline lands on the flow, never on an old run painted over it. A run
 * is something you go and get — `Pipeline → run id` in the Runs tab — and from
 * there either onto these stage boxes or into the job that ran as one of them.
 *
 * The canvas stays mounted across tabs so React Flow keeps the viewport.
 */
function PipelineWorkspace({ resolved }: { resolved: ResolvedPipeline }) {
  // In the store, so the run panel can send the user here from the side.
  const view = usePipelineEditorStore((state) => state.workspaceView)
  const setView = usePipelineEditorStore((state) => state.setWorkspaceView)
  const navigate = useNavigate()

  const pipeline = usePipelineEditorStore((state) => state.pipeline)
  const run = usePipelineEditorStore((state) => state.run)
  const runView = usePipelineEditorStore((state) => state.runView)
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)

  return (
    <main className="flex min-w-0 flex-1 flex-col">
      <WorkspaceTabs
        value={view}
        onChange={setView}
        tabs={PIPELINE_TABS}
        ariaLabel="Pipeline workspace"
      />

      <div className="relative min-h-0 flex-1">
        <div
          role="tabpanel"
          id={workspacePanelId('flow')}
          aria-labelledby={workspaceTabId('flow')}
          className={cn('absolute inset-0', view !== 'flow' && 'hidden')}
        >
          <PipelineCanvas resolved={resolved} />
          {/* Says which execution the stage boxes are describing, and loads it. */}
          <PipelineRunViewBanner />
        </div>

        {view === 'runs' && pipeline && (
          <div
            role="tabpanel"
            id={workspacePanelId('runs')}
            aria-labelledby={workspaceTabId('runs')}
            className="absolute inset-0 bg-surface"
          >
            <RunsBrowser
              runnerUrl={runnerUrl}
              runnerToken={runnerToken}
              workflowId={pipeline.workflowId}
              pipelineId={pipeline.id}
              refreshToken={run?.runId}
              subject={pipeline.name}
              // A whole run paints every stage box; one job of it opens that job's
              // canvas showing the same execution, step by step.
              onViewRun={(runId) => {
                void showPipelineRun(runId, runnerUrl, runnerToken)
                setView('flow')
              }}
              viewingRunId={runView?.runId ?? null}
              onViewJobRun={(record, jobRun) => {
                if (!jobRun.jobId) return
                navigate(`/jobs/${jobRun.jobId}`, {
                  state: { runId: record.id, jobRunId: jobRun.id },
                })
              }}
              viewActionLabel="View on canvas"
            />
          </div>
        )}
      </div>
    </main>
  )
}

/** Save, undo and redo, matching the pipeline editor's map. */
function PipelineShortcuts() {
  const save = usePipelineEditorStore((state) => state.save)
  const undo = usePipelineEditorStore((state) => state.undo)
  const redo = usePipelineEditorStore((state) => state.redo)

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      const mod = event.metaKey || event.ctrlKey
      if (!mod) return
      const key = event.key.toLowerCase()
      if (key === 's') {
        event.preventDefault()
        void save()
        toast.success('Pipeline saved')
      } else if (key === 'z') {
        event.preventDefault()
        if (event.shiftKey) redo()
        else undo()
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [redo, save, undo])

  return null
}

function PipelineTopBar({
  onBack,
  panelOpen,
  onTogglePanel,
}: {
  onBack: () => void
  panelOpen: boolean
  onTogglePanel: () => void
}) {
  const pipeline = usePipelineEditorStore((state) => state.pipeline)
  const dirty = usePipelineEditorStore((state) => state.dirty)
  const saving = usePipelineEditorStore((state) => state.saving)
  const save = usePipelineEditorStore((state) => state.save)
  const undo = usePipelineEditorStore((state) => state.undo)
  const redo = usePipelineEditorStore((state) => state.redo)
  const past = usePipelineEditorStore((state) => state.past.length)
  const future = usePipelineEditorStore((state) => state.future.length)
  const updatePipelineMeta = useLibraryStore((state) => state.updatePipelineMeta)
  const knownTags = useLibraryStore((state) =>
    collectTags([...state.jobs, ...state.pipelines, ...state.workflows]),
  )

  const [name, setName] = useState(pipeline?.name ?? '')
  useEffect(() => setName(pipeline?.name ?? ''), [pipeline?.name])

  const commitName = () => {
    const trimmed = name.trim()
    if (!pipeline || !trimmed || trimmed === pipeline.name) {
      setName(pipeline?.name ?? '')
      return
    }
    void updatePipelineMeta(pipeline.id, { name: trimmed })
    usePipelineEditorStore.setState({ pipeline: { ...pipeline, name: trimmed } })
  }

  const commitTags = (tags: string[]) => {
    if (!pipeline) return
    void updatePipelineMeta(pipeline.id, { tags })
    usePipelineEditorStore.setState({ pipeline: { ...pipeline, tags } })
  }

  return (
    <header className="flex h-14 shrink-0 items-center gap-3 border-b border-line bg-surface px-3">
      <IconButton label="Back to the workflow" onClick={onBack}>
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
              setName(pipeline?.name ?? '')
              event.currentTarget.blur()
            }
          }}
          aria-label="Pipeline name"
          className="h-8 max-w-sm border-transparent bg-transparent px-2 text-sm font-medium hover:border-line focus:bg-surface-sunken"
        />
      </div>

      <TagsPopover
        tags={pipeline?.tags ?? []}
        onChange={commitTags}
        suggestions={knownTags}
        subject={pipeline?.name ?? 'this pipeline'}
      />

      <div className="flex items-center gap-1">
        <Tooltip content="Undo">
          <IconButton label="Undo" onClick={undo} disabled={past === 0} size="sm">
            <Undo2 />
          </IconButton>
        </Tooltip>
        <Tooltip content="Redo">
          <IconButton label="Redo" onClick={redo} disabled={future === 0} size="sm">
            <Redo2 />
          </IconButton>
        </Tooltip>
        <Tooltip content={panelOpen ? 'Hide the run panel' : 'Show the run panel'}>
          <IconButton
            label={panelOpen ? 'Hide the run panel' : 'Show the run panel'}
            size="sm"
            active={panelOpen}
            onClick={onTogglePanel}
          >
            {panelOpen ? <PanelRightClose /> : <PanelRightOpen />}
          </IconButton>
        </Tooltip>
      </div>

      <CreditsBadge linkToBilling={false} />

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

function PipelineStatusBar({
  stageCount,
  issues,
}: {
  stageCount: number
  issues: ValidationIssue[]
}) {
  const lastSavedAt = usePipelineEditorStore((state) => state.lastSavedAt)

  const errors = issues.filter((issue) => issue.severity === 'error').length
  const warnings = issues.filter((issue) => issue.severity === 'warning').length

  return (
    <footer className="flex h-8 shrink-0 items-center gap-3 border-t border-line bg-surface px-3 text-2xs text-content-subtle">
      <span>{plural(stageCount, 'stage')}</span>
      <span className="flex items-center gap-2">
        {errors > 0 ? (
          <Badge tone="danger" icon={<AlertTriangle />}>
            {plural(errors, 'blocking issue')}
          </Badge>
        ) : (
          stageCount > 0 && (
            <Badge tone="success" icon={<CircleCheck />}>
              ready to run
            </Badge>
          )
        )}
        {warnings > 0 && <Badge tone="warning">{plural(warnings, 'warning')}</Badge>}
      </span>
      <span className="ml-auto">
        {lastSavedAt ? `Saved ${relativeTime(lastSavedAt)}` : 'Not saved yet'}
      </span>
    </footer>
  )
}
