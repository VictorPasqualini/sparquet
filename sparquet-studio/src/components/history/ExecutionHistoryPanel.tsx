import { Eye, History as HistoryIcon, RefreshCw } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

import { RunDetailDialog } from '@/components/history/RunDetailDialog'
import { StatusIcon, formatTimestamp, statusTone } from '@/components/history/status'
import { Badge, EmptyState, IconButton, SectionTitle, Spinner, Tooltip } from '@/components/ui'
import { isRunnerError } from '@/lib/runner/client'
import { getRun, listRuns } from '@/lib/runner/history'
import { cn } from '@/lib/utils/cn'
import { formatDuration, relativeTime } from '@/lib/utils/format'
import type { ExecutionStatus, JobRunRecord, PipelineRunRecord } from '@/types/history'

/**
 * Reads back what the runner already persisted to its SQLite database — separate
 * from the live `run`/`stageResults` in `useEditorStore`/`usePipelineEditorStore`,
 * which are reset on every `open()`/`close()` and never survive navigating away.
 * This panel is what lets a user come back to a job or pipeline and still see which
 * step failed on a run from before they closed it.
 *
 * The panel itself only ever shows the SHAPE of the history: how the last runs
 * went, how long each took, which one is on the canvas. Everything below a run —
 * its stages, its steps, its logs — lives in `RunDetailDialog`, because a sidebar
 * column this narrow cannot hold it without becoming unreadable.
 */
export interface ExecutionHistoryPanelProps {
  runnerUrl: string
  runnerToken?: string
  workflowId?: string
  jobId?: string
  pipelineId?: string
  /** Bump (e.g. with the new run's id) after a run finishes, to refetch the list. */
  refreshToken?: string
  /**
   * Paints one job execution onto the canvas. Omitted where there is no canvas to
   * paint it on.
   */
  onViewJobRun?: (run: PipelineRunRecord, jobRun: JobRunRecord) => void
  /** What that action is called here — a Job views a run, a Pipeline opens a Job. */
  viewActionLabel?: string
  /** The job execution already on the canvas, so its row can say so. */
  viewingJobRunId?: string | null
  /**
   * Paints a WHOLE execution onto the canvas — the pipeline surface, where one run
   * covers every stage. Takes the run id: a list row carries no `jobs`, so the
   * caller has to fetch the detail anyway.
   */
  onViewRun?: (runId: string) => void
  /** The execution already on the canvas, so its row can say so. */
  viewingRunId?: string | null
  /** What the run-level action is called. */
  runViewActionLabel?: string
}

/** How many runs the strip at the top summarises — enough to read a trend at a glance. */
const STRIP_LENGTH = 14

export function ExecutionHistoryPanel({
  runnerUrl,
  runnerToken,
  workflowId,
  jobId,
  pipelineId,
  refreshToken,
  onViewJobRun,
  viewActionLabel = 'View on canvas',
  viewingJobRunId,
  onViewRun,
  viewingRunId,
  runViewActionLabel = 'View on canvas',
}: ExecutionHistoryPanelProps) {
  const [runs, setRuns] = useState<PipelineRunRecord[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [openRunId, setOpenRunId] = useState<string | null>(null)
  const [reloadKey, setReloadKey] = useState(0)
  const abortRef = useRef<AbortController | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    abortRef.current?.abort()
    abortRef.current = controller
    setLoading(true)
    setError(null)
    listRuns(runnerUrl, { workflowId, jobId, pipelineId, limit: 20 }, controller.signal, runnerToken)
      .then((result) => {
        if (controller.signal.aborted) return
        setRuns(result)
      })
      .catch((err) => {
        if (controller.signal.aborted) return
        setError(isRunnerError(err) ? err.message : 'Failed to load execution history.')
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false)
      })
    return () => controller.abort()
  }, [runnerUrl, runnerToken, workflowId, jobId, pipelineId, refreshToken, reloadKey])

  /**
   * The canvas action, from a list row. A pipeline surface paints the whole run;
   * a job surface paints one job execution, and the list row does not carry it —
   * hence the fetch, which the dialog would otherwise make the user do by hand.
   */
  const viewOnCanvas = (summary: PipelineRunRecord) => {
    if (onViewRun) {
      onViewRun(summary.id)
      return
    }
    if (!onViewJobRun) return
    getRun(runnerUrl, summary.id, undefined, runnerToken)
      .then((detail) => {
        if (!detail) return
        const job =
          (jobId ? detail.jobs.find((candidate) => candidate.jobId === jobId) : undefined) ??
          detail.jobs[0]
        if (job) onViewJobRun(detail, job)
      })
      .catch((err: unknown) => {
        setError(isRunnerError(err) ? err.message : 'Failed to open that execution.')
      })
  }

  const canView = Boolean(onViewRun ?? onViewJobRun)
  const dialog = (
    <RunDetailDialog
      open={openRunId !== null}
      onOpenChange={(next) => {
        if (!next) setOpenRunId(null)
      }}
      runId={openRunId}
      runnerUrl={runnerUrl}
      runnerToken={runnerToken}
      focusJobId={jobId}
      onViewJobRun={
        onViewJobRun
          ? (record, jobRun) => {
              onViewJobRun(record, jobRun)
              setOpenRunId(null)
            }
          : undefined
      }
      viewActionLabel={viewActionLabel}
      viewingJobRunId={viewingJobRunId}
    />
  )

  if (!loading && !error && runs.length === 0) {
    return (
      <section className="space-y-2">
        <SectionTitle>History</SectionTitle>
        <EmptyState
          icon={<HistoryIcon />}
          title="No past executions"
          description="Runs of this job are recorded here once they finish, and stay even after you navigate away."
        />
      </section>
    )
  }

  return (
    <section className="space-y-2">
      <SectionTitle
        action={
          <IconButton
            size="sm"
            label="Refresh history"
            disabled={loading}
            onClick={() => setReloadKey((key) => key + 1)}
          >
            <RefreshCw className={loading ? 'animate-spin' : undefined} />
          </IconButton>
        }
      >
        History
      </SectionTitle>

      {error && <p className="text-2xs text-state-danger">{error}</p>}

      {loading && runs.length === 0 && (
        <div className="flex items-center gap-2 text-2xs text-content-subtle">
          <Spinner className="h-3.5 w-3.5" /> Loading history…
        </div>
      )}

      {runs.length > 1 && <RunStrip runs={runs} onOpen={setOpenRunId} openRunId={openRunId} />}

      <div className="card divide-y divide-line overflow-hidden p-0">
        {runs.map((run) => (
          <RunRow
            key={run.id}
            run={run}
            onOpen={() => setOpenRunId(run.id)}
            onView={canView ? () => viewOnCanvas(run) : undefined}
            viewActionLabel={onViewRun ? runViewActionLabel : viewActionLabel}
            onCanvas={run.id === viewingRunId}
          />
        ))}
      </div>

      {dialog}
    </section>
  )
}

const STRIP_TONE: Record<ExecutionStatus, string> = {
  success: 'bg-state-success',
  failed: 'bg-state-danger',
  cancelled: 'bg-state-warning',
  skipped: 'bg-content-subtle/40',
  running: 'bg-state-info animate-pulse',
  pending: 'bg-line',
}

/**
 * The last runs as bars, oldest on the left — how healthy this job has been, and
 * how its duration is trending, in one glance and no scrolling. Bar height is
 * relative to the longest run shown, so a slow run stands out before it is read.
 */
function RunStrip({
  runs,
  onOpen,
  openRunId,
}: {
  runs: PipelineRunRecord[]
  onOpen: (runId: string) => void
  openRunId: string | null
}) {
  const shown = runs.slice(0, STRIP_LENGTH).reverse()
  const longest = Math.max(1, ...shown.map((run) => run.durationMs ?? 0))

  return (
    <div className="flex h-10 items-end gap-1 rounded-lg border border-line bg-surface-sunken/50 px-2 py-1.5">
      {shown.map((run) => (
        <Tooltip
          key={run.id}
          content={`${run.name ?? run.id} · ${run.status} · ${relativeStarted(run.startedAt)} · ${formatDuration(run.durationMs ?? undefined)}`}
        >
          <button
            type="button"
            aria-label={`Open the run from ${relativeStarted(run.startedAt)}`}
            onClick={() => onOpen(run.id)}
            style={{ height: `${25 + 75 * ((run.durationMs ?? 0) / longest)}%` }}
            className={cn(
              'min-w-0 flex-1 rounded-sm transition-opacity hover:opacity-100',
              STRIP_TONE[run.status],
              openRunId === run.id ? 'opacity-100 ring-1 ring-brand-500' : 'opacity-70',
            )}
          />
        </Tooltip>
      ))}
    </div>
  )
}

/**
 * One execution as a row: how it ended, when, how long it took — and, when it
 * failed, the first line of why. The rest is one click away in the dialog.
 */
function RunRow({
  run,
  onOpen,
  onView,
  viewActionLabel,
  onCanvas,
}: {
  run: PipelineRunRecord
  onOpen: () => void
  onView?: () => void
  viewActionLabel: string
  onCanvas: boolean
}) {
  return (
    // The canvas action is a sibling of the row button, not a child: a button
    // inside a button is invalid, and the row must stay openable by keyboard.
    <div className="flex items-stretch hover:bg-surface-sunken">
      <button
        type="button"
        onClick={onOpen}
        title="Open this execution"
        className="flex min-w-0 flex-1 flex-col gap-0.5 px-3 py-2 text-left"
      >
        <span className="flex items-center gap-2 text-xs">
          <StatusIcon status={run.status} />
          <span className="min-w-0 flex-1 truncate text-content">{run.name ?? run.id}</span>
          {/* A Pipeline run reached this Job as one of its stages. */}
          {run.kind === 'pipeline' && <Badge tone="neutral">pipeline</Badge>}
          {onCanvas && <Badge tone="brand">on canvas</Badge>}
          <span className="shrink-0 text-2xs text-content-subtle">
            {formatDuration(run.durationMs ?? undefined)}
          </span>
        </span>
        <span className="flex items-center gap-2 pl-6 text-2xs text-content-subtle">
          <span className="shrink-0" title={formatTimestamp(run.startedAt)}>
            {relativeStarted(run.startedAt)}
          </span>
          {run.error ? (
            // One line only: the full message is in the dialog, in a card that
            // scrolls instead of pushing the whole panel down.
            <span
              className={cn(
                'min-w-0 flex-1 truncate',
                run.status === 'cancelled' ? 'text-state-warning' : 'text-state-danger',
              )}
              title={run.error}
            >
              {run.error}
            </span>
          ) : (
            <Badge tone={statusTone(run.status)}>{run.status}</Badge>
          )}
        </span>
      </button>
      {onView && (
        <span className="flex shrink-0 items-center pr-2">
          <Tooltip content={onCanvas ? 'Already on the canvas' : viewActionLabel}>
            <IconButton size="sm" label={viewActionLabel} onClick={onView}>
              <Eye />
            </IconButton>
          </Tooltip>
        </span>
      )}
    </div>
  )
}

/** `3 min ago`, `yesterday`, `12 Mar` — the exact clock time stays in the tooltip. */
function relativeStarted(iso: string | null): string {
  if (!iso) return '—'
  const parsed = Date.parse(iso)
  return Number.isNaN(parsed) ? iso : relativeTime(parsed)
}
