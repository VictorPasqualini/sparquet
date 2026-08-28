/**
 * The execution history of one Job or one Pipeline, as a table in the middle of
 * the screen.
 *
 * Opening a Job or a Pipeline shows the flow as it stands — never an old run
 * painted over it. The executions live here instead, one row each, and drilling
 * in is explicit: `Job → run id`, `Pipeline → run id`. That is also why the run id
 * is a column and not a tooltip; it is the address a person quotes in a ticket.
 *
 * Clicking the id paints that run onto the canvas — every step carrying the status
 * it ended with. The numbers behind it (timestamps, lineage, logs) are one further
 * click away, in the dialog the action column opens.
 *
 * Twin of `ExecutionHistoryPanel`, which says the same thing in a 380px column and
 * therefore can only afford the shape of the history. Here there is room for who
 * launched it, as whom, and for how long.
 */

import { History as HistoryIcon, PanelRight, RefreshCw } from 'lucide-react'
import { useEffect, useRef, useState, type ReactNode } from 'react'

import { RunDetailDialog } from '@/components/history/RunDetailDialog'
import { StatusIcon, formatTimestamp, statusTone } from '@/components/history/status'
import { Badge, Button, EmptyState, ErrorCard, Spinner, Tooltip } from '@/components/ui'
import { isRunnerError } from '@/lib/runner/client'
import { getRun, listRuns } from '@/lib/runner/history'
import { cn } from '@/lib/utils/cn'
import { formatDuration, plural, relativeTime } from '@/lib/utils/format'
import type { JobRunRecord, PipelineRunRecord } from '@/types/history'

/** How many executions the table holds before the user has to narrow the question. */
const PAGE_SIZE = 50

const LAUNCH_LABELS: Record<string, string> = {
  manual: 'Manually',
  scheduled: 'Scheduled',
  api: 'API',
}

export interface RunsBrowserProps {
  runnerUrl: string
  runnerToken?: string
  workflowId?: string
  jobId?: string
  pipelineId?: string
  /** Bump after a run finishes to refetch the table. */
  refreshToken?: string
  /** The Job or Pipeline these runs belong to — the left half of the breadcrumb. */
  subject: string
  /** Paints one job execution onto a job canvas. */
  onViewJobRun?: (run: PipelineRunRecord, jobRun: JobRunRecord) => void
  /** Paints a whole execution onto a pipeline canvas. */
  onViewRun?: (runId: string) => void
  viewActionLabel?: string
  /** The execution already on the canvas, so its row can say so. */
  viewingRunId?: string | null
  viewingJobRunId?: string | null
}

export function RunsBrowser({
  runnerUrl,
  runnerToken,
  workflowId,
  jobId,
  pipelineId,
  refreshToken,
  subject,
  onViewJobRun,
  onViewRun,
  viewActionLabel = 'View on canvas',
  viewingRunId,
  viewingJobRunId,
}: RunsBrowserProps) {
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
    listRuns(
      runnerUrl,
      { workflowId, jobId, pipelineId, limit: PAGE_SIZE },
      controller.signal,
      runnerToken,
    )
      .then((result) => {
        if (controller.signal.aborted) return
        setRuns(result)
      })
      .catch((err: unknown) => {
        if (controller.signal.aborted) return
        setError(isRunnerError(err) ? err.message : 'Failed to load execution history.')
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false)
      })
    return () => controller.abort()
  }, [runnerUrl, runnerToken, workflowId, jobId, pipelineId, refreshToken, reloadKey])

  /**
   * The canvas action from a row. A pipeline surface paints the whole run; a job
   * surface paints one job execution, which the list row does not carry — hence
   * the fetch, rather than making the user open the dialog to get there.
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

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex shrink-0 items-center gap-2 border-b border-line px-4 py-2.5">
        <div className="min-w-0">
          <p className="truncate text-xs font-medium text-content">{subject}</p>
          <p className="text-2xs text-content-subtle">
            {loading && runs.length === 0
              ? 'Loading executions…'
              : `${plural(runs.length, 'execution')}, most recent first`}
          </p>
        </div>
        <Button
          size="xs"
          variant="ghost"
          className="ml-auto"
          disabled={loading}
          icon={<RefreshCw className={cn('h-3 w-3', loading && 'animate-spin')} />}
          onClick={() => setReloadKey((key) => key + 1)}
        >
          Refresh
        </Button>
      </div>

      {error && (
        <div className="shrink-0 px-4 pt-3">
          <ErrorCard message={error} size="sm" />
        </div>
      )}

      {loading && runs.length === 0 && (
        <div className="flex flex-1 items-center justify-center">
          <Spinner className="h-5 w-5" />
        </div>
      )}

      {!loading && runs.length === 0 && !error && (
        <div className="flex flex-1 items-center justify-center p-6">
          <EmptyState
            icon={<HistoryIcon />}
            title="No executions yet"
            description="Every run is recorded here once it starts, and stays after you navigate away. Run this from the Run panel to get the first one."
          />
        </div>
      )}

      {runs.length > 0 && (
        <div className="scroll-area min-h-0 flex-1">
          <table className="w-full border-collapse text-2xs">
            <thead className="sticky top-0 z-10 bg-surface">
              <tr className="border-b border-line text-left text-content-subtle">
                <Th className="w-40">Status</Th>
                <Th>Run id</Th>
                <Th className="w-44">Started</Th>
                <Th className="w-24">Duration</Th>
                <Th className="w-36">Run as</Th>
                <Th className="w-28">Launched</Th>
                {canView && <Th className="w-10" />}
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <RunRow
                  key={run.id}
                  run={run}
                  // The id goes to the canvas: what a person wants from a run id is
                  // to SEE that run, each step carrying the status it ended with.
                  // The dialog is one click further, in the action column.
                  onOpen={canView ? () => viewOnCanvas(run) : () => setOpenRunId(run.id)}
                  openLabel={canView ? viewActionLabel : 'Open the execution details'}
                  onDetails={() => setOpenRunId(run.id)}
                  showDetailsAction={canView}
                  onCanvas={run.id === viewingRunId}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}

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
    </div>
  )
}

function Th({ children, className }: { children?: ReactNode; className?: string }) {
  return (
    <th scope="col" className={cn('px-4 py-2 font-medium', className)}>
      {children}
    </th>
  )
}

function RunRow({
  run,
  onOpen,
  openLabel,
  onDetails,
  showDetailsAction,
  onCanvas,
}: {
  run: PipelineRunRecord
  onOpen: () => void
  openLabel: string
  onDetails: () => void
  showDetailsAction: boolean
  onCanvas: boolean
}) {
  return (
    <tr className="border-b border-line/60 align-middle hover:bg-surface-sunken/60">
      <td className="px-4 py-2">
        <span className="flex items-center gap-1.5">
          <StatusIcon status={run.status} className="h-3.5 w-3.5" />
          <Badge tone={statusTone(run.status)}>{run.status}</Badge>
          {run.kind === 'pipeline' && <Badge tone="neutral">pipeline</Badge>}
        </span>
      </td>
      <td className="min-w-0 px-4 py-2">
        {/* The drill-down: the id IS the link, because it is what a person quotes. */}
        <button
          type="button"
          onClick={onOpen}
          title={`${openLabel} — ${run.id}`}
          className="max-w-full truncate font-mono text-brand-600 hover:underline dark:text-brand-400"
        >
          {run.id}
        </button>
        {onCanvas && <Badge tone="brand" className="ml-2">on canvas</Badge>}
        {run.error && (
          <p
            className={cn(
              'mt-0.5 truncate',
              run.status === 'cancelled' ? 'text-state-warning' : 'text-state-danger',
            )}
            title={run.error}
          >
            {run.error}
          </p>
        )}
      </td>
      <td className="px-4 py-2 text-content-muted" title={formatTimestamp(run.startedAt)}>
        {relativeStarted(run.startedAt)}
      </td>
      <td className="px-4 py-2 text-content-muted">{formatDuration(run.durationMs ?? undefined)}</td>
      <td className="px-4 py-2 text-content-muted">{run.runAs ?? '—'}</td>
      <td className="px-4 py-2 text-content-muted">
        {run.launched ? (LAUNCH_LABELS[run.launched] ?? run.launched) : '—'}
      </td>
      {showDetailsAction && (
        <td className="px-2 py-2">
          <Tooltip content="Execution details">
            <Button
              size="xs"
              variant="ghost"
              onClick={onDetails}
              icon={<PanelRight className="h-3 w-3" />}
            >
              <span className="sr-only">Execution details</span>
            </Button>
          </Tooltip>
        </td>
      )}
    </tr>
  )
}

/** `3 min ago`, `yesterday`, `12 Mar` — the exact clock time stays in the tooltip. */
function relativeStarted(iso: string | null): string {
  if (!iso) return '—'
  const parsed = Date.parse(iso)
  return Number.isNaN(parsed) ? iso : relativeTime(parsed)
}
