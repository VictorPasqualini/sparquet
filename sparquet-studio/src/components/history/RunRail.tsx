/**
 * The last executions of the open pipeline, as a strip above the canvas.
 *
 * Opening a pipeline used to say nothing about how it has been doing: the runs
 * were one tab away, so "is this healthy" cost a click, a list and a read. The
 * strip answers that question without taking the canvas — one square per run,
 * newest on the right, so a wall of green and one red square are different
 * pictures before anything is read.
 *
 * Deliberately NOT a canvas painted with the last run on open. The canvas is
 * where the pipeline is edited, and an old execution drawn over it is state
 * nobody asked for; `PipelineRunViewBanner` documents that choice. The strip
 * offers the run instead — clicking a square is what pins it onto the stages.
 *
 * Width carries duration, so a run that took ten times longer is ten times wider
 * and a slow night is visible without reading a single number. Colour carries
 * the outcome. Between them a month of behaviour fits in one line of pixels.
 *
 * The Runs tab keeps its job: filtering, logs, and drilling into one stage. This
 * answers "is it healthy", that one answers "what happened on the 12th".
 */

import { ChevronDown, History as HistoryIcon, Play } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'

import { StatusIcon, formatTimestamp } from '@/components/history/status'
import { IconButton, Spinner, Tooltip } from '@/components/ui'
import { listRuns } from '@/lib/runner/history'
import { formatDuration } from '@/lib/utils/format'
import { cn } from '@/lib/utils/cn'
import type { ExecutionStatus, PipelineRunRecord } from '@/types/history'

/** How many squares fit before the strip stops being scannable. */
const VISIBLE = 20

/** Narrowest a square may be drawn: below this it stops reading as a square. */
const MIN_WIDTH = 6
const MAX_WIDTH = 34

const TONES: Record<ExecutionStatus, string> = {
  success: 'bg-state-success',
  failed: 'bg-state-danger',
  running: 'bg-brand-500 animate-pulse',
  pending: 'bg-content-subtle/40',
  skipped: 'bg-content-subtle/40',
  cancelled: 'bg-state-warning',
}

/**
 * Square width from duration, on a square-root scale.
 *
 * Linear would let one stuck run flatten every other square to a sliver, and a
 * flat strip says nothing. The root keeps a 10x slower run visibly wider without
 * letting a 1000x outlier own the row.
 */
function widthOf(durationMs: number | null, slowest: number): number {
  if (durationMs === null || slowest <= 0) return MIN_WIDTH + 4
  const share = Math.sqrt(Math.max(durationMs, 1) / slowest)
  return Math.round(MIN_WIDTH + share * (MAX_WIDTH - MIN_WIDTH))
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

export interface RunRailProps {
  runnerUrl: string
  runnerToken?: string
  workflowId: string | null
  pipelineId: string
  /** Changes when a run finishes here, which is when the strip should reload. */
  refreshToken?: string | null
  /** The run currently painted on the stages, highlighted in the strip. */
  viewingRunId?: string | null
  /** Pins the picked execution onto the stage boxes. */
  onSelect: (runId: string) => void
  /** Opens the Runs tab, for the questions a strip cannot answer. */
  onOpenHistory: () => void
  collapsed: boolean
  onCollapsedChange: (collapsed: boolean) => void
}

export function RunRail({
  runnerUrl,
  runnerToken,
  workflowId,
  pipelineId,
  refreshToken,
  viewingRunId,
  onSelect,
  onOpenHistory,
  collapsed,
  onCollapsedChange,
}: RunRailProps) {
  const [runs, setRuns] = useState<PipelineRunRecord[]>([])
  const [loading, setLoading] = useState(true)
  const [failure, setFailure] = useState('')

  const load = useCallback(
    async (signal: AbortSignal) => {
      setLoading(true)
      try {
        const found = await listRuns(
          runnerUrl,
          { workflowId: workflowId ?? undefined, pipelineId, limit: VISIBLE },
          signal,
          runnerToken,
        )
        if (signal.aborted) return
        setRuns(found)
        setFailure('')
      } catch (error) {
        if (signal.aborted) return
        // An unreachable runner is the normal state of a browser-only Studio, so
        // the strip goes quiet rather than shouting an error over the canvas.
        setFailure(messageOf(error))
        setRuns([])
      } finally {
        if (!signal.aborted) setLoading(false)
      }
    },
    [pipelineId, runnerToken, runnerUrl, workflowId],
  )

  useEffect(() => {
    if (collapsed) return
    const controller = new AbortController()
    void load(controller.signal)
    return () => controller.abort()
  }, [collapsed, load, refreshToken])

  // Oldest on the left: time runs the way it is read, so the right-hand end is
  // always "now" and the eye lands there first.
  const ordered = useMemo(() => [...runs].reverse(), [runs])
  const slowest = useMemo(
    () => ordered.reduce((most, run) => Math.max(most, run.durationMs ?? 0), 0),
    [ordered],
  )
  const latest = runs[0]

  if (collapsed) {
    return (
      <div className="flex items-center gap-2 border-b border-line bg-surface px-3 py-1">
        <button
          type="button"
          onClick={() => onCollapsedChange(false)}
          className="flex items-center gap-1.5 text-2xs text-content-muted hover:text-content"
        >
          <HistoryIcon className="h-3 w-3" aria-hidden />
          Show recent runs
        </button>
      </div>
    )
  }

  return (
    <div className="flex min-h-[2.25rem] items-center gap-3 border-b border-line bg-surface px-3 py-1">
      <span className="shrink-0 text-2xs uppercase tracking-wide text-content-subtle">
        Recent
      </span>

      {loading && runs.length === 0 ? (
        <Spinner className="h-3 w-3" />
      ) : failure ? (
        <span className="min-w-0 flex-1 truncate text-2xs text-content-subtle" title={failure}>
          No run history — the runner is not answering.
        </span>
      ) : ordered.length === 0 ? (
        <span className="flex min-w-0 flex-1 items-center gap-1.5 text-2xs text-content-subtle">
          <Play className="h-3 w-3" aria-hidden />
          Never run. The first execution shows up here.
        </span>
      ) : (
        <>
          <div className="flex min-w-0 flex-1 items-end gap-[3px] overflow-hidden">
            {ordered.map((run) => (
              <Tooltip
                key={run.id}
                content={
                  <span className="block space-y-0.5">
                    <span className="block">
                      {run.status} · {formatTimestamp(run.startedAt)}
                    </span>
                    <span className="block text-content-subtle">
                      {formatDuration(run.durationMs ?? undefined)}
                      {run.runAs ? ` · ${run.runAs}` : ''}
                      {run.launched ? ` · ${run.launched}` : ''}
                    </span>
                    {run.error ? (
                      <span className="block max-w-[18rem] text-state-danger">{run.error}</span>
                    ) : null}
                  </span>
                }
              >
                <button
                  type="button"
                  aria-label={`Show run from ${formatTimestamp(run.startedAt)} on the canvas`}
                  onClick={() => onSelect(run.id)}
                  style={{ width: `${widthOf(run.durationMs, slowest)}px` }}
                  className={cn(
                    'h-4 shrink-0 rounded-[3px] transition-transform hover:scale-y-125',
                    TONES[run.status] ?? TONES.pending,
                    run.id === viewingRunId &&
                      'ring-2 ring-content ring-offset-1 ring-offset-surface',
                  )}
                />
              </Tooltip>
            ))}
          </div>

          {latest ? (
            <span className="flex shrink-0 items-center gap-1.5 text-2xs text-content-subtle">
              <StatusIcon status={latest.status} className="h-3 w-3" />
              <span className="hidden sm:inline">{formatTimestamp(latest.startedAt)}</span>
              {latest.durationMs !== null && (
                <span className="tabular-nums">{formatDuration(latest.durationMs)}</span>
              )}
            </span>
          ) : null}
        </>
      )}

      <button
        type="button"
        onClick={onOpenHistory}
        className="shrink-0 text-2xs text-content-muted underline-offset-2 hover:text-content hover:underline"
      >
        All runs
      </button>

      <IconButton size="sm" label="Hide recent runs" onClick={() => onCollapsedChange(true)}>
        <ChevronDown />
      </IconButton>
    </div>
  )
}
