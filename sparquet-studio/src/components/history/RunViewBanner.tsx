/**
 * The canvas caption for a past execution, and the loader that puts one there.
 *
 * Opening a Job used to leave every box blank: the live statuses are dropped on
 * `close()`, and nothing read the runner's history back. So this mounts over the
 * canvas, loads the Job's most recent execution (or the exact one a Pipeline stage
 * navigated to) and hands it to `showRunView`, which paints each box with the
 * status its step ended with. The banner then says WHICH run the canvas is
 * describing — without it, a red box would read as a problem with the graph as it
 * stands rather than the outcome of a run from an hour ago.
 */

import { AlertTriangle, History as HistoryIcon, X } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import { useLocation } from 'react-router-dom'

import { RunDetailDialog } from '@/components/history/RunDetailDialog'
import { StatusIcon, formatTimestamp } from '@/components/history/status'
import { Badge, IconButton } from '@/components/ui'
import { resolveRunView } from '@/lib/runner/runView'
import type { RunViewRequest } from '@/lib/runner/runView'
import { formatDuration, plural } from '@/lib/utils/format'
import { useEditorStore } from '@/store/editor'
import { useSettingsStore } from '@/store/settings'

export type { RunViewRequest }

export function RunViewBanner() {
  useJobRunView()

  const runView = useEditorStore((state) => state.runView)
  const clearRunView = useEditorStore((state) => state.clearRunView)
  const showRunView = useEditorStore((state) => state.showRunView)
  const running = useEditorStore((state) => state.running)
  const jobId = useEditorStore((state) => state.job?.id ?? null)
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const [detailOpen, setDetailOpen] = useState(false)

  // A run in flight paints the boxes itself, and the Run panel already narrates it.
  if (!runView || running) return null

  return (
    <div className="pointer-events-none absolute inset-x-0 top-0 z-10 flex justify-center p-2">
      <div className="pointer-events-auto flex max-w-full items-center gap-2 rounded-lg border border-line bg-surface/95 px-2.5 py-1.5 text-2xs shadow-md backdrop-blur">
        <HistoryIcon className="h-3.5 w-3.5 shrink-0 text-content-subtle" aria-hidden />
        <span className="shrink-0 text-content-muted">
          {runView.pinned ? 'Viewing run' : 'Last run'}
        </span>
        <StatusIcon status={runView.status} className="h-3.5 w-3.5" />
        <span className="shrink-0 text-content">{runView.status}</span>
        <span className="shrink-0 text-content-subtle">{formatTimestamp(runView.startedAt)}</span>
        {runView.durationMs !== null && (
          <span className="shrink-0 text-content-subtle">
            {formatDuration(runView.durationMs)}
          </span>
        )}
        {/* Ran as one stage of a Studio Pipeline, not on its own. */}
        {runView.kind === 'pipeline' && (
          <Badge tone="neutral">{runView.runName ? `via ${runView.runName}` : 'pipeline'}</Badge>
        )}
        {runView.unmatchedSteps > 0 && (
          <Badge
            tone="warning"
            icon={<AlertTriangle className="h-3 w-3" />}
            className="shrink-0"
          >
            {`${plural(runView.unmatchedSteps, 'step')} not on this canvas`}
          </Badge>
        )}
        {/* Opens the execution itself, not the Run panel: that panel reports the run
            THIS tab launched, so on a run loaded from the history it had nothing to
            show and the button read as broken. This is the same dialog the Runs tab
            opens, on the same run the boxes are painted with. */}
        <button
          type="button"
          onClick={() => setDetailOpen(true)}
          className="shrink-0 rounded px-1 text-brand-600 transition-colors hover:bg-surface-sunken dark:text-brand-400"
        >
          Details
        </button>
        <IconButton size="sm" label="Stop showing this run" onClick={clearRunView}>
          <X />
        </IconButton>
      </div>

      <RunDetailDialog
        open={detailOpen}
        onOpenChange={setDetailOpen}
        runId={runView.runId}
        runnerUrl={runnerUrl}
        runnerToken={runnerToken}
        // A pipeline run has one stage per job: preselect the one this canvas is.
        focusJobId={jobId ?? undefined}
        viewingJobRunId={runView.jobRunId}
        // Picking another stage of the same run repaints the canvas with it, so the
        // banner keeps describing what the boxes show.
        onViewJobRun={(record, jobRun) => {
          showRunView(record, jobRun, { pinned: true })
          setDetailOpen(false)
        }}
      />
    </div>
  )
}

/**
 * Keeps the canvas showing an execution of the Job that is open.
 *
 * Loads on two occasions: something asks for one specific run — a Pipeline stage,
 * or a row in the Runs tab — and a live run ends (the persisted rows carry the
 * rows/paths and the failed step that the stream does not). Opening the Job loads
 * nothing: the canvas is the graph as it stands, and its executions are a list
 * you drill into, `Job → Run id`, not something pinned on top of the editor.
 *
 * The runner is optional, so every failure here is silent: the Run panel is where
 * a connection problem belongs.
 */
function useJobRunView(): void {
  const jobId = useEditorStore((state) => state.job?.id ?? null)
  const running = useEditorStore((state) => state.running)
  const showRunView = useEditorStore((state) => state.showRunView)
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const location = useLocation()

  // Read once: a navigation state object is a fresh identity on every render, and
  // the request is consumed by the first load that honours it.
  const requested = useRef<RunViewRequest | null>(
    (location.state as RunViewRequest | null)?.runId !== undefined
      ? (location.state as RunViewRequest)
      : null,
  )
  const wasRunning = useRef(running)

  useEffect(() => {
    // While a run is streaming, the canvas belongs to it.
    if (!jobId || running) {
      wasRunning.current = running
      return
    }
    const justFinished = wasRunning.current
    wasRunning.current = running
    // A run the user chose stays until they leave it, unless a run just ended —
    // then the canvas is about to describe that one anyway.
    if (!justFinished && useEditorStore.getState().runView?.pinned === true) return
    // Nothing asked for, nothing just ended: opening a Job shows the Job.
    if (!justFinished && requested.current === null) return

    const controller = new AbortController()
    let cancelled = false

    void (async () => {
      try {
        // Read, but do not consume: an effect that is torn down before it finishes
        // — React runs every effect twice in development, and a token or URL change
        // restarts this one — must leave the request for the pass that replaces it.
        const resolved = await resolveRunView({
          jobId,
          request: requested.current,
          runnerUrl,
          runnerToken,
          signal: controller.signal,
          // Only a run that just ended may be found by "the most recent one".
          allowLatest: justFinished,
        })
        if (cancelled || !resolved) return
        // The editor may have moved on to another job while this was in flight.
        if (useEditorStore.getState().job?.id !== jobId) return
        showRunView(resolved.run, resolved.jobRun, { pinned: resolved.pinned })
        // Honoured: from here on this Job loads its own latest run like any other.
        requested.current = null
      } catch {
        // No runner, no history. Nothing to report here.
      }
    })()

    return () => {
      cancelled = true
      controller.abort()
    }
  }, [jobId, running, runnerUrl, runnerToken, showRunView])
}
