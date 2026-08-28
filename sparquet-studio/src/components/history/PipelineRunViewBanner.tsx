/**
 * The pipeline canvas caption for a past execution, and the loader that puts one
 * there.
 *
 * Twin of `RunViewBanner`, one level up: that one paints the steps of a job onto
 * the job canvas, this one paints the job executions of a pipeline run onto the
 * stage boxes. Same reason for existing — opening a pipeline used to leave every
 * stage blank, and a coloured box with no caption cannot say WHICH run it is
 * reporting.
 *
 * It is also what makes the drill-down work: the run view records a `job_run` id
 * per stage, so opening a stage lands on that job's canvas showing the very
 * execution the stage box is coloured by.
 */

import { AlertTriangle, History as HistoryIcon, X } from 'lucide-react'
import { useEffect, useRef } from 'react'

import { StatusIcon, formatTimestamp } from '@/components/history/status'
import { Badge, IconButton } from '@/components/ui'
import { getRun, listRuns } from '@/lib/runner/history'
import { formatDuration, plural } from '@/lib/utils/format'
import { usePipelineEditorStore } from '@/store/pipelineEditor'
import { useSettingsStore } from '@/store/settings'

export function PipelineRunViewBanner() {
  usePipelineRunView()

  const runView = usePipelineEditorStore((state) => state.runView)
  const clearRunView = usePipelineEditorStore((state) => state.clearRunView)
  const running = usePipelineEditorStore((state) => state.running)

  // A run in flight paints the boxes itself, and the run panel narrates it.
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
        <span className="hidden shrink-0 text-content-subtle sm:inline">
          · open a stage to see its steps
        </span>
        {runView.unmatchedJobs > 0 && (
          <Badge tone="warning" icon={<AlertTriangle className="h-3 w-3" />} className="shrink-0">
            {`${plural(runView.unmatchedJobs, 'stage')} not on this canvas`}
          </Badge>
        )}
        <IconButton size="sm" label="Stop showing this run" onClick={clearRunView}>
          <X />
        </IconButton>
      </div>
    </div>
  )
}

/**
 * Loads a persisted execution and paints it, pinned — what the history list calls
 * when a run is picked by hand.
 *
 * Lives here rather than in the panel because the panel's list rows carry no
 * `jobs`: only `GET /runs/{id}` does, and without them there is nothing to map
 * onto the stages.
 */
export async function showPipelineRun(
  runId: string,
  runnerUrl: string,
  runnerToken?: string,
): Promise<void> {
  const run = await getRun(runnerUrl, runId, undefined, runnerToken)
  if (run) usePipelineEditorStore.getState().showRunView(run, { pinned: true })
}

/**
 * Keeps the stage boxes showing an execution of the pipeline that is open.
 *
 * Loads on one occasion only: a live run ends, where the persisted rows carry the
 * `job_run` ids the stream never sends. Opening a pipeline loads nothing — the
 * canvas is the flow as it stands, and its executions live in the Runs tab, which
 * is where one is picked (`Pipeline → Run id`) and pinned here by hand.
 *
 * The runner is optional, so every failure here is silent: a connection problem
 * belongs in the run panel, not on the canvas.
 */
function usePipelineRunView(): void {
  const pipelineId = usePipelineEditorStore((state) => state.pipeline?.id ?? null)
  const running = usePipelineEditorStore((state) => state.running)
  const showRunView = usePipelineEditorStore((state) => state.showRunView)
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)

  const wasRunning = useRef(running)

  useEffect(() => {
    // While a run is streaming, the canvas belongs to it.
    if (!pipelineId || running) {
      wasRunning.current = running
      return
    }
    const justFinished = wasRunning.current
    wasRunning.current = running
    // Opening a pipeline shows the pipeline; only a run that just ended paints itself.
    if (!justFinished) return

    const controller = new AbortController()
    let cancelled = false

    void (async () => {
      try {
        const recent = await listRuns(
          runnerUrl,
          { pipelineId, limit: 1 },
          controller.signal,
          runnerToken,
        )
        const runId = recent[0]?.id
        if (runId === undefined) return
        const run = await getRun(runnerUrl, runId, controller.signal, runnerToken)
        if (cancelled || !run) return
        // The editor may have moved on to another pipeline while this was in flight.
        if (usePipelineEditorStore.getState().pipeline?.id !== pipelineId) return
        showRunView(run)
      } catch {
        // No runner, no history. Nothing to report here.
      }
    })()

    return () => {
      cancelled = true
      controller.abort()
    }
  }, [pipelineId, running, runnerUrl, runnerToken, showRunView])
}
