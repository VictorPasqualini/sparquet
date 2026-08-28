/**
 * Run panel of a pipeline: one button, one stage list, one log stream.
 *
 * It speaks to `POST /run/flow/stream`, which executes the stages in the order it
 * is given and reports each one as it starts and finishes. Progress lands on the
 * canvas too (`StageNode`), so this panel carries the numbers — rows, duration,
 * errors — and the log lines, each labelled with the stage that emitted it.
 */

import {
  ChevronRight,
  CircleCheck,
  CircleSlash,
  CircleStop,
  CircleX,
  ListOrdered,
  Play,
  RefreshCw,
  Rocket,
  Square,
  Table2,
  Terminal,
  TriangleAlert,
  type LucideIcon,
} from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import { stepLook } from '@/components/canvas/stepLook'
import { ExecutionHistoryPanel } from '@/components/history/ExecutionHistoryPanel'
import { showPipelineRun } from '@/components/history/PipelineRunViewBanner'
import { RunResultTable } from '@/components/panels/RunResultTable'
import {
  Badge,
  Button,
  EmptyState,
  ErrorCard,
  SectionTitle,
  Spinner,
  Tooltip,
} from '@/components/ui'
import { isErrorText, sameErrorText } from '@/lib/runner/errorText'
import { planPipelineRun, type ResolvedPipeline } from '@/lib/pipeline'
import {
  cancelRun,
  checkRunnerHealth,
  isRunnerError,
  RUNNER_START_COMMAND,
  runPipelineStream,
} from '@/lib/runner/client'
import { cn } from '@/lib/utils/cn'
import { formatClockTime, formatCount, formatDuration, plural } from '@/lib/utils/format'
import { usePipelineEditorStore } from '@/store/pipelineEditor'
import { useSettingsStore } from '@/store/settings'
import type { PipelineStageResult, RunLogLine, StepStatus } from '@/types/studio'

/** Preview rows requested for the LAST stage; the table caps at 50 too. */
const PREVIEW_ROWS = 50

type RunnerStatus = 'checking' | 'connected' | 'offline'

export function PipelineRunPanel({ resolved }: { resolved: ResolvedPipeline }) {
  const navigate = useNavigate()

  const pipeline = usePipelineEditorStore((state) => state.pipeline)
  const running = usePipelineEditorStore((state) => state.running)
  const run = usePipelineEditorStore((state) => state.run)
  const logs = usePipelineEditorStore((state) => state.logs)
  const stageStatus = usePipelineEditorStore((state) => state.stageStatus)
  const stageResults = usePipelineEditorStore((state) => state.stageResults)
  const startRun = usePipelineEditorStore((state) => state.startRun)
  const appendLog = usePipelineEditorStore((state) => state.appendLog)
  const markStage = usePipelineEditorStore((state) => state.markStage)
  const setStageResult = usePipelineEditorStore((state) => state.setStageResult)
  const finishRun = usePipelineEditorStore((state) => state.finishRun)
  const runView = usePipelineEditorStore((state) => state.runView)

  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const runAs = useSettingsStore((state) => state.runAs)

  const [runner, setRunner] = useState<RunnerStatus>('checking')
  const [runnerNote, setRunnerNote] = useState<string | null>(null)
  const [stopping, setStopping] = useState(false)
  const abort = useRef<AbortController | null>(null)
  const healthAbort = useRef<AbortController | null>(null)
  /** The execution id the runner opened for this run — what Stop cancels. */
  const liveRunId = useRef<string | null>(null)

  const checkHealth = useCallback(async (url: string) => {
    healthAbort.current?.abort()
    const controller = new AbortController()
    healthAbort.current = controller
    setRunner('checking')
    try {
      await checkRunnerHealth(url, controller.signal)
      if (controller.signal.aborted) return
      setRunner('connected')
      setRunnerNote(null)
    } catch (error) {
      if (controller.signal.aborted) return
      setRunner('offline')
      setRunnerNote(isUnreachable(error) ? null : messageOf(error))
    }
  }, [])

  useEffect(() => {
    // Read the URL imperatively: typing a new address elsewhere must not fire a
    // request per keystroke.
    void checkHealth(useSettingsStore.getState().runnerUrl)
    return () => healthAbort.current?.abort()
  }, [checkHealth])

  const plan = useMemo(() => planPipelineRun(resolved), [resolved])

  const stageLabel = useMemo(() => {
    const labels = new Map<string, string>()
    for (const stage of resolved.stages) labels.set(stage.id, `${stage.order}. ${stage.name}`)
    return labels
  }, [resolved.stages])

  const disabledReason = running
    ? 'A run is already in progress'
    : plan.blockers.length > 0
      ? plan.blockers[0].message
      : null

  const execute = async () => {
    if (plan.stages.length === 0) return

    const controller = new AbortController()
    abort.current = controller
    startRun(plan.stages.map((stage) => stage.id))
    const startedAt = performance.now()

    try {
      await runPipelineStream(
        runnerUrl,
        {
          stages: plan.stages,
          limit: PREVIEW_ROWS,
          stopOnError: true,
          workflowId: pipeline?.workflowId,
          pipelineId: pipeline?.id,
          name: pipeline?.name,
          runAs: runAs || undefined,
          launched: 'manual',
        },
        {
          onStart: (start) => {
            // The run exists on the runner from here on, so Stop can cancel it.
            liveRunId.current = start.runId ?? null
            appendLog(line('info', `Running ${plural(start.total, 'stage')} in sequence`))
          },
          onStageStart: (stage) => markStage(stage.id, 'running'),
          onLog: appendLog,
          onStageResult: setStageResult,
          onResult: (result) =>
            finishRun(
              result.durationMs
                ? result
                : { ...result, durationMs: Math.round(performance.now() - startedAt) },
            ),
          onError: (message) => {
            appendLog(line('error', message))
            finishRun({
              status: 'error',
              error: message,
              stages: [],
              logs: [],
              durationMs: Math.round(performance.now() - startedAt),
            })
          },
        },
        controller.signal,
        runnerToken,
      )
      setRunner('connected')
    } catch (error) {
      // Only the fallback path lands here: a cancel the runner accepted closes the
      // stream normally, with a `cancelled` result of its own.
      const cancelled = controller.signal.aborted
      const message = cancelled ? 'Cancelled before it finished' : messageOf(error)
      appendLog(line(cancelled ? 'warning' : 'error', message))
      finishRun({
        status: cancelled ? 'cancelled' : 'error',
        error: cancelled ? undefined : message,
        stages: [],
        logs: [],
        durationMs: Math.round(performance.now() - startedAt),
      })
      if (isUnreachable(error)) {
        setRunner('offline')
        setRunnerNote(null)
      }
    } finally {
      if (abort.current === controller) abort.current = null
      liveRunId.current = null
      setStopping(false)
      // `finishRun` already cleared `running` on every path above, except when the
      // stream ended without a `result` event — this keeps the button usable.
      if (usePipelineEditorStore.getState().running) {
        finishRun({
          status: 'error',
          error: 'The runner closed the stream without a result.',
          stages: [],
          logs: [],
        })
      }
    }
  }

  /**
   * Stop: cancels the run ON THE RUNNER, then falls back to dropping the stream.
   *
   * The stage in flight takes the Spark cancellation and the ones behind it never
   * start — aborting alone would leave the whole sequence running unwatched.
   */
  const stopRun = async () => {
    setStopping(true)
    const runId = liveRunId.current
    const accepted = runId ? await cancelRun(runnerUrl, runId, runnerToken) : false
    if (!accepted) abort.current?.abort()
  }

  /**
   * The pipeline's error is the failing stage's error, and that stage prints it
   * again in the list right below. Once is enough, and the row is the better
   * place: it also says which stage it was. The banner keeps the message only
   * when no stage claims it — a runner that never started one, say.
   */
  const bannerError = isErrorText(run?.error)
    ? Object.values(stageResults).some((result) => sameErrorText(result?.error, run?.error))
      ? undefined
      : run?.error
    : undefined

  return (
    <div className="flex h-full min-h-0 flex-col bg-surface">
      <header className="shrink-0 space-y-3 border-b border-line px-3 py-3">
        <div className="flex items-center justify-between gap-2">
          <h2 className="text-sm font-semibold text-content">Run pipeline</h2>
          <RunnerPill
            status={runner}
            note={runnerNote}
            onRetry={() => void checkHealth(runnerUrl)}
          />
        </div>

        <div className="flex items-center gap-2">
          <Tooltip
            content={disabledReason ?? 'Run every stage, in the order drawn on the canvas'}
          >
            <span className="min-w-0 flex-1">
              <Button
                variant="primary"
                size="lg"
                fullWidth
                icon={<Play className="h-4 w-4" />}
                loading={running}
                disabled={disabledReason !== null}
                onClick={() => void execute()}
              >
                {running ? 'Running…' : `Run ${plural(plan.stages.length, 'stage')}`}
              </Button>
            </span>
          </Tooltip>
          {running && (
            <Tooltip content="Cancels the execution on the runner, not just this window">
              <span>
                <Button
                  variant="danger"
                  size="lg"
                  icon={<Square className="h-3.5 w-3.5" />}
                  disabled={stopping}
                  onClick={() => void stopRun()}
                >
                  {stopping ? 'Stopping…' : 'Stop'}
                </Button>
              </span>
            </Tooltip>
          )}
        </div>

        {plan.blockers.length > 0 && (
          <ul className="space-y-1">
            {plan.blockers.slice(0, 4).map((blocker) => (
              <li
                key={blocker.id}
                className="flex items-start gap-1.5 text-2xs text-state-danger"
              >
                <CircleX className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
                <span>{blocker.message}</span>
              </li>
            ))}
          </ul>
        )}

        {runner === 'offline' && (
          <p className="flex items-start gap-1.5 text-2xs text-content-muted">
            <TriangleAlert
              className="mt-0.5 h-3.5 w-3.5 shrink-0 text-state-warning"
              aria-hidden
            />
            <span>
              {runnerNote ??
                `Nothing is answering at ${runnerUrl}. Start it with \`${RUNNER_START_COMMAND}\` from the sparquet-studio directory, then retry.`}
            </span>
          </p>
        )}
      </header>

      <div className="scroll-area flex-1 space-y-4 p-3">
        {run && (
          <StatusBanner status={run.status} error={bannerError} durationMs={run.durationMs} />
        )}

        {resolved.stages.length > 0 && (
          <section className="space-y-2">
            <SectionTitle>Stages</SectionTitle>
            <ol className="space-y-1">
              {resolved.stages.map((stage) => (
                <StageRow
                  key={stage.id}
                  order={stage.order}
                  name={stage.name}
                  status={stageStatus[stage.id]}
                  result={stageResults[stage.id]}
                />
              ))}
            </ol>
          </section>
        )}

        {run?.preview && (
          <PreviewSection
            columns={run.preview.columns}
            rows={run.preview.rows}
            truncated={run.preview.truncated}
          />
        )}

        {logs.length > 0 ? (
          <LogStream logs={logs} stageLabel={stageLabel} />
        ) : (
          !run && (
            <EmptyState
              icon={<Rocket />}
              title="No run yet"
              description="Run the pipeline to watch each pipeline execute in order. Everything runs on your machine."
            />
          )
        )}

        {pipeline && (
          <ExecutionHistoryPanel
            runnerUrl={runnerUrl}
            runnerToken={runnerToken}
            workflowId={pipeline.workflowId}
            pipelineId={pipeline.id}
            refreshToken={run?.runId}
            // A whole run paints every stage box; one job of it opens that job's
            // canvas showing the same execution, step by step.
            onViewRun={(runId) => void showPipelineRun(runId, runnerUrl, runnerToken)}
            viewingRunId={runView?.runId ?? null}
            onViewJobRun={(record, jobRun) => {
              if (!jobRun.jobId) return
              navigate(`/jobs/${jobRun.jobId}`, {
                state: { runId: record.id, jobRunId: jobRun.id },
              })
            }}
            viewActionLabel="Open job"
          />
        )}
      </div>
    </div>
  )
}

/* ------------------------------------------------------------------ pieces */

function RunnerPill({
  status,
  note,
  onRetry,
}: {
  status: RunnerStatus
  note: string | null
  onRetry: () => void
}) {
  if (status === 'checking') {
    return (
      <Badge tone="neutral" icon={<Spinner className="h-3 w-3" />}>
        Checking
      </Badge>
    )
  }

  const connected = status === 'connected'
  return (
    <Tooltip content={connected ? 'The local runner is answering' : (note ?? 'Runner offline')}>
      <button
        type="button"
        onClick={onRetry}
        aria-label="Check the runner connection again"
        className="rounded-full focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/50"
      >
        <Badge tone={connected ? 'success' : 'danger'} icon={<RefreshCw />}>
          {connected ? 'Runner online' : 'Runner offline'}
        </Badge>
      </button>
    </Tooltip>
  )
}

const BANNERS: Record<
  string,
  { icon: LucideIcon; title: string; className: string; iconClassName: string }
> = {
  success: {
    icon: CircleCheck,
    title: 'Pipeline finished',
    className: 'border-state-success/40 bg-state-success/10',
    iconClassName: 'text-state-success',
  },
  error: {
    icon: CircleX,
    title: 'Pipeline failed',
    className: 'border-state-danger/40 bg-state-danger/10',
    iconClassName: 'text-state-danger',
  },
  cancelled: {
    icon: CircleStop,
    title: 'Pipeline cancelled',
    className: 'border-state-warning/40 bg-state-warning/10',
    iconClassName: 'text-state-warning',
  },
  idle: {
    icon: CircleSlash,
    title: 'No run',
    className: 'border-line bg-surface-sunken',
    iconClassName: 'text-content-subtle',
  },
}

function StatusBanner({
  status,
  error,
  durationMs,
}: {
  status: string
  error?: string
  durationMs?: number
}) {
  const look = BANNERS[status] ?? BANNERS.idle
  const Icon = look.icon

  return (
    <div
      className={cn('flex items-start gap-2.5 rounded-xl border px-3 py-2.5', look.className)}
    >
      <Icon className={cn('mt-0.5 h-3.5 w-3.5 shrink-0', look.iconClassName)} aria-hidden />
      <div className="min-w-0 flex-1 space-y-1">
        <p className="text-xs font-medium text-content">{look.title}</p>
        {/* The stage's own error can be a whole stack trace: it gets a scrollable
            card of its own, so it cannot push the stages and the logs off screen. */}
        {error ? (
          <ErrorCard
            message={error}
            tone={status === 'cancelled' ? 'warning' : 'danger'}
            className="bg-surface/70"
          />
        ) : (
          <p className="break-words text-2xs leading-relaxed text-content-muted">
            {`Took ${formatDuration(durationMs)}.`}
          </p>
        )}
      </div>
    </div>
  )
}

function StageRow({
  order,
  name,
  status,
  result,
}: {
  order: number
  name: string
  status: StepStatus | undefined
  result: PipelineStageResult | undefined
}) {
  const look = stepLook(status)
  const Icon = look?.icon ?? ListOrdered
  const waiting = status === undefined || status === 'pending'

  return (
    <li className="rounded-lg border border-line bg-surface px-2.5 py-1.5">
      <div className="flex items-center gap-2">
        <span
          role="img"
          aria-label={waiting ? 'Waiting to run' : look?.label}
          title={waiting ? 'Waiting to run' : look?.label}
          className={cn(
            'inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full',
            look?.chip ?? 'bg-surface-sunken text-content-subtle',
            look?.spin,
          )}
        >
          <Icon className="h-3 w-3" aria-hidden />
        </span>
        <span className="shrink-0 text-2xs tabular-nums text-content-subtle">{order}</span>
        <span className="min-w-0 flex-1 truncate text-xs text-content" title={name}>
          {name}
        </span>
        {result?.durationMs !== undefined && (
          <span className="shrink-0 text-2xs tabular-nums text-content-subtle">
            {formatDuration(result.durationMs)}
          </span>
        )}
      </div>
      {result && (
        <p className="pl-6 text-2xs tabular-nums text-content-muted">
          {formatCount(result.rowsRead)} read · {formatCount(result.rowsWritten)} written
        </p>
      )}
      {result?.error && (
        <ErrorCard
          message={result.error}
          tone={result.status === 'cancelled' ? 'warning' : 'danger'}
          size="sm"
          className="ml-6 mt-1"
        />
      )}
    </li>
  )
}

function PreviewSection({
  columns,
  rows,
  truncated,
}: {
  columns: string[]
  rows: unknown[][]
  truncated: boolean
}) {
  const [open, setOpen] = useState(false)

  return (
    <section className="space-y-2">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
        className="flex w-full items-center gap-2 rounded-lg border border-line bg-surface px-2.5 py-1.5 text-xs text-content transition-colors hover:bg-surface-sunken"
      >
        <ChevronRight
          className={cn(
            'h-3.5 w-3.5 text-content-subtle transition-transform',
            open && 'rotate-90',
          )}
          aria-hidden
        />
        <Table2 className="h-3.5 w-3.5 text-content-subtle" aria-hidden />
        <span className="font-medium">Last stage preview</span>
        <span className="text-content-subtle">
          {rows.length}
          {truncated ? '+' : ''} × {columns.length}
        </span>
      </button>
      {open && <RunResultTable columns={columns} rows={rows} truncated={truncated} />}
    </section>
  )
}

const LOG_LEVEL_CLASS: Record<RunLogLine['level'], string> = {
  debug: 'text-content-subtle',
  info: 'text-state-info',
  warning: 'text-state-warning',
  error: 'text-state-danger',
}

function LogStream({
  logs,
  stageLabel,
}: {
  logs: RunLogLine[]
  stageLabel: ReadonlyMap<string, string>
}) {
  const [open, setOpen] = useState(true)
  const failures = logs.filter((line) => line.level === 'error').length

  return (
    <section className="space-y-2">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
        className="flex w-full items-center gap-2 rounded-lg border border-line bg-surface px-2.5 py-1.5 text-xs text-content transition-colors hover:bg-surface-sunken"
      >
        <ChevronRight
          className={cn(
            'h-3.5 w-3.5 text-content-subtle transition-transform',
            open && 'rotate-90',
          )}
          aria-hidden
        />
        <Terminal className="h-3.5 w-3.5 text-content-subtle" aria-hidden />
        <span className="font-medium">Logs</span>
        <span className="text-content-subtle">{logs.length}</span>
        {failures > 0 && (
          <Badge tone="danger" className="ml-auto">
            {failures} error{failures === 1 ? '' : 's'}
          </Badge>
        )}
      </button>

      {open && (
        <div className="max-h-72 overflow-auto rounded-xl border border-line bg-surface-sunken p-2 font-mono text-2xs">
          {logs.map((line, index) => (
            <div key={`${line.ts}-${index}`} className="flex gap-2 py-0.5">
              <span className="shrink-0 text-content-subtle">{formatClockTime(line.ts)}</span>
              <span className={cn('w-14 shrink-0 uppercase', LOG_LEVEL_CLASS[line.level])}>
                {line.level}
              </span>
              {line.stageId && (
                <span className="w-28 shrink-0 truncate text-brand-500" aria-label="pipeline stage">
                  {stageLabel.get(line.stageId) ?? line.stageId}
                </span>
              )}
              <span className="min-w-0 whitespace-pre-wrap break-words text-content-muted">
                {line.message}
              </span>
            </div>
          ))}
        </div>
      )}
    </section>
  )
}

/* ----------------------------------------------------------------- helpers */

function line(level: RunLogLine['level'], message: string): RunLogLine {
  return { ts: Date.now(), level, message }
}

function isUnreachable(error: unknown): boolean {
  return isRunnerError(error) && error.kind === 'unreachable'
}

function messageOf(error: unknown): string {
  if (error instanceof Error) return error.message
  return typeof error === 'string' ? error : 'The runner could not be reached'
}
