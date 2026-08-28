import {
  ArrowDownToLine,
  ArrowUpFromLine,
  Check,
  ChevronRight,
  CircleCheck,
  CircleSlash,
  CircleStop,
  CircleX,
  Clock3,
  Copy,
  KeyRound,
  Play,
  PlugZap,
  RefreshCw,
  Rocket,
  ShieldAlert,
  ShieldCheck,
  Square,
  Table2,
  Terminal,
  TriangleAlert,
  X,
  type LucideIcon,
} from 'lucide-react'
import { useCallback, useEffect, useId, useMemo, useRef, useState } from 'react'

import {
  Badge,
  Button,
  EmptyState,
  ErrorCard,
  Field,
  IconButton,
  Input,
  SectionTitle,
  Spinner,
  Toggle,
  Tooltip,
} from '@/components/ui'
import { getTransformation, getValidationSink, getValidator } from '@/catalog'
import { stepLook } from '@/components/canvas/stepLook'
import { ExecutionHistoryPanel } from '@/components/history/ExecutionHistoryPanel'
import {
  checkRunnerHealth,
  createStepTimer,
  DEFAULT_RUNNER_URL,
  isRunnerError,
  cancelRun,
  runJobStream,
  RUNNER_INSTALL_COMMAND,
  RUNNER_START_COMMAND,
  validateJob,
  type RunnerHealth,
  type RunStepEvent,
} from '@/lib/runner/client'
import { nodeIdForStep, pendingStatuses } from '@/lib/runner/stepNodes'
import { cn } from '@/lib/utils/cn'
import { formatClockTime, formatCount, formatDuration } from '@/lib/utils/format'
import { nodeOrdinals, useEditorStore } from '@/store/editor'
import { useSettingsStore } from '@/store/settings'
import type {
  ParamDefinition,
  RunLogLine,
  RunResult,
  RunStatus,
  StepStatus,
  StudioNode,
} from '@/types/studio'

import { RunResultTable } from './RunResultTable'

/** Rows requested from the runner for the preview; the table caps at 50 too. */
const PREVIEW_ROWS = 50

const RUNNER_COMMANDS = ['cd sparquet-studio', RUNNER_INSTALL_COMMAND, RUNNER_START_COMMAND]

type RunnerStatus = 'checking' | 'connected' | 'offline'
type RunMode = 'run' | 'validate'
type ParamValue = ParamDefinition['value']

/** The runner answered and refused: 401 wants the token, 403 refuses this origin. */
interface AuthIssue {
  status: 401 | 403
  message: string
  mode: RunMode
}

export function RunPanel() {
  const job = useEditorStore((state) => state.job)
  const params = useEditorStore((state) => state.params)
  const setParams = useEditorStore((state) => state.setParams)
  const nodes = useEditorStore((state) => state.nodes)
  const issues = useEditorStore((state) => state.issues)
  const compile = useEditorStore((state) => state.compile)
  const run = useEditorStore((state) => state.run)
  const running = useEditorStore((state) => state.running)
  const setRun = useEditorStore((state) => state.setRun)
  const setRunning = useEditorStore((state) => state.setRunning)
  const setStepStatus = useEditorStore((state) => state.setStepStatus)
  const setStepStatuses = useEditorStore((state) => state.setStepStatuses)
  const showRunView = useEditorStore((state) => state.showRunView)
  const runView = useEditorStore((state) => state.runView)

  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const setRunnerUrl = useSettingsStore((state) => state.setRunnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const runAs = useSettingsStore((state) => state.runAs)
  const setRunnerToken = useSettingsStore((state) => state.setRunnerToken)

  const [runner, setRunner] = useState<RunnerStatus>('checking')
  const [health, setHealth] = useState<RunnerHealth | null>(null)
  const [runnerNote, setRunnerNote] = useState<string | null>(null)
  const [authIssue, setAuthIssue] = useState<AuthIssue | null>(null)
  const [mode, setMode] = useState<RunMode>('run')
  const [stopping, setStopping] = useState(false)

  const runAbort = useRef<AbortController | null>(null)
  const healthAbort = useRef<AbortController | null>(null)
  /** The execution id the runner opened for this run — what Stop cancels. */
  const liveRunId = useRef<string | null>(null)

  const checkHealth = useCallback(async (url: string) => {
    healthAbort.current?.abort()
    const controller = new AbortController()
    healthAbort.current = controller
    setRunner('checking')
    try {
      const payload = await checkRunnerHealth(url, controller.signal)
      if (controller.signal.aborted) return
      setHealth(payload)
      setRunner('connected')
      setRunnerNote(null)
    } catch (error) {
      if (controller.signal.aborted) return
      setHealth(null)
      setRunner('offline')
      // "Unreachable" needs no message: the setup card below already explains it.
      setRunnerNote(isUnreachable(error) ? null : errorMessage(error))
    }
  }, [])

  useEffect(() => {
    // Read the URL imperatively: typing a new address must not fire a request per keystroke.
    void checkHealth(useSettingsStore.getState().runnerUrl)
    return () => healthAbort.current?.abort()
  }, [checkHealth])

  const blocking = useMemo(() => issues.filter((issue) => issue.severity === 'error'), [issues])

  const overwriteTargets = useMemo(
    () =>
      nodes.flatMap((node) =>
        node.data.kind === 'sink' && node.data.mode === 'overwrite'
          ? [node.data.path || node.data.label || node.data.format]
          : [],
      ),
    [nodes],
  )

  const paramValues = useMemo(() => {
    const values: Record<string, ParamValue> = {}
    for (const param of params) {
      if (param.key) values[param.key] = param.value
    }
    return values
  }, [params])

  const updateParam = (id: string, value: ParamValue) => {
    setParams(params.map((param) => (param.id === id ? { ...param, value } : param)))
  }

  const disabledReason = running
    ? 'A run is already in progress'
    : blocking.length > 0
      ? `Fix ${blocking.length} blocking ${blocking.length === 1 ? 'issue' : 'issues'} first`
      : null

  const execute = async (next: RunMode) => {
    const { pipeline } = compile()
    if (!pipeline) {
      setRun({
        status: 'error',
        error: 'The graph does not compile into a pipeline yet.',
        logs: [],
      })
      return
    }

    const controller = new AbortController()
    runAbort.current = controller
    setMode(next)
    setAuthIssue(null)
    setRunning(true)
    setRun({
      status: next === 'run' ? 'running' : 'connecting',
      pipelineName: pipeline.name,
      logs: [
        {
          ts: Date.now(),
          level: 'info',
          message:
            next === 'run'
              ? 'Sending the pipeline to the runner'
              : 'Validating the configuration',
        },
      ],
    })

    const startedAt = performance.now()

    try {
      if (next === 'run') {
        // Streamed run: logs land as Spark produces them, so a long write shows
        // progress instead of a frozen panel. The final `result` event carries the
        // same payload the blocking endpoint returns.
        const streamed: RunLogLine[] = []
        let settled: RunResult | null = null
        let lastStarted: string | null = null

        // The runner reports a step by its position in the compiled JSON (or, for
        // the quality datasets, by role). These lanes are what map that back onto
        // the canvas — the same mapping a run read back from history goes through.
        const lanes = useEditorStore.getState().stepNodeLanes()
        setStepStatuses(pendingStatuses(lanes))

        const nodeForStep = (step: RunStepEvent): string | undefined =>
          nodeIdForStep(lanes, step)

        // Two timestamped log lines bracket every step, so the panel times each one
        // itself and the framework reports no duration at all. What the number does
        // and does not mean is spelled out on `createStepTimer`.
        const timer = createStepTimer()

        await runJobStream(
          runnerUrl,
          {
            pipeline,
            params: paramValues,
            limit: PREVIEW_ROWS,
            workflowId: job?.workflowId,
            jobId: job?.id,
            jobName: job?.name,
            runAs: runAs || undefined,
            launched: 'manual',
          },
          {
            // The run exists on the runner from here on, so Stop can cancel it.
            onStart: (start) => {
              liveRunId.current = start.runId ?? null
            },
            onLog: (line) => {
              streamed.push(line)
              setRun({
                status: 'running',
                pipelineName: pipeline.name,
                logs: [...streamed],
              })
            },
            onStep: (event) => {
              const nodeId = nodeForStep(event)
              if (!nodeId) return
              if (event.status === 'running') {
                timer.start(nodeId, event.ts)
                setStepStatus(nodeId, 'running')
                lastStarted = nodeId
                return
              }
              // Closing marker: the pair is complete, so the step can be timed. A
              // skipped step never opened one and is left without a duration.
              setStepStatus(nodeId, event.status, timer.finish(nodeId, event.ts))
              lastStarted = null
            },
            onResult: (result) => {
              settled = result
              // A failed run stops mid-chain: the step that was running never
              // reports 'applied', so mark it as the failure point. A cancelled one
              // stops the same way, but the step was stopped, not broken.
              if (lastStarted && (result.status === 'error' || result.status === 'cancelled')) {
                setStepStatus(lastStarted, result.status === 'cancelled' ? 'cancelled' : 'error')
              }
            },
          },
          controller.signal,
          runnerToken,
        )

        const elapsed = Math.round(performance.now() - startedAt)
        const result: RunResult = settled ?? {
          status: 'error',
          pipelineName: pipeline.name,
          error: 'The runner closed the stream without a result.',
          logs: streamed,
        }
        // Keep the streamed lines: the JVM/stdout windows only exist on the stream.
        const merged: RunResult = {
          ...result,
          logs: streamed.length >= (result.logs?.length ?? 0) ? streamed : result.logs,
        }
        setRun(merged.durationMs ? merged : { ...merged, durationMs: elapsed })
      } else {
        const outcome = await validateJob(
          runnerUrl,
          pipeline,
          controller.signal,
          runnerToken,
        )
        const message = outcome.valid
          ? 'The runner accepted the configuration'
          : (outcome.error ?? 'The runner rejected the configuration')
        setRun({
          status: outcome.valid ? 'success' : 'error',
          pipelineName: pipeline.name,
          durationMs: Math.round(performance.now() - startedAt),
          error: outcome.valid ? undefined : message,
          logs: [{ ts: Date.now(), level: outcome.valid ? 'info' : 'error', message }],
        })
      }
      setRunner('connected')
    } catch (error) {
      const refused = refusedStatus(error)
      if (controller.signal.aborted) {
        // Only the fallback path lands here: the runner either refused the cancel
        // or was never asked (a `validate`, or a run that had not started yet).
        setRun({
          status: 'cancelled',
          pipelineName: pipeline.name,
          durationMs: Math.round(performance.now() - startedAt),
          logs: [{ ts: Date.now(), level: 'warning', message: 'Cancelled before it finished' }],
        })
      } else if (refused !== null) {
        // The runner answered, it just refused the call — nothing about it is down,
        // and the card below fixes it without leaving the panel.
        setAuthIssue({ status: refused, message: errorMessage(error), mode: next })
        setRun(null)
        setRunner('connected')
      } else {
        const message = errorMessage(error)
        setRun({
          status: 'error',
          pipelineName: pipeline.name,
          durationMs: Math.round(performance.now() - startedAt),
          error: message,
          logs: [{ ts: Date.now(), level: 'error', message }],
        })
        if (isUnreachable(error)) {
          setHealth(null)
          setRunner('offline')
          setRunnerNote(null)
        }
      }
    } finally {
      if (runAbort.current === controller) runAbort.current = null
      liveRunId.current = null
      setStopping(false)
      setRunning(false)
    }
  }

  /**
   * Stop: cancels the run ON THE RUNNER, then falls back to dropping the stream.
   *
   * Aborting alone would leave Spark working to the end with nobody watching — the
   * panel would say "cancelled" while the write it started kept going.
   */
  const stopRun = async () => {
    setStopping(true)
    const runId = liveRunId.current
    const accepted = runId ? await cancelRun(runnerUrl, runId, runnerToken) : false
    // Cancelled runs still close their stream properly, with a `cancelled` result:
    // only refuse-to-cancel (finished already, runner gone, a `validate`) aborts.
    if (!accepted) runAbort.current?.abort()
  }

  const sparkMissing = runner === 'connected' && health !== null && !health.sparkAvailable

  return (
    <div className="flex h-full min-h-0 flex-col bg-surface">
      <header className="shrink-0 space-y-3 border-b border-line px-3 py-3">
        <div className="flex items-center justify-between gap-2">
          <h2 className="text-sm font-semibold text-content">Run</h2>
          <div className="flex items-center gap-1">
            <RunnerPill status={runner} health={health} note={runnerNote} />
            <IconButton
              size="sm"
              label="Check the runner connection"
              disabled={runner === 'checking'}
              onClick={() => void checkHealth(runnerUrl)}
            >
              <RefreshCw className={cn(runner === 'checking' && 'animate-spin')} />
            </IconButton>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Tooltip content={disabledReason ?? 'Compile the graph and execute it on the runner'}>
            <span className="min-w-0 flex-1">
              <Button
                variant="primary"
                size="lg"
                fullWidth
                icon={<Play className="h-4 w-4" />}
                loading={running}
                disabled={disabledReason !== null}
                onClick={() => void execute('run')}
              >
                {running ? 'Running…' : 'Run pipeline'}
              </Button>
            </span>
          </Tooltip>

          {running ? (
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
          ) : (
            <Tooltip content="Check the configuration without touching Spark or any sink">
              <span>
                <Button
                  variant="secondary"
                  size="lg"
                  icon={<ShieldCheck className="h-4 w-4" />}
                  disabled={disabledReason !== null}
                  onClick={() => void execute('validate')}
                >
                  Validate only
                </Button>
              </span>
            </Tooltip>
          )}
        </div>

        {blocking.length > 0 && (
          <p className="flex items-center gap-1.5 text-2xs text-state-danger">
            <CircleX className="h-3.5 w-3.5 shrink-0" aria-hidden />
            {blocking.length === 1
              ? '1 blocking issue keeps the graph from compiling'
              : `${blocking.length} blocking issues keep the graph from compiling`}
          </p>
        )}

        {sparkMissing && (
          <p className="flex items-center gap-1.5 text-2xs text-state-warning">
            <TriangleAlert className="h-3.5 w-3.5 shrink-0" aria-hidden />
            The runner is up but PySpark was not detected — runs will fail until it is
            installed.
          </p>
        )}
      </header>

      <div className="scroll-area flex-1 space-y-4 p-3">
        {runner === 'offline' && (
          <RunnerSetup
            url={runnerUrl}
            note={runnerNote}
            onUrlChange={setRunnerUrl}
            onRetry={() => void checkHealth(runnerUrl)}
          />
        )}

        {authIssue && (
          <RunnerAuthNotice
            issue={authIssue}
            token={runnerToken}
            onTokenChange={setRunnerToken}
            onRetry={() => void execute(authIssue.mode)}
          />
        )}

        {overwriteTargets.length > 0 && <OverwriteNotice targets={overwriteTargets} />}

        {params.length > 0 && (
          <section className="space-y-2">
            <SectionTitle>Parameters</SectionTitle>
            <div className="card space-y-3 p-3">
              {params.map((param) => (
                <ParamControl
                  key={param.id}
                  param={param}
                  onChange={(value) => updateParam(param.id, value)}
                />
              ))}
            </div>
          </section>
        )}

        {run ? (
          <RunReport run={run} mode={mode} />
        ) : (
          // The auth card already says why there is no report; two cards would repeat it.
          !authIssue && (
            <EmptyState
              icon={<Rocket />}
              title="No run yet"
              description="Execute the pipeline to see rows, validations and logs here. Everything runs on your machine."
            />
          )
        )}

        {job && (
          <ExecutionHistoryPanel
            runnerUrl={runnerUrl}
            runnerToken={runnerToken}
            workflowId={job.workflowId}
            jobId={job.id}
            refreshToken={run?.runId}
            // Picking a run here paints it on the canvas: the user asked which box
            // did what, and the canvas is where the boxes are.
            onViewJobRun={(record, jobRun) => showRunView(record, jobRun, { pinned: true })}
            viewingJobRunId={runView?.jobRunId ?? null}
            viewingRunId={runView?.runId ?? null}
          />
        )}
      </div>
    </div>
  )
}

/* ------------------------------------------------------------------ runner */

interface RunnerPillProps {
  status: RunnerStatus
  health: RunnerHealth | null
  note: string | null
}

function RunnerPill({ status, health, note }: RunnerPillProps) {
  if (status === 'checking') {
    return (
      <Badge tone="neutral" icon={<Spinner className="h-3 w-3" />}>
        Checking
      </Badge>
    )
  }

  const connected = status === 'connected'
  const sparkMissing = connected && health !== null && !health.sparkAvailable
  const tooltip = connected
    ? [
        health?.version ? `Runner ${health.version}` : 'The local runner is answering',
        health?.frameworkVersion ? `Sparquet ${health.frameworkVersion}` : null,
        sparkMissing ? 'PySpark not detected' : null,
      ]
        .filter(Boolean)
        .join(' · ')
    : (note ?? 'Nothing is answering at the runner URL')

  return (
    <Tooltip content={tooltip}>
      <span>
        <Badge tone={connected ? (sparkMissing ? 'warning' : 'success') : 'danger'}>
          <span
            className={cn(
              'h-1.5 w-1.5 rounded-full',
              connected
                ? sparkMissing
                  ? 'bg-state-warning'
                  : 'bg-state-success'
                : 'bg-state-danger',
            )}
            aria-hidden
          />
          {connected ? 'Runner online' : 'Runner offline'}
        </Badge>
      </span>
    </Tooltip>
  )
}

interface RunnerSetupProps {
  url: string
  note: string | null
  onUrlChange: (url: string) => void
  onRetry: () => void
}

function RunnerSetup({ url, note, onUrlChange, onRetry }: RunnerSetupProps) {
  const inputId = useId()
  return (
    <section className="card space-y-3 border-brand-500/30 p-3 animate-fade-in">
      <div className="flex items-start gap-2.5">
        <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-brand-500/12 text-brand-600 dark:text-brand-400">
          <PlugZap className="h-4 w-4" aria-hidden />
        </span>
        <div className="space-y-1">
          <p className="text-sm font-medium text-content">Start the local runner</p>
          <p className="text-xs leading-relaxed text-content-muted">
            Studio executes pipelines through a small service on your machine. Run these
            commands from the repository, then retry.
          </p>
        </div>
      </div>

      <ol className="space-y-1.5">
        {RUNNER_COMMANDS.map((command, index) => (
          <CommandRow key={command} step={index + 1} command={command} />
        ))}
      </ol>

      <p className="text-2xs leading-relaxed text-content-subtle">
        On startup the runner prints a token in its terminal. Paste it under Settings → Local
        runner, or in the card this panel shows the first time a run is refused.
      </p>

      <Field
        label="Runner URL"
        htmlFor={inputId}
        help="Where Studio looks for the service. Change it if you picked another port."
      >
        <Input
          id={inputId}
          mono
          value={url}
          spellCheck={false}
          placeholder={DEFAULT_RUNNER_URL}
          onChange={(event) => onUrlChange(event.target.value)}
        />
      </Field>

      <div className="flex items-center gap-2">
        <Button
          variant="primary"
          size="sm"
          icon={<RefreshCw className="h-3.5 w-3.5" />}
          onClick={onRetry}
        >
          Retry
        </Button>
        <p className="min-w-0 truncate text-2xs text-content-subtle">
          {note ?? 'Nothing answered at that address yet.'}
        </p>
      </div>
    </section>
  )
}

interface RunnerAuthNoticeProps {
  issue: AuthIssue
  token: string
  onTokenChange: (token: string) => void
  onRetry: () => void
}

function RunnerAuthNotice({ issue, token, onTokenChange, onRetry }: RunnerAuthNoticeProps) {
  const inputId = useId()
  const needsToken = issue.status === 401

  return (
    <section className="card space-y-3 border-state-warning/40 p-3 animate-fade-in">
      <div className="flex items-start gap-2.5">
        <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-state-warning/12 text-state-warning">
          {needsToken ? (
            <KeyRound className="h-4 w-4" aria-hidden />
          ) : (
            <ShieldAlert className="h-4 w-4" aria-hidden />
          )}
        </span>
        <div className="space-y-1">
          <p className="text-sm font-medium text-content">
            {needsToken ? 'This runner needs its token' : 'This runner refused the origin'}
          </p>
          <p className="text-xs leading-relaxed text-content-muted">
            {needsToken
              ? 'The runner prints a token in its terminal when it starts. Paste it here and retry — it is kept with your settings and sent only to the runner.'
              : 'The runner only accepts requests coming from the origin Studio is served on. Start it with SPARQUET_STUDIO_ORIGINS set to this origin to widen the allow-list.'}
          </p>
        </div>
      </div>

      {needsToken && (
        <Field label="Runner token" htmlFor={inputId}>
          <Input
            id={inputId}
            mono
            type="password"
            autoComplete="off"
            spellCheck={false}
            value={token}
            placeholder="Paste the token from the runner terminal"
            onChange={(event) => onTokenChange(event.target.value)}
          />
        </Field>
      )}

      <p className="text-2xs leading-relaxed text-content-subtle">{issue.message}</p>

      <Button
        variant="primary"
        size="sm"
        icon={<RefreshCw className="h-3.5 w-3.5" />}
        disabled={needsToken && token.trim().length === 0}
        onClick={onRetry}
      >
        {issue.mode === 'run' ? 'Retry run' : 'Retry validation'}
      </Button>
    </section>
  )
}

function CommandRow({ step, command }: { step: number; command: string }) {
  const [copied, setCopied] = useState(false)

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(command)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1400)
    } catch {
      setCopied(false)
    }
  }

  return (
    <li className="flex items-center gap-2">
      <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-md border border-line bg-surface-sunken text-2xs text-content-subtle">
        {step}
      </span>
      <code className="min-w-0 flex-1 truncate rounded-lg border border-line bg-surface-sunken px-2 py-1.5 font-mono text-2xs text-content-muted">
        {command}
      </code>
      <IconButton
        size="sm"
        label={copied ? 'Copied' : `Copy command ${step}`}
        onClick={() => void copy()}
      >
        {copied ? <Check className="text-state-success" /> : <Copy />}
      </IconButton>
    </li>
  )
}

function OverwriteNotice({ targets }: { targets: string[] }) {
  return (
    <div className="flex items-start gap-2.5 rounded-xl border border-state-warning/40 bg-state-warning/10 px-3 py-2.5">
      <TriangleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0 text-state-warning" aria-hidden />
      <div className="min-w-0 space-y-1">
        <p className="text-xs font-medium text-content">This run writes for real</p>
        <p className="text-2xs leading-relaxed text-content-muted">
          {targets.length === 1
            ? 'One destination replaces'
            : `${targets.length} destinations replace`}{' '}
          existing data with mode <span className="font-mono">overwrite</span>.
        </p>
        <ul className="space-y-0.5">
          {targets.map((target, index) => (
            <li
              key={`${target}-${index}`}
              className="truncate font-mono text-2xs text-content-muted"
            >
              {target}
            </li>
          ))}
        </ul>
      </div>
    </div>
  )
}

/* ------------------------------------------------------------------ params */

interface ParamControlProps {
  param: ParamDefinition
  onChange: (value: ParamValue) => void
}

function ParamControl({ param, onChange }: ParamControlProps) {
  const controlId = useId()

  if (param.type === 'boolean') {
    return (
      <Toggle
        checked={param.value === true}
        onCheckedChange={onChange}
        label={param.key}
        description={
          param.description ?? 'Off resolves to an empty value, skipping guarded steps'
        }
      />
    )
  }

  if (param.type === 'list') {
    return (
      <Field
        label={param.key}
        htmlFor={controlId}
        help={param.description ?? 'Sent as a SQL IN list'}
      >
        <ChipsInput id={controlId} values={toStringList(param.value)} onChange={onChange} />
      </Field>
    )
  }

  const text =
    typeof param.value === 'string' || typeof param.value === 'number'
      ? String(param.value)
      : ''

  return (
    <Field label={param.key} htmlFor={controlId} help={param.description}>
      <Input
        id={controlId}
        mono
        spellCheck={false}
        type={param.type === 'number' ? 'number' : 'text'}
        value={text}
        onChange={(event) =>
          onChange(
            param.type === 'number' ? toNumberValue(event.target.value) : event.target.value,
          )
        }
      />
    </Field>
  )
}

interface ChipsInputProps {
  id: string
  values: string[]
  onChange: (values: string[]) => void
}

function ChipsInput({ id, values, onChange }: ChipsInputProps) {
  const [draft, setDraft] = useState('')

  const commit = (raw: string) => {
    const parts = raw
      .split(',')
      .map((part) => part.trim())
      .filter((part) => part.length > 0 && !values.includes(part))
    setDraft('')
    if (parts.length > 0) onChange([...values, ...parts])
  }

  return (
    <div className="space-y-1.5">
      {values.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {values.map((value) => (
            <span key={value} className="chip font-mono">
              {value}
              <button
                type="button"
                aria-label={`Remove ${value}`}
                onClick={() => onChange(values.filter((item) => item !== value))}
                className="text-content-subtle transition-colors hover:text-state-danger"
              >
                <X className="h-3 w-3" aria-hidden />
              </button>
            </span>
          ))}
        </div>
      )}
      <Input
        id={id}
        mono
        spellCheck={false}
        value={draft}
        placeholder="Add a value, then press Enter"
        onChange={(event) => setDraft(event.target.value)}
        onBlur={() => commit(draft)}
        onKeyDown={(event) => {
          if (event.key === 'Enter' || event.key === ',') {
            event.preventDefault()
            commit(draft)
          } else if (event.key === 'Backspace' && draft === '' && values.length > 0) {
            onChange(values.slice(0, -1))
          }
        }}
      />
    </div>
  )
}

/* ----------------------------------------------------------------- results */

function RunReport({ run, mode }: { run: RunResult; mode: RunMode }) {
  const settled = run.status !== 'running' && run.status !== 'connecting'
  const showMetrics = settled && mode === 'run'

  return (
    <div className="space-y-4 animate-fade-in">
      <StatusBanner run={run} mode={mode} />

      {showMetrics && (
        <div className="grid grid-cols-3 gap-2">
          <Metric icon={ArrowDownToLine} label="Rows read" value={formatCount(run.rowsRead)} />
          <Metric
            icon={ArrowUpFromLine}
            label="Rows written"
            value={formatCount(run.rowsWritten)}
          />
          <Metric icon={Clock3} label="Duration" value={formatDuration(run.durationMs)} />
        </div>
      )}

      {showMetrics && run.outputMetrics && run.outputMetrics.length > 1 && (
        <section className="space-y-2">
          <SectionTitle>Outputs</SectionTitle>
          <div className="overflow-x-auto rounded-xl border border-line bg-surface">
            <table className="w-full border-collapse text-left text-xs">
              <thead>
                <tr className="bg-surface-sunken text-2xs uppercase tracking-wider text-content-subtle">
                  <th scope="col" className="px-2.5 py-1.5 font-medium">
                    Destination
                  </th>
                  <th scope="col" className="px-2.5 py-1.5 font-medium">
                    Format
                  </th>
                  <th scope="col" className="px-2.5 py-1.5 font-medium">
                    Mode
                  </th>
                  <th scope="col" className="px-2.5 py-1.5 text-right font-medium">
                    Rows
                  </th>
                </tr>
              </thead>
              <tbody>
                {run.outputMetrics.map((out, index) => (
                  <tr key={`${out.path}-${index}`} className="border-t border-line">
                    <td className="px-2.5 py-1.5 font-mono text-2xs text-content">
                      {out.path}
                    </td>
                    <td className="whitespace-nowrap px-2.5 py-1.5 text-2xs text-content-muted">
                      {out.format}
                    </td>
                    <td className="whitespace-nowrap px-2.5 py-1.5 text-2xs text-content-muted">
                      {out.mode || '—'}
                    </td>
                    <td className="px-2.5 py-1.5 text-right tabular-nums text-content-muted">
                      {formatCount(out.rowsWritten)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {showMetrics && <StepTimings />}

      {run.validations && run.validations.length > 0 && (
        <section className="space-y-2">
          <SectionTitle>Validations</SectionTitle>
          <div className="overflow-x-auto rounded-xl border border-line bg-surface">
            <table className="w-full border-collapse text-left text-xs">
              <thead>
                <tr className="bg-surface-sunken text-2xs uppercase tracking-wider text-content-subtle">
                  <th scope="col" className="px-2.5 py-1.5 font-medium">
                    Rule
                  </th>
                  <th scope="col" className="px-2.5 py-1.5 font-medium">
                    Result
                  </th>
                  <th scope="col" className="px-2.5 py-1.5 text-right font-medium">
                    Failed
                  </th>
                  <th scope="col" className="px-2.5 py-1.5 font-medium">
                    Message
                  </th>
                </tr>
              </thead>
              <tbody>
                {run.validations.map((rule, index) => (
                  <tr key={`${rule.type}-${index}`} className="border-t border-line">
                    <td className="whitespace-nowrap px-2.5 py-1.5 font-mono text-2xs text-content">
                      {rule.type}
                    </td>
                    <td className="px-2.5 py-1.5">
                      <Badge tone={rule.passed ? 'success' : 'danger'}>
                        {rule.passed ? 'passed' : 'failed'}
                      </Badge>
                    </td>
                    <td className="px-2.5 py-1.5 text-right tabular-nums text-content-muted">
                      {rule.failedCount === undefined ? '—' : formatCount(rule.failedCount)}
                    </td>
                    <td className="px-2.5 py-1.5 text-2xs text-content-muted">
                      {rule.message ?? '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {run.preview && (
        <PreviewSection
          columns={run.preview.columns}
          rows={run.preview.rows}
          truncated={run.preview.truncated}
        />
      )}

      {run.logs.length > 0 && <LogStream logs={run.logs} />}
    </div>
  )
}

/* -------------------------------------------------------------- step times */

/** How a step is named in the timing list — the box's own label, or its type. */
function stepLabel(node: StudioNode): string {
  const data = node.data
  switch (data.kind) {
    case 'source':
      return data.label ?? `read ${data.format || 'source'}`
    case 'transform':
      return data.label ?? getTransformation(data.transform)?.label ?? data.transform
    case 'validation':
      return data.label ?? getValidator(data.validator)?.label ?? data.validator
    case 'sink':
      return data.dqRole
        ? (data.label ?? getValidationSink(data.dqRole)?.label ?? data.dqRole)
        : (data.label ?? `write ${data.format || 'output'}`)
    default:
      return data.label ?? data.kind
  }
}

/**
 * How long each step took, in execution order, once the run has settled.
 *
 * The durations are DERIVED: the panel subtracts the timestamps of the two log
 * lines that bracket each step (see `createStepTimer`). They are wall clock per
 * step and nothing else — deliberately not totalled, because they do not add up
 * to the run's duration and pretending otherwise would be a lie: Spark is lazy,
 * so a transformation only builds a plan and reads near zero.
 */
function StepTimings() {
  const nodes = useEditorStore((state) => state.nodes)
  const edges = useEditorStore((state) => state.edges)
  const stepStatus = useEditorStore((state) => state.stepStatus)
  const stepDuration = useEditorStore((state) => state.stepDuration)

  const rows = useMemo(() => {
    const ordinals = nodeOrdinals({ nodes, edges })
    // A node with no ordinal still ran (a quality dataset in a job with no rule on
    // the shared chain, say): keep it, at the end, rather than dropping the row.
    const last = Number.MAX_SAFE_INTEGER
    return nodes
      .filter((node) => stepStatus[node.id] !== undefined)
      .map((node) => ({
        id: node.id,
        ordinal: ordinals[node.id],
        label: stepLabel(node),
        status: stepStatus[node.id],
        durationMs: stepDuration[node.id],
      }))
      .sort((a, b) => (a.ordinal ?? last) - (b.ordinal ?? last))
  }, [nodes, edges, stepStatus, stepDuration])

  if (rows.length === 0) return null

  return (
    <section className="space-y-2">
      <SectionTitle>Step timings</SectionTitle>
      <ul className="divide-y divide-line overflow-hidden rounded-xl border border-line bg-surface">
        {rows.map((row) => (
          <StepTimingRow key={row.id} {...row} />
        ))}
      </ul>
      <p className="text-2xs leading-relaxed text-content-subtle">
        Wall clock measured between each step&apos;s start and end log line. Spark is lazy,
        so a transformation only builds the plan and reads near zero — the time lands on
        the read, on each validation rule (every rule is a Spark action) and on the writes.
        The rows are per step and do not add up to the run duration.
      </p>
    </section>
  )
}

function StepTimingRow({
  ordinal,
  label,
  status,
  durationMs,
}: {
  ordinal: number | undefined
  label: string
  status: StepStatus | undefined
  durationMs: number | undefined
}) {
  const look = stepLook(status)
  const Icon = look?.icon ?? Clock3
  const waiting = status === undefined || status === 'pending'
  const statusLabel = waiting ? 'Never reached by the run' : look!.label

  return (
    <li className="flex items-center gap-2 px-2.5 py-1.5">
      <span
        role="img"
        aria-label={statusLabel}
        title={statusLabel}
        className={cn(
          'inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full',
          look?.chip ?? 'bg-surface-sunken text-content-subtle',
        )}
      >
        <Icon className={cn('h-3 w-3', look?.spin)} aria-hidden />
      </span>
      <span className="w-4 shrink-0 text-right text-2xs tabular-nums text-content-subtle">
        {ordinal ?? '–'}
      </span>
      <span className="min-w-0 flex-1 truncate text-xs text-content" title={label}>
        {label}
      </span>
      <span className="shrink-0 text-2xs tabular-nums text-content-muted">
        {durationMs === undefined ? '—' : formatDuration(durationMs)}
      </span>
    </li>
  )
}

interface BannerLook {
  icon: LucideIcon
  title: string
  className: string
  iconClassName: string
}

const BANNERS: Record<RunStatus, BannerLook> = {
  idle: {
    icon: CircleSlash,
    title: 'No run',
    className: 'border-line bg-surface-sunken',
    iconClassName: 'text-content-subtle',
  },
  cancelled: {
    icon: CircleStop,
    title: 'Run cancelled',
    className: 'border-state-warning/40 bg-state-warning/10',
    iconClassName: 'text-state-warning',
  },
  connecting: {
    icon: Rocket,
    title: 'Contacting the runner',
    className: 'border-brand-500/30 bg-brand-500/10',
    iconClassName: 'text-brand-500',
  },
  running: {
    icon: Rocket,
    title: 'Running',
    className: 'border-brand-500/30 bg-brand-500/10',
    iconClassName: 'text-brand-500',
  },
  success: {
    icon: CircleCheck,
    title: 'Run finished',
    className: 'border-state-success/40 bg-state-success/10',
    iconClassName: 'text-state-success',
  },
  skipped: {
    icon: CircleSlash,
    title: 'Stopped early',
    className: 'border-state-info/40 bg-state-info/10',
    iconClassName: 'text-state-info',
  },
  error: {
    icon: CircleX,
    title: 'Run failed',
    className: 'border-state-danger/40 bg-state-danger/10',
    iconClassName: 'text-state-danger',
  },
}

function StatusBanner({ run, mode }: { run: RunResult; mode: RunMode }) {
  const look = BANNERS[run.status]
  const Icon = look.icon
  const pending = run.status === 'running' || run.status === 'connecting'
  const title =
    mode === 'validate' && run.status === 'success'
      ? 'Configuration is valid'
      : mode === 'validate' && run.status === 'error'
        ? 'Configuration rejected'
        : look.title
  // A failure carries the runner's own words, which can be a whole stack trace:
  // it goes in a scrollable card, not in the banner line, so a long one cannot
  // push the metrics and the logs off the panel.
  const failure = run.status === 'error' || run.status === 'cancelled' ? (run.error ?? null) : null
  const detail = failure ? null : bannerDetail(run, mode)

  return (
    <div
      className={cn('flex items-start gap-2.5 rounded-xl border px-3 py-2.5', look.className)}
    >
      {pending ? (
        <Spinner className="mt-0.5 h-3.5 w-3.5 text-brand-500" />
      ) : (
        <Icon className={cn('mt-0.5 h-3.5 w-3.5 shrink-0', look.iconClassName)} aria-hidden />
      )}
      <div className="min-w-0 flex-1 space-y-1">
        <p className="text-xs font-medium text-content">{title}</p>
        {detail && (
          <p className="break-words text-2xs leading-relaxed text-content-muted">{detail}</p>
        )}
        {failure && (
          <ErrorCard
            message={failure}
            tone={run.status === 'cancelled' ? 'warning' : 'danger'}
            className="bg-surface/70"
          />
        )}
      </div>
    </div>
  )
}

function bannerDetail(run: RunResult, mode: RunMode): string | null {
  switch (run.status) {
    case 'error':
      return 'The runner returned an error.'
    case 'cancelled':
      return 'The run was stopped before it finished.'
    case 'skipped':
      return 'A stop_if_empty step matched, so nothing was written.'
    case 'success':
      return mode === 'validate'
        ? 'The runner parsed the pipeline. Nothing was executed.'
        : (run.pipelineName ?? 'The pipeline completed.')
    case 'idle':
      return 'The request was aborted before the runner answered.'
    default:
      return run.pipelineName ?? null
  }
}

function Metric({
  icon: Icon,
  label,
  value,
}: {
  icon: LucideIcon
  label: string
  value: string
}) {
  return (
    <div className="rounded-xl border border-line bg-surface p-2.5">
      <p className="flex items-center gap-1.5 text-2xs text-content-subtle">
        <Icon className="h-3.5 w-3.5" aria-hidden />
        {label}
      </p>
      <p className="mt-1 text-sm font-semibold tabular-nums text-content">{value}</p>
    </div>
  )
}

/**
 * The output preview, collapsible so a wide result table does not push the logs
 * and metrics off screen. Starts collapsed so the run summary, validations and
 * logs stay on one screen; the header keeps the shape (rows × columns) visible,
 * so opening it is a deliberate act rather than the default cost of every run.
 */
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
        <span className="font-medium">Preview</span>
        <span className="text-content-subtle">
          {rows.length}
          {truncated ? '+' : ''} row{rows.length === 1 && !truncated ? '' : 's'} ×{' '}
          {columns.length} col{columns.length === 1 ? '' : 's'}
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

/**
 * The three log windows a run produces, behind one collapsible section.
 *
 * They are genuinely different streams, not a filter over one: `pipeline` is the
 * framework's own structured log, `output` is whatever the pipeline printed
 * (the `debug` transformation's `count`/`show` tables), and `spark` is the JVM's
 * stderr — where the real Spark stack trace or a Windows winutils failure shows
 * up, which never reaches the Python logger. A run that "worked" but wrote
 * nothing is usually explained in the Spark window, so it is worth its own tab.
 */
const LOG_TABS = [
  { id: 'pipeline', label: 'Pipeline', hint: 'The framework log' },
  { id: 'output', label: 'Output', hint: 'debug / show output' },
  { id: 'spark', label: 'Spark', hint: 'JVM stderr (log4j)' },
] as const

type LogTabId = (typeof LOG_TABS)[number]['id']

function tabFor(line: RunLogLine): LogTabId {
  if (line.source === 'spark') return 'spark'
  if (line.source === 'stdout') return 'output'
  return 'pipeline'
}

/**
 * "Transformation started" says nothing on its own. The runner ships the step's
 * `type`, `index` and `total` in the log context, so spell them out: which
 * transformation, and where it sits in the pipeline.
 */
function describeLog(line: RunLogLine): { label: string | null; message: string } {
  const context = line.context
  if (!context || context.step !== true) return { label: null, message: line.message }

  const type = typeof context.type === 'string' ? context.type : null
  const index = typeof context.index === 'number' ? context.index : null
  const total = typeof context.total === 'number' ? context.total : null
  if (!type) return { label: null, message: line.message }

  const step = index !== null && total !== null ? `${index + 1}/${total}` : null
  const verb = line.message.includes('applied')
    ? 'applied'
    : line.message.includes('pulada')
      ? 'skipped'
      : 'running'

  return {
    label: step ? `step ${step}` : null,
    message: `${type} — ${verb}`,
  }
}

function LogStream({ logs }: { logs: RunLogLine[] }) {
  const [open, setOpen] = useState(false)
  const [tab, setTab] = useState<LogTabId>('pipeline')
  const failures = logs.filter((line) => line.level === 'error').length

  const byTab = useMemo(() => {
    const groups: Record<LogTabId, RunLogLine[]> = { pipeline: [], output: [], spark: [] }
    for (const line of logs) groups[tabFor(line)].push(line)
    return groups
  }, [logs])

  const visible = byTab[tab]

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
        <div className="space-y-2">
          <div role="tablist" aria-label="Log windows" className="flex gap-1">
            {LOG_TABS.map((entry) => {
              const count = byTab[entry.id].length
              const errors = byTab[entry.id].filter((l) => l.level === 'error').length
              const selected = entry.id === tab
              return (
                <button
                  key={entry.id}
                  type="button"
                  role="tab"
                  aria-selected={selected}
                  title={entry.hint}
                  onClick={() => setTab(entry.id)}
                  className={cn(
                    'inline-flex items-center gap-1.5 rounded-lg border px-2 py-1 text-2xs transition-colors',
                    selected
                      ? 'border-brand-500/40 bg-brand-500/10 text-content'
                      : 'border-line bg-surface text-content-muted hover:bg-surface-sunken',
                  )}
                >
                  {entry.label}
                  <span className={cn(errors > 0 ? 'text-state-danger' : 'text-content-subtle')}>
                    {count}
                  </span>
                </button>
              )
            })}
          </div>

          <div className="max-h-64 overflow-auto rounded-xl border border-line bg-surface-sunken p-2 font-mono text-2xs">
            {visible.length === 0 ? (
              <p className="px-1 py-2 text-content-subtle">
                Nothing on this stream for this run.
              </p>
            ) : (
              visible.map((line, index) => {
                const { label, message } = describeLog(line)
                return (
                  <div key={`${line.ts}-${index}`} className="flex gap-2 py-0.5">
                    <span className="shrink-0 text-content-subtle">
                      {formatClockTime(line.ts)}
                    </span>
                    <span
                      className={cn('w-16 shrink-0 uppercase', LOG_LEVEL_CLASS[line.level])}
                    >
                      {line.level}
                    </span>
                    {label && (
                      <span className="shrink-0 text-brand-500" aria-label="pipeline step">
                        {label}
                      </span>
                    )}
                    <span className="min-w-0 whitespace-pre-wrap break-words text-content-muted">
                      {message}
                    </span>
                  </div>
                )
              })
            )}
          </div>
        </div>
      )}
    </section>
  )
}

/* ----------------------------------------------------------------- helpers */

function isUnreachable(error: unknown): boolean {
  return isRunnerError(error) && error.kind === 'unreachable'
}

/** 401 (no token) and 403 (origin refused) are fixable here, not generic failures. */
function refusedStatus(error: unknown): AuthIssue['status'] | null {
  if (!isRunnerError(error)) return null
  if (error.status === 401) return 401
  return error.status === 403 ? 403 : null
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message
  return typeof error === 'string' ? error : 'The runner could not be reached'
}

function toStringList(value: ParamValue): string[] {
  if (Array.isArray(value)) return value
  if (typeof value === 'string' && value.trim().length > 0) {
    return value
      .split(',')
      .map((part) => part.trim())
      .filter((part) => part.length > 0)
  }
  return []
}

function toNumberValue(raw: string): ParamValue {
  if (raw.trim() === '') return ''
  const parsed = Number(raw)
  return Number.isNaN(parsed) ? raw : parsed
}

