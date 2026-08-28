/**
 * One past execution, in full: every job it ran, every step of each, and every
 * log line the runner recorded for them.
 *
 * The history list stays a list — a run has far more detail than a sidebar row
 * can hold, so opening one moves it here, where the stages, the steps and the
 * logs each get the room they need.
 */

import { Eye, Info, ListTree, ScrollText } from 'lucide-react'
import { useEffect, useMemo, useState, type ReactNode } from 'react'

import { RunLogViewer, type RunLogSource } from '@/components/history/RunLogViewer'
import { StatusIcon, formatTimestamp, statusTone } from '@/components/history/status'
import { Badge, Button, ErrorCard, Modal, Segmented, Spinner } from '@/components/ui'
import { isRunnerError } from '@/lib/runner/client'
import { isErrorText, sameErrorText } from '@/lib/runner/errorText'
import { getJobRunConfig, getRun } from '@/lib/runner/history'
import { normalizeStepScope, stepDetails, stepLabel } from '@/lib/runner/stepNodes'
import { cn } from '@/lib/utils/cn'
import { formatCount, formatDuration, plural } from '@/lib/utils/format'
import type { RunCharge } from '@/types/credits'
import type {
  JobRunRecord,
  LineageDataset,
  PipelineRunRecord,
  StepRunRecord,
} from '@/types/history'

type DetailTab = 'steps' | 'logs' | 'details'

export interface RunDetailDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  /** The execution to show. A list row carries no `jobs`, so the id is enough. */
  runId: string | null
  runnerUrl: string
  runnerToken?: string
  /** Preselects the stage that belongs to this Studio Job, on a pipeline run. */
  focusJobId?: string
  /** Paints one job execution onto the canvas. Omitted where there is no canvas. */
  onViewJobRun?: (run: PipelineRunRecord, jobRun: JobRunRecord) => void
  viewActionLabel?: string
  /** The job execution already on the canvas, so the action can say so. */
  viewingJobRunId?: string | null
}

export function RunDetailDialog({
  open,
  onOpenChange,
  runId,
  runnerUrl,
  runnerToken,
  focusJobId,
  onViewJobRun,
  viewActionLabel = 'View on canvas',
  viewingJobRunId,
}: RunDetailDialogProps) {
  const [run, setRun] = useState<PipelineRunRecord | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selectedJobRunId, setSelectedJobRunId] = useState<string | null>(null)
  const [tab, setTab] = useState<DetailTab>('steps')

  useEffect(() => {
    if (!open || !runId) return
    const controller = new AbortController()
    setRun(null)
    setError(null)
    setLoading(true)
    getRun(runnerUrl, runId, controller.signal, runnerToken)
      .then((result) => {
        if (controller.signal.aborted) return
        setRun(result)
        // A pipeline run opened from a Job's history should land on that Job's
        // stage, not on the first one — that is the stage the user asked about.
        const focused = focusJobId ? result?.jobs.find((job) => job.jobId === focusJobId) : undefined
        setSelectedJobRunId((focused ?? result?.jobs[0])?.id ?? null)
      })
      .catch((err: unknown) => {
        if (controller.signal.aborted) return
        setError(isRunnerError(err) ? err.message : 'Failed to load this execution.')
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false)
      })
    return () => controller.abort()
  }, [open, runId, runnerUrl, runnerToken, focusJobId])

  const selected = run?.jobs.find((job) => job.id === selectedJobRunId) ?? run?.jobs[0] ?? null

  /**
   * Every stage of the execution, so the Logs tab reads like the live panel did.
   *
   * A pipeline run printed one stream while it ran; opening it afterwards has to
   * show that same stream, not whichever stage happens to be selected. A job run
   * has one stage and the merge is a no-op.
   */
  const logSources = useMemo<RunLogSource[]>(() => {
    if (!run) return []
    if (run.kind === 'pipeline') {
      return run.jobs.map((job, index) => ({
        id: job.id,
        label: job.name ?? job.jobId ?? `stage ${index + 1}`,
      }))
    }
    return selected ? [{ id: selected.id, label: selected.name ?? undefined }] : []
  }, [run, selected])

  /**
   * The same failure travels up: a step fails, its job carries the message, and
   * the run carries it again. Printed at all three levels it reads as three
   * problems. So the deepest level actually on screen keeps it and the ones above
   * stay quiet — the tab matters because the step cards only exist while the
   * Steps tab is open.
   */
  const stepErrors =
    tab === 'steps' && selected
      ? selected.steps.flatMap((step) => [step.errorMessage, step.errorDetails]).filter(isErrorText)
      : []
  const shownByStep = (message: string | null): boolean =>
    stepErrors.some((candidate) => sameErrorText(candidate, message))
  const showJobError = Boolean(selected?.error) && !shownByStep(selected?.error ?? null)
  const showRunError =
    Boolean(run?.error) &&
    !shownByStep(run?.error ?? null) &&
    !(showJobError && sameErrorText(selected?.error ?? null, run?.error ?? null))

  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      size="xl"
      title={run?.name ?? 'Execution'}
      description={run ? runSubtitle(run) : undefined}
    >
      <div className="flex h-[62vh] min-h-0 flex-col gap-3">
        {loading && (
          <div className="flex items-center gap-2 text-xs text-content-subtle">
            <Spinner className="h-4 w-4" /> Loading this execution…
          </div>
        )}

        {error && <ErrorCard message={error} />}

        {!loading && !error && !run && (
          <p className="text-xs text-content-subtle">
            This execution is no longer in the runner&apos;s history.
          </p>
        )}

        {run && (
          <>
            <div className="flex flex-wrap items-center gap-2 text-2xs text-content-subtle">
              <StatusIcon status={run.status} />
              <Badge tone={statusTone(run.status)}>{run.status}</Badge>
              {run.kind === 'pipeline' && <Badge tone="neutral">pipeline</Badge>}
              <span>{formatTimestamp(run.startedAt)}</span>
              <span>·</span>
              <span>{formatDuration(run.durationMs ?? undefined)}</span>
              <span className="ml-auto font-mono">{run.id}</span>
            </div>

            {showRunError && run.error && (
              <ErrorCard message={run.error} tone={toneOf(run.status)} size="sm" />
            )}

            <div className="flex min-h-0 flex-1 gap-3">
              {run.jobs.length > 1 && (
                <ul className="scroll-area w-48 shrink-0 space-y-1 border-r border-line pr-2">
                  {run.jobs.map((job, index) => (
                    <li key={job.id}>
                      <button
                        type="button"
                        onClick={() => setSelectedJobRunId(job.id)}
                        className={cn(
                          'flex w-full items-center gap-2 rounded-lg px-2 py-1.5 text-left text-2xs',
                          job.id === selected?.id
                            ? 'bg-surface-sunken text-content'
                            : 'text-content-muted hover:bg-surface-sunken/60',
                        )}
                      >
                        <StatusIcon status={job.status} className="h-3.5 w-3.5" />
                        <span className="min-w-0 flex-1 truncate">
                          {job.name ?? job.jobId ?? `stage ${index + 1}`}
                        </span>
                        <span className="shrink-0 text-content-subtle">
                          {formatDuration(job.durationMs ?? undefined)}
                        </span>
                      </button>
                    </li>
                  ))}
                </ul>
              )}

              {selected && (
                <div className="flex min-h-0 flex-1 flex-col gap-2">
                  <JobHeader
                    run={run}
                    job={selected}
                    onViewJobRun={onViewJobRun}
                    viewActionLabel={viewActionLabel}
                    viewing={selected.id === viewingJobRunId}
                    showError={showJobError}
                  />

                  <Segmented
                    size="sm"
                    ariaLabel="What to show about this job execution"
                    value={tab}
                    onChange={setTab}
                    options={[
                      {
                        value: 'steps',
                        label: (
                          <span className="flex items-center gap-1">
                            <ListTree className="h-3 w-3" /> Steps ({selected.steps.length})
                          </span>
                        ),
                      },
                      {
                        value: 'logs',
                        label: (
                          <span className="flex items-center gap-1">
                            <ScrollText className="h-3 w-3" /> Logs
                          </span>
                        ),
                      },
                      {
                        value: 'details',
                        label: (
                          <span className="flex items-center gap-1">
                            <Info className="h-3 w-3" /> Details
                          </span>
                        ),
                      },
                    ]}
                    className="self-start"
                  />

                  {tab === 'steps' && <StepList steps={selected.steps} />}
                  {tab === 'logs' && (
                    <RunLogViewer
                      key={run.id}
                      sources={logSources}
                      runnerUrl={runnerUrl}
                      runnerToken={runnerToken}
                      className="flex-1"
                    />
                  )}
                  {tab === 'details' && (
                    <JobDetails
                      run={run}
                      job={selected}
                      runnerUrl={runnerUrl}
                      runnerToken={runnerToken}
                    />
                  )}
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </Modal>
  )
}

/** `Started 12 Mar, 14:03:22 · 4.2 s · 2 stages` — the run in one line. */
function runSubtitle(run: PipelineRunRecord): string {
  const parts = [formatTimestamp(run.startedAt), formatDuration(run.durationMs ?? undefined)]
  if (run.kind === 'pipeline') parts.push(plural(run.jobs.length, 'stage'))
  return parts.join(' · ')
}

function toneOf(status: JobRunRecord['status']): 'danger' | 'warning' {
  return status === 'cancelled' ? 'warning' : 'danger'
}

function JobHeader({
  run,
  job,
  onViewJobRun,
  viewActionLabel,
  viewing,
  showError,
}: {
  run: PipelineRunRecord
  job: JobRunRecord
  onViewJobRun?: (run: PipelineRunRecord, jobRun: JobRunRecord) => void
  viewActionLabel: string
  viewing: boolean
  /** False when a step below already shows this very message. */
  showError: boolean
}) {
  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2 text-2xs text-content-subtle">
        <span className="text-xs font-medium text-content">
          {job.name ?? job.jobId ?? 'job'}
        </span>
        <Badge tone={statusTone(job.status)}>{job.status}</Badge>
        {job.rowsRead !== null && <span>{formatCount(job.rowsRead)} rows in</span>}
        {job.rowsWritten !== null && <span>{formatCount(job.rowsWritten)} rows out</span>}
        <span>{formatDuration(job.durationMs ?? undefined)}</span>
        {onViewJobRun && (
          <Button
            size="xs"
            variant="ghost"
            className="ml-auto"
            icon={<Eye className="h-3 w-3" />}
            // A skipped stage never ran a step, so there is no state to paint.
            disabled={job.status === 'skipped'}
            onClick={() => onViewJobRun(run, job)}
          >
            {viewing ? 'On canvas' : viewActionLabel}
          </Button>
        )}
      </div>
      {showError && job.error && (
        <ErrorCard message={job.error} tone={toneOf(job.status)} size="sm" />
      )}
    </div>
  )
}

const LAUNCH_LABELS: Record<string, string> = {
  manual: 'Manually',
  scheduled: 'Scheduled',
  api: 'API',
}

/**
 * The identity of one job execution: who asked for it, when it ran, and which
 * datasets it touched.
 *
 * Both levels are here on purpose — the run id addresses the whole execution
 * (`Pipeline → Run id`) while the job run id addresses this stage inside it, and
 * a report or a support question needs whichever one it is about.
 */
function JobDetails({
  run,
  job,
  runnerUrl,
  runnerToken,
}: {
  run: PipelineRunRecord
  job: JobRunRecord
  runnerUrl: string
  runnerToken?: string
}) {
  return (
    <div className="scroll-area min-h-0 flex-1 space-y-4 pr-1">
      <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-4 gap-y-1.5 text-2xs">
        <Detail label="Status">
          <span className="flex items-center gap-1.5">
            <StatusIcon status={job.status} className="h-3.5 w-3.5" />
            <Badge tone={statusTone(job.status)}>{job.status}</Badge>
          </span>
        </Detail>
        <Detail label="Job ID" mono>
          {job.jobId ?? '—'}
        </Detail>
        <Detail label="Job run ID" mono>
          {job.id}
        </Detail>
        <Detail label="Run ID" mono>
          {run.id}
        </Detail>
        {run.kind === 'pipeline' && (
          <Detail label="Pipeline ID" mono>
            {run.pipelineId ?? '—'}
          </Detail>
        )}
        <Detail label="Run as">{run.runAs ?? '—'}</Detail>
        <Detail label="Launched">
          {run.launched ? (LAUNCH_LABELS[run.launched] ?? run.launched) : '—'}
        </Detail>
        <Detail label="Started">{formatTimestamp(job.startedAt)}</Detail>
        <Detail label="Ended">{formatTimestamp(job.finishedAt)}</Detail>
        <Detail label="Duration">{formatDuration(job.durationMs ?? undefined)}</Detail>
        {job.rowsRead !== null && <Detail label="Rows in">{formatCount(job.rowsRead)}</Detail>}
        {job.rowsWritten !== null && <Detail label="Rows out">{formatCount(job.rowsWritten)}</Detail>}
        <Detail label="Config version" mono>
          {job.configHash ? shortHash(job.configHash) : '—'}
        </Detail>
        <Detail label="Credits">{describeCharge(job.credits)}</Detail>
      </dl>

      <ConfigVersion job={job} runnerUrl={runnerUrl} runnerToken={runnerToken} />

      <div className="space-y-1.5">
        <p className="text-2xs font-semibold uppercase tracking-wider text-content-subtle">
          Lineage
        </p>
        {job.lineage ? (
          <div className="grid gap-3 sm:grid-cols-2">
            <LineageSide title="Inputs" datasets={job.lineage.inputs} />
            <LineageSide title="Outputs" datasets={job.lineage.outputs} />
          </div>
        ) : (
          <p className="text-2xs text-content-subtle">
            No lineage recorded — the job named no dataset, or it ran before the runner started
            recording it.
          </p>
        )}
      </div>
    </div>
  )
}

/** `sha256:1f4a…` — enough to compare two runs by eye; the full value is one click away. */
function shortHash(hash: string): string {
  const [algorithm, digest] = hash.split(':')
  return digest ? `${algorithm}:${digest.slice(0, 12)}` : hash.slice(0, 12)
}

/**
 * The version of the JSON this execution ran.
 *
 * A Job keeps being edited, so "which Job ran" does not answer "what ran". The
 * fingerprint above says whether two executions used the same configuration; this
 * fetches the configuration itself, on demand — it is far larger than everything
 * else on this panel, and most readers only want the hash.
 */
function ConfigVersion({
  job,
  runnerUrl,
  runnerToken,
}: {
  job: JobRunRecord
  runnerUrl: string
  runnerToken?: string
}) {
  const [config, setConfig] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [open, setOpen] = useState(false)

  // A different stage means a different configuration: drop what was fetched.
  useEffect(() => {
    setConfig(null)
    setError(null)
    setOpen(false)
  }, [job.id])

  const load = async () => {
    setOpen(true)
    if (config !== null || loading) return
    setLoading(true)
    setError(null)
    try {
      const stored = await getJobRunConfig(runnerUrl, job.id, undefined, runnerToken)
      if (stored?.config) {
        setConfig(JSON.stringify(stored.config, null, 2))
      } else {
        setError(
          'The runner did not keep this configuration — the run predates it, or the JSON was too large to store.',
        )
      }
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : String(loadError))
    } finally {
      setLoading(false)
    }
  }

  if (!job.configHash) return null

  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between gap-2">
        <p className="text-2xs font-semibold uppercase tracking-wider text-content-subtle">
          Configuration
        </p>
        <Button
          size="xs"
          variant="ghost"
          onClick={open ? () => setOpen(false) : load}
          disabled={loading}
        >
          {loading ? <Spinner className="h-3 w-3" /> : null}
          {open ? 'Hide JSON' : 'View the JSON that ran'}
        </Button>
      </div>
      {open && error && <p className="text-2xs text-state-danger">{error}</p>}
      {open && config && (
        <pre className="scroll-area max-h-64 overflow-auto rounded-md border border-line bg-surface-sunken p-2 font-mono text-2xs leading-relaxed text-content">
          {config}
        </pre>
      )}
    </div>
  )
}

/**
 * What this execution cost, in one line.
 *
 * The unit is a successful write to a cluster, so a run that failed before
 * writing shows as free rather than as unknown, and so does a local run — the
 * runner never charges for either. `applied: false` means the runner is metering
 * without enforcing: the count is real, the deduction did not happen.
 */
function describeCharge(charge: RunCharge | null): string {
  if (!charge || charge.writes === 0) return 'Free — nothing was written off this machine'
  const writes = `${charge.writes} ${charge.writes === 1 ? 'write' : 'writes'}`
  const parts = [`${charge.amount} for ${writes}`]
  if (charge.freeAmount > 0) parts.push(`${charge.freeAmount} from the free allowance`)
  if (charge.shortfall > 0) parts.push(`${charge.shortfall} unpaid`)
  if (!charge.applied) parts.push('metered only')
  return parts.join(' · ')
}

function Detail({
  label,
  mono,
  children,
}: {
  label: string
  mono?: boolean
  children: ReactNode
}) {
  return (
    <>
      <dt className="text-content-subtle">{label}</dt>
      <dd className={cn('min-w-0 break-all text-content', mono && 'select-all font-mono')}>
        {children}
      </dd>
    </>
  )
}

/**
 * What the job declared it would read or write, taken from the submitted JSON —
 * so it is what the run addressed, not proof that every write happened.
 */
function LineageSide({ title, datasets }: { title: string; datasets: LineageDataset[] }) {
  return (
    <div className="space-y-1">
      <p className="text-2xs font-medium text-content-muted">{title}</p>
      {datasets.length === 0 ? (
        <p className="text-2xs text-content-subtle">None.</p>
      ) : (
        <ul className="space-y-1">
          {datasets.map((dataset, index) => (
            <li
              key={`${dataset.role}-${dataset.address ?? dataset.format ?? index}`}
              className="rounded-lg border border-line bg-surface-sunken/40 px-2 py-1.5"
            >
              <div className="flex items-center gap-1.5 text-2xs text-content-subtle">
                <Badge tone="neutral">{dataset.role}</Badge>
                {dataset.format && <span>{dataset.format}</span>}
                {dataset.mode && <span>· {dataset.mode}</span>}
              </div>
              {dataset.address && (
                <p
                  className="mt-1 break-all font-mono text-2xs text-content"
                  title={dataset.address}
                >
                  {dataset.address}
                </p>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

/**
 * The steps of one job execution, numbered straight through.
 *
 * The first question about a run is how far it got, and a number answers that
 * where a lane name does not — so the count runs 1, 2, 3 across the whole job
 * while the lane stays as a heading above its steps. The row says what the step
 * is and nothing else: rows, format and path are in the tooltip, and the Details
 * tab is where they are read.
 */
function StepList({ steps }: { steps: StepRunRecord[] }) {
  const groups = useMemo(() => {
    let number = 0
    return groupByScope(steps).map((group) => ({
      scope: group.scope,
      steps: group.steps.map((step) => ({ step, number: (number += 1) })),
    }))
  }, [steps])

  if (steps.length === 0) {
    return (
      <p className="text-2xs text-content-subtle">
        This job execution recorded no steps — it never reached one.
      </p>
    )
  }

  return (
    <div className="scroll-area min-h-0 flex-1 space-y-3 pr-1">
      {groups.map((group) => (
        <div key={group.scope} className="space-y-1">
          <p className="text-2xs font-semibold uppercase tracking-wider text-content-subtle">
            {group.scope}
          </p>
          <ul className="space-y-1">
            {group.steps.map((entry) => (
              <StepRow key={entry.step.id} step={entry.step} number={entry.number} />
            ))}
          </ul>
        </div>
      ))}
    </div>
  )
}

function StepRow({ step, number }: { step: StepRunRecord; number: number }) {
  const [showDetails, setShowDetails] = useState(false)
  const summary = stepDetailSummary(step)
  // The stack trace behind the message. Folded away: it is the one thing here
  // long enough to bury every other step in the list.
  const details =
    isErrorText(step.errorDetails) && !sameErrorText(step.errorDetails, step.errorMessage)
      ? step.errorDetails
      : null

  return (
    <li className="space-y-1 rounded-lg border border-line bg-surface-sunken/40 px-2 py-1.5">
      <div
        className="flex items-center gap-2 text-2xs"
        title={summary ? `${stepTiming(step)} · ${summary}` : stepTiming(step)}
      >
        <span className="w-4 shrink-0 text-right font-mono tabular-nums text-content-subtle">
          {number}
        </span>
        <StatusIcon status={step.status} className="h-3.5 w-3.5" />
        <span className="min-w-0 flex-1 truncate text-content">{step.role ?? step.type}</span>
        <span className="shrink-0 text-content-subtle">
          {formatDuration(step.durationMs ?? undefined)}
        </span>
      </div>
      {isErrorText(step.errorMessage) && (
        <ErrorCard message={step.errorMessage} tone={toneOf(step.status)} size="sm" />
      )}
      {details && (
        <div className="space-y-1">
          <button
            type="button"
            onClick={() => setShowDetails((open) => !open)}
            className="text-2xs text-content-subtle underline underline-offset-2 hover:text-content"
          >
            {showDetails ? 'Hide details' : 'Show details'}
          </button>
          {showDetails && <ErrorCard message={details} tone={toneOf(step.status)} size="sm" />}
        </div>
      )}
    </li>
  )
}

function stepTiming(step: StepRunRecord): string {
  return `${stepLabel(step)} · started ${formatTimestamp(step.startedAt)}`
}

/** The one or two facts worth a single line: how many rows, and where they went. */
function stepDetailSummary(step: StepRunRecord): string | null {
  const details = stepDetails(step)
  const parts: string[] = []
  if (typeof details.rows === 'number') parts.push(`${formatCount(details.rows)} rows`)
  if (typeof details.format === 'string') parts.push(details.format)
  if (typeof details.path === 'string') parts.push(details.path)
  return parts.length > 0 ? parts.join(' · ') : null
}

function groupByScope(steps: StepRunRecord[]): { scope: string; steps: StepRunRecord[] }[] {
  const groups: { scope: string; steps: StepRunRecord[] }[] = []
  for (const step of steps) {
    const scope = normalizeStepScope(step.scope)
    const current = groups.find((group) => group.scope === scope)
    if (current) current.steps.push(step)
    else groups.push({ scope, steps: [step] })
  }
  return groups
}
