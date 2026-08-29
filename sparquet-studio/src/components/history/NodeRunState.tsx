/**
 * What one box did on the run the canvas is showing.
 *
 * The canvas can only carry a status and a duration per box. Everything else the
 * runner recorded about that step — the rows it moved, the path it wrote, the rule
 * verdict, the error that stopped it — lands here, so clicking a box in a past
 * execution answers "what happened HERE", not just "did it pass".
 */

import { AlertTriangle } from 'lucide-react'

import { StatusIcon, formatTimestamp, statusTone } from '@/components/history/status'
import { Badge, ErrorCard, SectionTitle } from '@/components/ui'
import { stepDetails, stepLabel } from '@/lib/runner/stepNodes'
import { formatDuration } from '@/lib/utils/format'
import { useEditorStore } from '@/store/editor'
import type { ExecutionStatus, StepRunRecord } from '@/types/history'
import type { StepStatus } from '@/types/studio'

/** The canvas words for a status, in the history panel's vocabulary. */
const AS_EXECUTION_STATUS: Record<StepStatus, ExecutionStatus> = {
  pending: 'pending',
  running: 'running',
  success: 'success',
  skipped: 'skipped',
  cancelled: 'cancelled',
  error: 'failed',
}

export function NodeRunState({ nodeId }: { nodeId: string }) {
  const runView = useEditorStore((state) => state.runView)
  const running = useEditorStore((state) => state.running)
  const status = useEditorStore((state) => state.stepStatus[nodeId])
  const duration = useEditorStore((state) => state.stepDuration[nodeId])
  const steps = useEditorStore((state) => state.stepRuns[nodeId])

  // Nothing ran, or this box takes no part in a run (a note, an orphan).
  if (status === undefined) return null
  // A box the run never reached says so on the canvas already, dimmed; repeating
  // "pending" here would add a section with nothing in it.
  if (status === 'pending' && !steps?.length) return null

  return (
    <section className="space-y-2">
      <SectionTitle
        action={
          duration !== undefined ? (
            <span className="text-2xs text-content-subtle">{formatDuration(duration)}</span>
          ) : undefined
        }
      >
        {running ? 'This run' : 'Last run'}
      </SectionTitle>

      <div className="card space-y-2 p-2.5">
        <div className="flex items-center gap-2 text-2xs">
          <StatusIcon status={AS_EXECUTION_STATUS[status]} className="h-3.5 w-3.5" />
          <Badge tone={statusTone(AS_EXECUTION_STATUS[status])}>
            {AS_EXECUTION_STATUS[status]}
          </Badge>
          {runView && (
            <span className="min-w-0 flex-1 truncate text-right text-content-subtle">
              {formatTimestamp(runView.startedAt)}
            </span>
          )}
        </div>

        {/* A live run reports a status and nothing else: the rows and paths only
            exist once the runner has written the step to its database. */}
        {steps?.length
          ? steps.map((step) => <StepDetail key={step.id} step={step} />)
          : running && (
              <p className="text-2xs text-content-subtle">
                Details are recorded when the run finishes.
              </p>
            )}

        {status === 'pending' && (
          <p className="flex items-center gap-1.5 text-2xs text-content-subtle">
            <AlertTriangle className="h-3 w-3 shrink-0" aria-hidden />
            This box was not reached by the run.
          </p>
        )}
      </div>
    </section>
  )
}

/**
 * One step row. A box can own several: a validation rule with three `targets`
 * runs three times, and each run is its own step.
 */
function StepDetail({ step }: { step: StepRunRecord }) {
  const details = stepDetails(step)
  const facts: [string, string][] = []
  if (typeof details.rows === 'number') facts.push(['rows', String(details.rows)])
  if (typeof details.format === 'string') facts.push(['format', details.format])
  if (typeof details.path === 'string') facts.push(['path', details.path])
  if (typeof details.passed === 'boolean') facts.push(['passed', String(details.passed)])
  if (typeof details.severity === 'string') facts.push(['severity', details.severity])

  return (
    <div className="space-y-1 border-t border-line pt-2 first:border-0 first:pt-0">
      <div className="flex items-center gap-2 text-2xs">
        <StatusIcon status={step.status} className="h-3 w-3" />
        <span className="min-w-0 flex-1 truncate text-content-muted">{stepLabel(step)}</span>
        {step.durationMs !== null && (
          <span className="shrink-0 text-content-subtle">{formatDuration(step.durationMs)}</span>
        )}
      </div>

      {facts.length > 0 && (
        <dl className="space-y-0.5 text-2xs">
          {facts.map(([key, value]) => (
            <div key={key} className="flex items-baseline gap-2">
              <dt className="w-14 shrink-0 text-content-subtle">{key}</dt>
              <dd className="min-w-0 flex-1 truncate font-mono text-content-muted" title={value}>
                {value}
              </dd>
            </div>
          ))}
        </dl>
      )}

      {step.errorMessage && (
        <ErrorCard
          message={step.errorMessage}
          tone={step.status === 'cancelled' ? 'warning' : 'danger'}
          size="sm"
        />
      )}
    </div>
  )
}
