/**
 * One month of executions: how many ran, how they ended, how long they took.
 *
 * The operational half of this screen. Credits answer "what did it cost", and
 * that question skips most of what happened: a charge exists only for a write
 * that landed away from this machine, so a month spent developing locally, or a
 * month where everything failed before writing anything, costs nothing and still
 * had a shape. "How much did we run" is the question people ask first, and until
 * now Billing could not answer it.
 *
 * It reads the same month as the rest of the screen and offers the same kind of
 * breakdown, so the two halves can be compared line by line: a Job with many runs
 * and no credits is a Job that writes locally, and a Job with few runs and many
 * credits is one that writes a lot each time. Neither is visible from one half
 * alone.
 *
 * Duration is reported three ways because one number lies. The average is moved
 * by a single stuck run, the median is the run somebody actually waits for, and
 * p95 is the one that ruins an evening — and the gap between the median and p95
 * is the tell that a pipeline is unpredictable rather than slow.
 */

import { useCallback, useEffect, useMemo, useState } from 'react'

import { SectionTitle, Segmented, Spinner } from '@/components/ui'
import { monthName, shareOf } from '@/lib/billing'
import { getRunMetrics } from '@/lib/runner/history'
import { formatCount, formatDuration } from '@/lib/utils/format'
import { cn } from '@/lib/utils/cn'
import { useLibraryStore } from '@/store/library'
import { useSettingsStore } from '@/store/settings'
import type { RunGroupBy, RunMetrics } from '@/types/history'

const GROUPS: { value: RunGroupBy; label: string }[] = [
  { value: 'pipeline', label: 'Pipeline' },
  { value: 'job', label: 'Job' },
  { value: 'workflow', label: 'Workflow' },
  { value: 'user', label: 'User' },
]

/** How many lines are shown before the rest is folded away. */
const VISIBLE = 8

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** The day of the month, for the axis under the bars. */
function dayNumber(day: string): string {
  return String(Number(day.slice(8, 10)))
}

export interface RunActivityProps {
  /** The month to read. Owned by the screen, because the chart above selects it too. */
  period: string
}

export function RunActivity({ period }: RunActivityProps) {
  const url = useSettingsStore((state) => state.runnerUrl)
  const token = useSettingsStore((state) => state.runnerToken)
  const workflows = useLibraryStore((state) => state.workflows)
  const jobs = useLibraryStore((state) => state.jobs)
  const pipelines = useLibraryStore((state) => state.pipelines)

  const [groupBy, setGroupBy] = useState<RunGroupBy>('pipeline')
  const [data, setData] = useState<RunMetrics | null>(null)
  const [loading, setLoading] = useState(true)
  const [expanded, setExpanded] = useState(false)
  const [failure, setFailure] = useState('')

  // The runner labels a group from its own catalog, which only knows the records
  // it has seen a run from. A record that still exists in this browser should
  // read by the name it has here now.
  const localNames = useMemo(() => {
    const names = new Map<string, string>()
    for (const workflow of workflows) names.set(workflow.id, workflow.name)
    for (const pipeline of pipelines) names.set(pipeline.id, pipeline.name)
    for (const job of jobs) names.set(job.id, job.name)
    return names
  }, [jobs, pipelines, workflows])

  const load = useCallback(async () => {
    setLoading(true)
    try {
      setData(await getRunMetrics(url, { groupBy, period }, undefined, token))
      setFailure('')
    } catch (error) {
      setFailure(messageOf(error))
      setData(null)
    } finally {
      setLoading(false)
    }
  }, [groupBy, period, token, url])

  useEffect(() => {
    void load()
  }, [load])

  // A different dimension is a different list; leaving it expanded from the last
  // one would open on a hidden scroll position.
  useEffect(() => {
    setExpanded(false)
  }, [groupBy, period])

  const days = data?.days ?? []
  const busiest = days.reduce((most, day) => Math.max(most, day.runs), 0)
  const rows = data?.groups ?? []
  const peak = rows.reduce((most, row) => Math.max(most, row.runs), 0)
  const shown = expanded ? rows : rows.slice(0, VISIBLE)
  const hidden = rows.length - shown.length
  const total = data?.total ?? 0

  return (
    <div className="space-y-4">
      <SectionTitle
        action={<span className="text-2xs text-content-subtle">{monthName(period)}</span>}
      >
        What actually ran
      </SectionTitle>

      {loading && data === null ? (
        <div className="flex items-center justify-center py-6">
          <Spinner className="h-4 w-4" />
        </div>
      ) : failure ? (
        <p className="text-2xs leading-relaxed text-content-subtle">{failure}</p>
      ) : (
        <>
          <dl className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <Stat label="Executions" value={formatCount(total)}>
              {data && data.other > 0
                ? `${formatCount(data.other)} still running or cancelled`
                : `${formatCount(data?.succeeded ?? 0)} succeeded`}
            </Stat>
            <Stat
              label="Failed"
              value={formatCount(data?.failed ?? 0)}
              tone={(data?.failed ?? 0) > 0 ? 'warning' : undefined}
            >
              {total > 0
                ? `${Math.round(shareOf(data?.failed ?? 0, total))}% of the month`
                : 'Nothing ran'}
            </Stat>
            <Stat
              label="Average run"
              value={
                data?.durationMsAvg === null || data?.durationMsAvg === undefined
                  ? '—'
                  : formatDuration(data.durationMsAvg)
              }
            >
              {data?.durationMsP50 === null || data?.durationMsP50 === undefined
                ? 'No run finished'
                : `${formatDuration(data.durationMsP50)} median`}
            </Stat>
            <Stat
              label="Slowest 5%"
              value={
                data?.durationMsP95 === null || data?.durationMsP95 === undefined
                  ? '—'
                  : formatDuration(data.durationMsP95)
              }
            >
              {`${formatDuration(data?.durationMsTotal ?? 0)} in total`}
            </Stat>
          </dl>

          {/* Every day of the month is drawn, including the empty ones: a series
              that skips them makes a quiet month and a busy one the same shape. */}
          <div className="space-y-1.5">
            <div className="flex h-16 items-end gap-px" role="presentation">
              {days.map((day) => {
                const height = busiest > 0 ? Math.max(shareOf(day.runs, busiest), 2) : 2
                const bad = day.runs > 0 ? shareOf(day.failed, day.runs) : 0
                return (
                  <span
                    key={day.day}
                    title={`${day.day}: ${day.runs} ${day.runs === 1 ? 'run' : 'runs'}${
                      day.failed > 0 ? `, ${day.failed} failed` : ''
                    }`}
                    className="flex min-w-0 flex-1 flex-col justify-end"
                  >
                    <span
                      className={cn(
                        'block w-full rounded-sm',
                        day.runs > 0 ? 'bg-brand-500' : 'bg-surface-sunken',
                      )}
                      style={{ height: `${height}%` }}
                    >
                      {/* Failures are the red foot of the day, not a second bar:
                          the eye reads one column per day either way. */}
                      {bad > 0 && (
                        <span
                          className="mt-auto block w-full rounded-b-sm bg-state-danger"
                          style={{ height: `${bad}%`, marginTop: `${100 - bad}%` }}
                        />
                      )}
                    </span>
                  </span>
                )
              })}
            </div>
            {days.length > 0 && (
              <div className="flex justify-between text-2xs tabular-nums text-content-subtle">
                <span>{dayNumber(days[0].day)}</span>
                <span>{dayNumber(days[days.length - 1].day)}</span>
              </div>
            )}
          </div>

          <Segmented
            value={groupBy}
            onChange={setGroupBy}
            options={GROUPS}
            size="sm"
            ariaLabel="Group executions by"
          />

          {rows.length === 0 ? (
            <p className="rounded-lg border border-dashed border-line px-3 py-6 text-center text-2xs text-content-subtle">
              Nothing ran in {monthName(period)}.
            </p>
          ) : (
            <ul className="space-y-1.5">
              {shown.map((row) => {
                const name =
                  (row.key ? localNames.get(row.key) : null) ?? row.label ?? row.key ?? '—'
                const width = shareOf(row.runs, peak)
                const bad = row.runs > 0 ? shareOf(row.failed, row.runs) : 0
                return (
                  <li key={row.key ?? '∅'} className="space-y-1">
                    <div className="flex items-baseline justify-between gap-3">
                      <span
                        className={cn(
                          'min-w-0 truncate text-xs',
                          row.key ? 'text-content' : 'italic text-content-subtle',
                        )}
                        title={row.key ?? undefined}
                      >
                        {name}
                      </span>
                      <span className="shrink-0 text-xs tabular-nums text-content">
                        {row.runs}
                        <span className="ml-1 text-2xs text-content-subtle">
                          {row.runs === 1 ? 'run' : 'runs'}
                          {row.durationMsAvg === null
                            ? ''
                            : ` · ${formatDuration(row.durationMsAvg)} avg`}
                          {row.failed > 0 ? ` · ${row.failed} failed` : ''}
                        </span>
                      </span>
                    </div>
                    <span
                      className="block h-1.5 overflow-hidden rounded-full bg-surface-sunken"
                      role="presentation"
                    >
                      <span
                        className="flex h-full flex-row-reverse rounded-full bg-brand-500"
                        style={{ width: `${width}%` }}
                      >
                        {/* The failed share is the red end of the line's bar. */}
                        <span
                          className="block h-full shrink-0 bg-state-danger"
                          style={{ width: `${bad}%` }}
                        />
                      </span>
                    </span>
                  </li>
                )
              })}
            </ul>
          )}

          {hidden > 0 && (
            <button
              type="button"
              onClick={() => setExpanded(true)}
              className="text-2xs text-content-muted underline-offset-2 hover:text-content hover:underline"
            >
              Show {hidden} more
            </button>
          )}
        </>
      )}

      <p className="text-2xs leading-relaxed text-content-subtle">
        Counted from the runner's execution history, not from the credits: a local
        run and a run that failed before writing cost nothing and still happened.
        Grouping by Job counts each stage of a Pipeline separately, which is why its
        numbers are larger.
      </p>
    </div>
  )
}

interface StatProps {
  label: string
  value: string
  tone?: 'warning'
  children?: React.ReactNode
}

function Stat({ label, value, tone, children }: StatProps) {
  return (
    <div className="space-y-0.5">
      <dt className="text-2xs uppercase tracking-wide text-content-subtle">{label}</dt>
      <dd
        className={cn(
          'text-lg font-medium tabular-nums',
          tone === 'warning' ? 'text-state-warning' : 'text-content',
        )}
      >
        {value}
      </dd>
      {children && <p className="text-2xs text-content-subtle">{children}</p>}
    </div>
  )
}
