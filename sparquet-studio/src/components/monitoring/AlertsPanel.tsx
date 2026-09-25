/**
 * The rules that raise an alert, what they are currently saying, and the log of
 * when each one started and stopped.
 *
 * Three lists in one place because they are one subject read at three distances:
 * what is wrong now, what is being watched, and what has been wrong lately. A
 * rule with no visible verdict is a rule nobody trusts, and a verdict with no
 * visible rule is a red dot nobody can act on.
 *
 * Reading needs `monitoring:Read`; creating, editing and deleting a rule needs
 * `monitoring:Manage`. Somebody who can only read sees the same three lists with
 * no form and no delete — turning an alert off is a decision with consequences
 * for everybody else on call, which is exactly why it is a separate permission.
 *
 * "Check now" is a read: it asks the runner to evaluate the rules against the
 * history it already has. Somebody who has just fixed a Job should not have to
 * watch a stale alert for a minute to find out whether it cleared.
 */

import { BellRing, Plus, RefreshCw, Trash2 } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'

import { Button, Field, Input, SectionTitle, Select, Spinner, Toggle, useConfirm } from '@/components/ui'
import {
  ANY_JOB,
  createMonitor,
  deleteMonitor,
  describeDraft,
  evaluateMonitors,
  fetchMonitorEvents,
  fetchMonitorStatus,
  isForbidden,
  listMonitors,
  updateMonitor,
  type Monitor,
  type MonitorBaseline,
  type MonitorDraft,
  type MonitorEvent,
  type MonitorKind,
  type MonitorState,
} from '@/lib/runner/monitoring'
import { relativeTime } from '@/lib/utils/format'
import { cn } from '@/lib/utils/cn'
import { useAuthStore } from '@/store/auth'
import { useSettingsStore } from '@/store/settings'

const KIND_OPTIONS = [
  { value: 'failed', label: 'A run failed' },
  { value: 'late', label: 'No successful run for a while' },
  { value: 'duration', label: 'A run took too long' },
  { value: 'volume', label: 'A run wrote too few rows' },
]

/**
 * What the threshold means, per kind, in the unit the runner reads it in.
 *
 * `late` is in minutes because silence is measured in hours; `duration` is in
 * milliseconds because that is what the history records and what the alert text
 * quotes back. Saying so on the field is cheaper than a unit conversion nobody
 * can see.
 */
const THRESHOLD_LABEL: Record<MonitorKind, { absolute: string; median: string }> = {
  failed: { absolute: 'Consecutive failures', median: 'Consecutive failures' },
  late: { absolute: 'Minutes without a success', median: 'Minutes without a success' },
  duration: { absolute: 'Milliseconds', median: 'Times its median' },
  volume: { absolute: 'Rows', median: 'Fraction of its median' },
}

/** Only two kinds can reasonably be judged against the Job's own past. */
const SUPPORTS_MEDIAN: Record<MonitorKind, boolean> = {
  failed: false,
  late: false,
  duration: true,
  volume: true,
}

const DEFAULT_DRAFT: MonitorDraft = {
  kind: 'failed',
  jobId: ANY_JOB,
  threshold: 1,
  baseline: 'absolute',
  name: '',
}

/** Sensible starting numbers, so switching the kind does not leave a rule that
 *  reads "no successful run in 1 minute". */
const DEFAULT_THRESHOLD: Record<MonitorKind, { absolute: number; median: number }> = {
  failed: { absolute: 1, median: 1 },
  late: { absolute: 120, median: 120 },
  duration: { absolute: 300_000, median: 2 },
  volume: { absolute: 1, median: 0.5 },
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function parsedTime(value: string | null): number {
  if (!value) return 0
  const parsed = Date.parse(value)
  return Number.isFinite(parsed) ? parsed : 0
}

export function AlertsPanel() {
  const url = useSettingsStore((state) => state.runnerUrl)
  const token = useSettingsStore((state) => state.runnerToken)
  const can = useAuthStore((state) => state.can)
  const [confirm, confirmDialog] = useConfirm()

  const [monitors, setMonitors] = useState<Monitor[]>([])
  const [states, setStates] = useState<MonitorState[]>([])
  const [events, setEvents] = useState<MonitorEvent[]>([])
  const [draft, setDraft] = useState<MonitorDraft>(DEFAULT_DRAFT)
  const [adding, setAdding] = useState(false)
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState(false)
  const [failure, setFailure] = useState('')
  const [forbidden, setForbidden] = useState(false)

  const manage = can('monitoring:Manage')

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const [rules, status, log] = await Promise.all([
        listMonitors(url, token),
        fetchMonitorStatus(url, {}, token),
        fetchMonitorEvents(url, { limit: 30 }, token),
      ])
      setMonitors(rules)
      setStates(status)
      setEvents(log)
      setFailure('')
      setForbidden(false)
    } catch (error) {
      setMonitors([])
      setStates([])
      setEvents([])
      setForbidden(isForbidden(error))
      setFailure(isForbidden(error) ? '' : messageOf(error))
    } finally {
      setLoading(false)
    }
  }, [token, url])

  useEffect(() => {
    void load()
  }, [load])

  const act = useCallback(
    async (work: () => Promise<unknown>) => {
      setBusy(true)
      try {
        await work()
        await load()
      } catch (error) {
        setFailure(messageOf(error))
      } finally {
        setBusy(false)
      }
    },
    [load],
  )

  const firing = useMemo(
    () =>
      states
        .filter((state) => state.firing)
        .sort((a, b) => parsedTime(b.since) - parsedTime(a.since)),
    [states],
  )

  const setKind = useCallback((kind: MonitorKind) => {
    setDraft((current) => {
      const baseline: MonitorBaseline = SUPPORTS_MEDIAN[kind] ? current.baseline ?? 'absolute' : 'absolute'
      return { ...current, kind, baseline, threshold: DEFAULT_THRESHOLD[kind][baseline] }
    })
  }, [])

  const setBaseline = useCallback((baseline: MonitorBaseline) => {
    setDraft((current) => ({
      ...current,
      baseline,
      threshold: DEFAULT_THRESHOLD[current.kind][baseline],
    }))
  }, [])

  if (!can('monitoring:Read') || forbidden) {
    return (
      <div className="space-y-2">
        <SectionTitle>Alerts</SectionTitle>
        <p className="rounded-lg border border-dashed border-line px-3 py-6 text-center text-2xs leading-relaxed text-content-subtle">
          Reading the alerts needs <code>monitoring:Read</code>. Ask an administrator of this
          runner for the permission — the rules keep running either way.
        </p>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <section className="space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <SectionTitle>Firing now</SectionTitle>
          <Button
            size="xs"
            variant="ghost"
            disabled={busy}
            onClick={() => void act(() => evaluateMonitors(url, token))}
            icon={<RefreshCw className={cn('h-3 w-3', (loading || busy) && 'animate-spin')} />}
            title="Evaluate every rule against the history right now"
          >
            Check now
          </Button>
        </div>

        {loading && monitors.length === 0 ? (
          <div className="flex items-center justify-center py-6">
            <Spinner className="h-4 w-4" />
          </div>
        ) : failure ? (
          <p className="text-2xs leading-relaxed text-content-subtle">{failure}</p>
        ) : firing.length === 0 ? (
          <p className="flex items-center justify-center gap-2 rounded-lg border border-dashed border-line px-3 py-6 text-2xs text-content-subtle">
            <BellRing className="h-3.5 w-3.5" aria-hidden />
            {monitors.length === 0
              ? 'No rules yet. Add one below and the runner starts watching.'
              : 'Nothing is firing.'}
          </p>
        ) : (
          <ul className="space-y-2">
            {firing.map((state) => (
              <li
                key={`${state.monitorId}:${state.jobId}`}
                className="rounded-lg border border-state-danger/40 bg-state-danger/5 px-3 py-2 text-2xs"
              >
                <div className="flex flex-wrap items-baseline justify-between gap-2">
                  <span className="font-medium text-state-danger">
                    {state.jobName ?? state.jobId}
                  </span>
                  <span className="text-content-subtle">
                    since {relativeTime(parsedTime(state.since))}
                  </span>
                </div>
                <p className="mt-0.5 leading-relaxed">{state.reason}</p>
                {state.rule ? (
                  <p className="mt-0.5 text-content-subtle">
                    {state.name ? `${state.name} — ` : ''}
                    {state.rule}
                  </p>
                ) : null}
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <SectionTitle>Rules</SectionTitle>
          {manage ? (
            <Button
              size="xs"
              variant="ghost"
              onClick={() => setAdding((value) => !value)}
              icon={<Plus className="h-3 w-3" />}
            >
              {adding ? 'Cancel' : 'New rule'}
            </Button>
          ) : null}
        </div>

        {adding && manage ? (
          <form
            className="space-y-3 rounded-lg border border-line p-3"
            onSubmit={(event) => {
              event.preventDefault()
              void act(async () => {
                await createMonitor(
                  { ...draft, name: draft.name?.trim() ? draft.name.trim() : null },
                  url,
                  token,
                )
                setDraft(DEFAULT_DRAFT)
                setAdding(false)
              })
            }}
          >
            <div className="grid gap-3 sm:grid-cols-2">
              <Field label="When">
                <Select
                  value={draft.kind}
                  onValueChange={(value) => setKind(value as MonitorKind)}
                  options={KIND_OPTIONS}
                  className="h-7 text-2xs"
                  ariaLabel="What the rule watches"
                />
              </Field>
              <Field
                label="For"
                help="Leave as * and the rule covers every Job in the library, including ones added later."
              >
                <Input
                  value={draft.jobId ?? ANY_JOB}
                  onChange={(event) => setDraft({ ...draft, jobId: event.target.value })}
                  className="h-7 text-2xs"
                  aria-label="Which Job the rule watches"
                />
              </Field>
              <Field label={THRESHOLD_LABEL[draft.kind][draft.baseline ?? 'absolute']}>
                <Input
                  type="number"
                  step="any"
                  min="0"
                  value={String(draft.threshold ?? 1)}
                  onChange={(event) =>
                    setDraft({ ...draft, threshold: Number(event.target.value) })
                  }
                  className="h-7 text-2xs"
                  aria-label="Threshold"
                />
              </Field>
              {SUPPORTS_MEDIAN[draft.kind] ? (
                <Field
                  label="Compared against"
                  help="The median ignores one freak run, which the mean would let hide the next one."
                >
                  <Select
                    value={draft.baseline ?? 'absolute'}
                    onValueChange={(value) => setBaseline(value as MonitorBaseline)}
                    options={[
                      { value: 'median', label: "This Job's own median" },
                      { value: 'absolute', label: 'A fixed number' },
                    ]}
                    className="h-7 text-2xs"
                    ariaLabel="What the threshold is measured against"
                  />
                </Field>
              ) : null}
              <Field label="Name" help="Optional. Shown on the alert and sent to the webhook.">
                <Input
                  value={draft.name ?? ''}
                  onChange={(event) => setDraft({ ...draft, name: event.target.value })}
                  placeholder="Anything that fails"
                  className="h-7 text-2xs"
                  aria-label="Name"
                />
              </Field>
            </div>

            <p className="text-2xs leading-relaxed text-content-subtle">
              {describeDraft(draft)}
            </p>

            <Button type="submit" size="xs" disabled={busy}>
              Add rule
            </Button>
          </form>
        ) : null}

        {monitors.length === 0 && !loading ? (
          <p className="rounded-lg border border-dashed border-line px-3 py-6 text-center text-2xs text-content-subtle">
            Nothing is being watched yet.
          </p>
        ) : (
          <ul className="divide-y divide-line rounded-lg border border-line">
            {monitors.map((monitor) => {
              const hits = states.filter(
                (state) => state.monitorId === monitor.id && state.firing,
              ).length
              return (
                <li key={monitor.id} className="flex items-start gap-3 px-3 py-2 text-2xs">
                  <div className="min-w-0 flex-1">
                    <p className={cn('truncate', !monitor.enabled && 'text-content-subtle')}>
                      {monitor.name ?? monitor.rule}
                    </p>
                    <p className="text-content-subtle">{monitor.rule}</p>
                    {hits > 0 ? (
                      <p className="text-state-danger">
                        firing for {hits} {hits === 1 ? 'Job' : 'Jobs'}
                      </p>
                    ) : null}
                  </div>
                  {manage ? (
                    <>
                      <Toggle
                        checked={monitor.enabled}
                        onCheckedChange={(checked) =>
                          void act(() => updateMonitor(monitor.id, { enabled: checked }, url, token))
                        }
                        label="Enabled"
                      />
                      <button
                        type="button"
                        className="rounded p-1 text-content-subtle transition-colors hover:text-state-danger"
                        title="Delete this rule"
                        onClick={() => {
                          void (async () => {
                            const ok = await confirm({
                              title: 'Delete this rule?',
                              // The verdicts and the transition log go with it:
                              // an event whose rule is gone is a timestamp
                              // nobody can read.
                              message: `“${monitor.name ?? monitor.rule}” stops being checked, and the alerts it raised stop being listed. The runs themselves stay in the history.`,
                              confirmLabel: 'Delete',
                              confirmName: monitor.name ?? monitor.rule,
                              variant: 'danger',
                            })
                            if (ok) await act(() => deleteMonitor(monitor.id, url, token))
                          })()
                        }}
                      >
                        <Trash2 className="h-3.5 w-3.5" aria-hidden />
                        <span className="sr-only">Delete</span>
                      </button>
                    </>
                  ) : null}
                </li>
              )
            })}
          </ul>
        )}
      </section>

      <section className="space-y-3">
        <SectionTitle>Recent transitions</SectionTitle>
        <p className="text-2xs leading-relaxed text-content-subtle">
          Only the moments a rule changed its mind — started firing, or cleared. A rule that
          has been firing all week is one line here, not ten thousand.
        </p>
        {events.length === 0 ? (
          <p className="rounded-lg border border-dashed border-line px-3 py-6 text-center text-2xs text-content-subtle">
            Nothing has started or stopped firing yet.
          </p>
        ) : (
          <ul className="divide-y divide-line rounded-lg border border-line">
            {events.map((event) => (
              <li key={event.id} className="flex items-start gap-3 px-3 py-2 text-2xs">
                <span
                  className={cn(
                    'mt-1 h-1.5 w-1.5 shrink-0 rounded-full',
                    event.firing ? 'bg-state-danger' : 'bg-node-output',
                  )}
                  aria-hidden
                />
                <div className="min-w-0 flex-1">
                  <p className="truncate">{event.jobId}</p>
                  <p className="text-content-subtle">{event.reason}</p>
                </div>
                <span className="shrink-0 text-content-subtle">
                  {relativeTime(parsedTime(event.at))}
                </span>
              </li>
            ))}
          </ul>
        )}
      </section>

      {confirmDialog}
    </div>
  )
}
