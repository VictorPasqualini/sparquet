/**
 * Every schedule in the library, what the runner intends to do next, and what it
 * did last.
 *
 * The schedules themselves live in the records, so the browser already knows
 * what *should* happen. What only the runner knows is the next firing time and
 * the last run it started, and those are the two things somebody opens this to
 * see. A schedule the runner could not read is shown with its reason rather than
 * hidden — that row is a Job that silently never runs, which is the single most
 * useful thing this table can say.
 *
 * Reading needs `workspace:Read`, since a schedule is part of the record.
 * Evaluating needs `run:Execute`, because it starts executions: the button says
 * "Run what is due", not "Run", precisely because it fires nothing that is not
 * already overdue.
 */

import { AlertTriangle, CalendarClock, PlayCircle, RefreshCw } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'

import { Badge, Button, SectionTitle, Spinner } from '@/components/ui'
import {
  evaluateSchedules,
  isForbidden,
  listSchedules,
  type ScheduleEntry,
} from '@/lib/runner/scheduling'
import { describeCron } from '@/lib/scheduling/cron'
import { relativeTime } from '@/lib/utils/format'
import { cn } from '@/lib/utils/cn'
import { useAuthStore } from '@/store/auth'
import { useSettingsStore } from '@/store/settings'

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function parsedTime(value: string | null): number {
  if (!value) return 0
  const parsed = Date.parse(value)
  return Number.isFinite(parsed) ? parsed : 0
}

const MINUTE = 60_000
const HOUR = 60 * MINUTE
const DAY = 24 * HOUR

/**
 * How long until a schedule fires.
 *
 * `relativeTime` reads the past and would call tomorrow morning "just now", so
 * the one column that points forwards needs its own words. Past the second day
 * the clock time is what somebody actually wants, not a count of hours.
 */
export function timeUntil(at: number, now = Date.now()): string {
  if (!Number.isFinite(at) || at <= 0) return '—'
  const ahead = at - now
  if (ahead <= 0) return 'due now'
  if (ahead < MINUTE) return 'in under a minute'
  if (ahead < HOUR) return `in ${Math.round(ahead / MINUTE)} min`
  if (ahead < 2 * DAY) return `in ${Math.round(ahead / HOUR)} h`
  return new Date(at).toLocaleString()
}

/** Broken first, then paused, then by how soon each one fires. */
export function scheduleRank(entry: ScheduleEntry): number {
  if (entry.error) return 0
  if (!entry.enabled) return 1
  return 2
}

export function sortSchedules(entries: ScheduleEntry[]): ScheduleEntry[] {
  return [...entries].sort((a, b) => {
    if (scheduleRank(a) !== scheduleRank(b)) return scheduleRank(a) - scheduleRank(b)
    const next = parsedTime(a.nextFire) - parsedTime(b.nextFire)
    if (next !== 0) return next
    return a.name.localeCompare(b.name)
  })
}

export function SchedulesPanel() {
  const url = useSettingsStore((state) => state.runnerUrl)
  const token = useSettingsStore((state) => state.runnerToken)
  const can = useAuthStore((state) => state.can)

  const [entries, setEntries] = useState<ScheduleEntry[]>([])
  const [loading, setLoading] = useState(true)
  const [firing, setFiring] = useState(false)
  const [note, setNote] = useState('')
  const [failure, setFailure] = useState('')
  const [forbidden, setForbidden] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      setEntries(await listSchedules(url, token))
      setFailure('')
      setForbidden(false)
    } catch (error) {
      setEntries([])
      setForbidden(isForbidden(error))
      setFailure(isForbidden(error) ? '' : messageOf(error))
    } finally {
      setLoading(false)
    }
  }, [token, url])

  useEffect(() => {
    void load()
  }, [load])

  const sweep = async () => {
    setFiring(true)
    try {
      const result = await evaluateSchedules(url, token)
      const started = result.fires.filter((fire) => fire.started).length
      const refused = result.fires.find((fire) => !fire.started && fire.error)
      setNote(
        result.fires.length === 0
          ? `Checked ${result.checked}. Nothing was due.`
          : refused
            ? `Started ${started} of ${result.fires.length}. ${refused.name}: ${refused.error}`
            : `Started ${started} of ${result.checked}.`,
      )
      await load()
    } catch (error) {
      setNote(messageOf(error))
    } finally {
      setFiring(false)
    }
  }

  const rows = useMemo(() => sortSchedules(entries), [entries])
  const broken = useMemo(() => entries.filter((entry) => entry.error).length, [entries])

  if (!can('workspace:Read') || forbidden) {
    return (
      <div className="space-y-2">
        <SectionTitle>Schedules</SectionTitle>
        <p className="rounded-lg border border-dashed border-line px-3 py-6 text-center text-2xs leading-relaxed text-content-subtle">
          Reading the schedules needs <code>workspace:Read</code>. Ask an administrator of
          this runner for the permission.
        </p>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <p className="min-w-[12rem] flex-1 text-2xs leading-relaxed text-content-subtle">
          {entries.length === 1 ? '1 schedule' : `${entries.length} schedules`}
          {broken > 0 ? ` · ${broken} unreadable` : ''}
        </p>
        {can('run:Execute') && (
          <Button
            size="xs"
            variant="ghost"
            disabled={firing}
            onClick={() => void sweep()}
            icon={<PlayCircle className={cn('h-3 w-3', firing && 'animate-pulse')} />}
          >
            Run what is due
          </Button>
        )}
        <Button
          size="xs"
          variant="ghost"
          onClick={() => void load()}
          icon={<RefreshCw className={cn('h-3 w-3', loading && 'animate-spin')} />}
        >
          Refresh
        </Button>
      </div>

      {note && <p className="text-2xs leading-relaxed text-content-muted">{note}</p>}

      {loading && entries.length === 0 ? (
        <div className="flex items-center justify-center py-6">
          <Spinner className="h-4 w-4" />
        </div>
      ) : failure ? (
        <p className="text-2xs leading-relaxed text-content-subtle">{failure}</p>
      ) : rows.length === 0 ? (
        <p className="flex items-center justify-center gap-2 rounded-lg border border-dashed border-line px-3 py-6 text-2xs text-content-subtle">
          <CalendarClock className="h-3.5 w-3.5" aria-hidden />
          Nothing is scheduled. Open a Job or a Pipeline and set one in its header.
        </p>
      ) : (
        <div className="max-h-[calc(100vh-24rem)] min-h-[12rem] overflow-y-auto overscroll-contain rounded-lg border border-line">
          <table className="w-full text-left text-2xs">
            <thead className="sticky top-0 z-10 bg-surface-sunken text-content-subtle shadow-[0_1px_0_0_rgb(var(--line))]">
              <tr>
                <th className="px-3 py-2 font-medium">What</th>
                <th className="px-3 py-2 font-medium">When</th>
                <th className="w-28 px-3 py-2 font-medium">Next</th>
                <th className="w-32 px-3 py-2 font-medium">Last</th>
                <th className="w-24 px-3 py-2 font-medium">Runs as</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((entry) => (
                <tr key={`${entry.kind}:${entry.id}`} className="border-t border-line">
                  <td className="px-3 py-2">
                    <span className="font-medium text-content">{entry.name || entry.id}</span>
                    <Badge tone="neutral" className="ml-2">
                      {entry.kind === 'pipeline' ? 'Pipeline' : 'Job'}
                    </Badge>
                    {!entry.enabled && (
                      <Badge tone="neutral" className="ml-1">
                        Paused
                      </Badge>
                    )}
                  </td>
                  <td className="px-3 py-2 text-content-muted">
                    {entry.error ? (
                      <span className="flex items-center gap-1 text-state-danger">
                        <AlertTriangle className="h-3 w-3 shrink-0" aria-hidden />
                        {entry.error}
                      </span>
                    ) : (
                      entry.rule || describeCron(entry.cron, entry.timezone)
                    )}
                  </td>
                  <td className="px-3 py-2 text-content-muted">
                    {entry.nextFire ? timeUntil(parsedTime(entry.nextFire)) : '—'}
                  </td>
                  <td className="px-3 py-2 text-content-muted">
                    {entry.lastFire ? relativeTime(parsedTime(entry.lastFire)) : 'Never'}
                    {entry.lastStatus ? ` · ${entry.lastStatus}` : ''}
                  </td>
                  <td className="px-3 py-2 text-content-muted">{entry.runAs || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
