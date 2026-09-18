/**
 * Every Job in the library, and what its own runs say about it.
 *
 * The run list already exists and answers a different question. It is ordered by
 * time, so it is very good at "what happened this afternoon" and structurally
 * incapable of "which Job stopped running last Tuesday": a Job that has not run
 * is not near the top of a list ordered by time, it is nowhere in it. This table
 * has one row per Job whether or not that Job has ever run.
 *
 * The sparkline is of past *successful* runs, newest first, because that is what
 * a median rule compares against — the line on screen and the number the alert
 * uses are the same data, so a firing duration alert can be read against the
 * shape that produced it.
 *
 * Reading needs `monitoring:Read`. Somebody without it is told so rather than
 * shown an empty table.
 */

import { Activity, AlertTriangle, RefreshCw, Search } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'

import { Button, Input, SectionTitle, Segmented, Spinner } from '@/components/ui'
import { fetchJobHealth, isForbidden, type JobHealth } from '@/lib/runner/monitoring'
import { formatCount, formatDuration, relativeTime } from '@/lib/utils/format'
import { cn } from '@/lib/utils/cn'
import { useAuthStore } from '@/store/auth'
import { useSettingsStore } from '@/store/settings'

/** How a row reads at a glance, in the order somebody scanning cares about. */
type Verdict = 'failing' | 'stale' | 'healthy' | 'never'

/** A Job nobody has run in a week is not broken, but it is worth noticing. */
const STALE_AFTER_MS = 7 * 24 * 60 * 60 * 1000

const FILTERS = [
  { value: 'all', label: 'All' },
  { value: 'failing', label: 'Failing' },
  { value: 'stale', label: 'Idle' },
  { value: 'never', label: 'Never run' },
] as const

type Filter = (typeof FILTERS)[number]['value']

const TONE: Record<Verdict, string> = {
  failing: 'bg-state-danger',
  stale: 'bg-gold',
  healthy: 'bg-node-output',
  never: 'bg-content-subtle/40',
}

const LABEL: Record<Verdict, string> = {
  failing: 'Failing',
  stale: 'Idle',
  healthy: 'Healthy',
  never: 'Never run',
}

export function verdictOf(record: JobHealth, now = Date.now()): Verdict {
  if (record.lastStatus === null) return 'never'
  if (record.consecutiveFailures > 0) return 'failing'
  const last = record.lastStartedAt ? Date.parse(record.lastStartedAt) : NaN
  if (Number.isFinite(last) && now - last > STALE_AFTER_MS) return 'stale'
  return 'healthy'
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function parsedTime(value: string | null): number {
  if (!value) return 0
  const parsed = Date.parse(value)
  return Number.isFinite(parsed) ? parsed : 0
}

/**
 * The shape of the last runs, as a polyline.
 *
 * Oldest on the left, because that is the direction a trend is read in, and the
 * runner sends newest first. Scaled to its own maximum: these are one Job's runs
 * compared with each other, and a shared scale across Jobs would flatten every
 * small one into a straight line at the bottom.
 */
export function sparklinePoints(values: number[], width = 64, height = 16): string {
  const series = [...values].reverse()
  if (series.length === 0) return ''
  if (series.length === 1) {
    const middle = (height / 2).toFixed(1)
    return `0,${middle} ${width},${middle}`
  }
  const max = Math.max(...series)
  const min = Math.min(...series)
  const span = max - min || 1
  const step = width / (series.length - 1)
  return series
    .map((value, index) => {
      const x = (index * step).toFixed(1)
      const y = (height - ((value - min) / span) * height).toFixed(1)
      return `${x},${y}`
    })
    .join(' ')
}

export function JobHealthPanel() {
  const url = useSettingsStore((state) => state.runnerUrl)
  const token = useSettingsStore((state) => state.runnerToken)
  const can = useAuthStore((state) => state.can)

  const [records, setRecords] = useState<JobHealth[]>([])
  const [filter, setFilter] = useState<Filter>('all')
  const [search, setSearch] = useState('')
  const [loading, setLoading] = useState(true)
  const [failure, setFailure] = useState('')
  const [forbidden, setForbidden] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      setRecords(await fetchJobHealth(url, token))
      setFailure('')
      setForbidden(false)
    } catch (error) {
      setRecords([])
      setForbidden(isForbidden(error))
      setFailure(isForbidden(error) ? '' : messageOf(error))
    } finally {
      setLoading(false)
    }
  }, [token, url])

  useEffect(() => {
    void load()
  }, [load])

  const rows = useMemo(() => {
    const now = Date.now()
    const needle = search.trim().toLowerCase()
    return records
      .map((record) => ({ record, verdict: verdictOf(record, now) }))
      .filter((row) => (filter === 'all' ? true : row.verdict === filter))
      .filter((row) =>
        needle
          ? `${row.record.name ?? ''} ${row.record.jobId}`.toLowerCase().includes(needle)
          : true,
      )
      .sort((a, b) => {
        // Trouble first, then the quiet ones, then everything else by recency:
        // the top of this table should be the reason somebody opened it.
        const rank: Record<Verdict, number> = { failing: 0, stale: 1, never: 2, healthy: 3 }
        if (rank[a.verdict] !== rank[b.verdict]) return rank[a.verdict] - rank[b.verdict]
        return parsedTime(b.record.lastStartedAt) - parsedTime(a.record.lastStartedAt)
      })
  }, [filter, records, search])

  const failing = useMemo(
    () => records.filter((record) => record.consecutiveFailures > 0).length,
    [records],
  )

  if (!can('monitoring:Read') || forbidden) {
    return (
      <div className="space-y-2">
        <SectionTitle>Job health</SectionTitle>
        <p className="rounded-lg border border-dashed border-line px-3 py-6 text-center text-2xs leading-relaxed text-content-subtle">
          Reading the health of the library needs <code>monitoring:Read</code>. Ask an
          administrator of this runner for the permission.
        </p>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <Segmented
          value={filter}
          onChange={(value) => setFilter(value as Filter)}
          options={FILTERS.map((item) => ({ value: item.value, label: item.label }))}
          size="sm"
          ariaLabel="Filter by state"
        />
        <div className="min-w-[12rem] flex-1">
          <Input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search by Job name or id…"
            aria-label="Search the Jobs already loaded"
            className="h-7 text-2xs"
            leading={<Search aria-hidden />}
          />
        </div>
        <Button
          size="xs"
          variant="ghost"
          onClick={() => void load()}
          icon={<RefreshCw className={cn('h-3 w-3', loading && 'animate-spin')} />}
        >
          Refresh
        </Button>
      </div>

      <p className="text-2xs leading-relaxed text-content-subtle">
        {rows.length === records.length
          ? `${records.length} Jobs`
          : `${rows.length} of ${records.length} Jobs`}
        {failing > 0 ? ` · ${failing} failing` : ''}
      </p>

      {loading && records.length === 0 ? (
        <div className="flex items-center justify-center py-6">
          <Spinner className="h-4 w-4" />
        </div>
      ) : failure ? (
        <p className="text-2xs leading-relaxed text-content-subtle">{failure}</p>
      ) : rows.length === 0 ? (
        <p className="flex items-center justify-center gap-2 rounded-lg border border-dashed border-line px-3 py-6 text-2xs text-content-subtle">
          <Activity className="h-3.5 w-3.5" aria-hidden />
          {records.length === 0
            ? 'The runner has no Jobs on record yet. Run one and it appears here.'
            : 'No Job matches that filter.'}
        </p>
      ) : (
        <div className="max-h-[calc(100vh-24rem)] min-h-[16rem] overflow-y-auto overscroll-contain rounded-lg border border-line">
          <table className="w-full text-left text-2xs">
            <thead className="sticky top-0 z-10 bg-surface-sunken text-content-subtle shadow-[0_1px_0_0_rgb(var(--line))]">
              <tr>
                <th className="w-24 px-3 py-2 font-medium">State</th>
                <th className="px-3 py-2 font-medium">Job</th>
                <th className="w-28 px-3 py-2 font-medium">Last run</th>
                <th className="w-20 px-3 py-2 text-right font-medium">Took</th>
                <th className="w-20 px-3 py-2 text-right font-medium">Rows</th>
                <th className="w-20 px-3 py-2 font-medium">Trend</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(({ record, verdict }) => (
                <tr key={record.jobId} className="border-t border-line/60 align-top">
                  <td className="px-3 py-2">
                    <span className="flex items-center gap-1.5">
                      <span
                        className={cn('h-1.5 w-1.5 shrink-0 rounded-full', TONE[verdict])}
                        aria-hidden
                      />
                      <span className={verdict === 'failing' ? 'text-state-danger' : ''}>
                        {LABEL[verdict]}
                      </span>
                    </span>
                  </td>
                  <td className="px-3 py-2">
                    <span className="block truncate" title={record.jobId}>
                      {record.name ?? record.jobId}
                    </span>
                    {record.consecutiveFailures > 0 ? (
                      <span className="mt-0.5 flex items-start gap-1 text-state-danger">
                        <AlertTriangle className="mt-px h-3 w-3 shrink-0" aria-hidden />
                        <span className="line-clamp-2">
                          {record.consecutiveFailures} in a row
                          {record.lastError ? ` · ${record.lastError}` : ''}
                        </span>
                      </span>
                    ) : null}
                  </td>
                  <td className="px-3 py-2 text-content-subtle">
                    {record.lastStartedAt
                      ? relativeTime(parsedTime(record.lastStartedAt))
                      : 'never'}
                  </td>
                  <td className="px-3 py-2 text-right text-content-subtle">
                    {formatDuration(record.lastDurationMs ?? undefined)}
                  </td>
                  <td className="px-3 py-2 text-right text-content-subtle">
                    {formatCount(record.lastRowsWritten ?? undefined)}
                  </td>
                  <td className="px-3 py-2">
                    {record.durations.length > 1 ? (
                      <svg
                        viewBox="0 0 64 16"
                        className="h-4 w-16 text-content-subtle"
                        role="img"
                        aria-label={`Duration of the last ${record.durations.length} successful runs`}
                      >
                        <polyline
                          points={sparklinePoints(record.durations)}
                          fill="none"
                          stroke="currentColor"
                          strokeWidth="1"
                          vectorEffect="non-scaling-stroke"
                        />
                      </svg>
                    ) : (
                      <span className="text-content-subtle">—</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
