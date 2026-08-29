/**
 * A month of spending, sliced by team, by person, by workflow or by job.
 *
 * The account is always a team — a workflow is a folder, it can be renamed and
 * moved, and a budget attached to one would break the day somebody dragged a job
 * out of it. So the team pays, and these are the dimensions its single invoice is
 * read back by: every run charge carries the workflow it belonged to and the
 * person who started it, and this is where those two are added up.
 *
 * `charged` is the whole cost of the month and `waived` the part the free
 * allowance absorbed — the same convention the rest of the credits UI uses, so a
 * team inside its allowance reads "12 credits, all free" rather than "0".
 */

import { CalendarDays } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'

import { Segmented, SectionTitle, Spinner } from '@/components/ui'
import { getSpendBreakdown } from '@/lib/runner/credits'
import { cn } from '@/lib/utils/cn'
import { useAuthStore } from '@/store/auth'
import { useLibraryStore } from '@/store/library'
import { useSettingsStore } from '@/store/settings'
import type { SpendBreakdown as Breakdown, SpendGroupBy } from '@/types/credits'

const GROUPS: { value: SpendGroupBy; label: string }[] = [
  { value: 'workflow', label: 'Workflow' },
  { value: 'job', label: 'Job' },
  { value: 'user', label: 'User' },
  { value: 'team', label: 'Team' },
]

/** What a null key means, per dimension — it is never the same absence twice. */
const UNATTRIBUTED: Record<SpendGroupBy, string> = {
  workflow: 'Outside any workflow',
  job: 'Unnamed job',
  user: 'Shared runner token',
  team: 'No account',
}

const MONTHS_BACK = 6

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** `2026-08` as `August 2026`, so a period reads as a month and not as an id. */
function monthName(period: string): string {
  const [year, month] = period.split('-')
  const index = Number.parseInt(month ?? '', 10)
  if (!year || !Number.isFinite(index) || index < 1 || index > 12) return period
  const label = new Date(Date.UTC(2000, index - 1, 1)).toLocaleString(undefined, {
    month: 'long',
  })
  return `${label} ${year}`
}

/** This month and the five before it, newest first. */
function recentPeriods(): string[] {
  const now = new Date()
  return Array.from({ length: MONTHS_BACK }, (_, back) => {
    const date = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - back, 1))
    return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}`
  })
}

export function SpendBreakdown() {
  const url = useSettingsStore((state) => state.runnerUrl)
  const token = useSettingsStore((state) => state.runnerToken)
  const can = useAuthStore((state) => state.can)
  const workflows = useLibraryStore((state) => state.workflows)

  const periods = useMemo(recentPeriods, [])
  const [groupBy, setGroupBy] = useState<SpendGroupBy>('workflow')
  const [period, setPeriod] = useState(periods[0])
  const [data, setData] = useState<Breakdown | null>(null)
  const [loading, setLoading] = useState(true)
  const [failure, setFailure] = useState('')

  // The runner only knows the workflows it has seen a run from. A workflow that
  // exists in this browser and has never run should still read by name.
  const localNames = useMemo(
    () => new Map(workflows.map((workflow) => [workflow.id, workflow.name])),
    [workflows],
  )

  const load = useCallback(async () => {
    setLoading(true)
    try {
      setData(await getSpendBreakdown(url, { groupBy, period }, token))
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

  const rows = data?.groups ?? []
  const peak = rows.reduce((most, row) => Math.max(most, row.charged), 0)
  const scopeNote =
    data?.scope === 'all'
      ? 'Every account on this runner.'
      : "Your own team's spending."

  return (
    <div className="space-y-3">
      <SectionTitle
        action={
          <span className="flex items-center gap-1.5 text-2xs text-content-subtle">
            <CalendarDays className="h-3 w-3" aria-hidden />
            <select
              value={period}
              onChange={(event) => setPeriod(event.target.value)}
              aria-label="Billing period"
              className={cn(
                'rounded-md border border-line bg-surface px-1.5 py-0.5 text-2xs text-content',
                'focus:border-line-strong focus:outline-none',
              )}
            >
              {periods.map((item) => (
                <option key={item} value={item}>
                  {monthName(item)}
                </option>
              ))}
            </select>
          </span>
        }
      >
        Where the credits went
      </SectionTitle>

      <Segmented
        value={groupBy}
        onChange={setGroupBy}
        options={GROUPS}
        size="sm"
        ariaLabel="Group spending by"
      />

      {loading && data === null ? (
        <div className="flex items-center justify-center py-6">
          <Spinner className="h-4 w-4" />
        </div>
      ) : failure ? (
        <p className="text-2xs leading-relaxed text-content-subtle">{failure}</p>
      ) : rows.length === 0 ? (
        <p className="rounded-lg border border-dashed border-line px-3 py-6 text-center text-2xs text-content-subtle">
          Nothing was charged in {monthName(period)}. Local runs are free and never
          show up here.
        </p>
      ) : (
        <div className="overflow-hidden rounded-lg border border-line">
          <table className="w-full text-left text-2xs">
            <thead className="bg-surface-sunken text-content-subtle">
              <tr>
                <th className="px-3 py-2 font-medium">
                  {GROUPS.find((item) => item.value === groupBy)?.label}
                </th>
                <th className="w-20 px-3 py-2 text-right font-medium">Runs</th>
                <th className="w-20 px-3 py-2 text-right font-medium">Writes</th>
                <th className="w-28 px-3 py-2 text-right font-medium">Credits</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => {
                const name =
                  (row.key ? localNames.get(row.key) : null) ??
                  row.label ??
                  row.key ??
                  UNATTRIBUTED[groupBy]
                const share = peak > 0 ? Math.round((row.charged / peak) * 100) : 0
                return (
                  <tr key={row.key ?? '∅'} className="border-t border-line">
                    <td className="min-w-0 px-3 py-2">
                      <span
                        className={cn(
                          'block truncate',
                          row.key ? 'text-content' : 'text-content-subtle italic',
                        )}
                        title={row.key ?? undefined}
                      >
                        {name}
                      </span>
                      <span className="mt-1 block h-1 overflow-hidden rounded-full bg-surface-sunken">
                        <span
                          className="block h-full bg-brand-500"
                          style={{ width: `${share}%` }}
                        />
                      </span>
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums text-content-subtle">
                      {row.runs}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums text-content-subtle">
                      {row.writes}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums text-content">
                      {row.charged}
                      {row.waived > 0 ? (
                        <span className="ml-1 text-content-subtle">
                          ({row.waived} free)
                        </span>
                      ) : null}
                    </td>
                  </tr>
                )
              })}
            </tbody>
            <tfoot className="border-t border-line bg-surface-sunken">
              <tr>
                <td className="px-3 py-2 text-content">Total</td>
                <td className="px-3 py-2 text-right tabular-nums text-content-subtle">
                  {data?.total.runs ?? 0}
                </td>
                <td className="px-3 py-2 text-right tabular-nums text-content-subtle">
                  {data?.total.writes ?? 0}
                </td>
                <td className="px-3 py-2 text-right tabular-nums text-content">
                  {data?.total.charged ?? 0}
                </td>
              </tr>
            </tfoot>
          </table>
        </div>
      )}

      <p className="text-2xs leading-relaxed text-content-subtle">
        {scopeNote} The team is the account — a workflow can be renamed or moved
        between teams, so it is a way of reading the bill rather than a payer of
        its own.{' '}
        {can('credits:Read')
          ? ''
          : 'Reading other teams needs credits:Read.'}
      </p>
    </div>
  )
}
