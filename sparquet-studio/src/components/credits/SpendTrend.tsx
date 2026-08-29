/**
 * Six months of spending, as bars.
 *
 * One month's number answers "how much"; only the series answers "is that
 * normal", which is the question somebody who opens a bill actually has. A month
 * that doubled is visible here at a glance and invisible in any single total.
 *
 * The bars are also the period picker. A chart that shows an interesting month
 * and then makes you find it again in a dropdown is two controls doing one job,
 * so clicking a bar is what selects the month the breakdown below reads.
 *
 * Each bar is the whole cost of its month, with the part the free allowance
 * absorbed drawn lighter at the bottom. Keeping the two in one bar is what makes
 * the moment a team crosses out of its allowance a visible change of shape
 * rather than a number nobody was watching.
 */

import { TrendingDown, TrendingUp } from 'lucide-react'
import { useCallback, useEffect, useState } from 'react'

import { SectionTitle, Spinner } from '@/components/ui'
import { changeFrom, monthName, shareOf, shortMonth } from '@/lib/billing'
import { getSpendTimeline } from '@/lib/runner/credits'
import { cn } from '@/lib/utils/cn'
import { useSettingsStore } from '@/store/settings'
import type { SpendPeriod } from '@/types/credits'

export interface SpendTrendProps {
  /** The month the rest of the screen is reading, highlighted here. */
  period: string
  onSelect: (period: string) => void
  months?: number
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

export function SpendTrend({ period, onSelect, months = 6 }: SpendTrendProps) {
  const url = useSettingsStore((state) => state.runnerUrl)
  const token = useSettingsStore((state) => state.runnerToken)

  const [periods, setPeriods] = useState<SpendPeriod[]>([])
  const [loading, setLoading] = useState(true)
  const [failure, setFailure] = useState('')

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const timeline = await getSpendTimeline(url, { months }, token)
      setPeriods(timeline.periods)
      setFailure('')
    } catch (error) {
      setFailure(messageOf(error))
      setPeriods([])
    } finally {
      setLoading(false)
    }
  }, [months, token, url])

  useEffect(() => {
    void load()
  }, [load])

  const peak = periods.reduce((most, month) => Math.max(most, month.charged), 0)
  const latest = periods[periods.length - 1] ?? null
  const before = periods[periods.length - 2] ?? null
  const change = latest && before ? changeFrom(before.charged, latest.charged) : null

  if (loading && periods.length === 0) {
    return (
      <div className="flex items-center justify-center py-8">
        <Spinner className="h-4 w-4" />
      </div>
    )
  }

  if (failure) {
    return <p className="text-2xs leading-relaxed text-content-subtle">{failure}</p>
  }

  return (
    <div className="space-y-4">
      <SectionTitle>Spending over time</SectionTitle>

      <div className="grid grid-cols-3 gap-3">
        <Tile
          label="This month"
          value={latest?.charged ?? 0}
          hint={
            change === null
              ? before
                ? 'First month with spending'
                : 'No month to compare with'
              : `${change >= 0 ? '+' : ''}${change}% on ${shortMonth(before?.period ?? '')}`
          }
          trend={change}
        />
        <Tile label="Runs charged" value={latest?.runs ?? 0} hint="Remote runs this month" />
        <Tile
          label="Free of charge"
          value={latest?.waived ?? 0}
          hint="Absorbed by the monthly allowance"
        />
      </div>

      {peak === 0 ? (
        <p className="rounded-lg border border-dashed border-line px-3 py-6 text-center text-2xs text-content-subtle">
          Nothing has been charged in the last {periods.length || months} months. Local
          runs are free and never appear here.
        </p>
      ) : (
        <ul className="flex h-36 items-end gap-2">
          {periods.map((month) => {
            const height = shareOf(month.charged, peak)
            const free = shareOf(month.waived, Math.max(month.charged, 1))
            const active = month.period === period
            return (
              <li key={month.period} className="flex h-full flex-1 flex-col justify-end">
                <button
                  type="button"
                  onClick={() => onSelect(month.period)}
                  aria-pressed={active}
                  title={`${monthName(month.period)}: ${month.charged} credits over ${month.runs} runs`}
                  className={cn(
                    'group flex h-full w-full flex-col justify-end gap-1 rounded-md px-1 pt-1',
                    'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500',
                    active ? 'bg-surface-sunken' : 'hover:bg-surface-sunken/60',
                  )}
                >
                  <span
                    className={cn(
                      'text-center text-2xs tabular-nums',
                      active ? 'text-content' : 'text-content-subtle',
                    )}
                  >
                    {month.charged}
                  </span>
                  <span
                    className="flex w-full flex-col-reverse overflow-hidden rounded-sm bg-surface-sunken"
                    style={{ height: `${Math.max(height, month.charged > 0 ? 4 : 2)}%` }}
                  >
                    {/* The bottom of the bar is the free part; what sits above it
                        is what the team is actually paying for. */}
                    <span
                      className={cn(
                        'block w-full shrink-0',
                        active ? 'bg-brand-400' : 'bg-brand-500/40',
                      )}
                      style={{ height: `${free}%` }}
                    />
                    <span
                      className={cn(
                        'block w-full flex-1',
                        active ? 'bg-brand-500' : 'bg-brand-500/70',
                      )}
                    />
                  </span>
                  <span
                    className={cn(
                      'text-center text-2xs',
                      active ? 'font-medium text-content' : 'text-content-subtle',
                    )}
                  >
                    {shortMonth(month.period)}
                  </span>
                </button>
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}

function Tile({
  label,
  value,
  hint,
  trend,
}: {
  label: string
  value: number
  hint: string
  /** Undefined for a tile that is not a comparison; null when there is nothing
   *  to compare with. Only a number draws an arrow. */
  trend?: number | null
}) {
  const Arrow =
    trend === null || trend === undefined ? null : trend >= 0 ? TrendingUp : TrendingDown
  return (
    <div className="rounded-lg border border-line bg-surface-sunken px-3 py-2.5">
      <p className="text-2xs uppercase tracking-wide text-content-subtle">{label}</p>
      <p className="mt-1 flex items-center gap-1.5 text-2xl font-semibold tabular-nums text-content">
        {value}
        {Arrow ? (
          <Arrow
            className={cn(
              'h-3.5 w-3.5',
              // Spending more is the direction worth noticing on a bill, so up is
              // the warning colour here and down is the good one.
              (trend ?? 0) >= 0 ? 'text-state-warning' : 'text-state-success',
            )}
            aria-hidden
          />
        ) : null}
      </p>
      <p className="mt-0.5 text-2xs leading-relaxed text-content-subtle">{hint}</p>
    </div>
  )
}
