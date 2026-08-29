/**
 * One month of spending, sliced by team, by person, by workflow, by job or by tag.
 *
 * The account is always a team — a workflow is a folder, it can be renamed and
 * moved, and a budget attached to one would break the day somebody dragged a job
 * out of it. So the team pays, and these are the dimensions its single invoice is
 * read back by: every run charge carries the workflow it belonged to, the person
 * who started it and the tags it wore, and this is where those are added up.
 *
 * `charged` is the whole cost of the month and `waived` the part the free
 * allowance absorbed — the same convention the rest of the credits UI uses, so a
 * team inside its allowance reads "12 credits, all free" rather than "0".
 *
 * Four of the five dimensions partition the month: each run belongs to exactly
 * one team, one person, one workflow, one job, so the lines add up to the total.
 * Tags do not — a run wearing `finance` and `nightly` is counted in full under
 * both — which is the point of them and also why the bars are drawn against the
 * biggest line rather than against the total, and why the screen says so out
 * loud when the runner reports `overlapping`.
 */

import { Layers } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'

import { Segmented, SectionTitle, Spinner } from '@/components/ui'
import { monthName, shareOf } from '@/lib/billing'
import { getSpendBreakdown } from '@/lib/runner/credits'
import { cn } from '@/lib/utils/cn'
import { useAuthStore } from '@/store/auth'
import { useLibraryStore } from '@/store/library'
import { useSettingsStore } from '@/store/settings'
import type { SpendBreakdown as Breakdown, SpendGroupBy } from '@/types/credits'

const GROUPS: { value: SpendGroupBy; label: string }[] = [
  { value: 'workflow', label: 'Workflow' },
  { value: 'job', label: 'Job' },
  { value: 'tag', label: 'Tag' },
  { value: 'user', label: 'User' },
  { value: 'team', label: 'Team' },
]

/** What a null key means, per dimension — it is never the same absence twice. */
const UNATTRIBUTED: Record<SpendGroupBy, string> = {
  workflow: 'Outside any workflow',
  job: 'Unnamed job',
  tag: 'Untagged',
  user: 'Shared runner token',
  team: 'No account',
}

/** How many lines are shown before the rest is folded into one. */
const VISIBLE = 8

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

export interface SpendBreakdownProps {
  /** The month to read. Owned by the screen, because the chart above selects it too. */
  period: string
}

export function SpendBreakdown({ period }: SpendBreakdownProps) {
  const url = useSettingsStore((state) => state.runnerUrl)
  const token = useSettingsStore((state) => state.runnerToken)
  const can = useAuthStore((state) => state.can)
  const workflows = useLibraryStore((state) => state.workflows)
  const jobs = useLibraryStore((state) => state.jobs)

  const [groupBy, setGroupBy] = useState<SpendGroupBy>('workflow')
  const [data, setData] = useState<Breakdown | null>(null)
  const [loading, setLoading] = useState(true)
  const [expanded, setExpanded] = useState(false)
  const [failure, setFailure] = useState('')

  // The runner only knows the records it has seen a run from, and it stores the
  // name as it was then. A record that exists in this browser should read by the
  // name it has now.
  const localNames = useMemo(() => {
    const names = new Map<string, string>()
    for (const workflow of workflows) names.set(workflow.id, workflow.name)
    for (const job of jobs) names.set(job.id, job.name)
    return names
  }, [jobs, workflows])

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

  // A different dimension is a different list; leaving it expanded from the last
  // one would open on a hidden scroll position.
  useEffect(() => {
    setExpanded(false)
  }, [groupBy, period])

  const rows = data?.groups ?? []
  const peak = rows.reduce((most, row) => Math.max(most, row.charged), 0)
  const shown = expanded ? rows : rows.slice(0, VISIBLE)
  const hidden = rows.length - shown.length
  const total = data?.total
  const scopeNote =
    data?.scope === 'all' ? 'Every account on this runner.' : "Your own team's spending."

  return (
    <div className="space-y-3">
      <SectionTitle
        action={
          <span className="text-2xs text-content-subtle">{monthName(period)}</span>
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
          Nothing was charged in {monthName(period)}. Local runs are free and never show
          up here.
        </p>
      ) : (
        <>
          <ul className="space-y-1.5">
            {shown.map((row) => {
              const name =
                (row.key ? localNames.get(row.key) : null) ??
                row.label ??
                row.key ??
                UNATTRIBUTED[groupBy]
              const width = shareOf(row.charged, peak)
              const free = shareOf(row.waived, Math.max(row.charged, 1))
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
                      {row.charged}
                      <span className="ml-1 text-2xs text-content-subtle">
                        {row.runs} {row.runs === 1 ? 'run' : 'runs'}
                        {row.waived > 0 ? ` · ${row.waived} free` : ''}
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
                      {/* The paid part is the solid end of the bar; what the
                          allowance absorbed is the pale tail behind it. */}
                      <span
                        className="block h-full shrink-0 bg-brand-500/35"
                        style={{ width: `${free}%` }}
                      />
                    </span>
                  </span>
                </li>
              )
            })}
          </ul>

          {hidden > 0 && (
            <button
              type="button"
              onClick={() => setExpanded(true)}
              className="text-2xs text-content-muted underline-offset-2 hover:text-content hover:underline"
            >
              Show {hidden} more
            </button>
          )}

          <div className="flex items-baseline justify-between gap-3 border-t border-line pt-2 text-xs">
            <span className="text-content-muted">
              Total for {monthName(period)}
            </span>
            <span className="tabular-nums text-content">
              {total?.charged ?? 0}
              <span className="ml-1 text-2xs text-content-subtle">
                {total?.runs ?? 0} {total?.runs === 1 ? 'run' : 'runs'}
                {(total?.waived ?? 0) > 0 ? ` · ${total?.waived} free` : ''}
              </span>
            </span>
          </div>

          {data?.overlapping && (
            <p className="flex items-start gap-1.5 rounded-md bg-surface-sunken px-2.5 py-2 text-2xs leading-relaxed text-content-muted">
              <Layers className="mt-0.5 h-3 w-3 shrink-0" aria-hidden />
              <span>
                A run wearing two tags is counted in full under each, so these lines
                add up to more than the total. That is deliberate: the question a tag
                answers is what one label costs, not how a month divides.
              </span>
            </p>
          )}
        </>
      )}

      <p className="text-2xs leading-relaxed text-content-subtle">
        {scopeNote} The team is the account — a workflow can be renamed or moved
        between teams, so it is a way of reading the bill rather than a payer of its
        own. {can('credits:Read') ? '' : 'Reading other teams needs credits:Read.'}
      </p>
    </div>
  )
}
