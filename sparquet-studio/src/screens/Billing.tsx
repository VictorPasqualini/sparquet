/**
 * Billing: what this team may spend, what it has spent, and on what.
 *
 * Its own screen rather than a section of Settings because it is a service, not
 * a preference — the balance decides whether a run starts at all, and somebody
 * looking for it after a refused run should not have to scroll through themes
 * and API keys to find it.
 *
 * The account is the team. A workflow is a folder: it gets renamed, and it moves
 * between teams — a budget attached to one would break the day somebody dragged a
 * job out of it. So there is one payer and several ways of reading its invoice,
 * which is what the breakdown below is.
 *
 * The screen owns the month, because the trend chart and the breakdown are the
 * same question asked at two zoom levels: the bars say which month is worth
 * looking at, and clicking one is what points the breakdown at it.
 *
 * The last card reads the same month from the execution history instead of the
 * ledger. Cost and activity are different questions with different answers — a
 * month of local development costs nothing and still ran hundreds of times — and
 * putting them on one screen is what lets one be read against the other.
 */

import { Coins } from 'lucide-react'
import { useState } from 'react'

import { CreditsPanel } from '@/components/credits/CreditsPanel'
import { RunActivity } from '@/components/credits/RunActivity'
import { SpendBreakdown } from '@/components/credits/SpendBreakdown'
import { SpendTrend } from '@/components/credits/SpendTrend'
import { PageHeader, PageShell } from '@/components/layout/PageShell'
import { currentPeriod } from '@/lib/billing'

export function Billing() {
  const [period, setPeriod] = useState(currentPeriod)

  return (
    <PageShell width="wide">
      <PageHeader
        icon={<Coins />}
        title="Billing"
        description="Execution credits — one per successful write that lands away from this
          machine, so local runs and runs that failed before writing are free — and, below them,
          what actually ran this month whether or not it cost anything."
      />

      <div className="space-y-6">
        <div className="card space-y-5 p-5">
          <CreditsPanel />
        </div>
        <div className="card space-y-5 p-5">
          <SpendTrend period={period} onSelect={setPeriod} />
        </div>
        <div className="card space-y-5 p-5">
          <SpendBreakdown period={period} />
        </div>
        <div className="card space-y-5 p-5">
          <RunActivity period={period} />
        </div>
      </div>
    </PageShell>
  )
}
