/**
 * Execution credits, as rendered inside the Billing section of Settings.
 *
 * The unit is a successful write to somewhere other than this machine. A Job
 * that runs locally is free; anything that leaves — a cluster master, Spark
 * Connect, a hosted runtime — costs one credit for each write it completes. The
 * runner decides both of those from the Job's own `spark` block and from what
 * the run actually produced, not from anything the caller claims, so this panel
 * only reports what happened. A run that failed before writing wrote nothing and
 * therefore cost nothing.
 *
 * The account is a team, not a person: a squad shares one allowance and one
 * balance. Every team gets a free allowance each calendar month, and it is spent
 * before any balance somebody granted.
 *
 * Two numbers are shown because they answer different questions. `balance` is
 * what may still be spent; `spent` is what remote work has cost so far. On a
 * runner that meters without enforcing, the second climbs while the first stands
 * still, and that is the normal state until somebody sets
 * `SPARQUET_STUDIO_CREDITS=on`.
 */

import { Coins, Plus } from 'lucide-react'
import { useCallback, useEffect, useState } from 'react'
import { toast } from 'sonner'

import { Badge, Button, Field, Input, Modal, Spinner } from '@/components/ui'
import {
  getCreditLedger,
  getMyCredits,
  grantCredits,
  listCreditAccounts,
} from '@/lib/runner/credits'
import { useAuthStore } from '@/store/auth'
import { useSettingsStore } from '@/store/settings'
import type { CreditAccount, CreditEntry, CreditStatus } from '@/types/credits'

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function useRunner(): { url: string; token: string } {
  const url = useSettingsStore((state) => state.runnerUrl)
  const token = useSettingsStore((state) => state.runnerToken)
  return { url, token }
}

/** `2026-08` as `August 2026`, so the period reads as a month and not as an id. */
function monthName(period: string): string {
  const [year, month] = period.split('-')
  const index = Number.parseInt(month ?? '', 10)
  if (!year || !Number.isFinite(index) || index < 1 || index > 12) return period
  const label = new Date(Date.UTC(2000, index - 1, 1)).toLocaleString(undefined, { month: 'long' })
  return `${label} ${year}`
}

function Metric({
  label,
  value,
  hint,
}: {
  label: string
  value: string | number
  hint?: string
}) {
  return (
    <div className="min-w-0">
      <p className="text-2xs uppercase tracking-wide text-content-subtle">{label}</p>
      <p className="mt-0.5 text-lg tabular-nums leading-none text-content">{value}</p>
      {hint ? <p className="mt-1 text-2xs leading-relaxed text-content-subtle">{hint}</p> : null}
    </div>
  )
}

/** The free allowance as a bar, because "9 of 40" is easier to see than to read. */
function AllowanceBar({ used, total }: { used: number; total: number }) {
  if (total <= 0) return null
  const filled = Math.min(100, Math.round((used / total) * 100))
  return (
    <div className="space-y-1">
      <div className="flex items-baseline justify-between text-2xs text-content-subtle">
        <span>Free allowance this month</span>
        <span className="tabular-nums">
          {used} / {total}
        </span>
      </div>
      <div className="h-1.5 overflow-hidden rounded-full bg-surface-sunken">
        <div
          className={filled >= 100 ? 'h-full bg-state-warning' : 'h-full bg-brand-500'}
          style={{ width: `${filled}%` }}
        />
      </div>
    </div>
  )
}

export function CreditsPanel() {
  const { url, token } = useRunner()
  const can = useAuthStore((state) => state.can)

  const mayRead = can('credits:Read')
  const mayManage = can('credits:Manage')

  const [status, setStatus] = useState<CreditStatus | null>(null)
  const [accounts, setAccounts] = useState<CreditAccount[]>([])
  const [ledger, setLedger] = useState<CreditEntry[]>([])
  const [loading, setLoading] = useState(true)
  const [failure, setFailure] = useState('')
  const [granting, setGranting] = useState<CreditAccount | null>(null)

  const reload = useCallback(async () => {
    setLoading(true)
    try {
      const mine = await getMyCredits(url, token)
      setStatus(mine)
      setLedger(await getCreditLedger(url, mine.account.id, 20, token))
      setAccounts(mayRead ? await listCreditAccounts(url, token) : [])
      setFailure('')
    } catch (error) {
      setFailure(messageOf(error))
    } finally {
      setLoading(false)
    }
  }, [mayRead, token, url])

  useEffect(() => {
    void reload()
  }, [reload])

  if (loading && status === null) {
    return (
      <div className="flex items-center justify-center py-6">
        <Spinner className="h-4 w-4" />
      </div>
    )
  }

  if (failure) {
    return <p className="text-2xs leading-relaxed text-content-subtle">{failure}</p>
  }

  if (!status) return null

  const { account, usage } = status
  const others = accounts.filter((item) => item.id !== account.id)

  return (
    <>
      <div className="space-y-3 rounded-lg border border-line bg-surface-sunken px-3 py-3">
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <p className="flex items-center gap-1.5 text-sm text-content">
              <Coins className="h-3.5 w-3.5 text-content-subtle" />
              {account.username || account.id}
              <Badge tone={status.enforced ? 'warning' : 'neutral'}>
                {status.enforced ? 'enforced' : 'metering only'}
              </Badge>
            </p>
            <p className="mt-0.5 text-2xs leading-relaxed text-content-subtle">
              This team's account, shared by everyone in it.{' '}
              {status.creditsPerWrite === 1
                ? 'One credit per successful write'
                : `${status.creditsPerWrite} credits per successful write`}{' '}
              that lands away from this machine; local runs and failed runs are free.
              {status.enforced
                ? ''
                : ' Nothing is being deducted — the runner is only keeping the count.'}
            </p>
          </div>
          {mayManage ? (
            <Button
              size="sm"
              variant="secondary"
              icon={<Plus className="h-3.5 w-3.5" />}
              onClick={() => setGranting(account)}
            >
              Add credits
            </Button>
          ) : null}
        </div>

        <AllowanceBar used={account.freeUsed} total={account.freeMonthly} />

        <div className="grid grid-cols-2 gap-3 border-t border-line pt-3 sm:grid-cols-4">
          <Metric
            label="Available"
            value={account.available}
            hint={`${account.freeRemaining} free + ${account.balance} bought`}
          />
          <Metric label="Balance" value={account.balance} hint="Granted credits" />
          <Metric
            label="Writes"
            value={usage.writes}
            hint={`Remote writes in ${monthName(usage.period || account.period)}`}
          />
          <Metric
            label="Charged"
            value={usage.charged}
            hint={usage.waived > 0 ? `${usage.waived} covered by the allowance` : 'This month'}
          />
        </div>
      </div>

      {mayRead && others.length > 0 ? (
        <div className="space-y-1">
          <p className="text-2xs text-content-subtle">Other teams</p>
          <div className="rounded-lg border border-line">
            <ul className="divide-y divide-line">
              {others.map((item) => (
                <li key={item.id} className="flex items-center gap-3 px-3 py-2">
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm text-content">{item.username || item.id}</p>
                    <p className="text-2xs text-content-subtle">
                      {item.available} available · {item.freeUsed}/{item.freeMonthly} free used ·{' '}
                      {item.spent} spent
                    </p>
                  </div>
                  {mayManage ? (
                    <Button size="sm" variant="ghost" onClick={() => setGranting(item)}>
                      Grant
                    </Button>
                  ) : null}
                </li>
              ))}
            </ul>
          </div>
        </div>
      ) : null}

      <div className="space-y-1">
        <p className="text-2xs text-content-subtle">Recent charges</p>
        {ledger.length === 0 ? (
          <p className="rounded-lg border border-line px-3 py-2.5 text-2xs text-content-subtle">
            Nothing yet. Only writes that leave this machine are written here.
          </p>
        ) : (
          <ul className="divide-y divide-line rounded-lg border border-line">
            {ledger.map((entry) => (
              <li key={entry.id} className="flex items-center gap-3 px-3 py-2">
                <div className="min-w-0 flex-1">
                  <p className="truncate text-2xs text-content">
                    {entry.jobName || entry.reason}
                    {entry.target ? (
                      <span className="ml-1.5 text-content-subtle">{entry.target}</span>
                    ) : null}
                  </p>
                  <p className="text-2xs text-content-subtle">
                    {entry.createdAt.slice(0, 16).replace('T', ' ')}
                    {entry.writes > 0
                      ? ` · ${entry.writes} ${entry.writes === 1 ? 'write' : 'writes'}`
                      : ''}
                    {entry.freeAmount > 0 ? ` · ${entry.freeAmount} free` : ''}
                    {entry.shortfall > 0 ? ` · ${entry.shortfall} unpaid` : ''}
                    {entry.applied ? '' : ' · metered only'}
                  </p>
                </div>
                <span className="text-2xs tabular-nums text-content-subtle">
                  {entry.amount > 0 ? `+${entry.amount}` : entry.amount}
                </span>
              </li>
            ))}
          </ul>
        )}
      </div>

      <GrantDialog
        account={granting}
        onOpenChange={(open) => {
          if (!open) setGranting(null)
        }}
        onGrant={async (amount, note) => {
          if (!granting) return
          await grantCredits(url, granting.id, amount, note, token)
          await reload()
        }}
      />
    </>
  )
}

function GrantDialog({
  account,
  onOpenChange,
  onGrant,
}: {
  account: CreditAccount | null
  onOpenChange: (open: boolean) => void
  onGrant: (amount: number, note?: string) => Promise<void>
}) {
  const [amount, setAmount] = useState('10')
  const [note, setNote] = useState('')
  const [busy, setBusy] = useState(false)

  const parsed = Number.parseInt(amount, 10)
  const valid = Number.isFinite(parsed) && parsed !== 0

  const submit = () => {
    setBusy(true)
    void onGrant(parsed, note.trim() || undefined)
      .then(() => {
        toast.success(`${parsed > 0 ? 'Granted' : 'Removed'} ${Math.abs(parsed)} credits`)
        setNote('')
        onOpenChange(false)
      })
      .catch((error: unknown) => toast.error(messageOf(error)))
      .finally(() => setBusy(false))
  }

  return (
    <Modal
      open={account !== null}
      onOpenChange={onOpenChange}
      title={account ? `Credits for ${account.username || account.id}` : 'Credits'}
      description="Granted credits are spent after the free monthly allowance runs out. A negative amount takes them back; the ledger keeps both, and a balance never goes below zero."
      size="sm"
      footer={
        <Button size="sm" loading={busy} disabled={!valid} onClick={submit}>
          Apply
        </Button>
      }
    >
      <div className="space-y-3">
        <Field label="Amount" help="One credit pays for one successful write off this machine.">
          <Input
            type="number"
            value={amount}
            onChange={(event) => setAmount(event.target.value)}
          />
        </Field>
        <Field label="Note" help="Optional. Stored with the ledger entry.">
          <Input value={note} onChange={(event) => setNote(event.target.value)} />
        </Field>
      </div>
    </Modal>
  )
}
