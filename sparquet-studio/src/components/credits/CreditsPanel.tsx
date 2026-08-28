/**
 * Execution credits, as rendered inside Settings.
 *
 * A Job that runs on the runner's own machine is free; anything that leaves it —
 * a cluster master, Spark Connect, a hosted runtime — costs one coin. The runner
 * decides that from the Job's own `spark` block, not from anything the caller
 * claims, so this panel only reports what happened.
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

  return (
    <>
      <div className="rounded-lg border border-line bg-surface-sunken px-3 py-2.5">
        <div className="flex items-center justify-between gap-4">
          <div className="min-w-0">
            <p className="flex items-center gap-1.5 text-sm text-content">
              <Coins className="h-3.5 w-3.5 text-content-subtle" />
              {status.enforced ? `${status.account.balance} credits left` : 'Credits are not enforced'}
              <Badge tone={status.enforced ? 'warning' : 'neutral'}>
                {status.enforced ? 'enforced' : 'metering only'}
              </Badge>
            </p>
            <p className="mt-0.5 text-2xs leading-relaxed text-content-subtle">
              {status.creditsPerJob} per Job that runs away from this machine; local runs are free.
              Remote work has cost <code>{status.account.spent}</code> so far
              {status.enforced ? '.' : ' — recorded, but nothing was deducted.'}
            </p>
          </div>
          {mayManage ? (
            <Button
              size="sm"
              variant="secondary"
              icon={<Plus className="h-3.5 w-3.5" />}
              onClick={() => setGranting(status.account)}
            >
              Add credits
            </Button>
          ) : null}
        </div>
      </div>

      {mayRead && accounts.length > 0 ? (
        <div className="rounded-lg border border-line">
          <ul className="divide-y divide-line">
            {accounts.map((account) => (
              <li key={account.id} className="flex items-center gap-3 px-3 py-2">
                <div className="min-w-0 flex-1">
                  <p className="truncate text-sm text-content">{account.username || account.id}</p>
                  <p className="text-2xs text-content-subtle">
                    {account.balance} left · {account.spent} spent
                  </p>
                </div>
                {mayManage ? (
                  <Button size="sm" variant="ghost" onClick={() => setGranting(account)}>
                    Grant
                  </Button>
                ) : null}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <div className="space-y-1">
        <p className="text-2xs text-content-subtle">Recent charges</p>
        {ledger.length === 0 ? (
          <p className="rounded-lg border border-line px-3 py-2.5 text-2xs text-content-subtle">
            Nothing yet. Only Jobs that leave this machine are written here.
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
      description="A negative amount takes credits back. The ledger keeps both, and a balance never goes below zero."
      size="sm"
      footer={
        <Button size="sm" loading={busy} disabled={!valid} onClick={submit}>
          Apply
        </Button>
      }
    >
      <div className="space-y-3">
        <Field label="Amount" help="One credit pays for one Job that runs off this machine.">
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
