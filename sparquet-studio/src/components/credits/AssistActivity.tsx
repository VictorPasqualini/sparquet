/**
 * One month of assistant turns, and what they cost.
 *
 * Here rather than on the Assistant screen because the question it answers — is
 * this thing costing us anything — is a billing question, and the person asking
 * it is looking at an invoice, not at a chat.
 *
 * Usage and cost are shown as two separate numbers on purpose. A turn answered
 * by a model on the runner's own machine is recorded with an amount of zero, so
 * a team that moved its assistant in-house watches the turns climb while the
 * charge stays flat — which is the evidence that the move worked. Summing them
 * into one figure would hide exactly that.
 *
 * Nothing here is read from the ledger. The ledger records movements of money
 * and stays empty when nothing moved; this reads `assist_usage`, which records
 * work whether or not anyone paid for it.
 */

import { useCallback, useEffect, useState } from 'react'

import { SectionTitle, Spinner } from '@/components/ui'
import { monthName } from '@/lib/billing'
import { getAssistUsage } from '@/lib/runner/assistant'
import { cn } from '@/lib/utils/cn'
import { formatCount, formatDuration } from '@/lib/utils/format'
import { useSettingsStore } from '@/store/settings'
import type { AssistSummary, AssistTurn } from '@/types/assistant'

/** How many turns are listed before the rest is folded away. */
const VISIBLE = 6

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** `2026-09-16T14:02:11` → `16 Sep, 14:02`, in the reader's own locale. */
function whenOf(iso: string): string {
  const at = new Date(iso.endsWith('Z') ? iso : `${iso}Z`)
  if (Number.isNaN(at.getTime())) return iso
  return at.toLocaleString(undefined, {
    day: 'numeric',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
  })
}

export interface AssistActivityProps {
  /** The month to read. Owned by the screen, because the chart above selects it too. */
  period: string
}

export function AssistActivity({ period }: AssistActivityProps) {
  const url = useSettingsStore((state) => state.runnerUrl)
  const token = useSettingsStore((state) => state.runnerToken)

  const [data, setData] = useState<AssistSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [expanded, setExpanded] = useState(false)
  const [failure, setFailure] = useState('')

  const load = useCallback(async () => {
    setLoading(true)
    try {
      setData(await getAssistUsage(url, token, { period, limit: 20 }))
      setFailure('')
    } catch (error) {
      setFailure(messageOf(error))
      setData(null)
    } finally {
      setLoading(false)
    }
  }, [period, token, url])

  useEffect(() => {
    void load()
  }, [load])

  useEffect(() => {
    setExpanded(false)
  }, [period])

  const turns = data?.turns ?? 0
  const recent = data?.recent ?? []
  const shown = expanded ? recent : recent.slice(0, VISIBLE)
  const hidden = recent.length - shown.length
  const tokensIn = data?.inputTokens ?? 0
  const tokensOut = data?.outputTokens ?? 0

  return (
    <div className="space-y-4">
      <SectionTitle
        action={<span className="text-2xs text-content-subtle">{monthName(period)}</span>}
      >
        What the assistant answered
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
            <Stat label="Turns" value={formatCount(turns)}>
              {turns === 0
                ? 'Nobody asked anything'
                : `${formatCount(data?.toolCalls ?? 0)} tool calls`}
            </Stat>
            <Stat label="On this machine" value={formatCount(data?.localTurns ?? 0)}>
              {/* The sentence people come here for. */}
              Free — the model never left the runner
            </Stat>
            <Stat
              label="Charged"
              value={formatCount(data?.charged ?? 0)}
              tone={(data?.charged ?? 0) > 0 ? 'warning' : undefined}
            >
              {(data?.remoteTurns ?? 0) === 0
                ? 'No turn went to a paid provider'
                : `${formatCount(data?.remoteTurns ?? 0)} turns off this machine`}
            </Stat>
            <Stat label="Model time" value={formatDuration((data?.seconds ?? 0) * 1000)}>
              {tokensIn + tokensOut === 0
                ? 'No provider reported tokens'
                : `${formatCount(tokensIn)} in · ${formatCount(tokensOut)} out`}
            </Stat>
          </dl>

          {recent.length > 0 && (
            <div className="space-y-1">
              {shown.map((turn) => (
                <TurnRow key={turn.id} turn={turn} />
              ))}
              {hidden > 0 && (
                <button
                  type="button"
                  onClick={() => setExpanded(true)}
                  className="text-2xs text-content-subtle transition-colors hover:text-brand-500"
                >
                  Show {hidden} more
                </button>
              )}
            </div>
          )}
        </>
      )}
    </div>
  )
}

function TurnRow({ turn }: { turn: AssistTurn }) {
  const tokens = turn.inputTokens + turn.outputTokens

  return (
    <div className="flex items-baseline gap-3 border-b border-line py-1 last:border-0">
      <span className="w-28 shrink-0 text-2xs text-content-subtle">
        {whenOf(turn.createdAt)}
      </span>
      <span className="min-w-0 flex-1 truncate font-mono text-2xs text-content-muted">
        {turn.model || turn.provider}
        {turn.actor ? ` · ${turn.actor}` : ''}
      </span>
      {turn.toolCalls > 0 && (
        <span className="shrink-0 text-2xs text-content-subtle">
          {turn.toolCalls} {turn.toolCalls === 1 ? 'tool' : 'tools'}
        </span>
      )}
      {tokens > 0 && (
        <span className="shrink-0 tabular-nums text-2xs text-content-subtle">
          {formatCount(tokens)} tok
        </span>
      )}
      <span
        className={cn(
          'w-14 shrink-0 text-right tabular-nums text-2xs',
          turn.amount > 0 ? 'text-state-warning' : 'text-content-subtle',
        )}
      >
        {turn.amount > 0 ? `−${turn.amount}` : 'free'}
      </span>
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
