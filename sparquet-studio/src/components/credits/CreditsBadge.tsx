/**
 * What this team can still spend, small enough to live in any header.
 *
 * The number that matters while working is `available`: the free allowance plus
 * the balance, less whatever runs in flight are holding. A run is refused with
 * `402` against that number, so it is the one that answers "can I press Run".
 *
 * It renders nothing at all when there is nothing honest to say — no runner, no
 * session, a runner that only meters without enforcing. A badge that showed a
 * balance nobody is enforcing would be inviting people to budget around a rule
 * that does not exist.
 */

import { Coins } from 'lucide-react'
import { useEffect } from 'react'
import { Link } from 'react-router-dom'

import { Tooltip } from '@/components/ui'
import { cn } from '@/lib/utils/cn'
import { useCreditsStore } from '@/store/credits'

interface CreditsBadgeProps {
  className?: string
  /** Off inside the editors, which own the whole viewport and have no router
   *  route to go back from. */
  linkToBilling?: boolean
}

export function CreditsBadge({ className, linkToBilling = true }: CreditsBadgeProps) {
  const status = useCreditsStore((state) => state.status)
  const refresh = useCreditsStore((state) => state.refresh)

  useEffect(() => {
    void refresh()
  }, [refresh])

  if (!status || !status.enforced) return null

  const { account } = status
  const empty = account.available <= 0
  const low = !empty && account.available <= Math.max(1, status.creditsPerWrite * 3)

  const hint = [
    `${account.available} credit${account.available === 1 ? '' : 's'} available`,
    `${account.freeRemaining} free left this month, ${account.balance} granted`,
    account.held > 0 ? `${account.held} held by runs in flight` : '',
    empty ? 'Runs are refused until this team gets more.' : '',
  ]
    .filter(Boolean)
    .join(' · ')

  const body = (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-2xs tabular-nums transition-colors',
        empty
          ? 'border-state-danger/40 bg-state-danger/10 text-state-danger'
          : low
            ? 'border-state-warning/40 bg-state-warning/10 text-state-warning'
            : 'border-line text-content-subtle hover:border-line-strong hover:text-content',
        className,
      )}
    >
      <Coins className="h-3 w-3 shrink-0" aria-hidden />
      {account.available}
    </span>
  )

  return (
    <Tooltip content={hint} side="bottom">
      {linkToBilling ? (
        <Link to="/billing" aria-label={hint} className="no-drag">
          {body}
        </Link>
      ) : (
        <span aria-label={hint}>{body}</span>
      )}
    </Tooltip>
  )
}
