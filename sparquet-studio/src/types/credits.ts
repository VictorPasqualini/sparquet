/**
 * Execution credits, as the runner's `/credits/*` endpoints describe them.
 *
 * One credit per **successful write**, and only when the write does not happen on
 * the runner's own machine — see `server/credits.py` for what counts as leaving
 * it. A run that failed before writing wrote nothing and therefore costs nothing.
 *
 * The account is a team, not a person: a squad has one budget and one invoice.
 * Every team gets a free allowance each calendar month, spent before any balance
 * somebody granted. And two numbers that look alike are not: `balance` is what
 * may still be spent, `spent` is what remote work has cost so far whether or not
 * a balance was actually taken — a runner meters from day one and only enforces
 * when somebody turns enforcement on.
 */

export interface CreditAccount {
  /** The team id, or `token` on a runner with no users. */
  id: string
  /** The team's name. */
  username: string
  balance: number
  spent: number
  /** `YYYY-MM`. The allowance below belongs to it and refills when it turns. */
  period: string
  freeUsed: number
  freeMonthly: number
  freeRemaining: number
  /** What could be spent right now: the rest of the allowance plus the balance. */
  available: number
  createdAt: string | null
  updatedAt: string | null
}

/** One month in three numbers. `waived` is what the free allowance covered. */
export interface CreditUsage {
  period: string
  writes: number
  charged: number
  waived: number
}

export interface CreditStatus {
  account: CreditAccount
  /** False means the ledger is watching but nothing is being blocked. */
  enforced: boolean
  creditsPerWrite: number
  freeMonthly: number
  usage: CreditUsage
}

/**
 * What one execution cost, as it comes back with a run and with a past run read
 * from history — so the price shows up next to the work rather than only in the
 * billing screen.
 */
export interface RunCharge {
  amount: number
  writes: number
  /** False on a runner that meters without enforcing: nothing actually moved. */
  applied: boolean
  freeAmount: number
  /** Non-zero only when a run wrote more than its team could pay for. */
  shortfall: number
  target: string | null
  balanceAfter: number | null
}

export interface CreditEntry {
  id: string
  accountId: string
  /** Negative for a run, positive for a grant. */
  amount: number
  reason: string
  /** False for a metered row that cost nobody anything. */
  applied: boolean
  balanceAfter: number
  createdAt: string
  /** Successful writes this row paid for. Zero on a grant. */
  writes: number
  /** How much of it the free monthly allowance covered. */
  freeAmount: number
  shortfall: number
  period: string | null
  jobRunId: string | null
  pipelineRunId: string | null
  /** Where it ran: `spark://…`, `databricks`, `connect sc://…`. */
  target: string | null
  jobName: string | null
  note: string | null
}
