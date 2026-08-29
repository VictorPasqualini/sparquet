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
  /** What could be spent right now: the allowance plus the balance, less holds. */
  available: number
  /** Reserved by runs still in flight. Promised, not spent — a hold comes back. */
  held: number
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
  /** The workflow the run belonged to. Null for a run started outside Studio. */
  workflowId: string | null
  /** Who ran it. Null when the caller was a shared runner token, which is nobody. */
  actor: string | null
  /** The labels the run carried when it was charged, frozen on the entry: a Job
   *  retagged later does not rewrite the months already billed. */
  tags: string[]
}

/** How a month of spending can be sliced. The team always pays; this is only
 *  how its invoice is read back. */
export type SpendGroupBy = 'team' | 'user' | 'workflow' | 'job' | 'tag'

/** One line of a bill. `key` null is spending with no such dimension — a run
 *  from a script belongs to no workflow, a Job nobody labelled has no tag —
 *  reported rather than dropped, so nothing is missing from the bill. */
export interface SpendGroup {
  key: string | null
  label: string | null
  writes: number
  /** The whole cost. `waived` is the part the free allowance absorbed. */
  charged: number
  waived: number
  runs: number
  lastAt: string | null
}

export interface SpendBreakdown {
  period: string
  groupBy: SpendGroupBy
  /** An account id, or `all` when the caller may see the whole runner. */
  scope: string
  total: SpendGroup
  groups: SpendGroup[]
  /**
   * True when one run can appear in several lines — the case for tags and for
   * nothing else, since a run wearing two of them is counted in full under each.
   * The lines then add up to more than `total`, and anything drawing them as
   * shares of a whole has to say so.
   */
  overlapping: boolean
}

/** One month of the spending series. */
export interface SpendPeriod {
  period: string
  writes: number
  charged: number
  waived: number
  runs: number
}

/** Several months, oldest first. Months with no spending are present as zeros:
 *  a gap would make the shape of the series lie. */
export interface SpendTimeline {
  scope: string
  periods: SpendPeriod[]
}
