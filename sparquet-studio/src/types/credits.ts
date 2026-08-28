/**
 * Execution credits, as the runner's `/credits/*` endpoints describe them.
 *
 * One coin per Job, and only when the Job does not run on the runner's own
 * machine — see `server/credits.py` for what counts as leaving it. Two numbers
 * matter and they are not the same: `balance` is what may still be spent, and
 * `spent` is what remote work has cost so far whether or not a balance was
 * actually taken. They differ because a runner meters from day one and only
 * enforces when somebody turns enforcement on.
 */

export interface CreditAccount {
  /** The user id, or `token` on a runner with no users. */
  id: string
  username: string
  balance: number
  spent: number
  createdAt: string | null
  updatedAt: string | null
}

export interface CreditStatus {
  account: CreditAccount
  /** False means the ledger is watching but nothing is being blocked. */
  enforced: boolean
  creditsPerJob: number
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
  jobRunId: string | null
  pipelineRunId: string | null
  /** Where it ran: `spark://…`, `databricks`, `connect sc://…`. */
  target: string | null
  jobName: string | null
  note: string | null
}
