/**
 * Which history a buffer reads and writes.
 *
 * The runs themselves live on the runner (`/query/history`), not in this
 * browser. The runner is the side that knows what happened to a statement — how
 * long it took, how many rows came back, what the failure said — and a saved
 * query is a file two people can open, so what it has been run as belongs next
 * to the file rather than in whichever browser happened to run it. Every run
 * says who made it.
 *
 * A buffer nobody has saved has no file to share. Its runs are keyed by the tab
 * *and* the person, so a scratch statement stays the business of whoever ran it
 * — the runner builds the key that way rather than filtering on the way out.
 *
 * This module is what decides which of the two a buffer is; see
 * `lib/runner/client` for the calls themselves.
 */

import type { HistoryScope } from '@/lib/runner/client'

export type { HistoryScope, QueryRun } from '@/lib/runner/client'

/**
 * What to ask the runner for.
 *
 * A saved query is identified by its file, so its runs survive the tab being
 * closed and opened again — and are the same runs a teammate sees. A tab nobody
 * has saved has nothing but itself to be identified by.
 */
export function historyScope(queryId: string | null, tabId: string): HistoryScope {
  return queryId ? { savedQueryId: queryId } : { tab: tabId }
}
