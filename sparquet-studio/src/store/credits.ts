/**
 * The team's credit standing, kept in one place so every screen can show it.
 *
 * A run is refused with `402` when the account cannot cover what it declares, and
 * the moment somebody wants to know the balance is the moment a run has just been
 * refused — which is why this is a store and not a hook inside the billing
 * screen. One fetch serves the badge in every header.
 *
 * It is deliberately quiet about failure: a runner that is offline, a Studio with
 * no token yet, or a user without `credits:Read` all end the same way, with
 * `status === null` and nothing on screen. Credits are a detail of a runner that
 * meters; they must never be the reason the editor shows an error.
 */

import { create } from 'zustand'

import { getMyCredits } from '@/lib/runner/credits'
import { useSettingsStore } from '@/store/settings'
import type { CreditStatus } from '@/types/credits'

/** Long enough not to poll, short enough that a stale badge is never a surprise. */
const STALE_AFTER_MS = 30_000

interface CreditsState {
  status: CreditStatus | null
  loading: boolean
  /** When the last successful read landed, as `Date.now()`. */
  fetchedAt: number
  /**
   * Reads the standing. `force` skips the staleness check — what a finished run
   * uses, because it has just changed the number.
   */
  refresh: (options?: { force?: boolean }) => Promise<void>
  clear: () => void
}

let inFlight: Promise<void> | null = null

export const useCreditsStore = create<CreditsState>((set, get) => ({
  status: null,
  loading: false,
  fetchedAt: 0,

  refresh: async (options) => {
    const force = options?.force === true
    if (!force && Date.now() - get().fetchedAt < STALE_AFTER_MS) return
    // Every header mounts at once on a screen change; they share one request.
    if (inFlight) return inFlight

    const { runnerUrl, runnerToken } = useSettingsStore.getState()
    set({ loading: true })
    inFlight = (async () => {
      try {
        const status = await getMyCredits(runnerUrl, runnerToken)
        set({ status, fetchedAt: Date.now() })
      } catch {
        // Offline, unauthenticated, or not allowed to read: all mean "no badge".
        set({ status: null, fetchedAt: Date.now() })
      } finally {
        set({ loading: false })
        inFlight = null
      }
    })()
    return inFlight
  },

  clear: () => set({ status: null, fetchedAt: 0 }),
}))

/** Called when a run ends: it just moved the balance. */
export function refreshCredits(): void {
  void useCreditsStore.getState().refresh({ force: true })
}
