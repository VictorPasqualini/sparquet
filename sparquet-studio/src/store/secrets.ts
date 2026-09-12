/**
 * The connection secrets this runner holds, as far as the browser may know them.
 *
 * Unlike every other store in Studio there is no local mirror: nothing is written
 * to IndexedDB and nothing survives a reload. The list is names, field names and
 * tags, which is already the most a screen is allowed to hold, and caching even
 * that on disk buys a faster panel at the cost of the property the feature
 * exists for. With no runner there are simply no secrets.
 *
 * A refusal is kept as `forbidden` rather than as an error: a user without
 * `secrets:Read` is a normal state of the product, not a fault to report.
 */

import { create } from 'zustand'

import {
  checkSecret,
  deleteSecret,
  isForbidden,
  listSecrets,
  putSecret,
} from '@/lib/runner/secrets'
import { useSettingsStore } from '@/store/settings'
import type { Secret, SecretCheck, SecretWrite } from '@/types/secrets'

interface SecretsState {
  items: Secret[]
  loaded: boolean
  loading: boolean
  /** True when the runner answered 403: the screen says so instead of erroring. */
  forbidden: boolean
  error: string | null
  /** The last check per secret, so a panel can show what resolved and what did not. */
  checks: Record<string, SecretCheck>

  load: (force?: boolean) => Promise<void>
  save: (name: string, body: SecretWrite) => Promise<Secret>
  remove: (name: string) => Promise<void>
  check: (name: string) => Promise<SecretCheck>
  byName: (name: string) => Secret | undefined
  clear: () => void
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

export const useSecretsStore = create<SecretsState>((set, get) => ({
  items: [],
  loaded: false,
  loading: false,
  forbidden: false,
  error: null,
  checks: {},

  load: async (force = false) => {
    if (!force && (get().loaded || get().loading)) return
    const { runnerUrl, runnerToken } = useSettingsStore.getState()
    set({ loading: true, error: null })
    try {
      const items = await listSecrets(runnerUrl, runnerToken)
      set({ items, loaded: true, forbidden: false, error: null })
    } catch (error) {
      if (isForbidden(error)) {
        set({ items: [], loaded: true, forbidden: true, error: null })
      } else {
        set({ items: [], loaded: true, forbidden: false, error: message(error) })
      }
    } finally {
      set({ loading: false })
    }
  },

  save: async (name, body) => {
    const { runnerUrl, runnerToken } = useSettingsStore.getState()
    const saved = await putSecret(name, body, runnerUrl, runnerToken)
    set((state) => {
      const rest = state.items.filter((item) => item.name !== saved.name)
      return { items: [...rest, saved].sort((a, b) => a.name.localeCompare(b.name)) }
    })
    return saved
  },

  remove: async (name) => {
    const { runnerUrl, runnerToken } = useSettingsStore.getState()
    await deleteSecret(name, runnerUrl, runnerToken)
    set((state) => {
      const checks = { ...state.checks }
      delete checks[name]
      return { items: state.items.filter((item) => item.name !== name), checks }
    })
  },

  check: async (name) => {
    const { runnerUrl, runnerToken } = useSettingsStore.getState()
    const result = await checkSecret(name, runnerUrl, runnerToken)
    set((state) => ({ checks: { ...state.checks, [name]: result } }))
    return result
  },

  byName: (name) => get().items.find((item) => item.name === name),

  clear: () =>
    set({ items: [], loaded: false, forbidden: false, error: null, checks: {} }),
}))
