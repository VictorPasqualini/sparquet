import { create } from 'zustand'
import { persist } from 'zustand/middleware'

import type { AiSettings } from '@/types/ai'

export type Theme = 'dark' | 'light'

/** Key read by the inline script in index.html to paint the right theme first. */
export const THEME_STORAGE_KEY = 'sparquet-studio:theme'

/**
 * The default follows the browser: an explicit choice is persisted and wins, and
 * until then the OS preference decides. Read here as well as in the pre-paint
 * script in index.html so the store never disagrees with what is on screen.
 */
export function storedTheme(): Theme | null {
  try {
    const value = localStorage.getItem(THEME_STORAGE_KEY)
    return value === 'dark' || value === 'light' ? value : null
  } catch {
    return null
  }
}

export function systemTheme(): Theme {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return 'light'
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

export interface CanvasPreferences {
  snapToGrid: boolean
  showGrid: boolean
  showMinimap: boolean
  animateEdges: boolean
  /** Auto-run the linter as the graph changes. */
  liveLint: boolean
  /**
   * The strip of recent executions above the pipeline canvas. On by default:
   * the question it answers — has this been failing — is the one somebody
   * opening a pipeline has before they have any other.
   */
  showRunRail: boolean
}

interface SettingsState {
  theme: Theme
  ai: AiSettings
  /**
   * The user chose this provider themselves, so nothing may move it.
   *
   * False means the setting is still the default nobody touched, which is what
   * lets the Studio adopt a local model it finds on the machine. The first
   * manual change here — any field — pins it for good.
   */
  aiPinned: boolean
  /** Persist the API key in localStorage. Off keeps it in memory for the session. */
  persistApiKey: boolean
  runnerUrl: string
  /** Shared secret printed by the local runner; required by /run and /validate. */
  runnerToken: string
  /**
   * Name every run started here is attributed to ("run as" in the history).
   * Empty means the runner records its own OS account instead — the runner
   * authenticates a token, not a person, so this is a label, never a permission.
   */
  runAs: string
  canvas: CanvasPreferences
  /** Dismissed the first-run tour. */
  onboarded: boolean

  setTheme: (theme: Theme) => void
  toggleTheme: () => void
  setAi: (patch: Partial<AiSettings>) => void
  /**
   * Adopts a provider the Studio discovered rather than one the user picked, so
   * the next machine — or the next restart, with the runner up this time — is
   * free to discover a different one.
   */
  adoptAi: (patch: Partial<AiSettings>) => void
  setPersistApiKey: (value: boolean) => void
  setRunnerUrl: (url: string) => void
  setRunnerToken: (token: string) => void
  setRunAs: (name: string) => void
  setCanvas: (patch: Partial<CanvasPreferences>) => void
  setOnboarded: (value: boolean) => void
}

/**
 * The provider a Studio nobody configured starts on.
 *
 * `runner` rather than a vendor because a default that needs a credit card is
 * not a default: the local runner asks for no key, bills nothing, and is the
 * only provider that can read this installation's own formats. A machine
 * without one is detected at boot (`detectLocalAi`) and moved to Ollama, or
 * left here to say what to install — never moved to a provider that charges.
 */
export const DEFAULT_AI_SETTINGS: AiSettings = {
  provider: 'runner',
  model: '',
  baseUrl: '',
  apiKey: '',
  temperature: 0.2,
  maxTokens: 8000,
  shareJobContext: true,
}

const DEFAULT_CANVAS: CanvasPreferences = {
  snapToGrid: true,
  showGrid: true,
  showMinimap: true,
  animateEdges: true,
  liveLint: true,
  showRunRail: true,
}

/** The slice written to localStorage; `partialize` below produces exactly this. */
type PersistedSettings = Pick<
  SettingsState,
  | 'ai'
  | 'aiPinned'
  | 'persistApiKey'
  | 'runnerUrl'
  | 'runnerToken'
  | 'runAs'
  | 'canvas'
  | 'onboarded'
>

export const useSettingsStore = create<SettingsState>()(
  persist(
    (set, get) => ({
      theme: storedTheme() ?? systemTheme(),
      ai: DEFAULT_AI_SETTINGS,
      aiPinned: false,
      persistApiKey: false,
      runnerUrl: 'http://127.0.0.1:8787',
      runnerToken: '',
      runAs: '',
      canvas: DEFAULT_CANVAS,
      onboarded: false,

      setTheme: (theme) => {
        applyTheme(theme)
        set({ theme })
      },
      toggleTheme: () => get().setTheme(get().theme === 'dark' ? 'light' : 'dark'),
      setAi: (patch) => set((state) => ({ ai: { ...state.ai, ...patch }, aiPinned: true })),
      adoptAi: (patch) => set((state) => ({ ai: { ...state.ai, ...patch } })),
      setPersistApiKey: (persistApiKey) => set({ persistApiKey }),
      setRunnerUrl: (runnerUrl) => set({ runnerUrl }),
      setRunnerToken: (runnerToken) => set({ runnerToken }),
      setRunAs: (runAs) => set({ runAs }),
      setCanvas: (patch) => set((state) => ({ canvas: { ...state.canvas, ...patch } })),
      setOnboarded: (onboarded) => set({ onboarded }),
    }),
    {
      name: 'sparquet-studio:settings',
      version: 2,
      // Version 1 had no `aiPinned` and defaulted to Anthropic. Anyone carrying
      // that state configured it — or decided not to — under a Studio where the
      // provider was theirs alone to set, so it is pinned on the way in. Only a
      // fresh install gets a provider chosen for it.
      migrate: (persisted, version) => {
        const state = (persisted ?? {}) as PersistedSettings
        return version < 2 ? { ...state, aiPinned: true } : state
      },
      // The API key is only written to disk when the user opts in. `theme` is
      // deliberately absent: it lives in THEME_STORAGE_KEY, written only when the
      // user picks one, so an untouched install keeps following the system
      // instead of freezing whatever it happened to be on the first visit.
      partialize: (state) => ({
        ai: state.persistApiKey ? state.ai : { ...state.ai, apiKey: '' },
        aiPinned: state.aiPinned,
        persistApiKey: state.persistApiKey,
        runnerUrl: state.runnerUrl,
        runnerToken: state.runnerToken,
        runAs: state.runAs,
        canvas: state.canvas,
        onboarded: state.onboarded,
      }),
      onRehydrateStorage: () => () => {
        paintTheme(useSettingsStore.getState().theme)
      },
    },
  ),
)

/** Paints the theme without recording it as a choice the user made. */
export function paintTheme(theme: Theme): void {
  document.documentElement.dataset.theme = theme
}

/**
 * Records an explicit choice: paints it and writes the standalone key the
 * pre-paint script in index.html reads before React boots. Only call this from a
 * user action — writing it on boot would turn 'follow the system' into a pin.
 */
export function applyTheme(theme: Theme): void {
  paintTheme(theme)
  try {
    localStorage.setItem(THEME_STORAGE_KEY, theme)
  } catch {
    // Private mode: the attribute above is still applied for this session.
  }
}
