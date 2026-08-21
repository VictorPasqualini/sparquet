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
}

interface SettingsState {
  theme: Theme
  ai: AiSettings
  /** Persist the API key in localStorage. Off keeps it in memory for the session. */
  persistApiKey: boolean
  runnerUrl: string
  /** Shared secret printed by the local runner; required by /run and /validate. */
  runnerToken: string
  canvas: CanvasPreferences
  /** Dismissed the first-run tour. */
  onboarded: boolean

  setTheme: (theme: Theme) => void
  toggleTheme: () => void
  setAi: (patch: Partial<AiSettings>) => void
  setPersistApiKey: (value: boolean) => void
  setRunnerUrl: (url: string) => void
  setRunnerToken: (token: string) => void
  setCanvas: (patch: Partial<CanvasPreferences>) => void
  setOnboarded: (value: boolean) => void
}

export const DEFAULT_AI_SETTINGS: AiSettings = {
  provider: 'anthropic',
  model: 'claude-sonnet-4-5',
  baseUrl: 'https://api.anthropic.com',
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
}

export const useSettingsStore = create<SettingsState>()(
  persist(
    (set, get) => ({
      theme: storedTheme() ?? systemTheme(),
      ai: DEFAULT_AI_SETTINGS,
      persistApiKey: false,
      runnerUrl: 'http://127.0.0.1:8787',
      runnerToken: '',
      canvas: DEFAULT_CANVAS,
      onboarded: false,

      setTheme: (theme) => {
        applyTheme(theme)
        set({ theme })
      },
      toggleTheme: () => get().setTheme(get().theme === 'dark' ? 'light' : 'dark'),
      setAi: (patch) => set((state) => ({ ai: { ...state.ai, ...patch } })),
      setPersistApiKey: (persistApiKey) => set({ persistApiKey }),
      setRunnerUrl: (runnerUrl) => set({ runnerUrl }),
      setRunnerToken: (runnerToken) => set({ runnerToken }),
      setCanvas: (patch) => set((state) => ({ canvas: { ...state.canvas, ...patch } })),
      setOnboarded: (onboarded) => set({ onboarded }),
    }),
    {
      name: 'sparquet-studio:settings',
      version: 1,
      // The API key is only written to disk when the user opts in. `theme` is
      // deliberately absent: it lives in THEME_STORAGE_KEY, written only when the
      // user picks one, so an untouched install keeps following the system
      // instead of freezing whatever it happened to be on the first visit.
      partialize: (state) => ({
        ai: state.persistApiKey ? state.ai : { ...state.ai, apiKey: '' },
        persistApiKey: state.persistApiKey,
        runnerUrl: state.runnerUrl,
        runnerToken: state.runnerToken,
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
