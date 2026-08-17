import { create } from 'zustand'
import { persist } from 'zustand/middleware'

import type { AiSettings } from '@/types/ai'

export type Theme = 'dark' | 'light'

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
      theme: 'dark',
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
      // The API key is only written to disk when the user opts in.
      partialize: (state) => ({
        theme: state.theme,
        ai: state.persistApiKey ? state.ai : { ...state.ai, apiKey: '' },
        persistApiKey: state.persistApiKey,
        runnerUrl: state.runnerUrl,
        runnerToken: state.runnerToken,
        canvas: state.canvas,
        onboarded: state.onboarded,
      }),
      onRehydrateStorage: () => (state) => {
        if (state) applyTheme(state.theme)
      },
    },
  ),
)

/** Key read by the inline script in index.html to paint the right theme first. */
export const THEME_STORAGE_KEY = 'sparquet-studio:theme'

/**
 * Writes the theme in both places: the DOM attribute the app renders against,
 * and the standalone key the pre-paint script reads before React boots.
 */
export function applyTheme(theme: Theme): void {
  document.documentElement.dataset.theme = theme
  try {
    localStorage.setItem(THEME_STORAGE_KEY, theme)
  } catch {
    // Private mode: the attribute above is still applied for this session.
  }
}
