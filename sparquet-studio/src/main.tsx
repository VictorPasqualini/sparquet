import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'

import { configureStorage } from '@/lib/storage/db'
import { useSettingsStore } from '@/store/settings'

import App from './App'
// React Flow's base styles first: index.css overrides handle/edge defaults.
import '@xyflow/react/dist/base.css'
import './index.css'


/**
 * Storage has to know where the runner is before anything reads a record: the
 * library lives in the runner's workspace, and only the settings know its address.
 * Re-applied on every change so starting the runner, or fixing the token, reaches
 * the files without a reload.
 */
function bindStorageToRunner(): void {
  const apply = (state: { runnerUrl: string; runnerToken: string }): void => {
    configureStorage({ baseUrl: state.runnerUrl, token: state.runnerToken })
  }
  apply(useSettingsStore.getState())
  useSettingsStore.subscribe(apply)
}

bindStorageToRunner()

const container = document.getElementById('root')

if (!container) {
  throw new Error('Root container missing in index.html')
}

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
