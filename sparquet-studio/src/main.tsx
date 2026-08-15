import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'

import App from './App'
// React Flow's base styles first: index.css overrides handle/edge defaults.
import '@xyflow/react/dist/base.css'
import './index.css'


const container = document.getElementById('root')

if (!container) {
  throw new Error('Root container missing in index.html')
}

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
