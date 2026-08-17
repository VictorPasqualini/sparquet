import { fileURLToPath, URL } from 'node:url'

import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    port: 5273,
    // Fail loudly instead of drifting to 5274: the local runner only trusts
    // http://localhost:5273 (SPARQUET_STUDIO_ORIGINS), so a "helpful" fallback
    // port silently breaks every run with a CORS refusal that looks like a
    // runner bug. A port clash is almost always a dev server left running.
    strictPort: true,
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
    rollupOptions: {
      output: {
        manualChunks: {
          react: ['react', 'react-dom', 'react-router-dom'],
          flow: ['@xyflow/react', '@dagrejs/dagre'],
          editor: ['@monaco-editor/react'],
        },
      },
    },
  },
})
