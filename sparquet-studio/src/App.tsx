import { lazy, Suspense, useEffect, useState, type ReactNode } from 'react'
import { createHashRouter, RouterProvider } from 'react-router-dom'
import { Toaster } from 'sonner'

import { AppShell } from '@/components/layout/AppShell'
import { Spinner, TooltipProvider } from '@/components/ui'
import { seedIfEmpty } from '@/lib/storage/seed'
import { Dashboard } from '@/screens/Dashboard'
import { NotFound } from '@/screens/NotFound'
import { useLibraryStore } from '@/store/library'
import { applyTheme, useSettingsStore } from '@/store/settings'

// Split per screen: the editor pulls React Flow and Monaco, which no other
// route needs, and the overview must stay instant.
const ProjectDetail = lazy(() =>
  import('@/screens/ProjectDetail').then((m) => ({ default: m.ProjectDetail })),
)
const Templates = lazy(() =>
  import('@/screens/Templates').then((m) => ({ default: m.Templates })),
)
const Learn = lazy(() => import('@/screens/Learn').then((m) => ({ default: m.Learn })))
const LessonDetail = lazy(() =>
  import('@/screens/LessonDetail').then((m) => ({ default: m.LessonDetail })),
)
const Settings = lazy(() =>
  import('@/screens/Settings').then((m) => ({ default: m.Settings })),
)
const WorkflowEditor = lazy(() =>
  import('@/screens/WorkflowEditor').then((m) => ({ default: m.WorkflowEditor })),
)

function Loading() {
  return (
    <div className="flex h-full items-center justify-center py-24">
      <Spinner className="h-5 w-5" />
    </div>
  )
}

const lazyRoute = (element: ReactNode) => <Suspense fallback={<Loading />}>{element}</Suspense>

// Hash routing keeps the app deployable as a static bundle on any host.
const router = createHashRouter([
  {
    element: <AppShell />,
    children: [
      { path: '/', element: <Dashboard /> },
      { path: '/projects/:projectId', element: lazyRoute(<ProjectDetail />) },
      { path: '/templates', element: lazyRoute(<Templates />) },
      { path: '/learn', element: lazyRoute(<Learn />) },
      { path: '/learn/:lessonId', element: lazyRoute(<LessonDetail />) },
      { path: '/settings', element: lazyRoute(<Settings />) },
      { path: '*', element: <NotFound /> },
    ],
  },
  { path: '/workflows/:workflowId', element: lazyRoute(<WorkflowEditor />) },
])

export default function App() {
  const theme = useSettingsStore((state) => state.theme)
  const load = useLibraryStore((state) => state.load)
  const [ready, setReady] = useState(false)

  useEffect(() => {
    applyTheme(theme)
  }, [theme])

  useEffect(() => {
    let cancelled = false
    void (async () => {
      await seedIfEmpty()
      await load()
      if (!cancelled) setReady(true)
    })()
    return () => {
      cancelled = true
    }
  }, [load])

  return (
    <TooltipProvider>
      {ready ? <RouterProvider router={router} /> : <BootScreen />}
      <Toaster
        position="bottom-right"
        toastOptions={{
          className:
            'bg-surface-overlay border border-line text-content text-xs rounded-xl shadow-pop',
        }}
      />
    </TooltipProvider>
  )
}

function BootScreen() {
  return (
    <div className="flex h-full flex-col items-center justify-center gap-4 bg-canvas">
      <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-brand-500 text-black">
        <svg viewBox="0 0 24 24" className="h-6 w-6" fill="currentColor" aria-hidden>
          <path d="M4 4h7v7H4zM13 4h7v4h-7zM13 10h7v10h-7zM4 13h7v7H4z" />
        </svg>
      </div>
      <p className="text-xs text-content-subtle">Loading Sparquet Studio…</p>
    </div>
  )
}
