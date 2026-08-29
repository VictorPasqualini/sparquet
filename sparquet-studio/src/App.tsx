import { lazy, Suspense, useEffect, useState, type ReactNode } from 'react'
import { createHashRouter, RouterProvider } from 'react-router-dom'
import { Toaster } from 'sonner'

import logoMark from '@/assets/logo.png'

import { LoginGate } from '@/components/auth/LoginGate'
import { AppShell } from '@/components/layout/AppShell'
import { Spinner, TooltipProvider } from '@/components/ui'
import { seedIfEmpty } from '@/lib/storage/seed'
import { Dashboard } from '@/screens/Dashboard'
import { NotFound } from '@/screens/NotFound'
import { useAuthStore } from '@/store/auth'
import { useLibraryStore } from '@/store/library'
import { paintTheme, storedTheme, useSettingsStore } from '@/store/settings'

// Split per screen: the editor pulls React Flow and Monaco, which no other
// route needs, and the overview must stay instant.
const WorkflowDetail = lazy(() =>
  import('@/screens/WorkflowDetail').then((m) => ({ default: m.WorkflowDetail })),
)
const Templates = lazy(() =>
  import('@/screens/Templates').then((m) => ({ default: m.Templates })),
)
const Learn = lazy(() => import('@/screens/Learn').then((m) => ({ default: m.Learn })))
const LessonDetail = lazy(() =>
  import('@/screens/LessonDetail').then((m) => ({ default: m.LessonDetail })),
)
const Settings = lazy(() => import('@/screens/Settings').then((m) => ({ default: m.Settings })))
const Billing = lazy(() => import('@/screens/Billing').then((m) => ({ default: m.Billing })))
const Access = lazy(() => import('@/screens/Access').then((m) => ({ default: m.Access })))
const JobEditor = lazy(() =>
  import('@/screens/JobEditor').then((m) => ({ default: m.JobEditor })),
)
const PipelineEditor = lazy(() =>
  import('@/screens/PipelineEditor').then((m) => ({ default: m.PipelineEditor })),
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
      { path: '/workflows/:workflowId', element: lazyRoute(<WorkflowDetail />) },
      { path: '/templates', element: lazyRoute(<Templates />) },
      { path: '/learn', element: lazyRoute(<Learn />) },
      { path: '/learn/:lessonId', element: lazyRoute(<LessonDetail />) },
      { path: '/billing', element: lazyRoute(<Billing />) },
      { path: '/access', element: lazyRoute(<Access />) },
      { path: '/settings', element: lazyRoute(<Settings />) },
      { path: '*', element: <NotFound /> },
    ],
  },
  // Both editors own the whole viewport, so they sit outside the AppShell chrome.
  { path: '/jobs/:jobId', element: lazyRoute(<JobEditor />) },
  { path: '/pipelines/:pipelineId', element: lazyRoute(<PipelineEditor />) },
])

export default function App() {
  const theme = useSettingsStore((state) => state.theme)
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const load = useLibraryStore((state) => state.load)
  const refreshAuth = useAuthStore((state) => state.refresh)
  const authReady = useAuthStore((state) => state.ready)
  const loginRequired = useAuthStore((state) => state.loginRequired)
  const principal = useAuthStore((state) => state.principal)
  const [ready, setReady] = useState(false)

  // The runner decides whether anyone has to sign in, so it is asked before the
  // library is read: on a runner with users, every one of those reads is a 401.
  const locked = authReady && loginRequired && principal === null

  useEffect(() => {
    paintTheme(theme)
  }, [theme])

  // With no explicit choice stored, keep following the OS while the app is open.
  useEffect(() => {
    if (storedTheme()) return
    const query = window.matchMedia('(prefers-color-scheme: dark)')
    const onChange = () =>
      useSettingsStore.setState({ theme: query.matches ? 'dark' : 'light' })
    query.addEventListener('change', onChange)
    return () => query.removeEventListener('change', onChange)
  }, [])

  useEffect(() => {
    void refreshAuth()
  }, [refreshAuth, runnerUrl, runnerToken])

  useEffect(() => {
    if (!authReady || locked) return
    let cancelled = false
    void (async () => {
      await seedIfEmpty()
      await load()
      if (!cancelled) setReady(true)
    })()
    return () => {
      cancelled = true
    }
  }, [authReady, locked, load])

  if (locked) {
    return (
      <TooltipProvider>
        <LoginGate />
      </TooltipProvider>
    )
  }

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
      <img src={logoMark} alt="" width={48} height={48} />
      <p className="text-xs text-content-subtle">Loading Sparquet Studio…</p>
    </div>
  )
}
