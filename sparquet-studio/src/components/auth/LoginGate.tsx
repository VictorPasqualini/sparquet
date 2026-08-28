/**
 * The screen shown when the runner has users and this browser has no session.
 *
 * It replaces the whole app rather than sitting over it, because with a login
 * required almost nothing behind it would load: the job library lives in the
 * runner's workspace, and every read of it would come back 401.
 *
 * A runner with no users never renders this — see `store/auth.ts`.
 */

import { useEffect, useState, type FormEvent } from 'react'

import logoMark from '@/assets/logo.png'
import { Button, ErrorCard, Field, Input, Spinner } from '@/components/ui'
import { useAuthStore } from '@/store/auth'
import { useSettingsStore } from '@/store/settings'

export function LoginGate() {
  const signIn = useAuthStore((state) => state.signIn)
  const clearError = useAuthStore((state) => state.clearError)
  const error = useAuthStore((state) => state.error)
  const busy = useAuthStore((state) => state.busy)
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)

  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')

  useEffect(() => clearError, [clearError])

  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (!username.trim() || !password) return
    void signIn(username.trim(), password).then((ok) => {
      // Never keep the password around after the attempt, successful or not.
      if (ok) setPassword('')
    })
  }

  return (
    <div className="flex h-full items-center justify-center bg-canvas p-6">
      <form
        onSubmit={submit}
        className="w-full max-w-sm rounded-2xl border border-line bg-surface p-6 shadow-pop"
      >
        <div className="mb-5 flex items-center gap-3">
          <img src={logoMark} alt="" width={32} height={32} />
          <div>
            <h1 className="text-sm font-semibold text-content">Sign in to Sparquet Studio</h1>
            <p className="text-[11px] text-content-subtle">{runnerUrl}</p>
          </div>
        </div>

        <div className="space-y-3">
          <Field label="Username" htmlFor="login-username">
            <Input
              id="login-username"
              autoFocus
              autoComplete="username"
              value={username}
              onChange={(event) => setUsername(event.target.value)}
            />
          </Field>
          <Field label="Password" htmlFor="login-password">
            <Input
              id="login-password"
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
            />
          </Field>
        </div>

        {error ? <ErrorCard className="mt-3" message={error} size="sm" copyable={false} /> : null}

        <Button type="submit" className="mt-4 w-full" disabled={busy || !username || !password}>
          {busy ? <Spinner className="h-4 w-4" /> : 'Sign in'}
        </Button>

        <p className="mt-4 text-[11px] leading-relaxed text-content-subtle">
          No account yet? The first one is created on the machine running the runner:
          <code className="mx-1 rounded bg-surface-sunken px-1 py-0.5">
            python server/auth.py create-admin
          </code>
        </p>
      </form>
    </div>
  )
}
