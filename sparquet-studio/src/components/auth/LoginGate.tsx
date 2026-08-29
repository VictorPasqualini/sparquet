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
import { toast } from 'sonner'

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
  const [recovering, setRecovering] = useState(false)

  useEffect(() => clearError, [clearError])

  if (recovering) {
    return <RecoverForm onDone={() => setRecovering(false)} />
  }

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

        <button
          type="button"
          className="mt-3 w-full text-[11px] text-content-subtle underline-offset-2 hover:underline"
          onClick={() => {
            clearError()
            setRecovering(true)
          }}
        >
          I have a recovery code
        </button>

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

/**
 * Spending a recovery code, for somebody who cannot log in.
 *
 * There is no "email me a link" here and there should not be: the runner has no
 * mail server. An administrator mints the code (Settings → Access) or an
 * operator does it at the terminal with `python server/auth.py recovery-code
 * <user>`, and it is handed over out of band. What it buys is the ability to
 * choose your own password rather than being told one.
 */
function RecoverForm({ onDone }: { onDone: () => void }) {
  const recoverPassword = useAuthStore((state) => state.recoverPassword)
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)

  const [code, setCode] = useState('')
  const [password, setPassword] = useState('')
  const [busy, setBusy] = useState(false)
  const [failure, setFailure] = useState('')

  const submit = (event: FormEvent) => {
    event.preventDefault()
    setBusy(true)
    setFailure('')
    void recoverPassword(code.trim(), password)
      .then(() => {
        toast.success('Password set. Sign in with it.')
        onDone()
      })
      .catch((error: unknown) => {
        setFailure(error instanceof Error ? error.message : String(error))
      })
      .finally(() => setBusy(false))
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
            <h1 className="text-sm font-semibold text-content">Use a recovery code</h1>
            <p className="text-[11px] text-content-subtle">{runnerUrl}</p>
          </div>
        </div>

        <div className="space-y-3">
          <Field label="Recovery code" htmlFor="recover-code">
            <Input
              id="recover-code"
              autoFocus
              value={code}
              onChange={(event) => setCode(event.target.value)}
            />
          </Field>
          <Field label="New password" htmlFor="recover-password" help="At least 8 characters.">
            <Input
              id="recover-password"
              type="password"
              autoComplete="new-password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
            />
          </Field>
        </div>

        {failure ? <ErrorCard className="mt-3" message={failure} size="sm" copyable={false} /> : null}

        <Button
          type="submit"
          className="mt-4 w-full"
          disabled={busy || code.trim().length === 0 || password.length < 8}
        >
          {busy ? <Spinner className="h-4 w-4" /> : 'Set password'}
        </Button>

        <button
          type="button"
          className="mt-3 w-full text-[11px] text-content-subtle underline-offset-2 hover:underline"
          onClick={onDone}
        >
          Back to sign in
        </button>

        <p className="mt-4 text-[11px] leading-relaxed text-content-subtle">
          No code? An administrator issues one in Settings → Access, or whoever runs the machine
          does it with
          <code className="mx-1 rounded bg-surface-sunken px-1 py-0.5">
            python server/auth.py recovery-code &lt;user&gt;
          </code>
        </p>
      </form>
    </div>
  )
}
