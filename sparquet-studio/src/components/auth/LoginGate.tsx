/**
 * The screen shown when the runner has users and this browser has no session.
 *
 * It replaces the whole app rather than sitting over it, because with a login
 * required almost nothing behind it would load: the job library lives in the
 * runner's workspace, and every read of it would come back 401.
 *
 * A runner with no users never renders this — see `store/auth.ts`.
 *
 * It carries the runner token field as well, folded away. The token normally
 * lives in Settings, which is behind this screen; when the runner has been
 * restarted with a different one, that is a closed loop with no way out through
 * the interface, and the only fix left is editing localStorage by hand.
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

        <RunnerTokenEscape />

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
 * The runner token, on the one screen that used to be unable to show it.
 *
 * A runner with users does not ask for the token to log in, so this is folded
 * away: it is there for the case where the token is what is actually wrong —
 * an older runner that still demands it everywhere, or a token rotated while
 * this browser held the previous one. Signing in would succeed and every screen
 * behind it would come back 401.
 *
 * Retrying is explicit rather than automatic on every keystroke: pasting a token
 * character by character would fire a request per character, each one a failed
 * attempt against a runner that now rate-limits them.
 */
function RunnerTokenEscape() {
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const setRunnerToken = useSettingsStore((state) => state.setRunnerToken)
  const refresh = useAuthStore((state) => state.refresh)
  const busy = useAuthStore((state) => state.busy)

  const [open, setOpen] = useState(false)

  if (!open) {
    return (
      <button
        type="button"
        className="mt-2 w-full text-[11px] text-content-subtle underline-offset-2 hover:underline"
        onClick={() => setOpen(true)}
      >
        The runner is asking for a token
      </button>
    )
  }

  return (
    <div className="mt-4 space-y-2 border-t border-line pt-3">
      <Field
        label="Runner token"
        htmlFor="login-runner-token"
        help="Printed in the runner's terminal on startup, or pinned with SPARQUET_STUDIO_TOKEN."
      >
        <Input
          id="login-runner-token"
          mono
          type="password"
          autoComplete="off"
          spellCheck={false}
          value={runnerToken}
          placeholder="Paste the token from the runner terminal"
          onChange={(event) => setRunnerToken(event.target.value)}
        />
      </Field>
      <Button
        type="button"
        variant="secondary"
        className="w-full"
        disabled={busy}
        onClick={() => void refresh()}
      >
        {busy ? <Spinner className="h-4 w-4" /> : 'Try the runner again'}
      </Button>
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

        <RunnerTokenEscape />

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
