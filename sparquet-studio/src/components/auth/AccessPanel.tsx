/**
 * Identity and user administration, as rendered inside Settings.
 *
 * Two runners are described here by the same panel. One has no users: the shared
 * token is the identity, everyone holding it can do everything, and the only
 * action offered is creating the first administrator — which is what turns it
 * into the other kind. The other has users, and then this shows who you are and,
 * if your roles allow it, who else has access.
 *
 * Every control here is a convenience. The runner enforces the same rules again
 * on each request, so a hidden button is a courtesy, not a lock.
 */

import { KeyRound, ShieldCheck, Trash2, UserPlus } from 'lucide-react'
import { useCallback, useEffect, useState } from 'react'
import { toast } from 'sonner'

import { Badge, Button, Field, Input, Modal, Select, Spinner, Toggle } from '@/components/ui'
import { useAuthStore } from '@/store/auth'
import type { AuthRole, AuthTeam, AuthUser, RecoveryCode } from '@/types/auth'

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

export function AccessPanel() {
  const loginRequired = useAuthStore((state) => state.loginRequired)
  const principal = useAuthStore((state) => state.principal)
  const session = useAuthStore((state) => state.session)
  const signOut = useAuthStore((state) => state.signOut)
  const can = useAuthStore((state) => state.can)
  const fetchUsers = useAuthStore((state) => state.fetchUsers)
  const fetchRoles = useAuthStore((state) => state.fetchRoles)
  const fetchTeams = useAuthStore((state) => state.fetchTeams)
  const addUser = useAuthStore((state) => state.addUser)
  const editUser = useAuthStore((state) => state.editUser)
  const removeUser = useAuthStore((state) => state.removeUser)
  const changePassword = useAuthStore((state) => state.changePassword)
  const issueRecovery = useAuthStore((state) => state.issueRecovery)

  const [users, setUsers] = useState<AuthUser[] | null>(null)
  const [roles, setRoles] = useState<AuthRole[]>([])
  const [teams, setTeams] = useState<AuthTeam[]>([])
  const [loading, setLoading] = useState(false)
  const [createOpen, setCreateOpen] = useState(false)

  const mayManage = can('iam:ManageUsers')
  const mayRead = can('iam:ReadUsers')

  const reload = useCallback(async () => {
    if (!mayRead) return
    setLoading(true)
    try {
      const [people, available, groups] = await Promise.all([
        fetchUsers(),
        fetchRoles(),
        fetchTeams(),
      ])
      setUsers(people)
      setRoles(available)
      setTeams(groups)
    } catch (error) {
      toast.error(messageOf(error))
    } finally {
      setLoading(false)
    }
  }, [fetchRoles, fetchTeams, fetchUsers, mayRead])

  useEffect(() => {
    void reload()
  }, [reload])

  return (
    <>
      <div className="rounded-lg border border-line bg-surface-sunken px-3 py-2.5">
        {loginRequired && principal ? (
          <div className="flex items-center justify-between gap-4">
            <div className="min-w-0">
              <p className="truncate text-sm text-content">
                {principal.displayName || principal.username}
              </p>
              <p className="mt-0.5 flex flex-wrap items-center gap-1 text-2xs text-content-subtle">
                Signed in as <code>{principal.username}</code>
                {principal.teamName ? (
                  <>
                    <span>in</span>
                    <Badge tone="info">{principal.teamName}</Badge>
                  </>
                ) : null}
                {principal.roles.map((role) => (
                  <Badge key={role} tone="neutral">
                    {role}
                  </Badge>
                ))}
                {principal.teamRoles
                  .filter((role) => !principal.roles.includes(role))
                  .map((role) => (
                    <Badge key={role} tone="neutral">
                      {role} (team)
                    </Badge>
                  ))}
              </p>
            </div>
            <Button size="sm" variant="secondary" onClick={() => void signOut()} disabled={!session}>
              Sign out
            </Button>
          </div>
        ) : (
          <p className="text-2xs leading-relaxed text-content-subtle">
            This runner has no users. The shared token is the identity, and everyone holding it can
            do everything. Create the first administrator to turn on sign-in — nobody is locked out
            by doing it, because the token still opens the login.
          </p>
        )}
      </div>

      {mayManage ? (
        <div className="flex items-center justify-between gap-4 border-t border-line pt-4">
          <div className="space-y-0.5">
            <p className="text-sm text-content">People</p>
            <p className="max-w-md text-2xs leading-relaxed text-content-subtle">
              Roles decide what each person may read, change and run. The last enabled administrator
              cannot be removed or demoted.
            </p>
          </div>
          <Button
            size="sm"
            variant="secondary"
            icon={<UserPlus className="h-3.5 w-3.5" />}
            onClick={() => setCreateOpen(true)}
          >
            Add user
          </Button>
        </div>
      ) : null}

      {mayRead ? (
        <div className="rounded-lg border border-line">
          {loading && users === null ? (
            <div className="flex items-center justify-center py-6">
              <Spinner className="h-4 w-4" />
            </div>
          ) : users && users.length > 0 ? (
            <ul className="divide-y divide-line">
              {users.map((user) => (
                <UserRow
                  key={user.id}
                  user={user}
                  roles={roles}
                  teams={teams}
                  editable={mayManage}
                  self={principal?.userId === user.id}
                  onChange={async (changes) => {
                    await editUser(user.id, changes)
                    await reload()
                  }}
                  onDelete={async () => {
                    await removeUser(user.id)
                    await reload()
                  }}
                  onPassword={async (password, currentPassword) => {
                    await changePassword(user.id, password, currentPassword)
                  }}
                  onRecovery={(password) => issueRecovery(user.id, password)}
                />
              ))}
            </ul>
          ) : (
            <p className="px-3 py-4 text-2xs text-content-subtle">No users yet.</p>
          )}
        </div>
      ) : null}

      <CreateUserDialog
        open={createOpen}
        roles={roles}
        teams={teams}
        onOpenChange={setCreateOpen}
        onCreate={async (body) => {
          await addUser(body)
          await reload()
        }}
      />
    </>
  )
}

function UserRow({
  user,
  roles,
  teams,
  editable,
  self,
  onChange,
  onDelete,
  onPassword,
  onRecovery,
}: {
  user: AuthUser
  roles: AuthRole[]
  teams: AuthTeam[]
  editable: boolean
  self: boolean
  onChange: (changes: { roles?: string[]; disabled?: boolean; team?: string }) => Promise<void>
  onDelete: () => Promise<void>
  onPassword: (password: string, currentPassword?: string) => Promise<void>
  onRecovery: (password: string) => Promise<RecoveryCode>
}) {
  const [busy, setBusy] = useState(false)
  const [passwordOpen, setPasswordOpen] = useState(false)
  const [recoveryOpen, setRecoveryOpen] = useState(false)
  const [recovery, setRecovery] = useState<RecoveryCode | null>(null)

  const guard = (action: () => Promise<void>) => () => {
    setBusy(true)
    void action()
      .catch((error: unknown) => toast.error(messageOf(error)))
      .finally(() => setBusy(false))
  }

  return (
    <li className="flex items-center gap-3 px-3 py-2.5">
      <div className="min-w-0 flex-1">
        <p className="truncate text-sm text-content">
          {user.username}
          {self ? <span className="ml-1.5 text-2xs text-content-subtle">(you)</span> : null}
        </p>
        <p className="text-2xs text-content-subtle">
          {user.lastLoginAt ? `Last signed in ${user.lastLoginAt.slice(0, 16).replace('T', ' ')}` : 'Never signed in'}
        </p>
      </div>

      <Select
        ariaLabel={`Role for ${user.username}`}
        className="w-32"
        disabled={!editable || busy}
        value={user.roles[0] ?? ''}
        options={roles.map((role) => ({ value: role.name, label: role.name }))}
        onValueChange={(value) => guard(() => onChange({ roles: [value] }))()}
      />

      <Select
        ariaLabel={`Team for ${user.username}`}
        className="w-32"
        disabled={!editable || busy || teams.length === 0}
        value={user.teamId ?? ''}
        options={teams.map((team) => ({ value: team.id, label: team.name }))}
        onValueChange={(value) => guard(() => onChange({ team: value }))()}
      />

      <Toggle
        label="Enabled"
        checked={!user.disabled}
        disabled={!editable || busy}
        onCheckedChange={(checked) => guard(() => onChange({ disabled: !checked }))()}
      />

      {editable ? (
        <>
          <Button size="sm" variant="ghost" disabled={busy} onClick={() => setPasswordOpen(true)}>
            Password
          </Button>
          <Button
            size="sm"
            variant="ghost"
            icon={<KeyRound className="h-3.5 w-3.5" />}
            disabled={busy}
            onClick={() => setRecoveryOpen(true)}
          >
            Recovery code
          </Button>
          <Button
            size="sm"
            variant="ghost"
            icon={<Trash2 className="h-3.5 w-3.5" />}
            disabled={busy}
            onClick={guard(onDelete)}
          >
            Remove
          </Button>
        </>
      ) : null}

      <PasswordDialog
        open={passwordOpen}
        username={user.username}
        self={self}
        onOpenChange={setPasswordOpen}
        onSubmit={onPassword}
      />

      <IssueRecoveryDialog
        open={recoveryOpen}
        username={user.username}
        onOpenChange={setRecoveryOpen}
        onIssue={async (password) => {
          setRecovery(await onRecovery(password))
        }}
      />

      <RecoveryDialog recovery={recovery} onClose={() => setRecovery(null)} />
    </li>
  )
}

/**
 * Asks for the administrator's OWN password before minting a recovery code.
 *
 * Minting one is a way to take over an account: whoever holds the code chooses
 * that person's next password. An unattended session with Studio open should not
 * be enough to do it, so the runner asks for the password again and this dialog
 * is where it is typed. It is never the password of the person being recovered —
 * they are by definition the one who cannot supply it.
 */
function IssueRecoveryDialog({
  open,
  username,
  onOpenChange,
  onIssue,
}: {
  open: boolean
  username: string
  onOpenChange: (open: boolean) => void
  onIssue: (password: string) => Promise<void>
}) {
  const [password, setPassword] = useState('')
  const [busy, setBusy] = useState(false)

  const close = (next: boolean) => {
    if (!next) setPassword('')
    onOpenChange(next)
  }

  const submit = () => {
    setBusy(true)
    void onIssue(password)
      .then(() => close(false))
      .catch((error: unknown) => toast.error(messageOf(error)))
      .finally(() => setBusy(false))
  }

  return (
    <Modal
      open={open}
      onOpenChange={close}
      title={`Issue a recovery code for ${username}`}
      description="Confirm with your own password. The code lets whoever holds it set that account's password, so it is worth as much as the account itself."
      size="sm"
      footer={
        <Button
          size="sm"
          icon={<KeyRound className="h-3.5 w-3.5" />}
          loading={busy}
          disabled={password.length === 0}
          onClick={submit}
        >
          Issue
        </Button>
      }
    >
      <Field label="Your password" help="Not theirs — yours, the one you signed in with.">
        <Input
          type="password"
          autoComplete="current-password"
          value={password}
          onChange={(event) => setPassword(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter' && password.length > 0) submit()
          }}
        />
      </Field>
    </Modal>
  )
}

/**
 * Shows a freshly minted recovery code — once.
 *
 * The runner keeps only its hash, so closing this dialog is the last time anyone
 * can read it. Issuing another one invalidates this, which is the way out if it
 * is lost.
 */
function RecoveryDialog({
  recovery,
  onClose,
}: {
  recovery: RecoveryCode | null
  onClose: () => void
}) {
  return (
    <Modal
      open={recovery !== null}
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title={recovery ? `Recovery code for ${recovery.username}` : 'Recovery code'}
      description="Hand it over out of band. It works once, and it lets the person choose their own password without knowing the old one."
      size="sm"
      footer={
        <Button size="sm" onClick={onClose}>
          Done
        </Button>
      }
    >
      {recovery ? (
        <div className="space-y-2">
          <code className="block break-all rounded-lg border border-line bg-surface-sunken px-3 py-2 text-2xs text-content">
            {recovery.code}
          </code>
          <p className="text-2xs leading-relaxed text-content-subtle">
            Valid until {recovery.expiresAt.slice(0, 16).replace('T', ' ')}. It is not stored in
            readable form — this is the only time it is shown. Redeeming it ends every session that
            account has open.
          </p>
        </div>
      ) : null}
    </Modal>
  )
}

function CreateUserDialog({
  open,
  roles,
  teams,
  onOpenChange,
  onCreate,
}: {
  open: boolean
  roles: AuthRole[]
  teams: AuthTeam[]
  onOpenChange: (open: boolean) => void
  onCreate: (body: {
    username: string
    password: string
    roles: string[]
    team?: string
  }) => Promise<void>
}) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [role, setRole] = useState('admin')
  const [team, setTeam] = useState('')
  const [busy, setBusy] = useState(false)

  const close = (next: boolean) => {
    if (!next) {
      setUsername('')
      setPassword('')
    }
    onOpenChange(next)
  }

  const submit = () => {
    setBusy(true)
    void onCreate({
      username: username.trim(),
      password,
      roles: [role],
      team: team || undefined,
    })
      .then(() => {
        toast.success(`${username.trim()} can now sign in`)
        close(false)
      })
      .catch((error: unknown) => toast.error(messageOf(error)))
      .finally(() => setBusy(false))
  }

  return (
    <Modal
      open={open}
      onOpenChange={close}
      title="Add a user"
      description="The password is hashed by the runner; it is never stored as typed and cannot be read back."
      size="sm"
      footer={
        <Button
          size="sm"
          loading={busy}
          disabled={username.trim().length === 0 || password.length < 8}
          onClick={submit}
        >
          Create
        </Button>
      }
    >
      <div className="space-y-3">
        <Field label="Username">
          <Input value={username} onChange={(event) => setUsername(event.target.value)} />
        </Field>
        <Field label="Password" help="At least 8 characters.">
          <Input
            type="password"
            autoComplete="new-password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
          />
        </Field>
        <Field label="Role">
          <Select
            value={role}
            onValueChange={setRole}
            options={
              roles.length > 0
                ? roles.map((item) => ({
                    value: item.name,
                    label: item.name,
                    hint: item.description,
                  }))
                : [{ value: 'admin', label: 'admin' }]
            }
          />
        </Field>
        <Field
          label="Team"
          help="Whose credit account pays for their runs. Left empty, they join the default team."
        >
          <Select
            value={team}
            placeholder="Default team"
            onValueChange={setTeam}
            options={teams.map((item) => ({ value: item.id, label: item.name }))}
          />
        </Field>
      </div>
    </Modal>
  )
}

function PasswordDialog({
  open,
  username,
  self,
  onOpenChange,
  onSubmit,
}: {
  open: boolean
  username: string
  self: boolean
  onOpenChange: (open: boolean) => void
  onSubmit: (password: string, currentPassword?: string) => Promise<void>
}) {
  const [current, setCurrent] = useState('')
  const [next, setNext] = useState('')
  const [busy, setBusy] = useState(false)

  const close = (value: boolean) => {
    if (!value) {
      setCurrent('')
      setNext('')
    }
    onOpenChange(value)
  }

  const submit = () => {
    setBusy(true)
    void onSubmit(next, self ? current : undefined)
      .then(() => {
        // Changing your own password ends the session it opened, by design.
        toast.success(self ? 'Password changed — sign in again' : `Password reset for ${username}`)
        close(false)
      })
      .catch((error: unknown) => toast.error(messageOf(error)))
      .finally(() => setBusy(false))
  }

  return (
    <Modal
      open={open}
      onOpenChange={close}
      title={self ? 'Change your password' : `Reset the password for ${username}`}
      description={
        self
          ? 'Every session opened with the old password stops working, including this one.'
          : 'The person keeps their roles; only the password changes. Their open sessions end.'
      }
      size="sm"
      footer={
        <Button
          size="sm"
          icon={<ShieldCheck className="h-3.5 w-3.5" />}
          loading={busy}
          disabled={next.length < 8 || (self && current.length === 0)}
          onClick={submit}
        >
          Save
        </Button>
      }
    >
      <div className="space-y-3">
        {self ? (
          <Field label="Current password">
            <Input
              type="password"
              autoComplete="current-password"
              value={current}
              onChange={(event) => setCurrent(event.target.value)}
            />
          </Field>
        ) : null}
        <Field label="New password" help="At least 8 characters.">
          <Input
            type="password"
            autoComplete="new-password"
            value={next}
            onChange={(event) => setNext(event.target.value)}
          />
        </Field>
      </div>
    </Modal>
  )
}
