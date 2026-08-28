/**
 * Teams: who shares a budget, and who inherits which roles.
 *
 * A team answers two questions with one object. Billing needs somebody to charge
 * who is not an individual — a squad has one allowance, not one per person — and
 * permissions need a way to say "everyone here may run things" without repeating
 * it on each account.
 *
 * A team only ever grants. Its roles are added to whatever each member holds
 * personally and never take anything away, because a group that can also revoke
 * turns "why can this person not do X" into an unanswerable question.
 */

import { Plus, Trash2, Users } from 'lucide-react'
import { useCallback, useEffect, useState } from 'react'
import { toast } from 'sonner'

import { Badge, Button, Field, Input, Modal, Spinner, useConfirm } from '@/components/ui'
import { useAuthStore } from '@/store/auth'
import type { AuthRole, AuthTeam } from '@/types/auth'

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** A row of role chips that toggle. Used for a team's inherited roles. */
function RolePicker({
  roles,
  selected,
  disabled,
  onChange,
}: {
  roles: AuthRole[]
  selected: string[]
  disabled?: boolean
  onChange: (next: string[]) => void
}) {
  if (roles.length === 0) {
    return <p className="text-2xs text-content-subtle">No roles to grant.</p>
  }
  return (
    <div className="flex flex-wrap gap-1.5">
      {roles.map((role) => {
        const held = selected.includes(role.name)
        return (
          <button
            key={role.name}
            type="button"
            disabled={disabled}
            title={role.description}
            className={[
              'rounded-md border px-2 py-1 text-2xs transition-colors',
              held
                ? 'border-brand-500 bg-brand-500/10 text-content'
                : 'border-line text-content-subtle hover:text-content',
              disabled ? 'cursor-default opacity-70' : '',
            ].join(' ')}
            onClick={() =>
              onChange(
                held ? selected.filter((item) => item !== role.name) : [...selected, role.name],
              )
            }
          >
            {role.name}
          </button>
        )
      })}
    </div>
  )
}

export function TeamsPanel() {
  const can = useAuthStore((state) => state.can)
  const principal = useAuthStore((state) => state.principal)
  const fetchTeams = useAuthStore((state) => state.fetchTeams)
  const fetchRoles = useAuthStore((state) => state.fetchRoles)
  const addTeam = useAuthStore((state) => state.addTeam)
  const editTeam = useAuthStore((state) => state.editTeam)
  const removeTeam = useAuthStore((state) => state.removeTeam)
  const [confirm, confirmDialog] = useConfirm()

  const mayRead = can('iam:ReadUsers')
  const mayManage = can('iam:ManageTeams')

  const [teams, setTeams] = useState<AuthTeam[] | null>(null)
  const [roles, setRoles] = useState<AuthRole[]>([])
  const [loading, setLoading] = useState(false)
  const [creating, setCreating] = useState(false)
  const [editing, setEditing] = useState<AuthTeam | null>(null)

  const reload = useCallback(async () => {
    if (!mayRead) return
    setLoading(true)
    try {
      const [found, available] = await Promise.all([fetchTeams(), fetchRoles()])
      setTeams(found)
      setRoles(available)
    } catch (error) {
      toast.error(messageOf(error))
    } finally {
      setLoading(false)
    }
  }, [fetchRoles, fetchTeams, mayRead])

  useEffect(() => {
    void reload()
  }, [reload])

  if (!mayRead) return null

  return (
    <>
      <div className="flex items-center justify-between gap-4 border-t border-line pt-4">
        <div className="space-y-0.5">
          <p className="text-sm text-content">Teams</p>
          <p className="max-w-md text-2xs leading-relaxed text-content-subtle">
            A team is one credit account shared by its members, and a way of granting the same
            roles to all of them at once. Deleting one moves its people to the default team.
          </p>
        </div>
        {mayManage ? (
          <Button
            size="sm"
            variant="secondary"
            icon={<Plus className="h-3.5 w-3.5" />}
            onClick={() => setCreating(true)}
          >
            New team
          </Button>
        ) : null}
      </div>

      <div className="rounded-lg border border-line">
        {loading && teams === null ? (
          <div className="flex items-center justify-center py-6">
            <Spinner className="h-4 w-4" />
          </div>
        ) : teams && teams.length > 0 ? (
          <ul className="divide-y divide-line">
            {teams.map((team) => (
              <li key={team.id} className="flex items-center gap-3 px-3 py-2.5">
                <div className="min-w-0 flex-1">
                  <p className="flex items-center gap-1.5 truncate text-sm text-content">
                    <Users className="h-3.5 w-3.5 text-content-subtle" />
                    {team.name}
                    {principal?.teamId === team.id ? (
                      <span className="text-2xs text-content-subtle">(yours)</span>
                    ) : null}
                  </p>
                  <p className="mt-0.5 flex flex-wrap items-center gap-1 text-2xs text-content-subtle">
                    {team.members} {team.members === 1 ? 'member' : 'members'}
                    {team.roles.map((role) => (
                      <Badge key={role} tone="neutral">
                        {role}
                      </Badge>
                    ))}
                  </p>
                </div>
                {mayManage ? (
                  <>
                    <Button size="sm" variant="ghost" onClick={() => setEditing(team)}>
                      Edit
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      icon={<Trash2 className="h-3.5 w-3.5" />}
                      onClick={() => {
                        void (async () => {
                          const ok = await confirm({
                            title: `Delete the team ${team.name}?`,
                            message:
                              'Its members move to the default team and keep their own roles. The ledger it built up stays where it is.',
                            confirmLabel: 'Delete',
                            variant: 'danger',
                          })
                          if (!ok) return
                          try {
                            await removeTeam(team.id)
                            toast.success(`Deleted ${team.name}`)
                            await reload()
                          } catch (error) {
                            toast.error(messageOf(error))
                          }
                        })()
                      }}
                    >
                      Delete
                    </Button>
                  </>
                ) : null}
              </li>
            ))}
          </ul>
        ) : (
          <p className="px-3 py-4 text-2xs text-content-subtle">No teams.</p>
        )}
      </div>

      <TeamDialog
        open={creating || editing !== null}
        team={editing}
        roles={roles}
        onOpenChange={(open) => {
          if (!open) {
            setCreating(false)
            setEditing(null)
          }
        }}
        onSubmit={async (body) => {
          if (editing) await editTeam(editing.id, body)
          else await addTeam(body)
          await reload()
        }}
      />

      {confirmDialog}
    </>
  )
}

function TeamDialog({
  open,
  team,
  roles,
  onOpenChange,
  onSubmit,
}: {
  open: boolean
  team: AuthTeam | null
  roles: AuthRole[]
  onOpenChange: (open: boolean) => void
  onSubmit: (body: { name: string; roles: string[] }) => Promise<void>
}) {
  const [name, setName] = useState('')
  const [selected, setSelected] = useState<string[]>([])
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    if (!open) return
    setName(team?.name ?? '')
    setSelected(team?.roles ?? [])
  }, [open, team])

  const submit = () => {
    setBusy(true)
    void onSubmit({ name: name.trim(), roles: selected })
      .then(() => {
        toast.success(team ? `Updated ${name.trim()}` : `Created ${name.trim()}`)
        onOpenChange(false)
      })
      .catch((error: unknown) => toast.error(messageOf(error)))
      .finally(() => setBusy(false))
  }

  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      title={team ? `Team ${team.name}` : 'New team'}
      description="Members share this team's credit account and inherit the roles below on top of their own."
      size="sm"
      footer={
        <Button size="sm" loading={busy} disabled={name.trim().length === 0} onClick={submit}>
          Save
        </Button>
      }
    >
      <div className="space-y-3">
        <Field label="Name">
          <Input value={name} onChange={(event) => setName(event.target.value)} />
        </Field>
        <Field
          label="Inherited roles"
          help="Optional. Everyone in the team gets these as well as whatever they hold personally."
        >
          <RolePicker roles={roles} selected={selected} onChange={setSelected} />
        </Field>
      </div>
    </Modal>
  )
}
