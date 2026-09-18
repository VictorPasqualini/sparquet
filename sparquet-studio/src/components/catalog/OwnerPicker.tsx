/**
 * Who a dataset, Job, Pipeline or Workflow belongs to.
 *
 * Ownership is the half of the model that the grants list cannot show. A grant
 * says what somebody may do today; an owner says who answers for the object —
 * holds every privilege on it, may re-grant it without being an administrator
 * of the runner, and cannot be shut out of it by a deny.
 *
 * Two things this has to say out loud, because guessing either one wrong sends
 * a person to the wrong screen:
 *   - an owner inherited from a container is NOT recorded here, so clearing it
 *     has to happen where it is written;
 *   - handing the object over is immediate and takes your own claim with it,
 *     unless you also own what contains it.
 */

import { Crown, UserRound, Users } from 'lucide-react'
import { useMemo, useState } from 'react'

import { Badge, Button, Field, Input, Segmented, Select, type SelectOption } from '@/components/ui'
import type { Owner, PrincipalKind, ResourceKind } from '@/lib/iam'
import type { AuthTeam, AuthUser } from '@/types/auth'

/** What the picker produces; the screen supplies the resource it belongs to. */
export type NewOwner = Pick<Owner, 'principalKind' | 'principalId' | 'principalLabel'>

export interface OwnerPickerProps {
  resource: ResourceKind
  /** The record on this resource itself, or null when it has none. */
  owner: Owner | null
  /**
   * The owner that actually applies and where it sits, own record or inherited.
   * `source` is a `kind/id`, so `workflow/p-42` or `dataset//lake/silver`.
   */
  effective: { owner: Owner; source: string } | null
  teams: AuthTeam[]
  users: AuthUser[]
  onAssign: (owner: NewOwner) => Promise<void> | void
  onClear: () => Promise<void> | void
  /** False shows who owns it without offering to change it. */
  editable?: boolean
}

function ownerName(owner: Owner, teams: AuthTeam[], users: AuthUser[]): string {
  if (owner.principalKind === 'team') {
    return teams.find((team) => team.id === owner.principalId)?.name
      ?? owner.principalLabel
      ?? owner.principalId
  }
  return users.find((user) => user.id === owner.principalId)?.username
    ?? owner.principalLabel
    ?? owner.principalId
}

export function OwnerPicker({
  resource,
  owner,
  effective,
  teams,
  users,
  onAssign,
  onClear,
  editable = true,
}: OwnerPickerProps) {
  const [kind, setKind] = useState<PrincipalKind>('team')
  const [principal, setPrincipal] = useState('')
  const [busy, setBusy] = useState(false)

  const options = useMemo<SelectOption[]>(() => {
    if (kind === 'team') {
      return teams.map((team) => ({
        value: team.id,
        label: team.name,
        hint: `${team.members} ${team.members === 1 ? 'member' : 'members'}`,
      }))
    }
    return users.map((user) => ({
      value: user.id,
      label: user.username,
      hint: user.displayName ?? undefined,
    }))
  }, [kind, teams, users])

  // Same fallback the grants editor takes: a runner with no user records still
  // has principals, so the id is typed rather than the control disappearing.
  const freeText = options.length === 0

  const inherited = Boolean(effective && !owner)

  const assign = async () => {
    const principalId = principal.trim()
    if (!principalId) return
    setBusy(true)
    try {
      await onAssign({
        principalKind: kind,
        principalId,
        principalLabel:
          options.find((option) => option.value === principalId)?.label ?? principalId,
      })
      setPrincipal('')
    } finally {
      setBusy(false)
    }
  }

  const clear = async () => {
    setBusy(true)
    try {
      await onClear()
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="space-y-2 rounded-lg border border-line p-3">
      <div className="flex flex-wrap items-center gap-2">
        <span className="flex items-center gap-1.5 text-xs font-medium text-content">
          <Crown className="h-4 w-4 text-state-warning" />
          Owner
        </span>
        {effective ? (
          <>
            <Badge
              tone={inherited ? 'neutral' : 'success'}
              icon={effective.owner.principalKind === 'team' ? <Users /> : <UserRound />}
            >
              {ownerName(effective.owner, teams, users)}
            </Badge>
            {inherited ? (
              <span className="text-2xs text-content-subtle">
                inherited from <code className="text-content-muted">{effective.source}</code>
              </span>
            ) : null}
          </>
        ) : (
          <Badge tone="warning">nobody</Badge>
        )}
      </div>

      <p className="text-2xs leading-relaxed text-content-subtle">
        {effective
          ? `The owner holds every level on this ${resource} and may change its rules without being an administrator. No deny reaches them.`
          : `This ${resource} has no owner, so only an administrator of the runner can change its rules.`}
      </p>

      {editable ? (
        <div className="flex flex-wrap items-end gap-2">
          <Segmented
            size="sm"
            ariaLabel="Owner kind"
            value={kind}
            onChange={(next) => {
              setKind(next)
              setPrincipal('')
            }}
            options={[
              { value: 'team', label: 'Team', title: 'Survives the person leaving' },
              { value: 'user', label: 'User', title: 'One person answers for it' },
            ]}
          />
          <Field label={kind === 'team' ? 'Team' : 'User'} className="min-w-[12rem] flex-1">
            {freeText ? (
              <Input
                value={principal}
                placeholder={kind === 'team' ? 'team id' : 'username'}
                onChange={(event) => setPrincipal(event.target.value)}
              />
            ) : (
              <Select
                value={principal}
                options={options}
                placeholder="Pick one"
                ariaLabel={kind === 'team' ? 'Team to own it' : 'User to own it'}
                onValueChange={setPrincipal}
              />
            )}
          </Field>
          <Button size="sm" disabled={busy || !principal.trim()} onClick={() => void assign()}>
            {owner ? 'Hand over' : 'Set owner'}
          </Button>
          {owner ? (
            <Button
              size="sm"
              variant="ghost"
              disabled={busy}
              onClick={() => void clear()}
              title="Leave it to the grants alone"
            >
              Clear
            </Button>
          ) : null}
        </div>
      ) : null}
    </section>
  )
}
