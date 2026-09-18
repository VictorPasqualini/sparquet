/**
 * The access rules on one resource, listed and edited.
 *
 * Written once for datasets, Jobs and Pipelines, because the three differ only
 * in the word used for the resource and in what each level lets someone do.
 *
 * Two things the list has to say out loud, or the screen lies:
 *   - a resource with NO rule is open to everyone who may reach the runner. A
 *     table nobody has closed is not a protected table;
 *   - a deny wins over every allow, so a rule can look granted and not be.
 * Both are stated in the panel rather than left for the reader to infer.
 *
 * The runner evaluates the same list again before it opens anything. Nothing
 * here is the enforcement — it is the editor for it.
 */

import { ShieldAlert, ShieldCheck, Trash2, UserRound, Users } from 'lucide-react'
import { useMemo, useState } from 'react'

import { Badge, Button, Field, Input, Segmented, Select, type SelectOption } from '@/components/ui'
import {
  ANY,
  LEVEL_HINT,
  MAX_NOTE,
  type AccessLevel,
  type Grant,
  type GrantEffect,
  type PrincipalKind,
  type ResourceKind,
} from '@/lib/iam'
import type { AuthTeam, AuthUser } from '@/types/auth'

/** What the panel needs to create a grant; the screen supplies the resource. */
export type NewGrant = Omit<Grant, 'id' | 'updatedAt' | 'resource' | 'resourceId'>

export interface GrantsPanelProps {
  resource: ResourceKind
  /** Teams the runner knows. Empty when it has no user records at all. */
  teams: AuthTeam[]
  users: AuthUser[]
  grants: readonly Grant[]
  onGrant: (grant: NewGrant) => Promise<void> | void
  onRevoke: (id: string) => Promise<void> | void
  /** False greys the editor: the reader may see the rules but not change them. */
  editable?: boolean
}

type PickerKind = PrincipalKind | 'everyone'

const LEVEL_OPTIONS: SelectOption[] = [
  { value: 'read', label: 'Read', hint: 'Query and inspect' },
  { value: 'write', label: 'Write', hint: 'Read, plus change it' },
  { value: 'admin', label: 'Admin', hint: 'Write, plus delete and re-grant' },
]

const LEVEL_TONE = { read: 'info', write: 'brand', admin: 'success' } as const

function principalName(
  grant: Grant,
  teams: AuthTeam[],
  users: AuthUser[],
): { label: string; icon: typeof Users } {
  if (grant.principalId === ANY) return { label: 'everyone', icon: Users }
  if (grant.principalKind === 'team') {
    const team = teams.find((item) => item.id === grant.principalId)
    return { label: team?.name ?? grant.principalLabel ?? grant.principalId, icon: Users }
  }
  const user = users.find((item) => item.id === grant.principalId)
  return {
    label: user?.username ?? grant.principalLabel ?? grant.principalId,
    icon: UserRound,
  }
}

export function GrantsPanel({
  resource,
  teams,
  users,
  grants,
  onGrant,
  onRevoke,
  editable = true,
}: GrantsPanelProps) {
  const [kind, setKind] = useState<PickerKind>('team')
  const [principal, setPrincipal] = useState('')
  const [level, setLevel] = useState<AccessLevel>('read')
  const [effect, setEffect] = useState<GrantEffect>('allow')
  const [note, setNote] = useState('')
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

  // A runner with no user records still has an identity — the shared token — so
  // the id is typed instead of picked rather than the editor going away.
  const freeText = kind !== 'everyone' && options.length === 0

  const sorted = useMemo(
    () =>
      [...grants].sort((a, b) => {
        // Denies first: they are the rules that change the answer.
        if (a.effect !== b.effect) return a.effect === 'deny' ? -1 : 1
        return b.updatedAt - a.updatedAt
      }),
    [grants],
  )

  const add = async () => {
    const principalId = kind === 'everyone' ? ANY : principal.trim()
    if (!principalId) return
    setBusy(true)
    try {
      await onGrant({
        principalKind: kind === 'user' ? 'user' : 'team',
        principalId,
        principalLabel:
          kind === 'everyone'
            ? undefined
            : (options.find((option) => option.value === principalId)?.label ?? principalId),
        level,
        effect,
        note: note.trim() || undefined,
      })
      setPrincipal('')
      setNote('')
    } finally {
      setBusy(false)
    }
  }

  const denies = sorted.filter((grant) => grant.effect === 'deny').length

  return (
    <section className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <span className="flex items-center gap-1.5 text-xs font-medium text-content">
          <ShieldCheck className="h-4 w-4 text-brand-400" />
          Who can use this {resource}
        </span>
        {grants.length === 0 ? (
          <Badge tone="warning">not restricted</Badge>
        ) : (
          <>
            <Badge tone="brand">{grants.length - denies} allow</Badge>
            {denies > 0 ? <Badge tone="danger">{denies} deny</Badge> : null}
          </>
        )}
      </div>

      <p className="text-2xs leading-relaxed text-content-subtle">
        {grants.length === 0
          ? `No rule names this ${resource}, so anyone who may reach the runner can use it. The first rule closes it to everyone the rules do not reach.`
          : 'A deny always wins, whatever else grants it. Levels are cumulative: admin includes write, write includes read.'}
      </p>

      {sorted.length > 0 ? (
        <ul className="divide-y divide-line rounded-lg border border-line">
          {sorted.map((grant) => {
            const { label, icon: Icon } = principalName(grant, teams, users)
            return (
              <li key={grant.id} className="flex flex-wrap items-center gap-2 px-3 py-2">
                {grant.effect === 'deny' ? (
                  <ShieldAlert className="h-4 w-4 shrink-0 text-danger-400" />
                ) : (
                  <Icon className="h-4 w-4 shrink-0 text-content-subtle" />
                )}
                <span className="min-w-0 truncate text-xs text-content">{label}</span>
                <Badge tone={grant.effect === 'deny' ? 'danger' : LEVEL_TONE[grant.level]}>
                  {grant.effect === 'deny' ? `deny ${grant.level}` : grant.level}
                </Badge>
                <span
                  className="text-2xs text-content-subtle"
                  title={LEVEL_HINT[resource][grant.level]}
                >
                  {grant.principalKind === 'team' && grant.principalId !== ANY ? 'team' : null}
                </span>
                {grant.note ? (
                  <span className="min-w-0 truncate text-2xs italic text-content-muted">
                    {grant.note}
                  </span>
                ) : null}
                {editable ? (
                  <Button
                    size="xs"
                    variant="ghost"
                    className="ml-auto"
                    onClick={() => void onRevoke(grant.id)}
                    title="Remove this rule"
                  >
                    <Trash2 />
                  </Button>
                ) : null}
              </li>
            )
          })}
        </ul>
      ) : null}

      {editable ? (
        <div className="space-y-2 rounded-lg border border-dashed border-line p-3">
          <Segmented
            size="sm"
            ariaLabel="Who the rule is about"
            value={kind}
            onChange={(next) => {
              setKind(next)
              setPrincipal(next === 'everyone' ? ANY : '')
            }}
            options={[
              { value: 'team', label: 'Team', title: 'Everyone in a team' },
              { value: 'user', label: 'User', title: 'One person' },
              {
                value: 'everyone',
                label: 'Everyone',
                title: 'Every identity the runner accepts. Useful as a broad allow with narrow denies on top.',
              },
            ]}
          />

          <div className="grid gap-2 sm:grid-cols-[minmax(0,1fr)_9rem_9rem]">
            <Field label={kind === 'user' ? 'User' : kind === 'team' ? 'Team' : 'Principal'}>
              {kind === 'everyone' ? (
                <Input value="everyone" disabled readOnly />
              ) : freeText ? (
                <Input
                  value={principal}
                  placeholder={kind === 'team' ? 'team id' : 'username'}
                  onChange={(event) => setPrincipal(event.target.value)}
                />
              ) : (
                <Select
                  value={principal}
                  options={options}
                  placeholder={kind === 'team' ? 'Pick a team' : 'Pick a user'}
                  onValueChange={setPrincipal}
                />
              )}
            </Field>
            <Field label="Level" help={LEVEL_HINT[resource][level]}>
              <Select
                value={level}
                options={LEVEL_OPTIONS}
                onValueChange={(value) => setLevel(value as AccessLevel)}
              />
            </Field>
            <Field label="Effect">
              <Select
                value={effect}
                options={[
                  { value: 'allow', label: 'Allow' },
                  { value: 'deny', label: 'Deny', hint: 'Beats every allow' },
                ]}
                onValueChange={(value) => setEffect(value as GrantEffect)}
              />
            </Field>
          </div>

          <div className="flex flex-wrap items-end gap-2">
            <Field
              className="min-w-0 flex-1"
              label="Why"
              help="Optional. The line whoever audits this in six months will read."
            >
              <Input
                value={note}
                maxLength={MAX_NOTE}
                placeholder="PII — analytics team only"
                onChange={(event) => setNote(event.target.value)}
              />
            </Field>
            <Button
              size="sm"
              loading={busy}
              disabled={kind !== 'everyone' && !principal.trim()}
              onClick={() => void add()}
            >
              Add rule
            </Button>
          </div>
        </div>
      ) : null}
    </section>
  )
}
