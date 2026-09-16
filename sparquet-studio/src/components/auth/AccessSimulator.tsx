/**
 * "What would this person be allowed to do?", answered by the runner.
 *
 * The two layers refuse for unrelated reasons — a missing action is a role to
 * fix, a missing level is an owner to ask — and an administrator staring at a
 * user's roles cannot tell which of the two is about to bite. So both answers
 * come back side by side, each naming the rule that produced it.
 *
 * The evaluation deliberately happens on the runner. Studio can work the grants
 * out in the browser and does, to grey out controls; but a simulator answering
 * from the browser would be a second implementation of the rules, and a
 * simulator that can disagree with reality is worse than none.
 */

import { Play, ShieldAlert, ShieldCheck } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { toast } from 'sonner'

import { useResourceChoices } from '@/components/auth/PolicyResourcePicker'
import { Badge, Button, Field, Input, Select, Spinner } from '@/components/ui'
import { useAuthStore } from '@/store/auth'
import type { AccessSimulation, AuthUser, PolicyVocabulary } from '@/types/auth'

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** The securable kinds layer two knows. `tag` is answered through the datasets that carry it. */
const KINDS = ['', 'dataset', 'job', 'pipeline', 'workflow', 'secret', 'query'] as const

const LEVELS = ['', 'read', 'write', 'admin'] as const

export function AccessSimulator() {
  const can = useAuthStore((state) => state.can)
  const fetchUsers = useAuthStore((state) => state.fetchUsers)
  const fetchPolicy = useAuthStore((state) => state.fetchPolicy)
  const simulate = useAuthStore((state) => state.simulate)

  const mayRead = can('iam:ReadUsers')

  const [users, setUsers] = useState<AuthUser[]>([])
  const [vocabulary, setVocabulary] = useState<PolicyVocabulary | null>(null)
  const [username, setUsername] = useState('')
  const [action, setAction] = useState('run:Execute')
  const [kind, setKind] = useState<string>('')
  const [resourceId, setResourceId] = useState('')
  const [level, setLevel] = useState<string>('')
  const [result, setResult] = useState<AccessSimulation | null>(null)
  const [busy, setBusy] = useState(false)

  const choices = useResourceChoices()

  const load = useCallback(async () => {
    if (!mayRead) return
    try {
      const [people, policy] = await Promise.all([fetchUsers(), fetchPolicy()])
      setUsers(people)
      setVocabulary(policy)
      setUsername((current) => current || people[0]?.username || '')
    } catch (error) {
      toast.error(messageOf(error))
    }
  }, [fetchPolicy, fetchUsers, mayRead])

  useEffect(() => {
    void load()
  }, [load])

  const actions = useMemo(
    () => [
      { value: '', label: 'No action — only the resource' },
      ...(vocabulary?.actions ?? []).map((item) => ({ value: item.name, label: item.name })),
    ],
    [vocabulary],
  )

  // Changing the kind invalidates the id: a Job id is not a dataset address.
  const options = choices[kind] ?? []

  const run = () => {
    if (!username) return
    setBusy(true)
    void simulate({
      username,
      action: action || null,
      resource: kind,
      resourceId: kind ? resourceId.trim() : '',
      level: level ? (level as 'read' | 'write' | 'admin') : null,
    })
      .then(setResult)
      .catch((error: unknown) => toast.error(messageOf(error)))
      .finally(() => setBusy(false))
  }

  if (!mayRead) return null

  return (
    <>
      <div className="space-y-0.5">
        <p className="text-sm text-content">Access simulator</p>
        <p className="max-w-md text-2xs leading-relaxed text-content-subtle">
          What somebody else would be allowed to do right now, answered by the runner itself —
          the roles, the grants, and which of the two decides.
        </p>
      </div>

      <div className="space-y-3 rounded-lg border border-line px-3 py-3">
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Field label="Person" help="Evaluated as if they had just signed in.">
            <Select
              ariaLabel="Person"
              value={username}
              options={
                users.length > 0
                  ? users.map((user) => ({
                      value: user.username,
                      label: user.displayName
                        ? `${user.username} — ${user.displayName}`
                        : user.username,
                    }))
                  : [{ value: '', label: 'No users on this runner' }]
              }
              onValueChange={setUsername}
            />
          </Field>

          <Field label="Action" help="Layer one: what the roles permit.">
            <Select
              ariaLabel="Action"
              value={action}
              options={actions}
              onValueChange={setAction}
            />
          </Field>

          <Field label="Resource" help="Layer two: the grants over one securable.">
            <Select
              ariaLabel="Resource kind"
              value={kind}
              options={KINDS.map((name) => ({
                value: name,
                label: name || 'None — only the action',
              }))}
              onValueChange={(next) => {
                setKind(next)
                setResourceId('')
              }}
            />
          </Field>

          <Field
            label="Which one"
            help={
              kind === 'dataset'
                ? 'The address, as a Job names it.'
                : kind
                  ? 'The record, by name.'
                  : 'Pick a resource kind first.'
            }
          >
            {kind === 'dataset' || !kind ? (
              <Input
                aria-label="Resource id"
                disabled={!kind}
                placeholder="/lake/silver/orders"
                list="simulator-datasets"
                value={resourceId}
                onChange={(event) => setResourceId(event.target.value)}
              />
            ) : (
              <Select
                ariaLabel="Resource id"
                value={resourceId}
                options={[
                  { value: '', label: `Pick a ${kind}` },
                  ...options.map((choice) => ({ value: choice.id, label: choice.label })),
                ]}
                onValueChange={setResourceId}
              />
            )}
          </Field>
        </div>

        <datalist id="simulator-datasets">
          {(choices.dataset ?? []).map((choice) => (
            <option key={choice.id} value={choice.id} />
          ))}
        </datalist>

        <div className="flex flex-wrap items-end gap-3">
          <Field
            label="Level"
            help="Left blank, the runner asks for whatever the action itself demands."
          >
            <Select
              ariaLabel="Level"
              className="w-40"
              value={level}
              options={LEVELS.map((name) => ({
                value: name,
                label: name || 'From the action',
              }))}
              onValueChange={setLevel}
            />
          </Field>
          <Button
            size="sm"
            icon={<Play className="h-3.5 w-3.5" />}
            loading={busy}
            disabled={!username || (Boolean(kind) && !resourceId.trim())}
            onClick={run}
          >
            Simulate
          </Button>
          {busy ? <Spinner className="h-4 w-4" /> : null}
        </div>

        {result ? <SimulationResult result={result} /> : null}
      </div>
    </>
  )
}

function SimulationResult({ result }: { result: AccessSimulation }) {
  const roles = result.roles.length > 0 ? result.roles.join(', ') : 'none'
  const personal = result.roles.filter((role) => !result.teamRoles.includes(role))

  return (
    <div className="space-y-3 rounded-lg border border-line bg-surface-raised px-3 py-3">
      <div className="flex items-start gap-2">
        {result.allowed ? (
          <ShieldCheck className="mt-0.5 h-4 w-4 shrink-0 text-emerald-400" />
        ) : (
          <ShieldAlert className="mt-0.5 h-4 w-4 shrink-0 text-amber-400" />
        )}
        <div className="min-w-0 space-y-1">
          <p className="text-sm text-content">
            {result.allowed ? 'Allowed' : 'Refused'}
          </p>
          <p className="text-2xs leading-relaxed text-content-subtle">{result.reason}</p>
        </div>
      </div>

      {result.found ? (
        <dl className="grid gap-2 text-2xs sm:grid-cols-2">
          <div>
            <dt className="text-content-subtle">Roles</dt>
            <dd className="text-content">
              {roles}
              {result.teamRoles.length > 0 ? (
                <span className="text-content-subtle">
                  {' '}
                  ({personal.length > 0 ? `${personal.join(', ')} personally, ` : ''}
                  {result.teamRoles.join(', ')} from {result.teamName ?? 'the team'})
                </span>
              ) : null}
            </dd>
          </div>

          {result.policy ? (
            <div>
              <dt className="text-content-subtle">Layer one — roles</dt>
              <dd className="flex flex-wrap items-center gap-1.5 text-content">
                <Badge tone={result.policy.allowed ? 'success' : 'danger'}>
                  {result.policy.allowed ? 'allows' : 'refuses'} {result.policy.action}
                </Badge>
                {result.policy.deniedOn ? (
                  <span className="text-content-subtle">
                    denied on <span className="font-mono">{result.policy.deniedOn}</span>
                  </span>
                ) : result.policy.allowedOn ? (
                  <span className="text-content-subtle">
                    via <span className="font-mono">{result.policy.allowedOn}</span>
                  </span>
                ) : (
                  <span className="text-content-subtle">
                    no statement matched{' '}
                    <span className="font-mono">{result.policy.targets.join(', ')}</span>
                  </span>
                )}
              </dd>
            </div>
          ) : null}

          {result.access ? (
            <div className="sm:col-span-2">
              <dt className="text-content-subtle">Layer two — grants</dt>
              <dd className="flex flex-wrap items-center gap-1.5 text-content">
                {!result.access.governed ? (
                  <Badge tone="neutral">ungoverned</Badge>
                ) : result.access.owned ? (
                  <Badge tone="success">owns it</Badge>
                ) : result.access.level ? (
                  <Badge tone="success">{result.access.level}</Badge>
                ) : (
                  <Badge tone="danger">no access</Badge>
                )}
                {result.levelAsked ? (
                  <span className="text-content-subtle">asked for {result.levelAsked}</span>
                ) : null}
                {result.access.source ? (
                  <span className="text-content-subtle">
                    from <span className="font-mono">{result.access.source}</span>
                  </span>
                ) : null}
              </dd>
              {result.access.chain.length > 0 ? (
                <dd className="mt-1 text-content-subtle">
                  {/* Where a rule could be written to change this answer — which is
                      the question an administrator asks the moment they see it. */}
                  Rules reaching it:{' '}
                  <span className="font-mono">{result.access.chain.join(' · ')}</span>
                </dd>
              ) : null}
            </div>
          ) : null}
        </dl>
      ) : null}
    </div>
  )
}
