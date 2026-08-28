/**
 * Roles, and the editor that writes custom ones.
 *
 * A role is a name for a list of statements, and a statement is the whole of the
 * policy language: an effect, a set of actions and a set of resources. The
 * vocabulary offered here is fetched from the runner rather than kept in a list
 * on this side, so an action added to `server/auth.py` shows up without a second
 * change and this editor can never offer one the server would reject.
 *
 * Built-in roles are shown but not editable. They are rewritten from code on
 * every start — that is how fixing a shipped policy fixes it everywhere — so an
 * edit made here would vanish at the next restart, and a control that silently
 * undoes itself is worse than no control.
 */

import { Pencil, Plus, ShieldCheck, Trash2 } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { toast } from 'sonner'

import { Badge, Button, Field, Input, Modal, Select, Spinner, useConfirm } from '@/components/ui'
import { useAuthStore } from '@/store/auth'
import type { AuthRole, PolicyAction, PolicyStatement, PolicyVocabulary } from '@/types/auth'

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** `run:Execute` and `iam:ReadUsers` grouped under `run` and `iam`. */
function byService(actions: PolicyAction[]): [string, PolicyAction[]][] {
  const groups = new Map<string, PolicyAction[]>()
  for (const action of actions) {
    const list = groups.get(action.service) ?? []
    list.push(action)
    groups.set(action.service, list)
  }
  return [...groups.entries()].sort(([a], [b]) => a.localeCompare(b))
}

/** How many things a role lets through, for the one-line summary in the list. */
function summarize(statements: PolicyStatement[]): string {
  const allows = statements.filter((statement) => statement.effect !== 'deny')
  const denies = statements.length - allows.length
  const actions = new Set(allows.flatMap((statement) => statement.actions ?? []))
  const head = actions.has('*')
    ? 'everything'
    : `${actions.size} ${actions.size === 1 ? 'action' : 'actions'}`
  return denies > 0 ? `${head}, ${denies} denied` : head
}

export function RolesPanel() {
  const can = useAuthStore((state) => state.can)
  const fetchRoles = useAuthStore((state) => state.fetchRoles)
  const fetchPolicy = useAuthStore((state) => state.fetchPolicy)
  const addRole = useAuthStore((state) => state.addRole)
  const editRole = useAuthStore((state) => state.editRole)
  const removeRole = useAuthStore((state) => state.removeRole)
  const [confirm, confirmDialog] = useConfirm()

  const mayRead = can('iam:ReadUsers')
  const mayManage = can('iam:ManageRoles')

  const [roles, setRoles] = useState<AuthRole[] | null>(null)
  const [vocabulary, setVocabulary] = useState<PolicyVocabulary | null>(null)
  const [loading, setLoading] = useState(false)
  const [editing, setEditing] = useState<AuthRole | null>(null)
  const [creating, setCreating] = useState(false)

  const reload = useCallback(async () => {
    if (!mayRead) return
    setLoading(true)
    try {
      const [available, policy] = await Promise.all([fetchRoles(), fetchPolicy()])
      setRoles(available)
      setVocabulary(policy)
    } catch (error) {
      toast.error(messageOf(error))
    } finally {
      setLoading(false)
    }
  }, [fetchPolicy, fetchRoles, mayRead])

  useEffect(() => {
    void reload()
  }, [reload])

  if (!mayRead) return null

  return (
    <>
      <div className="flex items-center justify-between gap-4 border-t border-line pt-4">
        <div className="space-y-0.5">
          <p className="text-sm text-content">Roles</p>
          <p className="max-w-md text-2xs leading-relaxed text-content-subtle">
            What a role permits, written as statements over actions and resources. The shipped
            roles cannot be edited; write your own next to them.
          </p>
        </div>
        {mayManage ? (
          <Button
            size="sm"
            variant="secondary"
            icon={<Plus className="h-3.5 w-3.5" />}
            onClick={() => setCreating(true)}
          >
            New role
          </Button>
        ) : null}
      </div>

      <div className="rounded-lg border border-line">
        {loading && roles === null ? (
          <div className="flex items-center justify-center py-6">
            <Spinner className="h-4 w-4" />
          </div>
        ) : roles && roles.length > 0 ? (
          <ul className="divide-y divide-line">
            {roles.map((role) => (
              <li key={role.name} className="flex items-center gap-3 px-3 py-2.5">
                <div className="min-w-0 flex-1">
                  <p className="flex items-center gap-1.5 truncate text-sm text-content">
                    {role.name}
                    {role.custom ? <Badge tone="neutral">custom</Badge> : null}
                  </p>
                  <p className="truncate text-2xs text-content-subtle">
                    {role.description || summarize(role.statements)}
                    <span className="ml-1.5">· {summarize(role.statements)}</span>
                  </p>
                </div>
                <Button
                  size="sm"
                  variant="ghost"
                  icon={<Pencil className="h-3.5 w-3.5" />}
                  onClick={() => setEditing(role)}
                >
                  {mayManage && role.custom ? 'Edit' : 'View'}
                </Button>
                {mayManage && role.custom ? (
                  <Button
                    size="sm"
                    variant="ghost"
                    icon={<Trash2 className="h-3.5 w-3.5" />}
                    onClick={() => {
                      void (async () => {
                        const ok = await confirm({
                          title: `Delete the role ${role.name}?`,
                          message:
                            'It is refused while anyone still holds it — move those people to another role first.',
                          confirmLabel: 'Delete',
                          variant: 'danger',
                        })
                        if (!ok) return
                        try {
                          await removeRole(role.name)
                          toast.success(`Deleted ${role.name}`)
                          await reload()
                        } catch (error) {
                          toast.error(messageOf(error))
                        }
                      })()
                    }}
                  >
                    Delete
                  </Button>
                ) : null}
              </li>
            ))}
          </ul>
        ) : (
          <p className="px-3 py-4 text-2xs text-content-subtle">No roles.</p>
        )}
      </div>

      <RoleDialog
        open={creating || editing !== null}
        role={editing}
        vocabulary={vocabulary}
        readOnly={!mayManage || (editing !== null && !editing.custom)}
        onOpenChange={(open) => {
          if (!open) {
            setCreating(false)
            setEditing(null)
          }
        }}
        onSubmit={async (body) => {
          if (editing) await editRole(editing.name, body)
          else await addRole(body)
          await reload()
        }}
      />

      {confirmDialog}
    </>
  )
}

const EMPTY_STATEMENT: PolicyStatement = { effect: 'allow', actions: [], resources: ['*'] }

function RoleDialog({
  open,
  role,
  vocabulary,
  readOnly,
  onOpenChange,
  onSubmit,
}: {
  open: boolean
  role: AuthRole | null
  vocabulary: PolicyVocabulary | null
  readOnly: boolean
  onOpenChange: (open: boolean) => void
  onSubmit: (body: {
    name: string
    description: string
    statements: PolicyStatement[]
  }) => Promise<void>
}) {
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [statements, setStatements] = useState<PolicyStatement[]>([EMPTY_STATEMENT])
  const [busy, setBusy] = useState(false)

  // Reopening for a different role has to reload the form; the dialog itself is
  // never unmounted between two roles.
  useEffect(() => {
    if (!open) return
    setName(role?.name ?? '')
    setDescription(role?.description ?? '')
    setStatements(
      role && role.statements.length > 0 ? role.statements : [{ ...EMPTY_STATEMENT }],
    )
  }, [open, role])

  const groups = useMemo(() => byService(vocabulary?.actions ?? []), [vocabulary])

  const valid =
    name.trim().length > 0 &&
    statements.some((statement) => (statement.actions ?? []).length > 0)

  const patch = (index: number, changes: Partial<PolicyStatement>) => {
    setStatements((current) =>
      current.map((statement, position) =>
        position === index ? { ...statement, ...changes } : statement,
      ),
    )
  }

  const submit = () => {
    setBusy(true)
    void onSubmit({
      name: name.trim(),
      description: description.trim(),
      statements: statements.filter((statement) => (statement.actions ?? []).length > 0),
    })
      .then(() => {
        toast.success(role ? `Updated ${name.trim()}` : `Created ${name.trim()}`)
        onOpenChange(false)
      })
      .catch((error: unknown) => toast.error(messageOf(error)))
      .finally(() => setBusy(false))
  }

  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      title={role ? `Role ${role.name}` : 'New role'}
      description="An explicit deny always wins over any allow, and anything not allowed is refused. Resources are matched with `*` wildcards, as `kind/id` — `job/*`, `workflow/etl`, or `*` for all of them."
      size="lg"
      footer={
        readOnly ? (
          <Button size="sm" variant="secondary" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        ) : (
          <Button
            size="sm"
            icon={<ShieldCheck className="h-3.5 w-3.5" />}
            loading={busy}
            disabled={!valid}
            onClick={submit}
          >
            Save
          </Button>
        )
      }
    >
      <div className="space-y-4">
        <div className="grid gap-3 sm:grid-cols-2">
          <Field label="Name" help="Lower-case, no spaces. It cannot be changed later.">
            <Input
              value={name}
              disabled={readOnly || role !== null}
              onChange={(event) => setName(event.target.value)}
            />
          </Field>
          <Field label="Description" help="Shown wherever the role is offered.">
            <Input
              value={description}
              disabled={readOnly}
              onChange={(event) => setDescription(event.target.value)}
            />
          </Field>
        </div>

        {statements.map((statement, index) => (
          <div key={index} className="space-y-3 rounded-lg border border-line px-3 py-3">
            <div className="flex items-center gap-3">
              <Select
                ariaLabel="Effect"
                className="w-28"
                disabled={readOnly}
                value={statement.effect === 'deny' ? 'deny' : 'allow'}
                options={[
                  { value: 'allow', label: 'Allow' },
                  { value: 'deny', label: 'Deny' },
                ]}
                onValueChange={(value) =>
                  patch(index, { effect: value === 'deny' ? 'deny' : 'allow' })
                }
              />
              <Input
                aria-label="Resources"
                className="flex-1"
                placeholder="*"
                disabled={readOnly}
                value={(statement.resources ?? ['*']).join(', ')}
                onChange={(event) =>
                  patch(index, {
                    resources: event.target.value
                      .split(',')
                      .map((part) => part.trim())
                      .filter((part) => part.length > 0),
                  })
                }
              />
              {!readOnly && statements.length > 1 ? (
                <Button
                  size="sm"
                  variant="ghost"
                  icon={<Trash2 className="h-3.5 w-3.5" />}
                  onClick={() =>
                    setStatements((current) =>
                      current.filter((_, position) => position !== index),
                    )
                  }
                >
                  Remove
                </Button>
              ) : null}
            </div>

            <div className="space-y-2">
              {groups.map(([service, actions]) => (
                <div key={service}>
                  <p className="text-2xs uppercase tracking-wide text-content-subtle">{service}</p>
                  <div className="mt-1 flex flex-wrap gap-1.5">
                    {actions.map((action) => {
                      const held = (statement.actions ?? []).includes(action.name)
                      return (
                        <button
                          key={action.name}
                          type="button"
                          disabled={readOnly}
                          title={action.description}
                          className={[
                            'rounded-md border px-2 py-1 text-2xs transition-colors',
                            held
                              ? 'border-brand-500 bg-brand-500/10 text-content'
                              : 'border-line text-content-subtle hover:text-content',
                            readOnly ? 'cursor-default opacity-70' : '',
                          ].join(' ')}
                          onClick={() => {
                            const current = statement.actions ?? []
                            patch(index, {
                              actions: held
                                ? current.filter((item) => item !== action.name)
                                : [...current, action.name],
                            })
                          }}
                        >
                          {action.name}
                        </button>
                      )
                    })}
                  </div>
                </div>
              ))}
              {!readOnly ? (
                <button
                  type="button"
                  className={[
                    'rounded-md border px-2 py-1 text-2xs transition-colors',
                    (statement.actions ?? []).includes('*')
                      ? 'border-brand-500 bg-brand-500/10 text-content'
                      : 'border-line text-content-subtle hover:text-content',
                  ].join(' ')}
                  onClick={() => {
                    const current = statement.actions ?? []
                    patch(index, {
                      actions: current.includes('*')
                        ? current.filter((item) => item !== '*')
                        : [...current, '*'],
                    })
                  }}
                >
                  * — every action, including ones added later
                </button>
              ) : null}
            </div>
          </div>
        ))}

        {!readOnly ? (
          <Button
            size="sm"
            variant="ghost"
            icon={<Plus className="h-3.5 w-3.5" />}
            onClick={() => setStatements((current) => [...current, { ...EMPTY_STATEMENT }])}
          >
            Add statement
          </Button>
        ) : null}
      </div>
    </Modal>
  )
}
