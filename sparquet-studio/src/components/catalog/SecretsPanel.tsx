/**
 * Connection secrets, in the catalog, beside the datasets they open.
 *
 * The screen is deliberately poor in one direction: it can create a secret,
 * rotate a field, retag it and delete it, and it can never show what is inside
 * one. There is no reveal button because there is no endpoint behind it — the
 * runner hands a value to Spark and to nothing else, so a screen that offered to
 * display one would be lying about the model rather than implementing it.
 *
 * What a form takes away instead is the reference: `{secret:pg-prod/password}`,
 * copied into the password box of a JDBC reader. The framework never sees that
 * syntax — `{param}` matches `\w+`, which contains neither `:` nor `/` — so the
 * reference is resolved by the runner on the way to Spark and by nobody else.
 */

import { Check, Copy, KeyRound, Plus, RefreshCw, Trash2, X } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
import { toast } from 'sonner'

import {
  Badge,
  Button,
  EmptyState,
  Field,
  IconButton,
  Input,
  Select,
  Spinner,
  Textarea,
  useConfirm,
} from '@/components/ui'
import { useAuthStore } from '@/store/auth'
import { useSecretsStore } from '@/store/secrets'
import { secretRef, type Secret, type SecretProvider } from '@/types/secrets'

const PROVIDERS: { value: SecretProvider; label: string; hint: string }[] = [
  {
    value: 'local',
    label: 'Encrypted on this runner',
    hint: 'Sealed with SPARQUET_STUDIO_SECRET_KEY and kept in the workspace. What a laptop uses.',
  },
  {
    value: 'env',
    label: 'From an environment variable',
    hint: 'Each field names a variable the runner reads. How a cloud secret manager reaches a process: the platform injects it, Studio only says which name to look under.',
  },
]

/** A field row in the editor: the name, and the value being set right now. */
interface Draft {
  name: string
  value: string
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function whenText(seconds: number): string {
  if (!seconds) return 'never'
  return new Date(seconds * 1000).toLocaleString()
}

function toTags(text: string): string[] {
  return text
    .split(',')
    .map((tag) => tag.trim().toLowerCase())
    .filter(Boolean)
}

/** Name, provider, description, tags and the fields being written. */
function SecretEditor({
  secret,
  onClose,
}: {
  /** The secret being changed, or `undefined` when creating one. */
  secret?: Secret
  onClose: () => void
}) {
  const save = useSecretsStore((state) => state.save)
  const [name, setName] = useState(secret?.name ?? '')
  const [provider, setProvider] = useState<SecretProvider>(secret?.provider ?? 'local')
  const [description, setDescription] = useState(secret?.description ?? '')
  const [tags, setTags] = useState((secret?.tags ?? []).join(', '))
  const [drafts, setDrafts] = useState<Draft[]>(
    secret ? [{ name: '', value: '' }] : [{ name: 'url', value: '' }],
  )
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const editing = secret !== undefined
  const envMode = provider === 'env'

  const submit = async () => {
    const values: Record<string, string | null> = {}
    for (const draft of drafts) {
      const key = draft.name.trim()
      if (!key) continue
      if (!draft.value.trim()) {
        setError(`${key} has no value. Leave the row out to keep the field as it is.`)
        return
      }
      values[key] = draft.value
    }
    if (!editing && Object.keys(values).length === 0) {
      setError('A secret needs at least one field — a url, a user, a password.')
      return
    }
    setBusy(true)
    setError(null)
    try {
      await save(name.trim().toLowerCase(), {
        provider,
        description,
        tags: toTags(tags),
        values,
      })
      toast.success(editing ? `${name} updated` : `${name} created`)
      onClose()
    } catch (caught) {
      setError(messageOf(caught))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="card space-y-3 p-3">
      <div className="grid gap-3 sm:grid-cols-2">
        <Field
          label="Name"
          help="Lower case, and what a reference uses: {secret:this-name/field}."
        >
          <Input
            value={name}
            disabled={editing}
            placeholder="pg-prod"
            onChange={(event) => setName(event.target.value)}
          />
        </Field>
        <Field label="Where the material is" help={PROVIDERS.find((p) => p.value === provider)?.hint}>
          <Select
            value={provider}
            ariaLabel="Secret provider"
            options={PROVIDERS.map((item) => ({ value: item.value, label: item.label }))}
            onValueChange={(next) => setProvider(next as SecretProvider)}
          />
        </Field>
      </div>

      <Field label="What it opens" help="Shown in the list. Never put the credential here.">
        <Textarea
          rows={2}
          value={description}
          placeholder="The production orders database, read-only user."
          onChange={(event) => setDescription(event.target.value)}
        />
      </Field>

      <Field
        label="Tags"
        help="The catalog's own vocabulary, comma separated. A deny on tag/pii closes this credential the same way it closes a table."
      >
        <Input value={tags} placeholder="pii, finance" onChange={(event) => setTags(event.target.value)} />
      </Field>

      <div className="space-y-2">
        <p className="text-[11px] text-content-subtle">
          {envMode
            ? 'Each field names an environment variable the runner reads. The value never passes through this browser.'
            : editing
              ? 'Only the fields written here change. Everything else keeps what it had — which is what rotating one password means.'
              : 'The fields this connection needs. Sent once, and never readable again.'}
        </p>
        {drafts.map((draft, index) => (
          <div key={index} className="flex items-center gap-2">
            <Input
              className="w-40"
              aria-label="Field name"
              placeholder="password"
              value={draft.name}
              onChange={(event) =>
                setDrafts((rows) =>
                  rows.map((row, at) => (at === index ? { ...row, name: event.target.value } : row)),
                )
              }
            />
            <Input
              className="flex-1"
              aria-label={envMode ? 'Environment variable' : 'Value'}
              type={envMode ? 'text' : 'password'}
              autoComplete="new-password"
              placeholder={envMode ? 'PGPASSWORD' : '••••••••'}
              value={draft.value}
              onChange={(event) =>
                setDrafts((rows) =>
                  rows.map((row, at) => (at === index ? { ...row, value: event.target.value } : row)),
                )
              }
            />
            <IconButton
              size="sm"
              label="Remove field"
              onClick={() => setDrafts((rows) => rows.filter((_, at) => at !== index))}
            >
              <X />
            </IconButton>
          </div>
        ))}
        <Button
          size="sm"
          variant="secondary"
          onClick={() => setDrafts((rows) => [...rows, { name: '', value: '' }])}
        >
          <Plus className="h-3.5 w-3.5" />
          Add field
        </Button>
      </div>

      {error ? <p className="text-xs text-state-danger">{error}</p> : null}

      <div className="flex items-center gap-2">
        <Button size="sm" disabled={busy || !name.trim()} onClick={() => void submit()}>
          {busy ? <Spinner className="h-3.5 w-3.5" /> : null}
          {editing ? 'Save' : 'Create secret'}
        </Button>
        <Button size="sm" variant="ghost" onClick={onClose}>
          Cancel
        </Button>
      </div>
    </div>
  )
}

/** One row: what it is, who may use it, and the references it hands out. */
function SecretRow({ secret, editable }: { secret: Secret; editable: boolean }) {
  const check = useSecretsStore((state) => state.check)
  const remove = useSecretsStore((state) => state.remove)
  const report = useSecretsStore((state) => state.checks[secret.name])
  const [editing, setEditing] = useState(false)
  const [busy, setBusy] = useState(false)
  const [confirm, confirmDialog] = useConfirm()

  const mayWrite = editable && (!secret.governed || secret.level === 'write' || secret.level === 'admin' || secret.owned)

  const copy = async (field: string) => {
    const reference = secretRef(secret.name, field)
    try {
      await navigator.clipboard.writeText(reference)
      toast.success(`${reference} copied`)
    } catch {
      toast.error('The browser refused the clipboard. Type the reference by hand.')
    }
  }

  const runCheck = async () => {
    setBusy(true)
    try {
      const result = await check(secret.name)
      toast[result.healthy ? 'success' : 'error'](
        result.healthy
          ? `${secret.name}: every field resolves`
          : `${secret.name}: ${Object.entries(result.fields)
              .filter(([, state]) => state !== 'ok')
              .map(([field]) => field)
              .join(', ')} did not resolve`,
      )
    } catch (error) {
      toast.error(messageOf(error))
    } finally {
      setBusy(false)
    }
  }

  const drop = async () => {
    const ok = await confirm({
      title: `Delete ${secret.name}?`,
      message:
        'Every Job, query and schema read that references it stops working at once, and the material cannot be recovered from here.',
      confirmLabel: 'Delete',
      confirmName: secret.name,
      variant: 'danger',
    })
    if (!ok) return
    try {
      await remove(secret.name)
      toast.success(`${secret.name} deleted`)
    } catch (error) {
      toast.error(messageOf(error))
    }
  }

  return (
    <div className="card space-y-2 p-3">
      {confirmDialog}
      <div className="flex flex-wrap items-center gap-2">
        <KeyRound className="h-3.5 w-3.5 text-content-subtle" />
        <span className="font-mono text-sm text-content">{secret.name}</span>
        <Badge tone={secret.provider === 'env' ? 'info' : 'brand'}>
          {secret.provider === 'env' ? 'environment' : 'encrypted here'}
        </Badge>
        {secret.owned ? <Badge tone="success">owned</Badge> : null}
        {secret.governed && secret.level ? <Badge tone="neutral">{secret.level}</Badge> : null}
        {!secret.governed ? (
          <span title="No rule names it, so anyone who may run may use it.">
            <Badge tone="warning">ungoverned</Badge>
          </span>
        ) : null}
        <span className="ml-auto flex items-center gap-1">
          <Button size="sm" variant="ghost" disabled={busy} onClick={() => void runCheck()}>
            {busy ? <Spinner className="h-3.5 w-3.5" /> : <Check className="h-3.5 w-3.5" />}
            Check
          </Button>
          {mayWrite ? (
            <>
              <Button size="sm" variant="ghost" onClick={() => setEditing((open) => !open)}>
                <RefreshCw className="h-3.5 w-3.5" />
                Rotate
              </Button>
              <IconButton size="sm" label={`Delete ${secret.name}`} onClick={() => void drop()}>
                <Trash2 />
              </IconButton>
            </>
          ) : null}
        </span>
      </div>

      {secret.description ? (
        <p className="text-xs text-content-muted">{secret.description}</p>
      ) : null}

      <div className="flex flex-wrap items-center gap-1.5">
        {secret.fields.map((fieldName) => {
          const state = report?.fields[fieldName]
          return (
            <button
              key={fieldName}
              type="button"
              onClick={() => void copy(fieldName)}
              title={`Copy ${secretRef(secret.name, fieldName)}${
                state && state !== 'ok' ? ` — last check: ${state}` : ''
              }`}
              className={[
                'inline-flex items-center gap-1 rounded-md border px-1.5 py-0.5 font-mono text-2xs transition-colors',
                state && state !== 'ok'
                  ? 'border-state-danger/40 text-state-danger'
                  : 'border-line text-content hover:border-brand-500/40',
              ].join(' ')}
            >
              {fieldName}
              {secret.provider === 'env' && secret.binding[fieldName] ? (
                <span className="text-content-subtle">= ${secret.binding[fieldName]}</span>
              ) : null}
              <Copy className="h-2.5 w-2.5" />
            </button>
          )
        })}
        {secret.tags.map((tag) => (
          <Badge key={tag} tone="neutral">
            {tag}
          </Badge>
        ))}
      </div>

      <p className="text-2xs text-content-subtle">
        Last written {whenText(secret.updatedAt)}
        {secret.updatedBy ? ` by ${secret.updatedBy}` : ''}
      </p>

      {editing ? <SecretEditor secret={secret} onClose={() => setEditing(false)} /> : null}
    </div>
  )
}

export function SecretsPanel() {
  const can = useAuthStore((state) => state.can)
  const items = useSecretsStore((state) => state.items)
  const loading = useSecretsStore((state) => state.loading)
  const loaded = useSecretsStore((state) => state.loaded)
  const forbidden = useSecretsStore((state) => state.forbidden)
  const error = useSecretsStore((state) => state.error)
  const load = useSecretsStore((state) => state.load)
  const [creating, setCreating] = useState(false)
  const [query, setQuery] = useState('')

  useEffect(() => {
    void load()
  }, [load])

  const mayWrite = can('secrets:Write')
  const visible = useMemo(() => {
    const needle = query.trim().toLowerCase()
    if (!needle) return items
    return items.filter(
      (item) =>
        item.name.includes(needle) ||
        item.description.toLowerCase().includes(needle) ||
        item.tags.some((tag) => tag.includes(needle)),
    )
  }, [items, query])

  if (forbidden) {
    return (
      <div className="card">
        <EmptyState
          icon={<KeyRound />}
          title="Not yours to see"
          description="Reading the connection secrets needs `secrets:Read`. Ask whoever administers this runner."
        />
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <p className="text-xs text-content-muted">
          A credential lives on the runner and is used by writing{' '}
          <code className="font-mono text-content">{'{secret:name/field}'}</code> into a
          connection field. Nothing here can show you a value — not this screen, and not the
          runner it asks.
        </p>
        <div className="ml-auto flex items-center gap-2">
          <Input
            className="h-9 w-56 py-0 text-xs"
            value={query}
            placeholder="Search name, tag or description"
            aria-label="Search secrets"
            onChange={(event) => setQuery(event.target.value)}
          />
          {mayWrite ? (
            <Button size="sm" onClick={() => setCreating((open) => !open)}>
              <Plus className="h-3.5 w-3.5" />
              New secret
            </Button>
          ) : null}
        </div>
      </div>

      {creating ? <SecretEditor onClose={() => setCreating(false)} /> : null}

      {error ? <p className="text-xs text-state-danger">{error}</p> : null}

      {loading && !loaded ? (
        <div className="flex items-center gap-2 text-xs text-content-subtle">
          <Spinner className="h-3.5 w-3.5" />
          Asking the runner what it holds…
        </div>
      ) : visible.length === 0 ? (
        <div className="card">
          <EmptyState
            icon={<KeyRound />}
            title={items.length === 0 ? 'No connection secrets yet' : 'No secret matches'}
            description={
              items.length === 0
                ? 'Create one here, then reference it from a JDBC reader instead of typing the password into the canvas.'
                : 'Try a shorter search.'
            }
          />
        </div>
      ) : (
        <div className="space-y-2">
          {visible.map((secret) => (
            <SecretRow key={secret.name} secret={secret} editable={mayWrite} />
          ))}
        </div>
      )}
    </div>
  )
}
