/**
 * The `resources` half of a policy statement, picked rather than typed.
 *
 * A statement names its resources as `kind/id` — `job/j1`, `workflow/*`, `*` —
 * and the ids are record ids, not names. Typed by hand that is a silent failure
 * waiting to happen: a role that names `job/etl-diario` looks right in the
 * editor and matches nothing at all, because the record's id is a uuid. So the
 * ids come from the library and the catalog, and anything already stored that
 * no longer resolves is flagged rather than quietly kept.
 *
 * Wildcards stay first-class: `*` and `kind/*` are the common cases, and a role
 * scoped to a whole kind is supposed to cover records created after it.
 */

import { Plus, X } from 'lucide-react'
import { useMemo, useState } from 'react'

import { Badge, Button, Input, Select } from '@/components/ui'
import { useCatalogStore } from '@/store/catalog'
import { useLibraryStore } from '@/store/library'

/** The kinds a statement may scope to, plus the "everything" row that has no id. */
const KINDS = ['*', 'workflow', 'pipeline', 'job', 'dataset', 'team', 'user'] as const

type Kind = (typeof KINDS)[number]

export interface ResourceChoice {
  id: string
  label: string
}

/**
 * Every id a policy or a simulation may name, by kind, with the name a person
 * would recognize it by.
 *
 * Shared with the access simulator: both ask "which Job?" and both have to mean
 * the ids the runner stores, not the names shown beside them.
 */
export function useResourceChoices(): Record<string, ResourceChoice[]> {
  const workflows = useLibraryStore((state) => state.workflows)
  const jobs = useLibraryStore((state) => state.jobs)
  const pipelines = useLibraryStore((state) => state.pipelines)
  const annotations = useCatalogStore((state) => state.annotations)

  return useMemo(() => {
    const named = (records: { id: string; name?: string }[]) =>
      records.map((record) => ({ id: record.id, label: record.name || record.id }))
    return {
      workflow: named(workflows),
      pipeline: named(pipelines),
      job: named(jobs),
      dataset: Object.keys(annotations).map((key) => ({ id: key, label: key })),
      team: [],
      user: [],
    }
  }, [annotations, jobs, pipelines, workflows])
}

/** `job/j1` split into its two halves; `*` has no id and no kind to check. */
function split(resource: string): { kind: string; id: string } {
  const at = resource.indexOf('/')
  if (at < 0) return { kind: resource, id: '' }
  return { kind: resource.slice(0, at), id: resource.slice(at + 1) }
}

export function PolicyResourcePicker({
  value,
  disabled = false,
  onChange,
}: {
  value: string[]
  disabled?: boolean
  onChange: (next: string[]) => void
}) {
  const [kind, setKind] = useState<Kind>('job')
  const [id, setId] = useState('*')
  const choices = useResourceChoices()

  /** The name behind an id, so a chip reads as something a person recognizes. */
  const nameOf = (resource: string): string | null => {
    const { kind: part, id: ident } = split(resource)
    if (!ident || ident === '*') return null
    const found = (choices[part] ?? []).find((choice) => choice.id === ident)
    return found ? found.label : null
  }

  /**
   * Whether an id still resolves. Only checked for the kinds this Studio has
   * records of: a `team/` or `user/` id lives in the runner's own database, and
   * a dataset is an address that exists because a Job mentions it — flagging
   * either would be crying wolf.
   */
  const unresolved = (resource: string): boolean => {
    const { kind: part, id: ident } = split(resource)
    if (!ident || ident === '*') return false
    if (part !== 'workflow' && part !== 'pipeline' && part !== 'job') return false
    return !(choices[part] ?? []).some((choice) => choice.id === ident)
  }

  const options = choices[kind] ?? []
  const wildcardOnly = kind === '*' || kind === 'team' || kind === 'user'
  const freeText = kind === 'dataset'

  const add = () => {
    const wanted = kind === '*' ? '*' : `${kind}/${(id || '*').trim() || '*'}`
    if (value.includes(wanted)) return
    onChange([...value, wanted])
    setId('*')
  }

  const remove = (resource: string) => {
    onChange(value.filter((item) => item !== resource))
  }

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-1.5">
        {value.length === 0 ? (
          // An empty list is `*` on the server. Saying so beats an empty row that
          // reads as "nothing", which is the opposite of what it does.
          <span className="text-2xs text-content-subtle">
            Nothing listed — the statement covers every resource.
          </span>
        ) : null}
        {value.map((resource) => {
          const name = nameOf(resource)
          const missing = unresolved(resource)
          return (
            <span
              key={resource}
              className={[
                'inline-flex items-center gap-1 rounded-md border px-1.5 py-0.5 text-2xs',
                missing ? 'border-amber-500/40 text-amber-300' : 'border-line text-content',
              ].join(' ')}
              title={
                missing
                  ? 'No record with this id in this workspace. It may have been deleted, or the id may be a name rather than an id.'
                  : name
                    ? `${resource} — ${name}`
                    : resource
              }
            >
              <span className="font-mono">{resource}</span>
              {name ? <span className="text-content-subtle">{name}</span> : null}
              {missing ? <Badge tone="warning">unknown</Badge> : null}
              {!disabled ? (
                <button
                  type="button"
                  aria-label={`Remove ${resource}`}
                  className="text-content-subtle transition-colors hover:text-content"
                  onClick={() => remove(resource)}
                >
                  <X className="h-3 w-3" />
                </button>
              ) : null}
            </span>
          )
        })}
      </div>

      {!disabled ? (
        <div className="flex flex-wrap items-center gap-2">
          <Select
            ariaLabel="Resource kind"
            className="w-32"
            value={kind}
            options={KINDS.map((name) => ({
              value: name,
              label: name === '*' ? '* everything' : name,
            }))}
            onValueChange={(next) => {
              setKind(next as Kind)
              setId('*')
            }}
          />
          {wildcardOnly ? null : freeText ? (
            <Input
              aria-label="Dataset address"
              className="w-64"
              placeholder="/lake/silver/orders or *"
              list="policy-resource-datasets"
              value={id}
              onChange={(event) => setId(event.target.value)}
            />
          ) : (
            <Select
              ariaLabel="Resource"
              className="w-64"
              value={id}
              options={[
                { value: '*', label: `* every ${kind}, including later ones` },
                ...options.map((choice) => ({ value: choice.id, label: choice.label })),
              ]}
              onValueChange={setId}
            />
          )}
          <datalist id="policy-resource-datasets">
            {(choices.dataset ?? []).map((choice) => (
              <option key={choice.id} value={choice.id} />
            ))}
          </datalist>
          <Button
            size="sm"
            variant="ghost"
            icon={<Plus className="h-3.5 w-3.5" />}
            onClick={add}
          >
            Add
          </Button>
        </div>
      ) : null}
    </div>
  )
}
