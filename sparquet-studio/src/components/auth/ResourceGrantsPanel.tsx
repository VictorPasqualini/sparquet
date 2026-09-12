/**
 * Access rules over Workflows, Jobs and Pipelines, in the screen where access lives.
 *
 * Datasets are edited in the catalog, on the dataset itself — that is where
 * somebody asks "who can read this table?". A Job or a Pipeline is asked about
 * from the other direction, usually as "what may this team run?", so the rules
 * for those are gathered here, next to the users and teams they name.
 *
 * The Workflow is in the picker because it is the container the other two
 * inherit from: one rule there governs everything inside it, which is the whole
 * reason a catalog-shaped model beats a rule per object.
 *
 * The same list, the same evaluation and the same enforcement as the catalog's:
 * one record in the workspace, re-read by the runner before it executes
 * anything. What this screen adds is the resource picker.
 */

import { Boxes, FolderTree, Tag, Workflow } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { toast } from 'sonner'

import { GrantsPanel, type NewGrant } from '@/components/catalog/GrantsPanel'
import { OwnerPicker, type NewOwner } from '@/components/catalog/OwnerPicker'
import { Badge, Field, Segmented, Select, Spinner, type SelectOption } from '@/components/ui'
import { grantsFor, tagScopes, type ResourceKind } from '@/lib/iam'
import { useAuthStore } from '@/store/auth'
import { useCatalogStore } from '@/store/catalog'
import { accessTo, effectiveOwnerOf, mayAdministerResource, useIamStore } from '@/store/iam'
import { useLibraryStore } from '@/store/library'
import type { AuthTeam, AuthUser } from '@/types/auth'

type Kind = Extract<ResourceKind, 'job' | 'pipeline' | 'workflow' | 'tag'>

const NOUN: Record<Kind, string> = {
  job: 'Job',
  pipeline: 'Pipeline',
  workflow: 'Workflow',
  tag: 'Tag',
}

const ICON: Record<Kind, typeof Boxes> = {
  job: Boxes,
  pipeline: Workflow,
  workflow: FolderTree,
  tag: Tag,
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

export function ResourceGrantsPanel() {
  const can = useAuthStore((state) => state.can)
  const principal = useAuthStore((state) => state.principal)
  const fetchTeams = useAuthStore((state) => state.fetchTeams)
  const fetchUsers = useAuthStore((state) => state.fetchUsers)

  const jobs = useLibraryStore((state) => state.jobs)
  const pipelines = useLibraryStore((state) => state.pipelines)
  const workflows = useLibraryStore((state) => state.workflows)

  const annotations = useCatalogStore((state) => state.annotations)
  const loadCatalog = useCatalogStore((state) => state.load)

  const grants = useIamStore((state) => state.grants)
  const owners = useIamStore((state) => state.owners)
  const loading = useIamStore((state) => state.loading)
  const load = useIamStore((state) => state.load)
  const addGrant = useIamStore((state) => state.grant)
  const revokeGrant = useIamStore((state) => state.revoke)
  const setOwner = useIamStore((state) => state.setOwner)
  const clearOwner = useIamStore((state) => state.clearOwner)

  const [kind, setKind] = useState<Kind>('job')
  const [selected, setSelected] = useState('')
  const [teams, setTeams] = useState<AuthTeam[]>([])
  const [users, setUsers] = useState<AuthUser[]>([])

  useEffect(() => {
    void load()
    // The tag list comes from the catalog: a tag exists because a dataset was
    // annotated with it, and this panel is often the first screen opened.
    void loadCatalog()
  }, [load, loadCatalog])

  useEffect(() => {
    if (!can('iam:ReadUsers')) return
    void (async () => {
      try {
        const [foundTeams, foundUsers] = await Promise.all([fetchTeams(), fetchUsers()])
        setTeams(foundTeams)
        setUsers(foundUsers)
      } catch {
        // A runner with no user records answers 403 or 404 here. The editor
        // still works: the principal is typed instead of picked.
      }
    })()
  }, [can, fetchTeams, fetchUsers])

  /**
   * Every tag anybody could write a rule on: the ones the catalog actually uses,
   * plus the ones a rule already names.
   *
   * The second half matters more than it looks. A tag is not a record — it
   * exists only while some dataset claims it — so a rule on `pii` would vanish
   * from this list the moment the last table lost the tag, taking with it the
   * only way to revoke the rule.
   */
  const tags = useMemo(() => {
    const found = new Set<string>()
    for (const annotation of Object.values(annotations)) {
      for (const [, id] of tagScopes(annotation.tags, {
        classification: annotation.classification,
        domain: annotation.domain,
      })) {
        found.add(id)
      }
    }
    for (const grant of grants) {
      if (grant.resource === 'tag') found.add(grant.resourceId)
    }
    return [...found].sort((left, right) => left.localeCompare(right))
  }, [annotations, grants])

  const options = useMemo<SelectOption[]>(() => {
    const source =
      kind === 'job'
        ? jobs.map((job) => ({ id: job.id, name: job.name }))
        : kind === 'pipeline'
          ? pipelines.map((pipeline) => ({ id: pipeline.id, name: pipeline.name }))
          : kind === 'tag'
            ? tags.map((tag) => ({ id: tag, name: tag }))
            : workflows.map((workflow) => ({ id: workflow.id, name: workflow.name }))
    const counted = source.map((item) => {
      const rules = grantsFor(grants, kind, item.id)
      const denies = rules.filter((rule) => rule.effect === 'deny').length
      const owned = owners.some((owner) => owner.resource === kind && owner.resourceId === item.id)
      const parts: string[] = []
      if (owned) parts.push('owned')
      if (rules.length > 0) {
        parts.push(`${rules.length - denies} allow${denies > 0 ? `, ${denies} deny` : ''}`)
      }
      return {
        value: item.id,
        label: item.name,
        hint: parts.length > 0 ? parts.join(' · ') : 'no rules',
      }
    })
    return counted.sort((left, right) => left.label.localeCompare(right.label))
  }, [grants, jobs, kind, owners, pipelines, tags, workflows])

  // Keep the picker on something that exists: switching kind, or deleting the
  // record, must not leave the panel editing rules for nothing.
  useEffect(() => {
    if (options.length === 0) {
      if (selected) setSelected('')
      return
    }
    if (!options.some((option) => option.value === selected)) setSelected(options[0].value)
  }, [options, selected])

  const scoped = useMemo(
    () => (selected ? grantsFor(grants, kind, selected) : []),
    [grants, kind, selected],
  )

  /*
   * Reading the rules needs nothing special — they are workspace data, and a
   * person who cannot see them cannot tell whether they are governed at all.
   * Changing them takes `iam:ManageGrants` over the runner OR ownership of this
   * particular resource, which is the point of having owners: the team that owns
   * a Workflow re-grants it without anybody handing them the whole platform.
   * `_owner_may_change_meta` in `server/main.py` checks exactly this again.
   *
   * The three memos below read the stores directly instead of subscribing, so
   * `grants`, `owners` and `principal` are in the deps to make them recompute.
   */
  const mayManage = useMemo(
    () => (selected ? mayAdministerResource(kind, selected) : can('iam:ManageGrants')),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [can, grants, kind, owners, principal, selected],
  )

  const ownership = useMemo(
    () => (selected ? effectiveOwnerOf(kind, selected) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [kind, owners, selected],
  )

  const ownRecord = useMemo(
    () => owners.find((owner) => owner.resource === kind && owner.resourceId === selected) ?? null,
    [kind, owners, selected],
  )

  /** What the person at the keyboard ends up with here, and where it came from. */
  const mine = useMemo(
    () => (selected ? accessTo(kind, selected) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [grants, kind, owners, principal, selected],
  )

  const onGrant = useCallback(
    async (input: NewGrant) => {
      if (!selected) return
      try {
        await addGrant({ ...input, resource: kind, resourceId: selected })
      } catch (error) {
        toast.error(messageOf(error))
      }
    },
    [addGrant, kind, selected],
  )

  const onRevoke = useCallback(
    async (id: string) => {
      try {
        await revokeGrant(id)
      } catch (error) {
        toast.error(messageOf(error))
      }
    },
    [revokeGrant],
  )

  const onAssignOwner = useCallback(
    async (input: NewOwner) => {
      if (!selected) return
      try {
        await setOwner({ ...input, resource: kind, resourceId: selected })
      } catch (error) {
        toast.error(messageOf(error))
      }
    },
    [kind, selected, setOwner],
  )

  const onClearOwner = useCallback(async () => {
    if (!selected) return
    try {
      await clearOwner(kind, selected)
    } catch (error) {
      toast.error(messageOf(error))
    }
  }, [clearOwner, kind, selected])

  const governed = useMemo(() => {
    const ids = new Set(
      grants.filter((grant) => grant.resource === kind).map((grant) => grant.resourceId),
    )
    for (const owner of owners) {
      if (owner.resource === kind) ids.add(owner.resourceId)
    }
    return ids.size
  }, [grants, kind, owners])

  const Icon = ICON[kind]
  // An inherited rule is the one thing a per-resource list cannot show: the
  // grant is written somewhere else, so the list here looks empty while the
  // answer is not. Say where it comes from instead of letting the page lie.
  const inherited = mine?.source && mine.source !== `${kind}/${selected}` ? mine.source : null

  return (
    <div className="space-y-4 border-t border-line pt-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-0.5">
          <p className="text-sm text-content">Who can run what</p>
          <p className="max-w-md text-2xs leading-relaxed text-content-subtle">
            Roles say whether somebody may run anything on this runner. These say which Jobs and
            Pipelines in particular — and which they may never touch, whatever else grants it. A
            rule on a Workflow reaches everything inside it. Datasets are governed the same way,
            from the catalog — or all at once, by the tag the catalog gives them.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {governed > 0 ? (
            <Badge tone="brand">
              {governed} {NOUN[kind]}
              {governed === 1 ? '' : 's'} restricted
            </Badge>
          ) : (
            <Badge tone="neutral">nothing restricted</Badge>
          )}
          {loading ? <Spinner className="h-3.5 w-3.5" /> : null}
        </div>
      </div>

      <div className="grid gap-2 sm:grid-cols-[16rem_minmax(0,1fr)]">
        <Field label="Kind">
          <Segmented
            size="sm"
            ariaLabel="Resource kind"
            value={kind}
            onChange={(next) => setKind(next)}
            options={[
              { value: 'job', label: 'Jobs', title: 'One compiled pipeline JSON' },
              {
                value: 'pipeline',
                label: 'Pipelines',
                title: 'An ordered sequence of Jobs',
              },
              {
                value: 'workflow',
                label: 'Workflows',
                title: 'The container everything inside it inherits from',
              },
              {
                value: 'tag',
                label: 'Tags',
                title: 'Every dataset the catalog gives this tag, including later ones',
              },
            ]}
          />
        </Field>
        <Field label={NOUN[kind]}>
          {options.length === 0 ? (
            <p className="flex items-center gap-1.5 py-1.5 text-xs italic text-content-subtle">
              <Icon className="h-3.5 w-3.5" />
              {kind === 'tag'
                ? 'No dataset in the catalog carries a tag yet.'
                : `The library has no ${NOUN[kind]} to restrict yet.`}
            </p>
          ) : (
            <Select
              value={selected}
              options={options}
              ariaLabel={`${NOUN[kind]} to restrict`}
              onValueChange={setSelected}
            />
          )}
        </Field>
      </div>

      {selected ? (
        <div className="space-y-3">
          {/* A tag has no owner on purpose: ownership is admin that no deny can
              reach, and handing that out over a label anybody may type onto a
              table would be handing out admin over tables never seen. */}
          {kind === 'tag' ? null : (
          <OwnerPicker
            resource={kind}
            owner={ownRecord}
            effective={ownership}
            teams={teams}
            users={users}
            onAssign={onAssignOwner}
            onClear={onClearOwner}
            editable={mayManage}
          />
          )}

          {inherited ? (
            <p className="rounded-lg border border-dashed border-line px-3 py-2 text-2xs leading-relaxed text-content-subtle">
              Your own access here — <strong className="text-content">{mine?.level}</strong> — comes
              from a rule on <code className="text-content-muted">{inherited}</code>, not from this{' '}
              {NOUN[kind]}. Change it there, or write a rule here to override it.
            </p>
          ) : null}

          <GrantsPanel
            resource={kind}
            teams={teams}
            users={users}
            grants={scoped}
            onGrant={onGrant}
            onRevoke={onRevoke}
            editable={mayManage}
          />
        </div>
      ) : null}
    </div>
  )
}
