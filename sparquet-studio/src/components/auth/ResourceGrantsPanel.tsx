/**
 * Access rules over Jobs and Pipelines, in the screen where access lives.
 *
 * Datasets are edited in the catalog, on the dataset itself — that is where
 * somebody asks "who can read this table?". A Job or a Pipeline is asked about
 * from the other direction, usually as "what may this team run?", so the rules
 * for those are gathered here, next to the users and teams they name.
 *
 * The same list, the same evaluation and the same enforcement as the catalog's:
 * one record in the workspace, re-read by the runner before it executes
 * anything. What this screen adds is the resource picker.
 */

import { Boxes, Workflow } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { toast } from 'sonner'

import { GrantsPanel, type NewGrant } from '@/components/catalog/GrantsPanel'
import { Badge, Field, Segmented, Select, Spinner, type SelectOption } from '@/components/ui'
import { grantsFor, type ResourceKind } from '@/lib/iam'
import { useAuthStore } from '@/store/auth'
import { useIamStore } from '@/store/iam'
import { useLibraryStore } from '@/store/library'
import type { AuthTeam, AuthUser } from '@/types/auth'

type Kind = Extract<ResourceKind, 'job' | 'pipeline'>

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

export function ResourceGrantsPanel() {
  const can = useAuthStore((state) => state.can)
  const fetchTeams = useAuthStore((state) => state.fetchTeams)
  const fetchUsers = useAuthStore((state) => state.fetchUsers)

  const jobs = useLibraryStore((state) => state.jobs)
  const pipelines = useLibraryStore((state) => state.pipelines)

  const grants = useIamStore((state) => state.grants)
  const loading = useIamStore((state) => state.loading)
  const load = useIamStore((state) => state.load)
  const addGrant = useIamStore((state) => state.grant)
  const revokeGrant = useIamStore((state) => state.revoke)

  const [kind, setKind] = useState<Kind>('job')
  const [selected, setSelected] = useState('')
  const [teams, setTeams] = useState<AuthTeam[]>([])
  const [users, setUsers] = useState<AuthUser[]>([])

  // Reading the rules needs nothing special — they are workspace data, and a
  // person who cannot see them cannot tell whether they are governed at all.
  // Changing them is the administrator's action, and the runner refuses it too.
  const mayManage = can('iam:ManageGrants')

  useEffect(() => {
    void load()
  }, [load])

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

  const options = useMemo<SelectOption[]>(() => {
    const source =
      kind === 'job'
        ? jobs.map((job) => ({ id: job.id, name: job.name }))
        : pipelines.map((pipeline) => ({ id: pipeline.id, name: pipeline.name }))
    const counted = source.map((item) => {
      const rules = grantsFor(grants, kind, item.id)
      const denies = rules.filter((rule) => rule.effect === 'deny').length
      return {
        value: item.id,
        label: item.name,
        hint:
          rules.length === 0
            ? 'no rules'
            : `${rules.length - denies} allow${denies > 0 ? `, ${denies} deny` : ''}`,
      }
    })
    return counted.sort((left, right) => left.label.localeCompare(right.label))
  }, [grants, jobs, kind, pipelines])

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

  const governed = useMemo(() => {
    const ids = new Set(
      grants.filter((grant) => grant.resource === kind).map((grant) => grant.resourceId),
    )
    return ids.size
  }, [grants, kind])

  return (
    <div className="space-y-4 border-t border-line pt-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-0.5">
          <p className="text-sm text-content">Who can run what</p>
          <p className="max-w-md text-2xs leading-relaxed text-content-subtle">
            Roles say whether somebody may run anything on this runner. These say which Jobs and
            Pipelines in particular — and which they may never touch, whatever else grants it.
            Datasets are governed the same way, from the catalog.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {governed > 0 ? (
            <Badge tone="brand">
              {governed} {kind === 'job' ? 'Job' : 'Pipeline'}
              {governed === 1 ? '' : 's'} restricted
            </Badge>
          ) : (
            <Badge tone="neutral">nothing restricted</Badge>
          )}
          {loading ? <Spinner className="h-3.5 w-3.5" /> : null}
        </div>
      </div>

      <div className="grid gap-2 sm:grid-cols-[13rem_minmax(0,1fr)]">
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
            ]}
          />
        </Field>
        <Field label={kind === 'job' ? 'Job' : 'Pipeline'}>
          {options.length === 0 ? (
            <p className="flex items-center gap-1.5 py-1.5 text-xs italic text-content-subtle">
              {kind === 'job' ? (
                <Boxes className="h-3.5 w-3.5" />
              ) : (
                <Workflow className="h-3.5 w-3.5" />
              )}
              The library has no {kind === 'job' ? 'Job' : 'Pipeline'} to restrict yet.
            </p>
          ) : (
            <Select
              value={selected}
              options={options}
              ariaLabel={kind === 'job' ? 'Job to restrict' : 'Pipeline to restrict'}
              onValueChange={setSelected}
            />
          )}
        </Field>
      </div>

      {selected ? (
        <GrantsPanel
          resource={kind}
          teams={teams}
          users={users}
          grants={scoped}
          onGrant={onGrant}
          onRevoke={onRevoke}
          editable={mayManage}
        />
      ) : null}
    </div>
  )
}
