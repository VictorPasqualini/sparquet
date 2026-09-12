import { Database, Network, Search, Share2, Sparkles, Table2, X } from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { toast } from 'sonner'

import {
  Button,
  EmptyState,
  IconButton,
  Input,
  Kbd,
  Segmented,
  Select,
  type SegmentedOption,
} from '@/components/ui'
import { PageHeader, PageShell } from '@/components/layout/PageShell'
import { CatalogBrowser } from '@/components/lineage/CatalogBrowser'
import { DatasetSheet } from '@/components/lineage/DatasetSheet'
import { LineageGraph } from '@/components/lineage/LineageGraph'
import {
  buildCatalog,
  buildColumnGraph,
  catalogStats,
  deriveSchemas,
  type DatasetAnnotation,
  type ProbedField,
} from '@/lib/datacatalog'
import { grantsByResource, type DatasetGrant } from '@/lib/iam'
import { fetchDatasetSchema } from '@/lib/runner/client'
import { sparkForDatasets } from '@/lib/runner/session'
import { buildLineage, type DatasetPlace } from '@/lib/lineage'
import {
  LINEAGE_EXAMPLE_WORKFLOW,
  lineageExampleTemplates,
} from '@/data/templates'
import { useCatalogStore } from '@/store/catalog'
import {
  effectiveOwnerOf,
  mayAdministerResource,
  useIamStore,
} from '@/store/iam'
import { useAuthStore } from '@/store/auth'
import type { AuthTeam, AuthUser } from '@/types/auth'
import { useLibraryStore } from '@/store/library'
import { useSettingsStore } from '@/store/settings'

type PlaceFilter = 'all' | DatasetPlace

const PLACE_LABEL: Record<DatasetPlace, string> = {
  external: 'External',
  intermediate: 'Handoff',
  terminal: 'Terminal',
  isolated: 'Isolated',
}

/** What each placement means, said once, where the reader is looking at it. */
const PLACE_HINT: Record<DatasetPlace, string> = {
  external: 'Read here, written somewhere else — it enters the platform from outside.',
  intermediate: 'One Job writes it, another reads it. This is where Jobs depend on each other.',
  terminal: 'Written and never read back here: the end of a chain, or a table consumed downstream.',
  isolated: 'Only one Job mentions it, and only on one side. Often a typo in an address.',
}

const FILTERS: PlaceFilter[] = ['all', 'intermediate', 'external', 'terminal', 'isolated']

type View = 'list' | 'graph'

const VIEWS: SegmentedOption<View>[] = [
  {
    value: 'list',
    title: 'Databases, buckets and the tables inside them — what each one is and who owns it',
    label: (
      <span className="flex items-center gap-1.5">
        <Table2 className="h-3 w-3" />
        Catalog
      </span>
    ),
  },
  {
    value: 'graph',
    title: 'The whole path, drawn: dataset, Job, dataset',
    label: (
      <span className="flex items-center gap-1.5">
        <Share2 className="h-3 w-3" />
        Lineage
      </span>
    ),
  },
]

/** Scope value that means "the whole library", not one workflow. */
const ALL_WORKFLOWS = 'all'

/**
 * Datasets, and which Jobs write and read them.
 *
 * A section of its own rather than a tab of the editor: what makes lineage worth
 * looking at is everything the editor cannot see — the OTHER Jobs that touch the
 * same path. Inside one canvas the answer is already on screen.
 */
/** Stable empty list, so an ungoverned dataset does not remount the panel. */
const EMPTY_GRANTS: DatasetGrant[] = []

export function Catalog() {
  const navigate = useNavigate()
  const jobs = useLibraryStore((state) => state.jobs)
  const workflows = useLibraryStore((state) => state.workflows)
  const annotations = useCatalogStore((state) => state.annotations)
  const loadCatalog = useCatalogStore((state) => state.load)
  const annotate = useCatalogStore((state) => state.annotate)
  const forget = useCatalogStore((state) => state.forget)
  const grants = useIamStore((state) => state.grants)
  const loadGrants = useIamStore((state) => state.load)
  const addGrant = useIamStore((state) => state.grant)
  const revokeGrant = useIamStore((state) => state.revoke)
  const owners = useIamStore((state) => state.owners)
  const setOwner = useIamStore((state) => state.setOwner)
  const clearOwner = useIamStore((state) => state.clearOwner)
  const fetchTeams = useAuthStore((state) => state.fetchTeams)
  const fetchUsers = useAuthStore((state) => state.fetchUsers)
  const [teams, setTeams] = useState<AuthTeam[]>([])
  const [users, setUsers] = useState<AuthUser[]>([])
  const createWorkflow = useLibraryStore((state) => state.createWorkflow)
  const createJob = useLibraryStore((state) => state.createJob)
  const createPipeline = useLibraryStore((state) => state.createPipeline)
  const [loadingExample, setLoadingExample] = useState(false)
  const [view, setView] = useState<View>('list')
  const [scope, setScope] = useState<string>(ALL_WORKFLOWS)
  const [place, setPlace] = useState<PlaceFilter>('all')
  const [query, setQuery] = useState('')
  const [openKey, setOpenKey] = useState<string | null>(null)
  const searchRef = useRef<HTMLInputElement>(null)

  /**
   * Who exists, for the grant picker. Only while a sheet is open, and a failure
   * is silent on purpose: a runner with no user records still has a token
   * identity, and the picker falls back to typing a name.
   */
  useEffect(() => {
    if (!openKey) return
    let alive = true
    void (async () => {
      try {
        const [nextTeams, nextUsers] = await Promise.all([fetchTeams(), fetchUsers()])
        if (!alive) return
        setTeams(nextTeams)
        setUsers(nextUsers)
      } catch {
        /* no IAM on this runner; the picker stays free text */
      }
    })()
    return () => {
      alive = false
    }
  }, [openKey, fetchTeams, fetchUsers])

  // The catalog is read here rather than at boot: it is only ever needed by this
  // screen, and the editor should not pay for it.
  useEffect(() => {
    void loadCatalog()
    void loadGrants()
  }, [loadCatalog, loadGrants])

  /**
   * A workflow scope narrows WHICH Jobs are read, not which datasets are shown.
   * Anything the scoped Jobs touch stays on screen, including a table whose other
   * side lives in another workflow — that is exactly the edge worth seeing, and
   * filtering by dataset would hide it.
   */
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)

  const scoped = useMemo(
    () => (scope === ALL_WORKFLOWS ? jobs : jobs.filter((job) => job.workflowId === scope)),
    [jobs, scope],
  )

  const index = useMemo(() => buildLineage(scoped), [scoped])
  const entries = useMemo(() => buildCatalog(index, annotations), [index, annotations])
  const stats = useMemo(() => catalogStats(entries), [entries])
  // Columns and types, read off the same canvases the lineage came from.
  const schemas = useMemo(() => deriveSchemas(scoped), [scoped])
  // Where each column came from and what it feeds, from that same walk.
  const columns = useMemo(() => buildColumnGraph(scoped), [scoped])
  // Access rules per dataset address. Grouped once here because both the browser
  // and the sheet want them, and both are keyed by the same address.
  const datasetGrants = useMemo(() => grantsByResource(grants, 'dataset'), [grants])

  const searched = useMemo(() => {
    const needle = query.trim().toLowerCase()
    if (!needle) return entries
    return entries.filter(({ dataset, annotation }) => {
      const documented = [
        annotation?.description,
        annotation?.owner,
        annotation?.domain,
        ...(annotation?.tags ?? []),
      ]
      return (
        dataset.key.toLowerCase().includes(needle) ||
        dataset.formats.some((format) => format.includes(needle)) ||
        documented.some((value) => value?.toLowerCase().includes(needle)) ||
        [...dataset.producers, ...dataset.consumers].some((mention) =>
          mention.jobName.toLowerCase().includes(needle),
        )
      )
    })
  }, [entries, query])

  const visible = useMemo(
    () =>
      place === 'all' ? searched : searched.filter((entry) => entry.dataset.place === place),
    [searched, place],
  )

  const open = useMemo(
    () => entries.find((entry) => entry.dataset.key === openKey) ?? null,
    [entries, openKey],
  )

  const workflowName = useCallback(
    (id: string) => workflows.find((workflow) => workflow.id === id)?.name ?? 'Unknown workflow',
    [workflows],
  )

  const knownTags = useMemo(() => {
    const tags = new Set<string>()
    for (const annotation of Object.values(annotations)) {
      for (const tag of annotation.tags) tags.add(tag)
    }
    return [...tags].sort()
  }, [annotations])

  const scopeOptions = useMemo(
    () => [
      { value: ALL_WORKFLOWS, label: 'All workflows' },
      ...workflows.map((workflow) => ({ value: workflow.id, label: workflow.name })),
    ],
    [workflows],
  )

  const options: SegmentedOption<PlaceFilter>[] = useMemo(
    () =>
      FILTERS.map((value) => {
        const count =
          value === 'all'
            ? searched.length
            : searched.filter((entry) => entry.dataset.place === value).length
        return {
          value,
          title: value === 'all' ? 'Every dataset' : PLACE_HINT[value],
          label: (
            <span className="flex items-center gap-1.5">
              {value === 'all' ? 'All' : PLACE_LABEL[value]}
              <span className="tabular-nums text-content-subtle">{count}</span>
            </span>
          ),
        }
      }),
    [searched],
  )

  const openJob = useCallback((jobId: string) => navigate(`/jobs/${jobId}`), [navigate])

  /**
   * Asks the runner what a dataset really contains.
   *
   * Everything else on this screen is derived from the canvas, which says what a
   * Job intends to write; this is the one answer that comes from the storage. It
   * reads no rows — the runner builds a reader and takes the schema.
   */
  const probe = useCallback(
    async (key: string, format: string): Promise<ProbedField[]> => {
      const schema = await fetchDatasetSchema(
        runnerUrl,
        {
          format,
          path: key,
          // How the Jobs that touch this dataset open it. A Delta table is
          // unreadable on a SparkSession built without its jars and extensions,
          // and those are honoured only when a session is created — so the
          // runner is told what this read needs and rebuilds if it has to.
          spark: sparkForDatasets(jobs, [key]),
        },
        undefined,
        runnerToken,
      )
      return schema.fields
    },
    [jobs, runnerUrl, runnerToken],
  )

  const save = useCallback(
    async (key: string, patch: Partial<DatasetAnnotation>) => {
      try {
        await annotate(key, patch)
      } catch (error) {
        toast.error(error instanceof Error ? error.message : 'Could not save the catalog entry')
      }
    },
    [annotate],
  )

  const drop = useCallback(
    async (key: string) => {
      try {
        await forget(key)
      } catch (error) {
        toast.error(error instanceof Error ? error.message : 'Could not remove the catalog entry')
      }
    },
    [forget],
  )

  /**
   * Loads the three Medallion Jobs as one workflow. They are ordinary templates —
   * what the example adds is the chain: bronze writes the path silver reads, and
   * silver writes the path gold reads, which is the only thing that links them.
   */
  const loadExample = useCallback(async () => {
    setLoadingExample(true)
    try {
      const workflow = await createWorkflow({
        name: LINEAGE_EXAMPLE_WORKFLOW,
        description:
          'Three Jobs that hand data to each other by address alone: landing to bronze, bronze to silver, silver to gold.',
        accent: 'sky',
      })
      const created = []
      for (const template of lineageExampleTemplates()) {
        created.push(
          await createJob({
            workflowId: workflow.id,
            name: template.name,
            description: template.summary,
            pipeline: template.pipeline,
          }),
        )
      }
      await createPipeline({
        workflowId: workflow.id,
        name: 'Medallion',
        description: 'Bronze, then silver, then gold — the order the addresses already imply.',
        jobIds: created.map((job) => job.id),
      })
      toast.success(`${created.length} Jobs added — the chain is in the graph below`)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Could not load the example')
    } finally {
      setLoadingExample(false)
    }
  }, [createJob, createPipeline, createWorkflow])

  const clear = useCallback(() => {
    setQuery('')
    setPlace('all')
    searchRef.current?.focus()
  }, [])

  if (jobs.length === 0) {
    return (
      <PageShell>
        <div className="card">
          <EmptyState
            icon={<Network />}
            title="No lineage yet"
            description="Lineage is read off the Jobs you have. Load the three-Job example to see a
              full chain — landing to bronze to silver to gold — or start from a template."
            action={
              <div className="flex items-center gap-2">
                <Button size="sm" onClick={loadExample} loading={loadingExample}>
                  <Sparkles />
                  Load the 3-Job example
                </Button>
                <Button size="sm" variant="secondary" onClick={() => navigate('/templates')}>
                  Browse templates
                </Button>
              </div>
            }
          />
        </div>
      </PageShell>
    )
  }

  return (
    <PageShell>
      <PageHeader
        icon={<Database />}
        title="Data catalog"
        description="Every table, bucket and topic your Jobs touch, grouped the way a metastore
          groups them — except nothing was registered anywhere: the hierarchy is read back out of
          the addresses. Lineage is the same inventory seen edge-first, which is why it lives
          here: two Jobs are linked by the address alone, one writes a path and another reads it.
          What the data MEANS is the one thing no pipeline can say, so it is the one thing you
          type."
        actions={
          <Button size="sm" variant="secondary" onClick={loadExample} loading={loadingExample}>
            <Sparkles />
            Load example
          </Button>
        }
      />

      <dl className="mb-5 grid grid-cols-2 gap-3 sm:grid-cols-4">
        {(
          [
            ['Datasets', String(index.datasets.length), 'Every address these Jobs read or write.'],
            ['Jobs', String(index.jobs.length), 'Jobs in scope.'],
            [
              'Handoffs between Jobs',
              String(index.edges.length),
              'One Job writes the address another reads. The only dependency the JSON declares.',
            ],
            [
              'Described',
              `${stats.documented}/${stats.total}`,
              'Datasets with a catalog entry that says what they are. Owner and classification are counted separately, inside.',
            ],
          ] as const
        ).map(([label, value, hint]) => (
          <div key={label} className="card px-3 py-2" title={hint}>
            <dt className="text-[11px] text-content-subtle">{label}</dt>
            <dd className="text-sm font-semibold tabular-nums text-content">{value}</dd>
          </div>
        ))}
      </dl>

      <div className="mb-5 flex flex-wrap items-center gap-3">
        <Segmented value={view} onChange={setView} options={VIEWS} size="sm" ariaLabel="View" />
        {workflows.length > 1 ? (
          <Select
            value={scope}
            onValueChange={setScope}
            options={scopeOptions}
            ariaLabel="Workflow"
            className="h-9 w-48 py-0 text-xs"
          />
        ) : null}
        {view === 'list' ? (
          <Segmented value={place} onChange={setPlace} options={options} size="sm" />
        ) : null}
        <div className="relative ml-auto w-full max-w-xs">
          <Input
            ref={searchRef}
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Escape' && query) {
                event.preventDefault()
                setQuery('')
              }
            }}
            placeholder="Search address, format or Job"
            aria-label="Search datasets"
            leading={<Search />}
            className="h-9 py-0 pr-9 text-xs"
          />
          {query ? (
            <span className="absolute right-1.5 top-1/2 -translate-y-1/2">
              <IconButton size="sm" label="Clear search" onClick={() => setQuery('')}>
                <X />
              </IconButton>
            </span>
          ) : (
            <span className="pointer-events-none absolute right-2.5 top-1/2 -translate-y-1/2">
              <Kbd>/</Kbd>
            </span>
          )}
        </div>
      </div>

      {view === 'graph' ? (
        <div className="space-y-2">
          <div className="h-[70vh] overflow-hidden rounded-lg border border-line">
            <LineageGraph
              index={index}
              annotations={annotations}
              query={query}
              onOpenJob={openJob}
              onOpenDataset={setOpenKey}
            />
          </div>
          <p className="text-2xs text-content-subtle">
            Click a box to trace it — everything upstream and downstream of it stays lit.
            Click the background to clear, double-click a Job to open its canvas, double-click a
            dataset to write its catalog entry.
          </p>
        </div>
      ) : visible.length === 0 ? (
        <div className="card">
          <EmptyState
            icon={<Search />}
            title="No dataset matches"
            description="Try a shorter search, or clear the filter to see every dataset."
            action={
              <Button size="sm" variant="secondary" onClick={clear}>
                Clear filters
              </Button>
            }
          />
        </div>
      ) : (
        <CatalogBrowser
          entries={visible}
          searching={query.trim() !== ''}
          schemas={schemas}
          grants={datasetGrants}
          owners={owners}
          onOpenDataset={setOpenKey}
          workflowName={workflowName}
        />
      )}

      {open ? (
        <DatasetSheet
          dataset={open.dataset}
          annotation={open.annotation}
          workflowNames={open.dataset.workflowIds.map(workflowName)}
          suggestions={knownTags}
          schema={schemas.get(open.dataset.key) ?? null}
          columns={columns}
          grants={datasetGrants.get(open.dataset.key) ?? EMPTY_GRANTS}
          teams={teams}
          users={users}
          onGrant={(input) => addGrant({ ...input, resource: 'dataset', resourceId: open.dataset.key })}
          onRevoke={revokeGrant}
          owner={
            owners.find(
              (owner) => owner.resource === 'dataset' && owner.resourceId === open.dataset.key,
            ) ?? null
          }
          ownerEffective={effectiveOwnerOf('dataset', open.dataset.key)}
          onAssignOwner={(input) =>
            setOwner({ ...input, resource: 'dataset', resourceId: open.dataset.key })
          }
          onClearOwner={() => clearOwner('dataset', open.dataset.key)}
          mayManageAccess={mayAdministerResource('dataset', open.dataset.key)}
          probe={runnerUrl ? (format) => probe(open.dataset.key, format) : null}
          onClose={() => setOpenKey(null)}
          onSave={(patch) => save(open.dataset.key, patch)}
          onForget={() => drop(open.dataset.key)}
          onOpenJob={openJob}
        />
      ) : null}
    </PageShell>
  )
}
