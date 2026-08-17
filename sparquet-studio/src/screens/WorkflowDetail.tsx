import {
  ArrowLeft,
  ArrowRight,
  Copy,
  FileJson,
  FolderSymlink,
  List,
  ListOrdered,
  MoreHorizontal,
  Palette,
  Pencil,
  Plus,
  Search,
  Share2,
  Trash2,
  Workflow as JobIcon,
} from 'lucide-react'
import {
  lazy,
  Suspense,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
} from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { toast } from 'sonner'

import {
  Badge,
  Button,
  EmptyState,
  Field,
  IconButton,
  Input,
  Menu,
  MenuContent,
  MenuItem,
  MenuSeparator,
  MenuTrigger,
  Modal,
  Popover,
  PopoverContent,
  PopoverTrigger,
  SectionTitle,
  Segmented,
  Select,
  Spinner,
  Textarea,
  useConfirm,
} from '@/components/ui'
import { TEMPLATES } from '@/data/templates'
import { compileGraph, serializePipeline } from '@/lib/compiler'
import { cn } from '@/lib/utils/cn'
import { downloadText } from '@/lib/utils/download'
import { plural, relativeTime } from '@/lib/utils/format'
import { slugify, useLibraryStore } from '@/store/library'
import {
  WORKFLOW_ACCENTS,
  type Pipeline,
  type WorkflowAccent,
  type Job,
  type JobTemplate,
} from '@/types/studio'

/** Only the pipeline view pulls React Flow, and only the workflow screen offers it. */
const InferredPipelineView = lazy(() =>
  import('@/components/pipeline/InferredPipelineView').then((m) => ({ default: m.InferredPipelineView })),
)

type SortKey = 'updated' | 'name'
type ViewKey = 'files' | 'pipeline'

const VIEWS: { id: ViewKey; label: string; icon: typeof List }[] = [
  // The vocabulary: a Workflow holds Jobs (one pipeline JSON each) and Pipelines
  // (ordered sets of Jobs that run in sequence).
  { id: 'files', label: 'Jobs', icon: List },
  { id: 'pipeline', label: 'Pipeline', icon: Share2 },
]

/** Workflow accents mapped onto the semantic palette — no raw colors anywhere. */
const ACCENT_DOT: Record<WorkflowAccent, string> = {
  amber: 'bg-brand-500',
  sky: 'bg-node-input',
  violet: 'bg-node-combine',
  emerald: 'bg-node-output',
  rose: 'bg-state-danger',
  slate: 'bg-node-inspect',
}

export function WorkflowDetail() {
  const { workflowId } = useParams<{ workflowId: string }>()
  const navigate = useNavigate()

  const workflow = useLibraryStore((state) =>
    state.workflows.find((candidate) => candidate.id === workflowId),
  )
  const jobs = useLibraryStore((state) => state.jobs)
  const pipelines = useLibraryStore((state) => state.pipelines)
  const updateWorkflow = useLibraryStore((state) => state.updateWorkflow)
  const deleteWorkflow = useLibraryStore((state) => state.deleteWorkflow)
  const duplicateJob = useLibraryStore((state) => state.duplicateJob)
  const deleteJob = useLibraryStore((state) => state.deleteJob)
  const deletePipeline = useLibraryStore((state) => state.deletePipeline)

  const [confirm, confirmDialog] = useConfirm()
  const [view, setView] = useState<ViewKey>('files')
  const [query, setQuery] = useState('')
  const [sort, setSort] = useState<SortKey>('updated')
  const [creating, setCreating] = useState(false)
  const [creatingPipeline, setCreatingPipeline] = useState(false)
  const [renaming, setRenaming] = useState<Job | null>(null)
  const [renamingPipeline, setRenamingPipeline] = useState<Pipeline | null>(null)
  const [moving, setMoving] = useState<Job | null>(null)

  const tabsRef = useRef<HTMLDivElement>(null)

  const [name, setName] = useState(workflow?.name ?? '')
  const [description, setDescription] = useState(workflow?.description ?? '')
  useEffect(() => setName(workflow?.name ?? ''), [workflow?.name])
  useEffect(() => setDescription(workflow?.description ?? ''), [workflow?.description])

  const owned = useMemo(
    () => jobs.filter((job) => job.workflowId === workflowId),
    [jobs, workflowId],
  )

  const ownedPipelines = useMemo(
    () =>
      pipelines
        .filter((pipeline) => pipeline.workflowId === workflowId)
        .sort((a, b) => b.updatedAt - a.updatedAt),
    [pipelines, workflowId],
  )

  const rows = useMemo(() => {
    const needle = query.trim().toLowerCase()
    const matched = needle ? owned.filter((job) => matches(job, needle)) : [...owned]
    return matched.sort((a, b) =>
      sort === 'name' ? a.name.localeCompare(b.name) : b.updatedAt - a.updatedAt,
    )
  }, [owned, query, sort])

  const totalNodes = useMemo(
    () => owned.reduce((total, job) => total + countNodes(job), 0),
    [owned],
  )

  if (!workflow) {
    return (
      <div className="mx-auto w-full max-w-5xl px-6 py-6">
        <div className="card">
          <EmptyState
            icon={<JobIcon />}
            title="Workflow not found"
            description="It may have been deleted from another tab."
            action={
              <Link
                to="/"
                className="text-xs text-brand-600 hover:underline dark:text-brand-400"
              >
                Back to overview
              </Link>
            }
          />
        </div>
      </div>
    )
  }

  const commitName = () => {
    const trimmed = name.trim()
    if (!trimmed || trimmed === workflow.name) {
      setName(workflow.name)
      return
    }
    void updateWorkflow(workflow.id, { name: trimmed })
  }

  const commitDescription = () => {
    const trimmed = description.trim()
    if (trimmed === workflow.description) return
    void updateWorkflow(workflow.id, { description: trimmed })
  }

  const handleDeleteWorkflow = async () => {
    const confirmed = await confirm({
      title: 'Delete workflow',
      message: (
        <>
          <span className="font-medium text-content">{workflow.name}</span> and its{' '}
          {plural(owned.length, 'job')} will be removed. This cannot be undone.
        </>
      ),
      confirmLabel: 'Delete workflow',
    })
    if (!confirmed) return
    await deleteWorkflow(workflow.id)
    toast.success('Workflow deleted')
    navigate('/')
  }

  const handleDuplicate = async (job: Job) => {
    const copy = await duplicateJob(job.id)
    if (copy) toast.success(`Duplicated as "${copy.name}"`)
  }

  const handleDelete = async (job: Job) => {
    const staged = ownedPipelines.filter((pipeline) =>
      pipeline.stages.some((stage) => stage.jobId === job.id),
    )
    const confirmed = await confirm({
      title: 'Delete job',
      message: (
        <>
          <span className="font-medium text-content">{job.name}</span> and its canvas will
          be removed. This cannot be undone.
          {staged.length > 0 && (
            <>
              {' '}
              It is a stage of {plural(staged.length, 'pipeline')}, which will show it as
              missing until you remove the stage.
            </>
          )}
        </>
      ),
      confirmLabel: 'Delete',
    })
    if (!confirmed) return
    await deleteJob(job.id)
    toast.success('Job deleted')
  }

  const handleDeletePipeline = async (pipeline: Pipeline) => {
    const confirmed = await confirm({
      title: 'Delete pipeline',
      message: (
        <>
          <span className="font-medium text-content">{pipeline.name}</span> will be removed. The
          pipelines it staged are not touched.
        </>
      ),
      confirmLabel: 'Delete',
    })
    if (!confirmed) return
    await deletePipeline(pipeline.id)
    toast.success('Pipeline deleted')
  }

  return (
    <div className="mx-auto w-full max-w-6xl space-y-6 px-6 py-6 animate-fade-in">
      <nav className="flex items-center gap-1 text-2xs text-content-subtle">
        <Link
          to="/"
          className="inline-flex items-center gap-1 rounded px-1 py-0.5 transition-colors hover:text-content"
        >
          <ArrowLeft className="h-3 w-3" />
          Overview
        </Link>
      </nav>

      <header className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0 flex-1 space-y-1">
          <div className="flex items-center gap-2">
            <span
              className={cn('h-2 w-2 shrink-0 rounded-full', ACCENT_DOT[workflow.accent])}
              aria-hidden
            />
            <Input
              value={name}
              aria-label="Workflow name"
              onChange={(event) => setName(event.target.value)}
              onBlur={commitName}
              onKeyDown={(event) => {
                if (event.key === 'Enter') event.currentTarget.blur()
                if (event.key === 'Escape') {
                  setName(workflow.name)
                  event.currentTarget.blur()
                }
              }}
              className="h-8 max-w-md border-transparent bg-transparent px-2 text-lg font-semibold tracking-tight hover:border-line focus:bg-surface-sunken"
            />
          </div>
          <Textarea
            value={description}
            rows={2}
            aria-label="Workflow description"
            placeholder="What lives in this workflow?"
            onChange={(event) => setDescription(event.target.value)}
            onBlur={commitDescription}
            className="max-w-2xl resize-none border-transparent bg-transparent px-2 py-1 text-xs text-content-muted hover:border-line focus:bg-surface-sunken"
          />
          <p className="px-2 text-2xs text-content-subtle">
            {plural(owned.length, 'job')} · {plural(totalNodes, 'node')} · updated{' '}
            {relativeTime(workflow.updatedAt)}
          </p>
        </div>

        <div className="flex items-center gap-1">
          <Popover>
            <PopoverTrigger asChild>
              <IconButton label="Workflow accent" size="sm">
                <Palette />
              </IconButton>
            </PopoverTrigger>
            <PopoverContent align="end">
              <AccentPicker
                value={workflow.accent}
                onChange={(accent) => void updateWorkflow(workflow.id, { accent })}
              />
            </PopoverContent>
          </Popover>
          <IconButton
            label="Delete workflow"
            size="sm"
            onClick={() => void handleDeleteWorkflow()}
            className="hover:text-state-danger"
          >
            <Trash2 />
          </IconButton>
        </div>
      </header>

      <div className="flex flex-wrap items-center gap-2">
        <div
          ref={tabsRef}
          role="tablist"
          aria-label="Workflow views"
          className="flex items-center gap-0.5 rounded-lg border border-line bg-surface-sunken p-0.5"
        >
          {VIEWS.map((tab, index) => {
            const active = tab.id === view
            return (
              <button
                key={tab.id}
                type="button"
                role="tab"
                id={viewTabId(tab.id)}
                aria-selected={active}
                aria-controls={viewPanelId(tab.id)}
                tabIndex={active ? 0 : -1}
                onKeyDown={(event) => onViewTabKeyDown(event, index, setView, tabsRef)}
                onClick={() => setView(tab.id)}
                className={cn(
                  'flex items-center gap-1.5 rounded-md px-2 py-1 text-2xs font-medium transition-colors',
                  active
                    ? 'bg-surface text-content shadow-sm'
                    : 'text-content-subtle hover:text-content',
                )}
              >
                <tab.icon className="h-3.5 w-3.5" aria-hidden />
                {tab.label}
              </button>
            )
          })}
        </div>

        {view === 'files' && (
          <>
            <Input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search jobs"
              aria-label="Search jobs"
              leading={<Search />}
              className="h-8 w-64 py-1 text-xs"
            />
            <Segmented
              size="sm"
              value={sort}
              onChange={setSort}
              options={[
                { value: 'updated', label: 'Updated', title: 'Most recently updated first' },
                { value: 'name', label: 'Name', title: 'Alphabetical' },
              ]}
            />
          </>
        )}

        <div className="ml-auto flex items-center gap-2">
          <Button
            size="sm"
            icon={<ListOrdered className="h-4 w-4" />}
            disabled={owned.length === 0}
            title={
              owned.length === 0
                ? 'Create a job first — a pipeline orders files that already exist'
                : 'Chain several pipelines into one sequential run'
            }
            onClick={() => setCreatingPipeline(true)}
          >
            New pipeline
          </Button>
          <Button
            size="sm"
            variant="primary"
            icon={<Plus className="h-4 w-4" />}
            onClick={() => setCreating(true)}
          >
            New job
          </Button>
        </div>
      </div>

      {view === 'pipeline' ? (
        <div
          role="tabpanel"
          id={viewPanelId('pipeline')}
          aria-labelledby={viewTabId('pipeline')}
          className="space-y-3"
        >
          <Suspense
            fallback={
              <div className="flex items-center justify-center py-24">
                <Spinner className="h-5 w-5" />
              </div>
            }
          >
            <InferredPipelineView jobs={owned} />
          </Suspense>

          {/* The map above is derived and read-only; this is the way to an
              order the author decides and the runner executes. */}
          <div className="card flex flex-wrap items-center gap-3 px-4 py-3">
            <ListOrdered className="h-4 w-4 shrink-0 text-brand-500" aria-hidden />
            <p className="min-w-0 flex-1 text-2xs leading-relaxed text-content-muted">
              Need these files to run one after another? A pipeline lets you draw the order
              yourself and run the whole sequence.
            </p>
            <Button
              size="sm"
              disabled={owned.length === 0}
              onClick={() => setCreatingPipeline(true)}
            >
              New pipeline
            </Button>
          </div>
        </div>
      ) : (
        <div
          role="tabpanel"
          id={viewPanelId('files')}
          aria-labelledby={viewTabId('files')}
          className="space-y-6"
        >
          {ownedPipelines.length > 0 && (
            <section className="space-y-2">
              <SectionTitle>Pipelines</SectionTitle>
              <ul className="divide-y divide-line overpipeline-hidden rounded-xl border border-line bg-surface shadow-card">
                {ownedPipelines.map((pipeline) => (
                  <PipelineRow
                    key={pipeline.id}
                    pipeline={pipeline}
                    jobs={owned}
                    onOpen={() => navigate(`/pipelines/${pipeline.id}`)}
                    onRename={() => setRenamingPipeline(pipeline)}
                    onDelete={() => void handleDeletePipeline(pipeline)}
                  />
                ))}
              </ul>
            </section>
          )}

          {owned.length === 0 ? (
            <div className="card">
              <EmptyState
                icon={<JobIcon />}
                title="No jobs in this workflow"
                description="Start from a blank canvas or pick a template that already wires a source, transformations and a destination."
                action={
                  <Button
                    size="sm"
                    variant="primary"
                    icon={<Plus className="h-4 w-4" />}
                    onClick={() => setCreating(true)}
                  >
                    New job
                  </Button>
                }
              />
            </div>
          ) : rows.length === 0 ? (
            <div className="card">
              <EmptyState
                icon={<Search />}
                title="No matches"
                description={`Nothing in this workflow matches "${query.trim()}".`}
                action={
                  <Button size="sm" onClick={() => setQuery('')}>
                    Clear search
                  </Button>
                }
              />
            </div>
          ) : (
            <ul
              aria-label="Pipelines"
              className="divide-y divide-line overpipeline-hidden rounded-xl border border-line bg-surface shadow-card"
            >
              {rows.map((job) => (
                <JobRow
                  key={job.id}
                  job={job}
                  onOpen={() => navigate(`/jobs/${job.id}`)}
                  onDuplicate={() => void handleDuplicate(job)}
                  onRename={() => setRenaming(job)}
                  onMove={() => setMoving(job)}
                  onExport={() => exportJob(job)}
                  onDelete={() => void handleDelete(job)}
                />
              ))}
            </ul>
          )}
        </div>
      )}

      {creating && (
        <NewJobModal workflowId={workflow.id} onClose={() => setCreating(false)} />
      )}
      {creatingPipeline && (
        <NewPipelineModal
          workflowId={workflow.id}
          jobs={owned}
          onClose={() => setCreatingPipeline(false)}
        />
      )}
      {renaming && (
        <RenameJobModal job={renaming} onClose={() => setRenaming(null)} />
      )}
      {renamingPipeline && (
        <RenamePipelineModal pipeline={renamingPipeline} onClose={() => setRenamingPipeline(null)} />
      )}
      {moving && <MoveJobModal job={moving} onClose={() => setMoving(null)} />}
      {confirmDialog}
    </div>
  )
}

/* -------------------------------------------------------------------- rows */

interface JobRowProps {
  job: Job
  onOpen: () => void
  onDuplicate: () => void
  onRename: () => void
  onMove: () => void
  onExport: () => void
  onDelete: () => void
}

function JobRow({
  job,
  onOpen,
  onDuplicate,
  onRename,
  onMove,
  onExport,
  onDelete,
}: JobRowProps) {
  return (
    <li className="flex items-center gap-2 pr-2 transition-colors hover:bg-surface-raised">
      <Link
        to={`/jobs/${job.id}`}
        className="flex min-w-0 flex-1 items-center gap-3 px-3 py-2.5 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand-500/50"
      >
        <span
          className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg border border-line bg-surface-sunken text-content-subtle"
          aria-hidden
        >
          <JobIcon className="h-3.5 w-3.5" />
        </span>

        <span className="min-w-0 flex-1">
          <span className="block truncate text-xs font-medium text-content">
            {job.name}
          </span>
          {job.description && (
            <span className="block truncate text-2xs text-content-subtle">
              {job.description}
            </span>
          )}
        </span>

        {job.tags.length > 0 && (
          <span className="hidden shrink-0 items-center gap-1 lg:flex">
            {job.tags.slice(0, 3).map((tag) => (
              <span key={tag} className="chip">
                {tag}
              </span>
            ))}
          </span>
        )}

        <span className="w-20 shrink-0 text-right text-2xs tabular-nums text-content-subtle">
          {plural(countNodes(job), 'node')}
        </span>
        <span className="hidden w-24 shrink-0 text-right text-2xs text-content-subtle sm:block">
          {relativeTime(job.updatedAt)}
        </span>
      </Link>

      <Menu>
        <MenuTrigger asChild>
          <IconButton label={`Actions for ${job.name}`} size="sm">
            <MoreHorizontal />
          </IconButton>
        </MenuTrigger>
        <MenuContent>
          <MenuItem icon={<ArrowRight />} onSelect={onOpen}>
            Open
          </MenuItem>
          <MenuItem icon={<Copy />} onSelect={onDuplicate}>
            Duplicate
          </MenuItem>
          <MenuItem icon={<Pencil />} onSelect={() => afterMenuClose(onRename)}>
            Rename
          </MenuItem>
          <MenuItem icon={<FolderSymlink />} onSelect={() => afterMenuClose(onMove)}>
            Move to workflow…
          </MenuItem>
          <MenuItem icon={<FileJson />} onSelect={onExport}>
            Export JSON
          </MenuItem>
          <MenuSeparator />
          <MenuItem danger icon={<Trash2 />} onSelect={() => afterMenuClose(onDelete)}>
            Delete
          </MenuItem>
        </MenuContent>
      </Menu>
    </li>
  )
}

interface PipelineRowProps {
  pipeline: Pipeline
  /** Jobs of this workflow, to tell a live stage from a broken reference. */
  jobs: readonly Job[]
  onOpen: () => void
  onRename: () => void
  onDelete: () => void
}

function PipelineRow({ pipeline, jobs, onOpen, onRename, onDelete }: PipelineRowProps) {
  const known = new Set(jobs.map((job) => job.id))
  const broken = pipeline.stages.filter((stage) => !known.has(stage.jobId)).length

  return (
    <li className="flex items-center gap-2 pr-2 transition-colors hover:bg-surface-raised">
      <Link
        to={`/pipelines/${pipeline.id}`}
        className="flex min-w-0 flex-1 items-center gap-3 px-3 py-2.5 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand-500/50"
      >
        <span
          className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg border border-line bg-brand-500/10 text-brand-500"
          aria-hidden
        >
          <ListOrdered className="h-3.5 w-3.5" />
        </span>

        <span className="min-w-0 flex-1">
          <span className="block truncate text-xs font-medium text-content">{pipeline.name}</span>
          <span className="block truncate text-2xs text-content-subtle">
            {pipeline.description || 'Runs its pipelines one after another'}
          </span>
        </span>

        {broken > 0 && (
          <Badge tone="danger">
            {broken} missing {broken === 1 ? 'pipeline' : 'pipelines'}
          </Badge>
        )}

        <span className="w-20 shrink-0 text-right text-2xs tabular-nums text-content-subtle">
          {plural(pipeline.stages.length, 'stage')}
        </span>
        <span className="hidden w-24 shrink-0 text-right text-2xs text-content-subtle sm:block">
          {relativeTime(pipeline.updatedAt)}
        </span>
      </Link>

      <Menu>
        <MenuTrigger asChild>
          <IconButton label={`Actions for ${pipeline.name}`} size="sm">
            <MoreHorizontal />
          </IconButton>
        </MenuTrigger>
        <MenuContent>
          <MenuItem icon={<ArrowRight />} onSelect={onOpen}>
            Open
          </MenuItem>
          <MenuItem icon={<Pencil />} onSelect={() => afterMenuClose(onRename)}>
            Rename
          </MenuItem>
          <MenuSeparator />
          <MenuItem danger icon={<Trash2 />} onSelect={() => afterMenuClose(onDelete)}>
            Delete
          </MenuItem>
        </MenuContent>
      </Menu>
    </li>
  )
}

/* ------------------------------------------------------------------ modals */

function NewJobModal({ workflowId, onClose }: { workflowId: string; onClose: () => void }) {
  const navigate = useNavigate()
  const createJob = useLibraryStore((state) => state.createJob)

  const nameId = useId()
  const [source, setSource] = useState<'blank' | 'template'>('blank')
  const [name, setName] = useState('')
  const [templateId, setTemplateId] = useState(TEMPLATES[0]?.id ?? '')
  const [busy, setBusy] = useState(false)

  const template = TEMPLATES.find((candidate) => candidate.id === templateId)
  const valid = source === 'blank' ? name.trim().length > 0 : Boolean(template)

  const submit = async () => {
    if (!valid || busy) return
    setBusy(true)
    try {
      const job =
        source === 'template' && template
          ? await createJob({
              workflowId,
              name: template.name,
              description: template.summary,
              pipeline: template.pipeline,
            })
          : await createJob({ workflowId, name: name.trim() })
      onClose()
      navigate(`/jobs/${job.id}`)
    } catch (error) {
      setBusy(false)
      toast.error('Could not create the job', {
        description: error instanceof Error ? error.message : String(error),
      })
    }
  }

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title="New job"
      description="Start from an empty canvas, or from a template you can take apart."
      size="lg"
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="primary"
            loading={busy}
            disabled={!valid}
            onClick={() => void submit()}
          >
            Create job
          </Button>
        </>
      }
    >
      <div className="space-y-4">
        <Segmented
          value={source}
          onChange={setSource}
          options={[
            { value: 'blank', label: 'Blank' },
            { value: 'template', label: 'From template' },
          ]}
        />

        {source === 'blank' ? (
          <Field
            label="Name"
            htmlFor={nameId}
            required
            help="You can rename it any time from the editor."
          >
            <Input
              id={nameId}
              autoFocus
              value={name}
              placeholder="Orders to curated"
              onChange={(event) => setName(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === 'Enter') void submit()
              }}
            />
          </Field>
        ) : (
          <ul className="scroll-area max-h-80 space-y-2 pr-1">
            {TEMPLATES.map((candidate) => (
              <li key={candidate.id}>
                <TemplateOption
                  template={candidate}
                  selected={candidate.id === templateId}
                  onSelect={() => setTemplateId(candidate.id)}
                />
              </li>
            ))}
          </ul>
        )}
      </div>
    </Modal>
  )
}

function TemplateOption({
  template,
  selected,
  onSelect,
}: {
  template: JobTemplate
  selected: boolean
  onSelect: () => void
}) {
  return (
    <button
      type="button"
      aria-pressed={selected}
      onClick={onSelect}
      className={cn(
        'w-full rounded-xl border p-3 text-left transition-colors',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/50',
        selected
          ? 'border-brand-500/60 bg-brand-500/5'
          : 'border-line bg-surface hover:border-line-strong hover:bg-surface-raised',
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <span className="truncate text-xs font-medium text-content">{template.name}</span>
        <Badge tone={selected ? 'brand' : 'neutral'}>{template.level}</Badge>
      </div>
      <p className="mt-1 text-2xs leading-relaxed text-content-muted">{template.summary}</p>
      {template.tags.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1">
          {template.tags.map((tag) => (
            <span key={tag} className="chip">
              {tag}
            </span>
          ))}
        </div>
      )}
    </button>
  )
}

/**
 * Creating a pipeline is picking the pipelines and the order they run in: the click
 * order IS the order, and the stages are linked head to tail from it. Everything
 * else — moving boxes, extra links, removing a stage — happens on the canvas.
 */
function NewPipelineModal({
  workflowId,
  jobs,
  onClose,
}: {
  workflowId: string
  jobs: readonly Job[]
  onClose: () => void
}) {
  const navigate = useNavigate()
  const createPipeline = useLibraryStore((state) => state.createPipeline)

  const nameId = useId()
  const [name, setName] = useState('')
  const [picked, setPicked] = useState<string[]>([])
  const [busy, setBusy] = useState(false)

  const toggle = (jobId: string) => {
    setPicked((current) =>
      current.includes(jobId)
        ? current.filter((id) => id !== jobId)
        : [...current, jobId],
    )
  }

  const submit = async () => {
    if (!name.trim() || busy) return
    setBusy(true)
    try {
      const pipeline = await createPipeline({
        workflowId,
        name: name.trim(),
        jobIds: picked,
      })
      onClose()
      navigate(`/pipelines/${pipeline.id}`)
    } catch (error) {
      setBusy(false)
      toast.error('Could not create the pipeline', {
        description: error instanceof Error ? error.message : String(error),
      })
    }
  }

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title="New pipeline"
      description="Chain pipelines of this workflow into one run. Pick them in the order they should execute — you can rewire everything on the canvas afterwards."
      size="lg"
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="primary"
            loading={busy}
            disabled={!name.trim()}
            onClick={() => void submit()}
          >
            Create pipeline
          </Button>
        </>
      }
    >
      <div className="space-y-4">
        <Field label="Name" htmlFor={nameId} required>
          <Input
            id={nameId}
            autoFocus
            value={name}
            placeholder="Nightly load"
            onChange={(event) => setName(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') void submit()
            }}
          />
        </Field>

        <fieldset className="space-y-2">
          <legend className="text-2xs font-semibold uppercase tracking-wider text-content-subtle">
            Stages ({picked.length} picked)
          </legend>
          <ul className="scroll-area max-h-72 space-y-1 pr-1">
            {jobs.map((job) => {
              const position = picked.indexOf(job.id)
              const selected = position >= 0
              return (
                <li key={job.id}>
                  <label
                    className={cn(
                      'flex cursor-pointer items-center gap-2.5 rounded-lg border px-2.5 py-2 transition-colors',
                      selected
                        ? 'border-brand-500/60 bg-brand-500/5'
                        : 'border-line hover:border-line-strong hover:bg-surface-raised',
                    )}
                  >
                    <input
                      type="checkbox"
                      checked={selected}
                      onChange={() => toggle(job.id)}
                      className="h-3.5 w-3.5 accent-brand-500"
                    />
                    <span className="w-5 shrink-0 text-2xs tabular-nums text-content-subtle">
                      {selected ? position + 1 : '—'}
                    </span>
                    <span className="min-w-0 flex-1 truncate text-xs text-content">
                      {job.name}
                    </span>
                    <span className="shrink-0 text-2xs text-content-subtle">
                      {plural(countNodes(job), 'node')}
                    </span>
                  </label>
                </li>
              )
            })}
          </ul>
          <p className="text-2xs text-content-subtle">
            Leave everything unpicked to start from an empty canvas.
          </p>
        </fieldset>
      </div>
    </Modal>
  )
}

function RenamePipelineModal({ pipeline, onClose }: { pipeline: Pipeline; onClose: () => void }) {
  const updatePipelineMeta = useLibraryStore((state) => state.updatePipelineMeta)
  const nameId = useId()
  const [name, setName] = useState(pipeline.name)

  const submit = async () => {
    const trimmed = name.trim()
    if (!trimmed || trimmed === pipeline.name) {
      onClose()
      return
    }
    await updatePipelineMeta(pipeline.id, { name: trimmed })
    onClose()
  }

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title="Rename pipeline"
      size="sm"
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" disabled={!name.trim()} onClick={() => void submit()}>
            Rename
          </Button>
        </>
      }
    >
      <Field label="Name" htmlFor={nameId} required>
        <Input
          id={nameId}
          autoFocus
          value={name}
          onChange={(event) => setName(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') void submit()
          }}
        />
      </Field>
    </Modal>
  )
}

function RenameJobModal({
  job,
  onClose,
}: {
  job: Job
  onClose: () => void
}) {
  const updateJobMeta = useLibraryStore((state) => state.updateJobMeta)
  const nameId = useId()
  const [name, setName] = useState(job.name)

  const submit = async () => {
    const trimmed = name.trim()
    if (!trimmed || trimmed === job.name) {
      onClose()
      return
    }
    await updateJobMeta(job.id, { name: trimmed })
    onClose()
  }

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title="Rename job"
      size="sm"
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" disabled={!name.trim()} onClick={() => void submit()}>
            Rename
          </Button>
        </>
      }
    >
      <Field label="Name" htmlFor={nameId} required>
        <Input
          id={nameId}
          autoFocus
          value={name}
          onChange={(event) => setName(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') void submit()
          }}
        />
      </Field>
    </Modal>
  )
}

function MoveJobModal({ job, onClose }: { job: Job; onClose: () => void }) {
  const workflows = useLibraryStore((state) => state.workflows)
  const updateJobMeta = useLibraryStore((state) => state.updateJobMeta)

  const selectId = useId()
  const others = workflows.filter((workflow) => workflow.id !== job.workflowId)
  const [target, setTarget] = useState(others[0]?.id ?? '')

  const submit = async () => {
    if (!target) return
    await updateJobMeta(job.id, { workflowId: target })
    onClose()
    const name = workflows.find((workflow) => workflow.id === target)?.name
    toast.success(name ? `Moved to ${name}` : 'Job moved')
  }

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title="Move job"
      description={`Pick the workflow "${job.name}" should belong to.`}
      size="sm"
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" disabled={!target} onClick={() => void submit()}>
            Move
          </Button>
        </>
      }
    >
      {others.length === 0 ? (
        <p className="text-xs text-content-muted">
          This is the only workflow. Create another one first from the overview.
        </p>
      ) : (
        <Field label="Workflow" htmlFor={selectId}>
          <Select
            id={selectId}
            value={target}
            onValueChange={setTarget}
            ariaLabel="Workflow"
            options={others.map((workflow) => ({ value: workflow.id, label: workflow.name }))}
          />
        </Field>
      )}
    </Modal>
  )
}

function AccentPicker({
  value,
  onChange,
}: {
  value: WorkflowAccent
  onChange: (accent: WorkflowAccent) => void
}) {
  return (
    <div className="flex items-center gap-1.5">
      {WORKFLOW_ACCENTS.map((accent) => (
        <button
          key={accent}
          type="button"
          aria-label={`${accent} accent`}
          aria-pressed={value === accent}
          onClick={() => onChange(accent)}
          className={cn(
            'flex h-7 w-7 items-center justify-center rounded-lg border transition-colors',
            'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/50',
            value === accent ? 'border-line-strong bg-surface-sunken' : 'border-transparent',
          )}
        >
          <span className={cn('h-3.5 w-3.5 rounded-full', ACCENT_DOT[accent])} />
        </button>
      ))}
    </div>
  )
}

/* ------------------------------------------------------------------- utils */

const viewTabId = (view: ViewKey) => `workflow-view-tab-${view}`
const viewPanelId = (view: ViewKey) => `workflow-view-panel-${view}`

/** Tabs owe the roving-focus contract: arrows move the selection and the focus. */
function onViewTabKeyDown(
  event: ReactKeyboardEvent<HTMLButtonElement>,
  index: number,
  setView: (view: ViewKey) => void,
  tabsRef: { current: HTMLDivElement | null },
): void {
  const delta = event.key === 'ArrowRight' ? 1 : event.key === 'ArrowLeft' ? -1 : 0
  if (delta === 0) return
  event.preventDefault()
  const next = (index + delta + VIEWS.length) % VIEWS.length
  setView(VIEWS[next].id)
  tabsRef.current?.querySelectorAll<HTMLButtonElement>('[role="tab"]')[next]?.focus()
}

/** Sticky notes are canvas annotations, never part of the compiled pipeline. */
function countNodes(job: Job): number {
  return job.graph.nodes.filter((node) => node.data.kind !== 'note').length
}

function matches(job: Job, needle: string): boolean {
  return (
    job.name.toLowerCase().includes(needle) ||
    job.description.toLowerCase().includes(needle) ||
    job.tags.some((tag) => tag.toLowerCase().includes(needle))
  )
}

/** Exports what Sparquet would run, not the canvas — so a broken graph cannot ship. */
function exportJob(job: Job): void {
  const { pipeline, issues } = compileGraph(job.graph, job.settings, job.params)
  if (!pipeline) {
    toast.error('Nothing to export yet', {
      description:
        issues.find((issue) => issue.severity === 'error')?.message ??
        'The job does not compile to a pipeline.',
    })
    return
  }
  downloadText(`${slugify(job.name)}.json`, serializePipeline(pipeline))
  toast.success('Pipeline JSON downloaded')
}

/**
 * Radix restores focus to the trigger while the menu closes; opening a dialog in
 * the same tick makes the two focus traps fight. One macrotask later is enough.
 */
function afterMenuClose(action: () => void): void {
  window.setTimeout(action, 0)
}
