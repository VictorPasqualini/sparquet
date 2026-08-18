import {
  ArrowRight,
  Boxes,
  Copy,
  FolderKanban,
  GraduationCap,
  LayoutTemplate,
  MoreHorizontal,
  Pencil,
  Plus,
  Sparkles,
  Trash2,
  Workflow as JobIcon,
} from 'lucide-react'
import { useId, useMemo, useState, type ReactNode } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { toast } from 'sonner'

import {
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
  SectionTitle,
  Select,
  useConfirm,
} from '@/components/ui'
import { TEMPLATES } from '@/data/templates'
import { SEED_WORKFLOW_NAME } from '@/lib/storage/seed'
import { cn } from '@/lib/utils/cn'
import { plural, relativeTime } from '@/lib/utils/format'
import { useLibraryStore } from '@/store/library'
import {
  WORKFLOW_ACCENTS,
  type Workflow,
  type WorkflowAccent,
  type Job,
} from '@/types/studio'

const RECENT_LIMIT = 8

/** Workflow accents mapped onto the semantic palette — no raw colors anywhere. */
const ACCENT: Record<WorkflowAccent, { dot: string; tile: string; surface: string }> = {
  amber: {
    dot: 'bg-brand-500',
    tile: 'bg-brand-500/15 text-brand-600 dark:text-brand-400',
    surface: 'border-brand-500/25 bg-brand-500/5 hover:border-brand-500/50',
  },
  sky: {
    dot: 'bg-node-input',
    tile: 'bg-node-input/15 text-node-input',
    surface: 'border-node-input/25 bg-node-input/5 hover:border-node-input/50',
  },
  violet: {
    dot: 'bg-node-combine',
    tile: 'bg-node-combine/15 text-node-combine',
    surface: 'border-node-combine/25 bg-node-combine/5 hover:border-node-combine/50',
  },
  emerald: {
    dot: 'bg-node-output',
    tile: 'bg-node-output/15 text-node-output',
    surface: 'border-node-output/25 bg-node-output/5 hover:border-node-output/50',
  },
  rose: {
    dot: 'bg-state-danger',
    tile: 'bg-state-danger/15 text-state-danger',
    surface: 'border-state-danger/25 bg-state-danger/5 hover:border-state-danger/50',
  },
  slate: {
    dot: 'bg-node-inspect',
    tile: 'bg-node-inspect/15 text-node-inspect',
    surface: 'border-node-inspect/25 bg-node-inspect/5 hover:border-node-inspect/50',
  },
}

export function Dashboard() {
  const navigate = useNavigate()
  const workflows = useLibraryStore((state) => state.workflows)
  const jobs = useLibraryStore((state) => state.jobs)
  const duplicateJob = useLibraryStore((state) => state.duplicateJob)
  const deleteJob = useLibraryStore((state) => state.deleteJob)

  const [confirm, confirmDialog] = useConfirm()
  const [creatingJob, setCreatingJob] = useState(false)
  const [creatingWorkflow, setCreatingWorkflow] = useState(false)
  const [renaming, setRenaming] = useState<Job | null>(null)

  const workflowsById = useMemo(
    () => new Map(workflows.map((workflow) => [workflow.id, workflow])),
    [workflows],
  )
  const recent = useMemo(
    () => [...jobs].sort((a, b) => b.updatedAt - a.updatedAt).slice(0, RECENT_LIMIT),
    [jobs],
  )
  const totalNodes = useMemo(
    () => jobs.reduce((total, job) => total + countNodes(job), 0),
    [jobs],
  )
  const firstRun = useMemo(() => !hasOwnWork(workflows, jobs), [workflows, jobs])

  const handleDuplicate = async (job: Job) => {
    const copy = await duplicateJob(job.id)
    if (copy) toast.success(`Duplicated as "${copy.name}"`)
  }

  const handleDelete = async (job: Job) => {
    const confirmed = await confirm({
      title: 'Delete job',
      message: (
        <>
          <span className="font-medium text-content">{job.name}</span> and its canvas will
          be removed. This cannot be undone.
        </>
      ),
      confirmLabel: 'Delete',
    })
    if (!confirmed) return
    await deleteJob(job.id)
    toast.success('Job deleted')
  }

  return (
    <div className="mx-auto w-full max-w-7xl space-y-8 px-6 py-6 animate-fade-in">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div className="space-y-1.5">
          <h1 className="text-lg font-semibold tracking-tight text-content">Overview</h1>
          <p className="max-w-2xl text-xs leading-relaxed text-content-muted">
            Sparquet Studio turns a pipeline into a canvas you can read — drop in sources,
            transformations and destinations, and Studio writes the JSON that Sparquet runs.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            size="sm"
            icon={<LayoutTemplate className="h-4 w-4" />}
            onClick={() => navigate('/templates')}
          >
            Start from template
          </Button>
          <Button
            size="sm"
            variant="primary"
            icon={<Plus className="h-4 w-4" />}
            onClick={() => setCreatingJob(true)}
          >
            New job
          </Button>
        </div>
      </header>

      {firstRun && <GettingStarted onCreateWorkflow={() => setCreatingWorkflow(true)} />}

      <section className="grid gap-3 sm:grid-cols-3">
        <StatTile icon={<FolderKanban />} label="Workflows" value={workflows.length} />
        <StatTile icon={<JobIcon />} label="Jobs" value={jobs.length} />
        <StatTile
          icon={<Boxes />}
          label="Nodes"
          value={totalNodes}
          hint="across all jobs"
        />
      </section>

      <section className="space-y-3">
        <SectionTitle
          action={
            jobs.length > RECENT_LIMIT ? (
              <span className="text-2xs text-content-subtle">
                Showing {RECENT_LIMIT} of {jobs.length}
              </span>
            ) : undefined
          }
        >
          Recent jobs
        </SectionTitle>

        {recent.length === 0 ? (
          <div className="card">
            <EmptyState
              icon={<JobIcon />}
              title="No jobs yet"
              description="A job is one pipeline: a source, the transformations it needs and where the result lands."
              action={
                <Button
                  size="sm"
                  variant="primary"
                  icon={<Plus className="h-4 w-4" />}
                  onClick={() => setCreatingJob(true)}
                >
                  New job
                </Button>
              }
            />
          </div>
        ) : (
          <ul className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
            {recent.map((job) => (
              <JobCard
                key={job.id}
                job={job}
                workflow={workflowsById.get(job.workflowId)}
                onOpen={() => navigate(`/jobs/${job.id}`)}
                onDuplicate={() => void handleDuplicate(job)}
                onRename={() => setRenaming(job)}
                onDelete={() => void handleDelete(job)}
              />
            ))}
          </ul>
        )}
      </section>

      <section className="space-y-3">
        <SectionTitle
          action={
            <Button
              size="xs"
              variant="ghost"
              icon={<Plus className="h-3.5 w-3.5" />}
              onClick={() => setCreatingWorkflow(true)}
            >
              New workflow
            </Button>
          }
        >
          Workflows
        </SectionTitle>

        {workflows.length === 0 ? (
          <div className="card">
            <EmptyState
              icon={<FolderKanban />}
              title="No workflows yet"
              description="Workflows group the pipelines of one domain — ingestion, ledger, reporting."
            />
          </div>
        ) : (
          <ul className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
            {workflows.map((workflow) => (
              <WorkflowCard
                key={workflow.id}
                workflow={workflow}
                jobCount={
                  jobs.filter((job) => job.workflowId === workflow.id).length
                }
              />
            ))}
          </ul>
        )}
      </section>

      {creatingJob && <NewJobModal onClose={() => setCreatingJob(false)} />}
      {creatingWorkflow && <NewWorkflowModal onClose={() => setCreatingWorkflow(false)} />}
      {renaming && (
        <RenameJobModal job={renaming} onClose={() => setRenaming(null)} />
      )}
      {confirmDialog}
    </div>
  )
}

/* ------------------------------------------------------------------ pieces */

function GettingStarted({ onCreateWorkflow }: { onCreateWorkflow: () => void }) {
  const steps = [
    {
      title: 'Create a workflow',
      body: 'Workflows group related pipelines. One per domain keeps names short and search useful.',
    },
    {
      title: 'Drop in nodes or ask the AI',
      body: 'Drag a source, a few transformations and a destination onto the canvas — or describe the pipeline and let the assistant draft it.',
    },
    {
      title: 'Run it locally',
      body: 'Point Studio at a local runner and execute the compiled JSON without leaving the canvas.',
    },
  ]

  return (
    <section className="rounded-xl border border-brand-500/30 bg-brand-500/5 p-4 animate-slide-up">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <span className="flex h-7 w-7 items-center justify-center rounded-lg bg-brand-500/15 text-brand-600 dark:text-brand-400">
            <Sparkles className="h-4 w-4" />
          </span>
          <div>
            <p className="text-sm font-medium text-content">
              Your first pipeline in three steps
            </p>
            <p className="text-2xs text-content-muted">
              Everything is stored in this browser — nothing leaves your machine.
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Link
            to="/learn"
            className="inline-flex items-center gap-1.5 rounded-lg px-2 py-1 text-xs text-brand-600 transition-colors hover:bg-brand-500/10 dark:text-brand-400"
          >
            <GraduationCap className="h-3.5 w-3.5" />
            Learn the basics
          </Link>
          <Button size="sm" variant="primary" onClick={onCreateWorkflow}>
            Create a workflow
          </Button>
        </div>
      </div>

      <ol className="mt-4 grid gap-3 sm:grid-cols-3">
        {steps.map((step, index) => (
          <li key={step.title} className="rounded-lg border border-line bg-surface p-3">
            <div className="flex items-center gap-2">
              <span className="flex h-5 w-5 items-center justify-center rounded-full bg-brand-500 text-2xs font-semibold text-black">
                {index + 1}
              </span>
              <p className="text-xs font-medium text-content">{step.title}</p>
            </div>
            <p className="mt-1.5 text-2xs leading-relaxed text-content-muted">{step.body}</p>
          </li>
        ))}
      </ol>
    </section>
  )
}

function StatTile({
  icon,
  label,
  value,
  hint,
}: {
  icon: ReactNode
  label: string
  value: number
  hint?: string
}) {
  return (
    <div className="card flex items-center gap-3 p-3">
      <span className="flex h-9 w-9 items-center justify-center rounded-lg bg-surface-sunken text-content-subtle [&_svg]:h-4 [&_svg]:w-4">
        {icon}
      </span>
      <div className="min-w-0">
        <p className="text-sm font-semibold tabular-nums text-content">{value}</p>
        <p className="truncate text-2xs text-content-subtle">
          {label}
          {hint && <span className="text-content-subtle"> · {hint}</span>}
        </p>
      </div>
    </div>
  )
}

interface JobCardProps {
  job: Job
  workflow?: Workflow
  onOpen: () => void
  onDuplicate: () => void
  onRename: () => void
  onDelete: () => void
}

function JobCard({
  job,
  workflow,
  onOpen,
  onDuplicate,
  onRename,
  onDelete,
}: JobCardProps) {
  const accent = ACCENT[workflow?.accent ?? 'slate']
  const tags = job.tags.slice(0, 3)

  return (
    <li className="relative">
      <Link
        to={`/jobs/${job.id}`}
        className={cn(
          'flex h-full flex-col gap-2 rounded-xl border border-line bg-surface p-3 pr-10 shadow-card',
          'transition-colors hover:border-line-strong hover:bg-surface-raised',
          'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/50',
        )}
      >
        <span className="flex items-center gap-1.5">
          <span className={cn('h-1.5 w-1.5 shrink-0 rounded-full', accent.dot)} aria-hidden />
          <span className="truncate text-2xs text-content-subtle">
            {workflow?.name ?? 'No workflow'}
          </span>
        </span>

        <span className="truncate text-sm font-medium text-content" title={job.name}>
          {job.name}
        </span>

        {tags.length > 0 && (
          <span className="flex flex-wrap gap-1">
            {tags.map((tag) => (
              <span key={tag} className="chip">
                {tag}
              </span>
            ))}
          </span>
        )}

        <span className="mt-auto flex items-center gap-1.5 pt-1 text-2xs text-content-subtle">
          <span>{plural(countNodes(job), 'node')}</span>
          <span aria-hidden>·</span>
          <span>{relativeTime(job.updatedAt)}</span>
        </span>
      </Link>

      <div className="absolute right-2 top-2">
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
            <MenuSeparator />
            <MenuItem danger icon={<Trash2 />} onSelect={() => afterMenuClose(onDelete)}>
              Delete
            </MenuItem>
          </MenuContent>
        </Menu>
      </div>
    </li>
  )
}

function WorkflowCard({ workflow, jobCount }: { workflow: Workflow; jobCount: number }) {
  const accent = ACCENT[workflow.accent]

  return (
    <li>
      <Link
        to={`/workflows/${workflow.id}`}
        className={cn(
          'flex h-full flex-col gap-2 rounded-xl border p-3 transition-colors',
          'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/50',
          accent.surface,
        )}
      >
        <span className="flex items-center gap-2">
          <span
            className={cn('flex h-7 w-7 items-center justify-center rounded-lg', accent.tile)}
            aria-hidden
          >
            <FolderKanban className="h-3.5 w-3.5" />
          </span>
          <span className="truncate text-sm font-medium text-content">{workflow.name}</span>
        </span>

        {workflow.description && (
          <span className="line-clamp-2 text-2xs leading-relaxed text-content-muted">
            {workflow.description}
          </span>
        )}

        <span className="mt-auto flex items-center gap-1.5 pt-1 text-2xs text-content-subtle">
          <span>{plural(jobCount, 'job')}</span>
          <span aria-hidden>·</span>
          <span>{relativeTime(workflow.updatedAt)}</span>
        </span>
      </Link>
    </li>
  )
}

/* ------------------------------------------------------------------ modals */

const NEW_WORKFLOW = '__new-workflow'

function NewJobModal({ onClose }: { onClose: () => void }) {
  const navigate = useNavigate()
  const workflows = useLibraryStore((state) => state.workflows)
  const createWorkflow = useLibraryStore((state) => state.createWorkflow)
  const createJob = useLibraryStore((state) => state.createJob)

  const nameId = useId()
  const workflowId = useId()
  const newWorkflowId = useId()

  const [name, setName] = useState('')
  const [target, setTarget] = useState(workflows[0]?.id ?? NEW_WORKFLOW)
  const [workflowName, setWorkflowName] = useState('')
  const [busy, setBusy] = useState(false)

  const needsWorkflowName = target === NEW_WORKFLOW
  const valid = name.trim().length > 0 && (!needsWorkflowName || workflowName.trim().length > 0)

  const submit = async () => {
    if (!valid || busy) return
    setBusy(true)
    try {
      const owner = needsWorkflowName
        ? (await createWorkflow({ name: workflowName.trim() })).id
        : target
      const job = await createJob({ workflowId: owner, name: name.trim() })
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
      description="Starts on an empty canvas. To begin from a worked example, use Start from template."
      size="md"
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
        <Field label="Name" htmlFor={nameId} required>
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

        <Field label="Workflow" htmlFor={workflowId}>
          <Select
            id={workflowId}
            value={target}
            onValueChange={setTarget}
            ariaLabel="Workflow"
            options={[
              ...workflows.map((workflow) => ({ value: workflow.id, label: workflow.name })),
              { value: NEW_WORKFLOW, label: 'New workflow…' },
            ]}
          />
        </Field>

        {needsWorkflowName && (
          <Field label="Workflow name" htmlFor={newWorkflowId} required>
            <Input
              id={newWorkflowId}
              value={workflowName}
              placeholder="Ingestion"
              onChange={(event) => setWorkflowName(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === 'Enter') void submit()
              }}
            />
          </Field>
        )}
      </div>
    </Modal>
  )
}

function NewWorkflowModal({ onClose }: { onClose: () => void }) {
  const navigate = useNavigate()
  const createWorkflow = useLibraryStore((state) => state.createWorkflow)

  const nameId = useId()
  const [name, setName] = useState('')
  const [accent, setAccent] = useState<WorkflowAccent>('amber')
  const [busy, setBusy] = useState(false)

  const submit = async () => {
    if (!name.trim() || busy) return
    setBusy(true)
    const workflow = await createWorkflow({ name: name.trim(), accent })
    onClose()
    navigate(`/workflows/${workflow.id}`)
  }

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title="New workflow"
      description="A workflow groups the pipelines that belong together."
      size="sm"
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
            Create workflow
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
            placeholder="Ingestion"
            onChange={(event) => setName(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') void submit()
            }}
          />
        </Field>
        <Field label="Accent">
          <AccentPicker value={accent} onChange={setAccent} />
        </Field>
      </div>
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
          <span className={cn('h-3.5 w-3.5 rounded-full', ACCENT[accent].dot)} />
        </button>
      ))}
    </div>
  )
}

/* ------------------------------------------------------------------- utils */

/** Sticky notes are canvas annotations, never part of the compiled pipeline. */
function countNodes(job: Job): number {
  return job.graph.nodes.filter((node) => node.data.kind !== 'note').length
}

/**
 * The seeded "Getting Started" workflow ships template copies. Until the user
 * adds something of their own, the dashboard stays in first-run mode.
 */
function hasOwnWork(workflows: Workflow[], jobs: Job[]): boolean {
  const seededWorkflows = new Set(
    workflows
      .filter((workflow) => workflow.name === SEED_WORKFLOW_NAME)
      .map((workflow) => workflow.id),
  )
  const templateNames = new Set(TEMPLATES.map((template) => template.name))

  return jobs.some(
    (job) => !seededWorkflows.has(job.workflowId) || !templateNames.has(job.name),
  )
}

/**
 * Radix restores focus to the trigger while the menu closes; opening a dialog in
 * the same tick makes the two focus traps fight. One macrotask later is enough.
 */
function afterMenuClose(action: () => void): void {
  window.setTimeout(action, 0)
}
