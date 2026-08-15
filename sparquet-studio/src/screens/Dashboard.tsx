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
  Workflow as WorkflowIcon,
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
import { SEED_PROJECT_NAME } from '@/lib/storage/seed'
import { cn } from '@/lib/utils/cn'
import { plural, relativeTime } from '@/lib/utils/format'
import { useLibraryStore } from '@/store/library'
import {
  PROJECT_ACCENTS,
  type Project,
  type ProjectAccent,
  type Workflow,
} from '@/types/studio'

const RECENT_LIMIT = 8

/** Project accents mapped onto the semantic palette — no raw colors anywhere. */
const ACCENT: Record<ProjectAccent, { dot: string; tile: string; surface: string }> = {
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
  const projects = useLibraryStore((state) => state.projects)
  const workflows = useLibraryStore((state) => state.workflows)
  const duplicateWorkflow = useLibraryStore((state) => state.duplicateWorkflow)
  const deleteWorkflow = useLibraryStore((state) => state.deleteWorkflow)

  const [confirm, confirmDialog] = useConfirm()
  const [creatingWorkflow, setCreatingWorkflow] = useState(false)
  const [creatingProject, setCreatingProject] = useState(false)
  const [renaming, setRenaming] = useState<Workflow | null>(null)

  const projectsById = useMemo(
    () => new Map(projects.map((project) => [project.id, project])),
    [projects],
  )
  const recent = useMemo(
    () => [...workflows].sort((a, b) => b.updatedAt - a.updatedAt).slice(0, RECENT_LIMIT),
    [workflows],
  )
  const totalNodes = useMemo(
    () => workflows.reduce((total, workflow) => total + countNodes(workflow), 0),
    [workflows],
  )
  const firstRun = useMemo(() => !hasOwnWork(projects, workflows), [projects, workflows])

  const handleDuplicate = async (workflow: Workflow) => {
    const copy = await duplicateWorkflow(workflow.id)
    if (copy) toast.success(`Duplicated as "${copy.name}"`)
  }

  const handleDelete = async (workflow: Workflow) => {
    const confirmed = await confirm({
      title: 'Delete workflow',
      message: (
        <>
          <span className="font-medium text-content">{workflow.name}</span> and its canvas will
          be removed. This cannot be undone.
        </>
      ),
      confirmLabel: 'Delete',
    })
    if (!confirmed) return
    await deleteWorkflow(workflow.id)
    toast.success('Workflow deleted')
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
            onClick={() => setCreatingWorkflow(true)}
          >
            New workflow
          </Button>
        </div>
      </header>

      {firstRun && <GettingStarted onCreateProject={() => setCreatingProject(true)} />}

      <section className="grid gap-3 sm:grid-cols-3">
        <StatTile icon={<FolderKanban />} label="Projects" value={projects.length} />
        <StatTile icon={<WorkflowIcon />} label="Workflows" value={workflows.length} />
        <StatTile
          icon={<Boxes />}
          label="Nodes"
          value={totalNodes}
          hint="across all workflows"
        />
      </section>

      <section className="space-y-3">
        <SectionTitle
          action={
            workflows.length > RECENT_LIMIT ? (
              <span className="text-2xs text-content-subtle">
                Showing {RECENT_LIMIT} of {workflows.length}
              </span>
            ) : undefined
          }
        >
          Recent workflows
        </SectionTitle>

        {recent.length === 0 ? (
          <div className="card">
            <EmptyState
              icon={<WorkflowIcon />}
              title="No workflows yet"
              description="A workflow is one pipeline: a source, the transformations it needs and where the result lands."
              action={
                <Button
                  size="sm"
                  variant="primary"
                  icon={<Plus className="h-4 w-4" />}
                  onClick={() => setCreatingWorkflow(true)}
                >
                  New workflow
                </Button>
              }
            />
          </div>
        ) : (
          <ul className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
            {recent.map((workflow) => (
              <WorkflowCard
                key={workflow.id}
                workflow={workflow}
                project={projectsById.get(workflow.projectId)}
                onOpen={() => navigate(`/workflows/${workflow.id}`)}
                onDuplicate={() => void handleDuplicate(workflow)}
                onRename={() => setRenaming(workflow)}
                onDelete={() => void handleDelete(workflow)}
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
              onClick={() => setCreatingProject(true)}
            >
              New project
            </Button>
          }
        >
          Projects
        </SectionTitle>

        {projects.length === 0 ? (
          <div className="card">
            <EmptyState
              icon={<FolderKanban />}
              title="No projects yet"
              description="Projects group the pipelines of one domain — ingestion, ledger, reporting."
            />
          </div>
        ) : (
          <ul className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
            {projects.map((project) => (
              <ProjectCard
                key={project.id}
                project={project}
                workflowCount={
                  workflows.filter((workflow) => workflow.projectId === project.id).length
                }
              />
            ))}
          </ul>
        )}
      </section>

      {creatingWorkflow && <NewWorkflowModal onClose={() => setCreatingWorkflow(false)} />}
      {creatingProject && <NewProjectModal onClose={() => setCreatingProject(false)} />}
      {renaming && (
        <RenameWorkflowModal workflow={renaming} onClose={() => setRenaming(null)} />
      )}
      {confirmDialog}
    </div>
  )
}

/* ------------------------------------------------------------------ pieces */

function GettingStarted({ onCreateProject }: { onCreateProject: () => void }) {
  const steps = [
    {
      title: 'Create a project',
      body: 'Projects group related pipelines. One per domain keeps names short and search useful.',
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
          <Button size="sm" variant="primary" onClick={onCreateProject}>
            Create a project
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

interface WorkflowCardProps {
  workflow: Workflow
  project?: Project
  onOpen: () => void
  onDuplicate: () => void
  onRename: () => void
  onDelete: () => void
}

function WorkflowCard({
  workflow,
  project,
  onOpen,
  onDuplicate,
  onRename,
  onDelete,
}: WorkflowCardProps) {
  const accent = ACCENT[project?.accent ?? 'slate']
  const tags = workflow.tags.slice(0, 3)

  return (
    <li className="relative">
      <Link
        to={`/workflows/${workflow.id}`}
        className={cn(
          'flex h-full flex-col gap-2 rounded-xl border border-line bg-surface p-3 pr-10 shadow-card',
          'transition-colors hover:border-line-strong hover:bg-surface-raised',
          'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/50',
        )}
      >
        <span className="flex items-center gap-1.5">
          <span className={cn('h-1.5 w-1.5 shrink-0 rounded-full', accent.dot)} aria-hidden />
          <span className="truncate text-2xs text-content-subtle">
            {project?.name ?? 'No project'}
          </span>
        </span>

        <span className="truncate text-sm font-medium text-content" title={workflow.name}>
          {workflow.name}
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
          <span>{plural(countNodes(workflow), 'node')}</span>
          <span aria-hidden>·</span>
          <span>{relativeTime(workflow.updatedAt)}</span>
        </span>
      </Link>

      <div className="absolute right-2 top-2">
        <Menu>
          <MenuTrigger asChild>
            <IconButton label={`Actions for ${workflow.name}`} size="sm">
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

function ProjectCard({ project, workflowCount }: { project: Project; workflowCount: number }) {
  const accent = ACCENT[project.accent]

  return (
    <li>
      <Link
        to={`/projects/${project.id}`}
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
          <span className="truncate text-sm font-medium text-content">{project.name}</span>
        </span>

        {project.description && (
          <span className="line-clamp-2 text-2xs leading-relaxed text-content-muted">
            {project.description}
          </span>
        )}

        <span className="mt-auto flex items-center gap-1.5 pt-1 text-2xs text-content-subtle">
          <span>{plural(workflowCount, 'workflow')}</span>
          <span aria-hidden>·</span>
          <span>{relativeTime(project.updatedAt)}</span>
        </span>
      </Link>
    </li>
  )
}

/* ------------------------------------------------------------------ modals */

const NEW_PROJECT = '__new-project'

function NewWorkflowModal({ onClose }: { onClose: () => void }) {
  const navigate = useNavigate()
  const projects = useLibraryStore((state) => state.projects)
  const createProject = useLibraryStore((state) => state.createProject)
  const createWorkflow = useLibraryStore((state) => state.createWorkflow)

  const nameId = useId()
  const projectId = useId()
  const newProjectId = useId()

  const [name, setName] = useState('')
  const [target, setTarget] = useState(projects[0]?.id ?? NEW_PROJECT)
  const [projectName, setProjectName] = useState('')
  const [busy, setBusy] = useState(false)

  const needsProjectName = target === NEW_PROJECT
  const valid = name.trim().length > 0 && (!needsProjectName || projectName.trim().length > 0)

  const submit = async () => {
    if (!valid || busy) return
    setBusy(true)
    try {
      const owner = needsProjectName
        ? (await createProject({ name: projectName.trim() })).id
        : target
      const workflow = await createWorkflow({ projectId: owner, name: name.trim() })
      onClose()
      navigate(`/workflows/${workflow.id}`)
    } catch (error) {
      setBusy(false)
      toast.error('Could not create the workflow', {
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
      title="New workflow"
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
            placeholder="Orders to curated"
            onChange={(event) => setName(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') void submit()
            }}
          />
        </Field>

        <Field label="Project" htmlFor={projectId}>
          <Select
            id={projectId}
            value={target}
            onValueChange={setTarget}
            ariaLabel="Project"
            options={[
              ...projects.map((project) => ({ value: project.id, label: project.name })),
              { value: NEW_PROJECT, label: 'New project…' },
            ]}
          />
        </Field>

        {needsProjectName && (
          <Field label="Project name" htmlFor={newProjectId} required>
            <Input
              id={newProjectId}
              value={projectName}
              placeholder="Ingestion"
              onChange={(event) => setProjectName(event.target.value)}
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

function NewProjectModal({ onClose }: { onClose: () => void }) {
  const navigate = useNavigate()
  const createProject = useLibraryStore((state) => state.createProject)

  const nameId = useId()
  const [name, setName] = useState('')
  const [accent, setAccent] = useState<ProjectAccent>('amber')
  const [busy, setBusy] = useState(false)

  const submit = async () => {
    if (!name.trim() || busy) return
    setBusy(true)
    const project = await createProject({ name: name.trim(), accent })
    onClose()
    navigate(`/projects/${project.id}`)
  }

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title="New project"
      description="A project groups the pipelines that belong together."
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
            Create project
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

function RenameWorkflowModal({
  workflow,
  onClose,
}: {
  workflow: Workflow
  onClose: () => void
}) {
  const updateWorkflowMeta = useLibraryStore((state) => state.updateWorkflowMeta)
  const nameId = useId()
  const [name, setName] = useState(workflow.name)

  const submit = async () => {
    const trimmed = name.trim()
    if (!trimmed || trimmed === workflow.name) {
      onClose()
      return
    }
    await updateWorkflowMeta(workflow.id, { name: trimmed })
    onClose()
  }

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title="Rename workflow"
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
  value: ProjectAccent
  onChange: (accent: ProjectAccent) => void
}) {
  return (
    <div className="flex items-center gap-1.5">
      {PROJECT_ACCENTS.map((accent) => (
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
function countNodes(workflow: Workflow): number {
  return workflow.graph.nodes.filter((node) => node.data.kind !== 'note').length
}

/**
 * The seeded "Getting Started" project ships template copies. Until the user
 * adds something of their own, the dashboard stays in first-run mode.
 */
function hasOwnWork(projects: Project[], workflows: Workflow[]): boolean {
  const seededProjects = new Set(
    projects
      .filter((project) => project.name === SEED_PROJECT_NAME)
      .map((project) => project.id),
  )
  const templateNames = new Set(TEMPLATES.map((template) => template.name))

  return workflows.some(
    (workflow) => !seededProjects.has(workflow.projectId) || !templateNames.has(workflow.name),
  )
}

/**
 * Radix restores focus to the trigger while the menu closes; opening a dialog in
 * the same tick makes the two focus traps fight. One macrotask later is enough.
 */
function afterMenuClose(action: () => void): void {
  window.setTimeout(action, 0)
}
