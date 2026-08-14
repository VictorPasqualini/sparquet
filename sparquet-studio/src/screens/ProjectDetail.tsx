import {
  ArrowLeft,
  ArrowRight,
  Copy,
  FileJson,
  FolderSymlink,
  MoreHorizontal,
  Palette,
  Pencil,
  Plus,
  Search,
  Trash2,
  Workflow as WorkflowIcon,
} from 'lucide-react'
import { useEffect, useId, useMemo, useState } from 'react'
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
  Segmented,
  Select,
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
  PROJECT_ACCENTS,
  type ProjectAccent,
  type Workflow,
  type WorkflowTemplate,
} from '@/types/studio'

type SortKey = 'updated' | 'name'

/** Project accents mapped onto the semantic palette — no raw colors anywhere. */
const ACCENT_DOT: Record<ProjectAccent, string> = {
  amber: 'bg-brand-500',
  sky: 'bg-node-input',
  violet: 'bg-node-combine',
  emerald: 'bg-node-output',
  rose: 'bg-state-danger',
  slate: 'bg-node-inspect',
}

export function ProjectDetail() {
  const { projectId } = useParams<{ projectId: string }>()
  const navigate = useNavigate()

  const project = useLibraryStore((state) =>
    state.projects.find((candidate) => candidate.id === projectId),
  )
  const workflows = useLibraryStore((state) => state.workflows)
  const updateProject = useLibraryStore((state) => state.updateProject)
  const deleteProject = useLibraryStore((state) => state.deleteProject)
  const duplicateWorkflow = useLibraryStore((state) => state.duplicateWorkflow)
  const deleteWorkflow = useLibraryStore((state) => state.deleteWorkflow)

  const [confirm, confirmDialog] = useConfirm()
  const [query, setQuery] = useState('')
  const [sort, setSort] = useState<SortKey>('updated')
  const [creating, setCreating] = useState(false)
  const [renaming, setRenaming] = useState<Workflow | null>(null)
  const [moving, setMoving] = useState<Workflow | null>(null)

  const [name, setName] = useState(project?.name ?? '')
  const [description, setDescription] = useState(project?.description ?? '')
  useEffect(() => setName(project?.name ?? ''), [project?.name])
  useEffect(() => setDescription(project?.description ?? ''), [project?.description])

  const owned = useMemo(
    () => workflows.filter((workflow) => workflow.projectId === projectId),
    [workflows, projectId],
  )

  const rows = useMemo(() => {
    const needle = query.trim().toLowerCase()
    const matched = needle ? owned.filter((workflow) => matches(workflow, needle)) : [...owned]
    return matched.sort((a, b) =>
      sort === 'name' ? a.name.localeCompare(b.name) : b.updatedAt - a.updatedAt,
    )
  }, [owned, query, sort])

  const totalNodes = useMemo(
    () => owned.reduce((total, workflow) => total + countNodes(workflow), 0),
    [owned],
  )

  if (!project) {
    return (
      <div className="mx-auto w-full max-w-5xl px-6 py-6">
        <div className="card">
          <EmptyState
            icon={<WorkflowIcon />}
            title="Project not found"
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
    if (!trimmed || trimmed === project.name) {
      setName(project.name)
      return
    }
    void updateProject(project.id, { name: trimmed })
  }

  const commitDescription = () => {
    const trimmed = description.trim()
    if (trimmed === project.description) return
    void updateProject(project.id, { description: trimmed })
  }

  const handleDeleteProject = async () => {
    const confirmed = await confirm({
      title: 'Delete project',
      message: (
        <>
          <span className="font-medium text-content">{project.name}</span> and its{' '}
          {plural(owned.length, 'workflow')} will be removed. This cannot be undone.
        </>
      ),
      confirmLabel: 'Delete project',
    })
    if (!confirmed) return
    await deleteProject(project.id)
    toast.success('Project deleted')
    navigate('/')
  }

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
              className={cn('h-2 w-2 shrink-0 rounded-full', ACCENT_DOT[project.accent])}
              aria-hidden
            />
            <Input
              value={name}
              aria-label="Project name"
              onChange={(event) => setName(event.target.value)}
              onBlur={commitName}
              onKeyDown={(event) => {
                if (event.key === 'Enter') event.currentTarget.blur()
                if (event.key === 'Escape') {
                  setName(project.name)
                  event.currentTarget.blur()
                }
              }}
              className="h-8 max-w-md border-transparent bg-transparent px-2 text-lg font-semibold tracking-tight hover:border-line focus:bg-surface-sunken"
            />
          </div>
          <Textarea
            value={description}
            rows={2}
            aria-label="Project description"
            placeholder="What lives in this project?"
            onChange={(event) => setDescription(event.target.value)}
            onBlur={commitDescription}
            className="max-w-2xl resize-none border-transparent bg-transparent px-2 py-1 text-xs text-content-muted hover:border-line focus:bg-surface-sunken"
          />
          <p className="px-2 text-2xs text-content-subtle">
            {plural(owned.length, 'workflow')} · {plural(totalNodes, 'node')} · updated{' '}
            {relativeTime(project.updatedAt)}
          </p>
        </div>

        <div className="flex items-center gap-1">
          <Popover>
            <PopoverTrigger asChild>
              <IconButton label="Project accent" size="sm">
                <Palette />
              </IconButton>
            </PopoverTrigger>
            <PopoverContent align="end">
              <AccentPicker
                value={project.accent}
                onChange={(accent) => void updateProject(project.id, { accent })}
              />
            </PopoverContent>
          </Popover>
          <IconButton
            label="Delete project"
            size="sm"
            onClick={() => void handleDeleteProject()}
            className="hover:text-state-danger"
          >
            <Trash2 />
          </IconButton>
        </div>
      </header>

      <div className="flex flex-wrap items-center gap-2">
        <Input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search workflows"
          aria-label="Search workflows"
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
        <div className="ml-auto">
          <Button
            size="sm"
            variant="primary"
            icon={<Plus className="h-4 w-4" />}
            onClick={() => setCreating(true)}
          >
            New workflow
          </Button>
        </div>
      </div>

      {owned.length === 0 ? (
        <div className="card">
          <EmptyState
            icon={<WorkflowIcon />}
            title="No workflows in this project"
            description="Start from a blank canvas or pick a template that already wires a source, transformations and a destination."
            action={
              <Button
                size="sm"
                variant="primary"
                icon={<Plus className="h-4 w-4" />}
                onClick={() => setCreating(true)}
              >
                New workflow
              </Button>
            }
          />
        </div>
      ) : rows.length === 0 ? (
        <div className="card">
          <EmptyState
            icon={<Search />}
            title="No matches"
            description={`Nothing in this project matches "${query.trim()}".`}
            action={
              <Button size="sm" onClick={() => setQuery('')}>
                Clear search
              </Button>
            }
          />
        </div>
      ) : (
        <ul className="divide-y divide-line overflow-hidden rounded-xl border border-line bg-surface shadow-card">
          {rows.map((workflow) => (
            <WorkflowRow
              key={workflow.id}
              workflow={workflow}
              onOpen={() => navigate(`/workflows/${workflow.id}`)}
              onDuplicate={() => void handleDuplicate(workflow)}
              onRename={() => setRenaming(workflow)}
              onMove={() => setMoving(workflow)}
              onExport={() => exportWorkflow(workflow)}
              onDelete={() => void handleDelete(workflow)}
            />
          ))}
        </ul>
      )}

      {creating && (
        <NewWorkflowModal projectId={project.id} onClose={() => setCreating(false)} />
      )}
      {renaming && (
        <RenameWorkflowModal workflow={renaming} onClose={() => setRenaming(null)} />
      )}
      {moving && <MoveWorkflowModal workflow={moving} onClose={() => setMoving(null)} />}
      {confirmDialog}
    </div>
  )
}

/* -------------------------------------------------------------------- rows */

interface WorkflowRowProps {
  workflow: Workflow
  onOpen: () => void
  onDuplicate: () => void
  onRename: () => void
  onMove: () => void
  onExport: () => void
  onDelete: () => void
}

function WorkflowRow({
  workflow,
  onOpen,
  onDuplicate,
  onRename,
  onMove,
  onExport,
  onDelete,
}: WorkflowRowProps) {
  return (
    <li className="flex items-center gap-2 pr-2 transition-colors hover:bg-surface-raised">
      <Link
        to={`/workflows/${workflow.id}`}
        className="flex min-w-0 flex-1 items-center gap-3 px-3 py-2.5 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand-500/50"
      >
        <span
          className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg border border-line bg-surface-sunken text-content-subtle"
          aria-hidden
        >
          <WorkflowIcon className="h-3.5 w-3.5" />
        </span>

        <span className="min-w-0 flex-1">
          <span className="block truncate text-xs font-medium text-content">
            {workflow.name}
          </span>
          {workflow.description && (
            <span className="block truncate text-2xs text-content-subtle">
              {workflow.description}
            </span>
          )}
        </span>

        {workflow.tags.length > 0 && (
          <span className="hidden shrink-0 items-center gap-1 lg:flex">
            {workflow.tags.slice(0, 3).map((tag) => (
              <span key={tag} className="chip">
                {tag}
              </span>
            ))}
          </span>
        )}

        <span className="w-20 shrink-0 text-right text-2xs tabular-nums text-content-subtle">
          {plural(countNodes(workflow), 'node')}
        </span>
        <span className="hidden w-24 shrink-0 text-right text-2xs text-content-subtle sm:block">
          {relativeTime(workflow.updatedAt)}
        </span>
      </Link>

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
          <MenuItem icon={<FolderSymlink />} onSelect={() => afterMenuClose(onMove)}>
            Move to project…
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

/* ------------------------------------------------------------------ modals */

function NewWorkflowModal({ projectId, onClose }: { projectId: string; onClose: () => void }) {
  const navigate = useNavigate()
  const createWorkflow = useLibraryStore((state) => state.createWorkflow)

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
      const workflow =
        source === 'template' && template
          ? await createWorkflow({
              projectId,
              name: template.name,
              description: template.summary,
              pipeline: template.pipeline,
            })
          : await createWorkflow({ projectId, name: name.trim() })
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
            Create workflow
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
  template: WorkflowTemplate
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

function MoveWorkflowModal({ workflow, onClose }: { workflow: Workflow; onClose: () => void }) {
  const projects = useLibraryStore((state) => state.projects)
  const updateWorkflowMeta = useLibraryStore((state) => state.updateWorkflowMeta)

  const selectId = useId()
  const others = projects.filter((project) => project.id !== workflow.projectId)
  const [target, setTarget] = useState(others[0]?.id ?? '')

  const submit = async () => {
    if (!target) return
    await updateWorkflowMeta(workflow.id, { projectId: target })
    onClose()
    const name = projects.find((project) => project.id === target)?.name
    toast.success(name ? `Moved to ${name}` : 'Workflow moved')
  }

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title="Move workflow"
      description={`Pick the project "${workflow.name}" should belong to.`}
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
          This is the only project. Create another one first from the overview.
        </p>
      ) : (
        <Field label="Project" htmlFor={selectId}>
          <Select
            id={selectId}
            value={target}
            onValueChange={setTarget}
            ariaLabel="Project"
            options={others.map((project) => ({ value: project.id, label: project.name }))}
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
          <span className={cn('h-3.5 w-3.5 rounded-full', ACCENT_DOT[accent])} />
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

function matches(workflow: Workflow, needle: string): boolean {
  return (
    workflow.name.toLowerCase().includes(needle) ||
    workflow.description.toLowerCase().includes(needle) ||
    workflow.tags.some((tag) => tag.toLowerCase().includes(needle))
  )
}

/** Exports what Sparquet would run, not the canvas — so a broken graph cannot ship. */
function exportWorkflow(workflow: Workflow): void {
  const { pipeline, issues } = compileGraph(workflow.graph, workflow.settings, workflow.params)
  if (!pipeline) {
    toast.error('Nothing to export yet', {
      description:
        issues.find((issue) => issue.severity === 'error')?.message ??
        'The workflow does not compile to a pipeline.',
    })
    return
  }
  downloadText(`${slugify(workflow.name)}.json`, serializePipeline(pipeline))
  toast.success('Pipeline JSON downloaded')
}

/**
 * Radix restores focus to the trigger while the menu closes; opening a dialog in
 * the same tick makes the two focus traps fight. One macrotask later is enough.
 */
function afterMenuClose(action: () => void): void {
  window.setTimeout(action, 0)
}
