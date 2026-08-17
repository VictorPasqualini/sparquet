import { ArrowRight, Check, Copy, Eye, Search, Sparkles, X } from 'lucide-react'
import { useCallback, useEffect, useId, useMemo, useRef, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { toast } from 'sonner'

import {
  Badge,
  Button,
  EmptyState,
  Field,
  IconButton,
  Input,
  Kbd,
  Modal,
  Segmented,
  Select,
  type BadgeTone,
  type SegmentedOption,
} from '@/components/ui'
import { TEMPLATES } from '@/data/templates'
import { serializePipeline } from '@/lib/compiler'
import { cn } from '@/lib/utils/cn'
import { copyText } from '@/lib/utils/download'
import { useLibraryStore } from '@/store/library'
import type { PipelineSpec } from '@/types/pipeline'
import type { JobTemplate } from '@/types/studio'

type Level = JobTemplate['level']
type LevelFilter = 'all' | Level

const LEVEL_TONE: Record<Level, BadgeTone> = {
  starter: 'success',
  intermediate: 'info',
  advanced: 'warning',
}

const LEVELS: { value: LevelFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'starter', label: 'Starter' },
  { value: 'intermediate', label: 'Intermediate' },
  { value: 'advanced', label: 'Advanced' },
]

/** Target of the create dialog. `template: null` is the blank AI job. */
interface CreateTarget {
  template: JobTemplate | null
}

export function Templates() {
  const location = useLocation()
  const [level, setLevel] = useState<LevelFilter>('all')
  const [query, setQuery] = useState('')
  const [preview, setPreview] = useState<JobTemplate | null>(null)
  const [target, setTarget] = useState<CreateTarget | null>(null)
  const searchRef = useRef<HTMLInputElement>(null)

  // Lessons deep-link here with the template they teach.
  const consumedState = useRef(false)
  useEffect(() => {
    if (consumedState.current) return
    const requested = (location.state as { templateId?: string } | null)?.templateId
    if (!requested) return
    const match = TEMPLATES.find((template) => template.id === requested)
    if (!match) return
    consumedState.current = true
    setTarget({ template: match })
  }, [location.state])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== '/' || event.metaKey || event.ctrlKey || event.altKey) return
      const active = event.target as HTMLElement | null
      if (active?.isContentEditable) return
      if (active && /^(INPUT|TEXTAREA|SELECT)$/.test(active.tagName)) return
      event.preventDefault()
      searchRef.current?.focus()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  const searched = useMemo(
    () => TEMPLATES.filter((template) => matches(template, query)),
    [query],
  )
  const visible = useMemo(
    () => searched.filter((template) => level === 'all' || template.level === level),
    [searched, level],
  )

  const options = useMemo<SegmentedOption<LevelFilter>[]>(
    () =>
      LEVELS.map(({ value, label }) => {
        const count =
          value === 'all'
            ? searched.length
            : searched.filter((template) => template.level === value).length
        return {
          value,
          title: `${count} ${count === 1 ? 'template' : 'templates'}`,
          label: (
            <span className="flex items-center gap-1.5">
              {label}
              <span className="tabular-nums text-content-subtle">{count}</span>
            </span>
          ),
        }
      }),
    [searched],
  )

  const clear = useCallback(() => {
    setQuery('')
    setLevel('all')
    searchRef.current?.focus()
  }, [])

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8 animate-fade-in">
      <header className="mb-6 space-y-1">
        <h1 className="text-sm font-semibold text-content">Templates</h1>
        <p className="max-w-2xl text-xs leading-relaxed text-content-muted">
          Working pipelines you can open, read and edit. Every one compiles to real Sparquet
          JSON, so a template is also the fastest way to learn a feature.
        </p>
      </header>

      <div className="mb-5 flex flex-wrap items-center gap-3">
        <Segmented value={level} onChange={setLevel} options={options} size="sm" />
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
            placeholder="Search name, summary or tag"
            aria-label="Search templates"
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

      {visible.length === 0 ? (
        <div className="card">
          <EmptyState
            icon={<Search />}
            title="No template matches"
            description={`Try a shorter search, or clear the level filter to see all ${TEMPLATES.length} templates.`}
            action={
              <Button size="sm" variant="secondary" onClick={clear}>
                Clear filters
              </Button>
            }
          />
        </div>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {visible.map((template) => (
            <TemplateCard
              key={template.id}
              template={template}
              onPreview={() => setPreview(template)}
              onUse={() => setTarget({ template })}
            />
          ))}
          <AiCard onStart={() => setTarget({ template: null })} />
        </div>
      )}

      {preview && (
        <PreviewDialog
          key={preview.id}
          template={preview}
          onClose={() => setPreview(null)}
          onUse={() => {
            setTarget({ template: preview })
            setPreview(null)
          }}
        />
      )}

      {target && (
        <CreateJobDialog
          key={target.template?.id ?? 'blank'}
          template={target.template}
          onClose={() => setTarget(null)}
        />
      )}
    </div>
  )
}

/* -------------------------------------------------------------------- cards */

interface TemplateCardProps {
  template: JobTemplate
  onPreview: () => void
  onUse: () => void
}

function TemplateCard({ template, onPreview, onUse }: TemplateCardProps) {
  return (
    <article className="card flex flex-col p-4 transition-colors hover:border-line-strong">
      <div className="mb-2 flex items-start justify-between gap-2">
        <h2 className="text-sm font-semibold leading-snug text-content">{template.name}</h2>
        <Badge tone={LEVEL_TONE[template.level]}>{template.level}</Badge>
      </div>

      <p className="text-xs leading-relaxed text-content-muted">{template.summary}</p>

      <ul className="mt-3 space-y-1.5">
        {template.highlights.map((highlight) => (
          <li key={highlight} className="flex items-start gap-1.5">
            <Check className="mt-0.5 h-3 w-3 shrink-0 text-brand-500" aria-hidden />
            <span className="text-2xs leading-relaxed text-content-subtle">{highlight}</span>
          </li>
        ))}
      </ul>

      <ul className="mt-3 flex flex-wrap gap-1">
        {template.tags.map((tag) => (
          <li key={tag} className="chip font-mono">
            {tag}
          </li>
        ))}
      </ul>

      <div className="mt-4 flex items-center gap-2 border-t border-line pt-3">
        <Button size="sm" variant="ghost" icon={<Eye />} onClick={onPreview}>
          Preview
        </Button>
        <Button
          size="sm"
          variant="primary"
          className="ml-auto"
          trailing={<ArrowRight className="h-3.5 w-3.5" />}
          onClick={onUse}
        >
          Use template
        </Button>
      </div>
    </article>
  )
}

function AiCard({ onStart }: { onStart: () => void }) {
  return (
    <button
      type="button"
      onClick={onStart}
      className={cn(
        'flex flex-col items-start rounded-xl border border-dashed border-brand-500/40 bg-brand-500/5 p-4 text-left',
        'transition-colors hover:border-brand-500/70 hover:bg-brand-500/10',
      )}
    >
      <span className="mb-3 flex h-8 w-8 items-center justify-center rounded-lg bg-brand-500/15 text-brand-600 dark:text-brand-400">
        <Sparkles className="h-4 w-4" aria-hidden />
      </span>
      <span className="text-sm font-semibold text-content">Nothing here fits</span>
      <span className="mt-1 text-xs leading-relaxed text-content-muted">
        Describe the pipeline you need in plain language and the assistant drafts the graph for
        you. You start on an empty canvas with the AI panel open.
      </span>
      <span className="mt-auto flex items-center gap-1.5 pt-4 text-xs font-medium text-brand-600 dark:text-brand-400">
        Build one with AI
        <ArrowRight className="h-3.5 w-3.5" aria-hidden />
      </span>
    </button>
  )
}

/* ------------------------------------------------------------------ preview */

interface PreviewDialogProps {
  template: JobTemplate
  onClose: () => void
  onUse: () => void
}

function PreviewDialog({ template, onClose, onUse }: PreviewDialogProps) {
  const json = useMemo(() => templateJson(template), [template])
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    if (!copied) return
    const timer = window.setTimeout(() => setCopied(false), 1400)
    return () => window.clearTimeout(timer)
  }, [copied])

  const copy = useCallback(() => {
    void (async () => {
      if (await copyText(json)) setCopied(true)
      else toast.error('Could not copy to the clipboard')
    })()
  }, [json])

  const lines = json.split('\n').length

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title={template.name}
      description={template.summary}
      size="xl"
      footer={
        <>
          <Button variant="ghost" onClick={onClose}>
            Close
          </Button>
          <Button
            variant="primary"
            trailing={<ArrowRight className="h-3.5 w-3.5" />}
            onClick={onUse}
          >
            Use template
          </Button>
        </>
      }
    >
      <div className="mb-2 flex items-center gap-2">
        <span className="font-mono text-2xs text-content-subtle">pipeline.json</span>
        <span className="text-2xs tabular-nums text-content-subtle">
          · {lines} {lines === 1 ? 'line' : 'lines'}
        </span>
        <IconButton size="sm" label="Copy pipeline JSON" onClick={copy} className="ml-auto">
          {copied ? <Check className="text-state-success" /> : <Copy />}
        </IconButton>
      </div>
      {/* Focusable so the JSON can be scrolled from the keyboard. */}
      <pre
        tabIndex={0}
        aria-label={`${template.name} pipeline JSON`}
        className="scroll-area max-h-[55vh] overpipeline-x-auto rounded-lg border border-line bg-surface-sunken p-3 font-mono text-2xs leading-relaxed text-content-muted"
      >
        {json}
      </pre>
    </Modal>
  )
}

/* ------------------------------------------------------------------- create */

const NEW_WORKFLOW = '__new__'

interface CreateJobDialogProps {
  /** `null` creates a blank job and opens the AI panel. */
  template: JobTemplate | null
  onClose: () => void
}

function CreateJobDialog({ template, onClose }: CreateJobDialogProps) {
  const navigate = useNavigate()
  const workflows = useLibraryStore((state) => state.workflows)
  const createWorkflow = useLibraryStore((state) => state.createWorkflow)
  const createJob = useLibraryStore((state) => state.createJob)

  const formId = useId()
  const workflowFieldId = `${formId}-workflow`
  const workflowNameFieldId = `${formId}-workflow-name`
  const nameFieldId = `${formId}-name`

  const [workflowId, setWorkflowId] = useState(() => workflows[0]?.id ?? NEW_WORKFLOW)
  const [workflowName, setWorkflowName] = useState('My pipelines')
  const [name, setName] = useState(template?.name ?? 'Untitled pipeline')
  const [busy, setBusy] = useState(false)

  const creatingWorkflow = workflowId === NEW_WORKFLOW
  const valid = name.trim().length > 0 && (!creatingWorkflow || workflowName.trim().length > 0)

  const workflowOptions = useMemo(
    () => [
      ...workflows.map((workflow) => ({ value: workflow.id, label: workflow.name })),
      { value: NEW_WORKFLOW, label: 'New workflow…' },
    ],
    [workflows],
  )

  const submit = useCallback(() => {
    if (!valid || busy) return
    setBusy(true)
    void (async () => {
      try {
        const owner = creatingWorkflow
          ? (await createWorkflow({ name: workflowName.trim() })).id
          : workflowId
        const job = await createJob({
          workflowId: owner,
          name: name.trim(),
          description: template?.summary,
          pipeline: template?.pipeline,
        })
        navigate(`/jobs/${job.id}`, {
          state: template ? undefined : { openAi: true },
        })
      } catch (error) {
        setBusy(false)
        toast.error('Could not create the job', {
          description: error instanceof Error ? error.message : String(error),
        })
      }
    })()
  }, [
    valid,
    busy,
    creatingWorkflow,
    createWorkflow,
    workflowName,
    workflowId,
    createJob,
    name,
    template,
    navigate,
  ])

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title={template ? 'Use this template' : 'New job with AI'}
      description={
        template
          ? 'A copy of the template is created — editing it never touches the original.'
          : 'An empty canvas opens with the AI panel ready for your description.'
      }
      size="sm"
      footer={
        <>
          <Button variant="ghost" onClick={onClose} disabled={busy}>
            Cancel
          </Button>
          <Button
            type="submit"
            form={formId}
            variant="primary"
            loading={busy}
            disabled={!valid}
          >
            {template ? 'Create job' : 'Open canvas'}
          </Button>
        </>
      }
    >
      <form
        id={formId}
        className="space-y-3"
        onSubmit={(event) => {
          event.preventDefault()
          submit()
        }}
      >
        <Field label="Workflow" htmlFor={workflowFieldId}>
          <Select
            id={workflowFieldId}
            value={workflowId}
            onValueChange={setWorkflowId}
            options={workflowOptions}
            ariaLabel="Workflow"
          />
        </Field>

        {creatingWorkflow && (
          <Field
            label="New workflow name"
            htmlFor={workflowNameFieldId}
            help="Workflows group related jobs — one per domain works well."
          >
            <Input
              id={workflowNameFieldId}
              value={workflowName}
              onChange={(event) => setWorkflowName(event.target.value)}
              placeholder="My pipelines"
            />
          </Field>
        )}

        <Field label="Job name" htmlFor={nameFieldId} required>
          <Input
            id={nameFieldId}
            autoFocus
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="Orders ingestion"
          />
        </Field>
      </form>
    </Modal>
  )
}

/* ------------------------------------------------------------------ helpers */

/** Templates carry real pipeline JSON; the compiler's serializer keeps key order readable. */
function templateJson(template: JobTemplate): string {
  return serializePipeline(template.pipeline as PipelineSpec)
}

/** Every whitespace-separated term must appear in the name, summary or tags. */
function matches(template: JobTemplate, query: string): boolean {
  const terms = query.toLowerCase().split(/\s+/).filter(Boolean)
  if (terms.length === 0) return true
  const haystack = [template.name, template.summary, ...template.tags].join(' ').toLowerCase()
  return terms.every((term) => haystack.includes(term))
}
