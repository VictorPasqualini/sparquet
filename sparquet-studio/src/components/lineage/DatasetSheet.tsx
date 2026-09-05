/**
 * One dataset, opened: what the pipelines say about it, and what a person says.
 *
 * The top half is derived and read-only — address, placement, formats, the Jobs
 * on each side. The bottom half is the catalog entry, and it is the only part
 * anybody has to type, because it is the only part no pipeline can tell us.
 */

import { ChevronRight, RefreshCw, Trash2, X } from 'lucide-react'
import { Fragment, useEffect, useMemo, useState } from 'react'

import {
  Badge,
  Button,
  Field,
  IconButton,
  Input,
  Modal,
  Select,
  Textarea,
  type BadgeTone,
  type SelectOption,
} from '@/components/ui'
import { addTag, hasTag, MAX_TAG_LENGTH, MAX_TAGS, removeTag } from '@/lib/tags'
import {
  CLASSIFICATIONS,
  compareSchema,
  dedupeSteps,
  dedupeUses,
  impactOf,
  MAX_DESCRIPTION,
  originsOf,
  type ColumnGraph,
  type ColumnStep,
  type DataClassification,
  type DatasetAnnotation,
  type DatasetSchema,
  type DriftRow,
  type DriftStatus,
  type FieldOrigin,
  type ProbedField,
  type SchemaConfidence,
} from '@/lib/datacatalog'
import type { DatasetMention, DatasetPlace, LineageDataset } from '@/lib/lineage'

const PLACE_LABEL: Record<DatasetPlace, string> = {
  external: 'External',
  intermediate: 'Handoff',
  terminal: 'Terminal',
  isolated: 'Isolated',
}

const PLACE_TONE: Record<DatasetPlace, BadgeTone> = {
  external: 'info',
  intermediate: 'brand',
  terminal: 'success',
  isolated: 'warning',
}

const CLASSIFICATION_HINT: Record<DataClassification, string> = {
  public: 'May leave the company.',
  internal: 'Everyone inside, nobody outside.',
  confidential: 'Named teams only.',
  restricted: 'Personal or regulated data — access is granted one person at a time.',
}

const CLASSIFICATION_OPTIONS: SelectOption[] = [
  { value: '', label: 'Not classified' },
  ...CLASSIFICATIONS.map((value) => ({
    value,
    label: value,
    hint: CLASSIFICATION_HINT[value],
  })),
]

/**
 * Where a column came from, in the words of the step that put it there.
 *
 * The origin is what makes a derived schema honest: `cast` and `projected`
 * were written down by a person, `computed` and `aggregated` are expressions
 * whose type only Spark knows, and `required` is a column a validation rule
 * insists on without ever saying what it holds.
 */
const ORIGIN_LABEL: Record<FieldOrigin, string> = {
  projected: 'selected',
  cast: 'cast',
  computed: 'expression',
  grouped: 'group key',
  aggregated: 'aggregate',
  joined: 'from the join',
  quality: 'data quality',
  required: 'required by a rule',
}

const CONFIDENCE_LABEL: Record<SchemaConfidence, string> = {
  complete: 'complete',
  partial: 'partial',
  unknown: 'unknown',
}

const CONFIDENCE_TONE: Record<SchemaConfidence, BadgeTone> = {
  complete: 'success',
  partial: 'warning',
  unknown: 'neutral',
}

const CONFIDENCE_HINT: Record<SchemaConfidence, string> = {
  complete: 'Every column is accounted for: the chain states the whole projection.',
  partial: 'These columns are proven; the source supplies the others at runtime.',
  unknown: 'Nothing in the canvas names a column of this dataset.',
}

const DRIFT_TONE: Record<DriftStatus, BadgeTone> = {
  match: 'success',
  type: 'danger',
  missing: 'danger',
  extra: 'warning',
  unstated: 'neutral',
}

const DRIFT_LABEL: Record<DriftStatus, string> = {
  match: 'matches storage',
  type: 'type differs',
  missing: 'not in storage',
  extra: 'not in the canvas',
  unstated: 'no type stated',
}

/** A format that can actually be opened — a view lives only inside a run. */
function probableFormat(formats: readonly string[]): string | null {
  return formats.find((format) => format !== 'view') ?? null
}

/** One trail of column hops, nearest first, as the sheet shows them. */
function TrailList({
  title,
  empty,
  steps,
  endOf,
  onOpenJob,
}: {
  title: string
  empty: string
  steps: readonly ColumnStep[]
  endOf: (step: ColumnStep) => { key: string; column: string }
  onOpenJob: (jobId: string) => void
}) {
  return (
    <div className="min-w-0 flex-1 space-y-1">
      <p className="text-[11px] font-semibold uppercase tracking-wide text-content-subtle">
        {title}
      </p>
      {steps.length === 0 ? (
        <p className="text-2xs text-content-subtle">{empty}</p>
      ) : (
        <ul className="space-y-1">
          {steps.map((step, index) => {
            const end = endOf(step)
            return (
              <li key={`${step.link.jobId}-${step.link.nodeId}-${end.key}-${end.column}-${index}`}>
                <button
                  type="button"
                  onClick={() => onOpenJob(step.link.jobId)}
                  className="block w-full rounded px-1 py-0.5 text-left transition
                    hover:bg-surface-raised"
                  title={`Stated by ${step.link.jobName}. Open the Job.`}
                >
                  <span className="block break-words font-mono text-2xs text-content">
                    {end.key}.{end.column}
                  </span>
                  <span className="block break-words text-2xs text-content-subtle">
                    {step.link.kind} · {step.link.jobName}
                    {step.depth > 1 ? ` · ${step.depth} hops` : ''}
                  </span>
                </button>
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}

/**
 * Where one column comes from, what it feeds, and who merely names it.
 *
 * The three answers are kept apart on purpose: a value path and a mention break
 * differently. Renaming a column breaks both; changing its type breaks only what
 * reads the value.
 */
function ColumnDetail({
  graph,
  datasetKey,
  column,
  onOpenJob,
}: {
  graph: ColumnGraph
  datasetKey: string
  column: string
  onOpenJob: (jobId: string) => void
}) {
  const ref = { key: datasetKey, column }
  const impact = impactOf(graph, ref)
  // The graph states a hop once per node that makes it; the reader wants the
  // distinct answers, not the count of how many nodes agree.
  const origins = dedupeSteps(originsOf(graph, ref), (step) => step.link.from)
  const feeds = dedupeSteps(impact.downstream, (step) => step.link.to)
  const named = dedupeUses(impact.uses)

  return (
    <div className="space-y-2 rounded-md border border-line/60 bg-surface p-2">
      <div className="flex flex-col gap-3 sm:flex-row">
        <TrailList
          title="Comes from"
          empty="Nothing upstream states it."
          steps={origins}
          endOf={(step) => step.link.from}
          onOpenJob={onOpenJob}
        />
        <TrailList
          title="Feeds"
          empty="No downstream column reads it."
          steps={feeds}
          endOf={(step) => step.link.to}
          onOpenJob={onOpenJob}
        />
      </div>
      <div className="space-y-1">
        <p className="text-[11px] font-semibold uppercase tracking-wide text-content-subtle">
          Named by
        </p>
        {named.length === 0 ? (
          <p className="text-2xs text-content-subtle">No step names it directly.</p>
        ) : (
          <div className="flex flex-wrap gap-1">
            {named.map((use, index) => (
              <button
                key={`${use.jobId}-${use.nodeId}-${use.step}-${index}`}
                type="button"
                onClick={() => onOpenJob(use.jobId)}
                className="rounded border border-line px-1.5 py-0.5 text-2xs text-content-muted
                  transition hover:bg-surface-raised hover:text-content"
                title={`${use.jobName}: ${use.step} (${use.role})`}
              >
                {use.jobName} · {use.step}
              </button>
            ))}
          </div>
        )}
      </div>
      {impact.truncated ? (
        <p className="text-2xs text-content-subtle">
          The trail is longer than this: it was cut at the depth limit.
        </p>
      ) : null}
    </div>
  )
}

/**
 * The columns, read off the canvas — and, on request, off the storage itself.
 *
 * The derived side is what the Jobs state out loud, which is why a type can be
 * missing while the column is not. The probed side is what the runner found when
 * it opened the dataset; when both are there, every row says whether they agree.
 */
function SchemaTable({
  schema,
  datasetKey,
  formats,
  columns,
  probe,
  onOpenJob,
}: {
  schema: DatasetSchema | null
  datasetKey: string
  formats: readonly string[]
  columns: ColumnGraph
  /** Opens the dataset on the runner. Absent when no runner is configured. */
  probe: ((format: string) => Promise<ProbedField[]>) | null
  onOpenJob: (jobId: string) => void
}) {
  const [probed, setProbed] = useState<ProbedField[] | null>(null)
  const [probing, setProbing] = useState(false)
  const [failure, setFailure] = useState<string | null>(null)
  const [open, setOpen] = useState<string | null>(null)
  const format = probableFormat(formats)

  // A different dataset in the same modal was never probed.
  useEffect(() => {
    setProbed(null)
    setFailure(null)
    setOpen(null)
  }, [datasetKey])

  const drift = useMemo(() => (probed ? compareSchema(schema, probed) : null), [schema, probed])

  const run = async () => {
    if (!probe || !format) return
    setProbing(true)
    setFailure(null)
    try {
      setProbed(await probe(format))
    } catch (error) {
      setProbed(null)
      setFailure(error instanceof Error ? error.message : String(error))
    } finally {
      setProbing(false)
    }
  }

  const rows: DriftRow[] = drift
    ? drift.rows
    : (schema?.fields ?? []).map((field) => ({
        name: field.name,
        derived: field.type,
        actual: null,
        status: 'unstated' as DriftStatus,
        nullable: null,
        origin: field.origin,
      }))
  const noteOf = (name: string) => schema?.fields.find((field) => field.name === name)?.note ?? null

  return (
    <section className="space-y-2 overflow-hidden rounded-lg border border-line bg-surface-sunken p-3">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5">
        <p className="text-[11px] font-semibold uppercase tracking-wide text-content-subtle">
          Schema
        </p>
        {schema ? (
          <span title={CONFIDENCE_HINT[schema.confidence]}>
            <Badge tone={CONFIDENCE_TONE[schema.confidence]}>
              {CONFIDENCE_LABEL[schema.confidence]}
            </Badge>
          </span>
        ) : null}
        <span className="text-xs text-content-subtle">
          {rows.length} {rows.length === 1 ? 'column' : 'columns'}
        </span>
        {drift ? (
          <Badge tone={drift.drifted ? 'danger' : 'success'}>
            {drift.drifted
              ? `${drift.type + drift.missing + (drift.complete ? drift.extra : 0)} drifted`
              : 'storage agrees'}
          </Badge>
        ) : null}
        <div className="ml-auto flex items-center gap-1">
          {schema?.source ? (
            <button
              type="button"
              onClick={() => onOpenJob(schema.source!.jobId)}
              className="max-w-[45%] truncate rounded px-1 py-0.5 text-xs text-content-muted
                transition hover:bg-surface-raised hover:text-content"
              title={
                schema.source.side === 'written'
                  ? 'Read off the chain that writes this dataset.'
                  : 'Read off what this Job demands of the dataset.'
              }
            >
              from {schema.source.jobName}
            </button>
          ) : null}
          {probe && format ? (
            <Button
              size="sm"
              variant="secondary"
              loading={probing}
              onClick={() => void run()}
              title={`Open this dataset as ${format} on the runner and read its schema. No rows are read.`}
            >
              <RefreshCw />
              {probed ? 'Read again' : 'Read from storage'}
            </Button>
          ) : null}
        </div>
      </div>

      {failure ? (
        <p className="break-words rounded border border-danger/40 bg-danger/10 px-2 py-1 text-2xs text-danger">
          {failure}
        </p>
      ) : null}

      <table className="w-full table-fixed text-xs">
        <thead>
          <tr className="text-left text-[11px] uppercase tracking-wide text-content-subtle">
            <th className="w-[36%] py-1 pr-2 font-medium">Column</th>
            <th className="w-[24%] py-1 pr-2 font-medium">Type</th>
            {drift ? <th className="w-[22%] py-1 pr-2 font-medium">Storage</th> : null}
            <th className={`${drift ? 'w-[18%]' : 'w-[40%]'} py-1 font-medium`}>
              {drift ? 'Drift' : 'Origin'}
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const note = noteOf(row.name)
            const expanded = open === row.name
            return (
              <Fragment key={row.name}>
                <tr className="border-t border-line/60 align-top">
                  <td className="py-1 pr-2">
                    <button
                      type="button"
                      onClick={() => setOpen(expanded ? null : row.name)}
                      className="flex w-full items-start gap-1 rounded text-left transition
                        hover:bg-surface-raised"
                      title="Where this column comes from, and what it feeds."
                    >
                      <ChevronRight
                        className={`mt-0.5 size-3 shrink-0 text-content-subtle transition ${
                          expanded ? 'rotate-90' : ''
                        }`}
                      />
                      <span className="block break-words font-mono text-content">{row.name}</span>
                    </button>
                  </td>
                  <td className="py-1 pr-2">
                    {row.derived ? (
                      <span className="block break-words font-mono text-content-muted">
                        {row.derived}
                      </span>
                    ) : (
                      <span className="text-content-subtle">not stated</span>
                    )}
                  </td>
                  {drift ? (
                    <td className="py-1 pr-2">
                      {row.actual ? (
                        <span className="block break-words font-mono text-content-muted">
                          {row.actual}
                          {row.nullable === false ? (
                            <span className="text-content-subtle"> not null</span>
                          ) : null}
                        </span>
                      ) : (
                        <span className="text-content-subtle">absent</span>
                      )}
                    </td>
                  ) : null}
                  <td className="py-1 text-content-subtle">
                    {drift ? (
                      <Badge tone={DRIFT_TONE[row.status]}>{DRIFT_LABEL[row.status]}</Badge>
                    ) : (
                      <span className="block">{row.origin ? ORIGIN_LABEL[row.origin] : '—'}</span>
                    )}
                    {note && !drift ? (
                      <span className="mt-0.5 block break-words font-mono text-2xs text-content-subtle/80">
                        {note}
                      </span>
                    ) : null}
                  </td>
                </tr>
                {expanded ? (
                  <tr className="border-t border-line/40">
                    <td colSpan={drift ? 4 : 3} className="px-1 py-2">
                      <ColumnDetail
                        graph={columns}
                        datasetKey={datasetKey}
                        column={row.name}
                        onOpenJob={onOpenJob}
                      />
                    </td>
                  </tr>
                ) : null}
              </Fragment>
            )
          })}
        </tbody>
      </table>

      {drift && !drift.complete && drift.extra > 0 ? (
        <p className="pt-1 text-2xs leading-relaxed text-content-subtle">
          {drift.extra} column{drift.extra === 1 ? '' : 's'} in storage no Job talks about. The
          derived schema is partial, so that is expected rather than a finding.
        </p>
      ) : null}
      {schema && schema.confidence !== 'complete' && !drift ? (
        <p className="pt-1 text-2xs leading-relaxed text-content-subtle">
          {CONFIDENCE_HINT[schema.confidence]}
        </p>
      ) : null}
    </section>
  )
}

/** The form's own copy of an annotation, so typing does not write on every key. */
interface Draft {
  description: string
  owner: string
  domain: string
  classification: DataClassification | ''
  tags: string[]
}

function draftOf(annotation: DatasetAnnotation | null): Draft {
  return {
    description: annotation?.description ?? '',
    owner: annotation?.owner ?? '',
    domain: annotation?.domain ?? '',
    classification: annotation?.classification ?? '',
    tags: annotation?.tags ?? [],
  }
}

function sameDraft(a: Draft, b: Draft): boolean {
  return (
    a.description === b.description &&
    a.owner === b.owner &&
    a.domain === b.domain &&
    a.classification === b.classification &&
    a.tags.length === b.tags.length &&
    a.tags.every((tag, index) => tag === b.tags[index])
  )
}

function MentionList({
  title,
  empty,
  mentions,
  onOpenJob,
}: {
  title: string
  empty: string
  mentions: DatasetMention[]
  onOpenJob: (jobId: string) => void
}) {
  return (
    <div className="min-w-0 flex-1">
      <p className="mb-1 text-[11px] font-semibold uppercase tracking-wide text-content-subtle">
        {title}
      </p>
      {mentions.length === 0 ? (
        <p className="text-xs text-content-subtle">{empty}</p>
      ) : (
        <ul className="space-y-0.5">
          {mentions.map((mention) => (
            <li key={`${mention.jobId}:${mention.nodeId}`}>
              <button
                type="button"
                onClick={() => onOpenJob(mention.jobId)}
                className="w-full truncate rounded px-1 py-0.5 text-left text-xs text-content-muted
                  transition hover:bg-surface-raised hover:text-content"
              >
                {mention.jobName}
                <span className="ml-1.5 text-[11px] text-content-subtle">{mention.role}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

/**
 * Tags on a dataset.
 *
 * Not the library's `TagsPopover`: that control explains that tags drive
 * billing, which is true of a Job and false of a table. Same storage rules
 * (`addTag`/`removeTag`), different thing being labelled.
 */
function TagBox({
  tags,
  suggestions,
  onChange,
}: {
  tags: string[]
  suggestions: string[]
  onChange: (tags: string[]) => void
}) {
  const [draft, setDraft] = useState('')
  const unused = suggestions.filter((tag) => !hasTag(tags, tag)).slice(0, 8)

  const commit = (value: string) => {
    const next = addTag(tags, value)
    if (next !== tags) onChange(next)
    setDraft('')
  }

  return (
    <div className="space-y-1.5">
      <div className="flex flex-wrap items-center gap-1">
        {tags.map((tag) => (
          <Badge key={tag} tone="neutral" className="pr-0.5">
            {tag}
            <IconButton size="xs" label={`Remove ${tag}`} onClick={() => onChange(removeTag(tags, tag))}>
              <X />
            </IconButton>
          </Badge>
        ))}
      </div>
      <Input
        value={draft}
        onChange={(event) => setDraft(event.target.value.slice(0, MAX_TAG_LENGTH))}
        onKeyDown={(event) => {
          if (event.key === 'Enter' || event.key === ',') {
            event.preventDefault()
            commit(draft)
            return
          }
          if (event.key === 'Backspace' && !draft && tags.length) onChange(tags.slice(0, -1))
        }}
        onBlur={() => commit(draft)}
        disabled={tags.length >= MAX_TAGS}
        aria-label="Add a tag to this dataset"
        placeholder={tags.length >= MAX_TAGS ? `${MAX_TAGS} tags is the limit` : 'pii, daily, core'}
        className="h-8"
      />
      {unused.length > 0 && tags.length < MAX_TAGS ? (
        <div className="flex flex-wrap gap-1">
          {unused.map((tag) => (
            <button
              key={tag}
              type="button"
              onClick={() => commit(tag)}
              className="rounded-full border border-line px-2 py-0.5 text-2xs text-content-muted
                hover:border-brand-400 hover:text-content"
            >
              {tag}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  )
}

export interface DatasetSheetProps {
  dataset: LineageDataset | null
  annotation: DatasetAnnotation | null
  /** Name of each workflow that touches it, already resolved by the screen. */
  workflowNames: string[]
  /** Tags already used elsewhere in the catalog. */
  suggestions: string[]
  /** Columns derived from the canvas, when any Job states one. */
  schema: DatasetSchema | null
  /** Column-level lineage of the whole library, for the per-column trails. */
  columns: ColumnGraph
  /**
   * Opens the dataset on the runner and returns the schema it really has. Null
   * when no runner is configured, and the sheet then shows only what it derived.
   */
  probe: ((format: string) => Promise<ProbedField[]>) | null
  onClose: () => void
  onSave: (patch: Partial<DatasetAnnotation>) => Promise<void>
  onForget: () => Promise<void>
  onOpenJob: (jobId: string) => void
}

export function DatasetSheet({
  dataset,
  annotation,
  workflowNames,
  suggestions,
  schema,
  columns,
  probe,
  onClose,
  onSave,
  onForget,
  onOpenJob,
}: DatasetSheetProps) {
  const [draft, setDraft] = useState<Draft>(() => draftOf(annotation))
  const [saving, setSaving] = useState(false)

  // A different dataset in the same modal is a different form.
  useEffect(() => {
    setDraft(draftOf(annotation))
  }, [dataset?.key, annotation])

  const dirty = useMemo(() => !sameDraft(draft, draftOf(annotation)), [draft, annotation])

  if (!dataset) return null

  const save = async () => {
    setSaving(true)
    try {
      await onSave(draft)
      onClose()
    } finally {
      setSaving(false)
    }
  }

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title={dataset.key}
      description="What the Jobs say about this dataset, and what your team says."
      size="lg"
      footer={
        <div className="flex w-full items-center gap-2">
          {annotation ? (
            <Button
              size="sm"
              variant="ghost"
              onClick={() => void onForget().then(onClose)}
              title="Delete this catalog entry. The dataset stays; only the description goes."
            >
              <Trash2 />
              Remove entry
            </Button>
          ) : null}
          <div className="ml-auto flex items-center gap-2">
            <Button size="sm" variant="secondary" onClick={onClose}>
              Cancel
            </Button>
            <Button size="sm" onClick={() => void save()} loading={saving} disabled={!dirty}>
              Save
            </Button>
          </div>
        </div>
      }
    >
      <div className="space-y-5">
        <section className="space-y-3 rounded-lg border border-line bg-surface-sunken p-3">
          <div className="flex flex-wrap items-center gap-2">
            <Badge tone={PLACE_TONE[dataset.place]}>{PLACE_LABEL[dataset.place]}</Badge>
            {dataset.formats.map((format) => (
              <Badge key={format} tone="neutral">
                {format}
              </Badge>
            ))}
            {dataset.sessionScoped ? <Badge tone="warning">session only</Badge> : null}
            {workflowNames.length > 1 ? (
              <span title={`Touched by: ${workflowNames.join(', ')}`}>
                <Badge tone="info">{workflowNames.length} workflows</Badge>
              </span>
            ) : null}
          </div>
          <div className="flex flex-col gap-4 sm:flex-row">
            <MentionList
              title="Written by"
              empty="Nothing here writes it."
              mentions={dataset.producers}
              onOpenJob={onOpenJob}
            />
            <MentionList
              title="Read by"
              empty="Nothing here reads it."
              mentions={dataset.consumers}
              onOpenJob={onOpenJob}
            />
          </div>
        </section>

        {(schema && schema.fields.length > 0) || probe ? (
          <SchemaTable
            schema={schema}
            datasetKey={dataset.key}
            formats={dataset.formats}
            columns={columns}
            probe={probe}
            onOpenJob={onOpenJob}
          />
        ) : null}

        <Field
          label="What is it"
          help={`Plain language: what the rows are, and what they are not. ${
            MAX_DESCRIPTION - draft.description.length
          } characters left.`}
        >
          <Textarea
            value={draft.description}
            maxLength={MAX_DESCRIPTION}
            rows={3}
            placeholder="One order per row, after deduplication and the customer join."
            onChange={(event) => setDraft({ ...draft, description: event.target.value })}
          />
        </Field>

        <div className="grid gap-4 sm:grid-cols-2">
          <Field label="Owner" help="Who to ask when it breaks — a person or a team.">
            <Input
              value={draft.owner}
              placeholder="data-platform@company"
              onChange={(event) => setDraft({ ...draft, owner: event.target.value })}
            />
          </Field>
          <Field label="Domain" help="Groups the catalog: finance, sales, crm.">
            <Input
              value={draft.domain}
              placeholder="sales"
              onChange={(event) => setDraft({ ...draft, domain: event.target.value })}
            />
          </Field>
        </div>

        <div className="grid gap-4 sm:grid-cols-2">
          <Field
            label="Classification"
            help="How far the data may travel. Blank is not the same as public."
          >
            <Select
              value={draft.classification}
              options={CLASSIFICATION_OPTIONS}
              placeholder="Not classified"
              onValueChange={(value) =>
                setDraft({ ...draft, classification: value as DataClassification | '' })
              }
            />
          </Field>
          <Field label="Tags" help="Free labels. Enter adds one, Backspace removes the last.">
            <TagBox
              tags={draft.tags}
              suggestions={suggestions}
              onChange={(tags) => setDraft({ ...draft, tags })}
            />
          </Field>
        </div>
      </div>
    </Modal>
  )
}
