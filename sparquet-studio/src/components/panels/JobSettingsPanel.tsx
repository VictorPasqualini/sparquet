/**
 * Job settings — everything the compiled JSON carries that is not a node.
 *
 * `name`, `description` and `spark` sit at the top of the pipeline JSON, and so does
 * the block-level part of `validations`: `on_failure`, the quality `report` and the
 * quarantine `outputs`. Those three describe what the job does with the verdict of
 * ALL its rules, so they belong here rather than on one arbitrary rule node.
 */

import { ChevronRight, Flame, Settings2, ShieldCheck } from 'lucide-react'
import { useState, type ReactNode } from 'react'

import { getFormat, ON_FAILURE_OPTIONS, WRITABLE_FORMATS } from '@/catalog'
import {
  Field,
  Input,
  SectionTitle,
  Segmented,
  Select,
  Textarea,
  Toggle,
} from '@/components/ui'
import { cn } from '@/lib/utils/cn'
import { useEditorStore } from '@/store/editor'
import type { OnFailureMode, OutputSpec, SparkSettings } from '@/types/pipeline'
import type { ValidationPolicy } from '@/types/studio'
import { DEFAULT_VALIDATION_POLICY } from '@/types/studio'

import { PlaceholderHints } from './fields/FieldRenderer'
import { KeyValueField } from './fields/widgets'

/** Same wording as the sink inspector, so a mode means one thing across the app. */
const MODE_HINTS: Record<string, string> = {
  overwrite: 'Replaces what is at the destination.',
  append: 'Adds rows and keeps what is already there.',
  merge: 'Upsert on the merge keys — Delta and Iceberg only.',
  ignore: 'Skips the write when the destination already exists.',
  error: 'Fails when the destination already exists.',
}

const ON_FAILURE_CHOICES: { value: OnFailureMode; label: string }[] = ON_FAILURE_OPTIONS.map(
  (option) => ({ value: option.value, label: option.label.split(' — ')[0] }),
)

const ON_FAILURE_HINTS: Record<OnFailureMode, string> = Object.fromEntries(
  ON_FAILURE_OPTIONS.map((option) => [option.value, option.hint]),
) as Record<OnFailureMode, string>

const NEW_SINK: OutputSpec = { format: 'csv', path: '', mode: 'overwrite' }

export function JobSettingsPanel() {
  const settings = useEditorStore((state) => state.settings)
  const setSettings = useEditorStore((state) => state.setSettings)
  const ruleCount = useEditorStore(
    (state) => state.nodes.filter((node) => node.data.kind === 'validation').length,
  )

  const policy: ValidationPolicy = settings.validations ?? DEFAULT_VALIDATION_POLICY
  const quarantine = policy.outputs ?? null

  const setPolicy = (patch: Partial<ValidationPolicy>) => {
    setSettings({ validations: { ...policy, ...patch } })
  }

  const setSpark = (patch: Partial<SparkSettings>) => {
    const spark: SparkSettings = { ...settings.spark, ...patch }
    for (const key of Object.keys(spark) as (keyof SparkSettings)[]) {
      const value = spark[key]
      if (value === undefined || value === '' || (typeof value === 'object' && !Object.keys(value).length)) {
        delete spark[key]
      }
    }
    setSettings({ spark })
  }

  const setQuarantine = (key: 'valid' | 'invalid', value: OutputSpec | null) => {
    const next: Record<string, OutputSpec> = { ...(quarantine ?? {}) }
    if (value) next[key] = value
    else delete next[key]
    setPolicy({ outputs: Object.keys(next).length > 0 ? next : null })
  }

  return (
    <div className="flex h-full min-h-0 flex-col bg-surface">
      <header className="flex h-11 shrink-0 items-center gap-2 border-b border-line px-3">
        <Settings2 className="h-3.5 w-3.5 text-content-subtle" aria-hidden />
        <span className="text-xs font-medium text-content">Job settings</span>
      </header>

      <div className="scroll-area flex-1 space-y-4 px-3 py-3">
        <section className="space-y-3">
          <SectionTitle>Pipeline</SectionTitle>
          <Field
            label="Name"
            help="Compiled as the JSON `name`; the runner logs and the exported file are named after it."
            htmlFor="job-pipeline-name"
          >
            <Input
              id="job-pipeline-name"
              mono
              value={settings.pipelineName}
              placeholder="my_pipeline"
              onChange={(event) => setSettings({ pipelineName: event.target.value })}
            />
          </Field>
          <Field label="Description" help="Optional. Travels with the JSON as `description`.">
            <Textarea
              rows={3}
              aria-label="Pipeline description"
              value={settings.description}
              placeholder="What this job reads, does and writes…"
              onChange={(event) => setSettings({ description: event.target.value })}
            />
          </Field>
        </section>

        <Collapsible
          title="Validations"
          icon={<ShieldCheck />}
          summary={
            ruleCount > 0
              ? `${ruleCount} rule${ruleCount === 1 ? '' : 's'} on the canvas`
              : 'No rule nodes yet'
          }
          defaultOpen
        >
          <Field label="On failure" help={ON_FAILURE_HINTS[policy.onFailure]}>
            <Segmented
              ariaLabel="On failure"
              value={policy.onFailure}
              onChange={(value) => setPolicy({ onFailure: value })}
              options={ON_FAILURE_CHOICES}
            />
          </Field>

          <SinkToggle
            id="dq-report"
            label="Quality report"
            description="One row per rule: pipeline, rule_type, passed, failed_count, message, validated_at."
            value={policy.report ?? null}
            onChange={(report) => setPolicy({ report })}
          />

          <Callout>
            The report is only written when the run reaches the outputs — in{' '}
            <span className="font-mono">fail</span> mode a violation aborts before it.
          </Callout>

          <SinkToggle
            id="dq-valid"
            label="Quarantine — valid rows"
            description="Rows that break no row-level rule (not_null, unique, range, regex, missing/invalid checks)."
            value={quarantine?.valid ?? null}
            onChange={(value) => setQuarantine('valid', value)}
          />
          <SinkToggle
            id="dq-invalid"
            label="Quarantine — invalid rows"
            description="The rows those same checks rejected, kept apart for inspection."
            value={quarantine?.invalid ?? null}
            onChange={(value) => setQuarantine('invalid', value)}
          />

          <Callout>
            The quarantine split is written <em>before</em> and <em>besides</em> the job&apos;s
            own destinations — it routes rows apart, it does not replace the outputs on the
            canvas.
          </Callout>

          {ruleCount === 0 && (policy.report || quarantine) && (
            <Callout tone="warning">
              This job has no validation rule on the canvas, so no{' '}
              <span className="font-mono">validations</span> block is compiled and none of
              these destinations is written. Drag a rule from the Quality section of the
              palette.
            </Callout>
          )}
        </Collapsible>

        <Collapsible
          title="Spark"
          icon={<Flame />}
          summary={settings.spark.master ?? settings.spark.app_name ?? 'Session defaults'}
        >
          <Field
            label="Application name"
            help="`spark.app_name`. Leave empty to let the framework name the session."
            htmlFor="job-spark-app"
          >
            <Input
              id="job-spark-app"
              mono
              value={settings.spark.app_name ?? ''}
              placeholder="MeuJob"
              onChange={(event) => setSpark({ app_name: event.target.value })}
            />
          </Field>
          <Field
            label="Master"
            help="`spark.master`, e.g. local[*] or yarn. Ignored on Databricks, where the active session is reused."
            htmlFor="job-spark-master"
          >
            <Input
              id="job-spark-master"
              mono
              value={settings.spark.master ?? ''}
              placeholder="local[*]"
              onChange={(event) => setSpark({ master: event.target.value })}
            />
          </Field>
          <Field label="Configs" help="Passed to the session builder as `spark.configs`.">
            <KeyValueField
              value={settings.spark.configs ?? {}}
              keyPlaceholder="spark.sql.shuffle.partitions"
              valuePlaceholder="200"
              onChange={(configs) =>
                setSpark({ configs: Object.keys(configs).length > 0 ? configs : undefined })
              }
            />
          </Field>
        </Collapsible>
      </div>
    </div>
  )
}

/* ------------------------------------------------------------------ pieces */

function Collapsible({
  title,
  icon,
  summary,
  defaultOpen = false,
  children,
}: {
  title: string
  icon: ReactNode
  /** One line shown on the header, so a closed section still says something. */
  summary?: string
  defaultOpen?: boolean
  children: ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)

  return (
    <section className="rounded-xl border border-line bg-surface-raised">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
        className="flex w-full items-center gap-2 rounded-xl px-2.5 py-2 text-left"
      >
        <ChevronRight
          className={cn(
            'h-3.5 w-3.5 shrink-0 text-content-subtle transition-transform',
            open && 'rotate-90',
          )}
          aria-hidden
        />
        <span className="shrink-0 text-content-subtle [&_svg]:h-3.5 [&_svg]:w-3.5" aria-hidden>
          {icon}
        </span>
        <span className="text-2xs font-semibold uppercase tracking-wider text-content-subtle">
          {title}
        </span>
        {summary && (
          <span className="ml-auto min-w-0 truncate text-2xs text-content-subtle">
            {summary}
          </span>
        )}
      </button>
      {open && <div className="space-y-3 border-t border-line px-2.5 py-3">{children}</div>}
    </section>
  )
}

function Callout({
  tone = 'info',
  children,
}: {
  tone?: 'info' | 'warning'
  children: ReactNode
}) {
  return (
    <p
      className={cn(
        'rounded-lg border p-2.5 text-2xs leading-relaxed text-content-muted',
        tone === 'info'
          ? 'border-state-info/25 bg-state-info/5'
          : 'border-state-warning/25 bg-state-warning/5',
      )}
    >
      {children}
    </p>
  )
}

/** A destination that is off until it is switched on — report and quarantine alike. */
function SinkToggle({
  id,
  label,
  description,
  value,
  onChange,
}: {
  id: string
  label: string
  description: string
  value: OutputSpec | null
  onChange: (value: OutputSpec | null) => void
}) {
  const format = getFormat(value?.format ?? '')

  const patch = (next: Partial<OutputSpec>) => {
    onChange({ ...(value ?? NEW_SINK), ...next })
  }

  return (
    <div className="space-y-2">
      <Toggle
        checked={Boolean(value)}
        onCheckedChange={(checked) => onChange(checked ? { ...NEW_SINK } : null)}
        label={label}
        description={description}
      />

      {value && (
        <div
          role="group"
          aria-label={`${label} destination`}
          className="space-y-3 rounded-lg border border-line bg-surface p-2.5"
        >
          <Field label="Format" htmlFor={`${id}-format`}>
            <Select
              id={`${id}-format`}
              ariaLabel={`${label} format`}
              value={value.format}
              onValueChange={(next) => {
                // The mode select renders blank on a value the new format rejects.
                const def = getFormat(next)
                const keepsMode = !def || def.modes.some((mode) => mode === value.mode)
                patch({
                  format: next,
                  ...(keepsMode ? {} : { mode: def?.modes[0] ?? 'overwrite' }),
                })
              }}
              options={WRITABLE_FORMATS.map((item) => ({
                value: item.id,
                label: item.label,
                hint: item.summary,
              }))}
            />
          </Field>
          <Field
            label={format?.pathLabel ?? 'Path'}
            help={format?.pathHelp}
            htmlFor={`${id}-path`}
          >
            <Input
              id={`${id}-path`}
              mono
              value={value.path}
              placeholder={format?.pathPlaceholder}
              onChange={(event) => patch({ path: event.target.value })}
            />
            <PlaceholderHints value={value.path} />
          </Field>
          <Field label="Write mode" help={MODE_HINTS[String(value.mode ?? '')]}>
            <Select
              ariaLabel={`${label} write mode`}
              value={String(value.mode ?? 'overwrite')}
              onValueChange={(mode) => patch({ mode })}
              options={(format?.modes ?? []).map((mode) => ({
                value: mode,
                label: mode,
                hint: MODE_HINTS[mode],
              }))}
            />
          </Field>
        </div>
      )}
    </div>
  )
}
