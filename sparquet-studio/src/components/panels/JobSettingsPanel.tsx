/**
 * Job settings — everything the compiled JSON carries that is not a node.
 *
 * `name`, `description` and `spark` sit at the top of the pipeline JSON, and so does
 * the one part of `validations` that is not data: `on_failure`. It decides what a
 * broken rule does to the RUN, which is a job-wide setting like the Spark configs.
 *
 * The three datasets the block WRITES — the quality report and the two quarantine
 * outputs — are destination nodes on the canvas, hanging off the last rule. A
 * dataset that gets written deserves a box like any other.
 */

import { ChevronRight, Flame, Settings2, ShieldCheck } from 'lucide-react'
import { useState, type ReactNode } from 'react'

import { ON_FAILURE_OPTIONS, VALIDATION_SINKS } from '@/catalog'
import { Field, Input, SectionTitle, Segmented, Textarea } from '@/components/ui'
import { cn } from '@/lib/utils/cn'
import { useEditorStore } from '@/store/editor'
import type { OnFailureMode, SparkSettings } from '@/types/pipeline'
import type { ValidationPolicy } from '@/types/studio'
import { DEFAULT_VALIDATION_POLICY } from '@/types/studio'

import { KeyValueField } from './fields/widgets'

const ON_FAILURE_CHOICES: { value: OnFailureMode; label: string }[] = ON_FAILURE_OPTIONS.map(
  (option) => ({ value: option.value, label: option.label.split(' — ')[0] }),
)

const ON_FAILURE_HINTS: Record<OnFailureMode, string> = Object.fromEntries(
  ON_FAILURE_OPTIONS.map((option) => [option.value, option.hint]),
) as Record<OnFailureMode, string>

export function JobSettingsPanel() {
  const settings = useEditorStore((state) => state.settings)
  const setSettings = useEditorStore((state) => state.setSettings)
  const ruleCount = useEditorStore(
    (state) => state.nodes.filter((node) => node.data.kind === 'validation').length,
  )

  const policy: ValidationPolicy = settings.validations ?? DEFAULT_VALIDATION_POLICY

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

          <Callout>
            The three datasets this block writes are <strong>boxes on the canvas</strong>, not
            settings. Drag from the{' '}
            {VALIDATION_SINKS.map((sink) => sink.handleLabel).join(', ')} handles under the
            last rule onto a destination node.
          </Callout>

          <Callout>
            Those are <strong>side outputs</strong>. The framework writes them from the same
            complete DataFrame it then hands to the job&apos;s own destinations, so quarantine
            copies rows out — it never takes them off the main chain. Every destination on the
            trunk still receives every row, invalid ones included.
          </Callout>
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
