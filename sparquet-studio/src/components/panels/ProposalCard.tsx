import { Braces, Check, Sparkles } from 'lucide-react'

import { Badge, Button } from '@/components/ui'
import { cn } from '@/lib/utils/cn'

/**
 * A pipeline the assistant proposed. The JSON is untrusted until it goes through
 * the compiler, so it stays `unknown` and is only read defensively for the preview.
 */
export interface AiProposal {
  pipeline: unknown
  summary: string
}

export interface ProposalCardProps {
  proposal: AiProposal
  onApply: () => void
  onView: () => void
  applied?: boolean
}

export function ProposalCard({
  proposal,
  onApply,
  onView,
  applied = false,
}: ProposalCardProps) {
  const stats = readStats(proposal.pipeline)

  return (
    <div
      className={cn(
        'rounded-xl border bg-surface-raised p-2.5 shadow-card animate-slide-up',
        applied ? 'border-state-success/40' : 'border-brand-500/30',
      )}
    >
      <div className="flex items-start gap-2">
        <span
          className={cn(
            'mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-md',
            'bg-brand-500/15 text-brand-600 dark:text-brand-400',
          )}
          aria-hidden
        >
          <Sparkles className="h-3 w-3" />
        </span>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-1.5">
            <p className="text-xs font-medium text-content">Pipeline proposal</p>
            {applied && (
              <Badge tone="success" icon={<Check />}>
                Applied
              </Badge>
            )}
          </div>
          <p className="mt-0.5 text-2xs leading-relaxed text-content-muted">
            {proposal.summary}
          </p>
        </div>
      </div>

      <dl className="mt-2 space-y-0.5 rounded-lg border border-line bg-surface-sunken px-2 py-1.5 font-mono text-2xs">
        <div className="flex gap-1.5">
          <dt className="shrink-0 text-content-subtle">name</dt>
          <dd className="truncate text-content">{stats.name}</dd>
        </div>
        {stats.lines.map((line) => (
          <div key={line} className="flex gap-1.5">
            <dt className="shrink-0 text-state-success" aria-label="added">
              +
            </dt>
            <dd className="truncate text-content-muted">{line}</dd>
          </div>
        ))}
      </dl>

      <p className="mt-1.5 text-2xs text-content-subtle">
        {stats.nodes} {stats.nodes === 1 ? 'node' : 'nodes'} on the canvas
      </p>

      <div className="mt-2 flex items-center gap-1.5">
        <Button
          size="xs"
          variant={applied ? 'secondary' : 'primary'}
          onClick={onApply}
          disabled={applied}
          icon={<Check className="h-3 w-3" />}
          className="flex-1"
        >
          {applied ? 'Applied' : 'Apply to canvas'}
        </Button>
        <Button
          size="xs"
          variant="secondary"
          onClick={onView}
          icon={<Braces className="h-3 w-3" />}
        >
          View JSON
        </Button>
      </div>
    </div>
  )
}

/* ------------------------------------------------------------------ preview */

interface ProposalStats {
  name: string
  /** Diff-ish summary rows, already formatted for display. */
  lines: string[]
  nodes: number
}

function readStats(pipeline: unknown): ProposalStats {
  const spec = asRecord(pipeline)
  const input = asRecord(spec?.input)
  const validations = asRecord(spec?.validations)

  const rawName = spec?.name
  const name = typeof rawName === 'string' && rawName.trim() ? rawName.trim() : 'untitled'

  const rawTransformations = spec?.transformations
  const transformations = Array.isArray(rawTransformations) ? rawTransformations.length : 0

  const rawOutputs = spec?.outputs
  const singleOutput = spec?.output
  const outputs = Array.isArray(rawOutputs) ? rawOutputs : singleOutput ? [singleOutput] : []

  const rawRules = validations?.rules
  const rules = Array.isArray(rawRules) ? rawRules.length : null

  const lines: string[] = []
  if (input) lines.push(`input · ${readString(input.format) ?? 'unset'}`)
  lines.push(
    `${transformations} ${transformations === 1 ? 'transformation' : 'transformations'}`,
  )
  if (validations) lines.push(`validations · ${rules ?? 0} ${rules === 1 ? 'rule' : 'rules'}`)
  const formats = outputs
    .map((output) => readString(asRecord(output)?.format))
    .filter((format): format is string => Boolean(format))
  lines.push(
    `${outputs.length} ${outputs.length === 1 ? 'output' : 'outputs'}` +
      (formats.length ? ` · ${unique(formats).join(', ')}` : ''),
  )

  const nodes = (input ? 1 : 0) + transformations + (validations ? 1 : 0) + outputs.length

  return { name, lines, nodes }
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null
  return value as Record<string, unknown>
}

function readString(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null
}

function unique(values: string[]): string[] {
  return [...new Set(values)]
}
