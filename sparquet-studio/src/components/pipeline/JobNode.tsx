/**
 * One box of the inferred pipeline: a pipeline FILE, not a step.
 *
 * Collapsed it answers "what does this stage read and write"; expanded it lists
 * the steps in execution order, which is the drill-down into the file without
 * leaving the map. "Open" is the way into the real editor.
 */

import { Handle, Position, type Node, type NodeProps } from '@xyflow/react'
import { ChevronDown, ExternalLink, ShieldCheck } from 'lucide-react'
import { memo } from 'react'

import { getFormat, getTransformation } from '@/catalog'
import { catalogIcon } from '@/components/canvas/icons'
import { Badge, Button } from '@/components/ui'
import type { JobSummary, JobStep } from '@/lib/pipeline'
import { cn } from '@/lib/utils/cn'
import { plural, truncateMiddle } from '@/lib/utils/format'

export type JobNodeData = {
  file: JobSummary
  expanded: boolean
  onToggle: (jobId: string) => void
  onOpen: (jobId: string) => void
}

export type JobRfNode = Node<JobNodeData, 'file'>

/** Kept in sync with the layout in InferredPipelineCanvas. */
export const JOB_NODE_WIDTH = 308

export const JobNode = memo(function JobNodeRenderer({ data }: NodeProps<JobRfNode>) {
  const { file, expanded, onToggle, onOpen } = data
  const stepsId = `file-steps-${file.jobId}`

  return (
    <div
      className="relative rounded-xl border border-line bg-surface shadow-card transition-shadow hover:shadow-raised"
      style={{ width: JOB_NODE_WIDTH }}
    >
      {/* Read-only map: the handles only anchor the links. */}
      <Handle type="target" position={Position.Left} isConnectable={false} />
      <Handle type="source" position={Position.Right} isConnectable={false} />

      <div className="flex items-start gap-2.5 border-b border-line px-3 py-2.5">
        <span
          className="mt-px flex h-6 w-6 shrink-0 items-center justify-center rounded-lg border border-line bg-surface-sunken text-2xs font-semibold tabular-nums text-content-muted"
          aria-hidden
        >
          {file.order}
        </span>
        <div className="min-w-0 flex-1">
          <p className="truncate text-xs font-medium text-content" title={file.name}>
            <span className="sr-only">Step {file.order}: </span>
            {file.name}
          </p>
          <p className="text-2xs text-content-subtle">
            {plural(file.transformationCount, 'transformation')}
            {file.hasValidations && (
              <>
                {' · '}
                <span className="inline-flex items-center gap-1 text-node-validate">
                  <ShieldCheck className="h-3 w-3" aria-hidden />
                  validations
                </span>
              </>
            )}
          </p>
        </div>
        <Button
          size="xs"
          variant="ghost"
          className="nodrag shrink-0"
          icon={<ExternalLink className="h-3 w-3" />}
          onClick={() => onOpen(file.jobId)}
          aria-label={`Open ${file.name} in the editor`}
        >
          Open
        </Button>
      </div>

      <div className="space-y-1 px-3 py-2">
        <Endpoint
          role="Reads"
          format={file.input?.format ?? ''}
          path={file.input?.path ?? ''}
        />
        {file.outputs.length === 0 ? (
          <Endpoint role="Writes" format="" path="" />
        ) : (
          file.outputs.map((output, index) => (
            <Endpoint
              key={`${output.format}-${output.path}-${index}`}
              role={index === 0 ? 'Writes' : ''}
              format={output.format}
              path={output.path}
              mode={output.mode}
            />
          ))
        )}
        {!file.compiled && (
          <p className="pt-0.5 text-2xs text-state-warning">
            Does not compile yet — read from the canvas.
          </p>
        )}
      </div>

      <button
        type="button"
        aria-expanded={expanded}
        aria-controls={stepsId}
        onClick={() => onToggle(file.jobId)}
        className={cn(
          'nodrag flex w-full items-center justify-between gap-2 rounded-b-xl border-t border-line px-3 py-1.5',
          'text-2xs text-content-muted transition-colors hover:bg-surface-raised hover:text-content',
          'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand-500/50',
        )}
      >
        <span>
          {expanded ? 'Hide' : 'Show'} {plural(file.steps.length, 'step')}
        </span>
        <ChevronDown
          className={cn('h-3.5 w-3.5 transition-transform', expanded && 'rotate-180')}
          aria-hidden
        />
      </button>

      {expanded && (
        <ol
          id={stepsId}
          className="scroll-area nowheel max-h-56 space-y-0.5 border-t border-line px-2 py-2"
        >
          {file.steps.map((step) => (
            <StepRow key={step.id} step={step} />
          ))}
          {file.steps.length === 0 && (
            <li className="px-1 py-2 text-2xs text-content-subtle">
              This file is still empty.
            </li>
          )}
        </ol>
      )}
    </div>
  )
})

/* ------------------------------------------------------------------ pieces */

function Endpoint({
  role,
  format,
  path,
  mode,
}: {
  role: string
  format: string
  path: string
  mode?: string
}) {
  return (
    <div className="flex items-center gap-2">
      <span className="w-10 shrink-0 text-2xs text-content-subtle">{role}</span>
      {format ? (
        <Badge tone={role === 'Reads' ? 'info' : 'neutral'}>
          {getFormat(format)?.label ?? format}
        </Badge>
      ) : (
        <span className="text-2xs text-content-subtle">nothing yet</span>
      )}
      {path && (
        <span
          className="min-w-0 flex-1 truncate font-mono text-2xs text-content-muted"
          title={path}
        >
          {truncateMiddle(path, 28)}
        </span>
      )}
      {mode && <span className="shrink-0 text-2xs text-content-subtle">{mode}</span>}
    </div>
  )
}

const STEP_ACCENT: Record<JobStep['kind'], string> = {
  input: 'text-node-input',
  transformation: 'text-node-transform',
  validations: 'text-node-validate',
  output: 'text-node-output',
}

function StepRow({ step }: { step: JobStep }) {
  const Icon = catalogIcon(stepIcon(step))
  return (
    <li className="flex items-center gap-2 rounded-lg px-1 py-1">
      <Icon className={cn('h-3.5 w-3.5 shrink-0', STEP_ACCENT[step.kind])} aria-hidden />
      <span className="shrink-0 text-2xs font-medium text-content">{step.label}</span>
      {step.detail && (
        <span
          className="min-w-0 flex-1 truncate font-mono text-2xs text-content-subtle"
          title={step.detail}
        >
          {step.detail}
        </span>
      )}
    </li>
  )
}

/** Steps borrow the palette icons so the list reads like the canvas. */
function stepIcon(step: JobStep): string {
  if (step.kind === 'validations') return 'ShieldCheck'
  if (step.kind === 'transformation') return getTransformation(step.type)?.icon ?? 'Sparkles'
  return getFormat(step.type)?.icon ?? 'Database'
}
