/**
 * One box of a pipeline: a whole pipeline FILE running as one stage.
 *
 * It mirrors the pipeline canvas chrome (`canvas/NodeShell.tsx`) — accent stripe,
 * icon chip, ordinal, hover toolbar, run-status badge — without reusing it: that
 * shell is bound to the pipeline editor store (mute, duplicate, node issues),
 * and a stage has none of those. The pieces that MUST look identical, the run
 * status icon and colour, come from the shared `stepLook`.
 */

import { Handle, NodeToolbar, Position, type Node, type NodeProps } from '@xyflow/react'
import {
  CircleX,
  ExternalLink,
  FileJson,
  FileWarning,
  Link2,
  Trash2,
  TriangleAlert,
  Unlink,
  Workflow as JobIcon,
} from 'lucide-react'
import { memo } from 'react'

import { getFormat } from '@/catalog'
import { stepLook } from '@/components/canvas/stepLook'
import { cancelConnect, startConnect, useConnectSource } from '@/components/canvas/NodeShell'
import { Badge, Button, IconButton, Tooltip } from '@/components/ui'
import type { ResolvedStage } from '@/lib/pipeline'
import { cn } from '@/lib/utils/cn'
import { formatCount, formatDuration, plural, truncateMiddle } from '@/lib/utils/format'
import { usePipelineEditorStore } from '@/store/pipelineEditor'
import type { PipelineStageOutcome, ValidationIssue } from '@/types/studio'

export type StageNodeData = {
  stage: ResolvedStage
  /** Issues scoped to this stage, from `resolvePipeline`. */
  issues: ValidationIssue[]
  /** Opens the job of a stage; the stage id is what carries the run being viewed. */
  onOpen: (jobId: string, stageId: string) => void
}

export type StageRfNode = Node<StageNodeData, 'stage'>

/**
 * Wider than a pipeline node (264px): a stage carries a file name, an Open action
 * and two endpoint rows, and a truncated path is useless. Kept below the layout
 * pitch in `lib/pipeline/pipeline.ts` so the link label still has room.
 */
export const STAGE_NODE_WIDTH = 320

/** A stage's wording differs from a transformation's; the icons and colours do not. */
const STAGE_STATUS_LABEL: Record<'running' | PipelineStageOutcome, string> = {
  running: 'Running now',
  success: 'Finished successfully',
  error: 'Failed — the pipeline stopped here',
  skipped: 'Skipped — it had no rows to process',
  cancelled: 'Cancelled — the run was stopped',
}

export const StageNode = memo(function StageNodeRenderer({
  id,
  data,
  selected,
}: NodeProps<StageRfNode>) {
  const { stage, issues, onOpen } = data

  const status = usePipelineEditorStore((state) => state.stageStatus[stage.id])
  const result = usePipelineEditorStore((state) => state.stageResults[stage.id])
  const removeStages = usePipelineEditorStore((state) => state.removeStages)
  const connect = usePipelineEditorStore((state) => state.connect)
  const select = usePipelineEditorStore((state) => state.select)

  const connectSource = useConnectSource()
  const isConnectSource = connectSource === stage.id
  const canReceive = connectSource !== null && !isConnectSource

  const look = stepLook(status)
  const StatusIcon = look?.icon
  const statusLabel =
    status && status !== 'pending'
      ? STAGE_STATUS_LABEL[status as 'running' | PipelineStageOutcome]
      : null

  /**
   * A stage that names a file has no Job behind it on purpose: the runner reads
   * the JSON when the stage starts. So there is nothing to open, nothing to
   * describe from the graph, and — unlike a dangling reference — nothing wrong.
   */
  const fromFile = typeof stage.path === 'string' && stage.path.length > 0
  const broken = stage.job === null && !fromFile
  const errorCount = issues.filter((issue) => issue.severity === 'error').length
  const warningCount = issues.filter((issue) => issue.severity === 'warning').length
  const flagged = errorCount + warningCount
  const issueSummary = issues.map((issue) => issue.message).join(' ')

  const finish = () => {
    if (connectSource === null) return
    connect(connectSource, stage.id)
    cancelConnect()
    select(stage.id)
  }

  /**
   * Drilling into the job is what people try first on a box, so the whole stage
   * opens on a double-click — `PipelineCanvas` binds Enter to the same thing for
   * the keyboard. A broken stage points at a deleted job: there is nothing to
   * open, and navigating to a dead route would hide the very state it reports.
   */
  const openJob = () => {
    if (broken || fromFile) return
    onOpen(stage.jobId, stage.id)
  }

  return (
    <div
      onDoubleClick={(event) => {
        // The pane zooms on double-click; a box is not the pane.
        event.stopPropagation()
        openJob()
      }}
      title={
        broken || fromFile
          ? undefined
          : `Open "${stage.name}" in the editor — double-click, or press Enter`
      }
      className={cn(
        'relative rounded-xl border bg-surface shadow-card transition-shadow hover:shadow-raised',
        selected ? 'border-brand-500 ring-2 ring-brand-500/40' : 'border-line',
        broken && 'border-dashed border-state-danger/60',
        fromFile && !selected && 'border-dashed',
        !selected && errorCount > 0 && 'ring-2 ring-state-danger/45',
        !selected && errorCount === 0 && warningCount > 0 && 'ring-2 ring-state-warning/40',
        // Selection and issues win the ring: a broken stage stays flagged mid-run.
        // The status badge always shows, so no run state is ever lost.
        !selected && flagged === 0 && look && look.ring,
        isConnectSource && 'ring-2 ring-brand-500',
      )}
      style={{ width: STAGE_NODE_WIDTH }}
    >
      <NodeToolbar nodeId={id} position={Position.Top} offset={2}>
        <div className="flex items-center gap-0.5 rounded-lg border border-line bg-surface-overlay p-0.5 shadow-pop">
          {isConnectSource && (
            <Tooltip content="Cancel the link" shortcut="Esc">
              <IconButton size="xs" label="Cancel link" onClick={cancelConnect}>
                <Unlink />
              </IconButton>
            </Tooltip>
          )}
          {canReceive && (
            <Tooltip content="Run this stage after the selected one">
              <IconButton size="xs" label="Finish the link here" onClick={finish}>
                <Link2 />
              </IconButton>
            </Tooltip>
          )}
          {connectSource === null && (
            <Tooltip content="Link to the stage that runs next" shortcut="C">
              <IconButton
                size="xs"
                label="Link to another stage"
                onClick={() => startConnect(stage.id)}
              >
                <Link2 />
              </IconButton>
            </Tooltip>
          )}
          <span className="mx-0.5 h-4 w-px bg-line" aria-hidden />
          <Tooltip content="Remove this stage from the pipeline">
            <IconButton
              size="xs"
              label="Remove stage"
              className="hover:bg-state-danger/12 hover:text-state-danger"
              onClick={() => removeStages([stage.id])}
            >
              <Trash2 />
            </IconButton>
          </Tooltip>
        </div>
      </NodeToolbar>

      <span
        className={cn(
          'absolute inset-y-0 left-0 w-1 rounded-l-xl',
          broken ? 'bg-state-danger' : fromFile ? 'bg-content-subtle' : 'bg-brand-500',
        )}
        aria-hidden
      />

      <div className="flex items-start gap-2.5 py-2.5 pl-4 pr-3">
        <span
          className={cn(
            'mt-px flex h-7 w-7 shrink-0 items-center justify-center rounded-lg',
            broken
              ? 'bg-state-danger/12 text-state-danger'
              : fromFile
                ? 'bg-surface-sunken text-content-subtle'
                : 'bg-brand-500/12 text-brand-500',
          )}
        >
          {broken ? (
            <FileWarning className="h-4 w-4" aria-hidden />
          ) : fromFile ? (
            <FileJson className="h-4 w-4" aria-hidden />
          ) : (
            <JobIcon className="h-4 w-4" aria-hidden />
          )}
        </span>

        <div className="min-w-0 flex-1">
          <div className="flex items-start gap-1.5">
            <span
              className="mt-0.5 shrink-0 rounded bg-surface-sunken px-1 text-2xs font-medium tabular-nums text-content-subtle"
              title={`Stage ${stage.order} in the execution order`}
            >
              {stage.order}
            </span>
            <p
              className="min-w-0 flex-1 truncate text-sm font-medium leading-5 text-content"
              title={stage.name}
            >
              <span className="sr-only">Stage {stage.order}: </span>
              {stage.name}
            </p>

            <span className="flex shrink-0 items-center gap-1 pt-1">
              {look && StatusIcon && statusLabel && (
                <span
                  role="img"
                  aria-label={statusLabel}
                  title={statusLabel}
                  className={cn(
                    'inline-flex h-4 w-4 items-center justify-center rounded-full',
                    look.chip,
                    look.spin,
                  )}
                >
                  <StatusIcon className="h-3 w-3" aria-hidden />
                </span>
              )}
              {flagged > 0 && (
                // The messages live in the label, not in a tooltip: this badge is
                // not interactive, so a hover-only tooltip would hide them from
                // the keyboard and from screen readers.
                <span
                  aria-label={`${flagged} ${errorCount > 0 ? 'problems' : 'warnings'} on this stage: ${issueSummary}`}
                  title={issueSummary}
                  className={cn(
                    'inline-flex items-center gap-0.5 rounded-full px-1 py-px text-2xs font-semibold',
                    errorCount > 0
                      ? 'bg-state-danger/15 text-state-danger'
                      : 'bg-state-warning/15 text-state-warning',
                  )}
                >
                  {errorCount > 0 ? (
                    <CircleX className="h-3 w-3" aria-hidden />
                  ) : (
                    <TriangleAlert className="h-3 w-3" aria-hidden />
                  )}
                  {flagged}
                </span>
              )}
            </span>
          </div>

          <p className="truncate text-2xs leading-4 text-content-muted">
            {broken
              ? `Reference ${stage.jobId}`
              : fromFile
                ? stage.path
                  : `${plural(stage.description?.transformationCount ?? 0, 'transformation')} · ${plural(
                    stage.description?.validationRuleCount ?? 0,
                    'validation',
                  )}`}
          </p>
        </div>

        {/* Always visible, not only in the hover toolbar: drilling into the
            pipeline is the whole point of a stage, and it must be reachable
            without a pointer — and without knowing the gestures. */}
        {!broken && !fromFile && (
          <Button
            size="xs"
            variant="ghost"
            className="nodrag shrink-0"
            icon={<ExternalLink className="h-3 w-3" />}
            onClick={openJob}
            aria-label={`Open ${stage.name} in the editor. Double-click the stage, or press Enter, to do the same.`}
          >
            Open
          </Button>
        )}
      </div>

      <div className="space-y-1 border-t border-line/70 py-2 pl-4 pr-3">
        {broken ? (
          <p className="text-2xs leading-relaxed text-state-danger">
            The job this stage points at was deleted. Remove the stage, or recreate the
            job.
          </p>
        ) : fromFile ? (
          // No endpoints: reading them would mean reading the file and holding a
          // copy, and the copy would be a lie the moment somebody edited the file.
          <p className="text-2xs leading-relaxed text-content-muted">
            Runs the JSON in the library as it stands. The runner reads the file when
            the stage starts, so an edit outside the Studio takes effect on the next
            run.
          </p>
        ) : (
          <>
            <Endpoint
              role="Reads"
              format={stage.description?.input?.format ?? ''}
              path={stage.description?.input?.path ?? ''}
            />
            {(stage.description?.outputs.length ?? 0) === 0 ? (
              <Endpoint role="Writes" format="" path="" />
            ) : (
              stage.description?.outputs.map((output, index) => (
                <Endpoint
                  key={`${output.format}-${output.path}-${index}`}
                  role={index === 0 ? 'Writes' : ''}
                  format={output.format}
                  path={output.path}
                  mode={output.mode}
                />
              ))
            )}
            {!stage.pipeline && (
              <p className="pt-0.5 text-2xs text-state-warning">
                Does not compile into a pipeline yet.
              </p>
            )}
          </>
        )}

        {result && (
          <div className="space-y-0.5 border-t border-line/70 pt-1.5">
            <p className="text-2xs tabular-nums text-content-muted">
              {formatCount(result.rowsRead)} read · {formatCount(result.rowsWritten)} written
              {result.durationMs !== undefined && ` · ${formatDuration(result.durationMs)}`}
            </p>
            {result.error && (
              // Two lines, never more: a Spark stack trace would grow the box past
              // everything around it on the canvas. The whole message stays one
              // hover away, and the run panel shows it in full in a scrollable card.
              <p
                title={result.error}
                className={cn(
                  'line-clamp-2 break-words text-2xs leading-relaxed',
                  status === 'cancelled' ? 'text-state-warning' : 'text-state-danger',
                )}
              >
                {result.error}
              </p>
            )}
          </div>
        )}
      </div>

      <Handle type="target" position={Position.Left} />
      <Handle type="source" position={Position.Right} />
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
          {truncateMiddle(path, 24)}
        </span>
      )}
      {mode && <span className="shrink-0 text-2xs text-content-subtle">{mode}</span>}
    </div>
  )
}
