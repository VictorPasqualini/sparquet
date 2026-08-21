/**
 * The chrome every canvas node shares: accent stripe, icon chip, title block,
 * status dots, handles and the hover toolbar.
 *
 * Node renderers stay declarative — they decide *what* to say (title, preview,
 * badges) and never re-implement geometry, selection or issue styling.
 */

import { Handle, NodeToolbar, Position } from '@xyflow/react'
import {
  CircleX,
  Copy,
  Eye,
  EyeOff,
  Link2,
  TriangleAlert,
  Trash2,
  Unlink,
} from 'lucide-react'
import { useMemo, useState, useSyncExternalStore, type ReactNode } from 'react'
import type { LucideIcon } from 'lucide-react'

import { IconButton, Tooltip } from '@/components/ui'
import { cn } from '@/lib/utils/cn'
import { formatDuration } from '@/lib/utils/format'
import type { NodeAccent } from '@/catalog'
import { nodeOrdinals, useEditorStore } from '@/store/editor'
import { HANDLE, type StepStatus, type ValidationIssue } from '@/types/studio'

import { stepLook } from './stepLook'

/** Every node is the same width so chains read as a column, not a staircase. */
const NODE_WIDTH = 'w-[264px]'

const ACCENT_STRIPE: Record<NodeAccent, string> = {
  input: 'bg-node-input',
  transform: 'bg-node-transform',
  combine: 'bg-node-combine',
  control: 'bg-node-control',
  inspect: 'bg-node-inspect',
  validate: 'bg-node-validate',
  output: 'bg-node-output',
}

const ACCENT_CHIP: Record<NodeAccent, string> = {
  input: 'bg-node-input/12 text-node-input',
  transform: 'bg-node-transform/12 text-node-transform',
  combine: 'bg-node-combine/12 text-node-combine',
  control: 'bg-node-control/12 text-node-control',
  inspect: 'bg-node-inspect/12 text-node-inspect',
  validate: 'bg-node-validate/12 text-node-validate',
  output: 'bg-node-output/12 text-node-output',
}

/* --------------------------------------------------------- keyboard connect */

/**
 * React Flow's `<Handle>` is pointer-only — no tabindex, no key handlers — so
 * dragging a link is unreachable from the keyboard. This tiny store backs the
 * two-step alternative: pick a source, then pick a target. It lives outside
 * React because every node and the canvas itself have to see the same pending
 * source, and the editor store is not ours to extend.
 *
 * A node id is all it holds: every node has exactly one output, so there is nothing
 * to choose between.
 */
let connectSource: string | null = null
const connectListeners = new Set<() => void>()

/** Pending connect source, readable outside React. */
export const readConnectSource = (): string | null => connectSource

export function subscribeConnectSource(listener: () => void): () => void {
  connectListeners.add(listener)
  return () => {
    connectListeners.delete(listener)
  }
}

function setConnectSource(next: string | null): void {
  if (connectSource === next) return
  connectSource = next
  for (const listener of connectListeners) listener()
}

/** Node a keyboard-started connection is waiting to leave, or `null`. */
export function useConnectSource(): string | null {
  return useSyncExternalStore(subscribeConnectSource, readConnectSource, readConnectSource)
}

export function startConnect(nodeId: string): void {
  setConnectSource(nodeId)
}

export function cancelConnect(): void {
  setConnectSource(null)
}

export interface NodeShellProps {
  /** React Flow node id — drives the toolbar actions and the issue lookup. */
  nodeId: string
  accent: NodeAccent
  icon: LucideIcon
  title: string
  subtitle?: string
  /** Small chips rendered under the body, e.g. format or guard indicators. */
  badges?: ReactNode
  issues?: ValidationIssue[]
  selected?: boolean
  /**
   * Muted state. Pass it only for nodes that can actually be muted (transforms):
   * `undefined` hides the mute control entirely.
   */
  disabled?: boolean
  /**
   * `scoped` is the quarantine of rejected rows: two incoming handles, because the
   * two links mean different things — `from` only records which validations wrote the
   * dataset, `scope` narrows the split to the rules linked into it.
   */
  inputs?: 'none' | 'single' | 'dual' | 'scoped'
  hasOutput?: boolean
  children?: ReactNode
}

export function NodeShell({
  nodeId,
  accent,
  icon: Icon,
  title,
  subtitle,
  badges,
  issues,
  selected = false,
  disabled,
  inputs = 'single',
  hasOutput = true,
  children,
}: NodeShellProps) {
  const [hovered, setHovered] = useState(false)
  const duplicateNode = useEditorStore((state) => state.duplicateNode)
  const removeNodes = useEditorStore((state) => state.removeNodes)
  const toggleDisabled = useEditorStore((state) => state.toggleDisabled)
  const select = useEditorStore((state) => state.select)
  const togglePanel = useEditorStore((state) => state.togglePanel)
  const onConnect = useEditorStore((state) => state.onConnect)

  const errorCount = issues?.filter((issue) => issue.severity === 'error').length ?? 0
  const warningCount = issues?.filter((issue) => issue.severity === 'warning').length ?? 0
  const flagged = errorCount + warningCount
  const canMute = disabled !== undefined
  // Severity must survive a monochrome screen, so it picks the icon, not just the hue.
  const IssueIcon = errorCount > 0 ? CircleX : TriangleAlert

  const ordinal = useNodeOrdinal(nodeId)
  const stepStatus = useNodeStepStatus(nodeId)
  const step = stepLook(stepStatus)
  const StepIcon = step?.icon
  const stepMs = useNodeStepDuration(nodeId)
  // No time while it runs: only the closing marker fixes the end of the step.
  const stepDurationLabel =
    step && stepStatus !== 'running' && stepMs !== undefined ? formatDuration(stepMs) : null
  const stepTooltip = step
    ? stepDurationLabel
      ? `${step.label} · ${stepDurationLabel} of wall clock between this step's start and end. Spark is lazy, so a transformation only builds the plan and reads near zero — the time lands on the read, the rules and the writes.`
      : step.label
    : null
  /** Queued: the run has this node in its plan but has not arrived yet. */
  const waiting = stepStatus === 'pending'

  const connectSource = useConnectSource()
  const isConnectSource = connectSource === nodeId
  const canReceiveConnection =
    connectSource !== null && !isConnectSource && inputs !== 'none'

  const completeConnection = (targetHandle: string) => {
    if (connectSource === null) return
    onConnect({
      source: connectSource,
      target: nodeId,
      sourceHandle: HANDLE.out,
      targetHandle,
    })
    cancelConnect()
    select(nodeId)
  }

  const messages = useMemo(
    () => (issues ?? []).filter((issue) => issue.severity !== 'info'),
    [issues],
  )

  return (
    <div
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      className={cn(
        'relative rounded-xl border bg-surface shadow-card transition-shadow hover:shadow-raised',
        NODE_WIDTH,
        selected ? 'border-brand-500 ring-2 ring-brand-500/40' : 'border-line',
        !selected && errorCount > 0 && 'ring-2 ring-state-danger/45',
        !selected && errorCount === 0 && warningCount > 0 && 'ring-2 ring-state-warning/40',
        // Ring AND border precedence: selection and issues first — a broken node
        // stays flagged even mid-run — so the run status only paints a node nothing
        // else claims. The status bar and the status band below ignore this rule and
        // always show, so no run state is ever lost on a node that also has issues.
        !selected && flagged === 0 && step && cn(step.ring, step.border),
        isConnectSource && 'ring-2 ring-brand-500',
        // Not reached yet: dimmed rather than badged, so a run in flight reads as a
        // bright wavefront crossing a quiet graph. Muted keeps its own dimming.
        !disabled && waiting && 'opacity-60',
        disabled && 'border-dashed opacity-55',
      )}
    >
      <NodeToolbar
        nodeId={nodeId}
        isVisible={hovered || selected || isConnectSource}
        position={Position.Top}
        offset={2}
      >
        {/* The padding bridges the gap to the node so hover survives the trip. */}
        <div
          className="pb-1.5"
          onMouseEnter={() => setHovered(true)}
          onMouseLeave={() => setHovered(false)}
        >
          <div className="flex items-center gap-0.5 rounded-lg border border-line bg-surface-overlay p-0.5 shadow-pop">
            {isConnectSource && (
              <Tooltip content="Cancel the connection" shortcut="Esc">
                <IconButton size="xs" label="Cancel connection" onClick={cancelConnect}>
                  <Unlink />
                </IconButton>
              </Tooltip>
            )}
            {canReceiveConnection && (
              <Tooltip content="Finish the connection here" shortcut="C">
                <IconButton
                  size="xs"
                  label={
                    inputs === 'dual' ? 'Connect to the left input' : 'Connect to this node'
                  }
                  onClick={() => completeConnection(HANDLE.in)}
                >
                  <Link2 />
                </IconButton>
              </Tooltip>
            )}
            {canReceiveConnection && inputs === 'dual' && (
              <Tooltip content="Finish the connection on the right input">
                <IconButton
                  size="xs"
                  label="Connect to the right input"
                  onClick={() => completeConnection(HANDLE.inRight)}
                >
                  <Link2 className="rotate-90" />
                </IconButton>
              </Tooltip>
            )}
            {connectSource === null && hasOutput && (
              <Tooltip content="Connect to another node" shortcut="C">
                <IconButton
                  size="xs"
                  label="Connect to another node"
                  onClick={() => startConnect(nodeId)}
                >
                  <Link2 />
                </IconButton>
              </Tooltip>
            )}
            <Tooltip content="Duplicate">
              <IconButton
                size="xs"
                label="Duplicate node"
                onClick={() => duplicateNode(nodeId)}
              >
                <Copy />
              </IconButton>
            </Tooltip>
            {canMute && (
              <Tooltip
                content={disabled ? 'Include in the pipeline' : 'Mute — leave out of the JSON'}
              >
                <IconButton
                  size="xs"
                  label={disabled ? 'Unmute node' : 'Mute node'}
                  onClick={() => toggleDisabled(nodeId)}
                >
                  {disabled ? <EyeOff /> : <Eye />}
                </IconButton>
              </Tooltip>
            )}
            <span className="mx-0.5 h-4 w-px bg-line" aria-hidden />
            <Tooltip content="Delete">
              <IconButton
                size="xs"
                label="Delete node"
                className="hover:bg-state-danger/12 hover:text-state-danger"
                onClick={() => removeNodes([nodeId])}
              >
                <Trash2 />
              </IconButton>
            </Tooltip>
          </div>
        </div>
      </NodeToolbar>

      <span
        className={cn('absolute inset-y-0 left-0 w-1 rounded-l-xl', ACCENT_STRIPE[accent])}
        aria-hidden
      />

      {/* Status bar along the top edge — the one channel still legible when the
          whole graph is zoomed out to fit and no label can be read. It starts
          after the accent stripe so the two never fight over the corner. */}
      {step && (
        <span
          className={cn(
            'pointer-events-none absolute left-1 right-0 top-0 overflow-hidden rounded-tr-xl',
            step.bar,
          )}
          aria-hidden
        >
          {step.live && (
            <span className="block h-full w-1/4 animate-status-sweep bg-state-info motion-reduce:hidden" />
          )}
        </span>
      )}

      <div className="flex items-start gap-2.5 py-2.5 pl-4 pr-3">
        <span
          className={cn(
            'mt-px flex h-7 w-7 shrink-0 items-center justify-center rounded-lg',
            ACCENT_CHIP[accent],
          )}
        >
          <Icon className="h-4 w-4" aria-hidden />
        </span>

        <div className="min-w-0 flex-1">
          <div className="flex items-start gap-1.5">
            {ordinal !== undefined && (
              <span
                className="mt-0.5 shrink-0 rounded bg-surface-sunken px-1 text-2xs font-medium tabular-nums text-content-subtle"
                title={`Step ${ordinal} in the execution order`}
              >
                {ordinal}
              </span>
            )}
            <p
              className="min-w-0 flex-1 truncate text-sm font-medium leading-5 text-content"
              title={title}
            >
              {title}
            </p>

            <span className="flex shrink-0 items-center gap-1 pt-1">
              {disabled && (
                <span
                  className="h-1.5 w-1.5 rounded-full bg-content-subtle"
                  title="Muted — left out of the compiled JSON"
                />
              )}
              {flagged > 0 && (
                <Tooltip
                  content={
                    <span className="block space-y-1">
                      {messages.slice(0, 4).map((issue) => (
                        <span key={issue.id} className="block">
                          {issue.message}
                        </span>
                      ))}
                      {messages.length > 4 && (
                        <span className="block text-content-subtle">
                          and {messages.length - 4} more
                        </span>
                      )}
                    </span>
                  }
                >
                  <button
                    type="button"
                    onClick={() => {
                      select(nodeId)
                      togglePanel('issues', true)
                    }}
                    aria-label={`${flagged} ${errorCount > 0 ? 'errors' : 'warnings'} on this node`}
                    className={cn(
                      'nodrag inline-flex items-center gap-0.5 rounded-full px-1 py-px text-2xs font-semibold',
                      errorCount > 0
                        ? 'bg-state-danger/15 text-state-danger'
                        : 'bg-state-warning/15 text-state-warning',
                    )}
                  >
                    <IssueIcon className="h-3 w-3" aria-hidden />
                    {flagged}
                  </button>
                </Tooltip>
              )}
            </span>
          </div>

          {subtitle && (
            <p className="truncate text-2xs leading-4 text-content-muted" title={subtitle}>
              {subtitle}
            </p>
          )}
        </div>
      </div>

      {(children || badges) && (
        <div className="space-y-1.5 border-t border-line/70 py-2 pl-4 pr-3">
          {children}
          {badges && <div className="flex flex-wrap items-center gap-1">{badges}</div>}
        </div>
      )}

      {/* The status in words, with its icon and the time it took. role="img" plus an
          aria-label is what makes the band announce as one thing; the pieces inside
          are decorative once the label carries them. */}
      {step && StepIcon && (
        <div
          role="img"
          aria-label={stepTooltip ?? step.label}
          title={stepTooltip ?? step.label}
          className={cn(
            'flex items-center gap-1.5 rounded-b-[11px] border-t px-3 py-1 text-2xs font-semibold',
            step.footer,
          )}
        >
          <StepIcon className={cn('h-3 w-3 shrink-0', step.spin)} aria-hidden />
          <span className="uppercase tracking-wide">{step.short}</span>
          {stepDurationLabel && (
            <span className="ml-auto font-medium tabular-nums opacity-80">
              {stepDurationLabel}
            </span>
          )}
        </div>
      )}

      {inputs !== 'none' && (
        <Handle
          type="target"
          position={Position.Left}
          id={HANDLE.in}
          className={cn((inputs === 'dual' || inputs === 'scoped') && '!top-[34%]')}
        />
      )}
      {inputs === 'dual' && (
        <>
          <Handle
            type="target"
            position={Position.Left}
            id={HANDLE.inRight}
            className="!top-[72%]"
          />
          <span className="pointer-events-none absolute -left-3 top-[72%] -translate-x-full -translate-y-1/2 text-2xs text-content-subtle">
            right
          </span>
        </>
      )}
      {inputs === 'scoped' && (
        <>
          <span className="pointer-events-none absolute -left-3 top-[34%] -translate-x-full -translate-y-1/2 text-2xs text-content-subtle">
            from
          </span>
          <Handle
            type="target"
            position={Position.Left}
            id={HANDLE.inScope}
            className="!top-[72%]"
          />
          <span className="pointer-events-none absolute -left-3 top-[72%] -translate-x-full -translate-y-1/2 text-2xs text-node-validate">
            scope
          </span>
        </>
      )}
      {hasOutput && <Handle type="source" position={Position.Right} id={HANDLE.out} />}
    </div>
  )
}

/** Issues scoped to one node. Kept here so every renderer subscribes the same way. */
export function useNodeIssues(nodeId: string): ValidationIssue[] {
  const issues = useEditorStore((state) => state.issues)
  return useMemo(() => issues.filter((issue) => issue.nodeId === nodeId), [issues, nodeId])
}

/**
 * Run status of one node, or `undefined` outside a run. Subscribing to the single
 * entry — not the whole map — keeps a streaming run from re-rendering every node
 * on every step event.
 */
export function useNodeStepStatus(nodeId: string): StepStatus | undefined {
  return useEditorStore((state) => state.stepStatus[nodeId])
}

/**
 * Wall-clock ms this node's step took, or `undefined` before it finished one.
 *
 * Derived by the Run panel from the two log lines that bracket the step, so no
 * framework field has to carry it. It is wall clock and nothing more: a lazy
 * transformation reads near 0 ms because building a plan really is that cheap.
 * The time shows up on the read, on every validation rule (each one is a Spark
 * action) and on the writes — the steps that actually touch data.
 */
export function useNodeStepDuration(nodeId: string): number | undefined {
  return useEditorStore((state) => state.stepDuration[nodeId])
}

/**
 * This node's place in the execution order, 1-based. Recomputed from the graph
 * rather than stored, so it follows edits immediately; `undefined` for nodes no
 * chain reaches (notes, orphans), which have no step to number.
 */
export function useNodeOrdinal(nodeId: string): number | undefined {
  const nodes = useEditorStore((state) => state.nodes)
  const edges = useEditorStore((state) => state.edges)
  return useMemo(() => nodeOrdinals({ nodes, edges })[nodeId], [nodes, edges, nodeId])
}

/** Keeps both ends of a path readable: `/data/bronze/…/clientes`. */
export function truncateMiddle(value: string, max = 34): string {
  if (value.length <= max) return value
  const head = Math.ceil((max - 1) / 2)
  const tail = Math.floor((max - 1) / 2)
  return `${value.slice(0, head)}…${value.slice(value.length - tail)}`
}

/** Single-line clamp for previews that keep their full text in a title attribute. */
export function truncateEnd(value: string, max = 46): string {
  const flat = value.replace(/\s+/g, ' ').trim()
  return flat.length <= max ? flat : `${flat.slice(0, max - 1)}…`
}
