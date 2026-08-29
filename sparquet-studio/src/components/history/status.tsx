/**
 * How a persisted execution status looks, in one place.
 *
 * The history panel, the canvas banner and the Inspector all describe the same
 * `ExecutionStatus`, and a run that reads as failed in one and neutral in another
 * is worse than no status at all.
 */

import { CircleCheck, CircleStop, CircleX, Clock3, MinusCircle } from 'lucide-react'

import { Spinner, type BadgeTone } from '@/components/ui'
import type { ExecutionStatus } from '@/types/history'

export function StatusIcon({ status, className }: { status: ExecutionStatus; className?: string }) {
  const size = className ?? 'h-4 w-4'
  switch (status) {
    case 'success':
      return <CircleCheck className={`${size} shrink-0 text-state-success`} aria-hidden />
    case 'failed':
      return <CircleX className={`${size} shrink-0 text-state-danger`} aria-hidden />
    case 'cancelled':
      return <CircleStop className={`${size} shrink-0 text-state-warning`} aria-hidden />
    case 'skipped':
      return <MinusCircle className={`${size} shrink-0 text-content-subtle`} aria-hidden />
    case 'running':
      return <Spinner className={`${size} shrink-0`} />
    default:
      return <Clock3 className={`${size} shrink-0 text-content-subtle`} aria-hidden />
  }
}

export function statusTone(status: ExecutionStatus): BadgeTone {
  switch (status) {
    case 'success':
      return 'success'
    case 'failed':
      return 'danger'
    // Stopped on purpose: a warning, never a failure — nothing broke.
    case 'cancelled':
      return 'warning'
    case 'running':
      return 'info'
    default:
      return 'neutral'
  }
}

/** An ISO timestamp as the local clock reads it. Em dash when the step never ran. */
export function formatTimestamp(iso: string | null): string {
  if (!iso) return '—'
  const parsed = new Date(iso)
  if (Number.isNaN(parsed.getTime())) return iso
  return parsed.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}
