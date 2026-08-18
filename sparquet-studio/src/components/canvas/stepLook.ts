/**
 * How a run status looks on a box, in one place.
 *
 * Shared by the pipeline canvas (per-transformation status) and the pipeline
 * canvas (per-stage status), so "running", "failed" and "skipped" never drift
 * apart between the two surfaces. Status is carried by the ICON as well as the
 * colour, and `label` is what a screen reader announces — never colour alone.
 */

import { CircleCheck, CircleSlash, CircleX, LoaderCircle, type LucideIcon } from 'lucide-react'

import type { StepStatus } from '@/types/studio'

export interface StepLook {
  icon: LucideIcon
  /** Read out by screen readers and shown on hover — status is never colour-only. */
  label: string
  ring: string
  chip: string
  spin?: string
}

/**
 * `pending` is deliberately absent: before a step runs there is nothing worth
 * drawing, so the whole indicator stays hidden until the runner reaches it.
 */
export const STEP_LOOK: Record<Exclude<StepStatus, 'pending'>, StepLook> = {
  running: {
    icon: LoaderCircle,
    label: 'Running',
    ring: 'ring-2 ring-state-info/60',
    chip: 'bg-state-info/15 text-state-info',
    spin: 'animate-pulse',
  },
  success: {
    icon: CircleCheck,
    label: 'Ran successfully',
    ring: 'ring-2 ring-state-success/45',
    chip: 'bg-state-success/15 text-state-success',
  },
  error: {
    icon: CircleX,
    label: 'Failed during the run',
    ring: 'ring-2 ring-state-danger/45',
    chip: 'bg-state-danger/15 text-state-danger',
  },
  skipped: {
    icon: CircleSlash,
    label: 'Skipped — the run never reached it',
    ring: 'ring-1 ring-line-strong',
    chip: 'bg-surface-sunken text-content-subtle',
  },
}

/** The look of one status, or `null` for `pending`/absent — nothing to draw yet. */
export function stepLook(status: StepStatus | undefined): StepLook | null {
  return status && status !== 'pending' ? STEP_LOOK[status] : null
}
