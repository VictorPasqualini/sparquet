/**
 * How a run status looks on a box, in one place.
 *
 * Shared by the job canvas (per-step status), the pipeline canvas (per-stage
 * status) and the two run panels, so "running", "failed" and "skipped" never
 * drift apart between the surfaces.
 *
 * Status is carried FOUR ways, never by colour alone, because the whole graph has
 * to be readable zoomed out and on a monochrome screen:
 *
 * | channel | what it is                                        |
 * |---------|---------------------------------------------------|
 * | `icon`  | a distinct glyph per status                       |
 * | `short` | the status in a word, printed on the box          |
 * | shape   | `bar` thickness and the footer's solid/dashed edge |
 * | colour  | `bar` / `footer` / `ring` tints                   |
 *
 * `label` is the full sentence a screen reader announces and a hover shows.
 */

import { CircleCheck, CircleSlash, CircleX, LoaderCircle, type LucideIcon } from 'lucide-react'

import type { StepStatus } from '@/types/studio'

export interface StepLook {
  icon: LucideIcon
  /** Read out by screen readers and shown on hover — status is never colour-only. */
  label: string
  /** One word, printed on the box next to the icon. */
  short: string
  ring: string
  chip: string
  spin?: string
  /**
   * The status bar across the top of a box: the channel that survives being
   * zoomed out, when text and icons are already too small to read.
   */
  bar: string
  /** The band under the body carrying the icon, the word and the duration. */
  footer: string
  /**
   * Border of the whole box. Applied only when neither selection nor an issue
   * claims the border — see the precedence note in `NodeShell`.
   */
  border: string
  /** The live state: the bar gets a sweeping highlight on top of its colour. */
  live?: boolean
}

/**
 * `pending` is deliberately absent. A step the run has not reached yet draws no
 * indicator at all — it is dimmed instead (`NodeShell`), which reads as "not
 * there yet" without adding a fifth badge to every box on the canvas.
 */
export const STEP_LOOK: Record<Exclude<StepStatus, 'pending'>, StepLook> = {
  running: {
    icon: LoaderCircle,
    label: 'Running now',
    short: 'Running',
    ring: 'ring-2 ring-state-info/60',
    chip: 'bg-state-info/15 text-state-info',
    spin: 'animate-spin motion-reduce:animate-none',
    // Thickest bar of the four, and the only animated one: at any moment a single
    // box is running, so one sweep on a 14-node graph is a beacon, not noise.
    bar: 'h-1 bg-state-info/35',
    footer: 'border-state-info/30 bg-state-info/10 text-state-info',
    border: 'border-state-info/60',
    live: true,
  },
  success: {
    icon: CircleCheck,
    label: 'Ran successfully',
    short: 'Done',
    ring: 'ring-2 ring-state-success/45',
    chip: 'bg-state-success/15 text-state-success',
    bar: 'h-[3px] bg-state-success/70',
    footer: 'border-state-success/25 bg-state-success/10 text-state-success',
    border: 'border-state-success/45',
  },
  error: {
    icon: CircleX,
    label: 'Failed during the run',
    short: 'Failed',
    ring: 'ring-2 ring-state-danger/45',
    chip: 'bg-state-danger/15 text-state-danger',
    bar: 'h-1 bg-state-danger',
    footer: 'border-state-danger/30 bg-state-danger/10 text-state-danger',
    border: 'border-state-danger/60',
  },
  skipped: {
    icon: CircleSlash,
    label: 'Skipped — the run never reached it',
    short: 'Skipped',
    ring: 'ring-1 ring-line-strong',
    chip: 'bg-surface-sunken text-content-subtle',
    // Hatched, not tinted: "nothing happened here" has to be distinguishable from
    // "it succeeded" with the colour taken away.
    bar: 'h-[3px] bg-line-strong [background-image:repeating-linear-gradient(45deg,transparent_0_3px,rgb(var(--surface))_3px_6px)]',
    footer: 'border-dashed border-line-strong bg-surface-sunken text-content-subtle',
    border: 'border-dashed border-line-strong',
  },
}

/** The look of one status, or `null` for `pending`/absent — nothing to draw yet. */
export function stepLook(status: StepStatus | undefined): StepLook | null {
  return status && status !== 'pending' ? STEP_LOOK[status] : null
}
