import { Loader2 } from 'lucide-react'
import { useRef, type ReactNode } from 'react'

import { cn } from '@/lib/utils/cn'

/* ------------------------------------------------------------------ Badge */

export type BadgeTone = 'neutral' | 'brand' | 'success' | 'warning' | 'danger' | 'info'

const BADGE_TONES: Record<BadgeTone, string> = {
  neutral: 'bg-surface-sunken text-content-muted border-line',
  brand: 'bg-brand-500/12 text-brand-600 dark:text-brand-400 border-brand-500/30',
  success: 'bg-state-success/12 text-state-success border-state-success/30',
  warning: 'bg-state-warning/12 text-state-warning border-state-warning/30',
  danger: 'bg-state-danger/12 text-state-danger border-state-danger/30',
  info: 'bg-state-info/12 text-state-info border-state-info/30',
}

export function Badge({
  children,
  tone = 'neutral',
  className,
  icon,
}: {
  children: ReactNode
  tone?: BadgeTone
  className?: string
  icon?: ReactNode
}) {
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-2xs font-medium',
        '[&_svg]:h-3 [&_svg]:w-3',
        BADGE_TONES[tone],
        className,
      )}
    >
      {icon}
      {children}
    </span>
  )
}

/* -------------------------------------------------------------------- Kbd */

export function Kbd({ children }: { children: ReactNode }) {
  return (
    <kbd className="inline-flex h-5 min-w-[1.25rem] items-center justify-center rounded border border-line bg-surface-sunken px-1 font-mono text-2xs text-content-muted">
      {children}
    </kbd>
  )
}

/* ---------------------------------------------------------------- Spinner */

export function Spinner({ className }: { className?: string }) {
  return <Loader2 className={cn('h-4 w-4 animate-spin text-content-subtle', className)} />
}

/* ------------------------------------------------------------- EmptyState */

export function EmptyState({
  icon,
  title,
  description,
  action,
  className,
}: {
  icon?: ReactNode
  title: string
  description?: ReactNode
  action?: ReactNode
  className?: string
}) {
  return (
    <div
      className={cn(
        'flex flex-col items-center justify-center gap-3 px-6 py-12 text-center',
        className,
      )}
    >
      {icon && (
        <div className="flex h-12 w-12 items-center justify-center rounded-2xl border border-line bg-surface-sunken text-content-subtle [&_svg]:h-5 [&_svg]:w-5">
          {icon}
        </div>
      )}
      <div className="space-y-1">
        <p className="text-sm font-medium text-content">{title}</p>
        {description && (
          <p className="mx-auto max-w-sm text-xs leading-relaxed text-content-muted">
            {description}
          </p>
        )}
      </div>
      {action}
    </div>
  )
}

/* ----------------------------------------------------------- SectionTitle */

export function SectionTitle({
  children,
  action,
  className,
}: {
  children: ReactNode
  action?: ReactNode
  className?: string
}) {
  return (
    <div className={cn('flex items-center justify-between gap-2', className)}>
      <h3 className="text-2xs font-semibold uppercase tracking-wider text-content-subtle">
        {children}
      </h3>
      {action}
    </div>
  )
}

/* -------------------------------------------------------------- Separator */

export function Separator({ className }: { className?: string }) {
  return <div className={cn('h-px w-full bg-line', className)} />
}

/* ----------------------------------------------------------- SegmentedTab */

export interface SegmentedOption<T extends string> {
  value: T
  label: ReactNode
  title?: string
}

/**
 * A single-select choice control, not a view switcher: it is a radiogroup, so it
 * owes the radio keyboard contract — one stop in the tab order, arrows move and
 * select within the group.
 */
export function Segmented<T extends string>({
  value,
  onChange,
  options,
  className,
  size = 'md',
  ariaLabel,
}: {
  value: T
  onChange: (value: T) => void
  options: SegmentedOption<T>[]
  className?: string
  size?: 'sm' | 'md'
  ariaLabel?: string
}) {
  const groupRef = useRef<HTMLDivElement>(null)

  // The radio pattern moves focus with the selection, or the next arrow press
  // would step from whichever button the focus was left behind on.
  const step = (from: number, delta: number) => {
    if (options.length === 0) return
    const index = (from + delta + options.length) % options.length
    onChange(options[index].value)
    groupRef.current?.querySelectorAll<HTMLButtonElement>('[role="radio"]')[index]?.focus()
  }

  return (
    <div
      ref={groupRef}
      role="radiogroup"
      aria-label={ariaLabel}
      className={cn(
        'inline-flex items-center gap-0.5 rounded-lg border border-line bg-surface-sunken p-0.5',
        className,
      )}
    >
      {options.map((option, index) => {
        const active = option.value === value
        return (
          <button
            key={option.value}
            role="radio"
            type="button"
            aria-checked={active}
            tabIndex={active ? 0 : -1}
            title={option.title}
            onClick={() => onChange(option.value)}
            onKeyDown={(event) => {
              if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
                event.preventDefault()
                step(index, 1)
              } else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
                event.preventDefault()
                step(index, -1)
              }
            }}
            className={cn(
              'rounded-md font-medium transition-colors',
              size === 'sm' ? 'px-2 py-1 text-2xs' : 'px-2.5 py-1.5 text-xs',
              active
                ? 'bg-surface text-content shadow-sm'
                : 'text-content-subtle hover:text-content',
            )}
          >
            {option.label}
          </button>
        )
      })}
    </div>
  )
}
