import { Loader2 } from 'lucide-react'
import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from 'react'

import { cn } from '@/lib/utils/cn'

export type ButtonVariant = 'primary' | 'secondary' | 'outline' | 'ghost' | 'danger' | 'success'
export type ButtonSize = 'xs' | 'sm' | 'md' | 'lg'

const VARIANTS: Record<ButtonVariant, string> = {
  primary:
    'bg-brand-500 text-black hover:bg-brand-400 active:bg-brand-600 shadow-sm font-semibold',
  secondary:
    'bg-surface-raised text-content border border-line hover:border-line-strong hover:bg-surface-sunken',
  outline:
    'border border-brand-500/60 text-brand-600 dark:text-brand-400 hover:bg-brand-500/10',
  ghost: 'text-content-muted hover:bg-surface-sunken hover:text-content',
  // `--danger`/`--success` lighten in dark theme, so the label follows the theme
  // instead of being pinned to white (2.8:1 / 1.9:1 on the dark tokens).
  danger: 'bg-state-danger text-content-inverted hover:opacity-90 font-semibold',
  success: 'bg-state-success text-content-inverted hover:opacity-90 font-semibold',
}

const SIZES: Record<ButtonSize, string> = {
  xs: 'h-7 px-2 text-2xs gap-1 rounded-md',
  sm: 'h-8 px-2.5 text-xs gap-1.5 rounded-lg',
  md: 'h-9 px-3.5 text-sm gap-2 rounded-lg',
  lg: 'h-11 px-5 text-sm gap-2 rounded-xl',
}

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: ButtonSize
  loading?: boolean
  icon?: ReactNode
  trailing?: ReactNode
  fullWidth?: boolean
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  {
    className,
    variant = 'secondary',
    size = 'md',
    loading = false,
    icon,
    trailing,
    fullWidth,
    disabled,
    children,
    type = 'button',
    ...props
  },
  ref,
) {
  return (
    <button
      ref={ref}
      type={type}
      disabled={disabled || loading}
      className={cn(
        'inline-flex select-none items-center justify-center whitespace-nowrap transition-colors',
        'disabled:pointer-events-none disabled:opacity-50',
        VARIANTS[variant],
        SIZES[size],
        fullWidth && 'w-full',
        className,
      )}
      {...props}
    >
      {loading ? (
        <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
      ) : (
        icon
      )}
      {children}
      {trailing}
    </button>
  )
})

export interface IconButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: ButtonSize
  /** Required — icon-only controls must expose an accessible name. */
  label: string
  active?: boolean
}

const ICON_SIZES: Record<ButtonSize, string> = {
  xs: 'h-6 w-6 rounded-md [&_svg]:h-3 [&_svg]:w-3',
  sm: 'h-7 w-7 rounded-md [&_svg]:h-3.5 [&_svg]:w-3.5',
  md: 'h-9 w-9 rounded-lg [&_svg]:h-4 [&_svg]:w-4',
  lg: 'h-11 w-11 rounded-xl [&_svg]:h-5 [&_svg]:w-5',
}

export const IconButton = forwardRef<HTMLButtonElement, IconButtonProps>(function IconButton(
  { className, variant = 'ghost', size = 'md', label, active, children, type = 'button', ...props },
  ref,
) {
  return (
    <button
      ref={ref}
      type={type}
      aria-label={label}
      title={label}
      aria-pressed={active}
      className={cn(
        'inline-flex items-center justify-center transition-colors disabled:pointer-events-none disabled:opacity-50',
        VARIANTS[variant],
        ICON_SIZES[size],
        active && 'bg-brand-500/15 text-brand-600 dark:text-brand-400',
        className,
      )}
      {...props}
    >
      {children}
    </button>
  )
})
