/**
 * The frame every screen sits in: one width, one padding, one header shape.
 *
 * Before this each screen invented its own — five different max widths, two
 * paddings, titles at two sizes, an icon chip on three screens out of ten. The
 * differences were accidental, and they showed: moving between screens the
 * content jumped sideways and the title changed size for no reason.
 *
 * The shape is the SQL editor's, because that is the one that reads best: a
 * small title, a sentence under it that says what the screen is FOR rather than
 * what it is called, and the actions on the right of the same line. The icon
 * chip is optional and only earns its place on a screen with a single subject.
 */

import type { ReactNode } from 'react'

/**
 * How wide the content may get. Reading width, not screen width: prose and
 * forms stop being readable long before a monitor runs out, while a table or a
 * canvas wants everything it can have.
 */
export type PageWidth = 'narrow' | 'default' | 'wide' | 'full'

const WIDTHS: Record<PageWidth, string> = {
  /** A single column of text — a lesson, an empty state. */
  narrow: 'max-w-3xl',
  /** Forms and panels, one column of them. */
  default: 'max-w-5xl',
  /** Lists and grids of cards. */
  wide: 'max-w-6xl',
  /** Editors and tables, where the data is the point. */
  full: 'max-w-[100rem]',
}

export interface PageShellProps {
  width?: PageWidth
  /** Extra classes for the content wrapper — spacing between sections, mostly. */
  className?: string
  children: ReactNode
}

export function PageShell({ width = 'wide', className = '', children }: PageShellProps) {
  return (
    <div className={`mx-auto w-full ${WIDTHS[width]} px-6 py-6 animate-fade-in ${className}`}>
      {children}
    </div>
  )
}

export interface PageHeaderProps {
  /** A lucide icon element. Sized here, so callers pass `<Database />` bare. */
  icon?: ReactNode
  title: ReactNode
  /** One or two sentences. What the screen is for, not what it is called. */
  description?: ReactNode
  /** Buttons, badges — pushed to the right of the title line. */
  actions?: ReactNode
  className?: string
}

export function PageHeader({
  icon,
  title,
  description,
  actions,
  className = '',
}: PageHeaderProps) {
  return (
    <header className={`mb-5 flex flex-wrap items-start gap-x-3 gap-y-2 ${className}`}>
      {icon ? (
        <span
          className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-line bg-surface-sunken text-content-muted [&_svg]:h-4 [&_svg]:w-4"
          aria-hidden
        >
          {icon}
        </span>
      ) : null}
      <div className="min-w-0 flex-1 space-y-1">
        <h1 className="text-sm font-semibold text-content">{title}</h1>
        {description ? (
          <p className="max-w-3xl text-xs leading-relaxed text-content-muted">{description}</p>
        ) : null}
      </div>
      {actions ? (
        <div className="flex shrink-0 flex-wrap items-center gap-2">{actions}</div>
      ) : null}
    </header>
  )
}
