/**
 * The tab strip under a page header, for screens whose sections are routes.
 *
 * `WorkspaceTabs` switches surfaces inside one editor and owns its state; these
 * are links. The difference matters: a section that is a URL can be linked to,
 * bookmarked, opened in a second window and reached by the back button, and a
 * screen that governs a shared runner is exactly where somebody wants to send
 * somebody else a link.
 *
 * So this is a `nav` of links rather than a tablist. A tablist that navigates
 * lies to a screen reader about what pressing it does, and takes the keyboard
 * contract of a control it is not.
 */

import type { LucideIcon } from 'lucide-react'
import { NavLink } from 'react-router-dom'
import type { ReactNode } from 'react'

import { cn } from '@/lib/utils/cn'

export interface PageTab {
  to: string
  label: string
  icon?: LucideIcon
  /** Matches only the exact path — for the tab that owns the parent route. */
  end?: boolean
  /** A count or dot after the label. */
  badge?: ReactNode
  title?: string
}

export function PageTabs({
  tabs,
  ariaLabel,
  className,
}: {
  tabs: PageTab[]
  ariaLabel: string
  className?: string
}) {
  return (
    <nav
      aria-label={ariaLabel}
      className={cn('-mt-1 mb-4 flex items-center gap-1 overflow-x-auto border-b border-line', className)}
    >
      {tabs.map((tab) => {
        const Icon = tab.icon
        return (
          <NavLink
            key={tab.to}
            to={tab.to}
            end={tab.end}
            title={tab.title}
            className={({ isActive }) =>
              cn(
                // The active tab is marked by a line that sits ON the border of
                // the strip, so the selected section reads as continuing into
                // the content below it rather than as a separate chip.
                'flex shrink-0 items-center gap-1.5 whitespace-nowrap border-b-2 px-2.5 py-2 text-xs transition-colors',
                isActive
                  ? 'border-brand-500 text-content'
                  : 'border-transparent text-content-subtle hover:border-line-strong hover:text-content',
              )
            }
          >
            {Icon ? <Icon className="h-3.5 w-3.5" aria-hidden /> : null}
            {tab.label}
            {tab.badge}
          </NavLink>
        )
      })}
    </nav>
  )
}
