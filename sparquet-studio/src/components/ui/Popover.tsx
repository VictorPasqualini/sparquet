import * as RadixPopover from '@radix-ui/react-popover'
import * as RadixTooltip from '@radix-ui/react-tooltip'
import type { ReactNode } from 'react'

import { cn } from '@/lib/utils/cn'

export function TooltipProvider({ children }: { children: ReactNode }) {
  return (
    <RadixTooltip.Provider delayDuration={350} skipDelayDuration={200}>
      {children}
    </RadixTooltip.Provider>
  )
}

export interface TooltipProps {
  content: ReactNode
  children: ReactNode
  side?: 'top' | 'right' | 'bottom' | 'left'
  /** Keyboard hint rendered on the right, e.g. ⌘K. */
  shortcut?: string
  disabled?: boolean
}

export function Tooltip({ content, children, side = 'top', shortcut, disabled }: TooltipProps) {
  if (disabled) return <>{children}</>
  return (
    <RadixTooltip.Root>
      <RadixTooltip.Trigger asChild>{children}</RadixTooltip.Trigger>
      <RadixTooltip.Portal>
        <RadixTooltip.Content
          side={side}
          sideOffset={8}
          className="z-50 flex max-w-xs items-center gap-2 rounded-lg border border-line bg-surface-overlay px-2.5 py-1.5 text-xs text-content shadow-pop animate-fade-in"
        >
          <span className="leading-relaxed">{content}</span>
          {shortcut && (
            <kbd className="rounded border border-line bg-surface-sunken px-1 py-0.5 font-mono text-2xs text-content-muted">
              {shortcut}
            </kbd>
          )}
        </RadixTooltip.Content>
      </RadixTooltip.Portal>
    </RadixTooltip.Root>
  )
}

export interface InfoPopoverProps {
  title: string
  content: string
  children: ReactNode
}

/** Long-form help attached to a control. Content is plain text with `code` spans. */
export function InfoPopover({ title, content, children }: InfoPopoverProps) {
  return (
    <RadixPopover.Root>
      <RadixPopover.Trigger asChild>{children}</RadixPopover.Trigger>
      <RadixPopover.Portal>
        <RadixPopover.Content
          side="right"
          align="start"
          sideOffset={10}
          collisionPadding={12}
          className="z-50 w-80 rounded-xl border border-line bg-surface-overlay p-3 shadow-pop animate-slide-left"
        >
          <p className="mb-1.5 text-xs font-semibold text-content">{title}</p>
          <div className="space-y-2 text-xs leading-relaxed text-content-muted">
            {content.split('\n\n').map((paragraph, index) => (
              <p key={index}>{renderInlineCode(paragraph)}</p>
            ))}
          </div>
          <RadixPopover.Arrow className="fill-[rgb(var(--surface-overlay))]" />
        </RadixPopover.Content>
      </RadixPopover.Portal>
    </RadixPopover.Root>
  )
}

/** Renders `backticked` spans as inline code; everything else stays plain text. */
export function renderInlineCode(text: string): ReactNode[] {
  return text.split(/(`[^`]+`)/g).map((part, index) => {
    if (part.startsWith('`') && part.endsWith('`') && part.length > 2) {
      return (
        <code
          key={index}
          className={cn(
            'rounded bg-surface-sunken px-1 py-0.5 font-mono text-[0.95em] text-brand-600',
            'dark:text-brand-400',
          )}
        >
          {part.slice(1, -1)}
        </code>
      )
    }
    return <span key={index}>{part}</span>
  })
}

export const Popover = RadixPopover.Root
export const PopoverTrigger = RadixPopover.Trigger

export function PopoverContent({
  children,
  className,
  align = 'end',
  side = 'bottom',
  sideOffset = 8,
}: {
  children: ReactNode
  className?: string
  align?: 'start' | 'center' | 'end'
  side?: 'top' | 'right' | 'bottom' | 'left'
  sideOffset?: number
}) {
  return (
    <RadixPopover.Portal>
      <RadixPopover.Content
        align={align}
        side={side}
        sideOffset={sideOffset}
        collisionPadding={12}
        className={cn(
          'z-50 rounded-xl border border-line bg-surface-overlay p-2 shadow-pop animate-slide-up',
          className,
        )}
      >
        {children}
      </RadixPopover.Content>
    </RadixPopover.Portal>
  )
}
