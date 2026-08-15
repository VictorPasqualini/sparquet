import * as RadixMenu from '@radix-ui/react-dropdown-menu'
import type { ReactNode } from 'react'

import { cn } from '@/lib/utils/cn'

export const Menu = RadixMenu.Root
export const MenuTrigger = RadixMenu.Trigger

export function MenuContent({
  children,
  align = 'end',
  className,
}: {
  children: ReactNode
  align?: 'start' | 'center' | 'end'
  className?: string
}) {
  return (
    <RadixMenu.Portal>
      <RadixMenu.Content
        align={align}
        sideOffset={6}
        collisionPadding={12}
        className={cn(
          'z-50 min-w-[11rem] rounded-xl border border-line bg-surface-overlay p-1 shadow-pop animate-slide-up',
          className,
        )}
      >
        {children}
      </RadixMenu.Content>
    </RadixMenu.Portal>
  )
}

export function MenuItem({
  children,
  onSelect,
  icon,
  shortcut,
  danger,
  disabled,
}: {
  children: ReactNode
  onSelect?: () => void
  icon?: ReactNode
  shortcut?: string
  danger?: boolean
  disabled?: boolean
}) {
  return (
    <RadixMenu.Item
      disabled={disabled}
      onSelect={onSelect}
      className={cn(
        'flex cursor-pointer select-none items-center gap-2 rounded-lg px-2 py-1.5 text-xs outline-none',
        'data-[highlighted]:bg-surface-sunken data-[disabled]:pointer-events-none data-[disabled]:opacity-50',
        danger ? 'text-state-danger' : 'text-content',
        '[&_svg]:h-3.5 [&_svg]:w-3.5 [&_svg]:text-content-subtle',
      )}
    >
      {icon}
      <span className="flex-1">{children}</span>
      {shortcut && (
        <kbd className="font-mono text-2xs text-content-subtle">{shortcut}</kbd>
      )}
    </RadixMenu.Item>
  )
}

export function MenuSeparator() {
  return <RadixMenu.Separator className="my-1 h-px bg-line" />
}

export function MenuLabel({ children }: { children: ReactNode }) {
  return (
    <RadixMenu.Label className="px-2 py-1 text-2xs font-semibold uppercase tracking-wide text-content-subtle">
      {children}
    </RadixMenu.Label>
  )
}
