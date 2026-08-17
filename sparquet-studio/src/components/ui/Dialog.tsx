import * as RadixDialog from '@radix-ui/react-dialog'
import { X } from 'lucide-react'
import { useState, type ReactNode } from 'react'

import { cn } from '@/lib/utils/cn'

import { Button, type ButtonVariant } from './Button'

export interface ModalProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  title: string
  description?: string
  children: ReactNode
  footer?: ReactNode
  size?: 'sm' | 'md' | 'lg' | 'xl'
  /** Hides the close button for pipelines that must be completed. */
  dismissible?: boolean
}

const SIZES = {
  sm: 'max-w-sm',
  md: 'max-w-lg',
  lg: 'max-w-2xl',
  xl: 'max-w-4xl',
}

export function Modal({
  open,
  onOpenChange,
  title,
  description,
  children,
  footer,
  size = 'md',
  dismissible = true,
}: ModalProps) {
  return (
    <RadixDialog.Root open={open} onOpenChange={onOpenChange}>
      <RadixDialog.Portal>
        <RadixDialog.Overlay className="fixed inset-0 z-40 bg-black/50 backdrop-blur-sm animate-fade-in" />
        <RadixDialog.Content
          onInteractOutside={(event) => {
            if (!dismissible) event.preventDefault()
          }}
          className={cn(
            'fixed left-1/2 top-1/2 z-50 w-[calc(100vw-2rem)] -translate-x-1/2 -translate-y-1/2',
            'rounded-2xl border border-line bg-surface shadow-pop animate-slide-up',
            'flex max-h-[calc(100vh-4rem)] flex-col',
            SIZES[size],
          )}
        >
          <div className="flex items-start justify-between gap-4 border-b border-line px-5 py-4">
            <div className="space-y-1">
              <RadixDialog.Title className="text-sm font-semibold text-content">
                {title}
              </RadixDialog.Title>
              {description && (
                <RadixDialog.Description className="text-xs leading-relaxed text-content-muted">
                  {description}
                </RadixDialog.Description>
              )}
            </div>
            {dismissible && (
              <RadixDialog.Close asChild>
                <button
                  aria-label="Close"
                  className="rounded-lg p-1 text-content-subtle transition-colors hover:bg-surface-sunken hover:text-content"
                >
                  <X className="h-4 w-4" />
                </button>
              </RadixDialog.Close>
            )}
          </div>

          <div className="scroll-area flex-1 px-5 py-4">{children}</div>

          {footer && (
            <div className="flex items-center justify-end gap-2 border-t border-line px-5 py-3">
              {footer}
            </div>
          )}
        </RadixDialog.Content>
      </RadixDialog.Portal>
    </RadixDialog.Root>
  )
}

export interface ConfirmOptions {
  title: string
  message: ReactNode
  confirmLabel?: string
  cancelLabel?: string
  variant?: ButtonVariant
}

/**
 * Imperative confirmation modal.
 *
 * const [confirm, confirmDialog] = useConfirm()
 * if (await confirm({ title: 'Delete workflow', message: '…' })) { … }
 */
export function useConfirm(): [
  (options: ConfirmOptions) => Promise<boolean>,
  ReactNode,
] {
  const [state, setState] = useState<{
    options: ConfirmOptions
    resolve: (value: boolean) => void
  } | null>(null)

  const confirm = (options: ConfirmOptions) =>
    new Promise<boolean>((resolve) => setState({ options, resolve }))

  const settle = (value: boolean) => {
    state?.resolve(value)
    setState(null)
  }

  const dialog = state ? (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) settle(false)
      }}
      title={state.options.title}
      size="sm"
      footer={
        <>
          <Button variant="ghost" onClick={() => settle(false)}>
            {state.options.cancelLabel ?? 'Cancel'}
          </Button>
          <Button variant={state.options.variant ?? 'danger'} onClick={() => settle(true)}>
            {state.options.confirmLabel ?? 'Confirm'}
          </Button>
        </>
      }
    >
      <div className="text-sm leading-relaxed text-content-muted">{state.options.message}</div>
    </Modal>
  ) : null

  return [confirm, dialog]
}

export const DialogClose = RadixDialog.Close
