/**
 * A runner error, bounded.
 *
 * Spark reports failures as stack traces thousands of characters long. Printed
 * straight into a side panel, one of them pushes every other section off the
 * screen; printed into a canvas box, it swallows the box. So the message lives in
 * its own scroll area with a fixed ceiling, and carries a copy button — the first
 * thing anyone does with a stack trace is paste it somewhere else.
 */

import { Check, Copy } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

import { cn } from '@/lib/utils/cn'
import { copyText } from '@/lib/utils/download'

export type ErrorCardTone = 'danger' | 'warning'

const TONES: Record<ErrorCardTone, { box: string; text: string }> = {
  danger: {
    box: 'border-state-danger/30 bg-state-danger/10',
    text: 'text-state-danger',
  },
  warning: {
    box: 'border-state-warning/30 bg-state-warning/10',
    text: 'text-state-warning',
  },
}

export interface ErrorCardProps {
  message: string
  tone?: ErrorCardTone
  /** Height ceiling of the scroll area. `sm` suits a step row, `md` a panel. */
  size?: 'sm' | 'md'
  /** Copy is worth a button on a panel, not on a one-line row. */
  copyable?: boolean
  className?: string
}

export function ErrorCard({
  message,
  tone = 'danger',
  size = 'md',
  copyable = true,
  className,
}: ErrorCardProps) {
  const [copied, setCopied] = useState(false)
  const timer = useRef<number | undefined>(undefined)
  const look = TONES[tone]

  useEffect(() => () => window.clearTimeout(timer.current), [])

  const copy = () => {
    void copyText(message).then((ok) => {
      if (!ok) return
      setCopied(true)
      window.clearTimeout(timer.current)
      timer.current = window.setTimeout(() => setCopied(false), 1400)
    })
  }

  return (
    <div className={cn('relative rounded border', look.box, className)}>
      {/* The scroll lives on the text, not on the card, so the copy button stays
          pinned while a long trace scrolls under it. `nowheel` keeps a wheel
          gesture inside the box instead of zooming the canvas behind it. */}
      <pre
        className={cn(
          'nowheel overflow-auto whitespace-pre-wrap break-words px-2 py-1 font-mono text-2xs leading-relaxed',
          size === 'sm' ? 'max-h-20' : 'max-h-40',
          copyable && 'pr-7',
          look.text,
        )}
      >
        {message}
      </pre>
      {copyable && (
        <button
          type="button"
          onClick={copy}
          aria-label={copied ? 'Error copied' : 'Copy the error'}
          title={copied ? 'Copied' : 'Copy the error'}
          className={cn(
            'nodrag absolute right-1 top-1 rounded p-1 transition-colors',
            'text-content-subtle hover:bg-surface hover:text-content',
            'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/50',
          )}
        >
          {copied ? (
            <Check className="h-3 w-3" aria-hidden />
          ) : (
            <Copy className="h-3 w-3" aria-hidden />
          )}
        </button>
      )}
    </div>
  )
}
