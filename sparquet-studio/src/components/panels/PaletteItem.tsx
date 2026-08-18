import { Plus } from 'lucide-react'
import type { DragEvent, ReactNode } from 'react'

import type { NodeAccent } from '@/catalog'
import { Tooltip } from '@/components/ui'
import { cn } from '@/lib/utils/cn'

/** Drag MIME type shared with the canvas drop handler. */
export const PALETTE_DRAG_TYPE = 'application/sparquet-node'

/** What a palette drag carries; the canvas turns it into a node at the drop point. */
export type PaletteDragPayload =
  | { kind: 'transform'; type: string }
  | { kind: 'source'; format: string }
  | { kind: 'sink'; format: string }
  | { kind: 'validation'; type: string }
  | { kind: 'note' }

/** Icon tile tint per node family. Written out so Tailwind keeps the classes. */
const ACCENT_CHIP: Record<NodeAccent, string> = {
  input: 'bg-node-input/12 text-node-input ring-node-input/25',
  transform: 'bg-node-transform/12 text-node-transform ring-node-transform/25',
  combine: 'bg-node-combine/12 text-node-combine ring-node-combine/25',
  control: 'bg-node-control/12 text-node-control ring-node-control/25',
  inspect: 'bg-node-inspect/12 text-node-inspect ring-node-inspect/25',
  validate: 'bg-node-validate/12 text-node-validate ring-node-validate/25',
  output: 'bg-node-output/12 text-node-output ring-node-output/25',
}

export interface PaletteItemProps {
  icon: ReactNode
  label: string
  /** One line shown on hover/focus; also the accessible description. */
  summary: string
  accent: NodeAccent
  onAdd: () => void
  dragPayload: PaletteDragPayload
}

/**
 * One draggable entry of the node palette.
 *
 * It is a real button so the list stays keyboard operable — Enter adds the node —
 * while `draggable` covers the drop-anywhere path.
 */
export function PaletteItem({
  icon,
  label,
  summary,
  accent,
  onAdd,
  dragPayload,
}: PaletteItemProps) {
  const handleDragStart = (event: DragEvent<HTMLButtonElement>) => {
    event.dataTransfer.setData(PALETTE_DRAG_TYPE, JSON.stringify(dragPayload))
    event.dataTransfer.effectAllowed = 'copy'
  }

  return (
    <Tooltip content={summary} side="right">
      <button
        type="button"
        data-palette-item=""
        draggable
        onDragStart={handleDragStart}
        onClick={onAdd}
        className={cn(
          'group flex w-full items-center gap-2.5 rounded-lg border border-transparent',
          'px-2 py-1.5 text-left transition-colors',
          'hover:border-line hover:bg-surface-raised active:bg-surface-sunken',
        )}
      >
        <span
          className={cn(
            'flex h-6 w-6 shrink-0 items-center justify-center rounded-md ring-1 ring-inset',
            ACCENT_CHIP[accent],
          )}
        >
          {icon}
        </span>
        <span className="min-w-0 flex-1 truncate text-xs font-medium text-content">
          {label}
        </span>
        <Plus
          className={cn(
            'h-3.5 w-3.5 shrink-0 text-content-subtle opacity-0 transition-opacity',
            'group-hover:opacity-100 group-focus-visible:opacity-100',
          )}
          aria-hidden
        />
      </button>
    </Tooltip>
  )
}
