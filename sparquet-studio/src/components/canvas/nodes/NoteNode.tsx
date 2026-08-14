import { NodeToolbar, Position, type NodeProps } from '@xyflow/react'
import { Copy, PenLine, Trash2 } from 'lucide-react'
import { memo, useRef, useState, type KeyboardEvent } from 'react'

import { IconButton, Textarea, Tooltip } from '@/components/ui'
import { cn } from '@/lib/utils/cn'
import { useEditorStore } from '@/store/editor'
import type { NoteNode as NoteNodeType, NoteNodeData } from '@/types/studio'

const TONES: Record<NoteNodeData['tone'], string> = {
  brand: 'border-brand-500/40 bg-brand-500/10',
  neutral: 'border-line bg-surface-raised',
  info: 'border-state-info/40 bg-state-info/10',
  warning: 'border-state-warning/40 bg-state-warning/10',
}

export const NoteNode = memo(function NoteNodeRenderer({
  id,
  data,
  selected,
}: NodeProps<NoteNodeType>) {
  const updateNodeData = useEditorStore((state) => state.updateNodeData)
  const duplicateNode = useEditorStore((state) => state.duplicateNode)
  const removeNodes = useEditorStore((state) => state.removeNodes)

  const [editing, setEditing] = useState(false)
  const [draft, setDraft] = useState(data.text)
  const [hovered, setHovered] = useState(false)
  // Escape must discard the draft, and blur fires right after — this skips the save.
  const cancelled = useRef(false)

  const startEditing = () => {
    // A dblclick inside the textarea bubbles to the wrapper; restarting here would
    // reset the draft to the last committed text and discard everything typed.
    if (editing) return
    setDraft(data.text)
    cancelled.current = false
    setEditing(true)
  }

  const commit = () => {
    setEditing(false)
    if (cancelled.current || draft === data.text) return
    updateNodeData(id, { text: draft })
  }

  const onKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === 'Escape') {
      cancelled.current = true
      event.currentTarget.blur()
    }
    if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
      event.currentTarget.blur()
    }
  }

  return (
    <div
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      onDoubleClick={startEditing}
      className={cn(
        'w-[240px] rounded-xl border px-3 py-2.5 shadow-card transition-shadow',
        TONES[data.tone] ?? TONES.brand,
        selected && 'ring-2 ring-brand-500/40',
      )}
    >
      <NodeToolbar
        nodeId={id}
        isVisible={hovered || selected}
        position={Position.Top}
        offset={2}
      >
        <div
          className="pb-1.5"
          onMouseEnter={() => setHovered(true)}
          onMouseLeave={() => setHovered(false)}
        >
          <div className="flex items-center gap-0.5 rounded-lg border border-line bg-surface-overlay p-0.5 shadow-pop">
            <Tooltip content="Edit note">
              <IconButton size="xs" label="Edit note" onClick={startEditing}>
                <PenLine />
              </IconButton>
            </Tooltip>
            <Tooltip content="Duplicate">
              <IconButton size="xs" label="Duplicate note" onClick={() => duplicateNode(id)}>
                <Copy />
              </IconButton>
            </Tooltip>
            <span className="mx-0.5 h-4 w-px bg-line" aria-hidden />
            <Tooltip content="Delete">
              <IconButton
                size="xs"
                label="Delete note"
                className="hover:bg-state-danger/12 hover:text-state-danger"
                onClick={() => removeNodes([id])}
              >
                <Trash2 />
              </IconButton>
            </Tooltip>
          </div>
        </div>
      </NodeToolbar>

      {editing ? (
        <Textarea
          autoFocus
          rows={4}
          value={draft}
          aria-label="Note text"
          onChange={(event) => setDraft(event.target.value)}
          onBlur={commit}
          onKeyDown={onKeyDown}
          className="nodrag nowheel min-h-[84px] resize-none border-transparent bg-transparent p-0 text-xs leading-relaxed text-content shadow-none focus:border-transparent focus:bg-transparent focus:ring-0"
        />
      ) : (
        <p className="min-h-[84px] whitespace-pre-wrap break-words text-xs leading-relaxed text-content">
          {data.text || (
            <span className="text-content-subtle">Double-click to write a note</span>
          )}
        </p>
      )}
    </div>
  )
})
