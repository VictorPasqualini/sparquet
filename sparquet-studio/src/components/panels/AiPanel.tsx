/**
 * The assistant beside the canvas.
 *
 * All of it lives in `AiConversation`, which is the same component the screen at
 * `/` renders. What is left here is the only thing that differs: this one has a
 * job open, so it hands over what is on the canvas and takes a proposal back.
 *
 * The context is read at send time rather than passed down as a value. Between
 * typing a question and sending it the canvas moves — a node gets selected, the
 * linter re-runs — and what travels has to be what is on screen when the request
 * leaves, not what was there when the panel last rendered.
 */

import { toast } from 'sonner'

import { AiConversation, MAX_CONTEXT_ISSUES } from '@/components/ai/AiConversation'
import { useEditorStore } from '@/store/editor'

export function AiPanel() {
  const applyPipeline = useEditorStore((state) => state.applyPipeline)
  const undo = useEditorStore((state) => state.undo)
  const issues = useEditorStore((state) => state.issues)

  return (
    <AiConversation
      layout="panel"
      issueCount={issues.length}
      readContext={() => {
        const editor = useEditorStore.getState()
        return {
          pipeline: editor.compile().pipeline,
          issues: editor.issues.slice(0, MAX_CONTEXT_ISSUES),
          selectedNode: editor.nodes.find((node) => node.id === editor.selectedNodeId) ?? null,
        }
      }}
      onApply={(pipeline) => {
        const found = applyPipeline(pipeline)
        toast.success('Pipeline applied to the canvas', {
          description: found.length
            ? `${found.length} ${found.length === 1 ? 'issue' : 'issues'} to review`
            : 'No issues found',
          action: { label: 'Undo', onClick: () => undo() },
        })
      }}
    />
  )
}
