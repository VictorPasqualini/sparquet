/**
 * The keyboard map the UI implements, and the source of the shortcuts sheet.
 *
 * `Mod` is the platform modifier — Cmd on macOS, Ctrl everywhere else — resolved
 * once at render time so no entry has to be duplicated per platform. `Drag` is a
 * pointer gesture held together with the keys before it.
 *
 * Scope decides who handles the event: `global` fires anywhere, `canvas` only
 * while the graph has focus, `inspector` only while a field is focused. A key
 * bound in a narrower scope wins over the same key bound globally.
 */

export type ShortcutScope = 'global' | 'canvas' | 'inspector'

export interface Shortcut {
  keys: string[]
  label: string
  scope: ShortcutScope
}

export const SHORTCUTS: Shortcut[] = [
  { keys: ['Mod', 'K'], label: 'Open the command palette', scope: 'global' },
  { keys: ['Mod', 'S'], label: 'Save the workflow', scope: 'global' },
  { keys: ['Mod', 'Z'], label: 'Undo the last change', scope: 'global' },
  { keys: ['Mod', 'Shift', 'Z'], label: 'Redo the last undone change', scope: 'global' },
  { keys: ['Mod', 'Enter'], label: 'Run the pipeline', scope: 'global' },
  { keys: ['Mod', '/'], label: 'Toggle the AI assistant panel', scope: 'global' },
  { keys: ['Mod', 'J'], label: 'Toggle the JSON panel', scope: 'global' },
  { keys: ['Escape'], label: 'Close the open panel, or clear the selection', scope: 'global' },

  { keys: ['Delete'], label: 'Remove the selected nodes and edges', scope: 'canvas' },
  { keys: ['Mod', 'D'], label: 'Duplicate the selection', scope: 'canvas' },
  { keys: ['Space', 'Drag'], label: 'Pan the canvas', scope: 'canvas' },
  { keys: ['Mod', '0'], label: 'Fit the graph to the viewport', scope: 'canvas' },
  { keys: ['Mod', 'Shift', 'L'], label: 'Re-run the automatic layout', scope: 'canvas' },

  { keys: ['Tab'], label: 'Move to the next field', scope: 'inspector' },
  { keys: ['Shift', 'Tab'], label: 'Move to the previous field', scope: 'inspector' },
  {
    keys: ['Escape'],
    label: 'Discard the edit in progress and restore the saved value',
    scope: 'inspector',
  },
]
