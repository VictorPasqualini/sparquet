import Editor, { type EditorProps, type OnMount } from '@monaco-editor/react'
import { AlertTriangle, Check, Copy, Download, FileJson, RotateCcw, Wand2 } from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { toast } from 'sonner'

import {
  Button,
  EmptyState,
  IconButton,
  Segmented,
  Spinner,
  type SegmentedOption,
} from '@/components/ui'
import { compileGraph, serializePipeline } from '@/lib/compiler'
import { configureMonaco } from '@/lib/monaco'
import { cn } from '@/lib/utils/cn'
import { copyText, downloadText } from '@/lib/utils/download'
import { useEditorStore } from '@/store/editor'
import { slugify } from '@/store/library'
import { useSettingsStore } from '@/store/settings'
import type { ValidationIssue } from '@/types/studio'

type Mode = 'preview' | 'edit'

const MODES: SegmentedOption<Mode>[] = [
  { value: 'preview', label: 'Preview', title: 'Read-only JSON compiled from the canvas' },
  { value: 'edit', label: 'Edit', title: 'Paste pipeline JSON and apply it to the canvas' },
]

type EditorOptions = NonNullable<EditorProps['options']>

const BASE_OPTIONS: EditorOptions = {
  minimap: { enabled: false },
  fontSize: 12,
  wordWrap: 'on',
  automaticLayout: true,
  tabSize: 2,
  scrollBeyondLastLine: false,
  smoothScrolling: true,
  renderLineHighlight: 'none',
  lineNumbersMinChars: 3,
  padding: { top: 10, bottom: 10 },
  fontFamily: 'JetBrains Mono, ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
  scrollbar: { verticalScrollbarSize: 10, horizontalScrollbarSize: 10 },
}

configureMonaco()

export function JsonPanel() {
  const nodes = useEditorStore((state) => state.nodes)
  const edges = useEditorStore((state) => state.edges)
  const settings = useEditorStore((state) => state.settings)
  const applyPipeline = useEditorStore((state) => state.applyPipeline)
  const select = useEditorStore((state) => state.select)
  const togglePanel = useEditorStore((state) => state.togglePanel)
  const theme = useSettingsStore((state) => state.theme)

  // Same work as store.pipelineJson(), memoized on the inputs that can change it.
  const compiled = useMemo(
    () => compileGraph({ nodes, edges }, settings),
    [nodes, edges, settings],
  )
  const json = useMemo(
    () => (compiled.pipeline ? serializePipeline(compiled.pipeline) : ''),
    [compiled],
  )
  const blocking = useMemo(
    () => compiled.issues.filter((issue) => issue.severity === 'error'),
    [compiled],
  )

  const [mode, setMode] = useState<Mode>('preview')
  const [draft, setDraft] = useState('')
  const [touched, setTouched] = useState(false)
  const [copied, setCopied] = useState(false)

  // An untouched buffer tracks the canvas, so switching modes never shows stale JSON.
  useEffect(() => {
    if (mode !== 'edit' || touched) return
    setDraft(json)
  }, [mode, touched, json])

  useEffect(() => {
    if (!copied) return
    const timer = window.setTimeout(() => setCopied(false), 1400)
    return () => window.clearTimeout(timer)
  }, [copied])

  const editing = mode === 'edit'
  const active = editing ? draft : json
  const parsed = useMemo(() => (editing ? parseJson(draft) : null), [editing, draft])
  const stats = useMemo(() => measure(active), [active])
  const filename = `${slugify(settings.pipelineName || 'pipeline')}.json`
  const options = useMemo<EditorOptions>(
    () => ({ ...BASE_OPTIONS, readOnly: !editing }),
    [editing],
  )

  const handleChange = useCallback((value: string | undefined) => {
    setDraft(value ?? '')
    setTouched(true)
  }, [])

  const reset = useCallback(() => {
    setDraft(json)
    setTouched(false)
  }, [json])

  const applyDraft = useCallback(() => {
    if (!editing || !parsed?.ok) return
    const issues = applyPipeline(parsed.value)
    setTouched(false)
    setMode('preview')
    reportImport(issues)
  }, [editing, parsed, applyPipeline])

  // Monaco commands capture their closure once, so the keybinding reads a ref.
  const applyRef = useRef(applyDraft)
  useEffect(() => {
    applyRef.current = applyDraft
  }, [applyDraft])

  const handleMount = useCallback<OnMount>((instance, monaco) => {
    instance.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => applyRef.current())
  }, [])

  const handleCopy = useCallback(() => {
    void (async () => {
      if (await copyText(active)) setCopied(true)
      else toast.error('Could not copy to the clipboard')
    })()
  }, [active])

  const handleDownload = useCallback(() => {
    downloadText(filename, active)
  }, [filename, active])

  const reveal = useCallback(
    (nodeId: string) => {
      select(nodeId)
      togglePanel('inspector', true)
    },
    [select, togglePanel],
  )

  return (
    <div className="flex h-full min-h-0 flex-col bg-surface">
      <header className="flex items-center gap-2 border-b border-line px-2.5 py-2">
        <Segmented size="sm" value={mode} onChange={setMode} options={MODES} />
        <span className="ml-auto whitespace-nowrap text-2xs tabular-nums text-content-subtle">
          {stats}
        </span>
        <IconButton size="sm" label="Copy JSON" onClick={handleCopy} disabled={!active}>
          {copied ? <Check className="text-state-success" /> : <Copy />}
        </IconButton>
        <IconButton
          size="sm"
          label={`Download ${filename}`}
          onClick={handleDownload}
          disabled={!active}
        >
          <Download />
        </IconButton>
      </header>

      {!editing && !compiled.pipeline ? (
        <BlockingIssues issues={blocking} onReveal={reveal} />
      ) : (
        <div className="min-h-0 flex-1">
          <Editor
            height="100%"
            language="json"
            path="pipeline.json"
            value={active}
            theme={theme === 'dark' ? 'vs-dark' : 'light'}
            options={options}
            onChange={editing ? handleChange : undefined}
            onMount={handleMount}
            loading={
              <div className="flex h-full items-center justify-center">
                <Spinner />
              </div>
            }
          />
        </div>
      )}

      {editing && (
        <footer className="flex items-center gap-2 border-t border-line px-2.5 py-2">
          {parsed?.ok ? (
            <p className="flex min-w-0 items-center gap-1.5 text-2xs text-content-subtle">
              <Check className="h-3 w-3 shrink-0 text-state-success" aria-hidden />
              <span className="truncate">Valid JSON — applying rebuilds the canvas</span>
            </p>
          ) : draft.trim() ? (
            <p
              className="flex min-w-0 items-start gap-1.5 text-2xs text-state-danger"
              role="alert"
            >
              <AlertTriangle className="mt-px h-3 w-3 shrink-0" aria-hidden />
              <span className="truncate" title={parsed?.message}>
                {parsed?.message}
              </span>
            </p>
          ) : (
            <p className="min-w-0 truncate text-2xs text-content-subtle">
              Paste pipeline JSON to import it
            </p>
          )}
          <div className="ml-auto flex shrink-0 items-center gap-1.5">
            <Button
              size="xs"
              variant="ghost"
              icon={<RotateCcw />}
              onClick={reset}
              disabled={!touched}
            >
              Reset
            </Button>
            <Button
              size="xs"
              variant="primary"
              icon={<Wand2 />}
              onClick={applyDraft}
              disabled={!parsed?.ok}
              title="Apply to canvas (Ctrl/Cmd + Enter)"
            >
              Apply to canvas
            </Button>
          </div>
        </footer>
      )}
    </div>
  )
}

/* ------------------------------------------------------------------ pieces */

function BlockingIssues({
  issues,
  onReveal,
}: {
  issues: ValidationIssue[]
  onReveal: (nodeId: string) => void
}) {
  if (issues.length === 0) {
    return (
      <div className="flex-1">
        <EmptyState
          icon={<FileJson />}
          title="Nothing to compile"
          description="Add a source and an output to the canvas, and the pipeline JSON shows up here."
        />
      </div>
    )
  }

  return (
    <div className="scroll-area flex-1 p-2.5">
      <div className="mb-2.5 flex items-start gap-2 rounded-lg border border-state-danger/30 bg-state-danger/10 p-2.5">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-state-danger" aria-hidden />
        <div className="space-y-0.5">
          <p className="text-xs font-medium text-content">This graph does not compile yet</p>
          <p className="text-2xs leading-relaxed text-content-muted">
            Clear the {issues.length === 1 ? 'issue' : `${issues.length} issues`} below to see
            the pipeline JSON.
          </p>
        </div>
      </div>
      <ul className="space-y-1">
        {issues.map((issue) => {
          const nodeId = issue.nodeId
          return (
            <li key={issue.id}>
              {nodeId ? (
                <button
                  type="button"
                  onClick={() => onReveal(nodeId)}
                  className={cn(
                    'flex w-full items-start gap-2 rounded-lg border border-transparent px-2 py-1.5 text-left',
                    'transition-colors hover:border-line hover:bg-surface-sunken',
                    'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/40',
                  )}
                >
                  <IssueBody issue={issue} />
                </button>
              ) : (
                <div className="flex items-start gap-2 px-2 py-1.5">
                  <IssueBody issue={issue} />
                </div>
              )}
            </li>
          )
        })}
      </ul>
    </div>
  )
}

function IssueBody({ issue }: { issue: ValidationIssue }) {
  return (
    <>
      <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-state-danger" aria-hidden />
      <span className="min-w-0 flex-1">
        <span className="block text-xs leading-snug text-content">{issue.message}</span>
        {issue.hint && (
          <span className="mt-0.5 block text-2xs leading-relaxed text-content-muted">
            {issue.hint}
          </span>
        )}
      </span>
    </>
  )
}

/* ------------------------------------------------------------------ helpers */

type ParseState = { ok: true; value: unknown } | { ok: false; message: string }

function parseJson(text: string): ParseState {
  if (!text.trim()) return { ok: false, message: 'The editor is empty' }
  try {
    return { ok: true, value: JSON.parse(text) as unknown }
  } catch (error) {
    return { ok: false, message: describeJsonError(error, text) }
  }
}

/** Turns the engine's character offset into a line/column a human can jump to. */
function describeJsonError(error: unknown, text: string): string {
  const raw = error instanceof Error ? error.message : 'Invalid JSON'
  if (/line \d+/i.test(raw)) return raw

  const match = /position (\d+)/i.exec(raw)
  if (!match) return raw

  const offset = Number(match[1])
  const before = text.slice(0, offset)
  const line = before.split('\n').length
  const column = offset - before.lastIndexOf('\n')
  return `${raw} (line ${line}, column ${column})`
}

/** Compact "size · lines" readout for the toolbar. */
function measure(text: string): string {
  const bytes = new TextEncoder().encode(text).length
  const lines = text ? text.split('\n').length : 0
  const size = bytes < 1024 ? `${bytes} B` : `${(bytes / 1024).toFixed(1)} kB`
  return `${size} · ${lines} ${lines === 1 ? 'line' : 'lines'}`
}

function reportImport(issues: ValidationIssue[]): void {
  const errors = issues.filter((issue) => issue.severity === 'error')
  const warnings = issues.filter((issue) => issue.severity === 'warning')
  const description = issues
    .slice(0, 2)
    .map((issue) => issue.message)
    .join(' · ')

  if (errors.length > 0) {
    toast.error(`Applied with ${count(errors.length, 'error')}`, { description })
    return
  }
  if (warnings.length > 0) {
    toast.warning(`Applied with ${count(warnings.length, 'warning')}`, { description })
    return
  }
  toast.success('Canvas rebuilt from JSON')
}

function count(value: number, noun: string): string {
  return `${value} ${noun}${value === 1 ? '' : 's'}`
}
