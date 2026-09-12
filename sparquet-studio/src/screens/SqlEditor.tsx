/**
 * SQL over the catalog.
 *
 * The catalog already knows every address a Job touches and which format opens
 * it, so the only thing missing to query one is a name. This screen derives that
 * name, sends the SQL to the runner, and the runner opens each dataset the query
 * mentions through the framework's OWN `ReaderFactory` — the same code path a
 * Job takes. That is what makes a Delta or Iceberg table queryable here without
 * this screen knowing anything about either.
 *
 * Read-only by construction: the runner refuses anything that is not a SELECT,
 * WITH, EXPLAIN, DESCRIBE or SHOW, and registers the datasets as temp views that
 * it drops when the query ends. Writing data stays where it can be reviewed — in
 * a Job.
 *
 * Two things are deliberate and easy to miss:
 *
 *   - **Every query carries a row cap**, always, whether or not the SQL has a
 *     `LIMIT`. It bounds what is RETURNED, not what is scanned — a `GROUP BY`
 *     over a year of data still reads the year — so a timeout goes with it, and
 *     that is the control that actually bounds what a query can cost.
 *   - **The `spark` block travels with the query.** Connector jars and SQL
 *     extensions are honoured only when a SparkSession is created, so a Delta
 *     table is unreadable on a session built without them. The settings are not
 *     invented here: they are the union of what the Jobs touching these very
 *     datasets already declare (`sparkForDatasets`).
 */

import Editor, { type EditorProps, type OnMount } from '@monaco-editor/react'
import type { editor as MonacoEditor, IDisposable } from 'monaco-editor/esm/vs/editor/editor.api'
import {
  Copy,
  Database,
  Gauge,
  Play,
  RotateCcw,
  Search,
  SquareTerminal,
  TableProperties,
  Timer,
  TriangleAlert,
  X,
} from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState, type PointerEvent } from 'react'
import { useNavigate } from 'react-router-dom'

import { ASSET_HINT, NamespaceTree } from '@/components/catalog/NamespaceTree'
import { PageHeader, PageShell } from '@/components/layout/PageShell'
import { RunResultTable } from '@/components/panels/RunResultTable'
import { Badge, Button, EmptyState, Input, Kbd, Select, Spinner } from '@/components/ui'
import { buildNamespaceTree, describeAsset, deriveSchemas, type CatalogAsset } from '@/lib/datacatalog'
import { buildLineage } from '@/lib/lineage'
import { configureMonaco } from '@/lib/monaco'
import {
  cancelQuery,
  isRunnerError,
  runQuery,
  type QuerySource,
  type RunnerQueryResult,
} from '@/lib/runner/client'
import { sparkForDatasets } from '@/lib/runner/session'
import { mentionedAliases, viewAlias } from '@/lib/sql/views'
import { useLibraryStore } from '@/store/library'
import { useSettingsStore } from '@/store/settings'

configureMonaco()

/** Where the draft survives a reload. One editor, one draft — no history yet. */
const DRAFT_KEY = 'sparquet-studio:sql-draft'

/** How tall the editor was left, so the split survives a reload. */
const HEIGHT_KEY = 'sparquet-studio:sql-height'

/**
 * The row caps offered. The runner caps at 1000 whatever is asked; anything
 * larger belongs in a Job, not in a preview.
 *
 * 20 is the default because a preview is read by a person, and a person reads
 * the first screen: it is enough to see the shape of a table, the spelling of a
 * column and whether a join produced duplicates. Everything past that is paid
 * for — collected to the driver, serialized, sent over the wire — for rows
 * nobody scrolls to.
 */
const LIMITS = [20, 50, 100, 500, 1000] as const

const DEFAULT_LIMIT = 20

/**
 * What actually bounds a runaway query. The row cap does not: `LIMIT 20` on a
 * `GROUP BY` still scans everything, and on a cloud warehouse that is the bill.
 * Two minutes is long enough for a real read and short enough that a mistake is
 * a pause rather than an invoice.
 */
const TIMEOUTS = [
  { value: '30', label: '30 s' },
  { value: '120', label: '2 min' },
  { value: '300', label: '5 min' },
  { value: '900', label: '15 min' },
] as const

const DEFAULT_TIMEOUT = 120

const MIN_EDITOR_HEIGHT = 140
const MAX_EDITOR_HEIGHT = 720
const DEFAULT_EDITOR_HEIGHT = 280

type EditorOptions = NonNullable<EditorProps['options']>

const EDITOR_OPTIONS: EditorOptions = {
  minimap: { enabled: false },
  fontSize: 13,
  wordWrap: 'on',
  automaticLayout: true,
  tabSize: 2,
  scrollBeyondLastLine: false,
  smoothScrolling: true,
  renderLineHighlight: 'line',
  lineNumbersMinChars: 3,
  padding: { top: 10, bottom: 10 },
  fontFamily: 'JetBrains Mono, ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
  scrollbar: { verticalScrollbarSize: 10, horizontalScrollbarSize: 10 },
  suggestOnTriggerCharacters: true,
  quickSuggestions: { other: true, comments: false, strings: false },
  suggestSelection: 'first',
  tabCompletion: 'on',
}

/** The SQL a person writes here, minus what the catalog supplies. */
const KEYWORDS = [
  'SELECT',
  'FROM',
  'WHERE',
  'GROUP BY',
  'ORDER BY',
  'HAVING',
  'LIMIT',
  'JOIN',
  'LEFT JOIN',
  'INNER JOIN',
  'FULL OUTER JOIN',
  'ON',
  'AS',
  'WITH',
  'UNION ALL',
  'DISTINCT',
  'CASE WHEN',
  'COUNT(*)',
  'SUM',
  'AVG',
  'MIN',
  'MAX',
  'DESCRIBE',
  'EXPLAIN',
  'SHOW TABLES',
]

/** A dataset the runner can open, with the name the SQL calls it by. */
interface Attachable {
  key: string
  alias: string
  format: string
  columns: string[]
}

function readDraft(): string {
  try {
    return localStorage.getItem(DRAFT_KEY) ?? ''
  } catch {
    // A browser with storage disabled still gets an editor, just no draft.
    return ''
  }
}

function readHeight(): number {
  try {
    const stored = Number(localStorage.getItem(HEIGHT_KEY))
    if (Number.isFinite(stored) && stored >= MIN_EDITOR_HEIGHT && stored <= MAX_EDITOR_HEIGHT) {
      return stored
    }
  } catch {
    // Same as the draft: a convenience, not the user's data.
  }
  return DEFAULT_EDITOR_HEIGHT
}

export function SqlEditor() {
  const navigate = useNavigate()
  const jobs = useLibraryStore((state) => state.jobs)
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const theme = useSettingsStore((state) => state.theme)

  const [sql, setSql] = useState(readDraft)
  const [limit, setLimit] = useState<number>(DEFAULT_LIMIT)
  const [timeoutSeconds, setTimeoutSeconds] = useState<number>(DEFAULT_TIMEOUT)
  const [query, setQuery] = useState('')
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState<RunnerQueryResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [selection, setSelection] = useState('')
  const [height, setHeight] = useState(readHeight)

  const abortRef = useRef<AbortController | null>(null)
  const queryIdRef = useRef<string | null>(null)
  // Monaco binds the shortcut once, so the command has to reach the CURRENT run.
  const runRef = useRef<() => void>(() => {})
  const editorRef = useRef<MonacoEditor.IStandaloneCodeEditor | null>(null)
  const completionRef = useRef<IDisposable | null>(null)
  const dragRef = useRef<{ from: number; height: number } | null>(null)

  const index = useMemo(() => buildLineage(jobs), [jobs])
  const schemas = useMemo(() => deriveSchemas(jobs), [jobs])

  /**
   * Every dataset the runner can open, named.
   *
   * Session-scoped views are left out on purpose: a `view` lives inside one
   * pipeline run, so there is nothing on storage to read here. Sorted by address
   * so the alias a dataset gets never depends on the order the Jobs were saved.
   */
  const attachables = useMemo<Attachable[]>(() => {
    const taken = new Set<string>()
    return [...index.datasets]
      .filter((dataset) => !dataset.sessionScoped)
      .sort((left, right) => left.key.localeCompare(right.key))
      .map((dataset) => {
        const alias = viewAlias(dataset.key, taken)
        taken.add(alias)
        const format = dataset.formats.find((name) => name !== 'view') ?? dataset.formats[0] ?? ''
        return {
          key: dataset.key,
          alias,
          format,
          columns: (schemas.get(dataset.key)?.fields ?? []).map((field) => field.name),
        }
      })
      .filter((entry) => entry.format.length > 0)
  }, [index, schemas])

  const byKey = useMemo(
    () => new Map(attachables.map((entry) => [entry.key, entry])),
    [attachables],
  )

  const aliases = useMemo(() => attachables.map((entry) => entry.alias), [attachables])
  /** What the whole editor names — the sidebar highlight follows the buffer, not the selection. */
  const used = useMemo(() => new Set(mentionedAliases(sql, aliases)), [sql, aliases])

  const needle = query.trim().toLowerCase()

  const visible = useMemo(() => {
    if (!needle) return attachables
    return attachables.filter(
      (entry) =>
        entry.key.toLowerCase().includes(needle) ||
        entry.alias.includes(needle) ||
        entry.columns.some((column) => column.toLowerCase().includes(needle)),
    )
  }, [attachables, needle])

  /** The same three-tier tree the catalog draws, over the queryable datasets only. */
  const tree = useMemo(
    () => buildNamespaceTree(visible.map((entry) => describeAsset(entry.key, [entry.format]))),
    [visible],
  )

  // Read by the completion provider, which Monaco registers once and keeps.
  const catalogRef = useRef<Attachable[]>(attachables)
  catalogRef.current = attachables

  useEffect(() => {
    try {
      localStorage.setItem(DRAFT_KEY, sql)
    } catch {
      // Nothing to do: the draft is a convenience, not the user's data.
    }
  }, [sql])

  useEffect(() => {
    try {
      localStorage.setItem(HEIGHT_KEY, String(height))
    } catch {
      // Same.
    }
  }, [height])

  useEffect(() => () => completionRef.current?.dispose(), [])

  const run = useCallback(async () => {
    // A selection runs alone — the way every SQL console behaves, and the only
    // way to keep several statements in one buffer without splitting them.
    const statement = (selection.trim() || sql).trim()
    if (!statement || running) return

    const named = new Set(mentionedAliases(statement, aliases))
    const sources: QuerySource[] = attachables
      .filter((entry) => named.has(entry.alias))
      .map((entry) => ({ alias: entry.alias, format: entry.format, path: entry.key }))

    const controller = new AbortController()
    abortRef.current = controller
    const queryId = `q${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`
    queryIdRef.current = queryId
    setRunning(true)
    setError(null)

    try {
      const answer = await runQuery(
        runnerUrl,
        {
          sql: statement,
          sources,
          limit,
          queryId,
          timeoutSeconds,
          // What opens these datasets, as the Jobs that read them declare it.
          // Without this a Delta table fails with DATA_SOURCE_NOT_FOUND on a
          // session that was built for something else.
          spark: sparkForDatasets(jobs, sources.map((source) => source.path)),
        },
        controller.signal,
        runnerToken,
      )
      setResult(answer)
    } catch (caught) {
      if (caught instanceof DOMException && caught.name === 'AbortError') {
        setError('Query cancelled.')
      } else if (isRunnerError(caught)) {
        setError(caught.message)
      } else {
        setError(caught instanceof Error ? caught.message : 'The query failed.')
      }
      setResult(null)
    } finally {
      abortRef.current = null
      queryIdRef.current = null
      setRunning(false)
    }
  }, [
    aliases,
    attachables,
    jobs,
    limit,
    running,
    runnerToken,
    runnerUrl,
    selection,
    sql,
    timeoutSeconds,
  ])

  runRef.current = () => {
    void run()
  }

  const stop = useCallback(() => {
    const queryId = queryIdRef.current
    // Cancel first: aborting the request only drops this end of the socket, and
    // Spark would keep computing a query nobody is waiting for any more.
    if (queryId) void cancelQuery(runnerUrl, queryId, runnerToken)
    abortRef.current?.abort()
  }, [runnerToken, runnerUrl])

  const handleMount = useCallback<OnMount>((editor, monaco) => {
    editorRef.current = editor
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => runRef.current())
    editor.onDidChangeCursorSelection(() => {
      const model = editor.getModel()
      const range = editor.getSelection()
      setSelection(model && range && !range.isEmpty() ? model.getValueInRange(range) : '')
    })

    // The tables and their columns, completed as they are typed. Registered on
    // the language rather than on the model, and read through a ref, so it
    // survives every catalog change without re-registering.
    completionRef.current?.dispose()
    completionRef.current = monaco.languages.registerCompletionItemProvider('sql', {
      triggerCharacters: ['.', ' '],
      provideCompletionItems(model, position) {
        const word = model.getWordUntilPosition(position)
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        }
        const line = model.getValueInRange({
          startLineNumber: position.lineNumber,
          startColumn: 1,
          endLineNumber: position.lineNumber,
          endColumn: position.column,
        })

        // `orders.` — only that table's columns, and nothing else.
        const qualified = /([A-Za-z_]\w*)\.\w*$/.exec(line)
        if (qualified) {
          const table = catalogRef.current.find((entry) => entry.alias === qualified[1])
          return {
            suggestions: (table?.columns ?? []).map((column) => ({
              label: column,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: column,
              detail: table?.alias,
              range,
            })),
          }
        }

        const tables = catalogRef.current.map((entry) => ({
          label: entry.alias,
          kind: monaco.languages.CompletionItemKind.Struct,
          insertText: entry.alias,
          detail: `${entry.format} · ${entry.key}`,
          documentation:
            entry.columns.length > 0
              ? `Columns the canvas states: ${entry.columns.join(', ')}`
              : 'No columns derived from the canvas — the source supplies them.',
          sortText: `0${entry.alias}`,
          range,
        }))

        const seen = new Set<string>()
        const columns = catalogRef.current.flatMap((entry) =>
          entry.columns
            .filter((column) => !seen.has(column) && seen.add(column))
            .map((column) => ({
              label: column,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: column,
              detail: entry.alias,
              sortText: `1${column}`,
              range,
            })),
        )

        return {
          suggestions: [
            ...tables,
            ...columns,
            ...KEYWORDS.map((keyword) => ({
              label: keyword,
              kind: monaco.languages.CompletionItemKind.Keyword,
              insertText: keyword,
              sortText: `2${keyword}`,
              range,
            })),
          ],
        }
      },
    })

    editor.focus()
  }, [])

  /** Puts text where the cursor is, which is where a person clicking a column wants it. */
  const insert = useCallback((text: string) => {
    const editor = editorRef.current
    const range = editor?.getSelection()
    if (!editor || !range) {
      setSql((current) => (current ? `${current} ${text}` : text))
      return
    }
    editor.executeEdits('catalog', [{ range, text, forceMoveMarkers: true }])
    editor.focus()
  }, [])

  /** Clicking a table with an empty editor writes the query nobody wants to type. */
  const attachTable = useCallback(
    (entry: Attachable) => {
      if (!sql.trim()) {
        setSql(`SELECT *\nFROM ${entry.alias}`)
        return
      }
      insert(entry.alias)
    },
    [insert, sql],
  )

  const startDrag = useCallback(
    (event: PointerEvent<HTMLDivElement>) => {
      dragRef.current = { from: event.clientY, height }
      event.currentTarget.setPointerCapture(event.pointerId)
    },
    [height],
  )

  const onDrag = useCallback((event: PointerEvent<HTMLDivElement>) => {
    const drag = dragRef.current
    if (!drag) return
    const next = drag.height + (event.clientY - drag.from)
    setHeight(Math.min(MAX_EDITOR_HEIGHT, Math.max(MIN_EDITOR_HEIGHT, next)))
  }, [])

  const endDrag = useCallback((event: PointerEvent<HTMLDivElement>) => {
    dragRef.current = null
    event.currentTarget.releasePointerCapture(event.pointerId)
  }, [])

  const assetTrailing = useCallback(
    (asset: CatalogAsset) => {
      const entry = byKey.get(asset.key)
      if (!entry) return null
      return (
        <span className="flex shrink-0 items-center gap-1">
          {entry.columns.length > 0 ? (
            <span className="tabular-nums text-[10px] text-content-subtle">
              {entry.columns.length}
            </span>
          ) : null}
          <Badge tone="neutral">{entry.format}</Badge>
        </span>
      )
    },
    [byKey],
  )

  const assetChildren = useCallback(
    (asset: CatalogAsset) => {
      const entry = byKey.get(asset.key)
      if (!entry) return null
      if (entry.columns.length === 0) {
        return (
          <p className="px-2 py-1 text-[11px] italic text-content-subtle">
            No columns derived from the canvas. The source supplies them at read time.
          </p>
        )
      }
      return (
        <ul>
          {entry.columns.map((column) => (
            <li key={column}>
              <button
                type="button"
                onClick={() => insert(column)}
                title={`${entry.alias}.${column} — click to put it in the query`}
                className="flex w-full items-center gap-1.5 truncate rounded px-2 py-0.5 text-left
                  font-mono text-2xs text-content-muted transition hover:bg-surface-sunken hover:text-content"
              >
                <TableProperties className="h-3 w-3 shrink-0 text-content-subtle/70" />
                {column}
              </button>
            </li>
          ))}
        </ul>
      )
    },
    [byKey, insert],
  )

  if (attachables.length === 0) {
    return (
      <PageShell>
        <div className="card">
          <EmptyState
            icon={<SquareTerminal />}
            title="Nothing to query yet"
            description="The SQL editor reads the datasets your Jobs declare: every address in the
              catalog becomes a view you can name. Load the example chain in the catalog, or build
              a Job that reads something."
            action={
              <Button size="sm" onClick={() => navigate('/catalog')}>
                <Database />
                Open the catalog
              </Button>
            }
          />
        </div>
      </PageShell>
    )
  }

  const statement = (selection.trim() || sql).trim()
  const named = new Set(mentionedAliases(statement, aliases))
  const attached = attachables.filter((entry) => named.has(entry.alias))

  return (
    <PageShell width="full">
      <PageHeader
        icon={<SquareTerminal />}
        title="SQL editor"
        description="The runner opens each dataset with the same reader a Job uses — Delta and
          Iceberg included — and registers it as a temporary view named after its address. Reads
          only: SELECT, WITH, EXPLAIN, DESCRIBE and SHOW."
      />

      <div className="grid gap-4 lg:grid-cols-[minmax(0,18rem)_minmax(0,1fr)]">
        <aside className="card flex max-h-[44rem] flex-col p-0">
          <div className="border-b border-line p-2.5">
            <Input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Filter datasets and columns"
              leading={<Search className="h-3.5 w-3.5" />}
              aria-label="Filter datasets"
            />
          </div>
          <div className="min-h-0 flex-1 overflow-auto p-1.5">
            <NamespaceTree
              tree={tree}
              searching={needle.length > 0}
              onSelectAsset={(asset) => {
                const entry = byKey.get(asset.key)
                if (entry) attachTable(entry)
              }}
              isAssetActive={(asset) => {
                const entry = byKey.get(asset.key)
                return Boolean(entry && used.has(entry.alias))
              }}
              assetTrailing={assetTrailing}
              assetChildren={assetChildren}
              empty={
                <p className="px-2 py-6 text-center text-xs text-content-subtle">
                  No dataset matches “{query}”.
                </p>
              }
            />
          </div>
          <p className="border-t border-line px-2.5 py-2 text-[11px] text-content-subtle">
            {used.size === 0
              ? 'Only the datasets your query names are opened.'
              : `${used.size} attached to this query.`}
          </p>
        </aside>

        <section className="min-w-0 space-y-3">
          <div className="card flex flex-col p-0">
            <header className="flex flex-wrap items-center gap-2 border-b border-line px-2.5 py-2">
              <Button size="sm" onClick={() => void run()} disabled={running || !statement}>
                {running ? <Spinner className="h-3.5 w-3.5" /> : <Play />}
                {selection.trim() ? 'Run selection' : 'Run'}
              </Button>
              {running && (
                <Button size="sm" variant="secondary" onClick={stop}>
                  <X />
                  Stop
                </Button>
              )}
              <span className="text-[11px] text-content-subtle">
                <Kbd>Ctrl</Kbd> <Kbd>Enter</Kbd>
              </span>
              {attached.length > 0 ? (
                <span
                  className="hidden items-center gap-1 text-[11px] text-content-subtle sm:flex"
                  title={attached.map((entry) => `${entry.alias} → ${entry.key}`).join('\n')}
                >
                  <Database className="h-3 w-3" />
                  {attached.length} {attached.length === 1 ? 'view' : 'views'}
                </span>
              ) : null}

              <div className="ml-auto flex flex-wrap items-center gap-2">
                <span
                  className="flex items-center gap-1 text-[11px] text-content-subtle"
                  title="Every query is capped at this many rows, whether or not the SQL says
                    LIMIT. It bounds what comes back, not what Spark scans."
                >
                  <Gauge className="h-3 w-3" />
                  <label className="sr-only" htmlFor="sql-limit">
                    Rows to return
                  </label>
                  <Select
                    id="sql-limit"
                    ariaLabel="Rows to return"
                    value={String(limit)}
                    onValueChange={(value) => setLimit(Number(value))}
                    options={LIMITS.map((value) => ({
                      value: String(value),
                      label: `${value} rows`,
                    }))}
                  />
                </span>
                <span
                  className="flex items-center gap-1 text-[11px] text-content-subtle"
                  title="The runner stops the query when this passes. This is the control that
                    bounds cost: a row cap does not stop a scan."
                >
                  <Timer className="h-3 w-3" />
                  <label className="sr-only" htmlFor="sql-timeout">
                    Query timeout
                  </label>
                  <Select
                    id="sql-timeout"
                    ariaLabel="Query timeout"
                    value={String(timeoutSeconds)}
                    onValueChange={(value) => setTimeoutSeconds(Number(value))}
                    options={TIMEOUTS.map((option) => ({
                      value: option.value,
                      label: option.label,
                    }))}
                  />
                </span>
              </div>
            </header>

            <div style={{ height }} className="min-h-0">
              <Editor
                height="100%"
                language="sql"
                path="query.sql"
                value={sql}
                theme={theme === 'dark' ? 'vs-dark' : 'light'}
                options={EDITOR_OPTIONS}
                onChange={(value) => setSql(value ?? '')}
                onMount={handleMount}
                loading={
                  <div className="flex h-full items-center justify-center">
                    <Spinner />
                  </div>
                }
              />
            </div>

            {/* Drag to give the editor or the results more room, as a console should. */}
            <div
              role="separator"
              aria-orientation="horizontal"
              aria-label="Resize the editor"
              onPointerDown={startDrag}
              onPointerMove={onDrag}
              onPointerUp={endDrag}
              onDoubleClick={() => setHeight(DEFAULT_EDITOR_HEIGHT)}
              className="group flex h-2.5 cursor-row-resize items-center justify-center border-t
                border-line bg-surface-sunken/40 transition hover:bg-surface-raised"
            >
              <span className="h-0.5 w-8 rounded-full bg-line group-hover:bg-brand-400/60" />
            </div>

            <footer className="flex flex-wrap items-center gap-x-3 gap-y-1 px-2.5 py-1.5 text-[11px] text-content-subtle">
              <span>
                Capped at {limit} rows, stopped after {timeoutSeconds}s.
              </span>
              <span aria-hidden>·</span>
              <span title="A LIMIT bounds the rows returned. An aggregation or a join still reads
                everything it needs to answer, so the timeout is what bounds the cost.">
                the cap is on rows returned, not rows scanned
              </span>
            </footer>
          </div>

          {error && (
            /*
              A Spark error is a Java stack trace with a Python one wrapped around
              it — hundreds of lines. Left to grow it pushes the editor and the
              results off the screen, so it lives in a box of its own height and
              scrolls. The first line is the one that says what happened, so it
              is also shown on its own, above the trace.
            */
            <div
              className="overflow-hidden rounded-xl border border-state-danger/40 bg-state-danger/10"
              role="alert"
            >
              <div className="flex items-start gap-2 border-b border-state-danger/25 px-3 py-2">
                <TriangleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0 text-state-danger" />
                <p className="min-w-0 flex-1 break-words text-xs font-medium text-state-danger">
                  {error.split('\n')[0]}
                </p>
                <Button
                  size="xs"
                  variant="ghost"
                  onClick={() => void navigator.clipboard?.writeText(error)}
                  title="Copy the whole message"
                >
                  <Copy />
                  Copy
                </Button>
                <Button size="xs" variant="ghost" onClick={() => setError(null)} title="Dismiss">
                  <X />
                </Button>
              </div>
              <pre className="max-h-56 overflow-auto px-3 py-2 font-mono text-[11px] leading-relaxed text-state-danger/90">
                {error}
              </pre>
            </div>
          )}

          {result && !error && (
            <div className="space-y-2">
              <p className="flex flex-wrap items-center gap-2 text-[11px] text-content-subtle">
                <span className="tabular-nums">{result.rows.length} rows</span>
                <span aria-hidden>·</span>
                <span className="tabular-nums">{result.elapsedMs} ms</span>
                <span aria-hidden>·</span>
                <span className="tabular-nums">{result.columns.length} columns</span>
                {result.truncated && (
                  <Badge tone="warning">cut at {limit} — there are more rows</Badge>
                )}
                {result.sessionRestarted && (
                  <span
                    className="flex items-center gap-1"
                    title="The SparkSession was rebuilt so this query's connectors — Delta,
                      Iceberg, a JDBC driver — were on it. Jars and SQL extensions are read only
                      when a session is created, so an existing session cannot pick them up."
                  >
                    <RotateCcw className="h-3 w-3" />
                    <Badge tone="info">Spark session rebuilt</Badge>
                  </span>
                )}
                {attached.length > 0 && (
                  <span title={attached.map((entry) => `${entry.alias} → ${entry.key}`).join('\n')}>
                    {attached.map((entry) => entry.alias).join(', ')}
                  </span>
                )}
              </p>
              <RunResultTable
                columns={result.columns}
                rows={result.rows}
                truncated={result.truncated}
                maxRows={limit}
                emptyMessage="The query returned no rows."
                heightClass="max-h-[26rem]"
              />
            </div>
          )}

          {!result && !error && (
            <p className="rounded-xl border border-dashed border-line px-3 py-8 text-center text-xs text-content-subtle">
              Pick a table on the left, or write a query. Results appear here.{' '}
              <span title={ASSET_HINT.table}>Tables, folders and topics all read the same way.</span>
            </p>
          )}
        </section>
      </div>
    </PageShell>
  )
}
