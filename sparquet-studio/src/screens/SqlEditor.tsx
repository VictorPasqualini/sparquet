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
 *   - **A query is a file.** Several can be open at once, each a tab, and saving
 *     one writes `queries/<slug>.sql` into the library through the same backend
 *     every other record uses — so a query is reviewable, diffable and shared
 *     rather than stuck in one browser profile. A tab nobody has saved is still
 *     only a draft, kept in `localStorage` so a reload does not lose it.
 *   - **The `spark` block travels with the query.** Connector jars and SQL
 *     extensions are honoured only when a SparkSession is created, so a Delta
 *     table is unreadable on a session built without them. The settings are not
 *     invented here: they are the union of what the Jobs touching these very
 *     datasets already declare (`sparkForDatasets`).
 */

import Editor, { type EditorProps, type Monaco, type OnMount } from '@monaco-editor/react'
import type { editor as MonacoEditor, IDisposable } from 'monaco-editor/esm/vs/editor/editor.api'
import {
  ChartColumn,
  Check,
  Copy,
  Database,
  Download,
  Gauge,
  History,
  Network,
  Play,
  RotateCcw,
  Search,
  SquareTerminal,
  Table2,
  TableProperties,
  Timer,
  TriangleAlert,
  WandSparkles,
  Workflow,
  X,
} from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState, type PointerEvent } from 'react'
import { useNavigate } from 'react-router-dom'

import { ASSET_HINT, NamespaceTree } from '@/components/catalog/NamespaceTree'
import { PlanTree } from '@/components/sql/PlanTree'
import { QueryHistory } from '@/components/sql/QueryHistory'
import { QueryTabs, type QueryTabView } from '@/components/sql/QueryTabs'
import { PageHeader, PageShell } from '@/components/layout/PageShell'
import {
  WorkspaceTabs,
  workspacePanelId,
  workspaceTabId,
  type WorkspaceTab,
} from '@/components/layout/WorkspaceTabs'
import { RunResultTable } from '@/components/panels/RunResultTable'
import {
  Badge,
  Button,
  EmptyState,
  Input,
  Kbd,
  Modal,
  Select,
  Spinner,
  useConfirm,
} from '@/components/ui'
import { buildNamespaceTree, describeAsset, deriveSchemas, type CatalogAsset } from '@/lib/datacatalog'
import { buildLineage } from '@/lib/lineage'
import { configureMonaco } from '@/lib/monaco'
import {
  cancelQuery,
  clearQueryHistory,
  fetchQueryHistory,
  isRunnerError,
  moveQueryHistory,
  runQuery,
  validateQuery,
  type QuerySource,
  type QueryValidation,
  type RunnerQueryResult,
} from '@/lib/runner/client'
import { CreateJobFromQuery } from '@/components/sql/CreateJobFromQuery'
import { chartable } from '@/lib/sql/chartScale'
import { formatSql } from '@/lib/sql/format'
import { ResultChart } from '@/components/sql/ResultChart'
import { accessTo, useIamStore } from '@/store/iam'
import { sparkForDatasets } from '@/lib/runner/session'
import { historyScope, type QueryRun } from '@/lib/sql/history'
import { isPlanResult } from '@/lib/sql/plan'
import { mentionedAliases, viewAlias } from '@/lib/sql/views'
import { timestampedName, toCsv } from '@/lib/utils/csv'
import { downloadText } from '@/lib/utils/download'
import { useLibraryStore } from '@/store/library'
import { UNTITLED, useQueriesStore } from '@/store/queries'
import { useSettingsStore } from '@/store/settings'

configureMonaco()

/**
 * Where the open tabs survive a reload — the unsaved ones included.
 *
 * Deliberately `localStorage` and not the library: a tab nobody has named is a
 * scratchpad, and writing a file for every keystroke would fill the library with
 * `untitled-3.sql`. What is in the library is what somebody chose to save.
 */
const TABS_KEY = 'sparquet-studio:sql-tabs'

/** The single draft this screen kept before tabs existed, read once to migrate it. */
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
/**
 * How long the typing has to pause before the parser is asked.
 *
 * Long enough that a statement being typed is not sent word by word, short
 * enough that the marker lands while the mistake is still on screen.
 */
const SYNTAX_DELAY_MS = 600

/** Who owns the squiggles, so nothing else on this model is cleared with them. */
const SYNTAX_MARKER = 'sparquet-sql-syntax'

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

/**
 * One open buffer.
 *
 * `tabId` identifies the tab and `queryId` the file, and they are separate
 * because a tab outlives both a rename and a first save: the same buffer starts
 * as a draft, becomes `queries/orders.sql`, and is still the tab the person was
 * typing in. `savedSql` is what the library last accepted, so "unsaved" is a
 * comparison rather than a flag somebody has to remember to clear.
 */
interface SqlTab {
  tabId: string
  queryId: string | null
  name: string
  sql: string
  limit: number
  savedSql: string
}

/**
 * What the space under the editor is showing.
 *
 * Four answers to one run — the rows, the shape of them, the plan behind them,
 * and every earlier version of the statement — and they each want the whole
 * width, so they take turns rather than stack.
 */
type Surface = 'result' | 'chart' | 'plan' | 'history'

let tabSeq = 0

function newTab(fields: Partial<SqlTab> = {}): SqlTab {
  tabSeq += 1
  return {
    tabId: `t${Date.now().toString(36)}${tabSeq}`,
    queryId: null,
    name: UNTITLED,
    sql: '',
    limit: DEFAULT_LIMIT,
    savedSql: '',
    ...fields,
  }
}

function isTab(value: unknown): value is SqlTab {
  if (typeof value !== 'object' || value === null) return false
  const tab = value as Record<string, unknown>
  return (
    typeof tab.tabId === 'string' &&
    typeof tab.name === 'string' &&
    typeof tab.sql === 'string' &&
    typeof tab.savedSql === 'string' &&
    (tab.queryId === null || typeof tab.queryId === 'string')
  )
}

function readTabs(): SqlTab[] {
  try {
    const stored: unknown = JSON.parse(localStorage.getItem(TABS_KEY) ?? 'null')
    const tabs = Array.isArray(stored) ? stored.filter(isTab) : []
    if (tabs.length > 0) {
      return tabs.map((tab) => ({ ...tab, limit: Number(tab.limit) || DEFAULT_LIMIT }))
    }
    // Nothing to restore. The screen used to keep one unnamed draft, and losing
    // it to an upgrade would be the rudest possible way to announce tabs.
    const draft = localStorage.getItem(DRAFT_KEY) ?? ''
    if (draft.trim()) return [newTab({ sql: draft })]
  } catch {
    // A browser with storage disabled still gets an editor, just no drafts.
  }
  return [newTab()]
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

  const saved = useQueriesStore((state) => state.items)
  const loadQueries = useQueriesStore((state) => state.load)
  const createQuery = useQueriesStore((state) => state.create)
  const updateQuery = useQueriesStore((state) => state.update)
  const removeQuery = useQueriesStore((state) => state.remove)

  const [tabs, setTabs] = useState<SqlTab[]>(readTabs)
  const [activeTabId, setActiveTabId] = useState<string>(() => tabs[0]?.tabId ?? '')
  const [naming, setNaming] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [confirm, confirmDialog] = useConfirm()
  const [timeoutSeconds, setTimeoutSeconds] = useState<number>(DEFAULT_TIMEOUT)
  const [query, setQuery] = useState('')
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState<RunnerQueryResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [selection, setSelection] = useState('')
  /** Open while the query is being turned into a Job. */
  const [creatingJob, setCreatingJob] = useState(false)
  const [syntax, setSyntax] = useState<QueryValidation | null>(null)
  const [height, setHeight] = useState(readHeight)
  const [surface, setSurface] = useState<Surface>('result')
  const [history, setHistory] = useState<QueryRun[]>([])

  const abortRef = useRef<AbortController | null>(null)
  const queryIdRef = useRef<string | null>(null)
  // Monaco binds the shortcut once, so the command has to reach the CURRENT run.
  const runRef = useRef<() => void>(() => {})
  const saveRef = useRef<() => void>(() => {})
  const formatRef = useRef<() => void>(() => {})
  const editorRef = useRef<MonacoEditor.IStandaloneCodeEditor | null>(null)
  const monacoRef = useRef<Monaco | null>(null)
  const completionRef = useRef<IDisposable | null>(null)
  const dragRef = useRef<{ from: number; height: number } | null>(null)

  const active = tabs.find((tab) => tab.tabId === activeTabId) ?? tabs[0]
  const activeQueryId = active?.queryId ?? null

  // A saved query is a securable: the file can be closed to somebody who may
  // still read every table it names. The rules are loaded here rather than
  // assumed, because this screen is reachable without ever opening Access.
  const iamGrants = useIamStore((state) => state.grants)
  const iamOwners = useIamStore((state) => state.owners)
  const loadIam = useIamStore((state) => state.load)
  useEffect(() => {
    void loadIam()
  }, [loadIam])

  /** What this person holds on the open file. `null` while the tab is a draft. */
  const queryAccess = useMemo(
    () => (activeQueryId ? accessTo('query', activeQueryId) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [activeQueryId, iamGrants, iamOwners],
  )
  /**
   * Optimistic on purpose, like every other client-side check here: an
   * ungoverned file is editable, and the runner refuses what this gets wrong.
   * A wrong `false` would hide work somebody is entitled to do.
   */
  const mayEditQuery =
    !queryAccess?.governed || queryAccess.level === 'write' || queryAccess.level === 'admin'
  const mayDeleteQuery = !queryAccess?.governed || queryAccess.level === 'admin'
  const sql = active?.sql ?? ''
  const limit = active?.limit ?? DEFAULT_LIMIT
  /** Edits the tab being typed in. Everything that changes a buffer goes here. */
  const patchActive = useCallback(
    (patch: Partial<SqlTab>) => {
      setTabs((current) =>
        current.map((tab) => (tab.tabId === activeTabId ? { ...tab, ...patch } : tab)),
      )
    },
    [activeTabId],
  )

  /** Kept as a setter so the call sites that fold over the current text still can. */
  const setSql = useCallback(
    (next: string | ((current: string) => string)) => {
      setTabs((current) =>
        current.map((tab) =>
          tab.tabId === activeTabId
            ? { ...tab, sql: typeof next === 'function' ? next(tab.sql) : next }
            : tab,
        ),
      )
    },
    [activeTabId],
  )

  const setLimit = useCallback(
    (value: number) => patchActive({ limit: value }),
    [patchActive],
  )

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
    void loadQueries()
  }, [loadQueries])

  useEffect(() => {
    try {
      localStorage.setItem(TABS_KEY, JSON.stringify(tabs))
    } catch {
      // Nothing to do: the tab strip is a convenience, not the user's data —
      // what was saved is in the library, and that is the copy that matters.
    }
  }, [tabs])

  useEffect(() => {
    try {
      localStorage.setItem(HEIGHT_KEY, String(height))
    } catch {
      // Same.
    }
  }, [height])

  /**
   * Reads this buffer's runs back from the runner.
   *
   * The runner records them as it executes, so this is a read and never a write:
   * what a run cost and how it failed is what happened, not what a client
   * reported. Called again after each run rather than appended to locally, which
   * is also what makes a teammate's run on a saved query show up here.
   */
  const loadHistory = useCallback(
    (signal?: AbortSignal) => {
      const scope = historyScope(activeQueryId, activeTabId)
      void fetchQueryHistory(runnerUrl, scope, signal, runnerToken).then((runs) => {
        if (!signal?.aborted) setHistory(runs)
      })
    },
    [activeQueryId, activeTabId, runnerToken, runnerUrl],
  )

  // Switching tab switches history with it — a run belongs to the query it was
  // run from, not to the screen.
  useEffect(() => {
    const controller = new AbortController()
    loadHistory(controller.signal)
    return () => controller.abort()
  }, [loadHistory])

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
          // The runner checks the grants on the file as well as on the tables.
          // A draft sends nothing: there is no file to have a rule about.
          savedQueryId: activeQueryId ?? undefined,
          // What the run is filed under while the buffer has no file yet. The
          // runner keys a draft's history by the tab *and* the person, so a
          // scratch statement stays the business of whoever ran it.
          tab: activeTabId,
        },
        controller.signal,
        runnerToken,
      )
      setResult(answer)
      // An EXPLAIN comes back as one cell of text; showing it as a one-row
      // table is showing the plan in a window the width of a column.
      setSurface(isPlanResult(answer.columns, answer.rows) ? 'plan' : 'result')
      // The runner already wrote this run down as it executed it; read it back
      // rather than keeping a second, client-side account of the same thing.
      loadHistory()
    } catch (caught) {
      const message =
        caught instanceof DOMException && caught.name === 'AbortError'
          ? 'Query cancelled.'
          : isRunnerError(caught)
            ? caught.message
            : caught instanceof Error
              ? caught.message
              : 'The query failed.'
      setError(message)
      setResult(null)
      setSurface('result')
      // A failed run is kept too — "what did I run that broke" is asked at least
      // as often as the other question — and the runner records a refusal the
      // same way it records a result. A failure that never reached the runner,
      // a cancellation included, has nothing on the other side to read back.
      loadHistory()
    } finally {
      abortRef.current = null
      queryIdRef.current = null
      setRunning(false)
    }
  }, [
    activeQueryId,
    aliases,
    attachables,
    activeTabId,
    jobs,
    limit,
    loadHistory,
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

  saveRef.current = () => {
    void saveActive()
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
    monacoRef.current = monaco
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => runRef.current())
    // The shortcut every editor uses for this, so nobody has to learn ours.
    editor.addCommand(
      monaco.KeyMod.Shift | monaco.KeyMod.Alt | monaco.KeyCode.KeyF,
      () => formatRef.current(),
    )
    // Ctrl/Cmd+S inside the editor saves the query rather than offering to save
    // the page, which is what the browser would otherwise do with it.
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => saveRef.current())
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

  /**
   * Lays the statement out, over the selection when there is one.
   *
   * Applied as an edit rather than through `setSql` so it lands in the editor's
   * own undo stack: formatting somebody's query is only safe if one Ctrl+Z puts
   * it back exactly as it was.
   */
  const format = useCallback(() => {
    const editor = editorRef.current
    const model = editor?.getModel()
    if (!editor || !model) {
      setSql((current) => formatSql(current))
      return
    }
    const selected = editor.getSelection()
    const range = selected && !selected.isEmpty() ? selected : model.getFullModelRange()
    const source = model.getValueInRange(range)
    const formatted = formatSql(source)
    if (formatted !== source) {
      editor.executeEdits('format', [{ range, text: formatted, forceMoveMarkers: true }])
    }
    editor.focus()
  }, [setSql])

  formatRef.current = format

  /**
   * The parser's opinion of the buffer, asked for while it is being typed.
   *
   * Debounced because it crosses the network, and asked about the whole buffer
   * rather than the selection because the markers are drawn on the buffer. It
   * never reports a failure of its own: a runner that is off or busy means the
   * editor marks nothing, which is the same answer as "not checked".
   */
  useEffect(() => {
    if (!sql.trim()) {
      setSyntax(null)
      return
    }
    const controller = new AbortController()
    const handle = window.setTimeout(() => {
      void validateQuery(runnerUrl, sql, controller.signal, runnerToken).then((result) => {
        if (!controller.signal.aborted) setSyntax(result)
      })
    }, SYNTAX_DELAY_MS)
    return () => {
      window.clearTimeout(handle)
      controller.abort()
    }
  }, [runnerToken, runnerUrl, sql])

  // Monaco owns the squiggle; this only says where it goes. The owner string
  // keeps it apart from any other marker source on the same model.
  useEffect(() => {
    const monaco = monacoRef.current
    const model = editorRef.current?.getModel()
    if (!monaco || !model) return
    if (!syntax || !syntax.checked || syntax.ok) {
      monaco.editor.setModelMarkers(model, SYNTAX_MARKER, [])
      return
    }
    const line = Math.min(Math.max(syntax.line ?? 1, 1), model.getLineCount())
    const column = (syntax.column ?? 0) + 1
    monaco.editor.setModelMarkers(model, SYNTAX_MARKER, [
      {
        severity: monaco.MarkerSeverity.Error,
        message: syntax.message || 'The parser refused this statement.',
        startLineNumber: line,
        endLineNumber: line,
        startColumn: Math.min(column, model.getLineMaxColumn(line)),
        endColumn: model.getLineMaxColumn(line),
      },
    ])
  }, [syntax])

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
  }, [setSql])

  /** Clicking a table with an empty editor writes the query nobody wants to type. */
  const attachTable = useCallback(
    (entry: Attachable) => {
      if (!sql.trim()) {
        setSql(`SELECT *\nFROM ${entry.alias}`)
        return
      }
      insert(entry.alias)
    },
    [insert, setSql, sql],
  )

  /* ------------------------------------------------------------- the files */

  /** Unsaved means "the library does not have this". A never-saved empty tab is
   *  not unsaved — there is nothing in it to lose. */
  const isDirty = useCallback(
    (tab: SqlTab) => (tab.queryId === null ? tab.sql.trim().length > 0 : tab.sql !== tab.savedSql),
    [],
  )

  const openTab = useCallback(() => {
    const tab = newTab()
    setTabs((current) => [...current, tab])
    setActiveTabId(tab.tabId)
  }, [])

  /** Opening a query already on screen focuses it instead of opening it twice. */
  const openSaved = useCallback(
    (queryId: string) => {
      const existing = tabs.find((tab) => tab.queryId === queryId)
      if (existing) {
        setActiveTabId(existing.tabId)
        return
      }
      const query = saved.find((item) => item.id === queryId)
      if (!query) return
      const tab = newTab({
        queryId: query.id,
        name: query.name,
        sql: query.sql,
        savedSql: query.sql,
        limit: query.limit ?? DEFAULT_LIMIT,
      })
      setTabs((current) => [...current, tab])
      setActiveTabId(tab.tabId)
    },
    [saved, tabs],
  )

  const closeTab = useCallback(
    async (tabId: string) => {
      const tab = tabs.find((item) => item.tabId === tabId)
      if (!tab) return
      if (isDirty(tab)) {
        const ok = await confirm({
          title: `Close ${tab.name}?`,
          message:
            tab.queryId === null
              ? 'This query was never saved, so closing it loses what is in it.'
              : 'The changes since the last save are not in the library and will be lost.',
          confirmLabel: 'Close',
          variant: 'danger',
        })
        if (!ok) return
      }
      setTabs((current) => {
        const next = current.filter((item) => item.tabId !== tabId)
        // The strip is never empty: an editor with no buffer has nothing to show
        // and no obvious way back to one.
        const tabsLeft = next.length > 0 ? next : [newTab()]
        if (tabId === activeTabId) setActiveTabId(tabsLeft[tabsLeft.length - 1].tabId)
        return tabsLeft
      })
    },
    [activeTabId, confirm, isDirty, tabs],
  )

  /** Writes the current tab to the library, asking for a name the first time. */
  const saveActive = useCallback(
    async (name?: string) => {
      const tab = tabs.find((item) => item.tabId === activeTabId)
      if (!tab || saving) return
      if (tab.queryId !== null && !mayEditQuery) {
        setError('You do not hold write on this saved query. Save it under a new name instead.')
        return
      }
      if (tab.queryId === null && name === undefined) {
        setNaming(tab.name === UNTITLED ? '' : tab.name)
        return
      }
      setSaving(true)
      try {
        const fields = { name: name ?? tab.name, sql: tab.sql, limit: tab.limit }
        // `update` answers null when the query is gone — deleted from another
        // tab, or from another browser against the same runner. Saving it again
        // as a new file is better than telling somebody their work has no home.
        const stored =
          (tab.queryId !== null ? await updateQuery(tab.queryId, fields) : null) ??
          (await createQuery(fields))
        // The buffer just acquired a file, and the history follows the file from
        // now on: without this, saving would drop everything run to get here.
        // It also stops being private — having a file is what makes it shared.
        if (tab.queryId === null) {
          await moveQueryHistory(runnerUrl, tab.tabId, stored.id, runnerToken)
        }
        setTabs((current) =>
          current.map((item) =>
            item.tabId === tab.tabId
              ? { ...item, queryId: stored.id, name: stored.name, savedSql: stored.sql }
              : item,
          ),
        )
        setNaming(null)
      } catch (caught) {
        setError(caught instanceof Error ? caught.message : String(caught))
      } finally {
        setSaving(false)
      }
    },
    [activeTabId, createQuery, mayEditQuery, runnerToken, runnerUrl, saving, tabs, updateQuery],
  )

  const deleteActive = useCallback(async () => {
    const tab = tabs.find((item) => item.tabId === activeTabId)
    if (!tab?.queryId) return
    if (!mayDeleteQuery) {
      setError('Deleting a saved query needs admin on it.')
      return
    }
    const ok = await confirm({
      title: `Delete ${tab.name}?`,
      message: 'The file goes from the library. The tab stays open as a draft.',
      confirmLabel: 'Delete',
      variant: 'danger',
    })
    if (!ok) return
    await removeQuery(tab.queryId)
    setTabs((current) =>
      current.map((item) =>
        item.tabId === tab.tabId ? { ...item, queryId: null, savedSql: '' } : item,
      ),
    )
  }, [activeTabId, confirm, mayDeleteQuery, removeQuery, tabs])

  const tabViews = useMemo<QueryTabView[]>(
    () =>
      tabs.map((tab) => ({
        tabId: tab.tabId,
        queryId: tab.queryId,
        name: tab.name,
        dirty: isDirty(tab),
      })),
    [isDirty, tabs],
  )

  /* ----------------------------------------------------------- the results */

  /** The plan, when the runner answered with one instead of with rows. */
  const planText = useMemo(
    () =>
      result && isPlanResult(result.columns, result.rows) ? String(result.rows[0][0]) : null,
    [result],
  )

  /**
   * The result as a file.
   *
   * What is written is what came back — the rows already capped by the limit,
   * not the query re-run without one. Exporting more than was asked for would
   * make a download a second, invisible query, and a 20-row preview would
   * quietly become a full scan.
   */
  const exportResult = useCallback(
    (format: 'csv' | 'json') => {
      if (!result) return
      const stem = active && active.name !== UNTITLED ? active.name : 'query-result'
      if (format === 'csv') {
        downloadText(
          timestampedName(stem, 'csv'),
          toCsv(result.columns, result.rows),
          'text/csv;charset=utf-8',
        )
        return
      }
      // Objects rather than arrays of cells: a JSON export is read by a program,
      // and a program that has to remember column order is reading a CSV badly.
      const objects = result.rows.map((row) =>
        Object.fromEntries(result.columns.map((column, index) => [column, row[index] ?? null])),
      )
      downloadText(timestampedName(stem, 'json'), JSON.stringify(objects, null, 2))
    },
    [active, result],
  )

  // Offered only when the result has a number in it: a chart tab over a result
  // of strings is an invitation to an empty panel.
  const plottable = useMemo(() => (result ? chartable(result.fields) : false), [result])

  const surfaceTabs = useMemo<WorkspaceTab<Surface>[]>(() => {
    const list: WorkspaceTab<Surface>[] = [{ id: 'result', label: 'Result', icon: Table2 }]
    if (plottable) list.push({ id: 'chart', label: 'Chart', icon: ChartColumn })
    if (planText) list.push({ id: 'plan', label: 'Plan', icon: Network })
    list.push({
      id: 'history',
      label: 'History',
      icon: History,
      badge:
        history.length > 0 ? (
          <span className="tabular-nums text-content-subtle">{history.length}</span>
        ) : undefined,
    })
    return list
  }, [history.length, planText, plottable])

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
  // The plan and chart tabs exist only while the last run earns them, so a run
  // that returns rows after an EXPLAIN — or strings after numbers — must not
  // leave the screen pointing at a tab that is gone.
  const shownSurface: Surface =
    (surface === 'plan' && !planText) || (surface === 'chart' && !plottable) ? 'result' : surface

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
            <QueryTabs
              tabs={tabViews}
              activeTabId={active?.tabId ?? ''}
              saved={saved}
              saving={saving}
              onSelect={setActiveTabId}
              onClose={(tabId) => void closeTab(tabId)}
              onNew={openTab}
              onOpen={openSaved}
              onSave={() => void saveActive()}
              onRename={() => setNaming(active?.name === UNTITLED ? '' : (active?.name ?? ''))}
              onDelete={() => void deleteActive()}
            />
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
              {/* The way out of the console. A query that proved itself here is
                  the same statement a Job would run, and retyping it into a
                  canvas is how a good query stays an exploration forever. */}
              <Button
                size="sm"
                variant="ghost"
                onClick={() => setCreatingJob(true)}
                disabled={!statement}
                title="Create a Job that runs this statement"
              >
                <Workflow />
                Create Job
              </Button>

              <Button
                size="sm"
                variant="ghost"
                onClick={format}
                disabled={!sql.trim()}
                title="Lay this out — the selection when there is one, the whole buffer otherwise
                  (Shift+Alt+F)"
              >
                <WandSparkles />
                Format
              </Button>

              {/* Says nothing at all unless the parser was actually asked: an
                  editor with no runner behind it marks nothing rather than
                  guessing in a dialect it does not implement. */}
              {syntax?.checked ? (
                syntax.ok ? (
                  <span
                    className="hidden items-center gap-1 text-[11px] text-state-success sm:flex"
                    title="Spark's own parser accepted this statement. It says nothing about
                      whether the tables exist."
                  >
                    <Check className="h-3 w-3" />
                    Parses
                  </span>
                ) : (
                  <span
                    className="flex items-center gap-1 text-[11px] text-state-danger"
                    title={syntax.message}
                  >
                    <TriangleAlert className="h-3 w-3" />
                    {syntax.line ? `Syntax error on line ${syntax.line}` : 'Syntax error'}
                  </span>
                )
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

          {/*
            The rows, the plan and the runs are three answers about the same
            statement, and each wants the full width — so they take turns under
            one strip instead of stacking into a page nobody scrolls twice.
          */}
          {(result || history.length > 0) && (
            <div className="card overflow-hidden p-0">
              <WorkspaceTabs
                value={shownSurface}
                onChange={setSurface}
                tabs={surfaceTabs}
                ariaLabel="Query output"
                actions={
                  result && shownSurface === 'result' ? (
                    <span className="flex items-center gap-1">
                      <Button
                        size="xs"
                        variant="ghost"
                        onClick={() => exportResult('csv')}
                        icon={<Download className="h-3 w-3" />}
                        title="Download these rows as CSV — what came back, capped as it was run"
                      >
                        CSV
                      </Button>
                      <Button
                        size="xs"
                        variant="ghost"
                        onClick={() => exportResult('json')}
                        icon={<Download className="h-3 w-3" />}
                        title="Download these rows as JSON, one object per row"
                      >
                        JSON
                      </Button>
                    </span>
                  ) : null
                }
              />

              <div
                role="tabpanel"
                id={workspacePanelId(shownSurface)}
                aria-labelledby={workspaceTabId(shownSurface)}
                className="space-y-2 p-2.5"
              >
                {shownSurface === 'result' ? (
                  result && !error ? (
                    <>
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
                              Iceberg, a JDBC driver — were on it. Jars and SQL extensions are read
                              only when a session is created, so an existing session cannot pick
                              them up."
                          >
                            <RotateCcw className="h-3 w-3" />
                            <Badge tone="info">Spark session rebuilt</Badge>
                          </span>
                        )}
                        {attached.length > 0 && (
                          <span
                            title={attached
                              .map((entry) => `${entry.alias} → ${entry.key}`)
                              .join('\n')}
                          >
                            {attached.map((entry) => entry.alias).join(', ')}
                          </span>
                        )}
                      </p>
                      <RunResultTable
                        columns={result.columns}
                        rows={result.rows}
                        fields={result.fields}
                        truncated={result.truncated}
                        maxRows={limit}
                        sortable
                        inspectable
                        emptyMessage="The query returned no rows."
                        heightClass="max-h-[26rem]"
                      />
                    </>
                  ) : (
                    <p className="rounded-xl border border-dashed border-line px-3 py-8 text-center text-xs text-content-subtle">
                      {error
                        ? 'The last run failed — the message is above, and the statement is under History.'
                        : 'Nothing run in this tab yet.'}
                    </p>
                  )
                ) : null}

                {shownSurface === 'chart' && result ? (
                  <ResultChart
                    columns={result.columns}
                    rows={result.rows}
                    fields={result.fields}
                  />
                ) : null}

                {shownSurface === 'plan' && planText ? <PlanTree text={planText} /> : null}

                {shownSurface === 'history' ? (
                  <QueryHistory
                    runs={history}
                    shared={activeQueryId !== null}
                    onRestore={(text) => {
                      setSql(text)
                      setSurface('result')
                    }}
                    onClear={() => {
                      void clearQueryHistory(
                        runnerUrl,
                        historyScope(activeQueryId, activeTabId),
                        undefined,
                        runnerToken,
                      ).then(() => setHistory([]))
                    }}
                  />
                ) : null}
              </div>
            </div>
          )}

          {!result && !error && history.length === 0 && (
            <p className="rounded-xl border border-dashed border-line px-3 py-8 text-center text-xs text-content-subtle">
              Pick a table on the left, or write a query. Results appear here.{' '}
              <span title={ASSET_HINT.table}>Tables, folders and topics all read the same way.</span>
            </p>
          )}
        </section>
      </div>

      {/* Naming is a dialog rather than an editable tab label because the name
          becomes a file name — `queries/<slug>.sql` — and a rename that happens
          by accident renames a file in somebody's repository. */}
      <Modal
        open={naming !== null}
        onOpenChange={(open) => !open && setNaming(null)}
        title={active?.queryId ? 'Rename query' : 'Save query'}
        description="The name becomes the file name in the library."
        size="sm"
        footer={
          <>
            <Button variant="ghost" onClick={() => setNaming(null)}>
              Cancel
            </Button>
            <Button
              onClick={() => void saveActive(naming ?? '')}
              disabled={saving || !(naming ?? '').trim()}
            >
              {saving ? <Spinner className="h-3.5 w-3.5" /> : null}
              Save
            </Button>
          </>
        }
      >
        <Input
          value={naming ?? ''}
          onChange={(event) => setNaming(event.target.value)}
          placeholder="Orders by day"
          aria-label="Query name"
          autoFocus
          onKeyDown={(event) => {
            if (event.key === 'Enter' && (naming ?? '').trim()) void saveActive(naming ?? '')
          }}
        />
      </Modal>

      {creatingJob && (
        <CreateJobFromQuery
          sql={statement}
          datasets={attached.map(({ key, alias, format }) => ({ key, alias, format }))}
          spark={sparkForDatasets(
            jobs,
            attached.map((entry) => entry.key),
          )}
          suggestedName={active && active.name !== UNTITLED ? active.name : 'Query job'}
          onClose={() => setCreatingJob(false)}
        />
      )}

      {confirmDialog}
    </PageShell>
  )
}
