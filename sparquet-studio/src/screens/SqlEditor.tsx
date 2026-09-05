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
 */

import Editor, { type EditorProps, type OnMount } from '@monaco-editor/react'
import { ChevronRight, Database, Play, Search, SquareTerminal, X } from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import { RunResultTable } from '@/components/panels/RunResultTable'
import {
  Badge,
  Button,
  EmptyState,
  Input,
  Kbd,
  Select,
  Spinner,
} from '@/components/ui'
import { deriveSchemas } from '@/lib/datacatalog'
import { buildLineage } from '@/lib/lineage'
import { configureMonaco } from '@/lib/monaco'
import {
  cancelQuery,
  isRunnerError,
  runQuery,
  type QuerySource,
  type RunnerQueryResult,
} from '@/lib/runner/client'
import { mentionedAliases, viewAlias } from '@/lib/sql/views'
import { cn } from '@/lib/utils/cn'
import { useLibraryStore } from '@/store/library'
import { useSettingsStore } from '@/store/settings'

configureMonaco()

/** Where the draft survives a reload. One editor, one draft — no history yet. */
const DRAFT_KEY = 'sparquet-studio:sql-draft'

/** The runner caps at 1000; anything larger belongs in a Job, not in a preview. */
const LIMITS = [50, 100, 500, 1000] as const

const DEFAULT_LIMIT = 100

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
}

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

export function SqlEditor() {
  const navigate = useNavigate()
  const jobs = useLibraryStore((state) => state.jobs)
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const theme = useSettingsStore((state) => state.theme)

  const [sql, setSql] = useState(readDraft)
  const [limit, setLimit] = useState<number>(DEFAULT_LIMIT)
  const [query, setQuery] = useState('')
  const [expanded, setExpanded] = useState<string | null>(null)
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState<RunnerQueryResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  const abortRef = useRef<AbortController | null>(null)
  const queryIdRef = useRef<string | null>(null)
  // Monaco binds the shortcut once, so the command has to reach the CURRENT run.
  const runRef = useRef<() => void>(() => {})

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

  const aliases = useMemo(() => attachables.map((entry) => entry.alias), [attachables])
  const used = useMemo(() => new Set(mentionedAliases(sql, aliases)), [sql, aliases])

  const visible = useMemo(() => {
    const needle = query.trim().toLowerCase()
    if (!needle) return attachables
    return attachables.filter(
      (entry) =>
        entry.key.toLowerCase().includes(needle) ||
        entry.alias.includes(needle) ||
        entry.columns.some((column) => column.toLowerCase().includes(needle)),
    )
  }, [attachables, query])

  useEffect(() => {
    try {
      localStorage.setItem(DRAFT_KEY, sql)
    } catch {
      // Nothing to do: the draft is a convenience, not the user's data.
    }
  }, [sql])

  const run = useCallback(async () => {
    const statement = sql.trim()
    if (!statement || running) return

    const sources: QuerySource[] = attachables
      .filter((entry) => used.has(entry.alias))
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
        { sql: statement, sources, limit, queryId },
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
  }, [attachables, limit, running, runnerToken, runnerUrl, sql, used])

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
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => runRef.current())
    editor.focus()
  }, [])

  const insert = useCallback(
    (text: string) => {
      setSql((current) => {
        if (!current.trim()) return `SELECT *\nFROM ${text}\nLIMIT 100`
        const separator = current.endsWith(' ') || current.endsWith('\n') ? '' : ' '
        return `${current}${separator}${text}`
      })
    },
    [setSql],
  )

  if (attachables.length === 0) {
    return (
      <div className="mx-auto w-full max-w-6xl px-6 py-8 animate-fade-in">
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
      </div>
    )
  }

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8 animate-fade-in">
      <header className="mb-6 flex items-start gap-4">
        <div className="space-y-1">
          <h1 className="text-sm font-semibold text-content">SQL editor</h1>
          <p className="max-w-2xl text-xs leading-relaxed text-content-muted">
            Query the datasets the catalog already knows about. The runner opens each one with the
            same reader a Job uses — Delta and Iceberg tables included — and registers it as a
            temporary view named after its address, so the SQL never carries a path. Reads only:
            SELECT, WITH, EXPLAIN, DESCRIBE and SHOW. Writing data stays in a Job, where it can be
            reviewed and re-run.
          </p>
        </div>
      </header>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,17rem)_minmax(0,1fr)]">
        <aside className="card flex max-h-[36rem] flex-col p-0">
          <div className="border-b border-line p-2.5">
            <Input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Filter datasets and columns"
              leading={<Search className="h-3.5 w-3.5" />}
              aria-label="Filter datasets"
            />
          </div>
          <ul className="min-h-0 flex-1 overflow-auto p-1.5">
            {visible.map((entry) => {
              const open = expanded === entry.key
              return (
                <li key={entry.key}>
                  <div
                    className={cn(
                      'flex items-center gap-1 rounded-lg px-1.5 py-1',
                      used.has(entry.alias) && 'bg-brand-500/10',
                    )}
                  >
                    <button
                      type="button"
                      className="shrink-0 rounded p-0.5 text-content-subtle hover:text-content"
                      onClick={() => setExpanded(open ? null : entry.key)}
                      aria-label={open ? `Hide ${entry.alias} columns` : `Show ${entry.alias} columns`}
                      aria-expanded={open}
                    >
                      <ChevronRight className={cn('h-3 w-3 transition-transform', open && 'rotate-90')} />
                    </button>
                    <button
                      type="button"
                      className="min-w-0 flex-1 text-left"
                      onClick={() => insert(entry.alias)}
                      title={`${entry.key} — click to use in the query`}
                    >
                      <span className="block truncate font-mono text-2xs text-content">
                        {entry.alias}
                      </span>
                      <span className="block truncate text-[11px] text-content-subtle">
                        {entry.key}
                      </span>
                    </button>
                    <Badge tone="neutral">{entry.format}</Badge>
                  </div>
                  {open && (
                    <ul className="mb-1 ml-6 border-l border-line pl-2">
                      {entry.columns.length === 0 && (
                        <li className="py-1 text-[11px] text-content-subtle">
                          No columns derived from the canvas.
                        </li>
                      )}
                      {entry.columns.map((column) => (
                        <li key={column}>
                          <button
                            type="button"
                            className="w-full truncate rounded px-1.5 py-0.5 text-left font-mono text-2xs text-content-muted hover:bg-surface-sunken hover:text-content"
                            onClick={() => insert(column)}
                          >
                            {column}
                          </button>
                        </li>
                      ))}
                    </ul>
                  )}
                </li>
              )
            })}
            {visible.length === 0 && (
              <li className="px-2 py-6 text-center text-xs text-content-subtle">
                No dataset matches “{query}”.
              </li>
            )}
          </ul>
          <p className="border-t border-line px-2.5 py-2 text-[11px] text-content-subtle">
            {used.size === 0
              ? 'Only the datasets your query names are opened.'
              : `${used.size} attached to this query.`}
          </p>
        </aside>

        <section className="min-w-0 space-y-3">
          <div className="card flex flex-col p-0">
            <header className="flex flex-wrap items-center gap-2 border-b border-line px-2.5 py-2">
              <Button size="sm" onClick={() => void run()} disabled={running || !sql.trim()}>
                {running ? <Spinner className="h-3.5 w-3.5" /> : <Play />}
                Run
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
              <div className="ml-auto flex items-center gap-2">
                <label className="text-[11px] text-content-subtle" htmlFor="sql-limit">
                  Rows
                </label>
                <Select
                  id="sql-limit"
                  ariaLabel="Rows to return"
                  value={String(limit)}
                  onValueChange={(value) => setLimit(Number(value))}
                  options={LIMITS.map((value) => ({ value: String(value), label: String(value) }))}
                />
              </div>
            </header>
            <div className="h-72 min-h-0">
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
          </div>

          {error && (
            <p className="rounded-xl border border-state-danger/40 bg-state-danger/10 px-3 py-2 text-xs text-state-danger" role="alert">
              {error}
            </p>
          )}

          {result && !error && (
            <div className="space-y-2">
              <p className="flex flex-wrap items-center gap-2 text-[11px] text-content-subtle">
                <span className="tabular-nums">{result.rows.length} rows</span>
                <span aria-hidden>·</span>
                <span className="tabular-nums">{result.elapsedMs} ms</span>
                {result.truncated && (
                  <>
                    <span aria-hidden>·</span>
                    <span>cut at the row limit</span>
                  </>
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
              Pick a dataset on the left, or write a query. Results appear here.
            </p>
          )}
        </section>
      </div>
    </div>
  )
}
