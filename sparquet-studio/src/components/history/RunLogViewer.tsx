/**
 * The log history of an execution, read back from the runner's database.
 *
 * These are the same lines the run panel showed live: every source — the
 * framework, the JVM, anything the job printed — funnels through the runner's
 * event queue, and the runner persists them from there. So a run opened days
 * later reads exactly like it did while it was running.
 *
 * A pipeline run is several job executions, and the live panel shows them as one
 * stream. So does this: pass every stage and the lines come back merged in
 * execution order, each tagged with the stage that printed it. Opening a past run
 * and opening the panel that watched it must not answer differently.
 */

import { Check, Copy, FileText, Search } from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'

import { Button, EmptyState, Input, Segmented, Select, Spinner } from '@/components/ui'
import { isRunnerError } from '@/lib/runner/client'
import { getJobRunLogs } from '@/lib/runner/history'
import { cn } from '@/lib/utils/cn'
import { copyText } from '@/lib/utils/download'
import { plural } from '@/lib/utils/format'
import type { RunLogRecord } from '@/types/history'

const PAGE_SIZE = 500

type LevelFilter = 'all' | 'warning' | 'error'

/** Only the floor matters: `warning` keeps errors too, or it would hide the worst. */
const MIN_RANK: Record<LevelFilter, number> = { all: 0, warning: 2, error: 3 }

function levelRank(level: string): number {
  const upper = level.toUpperCase()
  if (upper.startsWith('ERROR') || upper.startsWith('CRITICAL') || upper.startsWith('FATAL')) {
    return 3
  }
  if (upper.startsWith('WARN')) return 2
  if (upper.startsWith('DEBUG') || upper.startsWith('TRACE')) return 0
  return 1
}

function levelClass(level: string): string {
  switch (levelRank(level)) {
    case 3:
      return 'text-state-danger'
    case 2:
      return 'text-state-warning'
    case 0:
      return 'text-content-subtle'
    default:
      return 'text-content-subtle'
  }
}

/** `14:03:22` from the ISO timestamp the runner stored, or the raw value if it is not one. */
function clock(iso: string): string {
  const parsed = Date.parse(iso)
  return Number.isNaN(parsed)
    ? iso
    : new Date(parsed).toLocaleTimeString(undefined, { hour12: false })
}

function contextText(context: Record<string, unknown>): string | null {
  const keys = Object.keys(context)
  if (keys.length === 0) return null
  try {
    return JSON.stringify(context)
  } catch {
    return null
  }
}

/** One stage of the execution being read. */
export interface RunLogSource {
  /** The `job_run` id whose lines these are. */
  id: string
  /** What to call it on screen. Only shown when there is more than one. */
  label?: string
}

/** A stored line plus which stage it came from. */
interface MergedLine extends RunLogRecord {
  sourceId: string
  sourceLabel: string
  /** Position of the stage in the run — what puts the merged stream in order. */
  sourceIndex: number
}

function lineText(line: MergedLine, withStage: boolean): string {
  const context = contextText(line.context)
  const stage = withStage ? `[${line.sourceLabel}] ` : ''
  return `${stage}${line.timestamp} ${line.level} [${line.source}] ${line.message}${context ? ` ${context}` : ''}`
}

/**
 * Execution order, not wall-clock order.
 *
 * Stages run one after another, so the stage index already orders them, and `seq`
 * orders the lines inside a stage. Sorting on the timestamp instead would let two
 * lines written in the same millisecond swap places on every render.
 */
function byExecutionOrder(a: MergedLine, b: MergedLine): number {
  return a.sourceIndex - b.sourceIndex || a.seq - b.seq
}

export interface RunLogViewerProps {
  /** The stages to read, in execution order. One entry for a job run. */
  sources: RunLogSource[]
  runnerUrl: string
  runnerToken?: string
  className?: string
}

export function RunLogViewer({ sources, runnerUrl, runnerToken, className }: RunLogViewerProps) {
  const [lines, setLines] = useState<MergedLine[]>([])
  const [total, setTotal] = useState(0)
  /** Per stage, the `seq` to continue from; null when that stage is fully read. */
  const [cursors, setCursors] = useState<Record<string, number | null>>({})
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [query, setQuery] = useState('')
  const [level, setLevel] = useState<LevelFilter>('all')
  const [source, setSource] = useState('all')
  const [stage, setStage] = useState('all')
  const [copied, setCopied] = useState(false)
  const copyTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  const stages = useMemo(
    () =>
      sources.map((entry, index) => ({
        id: entry.id,
        label: entry.label ?? `stage ${index + 1}`,
        index,
      })),
    [sources],
  )
  // The identity of the request, so a re-render with an equal array does not refetch.
  const stageKey = stages.map((entry) => entry.id).join('|')
  const multiStage = stages.length > 1

  // A different execution is a different log entirely — filters and pages reset
  // with it, or the first page of one run would sit above the next.
  useEffect(() => {
    const controller = new AbortController()
    setLines([])
    setTotal(0)
    setCursors({})
    setStage('all')
    setError(null)
    if (stages.length === 0) return
    setLoading(true)
    Promise.all(
      stages.map((entry) =>
        getJobRunLogs(runnerUrl, entry.id, { limit: PAGE_SIZE }, controller.signal, runnerToken)
          .then((page) => ({ entry, page })),
      ),
    )
      .then((pages) => {
        if (controller.signal.aborted) return
        const merged: MergedLine[] = []
        const nextCursors: Record<string, number | null> = {}
        let sum = 0
        for (const { entry, page } of pages) {
          for (const line of page.lines) {
            merged.push({
              ...line,
              sourceId: entry.id,
              sourceLabel: entry.label,
              sourceIndex: entry.index,
            })
          }
          nextCursors[entry.id] = page.nextAfter
          sum += page.total
        }
        setLines(merged.sort(byExecutionOrder))
        setCursors(nextCursors)
        setTotal(sum)
      })
      .catch((err: unknown) => {
        if (controller.signal.aborted) return
        setError(isRunnerError(err) ? err.message : 'Failed to load the logs of this run.')
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false)
      })
    return () => controller.abort()
    // `stages` is rebuilt on every render; `stageKey` is what actually changed.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stageKey, runnerUrl, runnerToken])

  useEffect(() => () => {
    if (copyTimer.current) clearTimeout(copyTimer.current)
  }, [])

  const sourceNames = useMemo(() => {
    const seen = new Set<string>()
    for (const line of lines) seen.add(line.source)
    return [...seen].sort()
  }, [lines])

  const visible = useMemo(() => {
    const needle = query.trim().toLowerCase()
    const floor = MIN_RANK[level]
    return lines.filter((line) => {
      if (levelRank(line.level) < floor) return false
      if (source !== 'all' && line.source !== source) return false
      if (stage !== 'all' && line.sourceId !== stage) return false
      if (!needle) return true
      return (
        line.message.toLowerCase().includes(needle) ||
        line.source.toLowerCase().includes(needle) ||
        line.level.toLowerCase().includes(needle)
      )
    })
  }, [lines, query, level, source, stage])

  /** Stages with lines left to read. */
  const pending = stages.filter((entry) => typeof cursors[entry.id] === 'number')

  const loadMore = () => {
    if (pending.length === 0 || loading) return
    setLoading(true)
    Promise.all(
      pending.map((entry) =>
        getJobRunLogs(
          runnerUrl,
          entry.id,
          { after: cursors[entry.id] ?? 0, limit: PAGE_SIZE },
          undefined,
          runnerToken,
        ).then((page) => ({ entry, page })),
      ),
    )
      .then((pages) => {
        const added: MergedLine[] = []
        const nextCursors: Record<string, number | null> = {}
        for (const { entry, page } of pages) {
          for (const line of page.lines) {
            added.push({
              ...line,
              sourceId: entry.id,
              sourceLabel: entry.label,
              sourceIndex: entry.index,
            })
          }
          nextCursors[entry.id] = page.nextAfter
        }
        setLines((current) => [...current, ...added].sort(byExecutionOrder))
        setCursors((current) => ({ ...current, ...nextCursors }))
      })
      .catch((err: unknown) => {
        setError(isRunnerError(err) ? err.message : 'Failed to load more log lines.')
      })
      .finally(() => setLoading(false))
  }

  const copyAll = () => {
    void copyText(visible.map((line) => lineText(line, multiStage)).join('\n')).then((ok) => {
      if (!ok) return
      setCopied(true)
      if (copyTimer.current) clearTimeout(copyTimer.current)
      copyTimer.current = setTimeout(() => setCopied(false), 1400)
    })
  }

  const filtering =
    query.trim().length > 0 || level !== 'all' || source !== 'all' || stage !== 'all'

  return (
    <div className={cn('flex min-h-0 flex-col gap-2', className)}>
      <div className="flex flex-wrap items-center gap-2">
        <Input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search these logs…"
          aria-label="Search these logs"
          leading={<Search className="h-3.5 w-3.5" />}
          className="h-8 min-w-[10rem] flex-1 text-xs"
        />
        <Segmented
          size="sm"
          ariaLabel="Minimum level"
          value={level}
          onChange={setLevel}
          options={[
            { value: 'all', label: 'All' },
            { value: 'warning', label: 'Warn+' },
            { value: 'error', label: 'Errors' },
          ]}
        />
        {multiStage && (
          <Select
            value={stage}
            onValueChange={setStage}
            ariaLabel="Stage"
            className="h-8 w-32 text-xs"
            options={[
              { value: 'all', label: 'All stages' },
              ...stages.map((entry) => ({ value: entry.id, label: entry.label })),
            ]}
          />
        )}
        {sourceNames.length > 1 && (
          <Select
            value={source}
            onValueChange={setSource}
            ariaLabel="Log source"
            className="h-8 w-28 text-xs"
            options={[
              { value: 'all', label: 'All sources' },
              ...sourceNames.map((name) => ({ value: name, label: name })),
            ]}
          />
        )}
        <Button
          size="xs"
          variant="ghost"
          icon={copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
          disabled={visible.length === 0}
          onClick={copyAll}
        >
          {copied ? 'Copied' : 'Copy'}
        </Button>
      </div>

      {error && <p className="text-2xs text-state-danger">{error}</p>}

      {loading && lines.length === 0 && (
        <div className="flex items-center gap-2 text-2xs text-content-subtle">
          <Spinner className="h-3.5 w-3.5" /> Loading logs…
        </div>
      )}

      {!loading && lines.length === 0 && !error && (
        <EmptyState
          icon={<FileText />}
          title="No logs recorded"
          description="This execution kept no log lines — runs from before log history was recorded show up empty here."
        />
      )}

      {lines.length > 0 && (
        <>
          <div className="scroll-area min-h-0 flex-1 rounded-lg border border-line bg-surface-sunken/60 py-1 font-mono text-2xs leading-relaxed">
            {visible.length === 0 ? (
              <p className="px-2 py-3 text-center text-content-subtle">
                No line matches this filter.
              </p>
            ) : (
              visible.map((line) => {
                const context = contextText(line.context)
                return (
                  <div
                    key={`${line.sourceId}:${line.seq}`}
                    className="flex gap-2 px-2 py-0.5 hover:bg-surface-sunken"
                  >
                    <span className="shrink-0 tabular-nums text-content-subtle">
                      {clock(line.timestamp)}
                    </span>
                    {multiStage && (
                      <span
                        className="hidden w-24 shrink-0 truncate text-content-subtle sm:inline"
                        title={line.sourceLabel}
                      >
                        {line.sourceLabel}
                      </span>
                    )}
                    <span
                      className={cn('w-12 shrink-0 uppercase', levelClass(line.level))}
                      title={line.level}
                    >
                      {line.level.slice(0, 4)}
                    </span>
                    <span className="min-w-0 flex-1">
                      <span className="whitespace-pre-wrap break-words text-content-muted">
                        {line.message}
                      </span>
                      {context && (
                        <span className="block break-words text-content-subtle">{context}</span>
                      )}
                    </span>
                    <span className="hidden shrink-0 text-content-subtle sm:inline">
                      {line.source}
                    </span>
                  </div>
                )
              })
            )}
          </div>

          <div className="flex items-center justify-between gap-2 text-2xs text-content-subtle">
            <span>
              {filtering
                ? `${visible.length} of ${plural(lines.length, 'line')} shown`
                : `${plural(lines.length, 'line')} of ${total}`}
            </span>
            {pending.length > 0 && (
              <Button size="xs" variant="ghost" disabled={loading} onClick={loadMore}>
                {loading
                  ? 'Loading…'
                  : `Load ${Math.min(PAGE_SIZE * pending.length, Math.max(1, total - lines.length))} more`}
              </Button>
            )}
          </div>
        </>
      )}
    </div>
  )
}
