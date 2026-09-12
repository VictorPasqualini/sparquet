/**
 * What this query has been run as.
 *
 * The editor holds one version of the text — the current one — so the statement
 * that answered the question twenty minutes ago exists nowhere the moment it is
 * edited. This is where it exists: every run of this query, newest first, with
 * what it cost and whether it worked, and a way to put any of them back in the
 * buffer.
 *
 * Kept per browser, not in the library; see `lib/sql/history`.
 */

import { Check, Copy, History, Trash2 } from 'lucide-react'
import { useState } from 'react'

import { Badge, Button } from '@/components/ui'
import type { QueryRun } from '@/lib/sql/history'
import { cn } from '@/lib/utils/cn'
import { copyText } from '@/lib/utils/download'

function formatAt(value: string): string {
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return value
  return parsed.toLocaleString(undefined, {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

/** The statement on one line, so a list of runs stays a list. */
function oneLine(sql: string): string {
  return sql.replace(/\s+/g, ' ').trim()
}

export function QueryHistory({
  runs,
  onRestore,
  onClear,
}: {
  runs: QueryRun[]
  /** Puts the statement back in the editor. */
  onRestore: (sql: string) => void
  onClear: () => void
}) {
  const [expanded, setExpanded] = useState<string | null>(null)
  const [copied, setCopied] = useState<string | null>(null)

  if (runs.length === 0) {
    return (
      <p className="flex items-center justify-center gap-2 rounded-xl border border-dashed border-line px-3 py-8 text-center text-xs text-content-subtle">
        <History className="h-3.5 w-3.5" aria-hidden />
        Nothing run from this query yet. Every run is kept here — the statement,
        not just the result.
      </p>
    )
  }

  return (
    <div className="overflow-hidden rounded-xl border border-line bg-surface">
      <div className="flex items-center gap-2 border-b border-line bg-surface-sunken px-2.5 py-1.5">
        <span className="text-[11px] text-content-subtle">
          {runs.length} {runs.length === 1 ? 'run' : 'runs'}, newest first — this browser only
        </span>
        <Button
          size="xs"
          variant="ghost"
          className="ml-auto"
          onClick={onClear}
          icon={<Trash2 className="h-3 w-3" />}
        >
          Clear
        </Button>
      </div>

      <ul className="max-h-[26rem] divide-y divide-line overflow-auto">
        {runs.map((run) => {
          const open = expanded === run.id
          return (
            <li key={run.id}>
              <div
                className={cn(
                  'flex cursor-pointer items-start gap-2 px-2.5 py-1.5 hover:bg-surface-sunken/60',
                  open && 'bg-surface-sunken/60',
                )}
                onClick={() => setExpanded(open ? null : run.id)}
              >
                <span className="w-32 shrink-0 text-[11px] tabular-nums text-content-subtle">
                  {formatAt(run.at)}
                </span>
                <span className="min-w-0 flex-1">
                  <span
                    className={cn(
                      'block truncate font-mono text-[11px]',
                      open ? 'whitespace-pre-wrap' : '',
                      run.error ? 'text-state-danger' : 'text-content',
                    )}
                  >
                    {open ? run.sql : oneLine(run.sql)}
                  </span>
                  {run.error ? (
                    <span className="block truncate text-[11px] text-state-danger/90">
                      {run.error}
                    </span>
                  ) : null}
                </span>
                <span className="flex shrink-0 items-center gap-1.5 text-[11px] tabular-nums text-content-subtle">
                  {run.error ? (
                    <Badge tone="danger">failed</Badge>
                  ) : (
                    <>
                      <span>{run.rows} rows</span>
                      {run.truncated ? <Badge tone="warning">cut</Badge> : null}
                      <span>{run.elapsedMs} ms</span>
                    </>
                  )}
                </span>
              </div>
              {open ? (
                <div
                  className="flex items-center gap-1 px-2.5 pb-2 pl-[9rem]"
                  onClick={(event) => event.stopPropagation()}
                >
                  <Button size="xs" variant="secondary" onClick={() => onRestore(run.sql)}>
                    Put back in the editor
                  </Button>
                  <Button
                    size="xs"
                    variant="ghost"
                    onClick={() => {
                      void copyText(run.sql).then((ok) => {
                        if (!ok) return
                        setCopied(run.id)
                        globalThis.setTimeout(() => setCopied(null), 1500)
                      })
                    }}
                    icon={
                      copied === run.id ? (
                        <Check className="h-3 w-3" />
                      ) : (
                        <Copy className="h-3 w-3" />
                      )
                    }
                  >
                    {copied === run.id ? 'Copied' : 'Copy'}
                  </Button>
                  <span className="text-[11px] text-content-subtle">
                    run with a cap of {run.limit} rows
                  </span>
                </div>
              ) : null}
            </li>
          )
        })}
      </ul>
    </div>
  )
}
