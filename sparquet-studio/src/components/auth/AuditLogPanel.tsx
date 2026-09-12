/**
 * The runner's audit log, as its own page.
 *
 * Every mutation it accepted and every request it refused — a 401, a 402 or a
 * 403 — with who asked, what they asked for and when. The rows come from the
 * server and are append-only there: nothing in this browser can write, edit or
 * delete one, which is the only reason the log is worth reading.
 *
 * Reading it needs `iam:ReadAudit`. Somebody without it is told so plainly
 * rather than shown an empty table, because "nothing happened" and "you may not
 * see what happened" are very different answers.
 *
 * Two kinds of filter, and they are not the same kind of thing. The service,
 * the outcome, the window and the cap are asked of the server — they decide
 * which rows are fetched at all. The search box narrows what came back, here,
 * with no round trip: somebody scanning a log types and retypes, and a request
 * per keystroke would make the runner audit the auditing.
 */

import { ChevronRight, Download, RefreshCw, ScrollText, Search } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'

import { Badge, Button, Input, SectionTitle, Segmented, Select, Spinner } from '@/components/ui'
import { isForbidden, listAuditEvents } from '@/lib/runner/audit'
import { cn } from '@/lib/utils/cn'
import { toCsv, timestampedName } from '@/lib/utils/csv'
import { downloadText } from '@/lib/utils/download'
import { useAuthStore } from '@/store/auth'
import { useSettingsStore } from '@/store/settings'
import type { AuditEvent } from '@/types/audit'

/** The services worth filtering by, as prefixes the server understands. */
const SCOPES = [
  { value: '', label: 'Everything' },
  { value: 'iam:*', label: 'Access' },
  { value: 'run:*', label: 'Runs' },
  { value: 'credits:*', label: 'Credits' },
] as const

type Scope = (typeof SCOPES)[number]['value']

/** How far back to ask, in hours. `0` means "as far as the cap reaches". */
const WINDOWS = [
  { value: '1', label: 'Last hour' },
  { value: '24', label: 'Last 24 hours' },
  { value: '168', label: 'Last 7 days' },
  { value: '720', label: 'Last 30 days' },
  { value: '0', label: 'Everything' },
]

const DEFAULT_WINDOW = '168'

/** How many rows to fetch. The log can be long; the browser holds all of them. */
const LIMITS = ['100', '250', '500', '1000']

const DEFAULT_LIMIT = '250'

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** `2026-08-29T14:03:11Z` as a local date and time, seconds included: an audit
 *  trail whose rows cannot be ordered by eye is half a trail. */
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

/** Everything on a row a search could reasonably mean, lowercased once. */
function haystack(event: AuditEvent): string {
  return [
    event.actor,
    event.actorId ?? '',
    event.team ?? '',
    event.action,
    event.method,
    event.path,
    event.resource ?? '',
    event.outcome,
    event.status === null ? '' : String(event.status),
    event.ip ?? '',
    event.roles.join(' '),
    event.detail ? JSON.stringify(event.detail) : '',
  ]
    .join(' ')
    .toLowerCase()
}

const CSV_COLUMNS = [
  'at',
  'actor',
  'actor_id',
  'team',
  'roles',
  'action',
  'method',
  'path',
  'resource',
  'outcome',
  'status',
  'ip',
  'detail',
]

/** The rows as they came from the server, not as the table renders them. */
function csvRow(event: AuditEvent): unknown[] {
  return [
    event.at,
    event.actor,
    event.actorId,
    event.team,
    event.roles.join(' '),
    event.action,
    event.method,
    event.path,
    event.resource,
    event.outcome,
    event.status,
    event.ip,
    event.detail ? JSON.stringify(event.detail) : null,
  ]
}

export function AuditLogPanel() {
  const url = useSettingsStore((state) => state.runnerUrl)
  const token = useSettingsStore((state) => state.runnerToken)
  const can = useAuthStore((state) => state.can)

  const [scope, setScope] = useState<Scope>('')
  const [deniedOnly, setDeniedOnly] = useState(false)
  const [windowHours, setWindowHours] = useState(DEFAULT_WINDOW)
  const [limit, setLimit] = useState(DEFAULT_LIMIT)
  const [search, setSearch] = useState('')
  const [events, setEvents] = useState<AuditEvent[]>([])
  const [expanded, setExpanded] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [failure, setFailure] = useState('')
  const [forbidden, setForbidden] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    const hours = Number(windowHours)
    try {
      setEvents(
        await listAuditEvents(
          url,
          {
            limit: Number(limit),
            action: scope || undefined,
            outcome: deniedOnly ? 'denied' : undefined,
            since:
              hours > 0 ? new Date(Date.now() - hours * 3_600_000).toISOString() : undefined,
          },
          token,
        ),
      )
      setFailure('')
      setForbidden(false)
    } catch (error) {
      setEvents([])
      setForbidden(isForbidden(error))
      setFailure(isForbidden(error) ? '' : messageOf(error))
    } finally {
      setLoading(false)
    }
  }, [deniedOnly, limit, scope, token, url, windowHours])

  useEffect(() => {
    void load()
  }, [load])

  const visible = useMemo(() => {
    const needle = search.trim().toLowerCase()
    if (!needle) return events
    return events.filter((event) => haystack(event).includes(needle))
  }, [events, search])

  const denied = useMemo(
    () => visible.filter((event) => event.outcome === 'denied').length,
    [visible],
  )

  const exportCsv = useCallback(() => {
    downloadText(
      timestampedName('audit-log', 'csv'),
      toCsv(CSV_COLUMNS, visible.map(csvRow)),
      'text/csv;charset=utf-8',
    )
  }, [visible])

  if (!can('iam:ReadAudit') || forbidden) {
    return (
      <div className="space-y-2">
        <SectionTitle>Audit log</SectionTitle>
        <p className="rounded-lg border border-dashed border-line px-3 py-6 text-center text-2xs leading-relaxed text-content-subtle">
          Reading the audit log needs <code>iam:ReadAudit</code>. Ask an
          administrator of this runner for the permission — the log itself is being
          written either way.
        </p>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <Segmented
          value={scope}
          onChange={setScope}
          options={SCOPES.map((item) => ({ value: item.value, label: item.label }))}
          size="sm"
          ariaLabel="Filter by service"
        />
        <button
          type="button"
          onClick={() => setDeniedOnly((value) => !value)}
          aria-pressed={deniedOnly}
          className={cn(
            'rounded-md border px-2 py-1 text-2xs transition-colors',
            deniedOnly
              ? 'border-state-danger/40 bg-state-danger/10 text-state-danger'
              : 'border-line text-content-subtle hover:border-line-strong hover:text-content',
          )}
        >
          Refused only{denied > 0 && !deniedOnly ? ` (${denied})` : ''}
        </button>

        <Select
          value={windowHours}
          onValueChange={setWindowHours}
          options={WINDOWS}
          className="h-7 w-36 text-2xs"
          ariaLabel="Time window"
        />
        <Select
          value={limit}
          onValueChange={setLimit}
          options={LIMITS.map((value) => ({ value, label: `${value} rows` }))}
          className="h-7 w-28 text-2xs"
          ariaLabel="How many rows to fetch"
        />

        <div className="min-w-[12rem] flex-1">
          <Input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search actor, action, path, resource, IP…"
            aria-label="Search the rows already loaded"
            className="h-7 text-2xs"
            leading={<Search aria-hidden />}
          />
        </div>

        <Button
          size="xs"
          variant="ghost"
          onClick={exportCsv}
          disabled={visible.length === 0}
          icon={<Download className="h-3 w-3" />}
          title="Download what is shown, as it came from the server"
        >
          CSV
        </Button>
        <Button
          size="xs"
          variant="ghost"
          onClick={() => void load()}
          icon={<RefreshCw className={cn('h-3 w-3', loading && 'animate-spin')} />}
        >
          Refresh
        </Button>
      </div>

      <p className="text-2xs leading-relaxed text-content-subtle">
        {visible.length === events.length
          ? `${events.length} events`
          : `${visible.length} of ${events.length} events`}
        {denied > 0 ? ` · ${denied} refused` : ''}
        {events.length === Number(limit)
          ? ' · the cap was reached, so there may be older events than these'
          : ''}
      </p>

      {loading && events.length === 0 ? (
        <div className="flex items-center justify-center py-6">
          <Spinner className="h-4 w-4" />
        </div>
      ) : failure ? (
        <p className="text-2xs leading-relaxed text-content-subtle">{failure}</p>
      ) : visible.length === 0 ? (
        <p className="flex items-center justify-center gap-2 rounded-lg border border-dashed border-line px-3 py-6 text-2xs text-content-subtle">
          <ScrollText className="h-3.5 w-3.5" aria-hidden />
          {events.length === 0
            ? 'Nothing recorded yet for this filter.'
            : 'No loaded event matches that search.'}
        </p>
      ) : (
        /*
          The log scrolls inside its own box rather than growing the page: the
          filters have to stay reachable while somebody reads row four hundred,
          and the header has to keep naming the columns of a row read at the
          bottom. The box takes what the viewport has left over, so a tall screen
          shows more rows without anything being configured.
        */
        <div className="max-h-[calc(100vh-22rem)] min-h-[20rem] overflow-y-auto overscroll-contain rounded-lg border border-line">
          <table className="w-full text-left text-2xs">
            <thead className="sticky top-0 z-10 bg-surface-sunken text-content-subtle shadow-[0_1px_0_0_rgb(var(--line))]">
              <tr>
                <th className="w-6 px-1 py-2" />
                <th className="w-36 px-3 py-2 font-medium">When</th>
                <th className="w-32 px-3 py-2 font-medium">Who</th>
                <th className="px-3 py-2 font-medium">Action</th>
                <th className="w-24 px-3 py-2 text-right font-medium">Result</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((event) => {
                const open = expanded === event.id
                return (
                  <AuditRow
                    key={event.id}
                    event={event}
                    open={open}
                    onToggle={() => setExpanded(open ? null : event.id)}
                  />
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

/**
 * One event, and — when opened — the rest of what the server recorded about it.
 *
 * The columns answer "who did what, and did it work". The detail answers "and
 * what exactly", which is the question asked of perhaps one row in a hundred:
 * the IP, the roles the decision was made with, the resource, and whatever the
 * route attached. Showing all of that inline would make every row four lines
 * tall to serve the one row somebody is actually looking for.
 */
function AuditRow({
  event,
  open,
  onToggle,
}: {
  event: AuditEvent
  open: boolean
  onToggle: () => void
}) {
  const facts: [string, string][] = []
  if (event.resource) facts.push(['Resource', event.resource])
  if (event.ip) facts.push(['IP', event.ip])
  if (event.roles.length > 0) facts.push(['Roles', event.roles.join(', ')])
  if (event.actorId) facts.push(['Actor id', event.actorId])
  facts.push(['At', event.at])

  return (
    <>
      <tr
        className={cn(
          'cursor-pointer border-t border-line align-top hover:bg-surface-sunken/60',
          open && 'bg-surface-sunken/60',
        )}
        onClick={onToggle}
      >
        <td className="px-1 py-2">
          <button
            type="button"
            aria-expanded={open}
            aria-label={open ? 'Hide the details' : 'Show the details'}
            onClick={(clicked) => {
              clicked.stopPropagation()
              onToggle()
            }}
            className="flex h-5 w-5 items-center justify-center rounded text-content-subtle hover:text-content"
          >
            <ChevronRight className={cn('h-3 w-3 transition-transform', open && 'rotate-90')} />
          </button>
        </td>
        <td className="px-3 py-2 tabular-nums text-content-subtle">{formatAt(event.at)}</td>
        <td className="px-3 py-2">
          <span className="block truncate text-content">{event.actor}</span>
          {event.team ? (
            <span className="block truncate text-content-subtle">{event.team}</span>
          ) : null}
        </td>
        <td className="min-w-0 px-3 py-2">
          <span className="block truncate text-content">{event.action}</span>
          <span className="block truncate font-mono text-content-subtle">
            {event.method} {event.path}
          </span>
        </td>
        <td className="px-3 py-2 text-right">
          <Badge tone={event.outcome === 'denied' ? 'danger' : 'neutral'}>
            {event.outcome === 'denied'
              ? `denied${event.status ? ` ${event.status}` : ''}`
              : 'ok'}
          </Badge>
        </td>
      </tr>
      {open ? (
        <tr className="border-t border-line bg-surface-sunken/40">
          <td />
          <td colSpan={4} className="space-y-2 px-3 pb-3 pt-1">
            <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1">
              {facts.map(([label, value]) => (
                <div key={label} className="contents">
                  <dt className="text-content-subtle">{label}</dt>
                  <dd className="truncate font-mono text-content">{value}</dd>
                </div>
              ))}
            </dl>
            {event.detail ? (
              <pre className="max-h-48 overflow-auto rounded-md border border-line bg-surface px-2 py-1.5 font-mono text-2xs leading-relaxed text-content-subtle">
                {JSON.stringify(event.detail, null, 2)}
              </pre>
            ) : null}
          </td>
        </tr>
      ) : null}
    </>
  )
}
