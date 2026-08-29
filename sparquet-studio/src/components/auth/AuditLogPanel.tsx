/**
 * The runner's audit log.
 *
 * Every mutation it accepted and every request it refused — a 401, a 402 or a
 * 403 — with who asked, what they asked for and when. The rows come from the
 * server and are append-only there: nothing in this browser can write, edit or
 * delete one, which is the only reason the log is worth reading.
 *
 * Reading it needs `iam:ReadAudit`. Somebody without it is told so plainly
 * rather than shown an empty table, because "nothing happened" and "you may not
 * see what happened" are very different answers.
 */

import { RefreshCw, ScrollText } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'

import { Badge, Button, SectionTitle, Segmented, Spinner } from '@/components/ui'
import { isForbidden, listAuditEvents } from '@/lib/runner/audit'
import { cn } from '@/lib/utils/cn'
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

const LIMIT = 100

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

export function AuditLogPanel() {
  const url = useSettingsStore((state) => state.runnerUrl)
  const token = useSettingsStore((state) => state.runnerToken)
  const can = useAuthStore((state) => state.can)

  const [scope, setScope] = useState<Scope>('')
  const [deniedOnly, setDeniedOnly] = useState(false)
  const [events, setEvents] = useState<AuditEvent[]>([])
  const [loading, setLoading] = useState(true)
  const [failure, setFailure] = useState('')
  const [forbidden, setForbidden] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      setEvents(
        await listAuditEvents(
          url,
          {
            limit: LIMIT,
            action: scope || undefined,
            outcome: deniedOnly ? 'denied' : undefined,
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
  }, [deniedOnly, scope, token, url])

  useEffect(() => {
    void load()
  }, [load])

  const denied = useMemo(
    () => events.filter((event) => event.outcome === 'denied').length,
    [events],
  )

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
      <SectionTitle
        action={
          <Button
            size="xs"
            variant="ghost"
            onClick={() => void load()}
            icon={<RefreshCw className={cn('h-3 w-3', loading && 'animate-spin')} />}
          >
            Refresh
          </Button>
        }
      >
        Audit log
      </SectionTitle>

      <p className="text-2xs leading-relaxed text-content-subtle">
        Every change this runner accepted and every request it refused, newest
        first. Written by the server and never edited — the last {LIMIT} entries
        are shown.
      </p>

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
      </div>

      {loading && events.length === 0 ? (
        <div className="flex items-center justify-center py-6">
          <Spinner className="h-4 w-4" />
        </div>
      ) : failure ? (
        <p className="text-2xs leading-relaxed text-content-subtle">{failure}</p>
      ) : events.length === 0 ? (
        <p className="flex items-center justify-center gap-2 rounded-lg border border-dashed border-line px-3 py-6 text-2xs text-content-subtle">
          <ScrollText className="h-3.5 w-3.5" aria-hidden />
          Nothing recorded yet for this filter.
        </p>
      ) : (
        <div className="overflow-hidden rounded-lg border border-line">
          <table className="w-full text-left text-2xs">
            <thead className="bg-surface-sunken text-content-subtle">
              <tr>
                <th className="w-36 px-3 py-2 font-medium">When</th>
                <th className="w-32 px-3 py-2 font-medium">Who</th>
                <th className="px-3 py-2 font-medium">Action</th>
                <th className="w-24 px-3 py-2 text-right font-medium">Result</th>
              </tr>
            </thead>
            <tbody>
              {events.map((event) => (
                <tr key={event.id} className="border-t border-line align-top">
                  <td className="px-3 py-2 tabular-nums text-content-subtle">
                    {formatAt(event.at)}
                  </td>
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
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
