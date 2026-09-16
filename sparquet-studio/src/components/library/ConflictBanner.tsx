/**
 * What the editor says when somebody else saved the same record first.
 *
 * A library is a folder, so two Studios can be open over it — a network share, a
 * synced folder, two checkouts of one repository, or simply two tabs. Until this
 * banner there was no message at all: the store had carried a `conflict` field
 * for a while and nothing rendered it, so the second save either landed on top
 * of the first in silence, or (once the runner started refusing stale writes)
 * failed with nothing on screen to explain why the editor had stopped saving.
 *
 * Two situations reach here and they are genuinely different:
 *
 *  - `refused` — the runner would not take the write. The edits on screen are
 *    NOT on disk, and they will stay unsaved until the person picks a side. That
 *    is deliberate: silently retrying after a banner nobody read is how the other
 *    machine's work disappears.
 *  - not `refused` — this browser noticed a second tab wrote first. The save went
 *    through; the point of the message is that the other version existed.
 *
 * The two buttons are the only two honest answers — take theirs, or keep mine —
 * and neither is the default, because only the person knows which side has the
 * work worth keeping.
 */

import { AlertTriangle, X } from 'lucide-react'
import { useState } from 'react'

import { Button, IconButton } from '@/components/ui'
import { relativeTime } from '@/lib/utils/format'

export interface ConflictBannerProps {
  /** The record as it is on disk, or null when there is no conflict to report. */
  conflict: { name?: string; updatedAt?: number } | null
  /** True when the runner refused the write, so nothing on screen is saved. */
  refused: boolean
  /** What the record is called in this editor: "job" or "pipeline". */
  noun: string
  /** Discard the edits on screen and open what is on disk. */
  onAdopt: () => Promise<void> | void
  /** Keep the edits on screen and write them over what is on disk. */
  onOverwrite: () => Promise<void> | void
  /** Close the message without choosing — only offered when nothing is at stake. */
  onDismiss: () => void
}

export function ConflictBanner({
  conflict,
  refused,
  noun,
  onAdopt,
  onOverwrite,
  onDismiss,
}: ConflictBannerProps) {
  const [busy, setBusy] = useState<'adopt' | 'overwrite' | null>(null)

  if (!conflict) return null

  const run = (which: 'adopt' | 'overwrite', action: () => Promise<void> | void) => () => {
    setBusy(which)
    void Promise.resolve(action()).finally(() => setBusy(null))
  }

  const when = typeof conflict.updatedAt === 'number' ? relativeTime(conflict.updatedAt) : null

  return (
    <div
      role="alert"
      className="flex flex-wrap items-center gap-x-3 gap-y-2 border-b border-state-warning/40 bg-state-warning/10 px-3 py-2 text-xs text-content"
    >
      <AlertTriangle className="h-4 w-4 shrink-0 text-state-warning" aria-hidden />
      <div className="min-w-0 flex-1">
        <p className="font-semibold">
          {refused
            ? `This ${noun} changed on disk, so your edits were not saved.`
            : `This ${noun} was saved somewhere else while you were editing it.`}
        </p>
        <p className="text-content-muted">
          {/* Naming the saved version is the whole point: "somebody saved" is not
              actionable, "saved as Pedidos v2, 3 minutes ago" is. */}
          {conflict.name ? `On disk: “${conflict.name}”` : 'Another copy is on disk'}
          {when ? `, ${when}.` : '.'}{' '}
          {refused
            ? 'Nothing you have on screen is saved yet — choose which version to keep.'
            : 'Your save went through; the other version is still on disk.'}
        </p>
      </div>
      <div className="flex shrink-0 items-center gap-2">
        <Button
          size="xs"
          variant="secondary"
          loading={busy === 'adopt'}
          disabled={busy !== null}
          onClick={run('adopt', onAdopt)}
        >
          Load what is on disk
        </Button>
        <Button
          size="xs"
          variant="danger"
          loading={busy === 'overwrite'}
          disabled={busy !== null}
          onClick={run('overwrite', onOverwrite)}
        >
          Overwrite with mine
        </Button>
        {/* Dismissing a refusal would hide a message about work that is not saved. */}
        {!refused && (
          <IconButton size="sm" label="Dismiss" onClick={onDismiss} disabled={busy !== null}>
            <X />
          </IconButton>
        )}
      </div>
    </div>
  )
}
