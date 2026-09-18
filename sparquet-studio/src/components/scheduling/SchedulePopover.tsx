/**
 * The schedule editor for one library record, in a popover.
 *
 * The same control on a Job and on a Pipeline, because a schedule means the same
 * thing on both: the runner starts it without anybody pressing Run. It sits in
 * the editor header next to the tag editor for the same reason that one does —
 * it is a property of the record, not of the canvas, and it is written into the
 * record, so it is committed with the project and travels through git.
 *
 * The expression is checked as it is typed. A schedule saved with an unreadable
 * expression is a Job that silently never runs, and the runner cannot warn about
 * that until the moment it declines to fire.
 *
 * The permissions are shown here but decided by the runner, which refuses the
 * save on its own (`_authorize_schedule_change`). Showing them matters because a
 * schedule is the one place where writing a record *is* starting a run: without
 * this, somebody who may edit a Job but not run it would type a cron expression,
 * save happily, and find out only from a 403 — or, worse, would name somebody
 * else in "run as" and borrow their access by typing a username.
 */

import { CalendarClock } from 'lucide-react'
import { useState } from 'react'

import { Button, Input, Popover, PopoverContent, PopoverTrigger, Select, Toggle } from '@/components/ui'
import { describeCron, LOCAL_ZONE, validateCron } from '@/lib/scheduling/cron'
import { useAuthStore } from '@/store/auth'
import type { ScheduleSpec } from '@/types/studio'

export interface SchedulePopoverProps {
  schedule?: ScheduleSpec
  onChange: (schedule: ScheduleSpec | undefined) => void
  /** What is being scheduled, for the labels a screen reader reads out. */
  subject?: string
  /**
   * `job/j1` — what the run would be authorized against. Left out for a record
   * with no id yet, which falls back to the same `*` the runner falls back to.
   */
  resource?: string
}

/** Starting points, so the common schedules never have to be typed out. */
const PRESETS: readonly { label: string; cron: string }[] = [
  { label: 'Every hour', cron: '0 * * * *' },
  { label: 'Every day at 06:00', cron: '0 6 * * *' },
  { label: 'Weekdays at 07:00', cron: '0 7 * * 1-5' },
  { label: 'Mondays at 08:00', cron: '0 8 * * mon' },
  { label: 'First of the month', cron: '0 0 1 * *' },
]

/**
 * The zones offered, newest-browser first.
 *
 * `Intl.supportedValuesOf` is the whole tz database and is what somebody in
 * Recife actually needs; where it is missing, the two values that always mean
 * something — the runner's own clock, and UTC — are still offered, and any other
 * name can be typed into the record by hand.
 */
function zoneOptions(): string[] {
  const withValues = Intl as typeof Intl & { supportedValuesOf?: (key: string) => string[] }
  const zones = withValues.supportedValuesOf ? withValues.supportedValuesOf('timeZone') : []
  return [LOCAL_ZONE, 'UTC', ...zones.filter((zone) => zone !== 'UTC')]
}

export function SchedulePopover({
  schedule,
  onChange,
  subject = 'this record',
  resource = '*',
}: SchedulePopoverProps) {
  const [cron, setCron] = useState(schedule?.cron ?? '')
  const [zones] = useState(zoneOptions)
  const can = useAuthStore((state) => state.can)
  const me = useAuthStore((state) => state.principal?.username ?? '')
  const mayRun = can('run:Execute', resource)
  const mayActForOthers = can('iam:ManageUsers')

  const timezone = schedule?.timezone ?? LOCAL_ZONE
  const problem = cron.trim().length > 0 ? validateCron(cron) : null
  const enabled = schedule?.enabled !== false
  const runAs = (schedule?.runAs ?? '').trim()
  const borrowsAnother = runAs !== '' && runAs !== me

  const commit = (patch: Partial<ScheduleSpec>) => {
    const next: ScheduleSpec = {
      cron: schedule?.cron ?? '',
      timezone: schedule?.timezone ?? LOCAL_ZONE,
      enabled,
      runAs: schedule?.runAs ?? '',
      ...patch,
    }
    if (next.cron.trim().length === 0 || validateCron(next.cron)) return
    onChange(next)
  }

  const clear = () => {
    setCron('')
    onChange(undefined)
  }

  const label = schedule?.cron
    ? enabled
      ? describeCron(schedule.cron, schedule.timezone)
      : 'Paused'
    : 'Schedule'

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button size="xs" variant="ghost" icon={<CalendarClock />}>
          {label}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[22rem] space-y-3 p-3">
        <div className="space-y-1">
          <p className="text-2xs font-semibold text-content">Schedule</p>
          <p className="text-2xs leading-relaxed text-content-muted">
            The runner starts this on its own. The schedule is saved in the record, so it is
            committed with the project. A run missed while the runner was off is not queued.
          </p>
        </div>

        {!mayRun && (
          <p
            role="status"
            className="rounded-md border border-state-danger/30 bg-state-danger/5 p-2 text-2xs leading-relaxed text-state-danger"
          >
            You are not allowed to run {subject}, so you cannot schedule it either — a
            schedule is a run the clock starts. Ask for run:Execute on this record.
          </p>
        )}

        <Input
          value={cron}
          disabled={!mayRun}
          onChange={(event) => setCron(event.target.value)}
          onBlur={() => commit({ cron: cron.trim() })}
          onKeyDown={(event) => {
            if (event.key === 'Enter') event.currentTarget.blur()
          }}
          aria-label={`Schedule for ${subject}, as five cron fields`}
          placeholder="0 6 * * *"
          className="h-8 font-mono"
        />

        {problem ? (
          <p className="text-2xs leading-relaxed text-state-danger">{problem}</p>
        ) : (
          cron.trim().length > 0 && (
            <p className="text-2xs text-content-muted">{describeCron(cron, timezone)}</p>
          )
        )}

        <ul className="flex flex-wrap gap-1">
          {PRESETS.map((preset) => (
            <li key={preset.cron}>
              <Button
                size="xs"
                variant="ghost"
                disabled={!mayRun}
                onClick={() => {
                  setCron(preset.cron)
                  commit({ cron: preset.cron })
                }}
              >
                {preset.label}
              </Button>
            </li>
          ))}
        </ul>

        <Select
          value={timezone}
          disabled={!mayRun}
          onValueChange={(zone) => commit({ timezone: zone })}
          ariaLabel="Timezone the schedule is read in"
          options={zones.map((zone) => ({
            value: zone,
            label: zone === LOCAL_ZONE ? "The runner's own clock" : zone,
          }))}
          className="h-8"
        />

        <Input
          value={schedule?.runAs ?? ''}
          onChange={(event) => commit({ runAs: event.target.value })}
          aria-label="User the scheduled run belongs to"
          placeholder="Run as — a user name"
          className="h-8"
          disabled={!mayRun}
        />
        <p className="text-2xs leading-relaxed text-content-subtle">
          A scheduled run carries that user's permissions, so it stops firing when their
          access does. Leave it empty only on a runner with no users.
        </p>
        {!mayActForOthers && borrowsAnother && (
          <p className="text-2xs leading-relaxed text-state-danger">
            You cannot schedule a run as {schedule?.runAs}. Naming another account needs
            iam:ManageUsers{me ? `; leave the field empty to run as ${me}` : ''}. The runner
            will refuse this save.
          </p>
        )}

        <div className="flex items-center justify-between gap-2 pt-1">
          <Toggle
            checked={enabled}
            disabled={!mayRun}
            onCheckedChange={(active) => commit({ enabled: active })}
            label="Active"
          />
          {schedule && (
            <Button size="xs" variant="ghost" disabled={!mayRun} onClick={clear}>
              Remove
            </Button>
          )}
        </div>
      </PopoverContent>
    </Popover>
  )
}
