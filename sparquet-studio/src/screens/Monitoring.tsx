/**
 * Monitoring: whether the library is doing what it is supposed to, and what
 * raises a hand when it stops.
 *
 * Its own screen for the same reason Billing and Access are: it is a service of
 * a shared runner, not a preference of this browser. It is also the one screen
 * somebody opens *without* a Job in mind — the question is "is anything wrong",
 * and every other screen answers it only for whatever is already open.
 *
 * Two sections, two routes. Health is the state of everything; Alerts is the
 * subset somebody asked to be told about. They are the same subject at two
 * levels of insistence, so they share a header and a strip — and an alert is
 * exactly the kind of thing people send each other a link to, so each has an
 * address.
 */

import { Activity, BellRing, CalendarClock } from 'lucide-react'
import { useParams } from 'react-router-dom'

import { PageHeader, PageShell } from '@/components/layout/PageShell'
import { PageTabs, type PageTab } from '@/components/layout/PageTabs'
import { AlertsPanel } from '@/components/monitoring/AlertsPanel'
import { JobHealthPanel } from '@/components/monitoring/JobHealthPanel'
import { SchedulesPanel } from '@/components/scheduling/SchedulesPanel'

const SECTIONS = ['health', 'alerts', 'schedules'] as const

type Section = (typeof SECTIONS)[number]

const TABS: PageTab[] = [
  { to: '/monitoring', end: true, label: 'Job health', icon: Activity },
  { to: '/monitoring/alerts', label: 'Alerts', icon: BellRing },
  { to: '/monitoring/schedules', label: 'Schedules', icon: CalendarClock },
]

const DESCRIPTIONS: Record<Section, string> = {
  health:
    'Every Job in the library and what its own runs say about it — including the ones that have stopped running, which a list ordered by time cannot show.',
  alerts:
    'The rules that raise a hand, what they are saying right now, and when each one started or cleared. A rule with no Job named watches every Job, including ones added later.',
  schedules:
    'What the runner starts on its own, when it fires next, and what it did last. A schedule lives in the record it belongs to, so it is committed with the project and travels through git.',
}

function sectionOf(param: string | undefined): Section {
  const found = SECTIONS.find((name) => name === param)
  return found ?? 'health'
}

export function Monitoring() {
  const { section: param } = useParams<{ section?: string }>()
  const section = sectionOf(param)

  return (
    <PageShell width="default">
      <PageHeader icon={<Activity />} title="Monitoring" description={DESCRIPTIONS[section]} />

      <PageTabs tabs={TABS} ariaLabel="Monitoring sections" />

      <div className="card space-y-5 p-5">
        {section === 'health' ? (
          <JobHealthPanel />
        ) : section === 'alerts' ? (
          <AlertsPanel />
        ) : (
          <SchedulesPanel />
        )}
      </div>
    </PageShell>
  )
}
