/**
 * Access & IAM: who can sign in to this runner, which team they are in, what
 * each role permits, which Jobs and tables those people reach — and the log of
 * what everybody actually did.
 *
 * Its own screen rather than a section of Settings for the same reason Billing
 * is: it governs a shared runner, not this browser's preferences.
 *
 * One screen, five routes. Stacked in a single column these sections answered
 * five unrelated questions in one scroll, and the audit log — six screens of
 * rows — sat under the forms, so every visit to add a user fetched a hundred
 * events nobody had asked for. They are sections of the same subject, so they
 * keep one header and one strip; they are separate *questions*, so each has an
 * address. An address is also what lets somebody send somebody else the log
 * rather than "Access, then scroll".
 */

import { FlaskConical, KeyRound, ScrollText, Shuffle, Users } from 'lucide-react'
import { useParams } from 'react-router-dom'

import { AccessPanel } from '@/components/auth/AccessPanel'
import { AccessSimulator } from '@/components/auth/AccessSimulator'
import { AuditLogPanel } from '@/components/auth/AuditLogPanel'
import { ResourceGrantsPanel } from '@/components/auth/ResourceGrantsPanel'
import { PageHeader, PageShell } from '@/components/layout/PageShell'
import { PageTabs, type PageTab } from '@/components/layout/PageTabs'
import { RolesPanel } from '@/components/auth/RolesPanel'
import { TeamsPanel } from '@/components/auth/TeamsPanel'

/** The sections, in the order the questions are usually asked. */
const SECTIONS = ['people', 'roles', 'rules', 'simulator', 'audit'] as const

type Section = (typeof SECTIONS)[number]

const TABS: PageTab[] = [
  { to: '/access', end: true, label: 'People & teams', icon: Users },
  { to: '/access/roles', label: 'Roles', icon: KeyRound },
  { to: '/access/rules', label: 'Who can run what', icon: Shuffle },
  { to: '/access/simulator', label: 'Simulator', icon: FlaskConical },
  { to: '/access/audit', label: 'Audit log', icon: ScrollText },
]

/** What the screen is for, said for the section actually open. */
const DESCRIPTIONS: Record<Section, string> = {
  people:
    'Who may sign in to this runner, and which team they belong to. A team grants — it never takes away.',
  roles:
    'What a role permits, as statements over actions and resources. Roles answer “may this person run anything at all”.',
  rules:
    'Which Jobs, Pipelines and tables in particular. A rule on a container reaches everything inside it, and an explicit deny closes it again.',
  simulator:
    'What somebody else would be allowed to do right now, answered by the runner itself — roles, rules, and which of the two decides.',
  audit:
    'Every change this runner accepted and every request it refused, newest first. Written by the server and never edited.',
}

function sectionOf(param: string | undefined): Section {
  const found = SECTIONS.find((name) => name === param)
  return found ?? 'people'
}

export function Access() {
  const { section: param } = useParams<{ section?: string }>()
  const section = sectionOf(param)

  return (
    // One width for all five. The log used to ask for `full` because it is a
    // table, and the price was the header, the description and the tab strip
    // jumping sideways on the way in and back on the way out — a section of a
    // screen that moves the screen reads as a different screen. The table fits
    // the column: three of its five cells are fixed-width and the detail row
    // scrolls inside itself.
    <PageShell width="default">
      <PageHeader icon={<Users />} title="Access & IAM" description={DESCRIPTIONS[section]} />

      <PageTabs tabs={TABS} ariaLabel="Access sections" />

      {section === 'people' ? (
        <div className="card space-y-5 p-5">
          <AccessPanel />
          <TeamsPanel />
        </div>
      ) : null}

      {section === 'roles' ? (
        <div className="card space-y-5 p-5">
          <RolesPanel />
        </div>
      ) : null}

      {section === 'rules' ? (
        <div className="card space-y-5 p-5">
          <ResourceGrantsPanel />
        </div>
      ) : null}

      {section === 'simulator' ? (
        <div className="card space-y-5 p-5">
          <AccessSimulator />
        </div>
      ) : null}

      {section === 'audit' ? (
        <div className="card space-y-5 p-5">
          <AuditLogPanel />
        </div>
      ) : null}
    </PageShell>
  )
}
