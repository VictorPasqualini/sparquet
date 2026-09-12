/**
 * Access & IAM: who can sign in to this runner, which team they are in, what
 * each role permits, and the log of what everybody actually did.
 *
 * Its own screen rather than a section of Settings for the same reason Billing
 * is: it governs a shared runner, not this browser's preferences, and the audit
 * log in particular is something people come looking for on purpose.
 */

import { ShieldCheck } from 'lucide-react'

import { AccessPanel } from '@/components/auth/AccessPanel'
import { AuditLogPanel } from '@/components/auth/AuditLogPanel'
import { ResourceGrantsPanel } from '@/components/auth/ResourceGrantsPanel'
import { PageHeader, PageShell } from '@/components/layout/PageShell'
import { RolesPanel } from '@/components/auth/RolesPanel'
import { TeamsPanel } from '@/components/auth/TeamsPanel'

export function Access() {
  return (
    <PageShell width="default">
      <PageHeader
        icon={<ShieldCheck />}
        title="Access & IAM"
        description="Users, teams and roles for this runner, what each of them may run, and the
          audit trail of every change it accepted or refused."
      />

      <div className="space-y-6">
        <div className="card space-y-5 p-5">
          <AccessPanel />
          <TeamsPanel />
          <RolesPanel />
          <ResourceGrantsPanel />
        </div>
        <div className="card space-y-5 p-5">
          <AuditLogPanel />
        </div>
      </div>
    </PageShell>
  )
}
