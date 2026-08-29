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
import { RolesPanel } from '@/components/auth/RolesPanel'
import { TeamsPanel } from '@/components/auth/TeamsPanel'

export function Access() {
  return (
    <div className="mx-auto w-full max-w-5xl px-6 py-8 animate-fade-in">
      <header className="flex items-start gap-3">
        <span className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-line bg-surface-sunken text-content-muted">
          <ShieldCheck className="h-4 w-4" aria-hidden />
        </span>
        <div className="space-y-0.5">
          <h1 className="text-lg font-semibold text-content">Access &amp; IAM</h1>
          <p className="text-xs leading-relaxed text-content-muted">
            Users, teams and roles for this runner, and the audit trail of every
            change it accepted or refused.
          </p>
        </div>
      </header>

      <div className="mt-8 space-y-6">
        <div className="card space-y-5 p-5">
          <AccessPanel />
          <TeamsPanel />
          <RolesPanel />
        </div>
        <div className="card space-y-5 p-5">
          <AuditLogPanel />
        </div>
      </div>
    </div>
  )
}
