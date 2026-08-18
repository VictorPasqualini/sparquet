import { ArrowLeft, Compass } from 'lucide-react'
import { Link, useLocation } from 'react-router-dom'

import { EmptyState } from '@/components/ui'

export function NotFound() {
  const { pathname } = useLocation()

  return (
    <div className="flex h-full items-center justify-center p-8">
      <EmptyState
        icon={<Compass />}
        title="This page does not exist"
        description={
          <>
            <span className="block">
              The link may be out of date, or the job it pointed to was deleted.
            </span>
            <code className="mt-2 inline-block rounded bg-surface-sunken px-1.5 py-0.5 font-mono text-2xs text-content-subtle">
              {pathname}
            </code>
          </>
        }
        action={
          <Link
            to="/"
            className="inline-flex h-8 items-center gap-2 rounded-lg border border-line bg-surface-raised px-3 text-xs font-medium text-content transition-colors hover:border-line-strong hover:bg-surface-sunken"
          >
            <ArrowLeft className="h-3.5 w-3.5" aria-hidden />
            Back to overview
          </Link>
        }
      />
    </div>
  )
}
