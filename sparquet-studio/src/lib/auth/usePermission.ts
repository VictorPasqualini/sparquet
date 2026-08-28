/**
 * Permission checks that re-render when the answer changes.
 *
 * `useAuthStore(state => state.can)` looks like it would do this and does not:
 * the selector returns the same function on every render, so a component that
 * subscribes to it is never told that the principal underneath changed. Somebody
 * whose role is edited while Studio is open would keep seeing the old buttons
 * until they navigated. These hooks subscribe to the principal itself.
 *
 * None of this is a security boundary. The runner evaluates the same policy on
 * every request and answers 403 regardless of what this returned, so a hidden
 * control is a courtesy — it saves the person from finding out by pressing it.
 */

import { useMemo } from 'react'

import { can } from '@/lib/auth/permissions'
import { useAuthStore } from '@/store/auth'

/** Whether the current principal may do `action`, optionally on one resource. */
export function usePermission(action: string, resource = '*'): boolean {
  const principal = useAuthStore((state) => state.principal)
  return useMemo(() => can(principal, action, resource), [principal, action, resource])
}

/**
 * The same answer with a reason attached: null when allowed, and otherwise the
 * sentence to put in the tooltip of the control being disabled.
 *
 * Written as a reason rather than a boolean because a greyed-out button with no
 * explanation is the worst of both worlds — the person cannot act and cannot
 * find out why.
 */
export function usePermissionReason(action: string, resource = '*'): string | null {
  const allowed = usePermission(action, resource)
  return allowed ? null : `Your role does not allow ${action}`
}
