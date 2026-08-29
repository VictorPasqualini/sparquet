/**
 * The same policy evaluation the runner does, run again in the browser.
 *
 * Not a security boundary — the runner decides, and a client that skipped this
 * would only get a 403 instead. This exists so the UI can grey out the button
 * rather than let someone find out what they are not allowed to do by pressing
 * it. It has to agree with `authorize()` in `server/auth.py`, which is why both
 * follow the same two rules: an explicit deny wins, and the default is no.
 */

import type { PolicyStatement, Principal } from '@/types/auth'

/** IAM-style match: `*` anywhere, and nothing else. */
function matches(pattern: string, value: string): boolean {
  if (pattern === '*') return true
  const star = pattern.indexOf('*')
  if (star === -1) return pattern === value
  const head = pattern.slice(0, star)
  const tail = pattern.slice(star + 1)
  return (
    value.startsWith(head) && value.endsWith(tail) && value.length >= head.length + tail.length
  )
}

function hits(statement: PolicyStatement, action: string, resource: string): boolean {
  const actions = statement.actions ?? []
  const resources = statement.resources ?? ['*']
  return (
    actions.some((pattern) => matches(pattern, action)) &&
    resources.some((pattern) => matches(pattern, resource))
  )
}

export function authorize(
  statements: PolicyStatement[],
  action: string,
  resource = '*',
): boolean {
  let allowed = false
  for (const statement of statements) {
    if (!hits(statement, action, resource)) continue
    if (statement.effect === 'deny') return false
    allowed = true
  }
  return allowed
}

/**
 * Whether this principal may do something.
 *
 * A missing principal is allowed everything on purpose: it means Studio has not
 * asked the runner yet, or the runner has no users at all, and in both cases the
 * UI must not start hiding things the person can in fact do. Where it matters,
 * the runner is still the one that decides.
 */
export function can(principal: Principal | null, action: string, resource = '*'): boolean {
  if (!principal) return true
  return authorize(principal.statements, action, resource)
}
