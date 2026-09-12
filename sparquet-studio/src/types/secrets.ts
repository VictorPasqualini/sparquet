/**
 * Connection secrets, as the browser is allowed to know them.
 *
 * There is no field here for a value, and there is no endpoint that would fill
 * one in. A secret is created by sending material to the runner and is used by
 * writing `{secret:name/field}` into a form; between those two moments nothing
 * reads it back — not the list, not the check, not an error a driver wrote.
 *
 * That is why these records live only in memory. Unlike the catalog and the
 * grants, they are never mirrored into IndexedDB: a cache of credential metadata
 * on disk in the browser buys a faster screen and costs the one property the
 * feature exists for.
 */

import type { AccessLevel } from '@/lib/iam'

/**
 * Where the material actually is.
 *
 * `local` is encrypted in the runner's workspace and is what a laptop uses.
 * `env` reads a named environment variable, which is how every cloud secret
 * manager ends up reaching a process — the platform injects it, and the runner
 * only has to know which name to look under.
 */
export type SecretProvider = 'local' | 'env'

export interface Secret {
  name: string
  provider: SecretProvider
  description: string
  /** The catalog's own vocabulary: a deny on `tag/pii` closes the credential too. */
  tags: string[]
  /** Field names, and only names — `url`, `user`, `password`. */
  fields: string[]
  /** For `env`, which variable each field reads. Empty for every other provider. */
  binding: Record<string, string>
  updatedAt: number
  updatedBy: string
  /** Layer two on this secret, for the person at the keyboard. */
  governed: boolean
  level: AccessLevel | null
  owned: boolean
}

export interface SecretWrite {
  provider: SecretProvider
  description?: string
  tags?: string[]
  /**
   * A patch over the fields: a value sets it, `null` removes it, and a field
   * left out keeps what it had. Rotating a password means sending that one
   * field — this browser could not resend the others, never having read them.
   */
  values: Record<string, string | null>
}

/** Whether every field still resolves. Not a connection test. */
export interface SecretCheck {
  name: string
  /** Field name to `"ok"`, or to what went wrong reading it. */
  fields: Record<string, string>
  healthy: boolean
}

/** The reference a form writes to use a secret, e.g. `{secret:pg-prod/password}`. */
export function secretRef(name: string, field: string): string {
  return `{secret:${name}/${field}}`
}

/** Every `{secret:name/field}` in a string, in the order they appear. */
export function secretRefsIn(text: string): Array<{ name: string; field: string }> {
  const pattern = /\{secret:([A-Za-z0-9][A-Za-z0-9._-]*)\/([A-Za-z0-9][A-Za-z0-9._-]*)\}/g
  const out: Array<{ name: string; field: string }> = []
  for (const match of text.matchAll(pattern)) out.push({ name: match[1], field: match[2] })
  return out
}
