/**
 * Storage backed by the runner's workspace — real JSON files on disk.
 *
 * This is the backend the library is meant to live on. The browser holds nothing
 * authoritative: it loads a snapshot on boot and writes every change through to
 * `PUT /workspace/...`, so what a user has is a directory they can diff, review
 * and commit, and a second machine opening the same checkout sees the same
 * library. IndexedDB stays behind it as an offline fallback, not as the store.
 *
 * The same shape is what a hosted deployment needs: swap the base URL for a
 * service address and the client does not change. Everything specific to "the
 * files are local" lives on the server side of this boundary.
 *
 * A Job is written twice by the server — the record here, and its COMPILED
 * pipeline JSON as the reviewable file. That compile happens here, in the
 * client, because the compiler is the client's: the server would otherwise need
 * a second implementation of it to produce the same file.
 */

import { compileGraph } from '@/lib/compiler'
import {
  authHeaders,
  DEFAULT_RUNNER_URL,
  RUNNER_UNREACHABLE_MESSAGE,
  RunnerError,
} from '@/lib/runner/client'
import { addressOf, keyOf, KEY, META_PREFIX, type RecordKind } from '@/lib/storage/keys'
import { toStorable, type StorageBackend } from '@/lib/storage/backend'
import type { Job } from '@/types/studio'

export interface RemoteBackendOptions {
  baseUrl?: string
  token?: string
}

interface WorkspaceDocument {
  kind: string
  id: string
  record: Record<string, unknown>
  path: string | null
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function normalizeBaseUrl(baseUrl: string): string {
  return baseUrl.replace(/\/+$/, '')
}

function documentsOf(value: unknown, field: string): WorkspaceDocument[] {
  if (!isRecord(value) || !Array.isArray(value[field])) return []
  return (value[field] as unknown[]).filter(isDocument)
}

function isDocument(value: unknown): value is WorkspaceDocument {
  return isRecord(value) && typeof value.id === 'string' && isRecord(value.record)
}

/**
 * The key a meta entry is addressed by on the server: the last segment only, so
 * `sparquet-studio:db:meta:seeded` is stored as `seeded` in `.studio/meta.json`.
 */
function metaKeyOf(key: string): string | null {
  return key.startsWith(META_PREFIX) ? key.slice(META_PREFIX.length) : null
}

/**
 * A Job's compiled pipeline, or null when it does not compile yet.
 *
 * Never throws: a half-built job must still save. The server then writes a
 * placeholder in the readable file instead of a config nobody can run.
 */
function compiledConfig(value: unknown): Record<string, unknown> | null {
  const job = value as Job
  if (!isRecord(value) || !isRecord(job.graph) || !isRecord(job.settings)) return null
  try {
    const result = compileGraph(job.graph, job.settings, job.params)
    return (result.pipeline as unknown as Record<string, unknown>) ?? null
  } catch {
    return null
  }
}

export class WorkspaceUnavailableError extends Error {
  constructor(cause?: unknown) {
    super(RUNNER_UNREACHABLE_MESSAGE)
    this.name = 'WorkspaceUnavailableError'
    if (cause !== undefined) this.cause = cause
  }
}

/**
 * Builds the workspace backend, or returns null when the runner is not there.
 *
 * Null is not an error: Studio falls back to browser storage and says so. An
 * error is only raised once the workspace HAS answered and then fails a write —
 * at that point silently keeping the change in the browser would be a lie.
 */
export async function workspaceBackend(
  options: RemoteBackendOptions = {},
): Promise<StorageBackend | null> {
  const baseUrl = normalizeBaseUrl(options.baseUrl ?? DEFAULT_RUNNER_URL)
  const token = options.token ?? ''

  const headers = (): Record<string, string> => {
    return { 'Content-Type': 'application/json', ...authHeaders(token) }
  }

  async function request(path: string, init: RequestInit): Promise<unknown> {
    let response: Response
    try {
      response = await fetch(`${baseUrl}${path}`, { ...init, headers: headers() })
    } catch (error) {
      throw new WorkspaceUnavailableError(error)
    }
    if (!response.ok) {
      let detail = `HTTP ${response.status}`
      try {
        const body = (await response.json()) as unknown
        if (isRecord(body) && typeof body.detail === 'string') detail = body.detail
      } catch {
        /* the status is all there is */
      }
      throw new RunnerError(
        `The workspace refused the write: ${detail}`,
        'http',
        response.status,
      )
    }
    try {
      return (await response.json()) as unknown
    } catch {
      return undefined
    }
  }

  let snapshot: unknown
  try {
    snapshot = await request('/workspace', { method: 'GET' })
  } catch {
    // No runner, an old runner without /workspace, a bad token: all mean the
    // same thing here — this backend cannot be used, try the next one.
    return null
  }
  if (!isRecord(snapshot)) return null

  // The mirror. Reads are served from it so the editor stays synchronous-fast;
  // writes update it only after the server has accepted them, so a failed save
  // never leaves the UI showing a value the files do not have.
  const cache = new Map<string, unknown>()
  const hydrate = (docs: WorkspaceDocument[], kind: RecordKind): void => {
    for (const doc of docs) cache.set(keyOf(kind, doc.id), doc.record)
  }
  hydrate(documentsOf(snapshot, 'workflows'), 'workflow')
  hydrate(documentsOf(snapshot, 'jobs'), 'job')
  hydrate(documentsOf(snapshot, 'pipelines'), 'pipeline')

  const meta = isRecord(snapshot.meta) ? snapshot.meta : {}
  for (const [name, value] of Object.entries(meta)) cache.set(`${META_PREFIX}${name}`, value)

  return {
    kind: 'workspace',

    get: async (key) => cache.get(key),

    set: async (key, value) => {
      const stored = toStorable(value)
      const address = addressOf(key)

      if (address) {
        const record = isRecord(stored) ? stored : {}
        await request(`/workspace/${address.kind}/${encodeURIComponent(address.id)}`, {
          method: 'PUT',
          body: JSON.stringify({
            record,
            config: address.kind === 'job' ? compiledConfig(record) : null,
          }),
        })
        cache.set(key, stored)
        return
      }

      const metaKey = metaKeyOf(key)
      if (metaKey) {
        await request(`/workspace/meta/${encodeURIComponent(metaKey)}`, {
          method: 'PUT',
          body: JSON.stringify({ value: stored ?? null }),
        })
        cache.set(key, stored)
        return
      }

      // Everything else — the migration/import backup — is scratch space for one
      // operation inside one session. Writing it to disk would put a copy of the
      // whole library in the repository for no one to read.
      cache.set(key, stored)
    },

    del: async (key) => {
      const address = addressOf(key)
      if (address) {
        await request(`/workspace/${address.kind}/${encodeURIComponent(address.id)}`, {
          method: 'DELETE',
        })
        cache.delete(key)
        return
      }
      const metaKey = metaKeyOf(key)
      if (metaKey) {
        await request(`/workspace/meta/${encodeURIComponent(metaKey)}`, { method: 'DELETE' })
      }
      cache.delete(key)
    },

    keys: async (prefix) => [...cache.keys()].filter((key) => key.startsWith(prefix)),
  }
}

/** Exported for the one-time push of a browser-held library into an empty workspace. */
export const WORKSPACE_VERSION_KEY = KEY.version
