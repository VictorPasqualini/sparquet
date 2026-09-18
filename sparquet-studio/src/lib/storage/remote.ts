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
  /** Hash of the record's bytes on disk. Absent from an older runner. */
  revision?: string | null
}

/**
 * The library moved under us: another Studio on the same directory, a `git
 * pull`, the file edited by hand. Distinct from `RunnerError` because it is not
 * a failure of the runner and not something to retry — the save was refused on
 * purpose, and the answer is to look at what arrived.
 */
export class WorkspaceConflictError extends Error {
  readonly kind: string
  readonly id: string
  /** The record as it now stands on disk, when the runner sent it. */
  readonly record: Record<string, unknown> | null

  constructor(kind: string, id: string, record: Record<string, unknown> | null, message: string) {
    super(message)
    this.name = 'WorkspaceConflictError'
    this.kind = kind
    this.id = id
    this.record = record
  }
}

export function isWorkspaceConflict(value: unknown): value is WorkspaceConflictError {
  return value instanceof WorkspaceConflictError
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
      let body: unknown
      try {
        body = (await response.json()) as unknown
      } catch {
        /* the status is all there is */
      }
      const payload = isRecord(body) ? body.detail : undefined
      if (typeof payload === 'string') detail = payload
      // 409 is the only status the workspace uses to mean "somebody else wrote
      // here first", and its detail is an object rather than a sentence.
      if (response.status === 409 && isRecord(payload)) {
        throw new WorkspaceConflictError(
          String(payload.kind ?? ''),
          String(payload.id ?? ''),
          isRecord(payload.record) ? payload.record : null,
          typeof payload.message === 'string' ? payload.message : 'The record changed on disk.',
        )
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
  // What each record hashed to when we last saw it. Sent back on the next save so
  // the runner can refuse a write that would land on top of somebody else's — the
  // cache above cannot notice that on its own, because a second machine writing
  // the same directory never touches this process.
  const revisions = new Map<string, string>()
  // Keys whose next write states no revision at all, because somebody looked at
  // the conflict and chose to overwrite. Cleared by that write, so the one after
  // it is guarded again.
  const unguarded = new Set<string>()
  const hydrate = (docs: WorkspaceDocument[], kind: RecordKind): void => {
    for (const doc of docs) {
      cache.set(keyOf(kind, doc.id), doc.record)
      if (typeof doc.revision === 'string') revisions.set(keyOf(kind, doc.id), doc.revision)
    }
  }
  hydrate(documentsOf(snapshot, 'workflows'), 'workflow')
  hydrate(documentsOf(snapshot, 'jobs'), 'job')
  hydrate(documentsOf(snapshot, 'pipelines'), 'pipeline')
  hydrate(documentsOf(snapshot, 'queries'), 'query')

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
        // No revision known means we have never seen this record — `''` says so,
        // and the runner turns it into a conflict if the file is in fact there.
        const answer = await request(
          `/workspace/${address.kind}/${encodeURIComponent(address.id)}`,
          {
            method: 'PUT',
            body: JSON.stringify({
              record,
              config: address.kind === 'job' ? compiledConfig(record) : null,
              ...(unguarded.has(key) ? {} : { revision: revisions.get(key) ?? '' }),
            }),
          },
        )
        unguarded.delete(key)
        cache.set(key, stored)
        // The revision the write returned is the one the next write starts from;
        // without this every second save in a row would conflict with the first.
        if (isRecord(answer) && typeof answer.revision === 'string') {
          revisions.set(key, answer.revision)
        } else {
          revisions.delete(key)
        }
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
        revisions.delete(key)
        return
      }
      const metaKey = metaKeyOf(key)
      if (metaKey) {
        await request(`/workspace/meta/${encodeURIComponent(metaKey)}`, { method: 'DELETE' })
      }
      cache.delete(key)
    },

    keys: async (prefix) => [...cache.keys()].filter((key) => key.startsWith(prefix)),

    overwriteNext: (key) => {
      unguarded.add(key)
    },

    refresh: async (key) => {
      const address = addressOf(key)
      if (!address) return cache.get(key)
      let answer: unknown
      try {
        answer = await request(
          `/workspace/${address.kind}/${encodeURIComponent(address.id)}`,
          { method: 'GET' },
        )
      } catch (error) {
        // A record deleted on the other machine answers 404. That is an answer,
        // not a failure: the cache is wrong and the caller wants to know.
        if (error instanceof RunnerError && error.status === 404) {
          cache.delete(key)
          revisions.delete(key)
          return undefined
        }
        throw error
      }
      if (!isRecord(answer)) return cache.get(key)
      const record = isRecord(answer.record) ? answer.record : undefined
      if (record === undefined) return cache.get(key)
      cache.set(key, record)
      // The revision read together with the record — the pair is the point. A
      // record adopted without its revision would be refused on the next save.
      if (typeof answer.revision === 'string') revisions.set(key, answer.revision)
      else revisions.delete(key)
      return record
    },
  }
}

/** Exported for the one-time push of a browser-held library into an empty workspace. */
export const WORKSPACE_VERSION_KEY = KEY.version
