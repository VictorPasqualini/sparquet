/**
 * Where the runner keeps the library, as `GET /workspace/root` describes it.
 *
 * The library is the user's own work — the JSON files the framework runs — so it
 * does not live inside the runner's checkout. A checkout is code: it gets pulled,
 * reset and deleted, and anything the product wrote into it is lost to the first
 * `git clean`, or committed by accident long before that. The default is the
 * platform's per-user data directory, and this is how the interface reads that
 * back and points it somewhere else.
 */

/** Why the root is the one it is. Ordered by precedence, strongest first. */
export type WorkspaceRootSource =
  /** `SPARQUET_STUDIO_WORKSPACE`: the deployment decided, nothing overrides it. */
  | 'env'
  /** Somebody chose it in the interface, and it was remembered. */
  | 'settings'
  /** An older directory that already held a library, adopted rather than abandoned. */
  | 'legacy'
  /** Nobody chose: the per-user data directory. */
  | 'default'

export interface WorkspaceLocation {
  /** Absolute path the runner reads and writes. */
  root: string
  source: WorkspaceRootSource
  /** The per-user directory the root falls back to when nothing is chosen. */
  default: string
  /** Where the choice is stored — outside the workspace, since it points at it. */
  settingsFile: string
  /** False when the runner cannot write there; the library is unusable until fixed. */
  writable: boolean
  /** True while the library still sits inside a checkout — the thing to move. */
  insideSourceTree: boolean
  /** True when the environment fixed it and the interface may not change it. */
  locked: boolean
}
