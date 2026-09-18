/**
 * A folder tree over the flat list of files the runner reports.
 *
 * The runner lists the library as paths — `vendas/jobs/ingestao.json` — and says
 * nothing about folders, because on disk there is nothing to say: a folder is
 * whatever the paths have in common. That is fine for a stage picker with five
 * entries and useless for the case this exists to serve, which is somebody who
 * has been running Sparquet from the CLI for two years and points the Studio at a
 * directory with two hundred confs in it. Importing them one at a time is not an
 * adoption path; browsing them is.
 *
 * So the tree is derived, never stored. Nothing here copies a file, writes a
 * sidecar or mints a record: the file on disk stays the truth and this is only
 * the reading of it. Every path in and out is relative to the library root with
 * forward slashes — the shape the runner speaks, on every platform.
 */

import type { LibraryFile } from '@/lib/runner/libraryFiles'

/** A folder, which exists only because files underneath it do. */
export interface LibraryFolder {
  /** Relative to the library root, no trailing slash. `''` is the root itself. */
  path: string
  /** Last segment — what to show in a list. */
  name: string
  /** Every file at or below it, so a folder can say how much is in it. */
  fileCount: number
}

/** What one folder holds: the folders directly inside it, and its own files. */
export interface LibraryLevel {
  folders: LibraryFolder[]
  files: LibraryFile[]
}

/** One step of the path back to the root, root first. */
export interface LibraryCrumb {
  path: string
  name: string
}

/** Everything above `path`, deepest last. `''` is the library root. */
export function parentOf(path: string): string {
  const clean = normalizeFolder(path)
  const at = clean.lastIndexOf('/')
  return at === -1 ? '' : clean.slice(0, at)
}

/**
 * Strips what a folder path must not carry: leading and trailing slashes, and
 * the doubled ones a joined path picks up. `''` for anything that means "root".
 */
export function normalizeFolder(path: string): string {
  return path
    .split('/')
    .filter((part) => part !== '' && part !== '.')
    .join('/')
}

/** The folder a file lives in. */
export function folderOf(file: Pick<LibraryFile, 'path'>): string {
  return parentOf(file.path)
}

/**
 * The trail from the root down to `folder`, for a breadcrumb.
 *
 * The root is always the first crumb, named for what it is rather than for the
 * directory it happens to be: the absolute path is shown once, in the header,
 * and repeating it in every breadcrumb only makes the trail unreadable.
 */
export function crumbsOf(folder: string, rootLabel = 'Library'): LibraryCrumb[] {
  const crumbs: LibraryCrumb[] = [{ path: '', name: rootLabel }]
  let walked = ''
  for (const part of normalizeFolder(folder).split('/').filter(Boolean)) {
    walked = walked ? `${walked}/${part}` : part
    crumbs.push({ path: walked, name: part })
  }
  return crumbs
}

/** True when `path` is `folder` itself or somewhere under it. */
export function isUnder(path: string, folder: string): boolean {
  const root = normalizeFolder(folder)
  if (!root) return true
  return path === root || path.startsWith(`${root}/`)
}

function compareByName(a: { name: string }, b: { name: string }): number {
  // Locale-aware, and numeric so `stage2` sorts before `stage10` — a library
  // written outside the Studio is full of numbered files.
  return a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: 'base' })
}

/**
 * What to show for one folder: the folders directly inside it and the files
 * directly in it, each sorted by name.
 *
 * A folder that only holds other folders still appears, with the count of
 * everything underneath — a library organised as `domain/layer/job.json` is all
 * empty folders at the top level, and a browser that showed nothing there would
 * be a browser nobody could start from.
 */
export function levelOf(files: LibraryFile[], folder = ''): LibraryLevel {
  const here = normalizeFolder(folder)
  const prefix = here ? `${here}/` : ''
  const counts = new Map<string, number>()
  const own: LibraryFile[] = []

  for (const file of files) {
    if (!isUnder(file.path, here)) continue
    const rest = file.path.slice(prefix.length)
    const cut = rest.indexOf('/')
    if (cut === -1) {
      own.push(file)
      continue
    }
    const child = rest.slice(0, cut)
    counts.set(child, (counts.get(child) ?? 0) + 1)
  }

  const folders = [...counts.entries()].map(([name, fileCount]) => ({
    path: here ? `${here}/${name}` : name,
    name,
    fileCount,
  }))
  folders.sort(compareByName)
  own.sort(compareByName)
  return { folders, files: own }
}

/**
 * Files whose path or name matches every word typed, ignoring case.
 *
 * Every word rather than the whole string, because the way somebody finds a file
 * in two hundred is by remembering two pieces of it — "vendas gold" — not the
 * exact order they appear in the path. A search reaches the whole library, not
 * the folder being browsed: looking for something you cannot find is exactly the
 * moment when the folder you are in is the wrong one.
 */
export function searchFiles(files: LibraryFile[], query: string): LibraryFile[] {
  const words = query.toLowerCase().split(/\s+/).filter(Boolean)
  if (!words.length) return []
  const matched = files.filter((file) => {
    const haystack = `${file.path} ${file.name}`.toLowerCase()
    return words.every((word) => haystack.includes(word))
  })
  return [...matched].sort((a, b) => a.path.localeCompare(b.path, undefined, { numeric: true }))
}
