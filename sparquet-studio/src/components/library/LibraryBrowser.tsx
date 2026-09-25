/**
 * The library as a directory: browse the folder, read what is in it, run it.
 *
 * The Studio's own library is a set of records it wrote, and everything else in
 * it — a Pipeline stage pointing at a path — reached one file at a time. That is
 * fine for somebody who started here and useless for the person this exists for:
 * two years of Sparquet run from the CLI, a repository with two hundred confs in
 * it, and no appetite for importing them one by one to find out whether the
 * Studio is worth adopting. Point the runner at that directory and this is the
 * reading of it.
 *
 * It is a tab rather than a screen of its own because every answer it gives leads
 * back into the rest of the Studio: a file with a Job behind it opens that Job's
 * canvas, and a file with nothing behind it is copied onto a canvas. Inside a
 * Workflow the copy lands in that workflow; on the overview, where no workflow is
 * the current one, it lands in the most recently touched one and the dialog says
 * so — an offer with a named destination, not one that asks a question first.
 *
 * What is on disk stays the truth: a file is re-read every time it is opened and
 * again when it runs, so an edit made in an editor, by a generator or by a `git
 * pull` takes effect without anybody remembering to re-import. Two things here
 * write, and both say so first: deleting removes the file from disk, and opening
 * an unowned file on a canvas copies it into a new Job — the original stays where
 * it is, and from then on there are two.
 *
 * A run started here is filed under the file itself (`file:<path>` in the
 * catalog), so it shows up in per-Job health, can be alerted on and can be
 * tagged for billing, exactly like a Job the Studio owns.
 */

import {
  ChevronRight,
  FileJson,
  Folder,
  Pencil,
  Play,
  RefreshCw,
  Search,
  SquarePen,
  Trash2,
  X,
} from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { toast } from 'sonner'

import {
  Badge,
  Button,
  EmptyState,
  ErrorCard,
  IconButton,
  Input,
  Spinner,
  useConfirm,
} from '@/components/ui'
import { usePermission, usePermissionReason } from '@/lib/auth/usePermission'
import { pipelineToGraph } from '@/lib/compiler'
import { crumbsOf, levelOf, searchFiles } from '@/lib/library/tree'
import { runPipelineStream } from '@/lib/runner/client'
import {
  deleteLibraryFile,
  listLibraryFiles,
  readLibraryFile,
  type LibraryFile,
} from '@/lib/runner/libraryFiles'
import { lintJob } from '@/lib/validation/lint'
import { copyText } from '@/lib/utils/download'
import { relativeTime } from '@/lib/utils/format'
import { useLibraryStore } from '@/store/library'
import { useSettingsStore } from '@/store/settings'
import type { PipelineSpec } from '@/types/pipeline'
import type { PipelineRunResult } from '@/types/studio'

/** Bytes, in the unit somebody reading a file list actually wants. */
function humanSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

/** Where a record the Studio owns is edited. */
function recordRoute(kind: string, id: string): string | null {
  if (kind === 'job') return `/jobs/${id}`
  if (kind === 'pipeline') return `/pipelines/${id}`
  if (kind === 'workflow') return `/workflows/${id}`
  return null
}

/* ------------------------------------------------------------------ listing */

interface Listing {
  root: string
  files: LibraryFile[]
  loading: boolean
  /** Set only when the runner answered and refused; an absent runner is its own state. */
  error: string | null
  /** True when there is no runner to ask — the library is a server-side directory. */
  unreachable: boolean
  refresh: () => void
}

function useListing(): Listing {
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const [state, setState] = useState<Omit<Listing, 'refresh'>>({
    root: '',
    files: [],
    loading: true,
    error: null,
    unreachable: false,
  })
  const [nonce, setNonce] = useState(0)

  useEffect(() => {
    const controller = new AbortController()
    setState((previous) => ({ ...previous, loading: true }))
    void (async () => {
      try {
        const listing = await listLibraryFiles(runnerUrl, runnerToken, controller.signal)
        if (controller.signal.aborted) return
        setState({
          root: listing.root,
          files: listing.files,
          loading: false,
          error: null,
          unreachable: false,
        })
      } catch (caught) {
        if (controller.signal.aborted) return
        // A runner that is not running is the normal case for somebody opening
        // the Studio in a browser, not a failure worth shouting about.
        const unreachable = caught instanceof Error && caught.name === 'TypeError'
        setState({
          root: '',
          files: [],
          loading: false,
          error: unreachable ? null : caught instanceof Error ? caught.message : String(caught),
          unreachable,
        })
      }
    })()
    return () => controller.abort()
  }, [runnerUrl, runnerToken, nonce])

  return { ...state, refresh: () => setNonce((value) => value + 1) }
}

/* ------------------------------------------------------------------- detail */

interface Preview {
  loading: boolean
  spec: PipelineSpec | null
  error: string | null
  /** What opening this file in the editor would complain about, before running it. */
  issues: { id: string; severity: string; message: string }[]
}

/**
 * Reads the selected file and lints it through the same compiler the editor uses.
 *
 * Linting before running is the point of showing anything at all here: a conf
 * written for an older build, or by hand, says nothing about itself until it
 * fails on a cluster. The unknown keys the decompiler preserves surface here too,
 * so "the Studio does not understand this key" is visible before the run rather
 * than after it.
 */
function usePreview(path: string | null): Preview {
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const [state, setState] = useState<Preview>({
    loading: false,
    spec: null,
    error: null,
    issues: [],
  })

  useEffect(() => {
    if (!path) {
      setState({ loading: false, spec: null, error: null, issues: [] })
      return
    }
    const controller = new AbortController()
    setState({ loading: true, spec: null, error: null, issues: [] })
    void (async () => {
      try {
        const spec = await readLibraryFile(runnerUrl, path, runnerToken, controller.signal)
        if (controller.signal.aborted) return
        const decompiled = pipelineToGraph(spec)
        const issues = [
          ...decompiled.issues,
          ...lintJob(decompiled.graph, decompiled.settings, []),
        ]
        setState({
          loading: false,
          spec,
          error: null,
          issues: issues.map((issue) => ({
            id: issue.id,
            severity: issue.severity,
            message: issue.message,
          })),
        })
      } catch (caught) {
        if (controller.signal.aborted) return
        setState({
          loading: false,
          spec: null,
          error: caught instanceof Error ? caught.message : String(caught),
          issues: [],
        })
      }
    })()
    return () => controller.abort()
  }, [path, runnerUrl, runnerToken])

  return state
}

/* --------------------------------------------------------------------- run */

interface RunState {
  running: boolean
  result: PipelineRunResult | null
  error: string | null
}

function FileRunner({ path }: { path: string }) {
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const denied = usePermissionReason('run:Execute')
  const [state, setState] = useState<RunState>({ running: false, result: null, error: null })
  const abort = useRef<AbortController | null>(null)

  // A run started here outlives the selection: switching files mid-run would
  // otherwise leave the runner executing something nobody is watching.
  useEffect(() => {
    setState({ running: false, result: null, error: null })
    return () => abort.current?.abort()
  }, [path])

  const start = useCallback(() => {
    const controller = new AbortController()
    abort.current = controller
    setState({ running: true, result: null, error: null })
    void runPipelineStream(
      runnerUrl,
      {
        // One stage, named after the file, pointed at the file: the runner reads
        // it as the stage starts and files the run under `file:<path>`.
        stages: [{ id: 'file', name: path.split('/').pop() ?? path, path }],
        name: path,
        launched: 'manual',
      },
      {
        onResult: (result) => setState({ running: false, result, error: null }),
        onError: (message) => setState({ running: false, result: null, error: message }),
      },
      controller.signal,
      runnerToken,
    ).catch((caught: unknown) => {
      if (controller.signal.aborted) return
      setState({
        running: false,
        result: null,
        error: caught instanceof Error ? caught.message : String(caught),
      })
    })
  }, [path, runnerUrl, runnerToken])

  const stage = state.result?.stages[0]

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <Button
          size="xs"
          variant="primary"
          loading={state.running}
          disabled={state.running || denied !== null}
          title={denied ?? undefined}
          onClick={start}
        >
          <Play />
          Run this file
        </Button>
        {state.running ? (
          <Button size="xs" variant="ghost" onClick={() => abort.current?.abort()}>
            Stop
          </Button>
        ) : null}
      </div>
      {state.error ? <ErrorCard tone="danger" message={state.error} /> : null}
      {state.result ? (
        <div className="rounded-lg border border-line bg-surface-sunken px-3 py-2 text-xs">
          <div className="flex flex-wrap items-center gap-2">
            <Badge tone={state.result.status === 'success' ? 'success' : 'danger'}>
              {state.result.status}
            </Badge>
            {typeof state.result.durationMs === 'number' ? (
              <span className="text-content-muted">{state.result.durationMs} ms</span>
            ) : null}
            {stage?.rowsRead !== undefined ? (
              <span className="text-content-muted">{stage.rowsRead} rows read</span>
            ) : null}
            {stage?.rowsWritten !== undefined ? (
              <span className="text-content-muted">{stage.rowsWritten} rows written</span>
            ) : null}
          </div>
          {state.result.error ? (
            <p className="mt-1 whitespace-pre-wrap text-state-danger">{state.result.error}</p>
          ) : null}
        </div>
      ) : null}
    </div>
  )
}

/* ------------------------------------------------------------------ detail */

interface FileDetailProps {
  file: LibraryFile
  /**
   * Where an unowned file is copied to when it is opened on a canvas.
   *
   * Absent when the library is browsed from the overview: there the destination
   * falls back to the workflow touched most recently, which the dialog names.
   */
  workflowId?: string
  /** Called once the file is gone from disk, so the listing stops showing it. */
  onDeleted: () => void
}

function FileDetail({ file, workflowId, onDeleted }: FileDetailProps) {
  const navigate = useNavigate()
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const createJob = useLibraryStore((state) => state.createJob)
  const workflows = useLibraryStore((state) => state.workflows)
  const mayWrite = usePermission('workspace:Write')
  const deniedDelete = usePermissionReason('workspace:Delete')
  const [confirm, confirmDialog] = useConfirm()
  const [busy, setBusy] = useState(false)

  const preview = usePreview(file.path)
  const declared =
    preview.spec && typeof (preview.spec as { name?: unknown }).name === 'string'
      ? ((preview.spec as { name?: string }).name as string)
      : null

  // A file the Studio wrote already has a canvas; any other one would need a copy
  // first, and those are different offers with different consequences.
  const owned = file.ownerKind && file.ownerId ? recordRoute(file.ownerKind, file.ownerId) : null

  // A copy has to land in a workflow. Browsing one settles it; browsing the
  // overview does not, and the least surprising answer there is the workflow the
  // person was last working in — said out loud in the dialog, never guessed at
  // silently.
  const destination = useMemo(() => {
    if (workflowId) return workflows.find((item) => item.id === workflowId) ?? null
    return [...workflows].sort((a, b) => b.updatedAt - a.updatedAt)[0] ?? null
  }, [workflowId, workflows])

  const handleOpenCanvas = async () => {
    if (owned) {
      navigate(owned)
      return
    }
    if (!preview.spec || busy || !destination) return
    const confirmed = await confirm({
      title: 'Open on a canvas',
      message: (
        <>
          A copy of <span className="font-medium text-content">{file.path}</span> becomes a
          Job in <span className="font-medium text-content">{destination.name}</span>, and
          the canvas edits the copy. The file on disk is left exactly as it is — from here
          on the two are separate.
        </>
      ),
      confirmLabel: 'Copy onto a canvas',
    })
    if (!confirmed) return
    setBusy(true)
    try {
      const job = await createJob({
        workflowId: destination.id,
        name: declared ?? file.name,
        description: `Copied from ${file.path}`,
        pipeline: preview.spec,
      })
      navigate(`/jobs/${job.id}`)
    } catch (caught) {
      setBusy(false)
      toast.error('Could not open it on a canvas', {
        description: caught instanceof Error ? caught.message : String(caught),
      })
    }
  }

  const handleDelete = async () => {
    const confirmed = await confirm({
      title: 'Delete this file',
      message: (
        <>
          <span className="font-mono text-content">{file.path}</span> is removed from the
          library directory on disk. Nothing is kept and this cannot be undone. Anything
          that runs it by path — a Pipeline stage, a schedule — starts failing.
        </>
      ),
      confirmLabel: 'Delete from disk',
      confirmName: file.name,
    })
    if (!confirmed) return
    setBusy(true)
    try {
      await deleteLibraryFile(runnerUrl, file.path, runnerToken)
      toast.success('File deleted')
      onDeleted()
    } catch (caught) {
      toast.error('Could not delete the file', {
        description: caught instanceof Error ? caught.message : String(caught),
      })
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="space-y-4">
      <div>
        <h2 className="truncate text-sm font-semibold text-content" title={file.path}>
          {declared ?? file.name}
        </h2>
        <p className="mt-0.5 break-all font-mono text-[11px] text-content-muted">{file.path}</p>
        <p className="mt-1 text-xs text-content-muted">
          {humanSize(file.size)} · edited {relativeTime(file.modified * 1000)}
          {file.ownerKind ? ` · written by this Studio as a ${file.ownerKind}` : ''}
        </p>
      </div>

      <FileRunner path={file.path} />

      <div className="flex flex-wrap items-center gap-2">
        <Button
          size="xs"
          variant="secondary"
          icon={owned ? <Pencil className="h-3.5 w-3.5" /> : <SquarePen className="h-3.5 w-3.5" />}
          loading={busy}
          disabled={busy || !mayWrite || (!owned && (!preview.spec || !destination))}
          title={
            !mayWrite
              ? 'Your role does not allow workspace:Write'
              : owned
                ? 'Edit the record this file belongs to'
                : destination
                  ? `Copy it into a Job in ${destination.name} and edit the copy`
                  : 'Create a workflow first — the copy has to live in one.'
          }
          onClick={() => void handleOpenCanvas()}
        >
          {owned ? 'Open canvas' : 'Open on a canvas'}
        </Button>
        <Button
          size="xs"
          variant="secondary"
          onClick={() => {
            void copyText(file.path)
            toast.success('Path copied. Paste it into a Pipeline stage.')
          }}
        >
          Copy path
        </Button>
        <IconButton
          size="sm"
          label="Delete file"
          className="ml-auto hover:text-state-danger"
          disabled={busy || deniedDelete !== null || Boolean(file.ownerKind)}
          title={
            file.ownerKind
              ? `This file is the ${file.ownerKind} itself — delete the ${file.ownerKind} to remove both halves.`
              : (deniedDelete ?? 'Delete this file from disk')
          }
          onClick={() => void handleDelete()}
        >
          <Trash2 />
        </IconButton>
      </div>

      {preview.loading ? (
        <div className="flex items-center gap-2 text-xs text-content-muted">
          <Spinner /> Reading the file…
        </div>
      ) : null}

      {preview.error ? <ErrorCard tone="danger" message={preview.error} /> : null}

      {preview.issues.length ? (
        <section className="space-y-1">
          {/* Shown before the run rather than after it: a conf written for an
              older build says nothing about itself until it fails on a cluster. */}
          <h3 className="text-xs font-semibold text-content">What the Studio reads here</h3>
          <ul className="space-y-1">
            {preview.issues.map((issue) => (
              <li key={issue.id} className="flex items-start gap-2 text-xs">
                <Badge tone={issue.severity === 'error' ? 'danger' : 'warning'}>
                  {issue.severity}
                </Badge>
                <span className="min-w-0 flex-1 text-content-muted">{issue.message}</span>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {preview.spec ? (
        <section className="space-y-1">
          <h3 className="text-xs font-semibold text-content">On disk</h3>
          <pre className="max-h-96 overflow-auto rounded-lg border border-line bg-surface-sunken p-3 font-mono text-[11px] leading-relaxed text-content-muted">
            {JSON.stringify(preview.spec, null, 2)}
          </pre>
        </section>
      ) : null}

      {confirmDialog}
    </div>
  )
}

/* ----------------------------------------------------------------- browser */

export interface LibraryBrowserProps {
  /** Folder being browsed, relative to the library root. `''` is the root. */
  folder: string
  /** Walk to another folder — the caller owns the URL this is reflected in. */
  onNavigate: (folder: string) => void
  /**
   * Workflow an unowned file is copied into when it is opened on a canvas.
   *
   * Optional: the overview browses the same library without being inside one.
   */
  workflowId?: string
}

export function LibraryBrowser({ folder, onNavigate, workflowId }: LibraryBrowserProps) {
  const listing = useListing()
  const [query, setQuery] = useState('')
  const [selected, setSelected] = useState<string | null>(null)

  const level = useMemo(() => levelOf(listing.files, folder), [listing.files, folder])
  const results = useMemo(() => searchFiles(listing.files, query), [listing.files, query])
  const searching = query.trim().length > 0
  const shown = searching ? results : level.files
  const selectedFile = useMemo(
    () => listing.files.find((file) => file.path === selected) ?? null,
    [listing.files, selected],
  )

  const goTo = (target: string) => {
    setSelected(null)
    onNavigate(target)
  }

  if (listing.unreachable) {
    return (
      <EmptyState
        icon={<FileJson />}
        title="No runner to read the directory"
        description="The library is a directory on the machine the runner runs on, so this needs one. Start the local runner, or point the Studio at one in Settings."
      />
    )
  }

  return (
    <div className="space-y-3">
      {listing.error ? <ErrorCard tone="danger" message={listing.error} /> : null}

      <p className="text-2xs text-content-muted">
        {listing.root
          ? `Every runnable JSON under ${listing.root}. Nothing is imported — the file on disk is what runs, and it is re-read every time.`
          : 'Every runnable JSON in the library directory. Nothing is imported — the file on disk is what runs.'}
      </p>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,26rem)]">
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-2">
            <nav className="flex min-w-0 flex-1 flex-wrap items-center gap-1 text-xs">
              {crumbsOf(folder).map((crumb, index, all) => (
                <span key={crumb.path} className="flex items-center gap-1">
                  {index > 0 ? (
                    <ChevronRight className="h-3 w-3 text-content-muted" aria-hidden />
                  ) : null}
                  {index === all.length - 1 ? (
                    <span className="font-medium text-content">{crumb.name}</span>
                  ) : (
                    <button
                      type="button"
                      className="text-content-muted hover:text-content hover:underline"
                      onClick={() => goTo(crumb.path)}
                    >
                      {crumb.name}
                    </button>
                  )}
                </span>
              ))}
            </nav>
            <div className="relative w-full sm:w-64">
              <Search
                className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-content-muted"
                aria-hidden
              />
              <Input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Search the whole library…"
                className="pl-7"
              />
              {searching ? (
                <button
                  type="button"
                  aria-label="Clear search"
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-content-muted hover:text-content"
                  onClick={() => setQuery('')}
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              ) : null}
            </div>
            <IconButton size="sm" label="Refresh" onClick={listing.refresh}>
              <RefreshCw />
            </IconButton>
          </div>

          {listing.loading ? (
            <div className="flex items-center gap-2 text-xs text-content-muted">
              <Spinner /> Reading the directory…
            </div>
          ) : null}

          <ul className="divide-y divide-line/60 rounded-lg border border-line">
            {!searching &&
              level.folders.map((entry) => (
                <li key={entry.path}>
                  <button
                    type="button"
                    className="flex w-full items-center gap-2 px-3 py-2 text-left text-xs hover:bg-surface-sunken"
                    onClick={() => goTo(entry.path)}
                  >
                    <Folder className="h-4 w-4 shrink-0 text-content-muted" aria-hidden />
                    <span className="min-w-0 flex-1 truncate font-medium text-content">
                      {entry.name}
                    </span>
                    <span className="shrink-0 text-content-muted">
                      {entry.fileCount} {entry.fileCount === 1 ? 'file' : 'files'}
                    </span>
                  </button>
                </li>
              ))}
            {shown.map((file) => (
              <li key={file.path}>
                <button
                  type="button"
                  aria-current={selected === file.path}
                  className={`flex w-full items-center gap-2 px-3 py-2 text-left text-xs hover:bg-surface-sunken ${
                    selected === file.path ? 'bg-surface-sunken' : ''
                  }`}
                  onClick={() => setSelected(file.path)}
                >
                  <FileJson className="h-4 w-4 shrink-0 text-content-muted" aria-hidden />
                  <span className="min-w-0 flex-1 truncate text-content">
                    {/* In a search the path is the answer; in a folder it is noise. */}
                    {searching ? file.path : file.name}
                  </span>
                  <span className="shrink-0 text-content-muted">{humanSize(file.size)}</span>
                </button>
              </li>
            ))}
            {!listing.loading && !level.folders.length && !shown.length ? (
              <li className="px-3 py-6">
                <EmptyState
                  icon={<FileJson />}
                  title={searching ? 'Nothing matches' : 'Nothing runnable here'}
                  description={
                    searching
                      ? 'Every word has to appear somewhere in the path.'
                      : 'This folder holds no .json the runner would run. Hidden files and the editor’s own state are never listed.'
                  }
                />
              </li>
            ) : null}
          </ul>
        </div>

        <aside className="rounded-lg border border-line p-4">
          {selectedFile ? (
            <FileDetail
              key={selectedFile.path}
              file={selectedFile}
              workflowId={workflowId}
              onDeleted={() => {
                setSelected(null)
                listing.refresh()
              }}
            />
          ) : (
            <EmptyState
              icon={<FileJson />}
              title="Pick a file"
              description="What it holds, what the Studio makes of it, and a way to run it as it stands."
            />
          )}
        </aside>
      </div>
    </div>
  )
}
