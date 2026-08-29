/**
 * The pipelines of this workflow, ready to become stages.
 *
 * Two ways in, like the node palette: drag one onto the canvas to place it where
 * you dropped it, or press the add button (also the keyboard path) to append it
 * at the end of the row.
 *
 * A job may appear as several stages — running the same file twice with
 * different upstream data is legitimate — so nothing is hidden once used; the
 * count of times it is already staged is shown instead.
 */

import { FileJson, Plus, RotateCcw, Search, Workflow as JobIcon } from 'lucide-react'
import { useMemo, useState, type DragEvent } from 'react'

import { EmptyState, IconButton, Input } from '@/components/ui'
import type { LibraryFile } from '@/lib/runner/libraryFiles'
import { cn } from '@/lib/utils/cn'
import { plural } from '@/lib/utils/format'
import type { Job } from '@/types/studio'

import { STAGE_DND_MIME, STAGE_FILE_DND_MIME } from './PipelineCanvas'

interface StagePickerProps {
  jobs: readonly Job[]
  /** How many stages already reference each job id. */
  usage: Record<string, number>
  onAdd: (jobId: string) => void
  /**
   * Runnable JSON files in the library — including ones the Studio never wrote.
   * `null` while they have not been read (no runner, or not answering).
   */
  files: readonly LibraryFile[] | null
  /** How many stages already reference each library path. */
  fileUsage: Record<string, number>
  onAddFile: (path: string) => void
  onRefreshFiles: () => void
  filesError: string | null
}

export function StagePicker({
  jobs,
  usage,
  onAdd,
  files,
  fileUsage,
  onAddFile,
  onRefreshFiles,
  filesError,
}: StagePickerProps) {
  const [query, setQuery] = useState('')

  const rows = useMemo(() => {
    const needle = query.trim().toLowerCase()
    const matched = needle
      ? jobs.filter(
          (job) =>
            job.name.toLowerCase().includes(needle) ||
            job.description.toLowerCase().includes(needle),
        )
      : jobs
    return [...matched].sort((a, b) => a.name.localeCompare(b.name))
  }, [query, jobs])

  // A file that already backs a Job is offered anyway: the Job is the way to edit
  // it, the file is the way to run it as it stands, and which of the two a stage
  // should point at is the author's call, not this list's.
  const fileRows = useMemo(() => {
    const needle = query.trim().toLowerCase()
    const all = files ?? []
    const matched = needle
      ? all.filter((file) => file.path.toLowerCase().includes(needle))
      : all
    return [...matched].sort((a, b) => a.path.localeCompare(b.path))
  }, [query, files])

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="shrink-0 space-y-2 border-b border-line px-3 py-3">
        <h2 className="text-sm font-semibold text-content">Pipelines</h2>
        <p className="text-2xs leading-relaxed text-content-muted">
          Every stage runs one of these JSON files, top to bottom of the order you draw.
        </p>
        <Input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search pipelines and files"
          aria-label="Search pipelines and files"
          leading={<Search />}
          className="h-8 py-1 text-xs"
        />
      </div>

      {jobs.length === 0 ? (
        <EmptyState
          icon={<JobIcon />}
          title="No pipelines yet"
          description="Create a job in this workflow first — a pipeline only orders files that already exist."
        />
      ) : rows.length === 0 ? (
        <p className="px-3 py-6 text-center text-2xs text-content-subtle">
          Nothing matches “{query.trim()}”.
        </p>
      ) : (
        <ul className="scroll-area min-h-0 flex-1 space-y-1 p-2">
          {rows.map((job) => (
            <li key={job.id}>
              <PickerRow
                job={job}
                staged={usage[job.id] ?? 0}
                onAdd={() => onAdd(job.id)}
              />
            </li>
          ))}
        </ul>
      )}

      <div className="shrink-0 border-t border-line">
        <div className="flex items-center gap-2 px-3 py-2">
          <h3 className="flex-1 text-2xs font-semibold uppercase tracking-wide text-content-subtle">
            Files in the library
          </h3>
          <IconButton size="xs" label="Re-read the library files" onClick={onRefreshFiles}>
            <RotateCcw />
          </IconButton>
        </div>
        <p className="px-3 pb-2 text-2xs leading-relaxed text-content-muted">
          A stage can run a <code>.json</code> directly. The file is the source — it is
          read when the stage runs, not imported, so the Studio does not compile or
          edit it.
        </p>
        {filesError ? (
          <p className="px-3 pb-3 text-2xs text-state-danger">
            {filesError}
          </p>
        ) : files === null ? (
          <p className="px-3 pb-3 text-2xs text-content-subtle">
            Start the local runner to list them.
          </p>
        ) : fileRows.length === 0 ? (
          <p className="px-3 pb-3 text-2xs text-content-subtle">
            {query.trim() ? `Nothing matches “${query.trim()}”.` : 'No JSON files yet.'}
          </p>
        ) : (
          <ul className="scroll-area max-h-52 space-y-1 p-2 pt-0">
            {fileRows.map((file) => (
              <li key={file.path}>
                <FileRow
                  file={file}
                  staged={fileUsage[file.path] ?? 0}
                  onAdd={() => onAddFile(file.path)}
                />
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  )
}

function FileRow({
  file,
  staged,
  onAdd,
}: {
  file: LibraryFile
  staged: number
  onAdd: () => void
}) {
  const onDragStart = (event: DragEvent<HTMLDivElement>) => {
    event.dataTransfer.setData(STAGE_FILE_DND_MIME, file.path)
    event.dataTransfer.effectAllowed = 'copy'
  }

  return (
    <div
      draggable
      onDragStart={onDragStart}
      onClick={onAdd}
      title={`Click to add ${file.path} as a stage, or drag it onto the canvas`}
      className={cn(
        'flex items-center gap-2 rounded-lg border border-line bg-surface px-2 py-1.5',
        'cursor-grab transition-colors hover:border-line-strong hover:bg-surface-raised',
      )}
    >
      <span
        className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md border border-line bg-surface-sunken text-content-subtle"
        aria-hidden
      >
        <FileJson className="h-3.5 w-3.5" />
      </span>
      <span className="min-w-0 flex-1">
        <span className="block truncate text-xs text-content" title={file.path}>
          {file.name}
        </span>
        <span className="block truncate text-2xs text-content-subtle" title={file.path}>
          {staged > 0 ? `${file.path} · already ${plural(staged, 'stage')}` : file.path}
        </span>
      </span>
      <IconButton
        size="xs"
        label={`Add ${file.path} as a stage`}
        onClick={(event) => {
          event.stopPropagation()
          onAdd()
        }}
      >
        <Plus />
      </IconButton>
    </div>
  )
}

function PickerRow({
  job,
  staged,
  onAdd,
}: {
  job: Job
  staged: number
  onAdd: () => void
}) {
  const onDragStart = (event: DragEvent<HTMLDivElement>) => {
    event.dataTransfer.setData(STAGE_DND_MIME, job.id)
    event.dataTransfer.effectAllowed = 'copy'
  }

  return (
    <div
      draggable
      onDragStart={onDragStart}
      // Clicking the row adds the stage, the same way the pipeline palette works:
      // dragging is precise but fiddly, and it was the only documented way in. The
      // `+` button stays as the keyboard-reachable affordance (a focusable control
      // nested in a focusable row would be the worse trade).
      onClick={onAdd}
      title={`Click to add ${job.name} as a stage, or drag it onto the canvas`}
      className={cn(
        'flex items-center gap-2 rounded-lg border border-line bg-surface px-2 py-1.5',
        'cursor-grab transition-colors hover:border-line-strong hover:bg-surface-raised',
      )}
    >
      <span
        className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md border border-line bg-surface-sunken text-content-subtle"
        aria-hidden
      >
        <JobIcon className="h-3.5 w-3.5" />
      </span>
      <span className="min-w-0 flex-1">
        <span className="block truncate text-xs text-content" title={job.name}>
          {job.name}
        </span>
        {staged > 0 && (
          <span className="block text-2xs text-content-subtle">
            already {plural(staged, 'stage')}
          </span>
        )}
      </span>
      <IconButton
        size="xs"
        label={`Add ${job.name} as a stage`}
        // The row already adds on click; without this the button would add twice.
        onClick={(event) => {
          event.stopPropagation()
          onAdd()
        }}
      >
        <Plus />
      </IconButton>
    </div>
  )
}
