/**
 * Inferred pipeline section: the canvas plus the copy that explains what a link is.
 *
 * Splitting it from the canvas keeps the empty states readable and lets the
 * workflow screen lazy-load React Flow only when this view is opened.
 */

import { Share2, Workflow as JobIcon } from 'lucide-react'
import { useMemo } from 'react'

import { EmptyState } from '@/components/ui'
import { deriveInferredPipeline } from '@/lib/pipeline'
import type { Job } from '@/types/studio'

import { InferredPipelineCanvas } from './InferredPipelineCanvas'

/** How a link is detected — the same sentence in every empty state. */
const HOW_LINKS_WORK =
  'Two files are linked when one writes what the other reads: a matching output and input path, or a shared temp view name (a global_temp view matches its bare name).'

/**
 * This view is deliberately read-only: it reports what the files already say about
 * each other. Wiring an order by hand — and running it — is a *pipeline*, so
 * say so here rather than letting someone try to drag these boxes around.
 */
const READ_ONLY_NOTE =
  'This map is read-only — it only reflects paths the files already share. To choose the order yourself and run the files one after another, create a pipeline with “New pipeline”.'

export function InferredPipelineView({ jobs }: { jobs: readonly Job[] }) {
  const pipeline = useMemo(() => deriveInferredPipeline(jobs), [jobs])

  if (jobs.length < 2) {
    return (
      <div className="card">
        <EmptyState
          icon={<Share2 />}
          title="A pipeline needs at least two files"
          description={`A workflow's pipeline shows how its pipeline files chain into each other. ${HOW_LINKS_WORK}`}
        />
      </div>
    )
  }

  return (
    <div className="space-y-3">
      {/* The boxes stay on screen: seeing the files is how you spot the path to fix. */}
      {pipeline.edges.length === 0 && (
        <div className="card">
          <EmptyState
            icon={<JobIcon />}
            title="No links between these files yet"
            description={`${HOW_LINKS_WORK} Until one of those matches, every file is drawn on its own below. ${READ_ONLY_NOTE}`}
          />
        </div>
      )}

      <div className="card h-[70vh] overpipeline-hidden">
        <InferredPipelineCanvas pipeline={pipeline} />
      </div>

      <p className="px-1 text-2xs text-content-subtle">
        {READ_ONLY_NOTE}
      </p>
    </div>
  )
}
