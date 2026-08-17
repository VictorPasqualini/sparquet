/**
 * First-run content: a workflow holding the starter templates, so the app is never
 * an empty canvas. Runs once — clearing the library does not bring it back.
 */

import { nanoid } from 'nanoid'

import { TEMPLATES, templateToJob } from '@/data/templates'
import * as db from '@/lib/storage/db'
import type { Workflow, Job, JobTemplate } from '@/types/studio'

export const SEED_WORKFLOW_NAME = 'Getting Started'

const SEED_WORKFLOW_DESCRIPTION =
  'Guided example pipelines. Open one, tweak it, run it — or delete the workflow once you are up to speed.'

/** Templates that make sense as a first read, in the order they should appear. */
function starterTemplates(): JobTemplate[] {
  const starters = TEMPLATES.filter((template) => template.level === 'starter')
  return starters.length > 0 ? starters : TEMPLATES.slice(0, 3)
}

/**
 * Concurrent callers share one run. React StrictMode invokes effects twice in
 * development, and two parallel seeds would each pass the "is it empty?" check
 * and write a duplicate workflow.
 */
let inFlight: Promise<Workflow | null> | null = null

export function seedIfEmpty(): Promise<Workflow | null> {
  if (!inFlight) inFlight = runSeed()
  return inFlight
}

async function runSeed(): Promise<Workflow | null> {
  if (await db.isSeeded()) return null

  const [workflows, jobs] = await Promise.all([db.listWorkflows(), db.listJobs()])
  if (workflows.length > 0 || jobs.length > 0) {
    await db.markSeeded()
    return null
  }

  const now = Date.now()
  const workflow: Workflow = {
    id: nanoid(10),
    name: SEED_WORKFLOW_NAME,
    description: SEED_WORKFLOW_DESCRIPTION,
    accent: 'amber',
    createdAt: now,
    updatedAt: now,
  }
  await db.saveWorkflow(workflow)

  for (const template of starterTemplates()) {
    try {
      const job: Job = templateToJob(template, workflow.id)
      await db.saveJob(job)
    } catch (error) {
      // One broken template must not stop the app from booting.
      console.error(`Sparquet Studio: could not seed template "${template.id}".`, error)
    }
  }

  await db.markSeeded()
  return workflow
}
