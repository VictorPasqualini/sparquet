/**
 * First-run content: a project holding the starter templates, so the app is never
 * an empty canvas. Runs once — clearing the library does not bring it back.
 */

import { nanoid } from 'nanoid'

import { TEMPLATES, templateToWorkflow } from '@/data/templates'
import * as db from '@/lib/storage/db'
import type { Project, Workflow, WorkflowTemplate } from '@/types/studio'

export const SEED_PROJECT_NAME = 'Getting Started'

const SEED_PROJECT_DESCRIPTION =
  'Guided example pipelines. Open one, tweak it, run it — or delete the project once you are up to speed.'

/** Templates that make sense as a first read, in the order they should appear. */
function starterTemplates(): WorkflowTemplate[] {
  const starters = TEMPLATES.filter((template) => template.level === 'starter')
  return starters.length > 0 ? starters : TEMPLATES.slice(0, 3)
}

/**
 * Concurrent callers share one run. React StrictMode invokes effects twice in
 * development, and two parallel seeds would each pass the "is it empty?" check
 * and write a duplicate project.
 */
let inFlight: Promise<Project | null> | null = null

export function seedIfEmpty(): Promise<Project | null> {
  if (!inFlight) inFlight = runSeed()
  return inFlight
}

async function runSeed(): Promise<Project | null> {
  if (await db.isSeeded()) return null

  const [projects, workflows] = await Promise.all([db.listProjects(), db.listWorkflows()])
  if (projects.length > 0 || workflows.length > 0) {
    await db.markSeeded()
    return null
  }

  const now = Date.now()
  const project: Project = {
    id: nanoid(10),
    name: SEED_PROJECT_NAME,
    description: SEED_PROJECT_DESCRIPTION,
    accent: 'amber',
    createdAt: now,
    updatedAt: now,
  }
  await db.saveProject(project)

  for (const template of starterTemplates()) {
    try {
      const workflow: Workflow = templateToWorkflow(template, project.id)
      await db.saveWorkflow(workflow)
    } catch (error) {
      // One broken template must not stop the app from booting.
      console.error(`Sparquet Studio: could not seed template "${template.id}".`, error)
    }
  }

  await db.markSeeded()
  return project
}
