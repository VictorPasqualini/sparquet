import { describe, expect, it } from 'vitest'

import { pipelineToGraph } from '@/lib/compiler'
import {
  fileStageName,
  linkRejection,
  newFileStage,
  newPipeline,
  planPipelineRun,
  resolvePipeline,
  wouldCreateCycle,
} from '@/lib/pipeline'
import type { Pipeline, PipelineLink, PipelineStage, Job } from '@/types/studio'

/* ---------------------------------------------------------------- helpers */

/**
 * Builds a job through the importer, like the library store does, so the
 * stages resolve against the same graph the editor stores and compiles back.
 */
function job(id: string, name: string, pipeline: unknown): Job {
  const imported = pipelineToGraph(pipeline)
  return {
    id,
    workflowId: 'workflow',
    name,
    description: '',
    tags: [],
    settings: imported.settings,
    graph: imported.graph,
    params: [],
    createdAt: 0,
    updatedAt: 0,
    revision: 1,
  }
}

const pipe = (name: string, inPath: string, outPath: string) => ({
  name,
  input: { format: 'csv', path: inPath },
  output: { format: 'parquet', path: outPath, mode: 'overwrite' },
})

const stage = (id: string, jobId: string, x = 0): PipelineStage => ({
  id,
  jobId,
  position: { x, y: 0 },
})

/** A stage that names a JSON in the library instead of a Job. */
const fileStage = (id: string, path: string, x = 0): PipelineStage => ({
  id,
  jobId: '',
  path,
  position: { x, y: 0 },
})

const link = (id: string, source: string, target: string): PipelineLink => ({ id, source, target })

function pipeline(stages: PipelineStage[], links: PipelineLink[]): Pipeline {
  return { ...newPipeline({ workflowId: 'workflow', name: 'Nightly' }), stages, links }
}

const BRONZE = job('w-bronze', 'Bronze', pipe('bronze', '/raw/orders', '/lake/bronze'))
const SILVER = job('w-silver', 'Silver', pipe('silver', '/lake/bronze', '/lake/silver'))
const GOLD = job('w-gold', 'Gold', pipe('gold', '/lake/silver', '/lake/gold'))
const JOBS = [BRONZE, SILVER, GOLD]

const orderOf = (resolved: { stages: { id: string; order: number }[] }) =>
  resolved.stages.map((entry) => [entry.id, entry.order])

/* ------------------------------------------------------------------ tests */

describe('resolvePipeline — ordering', () => {
  it('numbers stages by the links the author drew, not by insertion order', () => {
    // Stored back to front on purpose: the links decide, the array does not.
    const resolved = resolvePipeline(
      pipeline(
        [stage('c', 'w-gold'), stage('a', 'w-bronze'), stage('b', 'w-silver')],
        [link('l1', 'a', 'b'), link('l2', 'b', 'c')],
      ),
      JOBS,
    )

    expect(orderOf(resolved)).toEqual([
      ['a', 1],
      ['b', 2],
      ['c', 3],
    ])
    expect(resolved.issues.filter((issue) => issue.severity === 'error')).toEqual([])
  })

  it('orders a fan-out so a stage never runs before the one feeding it', () => {
    const resolved = resolvePipeline(
      pipeline(
        [stage('a', 'w-bronze'), stage('b', 'w-silver'), stage('c', 'w-gold')],
        [link('l1', 'a', 'b'), link('l2', 'a', 'c')],
      ),
      JOBS,
    )

    const orders = new Map(resolved.stages.map((entry) => [entry.id, entry.order]))
    expect(orders.get('a')).toBe(1)
    expect(orders.get('b')).toBeGreaterThan(1)
    expect(orders.get('c')).toBeGreaterThan(1)
  })

  it('is stable for unlinked stages, ordering them by name, and warns about each', () => {
    const resolved = resolvePipeline(
      pipeline([stage('z', 'w-gold'), stage('a', 'w-bronze')], []),
      JOBS,
    )

    expect(resolved.stages.map((entry) => entry.name)).toEqual(['Bronze', 'Gold'])
    expect(resolved.issues.map((issue) => issue.id).sort()).toEqual([
      'pipeline-unlinked-a',
      'pipeline-unlinked-z',
    ])
    expect(resolved.issues.every((issue) => issue.severity === 'warning')).toBe(true)
  })

  it('runs a stage nothing links to last, so dropping a box never renumbers the chain', () => {
    // "Bronze" would sort first by name, but it is not wired into the chain.
    const resolved = resolvePipeline(
      pipeline(
        [stage('a', 'w-silver'), stage('b', 'w-gold'), stage('loose', 'w-bronze')],
        [link('l1', 'a', 'b')],
      ),
      JOBS,
    )

    expect(orderOf(resolved)).toEqual([
      ['a', 1],
      ['b', 2],
      ['loose', 3],
    ])
    expect(resolved.issues.map((issue) => issue.id)).toEqual(['pipeline-unlinked-loose'])
  })

  it('drops links pointing at a stage that is no longer on the canvas', () => {
    const resolved = resolvePipeline(
      pipeline([stage('a', 'w-bronze')], [link('l1', 'a', 'deleted')]),
      JOBS,
    )

    expect(resolved.links).toEqual([])
    expect(resolved.stages).toHaveLength(1)
  })

  it('reports a cycle instead of inventing an order for it', () => {
    // Only reachable through an imported record: `linkRejection` refuses to draw it.
    const resolved = resolvePipeline(
      pipeline(
        [stage('a', 'w-bronze'), stage('b', 'w-silver')],
        [link('l1', 'a', 'b'), link('l2', 'b', 'a')],
      ),
      JOBS,
    )

    expect(resolved.issues.map((issue) => issue.id).sort()).toEqual([
      'pipeline-cycle-a',
      'pipeline-cycle-b',
    ])
    // Every stage still gets a number, so nothing disappears from the canvas.
    expect(resolved.stages.map((entry) => entry.order).sort()).toEqual([1, 2])
    expect(planPipelineRun(resolved).stages).toEqual([])
  })
})

describe('resolvePipeline — stage contents', () => {
  it('describes each stage from its job and carries the pipeline it would run', () => {
    const resolved = resolvePipeline(
      pipeline([stage('a', 'w-bronze'), stage('b', 'w-silver')], [link('l1', 'a', 'b')]),
      JOBS,
    )

    const [first] = resolved.stages
    expect(first.name).toBe('Bronze')
    expect(first.job).toBe(BRONZE)
    expect(first.description?.input).toEqual({ format: 'csv', path: '/raw/orders' })
    expect(first.description?.outputs).toEqual([
      { format: 'parquet', path: '/lake/bronze', mode: 'overwrite' },
    ])
    expect(first.pipeline?.name).toBe('bronze')
  })

  it('reads the same job twice when it is staged twice', () => {
    const resolved = resolvePipeline(
      pipeline([stage('a', 'w-bronze'), stage('b', 'w-bronze')], [link('l1', 'a', 'b')]),
      JOBS,
    )

    expect(resolved.stages.map((entry) => entry.name)).toEqual(['Bronze', 'Bronze'])
    expect(planPipelineRun(resolved).stages.map((entry) => entry.id)).toEqual(['a', 'b'])
  })
})

describe('resolvePipeline — broken references', () => {
  it('keeps the stage, marks it missing and blocks the run', () => {
    const resolved = resolvePipeline(
      pipeline([stage('a', 'w-bronze'), stage('b', 'w-deleted')], [link('l1', 'a', 'b')]),
      JOBS,
    )

    const broken = resolved.stages.find((entry) => entry.id === 'b')
    expect(broken?.job).toBeNull()
    expect(broken?.description).toBeNull()
    expect(broken?.pipeline).toBeNull()
    expect(broken?.order).toBe(2)

    const plan = planPipelineRun(resolved)
    expect(plan.stages).toEqual([])
    expect(plan.blockers.map((issue) => issue.id)).toContain('pipeline-missing-b')
  })

  it('blocks the run when a staged job does not compile', () => {
    const draft = job('w-draft', 'Draft', pipe('draft', '/raw', '/out'))
    // Drop every destination: the pipeline no longer compiles.
    draft.graph = {
      nodes: draft.graph.nodes.filter((node) => node.data.kind !== 'sink'),
      edges: [],
    }

    const resolved = resolvePipeline(pipeline([stage('a', 'w-draft')], []), [draft])
    const stageEntry = resolved.stages[0]

    expect(stageEntry.pipeline).toBeNull()
    // Still described, off the canvas, so the box is recognisable.
    expect(stageEntry.description?.input).toEqual({ format: 'csv', path: '/raw' })
    expect(planPipelineRun(resolved).blockers.map((issue) => issue.id)).toEqual([
      'pipeline-uncompilable-a',
    ])
  })
})

describe('resolvePipeline — stages backed by a file', () => {
  it('resolves without a job, and does not call that broken', () => {
    const resolved = resolvePipeline(
      pipeline([fileStage('a', 'vendas/jobs/ingestao.json')], []),
      JOBS,
    )
    const entry = resolved.stages[0]

    expect(entry.path).toBe('vendas/jobs/ingestao.json')
    expect(entry.job).toBeNull()
    // Nothing is read here: the runner reads the file when the stage starts.
    expect(entry.description).toBeNull()
    expect(entry.pipeline).toBeNull()
    expect(resolved.issues).toEqual([])
  })

  it('names the box after the file, without the extension', () => {
    const resolved = resolvePipeline(pipeline([fileStage('a', 'a/b/ingestao.json')], []), JOBS)

    expect(resolved.stages[0].name).toBe('ingestao')
    expect(fileStageName('a/b/ingestao.JSON')).toBe('ingestao')
  })

  it('orders and links like any other stage', () => {
    const resolved = resolvePipeline(
      pipeline(
        [stage('b', 'w-silver', 200), fileStage('a', 'bronze.json')],
        [link('l1', 'a', 'b')],
      ),
      JOBS,
    )

    expect(orderOf(resolved)).toEqual([
      ['a', 1],
      ['b', 2],
    ])
  })

  it('is still refused when it would close a cycle', () => {
    const resolved = resolvePipeline(
      pipeline(
        [fileStage('a', 'one.json'), fileStage('b', 'two.json', 200)],
        [link('l1', 'a', 'b'), link('l2', 'b', 'a')],
      ),
      JOBS,
    )

    expect(resolved.issues.map((issue) => issue.id)).toContain('pipeline-cycle-a')
  })

  it('sends the path instead of a compiled pipeline', () => {
    const plan = planPipelineRun(
      resolvePipeline(
        pipeline([stage('a', 'w-bronze'), fileStage('b', 'vendas/limpeza.json', 200)], [
          link('l1', 'a', 'b'),
        ]),
        JOBS,
      ),
    )

    expect(plan.blockers).toEqual([])
    expect(plan.stages.map((entry) => [entry.name, entry.path ?? null])).toEqual([
      ['Bronze', null],
      ['limpeza', 'vendas/limpeza.json'],
    ])
    expect(plan.stages[1].pipeline).toBeUndefined()
    expect(plan.stages[1].jobId).toBe('')
  })

  it('builds one with an empty job id, so nothing reads it as a deleted job', () => {
    const created = newFileStage('vendas/limpeza.json', { x: 10, y: 0 })

    expect(created.jobId).toBe('')
    expect(created.path).toBe('vendas/limpeza.json')
    expect(created.id.startsWith('stage-')).toBe(true)
  })
})

describe('link rules', () => {
  it('refuses a self-link', () => {
    expect(linkRejection([], 'a', 'a')).toBe('self')
    expect(wouldCreateCycle([], 'a', 'a')).toBe(true)
  })

  it('refuses a duplicate link, in that direction only', () => {
    const links = [link('l1', 'a', 'b')]
    expect(linkRejection(links, 'a', 'b')).toBe('duplicate')
    // The reverse direction is a cycle, not a duplicate.
    expect(linkRejection(links, 'b', 'a')).toBe('cycle')
  })

  it('refuses a link that closes a loop, however long the path', () => {
    const links = [link('l1', 'a', 'b'), link('l2', 'b', 'c'), link('l3', 'c', 'd')]
    expect(wouldCreateCycle(links, 'd', 'a')).toBe(true)
    expect(linkRejection(links, 'd', 'a')).toBe('cycle')
    // A shortcut forward is not a loop.
    expect(linkRejection(links, 'a', 'd')).toBeNull()
  })

  it('allows an unrelated link', () => {
    expect(linkRejection([link('l1', 'a', 'b')], 'c', 'd')).toBeNull()
  })
})

describe('planPipelineRun', () => {
  it('emits the stages in execution order, with the compiled pipeline of each', () => {
    const resolved = resolvePipeline(
      pipeline(
        [stage('c', 'w-gold'), stage('a', 'w-bronze'), stage('b', 'w-silver')],
        [link('l1', 'a', 'b'), link('l2', 'b', 'c')],
      ),
      JOBS,
    )

    const plan = planPipelineRun(resolved)
    expect(plan.blockers).toEqual([])
    expect(plan.stages.map((entry) => [entry.id, entry.name])).toEqual([
      ['a', 'Bronze'],
      ['b', 'Silver'],
      ['c', 'Gold'],
    ])
    expect(plan.stages[0].pipeline?.input).toEqual({ format: 'csv', path: '/raw/orders' })
    // No params declared: the key is omitted rather than sent empty.
    expect(plan.stages[0].params).toBeUndefined()
  })

  it('forwards the declared params of each staged job', () => {
    const parameterized = job('w-p', 'Params', pipe('params', '/raw/{dt}', '/out'))
    parameterized.params = [{ id: 'p1', key: 'dt', type: 'string', value: '2026-01-01' }]

    const plan = planPipelineRun(
      resolvePipeline(pipeline([stage('a', 'w-p')], []), [parameterized]),
    )

    expect(plan.stages[0].params).toEqual({ dt: '2026-01-01' })
  })

  it('refuses to run an empty pipeline', () => {
    const plan = planPipelineRun(resolvePipeline(pipeline([], []), JOBS))
    expect(plan.stages).toEqual([])
    expect(plan.blockers.map((issue) => issue.id)).toEqual(['pipeline-run-empty'])
  })

  it('runs nothing when one stage is broken — a sequence only means anything whole', () => {
    const resolved = resolvePipeline(
      pipeline([stage('a', 'w-bronze'), stage('b', 'w-gone')], [link('l1', 'a', 'b')]),
      JOBS,
    )
    expect(planPipelineRun(resolved).stages).toEqual([])
  })
})
