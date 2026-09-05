import { describe, expect, it } from 'vitest'

import { lineageExampleTemplates, templateToJob } from '@/data/templates'
import { buildLineage, datasetNodeId, jobNodeId, traceFrom } from './lineage'
import { toFlow } from './flow'

/** The three Medallion templates, imported exactly as the Load example button does. */
const exampleJobs = () =>
  lineageExampleTemplates().map((template) => templateToJob(template, 'wf'))

describe('the Medallion example', () => {
  it('imports as three Jobs', () => {
    expect(exampleJobs()).toHaveLength(3)
  })

  it('chains bronze to silver to gold on addresses alone', () => {
    const jobs = exampleJobs()
    const index = buildLineage(jobs)
    const [bronze, silver, gold] = jobs

    expect(index.edges).toEqual([
      { from: bronze.id, to: silver.id, datasetKey: '/lake/bronze/orders' },
      { from: silver.id, to: gold.id, datasetKey: '/lake/silver/orders' },
    ])
  })

  it('places every dataset of the chain', () => {
    const index = buildLineage(exampleJobs())
    const place = Object.fromEntries(
      index.datasets.map((dataset) => [dataset.key, dataset.place]),
    )

    expect(place).toEqual({
      '/lake/landing/orders': 'external',
      '/lake/raw/customers': 'external',
      '/lake/silver/orders_backfill': 'external',
      '/lake/bronze/orders': 'intermediate',
      '/lake/silver/orders': 'intermediate',
      '/lake/quality/orders_report': 'terminal',
      '/lake/quarantine/orders': 'terminal',
      '/lake/gold/revenue_by_country': 'terminal',
      '/lake/exports/revenue_by_country': 'terminal',
    })
  })

  it('counts the join side and the union side as reads', () => {
    const index = buildLineage(exampleJobs())
    const roles = (key: string) =>
      index.datasets.find((dataset) => dataset.key === key)?.consumers.map((m) => m.role)

    expect(roles('/lake/raw/customers')).toEqual(['join'])
    expect(roles('/lake/silver/orders_backfill')).toEqual(['union'])
  })
})

describe('traceFrom', () => {
  it('follows the whole path through the chain, however many Jobs away', () => {
    const jobs = exampleJobs()
    const index = buildLineage(jobs)
    const trace = traceFrom(index, datasetNodeId('/lake/bronze/orders'))

    // Upstream: the Job that writes it and the CSV that Job reads.
    expect(trace.upstream).toEqual(
      new Set([jobNodeId(jobs[0].id), datasetNodeId('/lake/landing/orders')]),
    )
    // Downstream: two Jobs away, the gold table is still on the path.
    expect(trace.downstream.has(datasetNodeId('/lake/gold/revenue_by_country'))).toBe(true)
    expect(trace.downstream.has(jobNodeId(jobs[2].id))).toBe(true)
    // A dimension read by silver is upstream of silver, not downstream of bronze.
    expect(trace.all.has(datasetNodeId('/lake/raw/customers'))).toBe(false)
  })

  it('terminates on a Job that reads back what it wrote', () => {
    const index = buildLineage(exampleJobs())
    const cycleIndex = {
      ...index,
      jobs: [
        {
          jobId: 'j',
          jobName: 'in place',
          workflowId: 'wf',
          reads: [{ role: 'input' as const, format: 'delta', address: '/t', nodeId: 'a' }],
          writes: [{ role: 'output' as const, format: 'delta', address: '/t', nodeId: 'b' }],
        },
      ],
    }
    const trace = traceFrom(cycleIndex, datasetNodeId('/t'))
    expect(trace.all).toEqual(new Set([datasetNodeId('/t'), jobNodeId('j')]))
  })
})

describe('toFlow', () => {
  it('puts a Job between the datasets it reads and the ones it writes', () => {
    const jobs = exampleJobs()
    const flow = toFlow(buildLineage(jobs))
    const bronze = jobNodeId(jobs[0].id)

    expect(flow.nodes.filter((node) => node.type === 'job')).toHaveLength(3)
    expect(flow.nodes.filter((node) => node.type === 'dataset')).toHaveLength(9)
    expect(
      flow.edges.some(
        (edge) => edge.source === datasetNodeId('/lake/landing/orders') && edge.target === bronze,
      ),
    ).toBe(true)
    expect(
      flow.edges.some(
        (edge) => edge.source === bronze && edge.target === datasetNodeId('/lake/bronze/orders'),
      ),
    ).toBe(true)
  })

  it('labels only the edges a plain arrow would not explain', () => {
    const flow = toFlow(buildLineage(exampleJobs()))
    const labels = flow.edges.map((edge) => edge.label).filter(Boolean)

    expect(labels).toContain('join')
    expect(labels).toContain('union')
    expect(labels).toContain('quarantine')
    expect(labels).toContain('report')
  })

  it('lays every node out somewhere, not all on top of each other', () => {
    const flow = toFlow(buildLineage(exampleJobs()))
    const positions = new Set(flow.nodes.map((node) => `${node.position.x},${node.position.y}`))
    expect(positions.size).toBe(flow.nodes.length)
  })
})
