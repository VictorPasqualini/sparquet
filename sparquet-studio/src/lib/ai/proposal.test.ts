/**
 * What happens to an answer after the model stops typing.
 *
 * The chat tests cover the wire and the parser; the compiler tests cover graphs
 * built on the canvas. Between them sits the path nothing held: a reply arrives
 * as prose with a fenced block, gets parsed, becomes a graph, gets linted and
 * compiled back to JSON — and only then does anyone find out that the assistant
 * proposes pipelines the Studio itself calls broken.
 *
 * So this walks the whole way for a proposal a model would plausibly write, and
 * holds the two things that make the Apply button worth having: the canvas
 * accepts it without an error, and what compiles back out is what was proposed.
 */

import { describe, expect, it } from 'vitest'

import { autoLayout, compileGraph, pipelineToGraph } from '@/lib/compiler'
import { extractProposalFor } from '@/lib/ai/parse'
import { lintJob } from '@/lib/validation/lint'
import type { PipelineSpec } from '@/types/pipeline'

/** A pipeline shaped the way the system prompt asks for one. */
const PIPELINE = {
  name: 'active_orders',
  description: 'Keeps the active orders and writes them partitioned by day.',
  input: {
    format: 'csv',
    path: '/data/raw/orders',
    options: { header: 'true', inferSchema: 'true' },
  },
  transformations: [
    { type: 'filter', condition: "status = 'ACTIVE'" },
    { type: 'select', columns: ['order_id', 'customer_id', 'total', 'order_date'] },
  ],
  validations: {
    rules: [
      { type: 'not_null', columns: ['order_id'], on_failure: 'fail' },
      { type: 'unique', columns: ['order_id'], on_failure: 'warn' },
    ],
  },
  output: {
    format: 'parquet',
    path: '/data/curated/active_orders',
    mode: 'overwrite',
    partition_by: ['order_date'],
  },
}

/** The reply as it leaves a model: a paragraph, one fenced block, nothing else. */
const REPLY = [
  'I kept the filter first so the select only touches the rows you keep, and',
  'partitioned the write by `order_date`.',
  '',
  '```json',
  JSON.stringify(PIPELINE, null, 2),
  '```',
].join('\n')

function accept(text: string) {
  const proposal = extractProposalFor('generate', text)
  expect(proposal.error).toBeUndefined()
  expect(proposal.pipeline).not.toBeNull()

  const decompiled = pipelineToGraph(proposal.pipeline)
  const graph = autoLayout(decompiled.graph)
  const issues = lintJob(graph, decompiled.settings, [])
  return { proposal, decompiled, graph, settings: decompiled.settings, issues }
}

describe('an accepted proposal', () => {
  it('is read out of the reply with the prose left behind', () => {
    const { proposal } = accept(REPLY)

    expect(proposal.summary).toContain('kept the filter first')
    expect(proposal.summary).not.toContain('"format"')
  })

  it('reaches the canvas without a decompiler error', () => {
    const { decompiled, graph } = accept(REPLY)

    expect(decompiled.issues.filter((issue) => issue.severity === 'error')).toEqual([])
    expect(graph.nodes.map((node) => node.type)).toEqual([
      'source',
      'transform',
      'transform',
      'validation',
      'validation',
      'sink',
    ])
  })

  it('passes the same linter the canvas runs', () => {
    const { issues } = accept(REPLY)

    expect(issues.filter((issue) => issue.severity === 'error')).toEqual([])
  })

  it('compiles back to the JSON the model proposed', () => {
    const { graph, settings } = accept(REPLY)
    const compiled = compileGraph(graph, settings, [])

    expect(compiled.issues.filter((issue) => issue.severity === 'error')).toEqual([])
    // The compiler writes the block-level `on_failure` even when every rule
    // already carries its own, so the default is on the page rather than in
    // somebody's head. Everything else comes back byte for byte.
    expect(compiled.pipeline).toEqual({
      ...PIPELINE,
      validations: { on_failure: 'fail', ...PIPELINE.validations },
    } as unknown as PipelineSpec)
  })

  it('survives a reply that fences the JSON without a language tag', () => {
    // Models drop the `json` after the backticks often enough that treating it
    // as "no proposal" would lose a good pipeline to a formatting habit.
    const { proposal } = accept(REPLY.replace('```json', '```'))

    expect(proposal.pipeline).toEqual(PIPELINE)
  })

  it('lets the linter catch a rule the model wrote in the singular', () => {
    // `columns` is a list; the framework iterates it, so `column: "order_id"`
    // would be walked character by character. The canvas has to say so before
    // the run does, and this is the shape a model gets wrong most often.
    const singular = REPLY.replace(/"columns": \[\s*"order_id"\s*\]/g, '"column": "order_id"')
    const { issues } = accept(singular)

    const errors = issues.filter((issue) => issue.severity === 'error')
    expect(errors).toHaveLength(2)
    expect(errors[0].message).toContain('"Columns" is required')
  })

  it('reports a truncated stream instead of applying half a pipeline', () => {
    const cut = REPLY.slice(0, REPLY.indexOf('"validations"'))
    const proposal = extractProposalFor('generate', cut)

    expect(proposal.pipeline).toBeNull()
    expect(proposal.error).toBeTruthy()
  })
})
