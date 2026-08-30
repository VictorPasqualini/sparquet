import { readdirSync, readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'

import { autoLayout, compileGraph, pipelineToGraph, serializePipeline } from '@/lib/compiler'
import type { PipelineSpec } from '@/types/pipeline'
import type {
  SinkNode,
  SourceNode,
  StudioEdge,
  StudioGraph,
  TransformNode,
  ValidationNode,
  ValidationSinkRole,
  JobSettings,
} from '@/types/studio'
import { HANDLE } from '@/types/studio'

/* ---------------------------------------------------------------- helpers */

const SETTINGS: JobSettings = { pipelineName: 'test', description: '', spark: {} }

const sourceNode = (id: string, format = 'csv', path = '/in'): SourceNode => ({
  id,
  type: 'source',
  position: { x: 0, y: 0 },
  data: { kind: 'source', format, path, options: {} },
})

const transformNode = (
  id: string,
  transform: string,
  params: Record<string, unknown> = {},
  extra: { disabled?: boolean; skipIfFalse?: string } = {},
): TransformNode => ({
  id,
  type: 'transform',
  position: { x: 0, y: 0 },
  data: { kind: 'transform', transform, params, ...extra },
})

const sinkNode = (
  id: string,
  format = 'parquet',
  path = '/out',
  dqRole?: ValidationSinkRole,
): SinkNode => ({
  id,
  type: 'sink',
  position: { x: 0, y: 0 },
  data: {
    kind: 'sink',
    format,
    path,
    mode: 'overwrite',
    partitionBy: [],
    columns: null,
    options: {},
    ...(dqRole ? { dqRole } : {}),
  },
})

const validationNode = (
  id: string,
  validator = 'not_null',
  params: Record<string, unknown> = { columns: ['id'] },
  extra: { disabled?: boolean } = {},
): ValidationNode => ({
  id,
  type: 'validation',
  position: { x: 0, y: 0 },
  data: { kind: 'validation', validator, params, ...extra },
})

const link = (source: string, target: string, handle: string = HANDLE.in): StudioEdge => ({
  id: `${source}->${target}:${handle}`,
  source,
  target,
  sourceHandle: HANDLE.out,
  targetHandle: handle,
})

const errorsOf = (issues: { severity: string; message: string }[]) =>
  issues.filter((issue) => issue.severity === 'error').map((issue) => issue.message)

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys)
  if (value !== null && typeof value === 'object') {
    const record = value as Record<string, unknown>
    const out: Record<string, unknown> = {}
    for (const key of Object.keys(record).sort()) out[key] = sortKeys(record[key])
    return out
  }
  return value
}

/** `output` and `outputs` are the same thing to the framework. */
function normalizePipeline(pipeline: unknown): unknown {
  if (pipeline === null || typeof pipeline !== 'object') return pipeline
  const record = { ...(pipeline as Record<string, unknown>) }
  if (record.output !== undefined && record.outputs === undefined) {
    record.outputs = [record.output]
    delete record.output
  }
  return sortKeys(record)
}

function roundTrip(pipeline: unknown): PipelineSpec {
  const imported = pipelineToGraph(pipeline)
  expect(errorsOf(imported.issues)).toEqual([])
  const compiled = compileGraph(imported.graph, imported.settings)
  expect(errorsOf(compiled.issues)).toEqual([])
  expect(compiled.pipeline).not.toBeNull()
  return compiled.pipeline as PipelineSpec
}

function expectRoundTrip(pipeline: unknown): PipelineSpec {
  const compiled = roundTrip(pipeline)
  expect(normalizePipeline(compiled)).toEqual(normalizePipeline(pipeline))
  return compiled
}

/* ------------------------------------------------------------ round trips */

describe('round trip', () => {
  it('keeps a single source and a single sink', () => {
    const pipeline = {
      name: 'customers',
      description: 'basic ingestion',
      input: { format: 'csv', path: '/data/customers.csv' },
      transformations: [
        { type: 'filter', condition: "status = 'active'" },
        { type: 'select', columns: ['id', 'name'] },
      ],
      output: { format: 'parquet', path: '/dw/customers', mode: 'overwrite' },
    }

    const compiled = expectRoundTrip(pipeline)
    expect(compiled.outputs).toBeUndefined()
    expect(compiled.output?.path).toBe('/dw/customers')

    const { graph } = pipelineToGraph(pipeline)
    expect(graph.nodes.map((node) => node.data.kind)).toEqual([
      'source',
      'transform',
      'transform',
      'sink',
    ])
    expect(graph.edges).toHaveLength(3)
    expect(graph.edges.every((edge) => edge.targetHandle === HANDLE.in)).toBe(true)
    expect(graph.nodes.some((node) => node.position.x !== 0)).toBe(true)
  })

  it('preserves empty structural params (maps/lists) through a round trip', () => {
    // An empty required container is a valid no-op in the framework but must survive
    // export: dropping it emits JSON the framework rejects (with_column with no
    // `columns` falls into the single-column branch → ValueError; `struct` without
    // `fields` → KeyError). Regression for the compiler pruning empty {} / [].
    const pipeline = {
      name: 'empty-structural-params',
      input: { format: 'csv', path: '/in' },
      transformations: [
        { type: 'with_column', columns: {} },
        { type: 'struct', column: 'payload', fields: {} },
        { type: 'rename', mappings: {} },
      ],
      output: { format: 'parquet', path: '/out', mode: 'overwrite' },
    }

    const compiled = expectRoundTrip(pipeline)
    const transforms = compiled.transformations as Array<Record<string, unknown>>
    expect(transforms[0]).toEqual({ type: 'with_column', columns: {} })
    expect(transforms[1]).toEqual({ type: 'struct', column: 'payload', fields: {} })
    expect(transforms[2]).toEqual({ type: 'rename', mappings: {} })
  })

  it('keeps per-output transformations on a multi-output pipeline', () => {
    const pipeline = {
      name: 'payload',
      input: { format: 'delta', path: 'lastros.silver' },
      transformations: [
        { type: 'filter', condition: "status = 'READY'" },
        { type: 'checkpoint' },
      ],
      outputs: [
        {
          format: 'parquet',
          path: '/events/json',
          mode: 'append',
          transformations: [
            { type: 'with_column', column: 'value', expression: 'to_json(payload)' },
          ],
          columns: ['id', 'value'],
        },
        {
          format: 'delta',
          path: 'lastros.gold',
          mode: 'append',
          transformations: [
            { type: 'with_column', column: 'total', expression: 'payload.values.total' },
          ],
        },
      ],
    }

    const compiled = expectRoundTrip(pipeline)
    expect(compiled.transformations).toHaveLength(2)
    expect(compiled.outputs).toHaveLength(2)
    expect(compiled.outputs?.[0].transformations).toHaveLength(1)
    expect(compiled.outputs?.[1].columns).toBeUndefined()
  })

  it('keeps per-output transformations on a single-output pipeline', () => {
    const pipeline = {
      name: 'checked',
      input: { format: 'delta', path: 'lastros.silver' },
      transformations: [{ type: 'filter', condition: "status = 'READY'" }],
      validations: {
        on_failure: 'warn',
        rules: [{ type: 'row_count', min: 1 }],
      },
      output: {
        format: 'kafka',
        path: 'events',
        transformations: [
          { type: 'with_column', column: 'value', expression: 'to_json(payload)' },
        ],
      },
    }

    const compiled = expectRoundTrip(pipeline)
    expect(compiled.transformations).toHaveLength(1)
    expect(compiled.output?.transformations).toHaveLength(1)
  })

  it('keeps a join with its right-hand sub-chain', () => {
    const pipeline = {
      name: 'enrich',
      input: { format: 'delta', path: 'sales.orders' },
      transformations: [
        { type: 'collect', column: 'customer_id', as: 'active_customers' },
        {
          type: 'join',
          input: { format: 'delta', path: 'sales.bronze_events' },
          with_transformations: [
            { type: 'filter', condition: 'customer_id IN ({{active_customers}})' },
            { type: 'select', columns: ['customer_id', 'score'] },
            { type: 'distinct' },
          ],
          on: 'customer_id',
          how: 'left',
        },
      ],
      output: { format: 'delta', path: 'sales.enriched', mode: 'overwrite' },
    }

    expectRoundTrip(pipeline)

    const { graph } = pipelineToGraph(pipeline)
    const sideEdges = graph.edges.filter((edge) => edge.targetHandle === HANDLE.inRight)
    expect(sideEdges).toHaveLength(1)
    expect(graph.nodes.filter((node) => node.data.kind === 'source')).toHaveLength(2)
  })

  it('keeps a union and its second source', () => {
    const pipeline = {
      name: 'stack',
      input: { format: 'parquet', path: '/data/current' },
      transformations: [
        {
          type: 'union',
          input: { format: 'parquet', path: '/data/history' },
          allow_missing_columns: true,
        },
      ],
      output: { format: 'parquet', path: '/data/all' },
    }

    const compiled = expectRoundTrip(pipeline)
    const union = compiled.transformations?.[0] as Record<string, unknown>
    expect(union.input).toEqual({ format: 'parquet', path: '/data/history' })
    expect(union.with).toBeUndefined()
    expect(union.with_transformations).toBeUndefined()
    expect(union.allow_missing_columns).toBe(true)
  })

  it('keeps a validations block with a report', () => {
    const pipeline = {
      name: 'quality',
      input: { format: 'csv', path: '/data/in.csv' },
      transformations: [{ type: 'filter', condition: 'id IS NOT NULL' }],
      validations: {
        on_failure: 'warn',
        report: { format: 'csv', path: '/dq/report', mode: 'overwrite' },
        rules: [
          { type: 'not_null', columns: ['id'] },
          { type: 'range', column: 'age', min: 0, max: 150 },
        ],
      },
      output: { format: 'csv', path: '/data/out' },
    }

    const compiled = expectRoundTrip(pipeline)
    expect(compiled.validations?.rules).toHaveLength(2)
    expect(compiled.validations?.report?.path).toBe('/dq/report')

    // One node per rule; the report is a STANDALONE destination beside the job's
    // own output — never in place of it, and wired to nothing.
    const { graph, settings } = pipelineToGraph(pipeline)
    const rules = graph.nodes.filter((node) => node.data.kind === 'validation')
    expect(rules).toHaveLength(2)
    expect(rules.map((node) => (node.data.kind === 'validation' ? node.data.validator : ''))).toEqual(
      ['not_null', 'range'],
    )

    const sinks = graph.nodes.filter((node) => node.data.kind === 'sink')
    expect(sinks).toHaveLength(2)
    const reportSink = sinks.find(
      (node) => node.data.kind === 'sink' && node.data.dqRole === 'report',
    )
    expect(reportSink?.data.kind === 'sink' && reportSink.data.path).toBe('/dq/report')
    // The report is anchored to the validations it comes out of — a box left floating
    // reads as a destination someone forgot to wire — and that anchor lands on the
    // plain input handle, which compiles to nothing.
    const anchors = graph.edges.filter((edge) => edge.target === reportSink?.id)
    expect(anchors).toHaveLength(1)
    expect(anchors[0]?.targetHandle).toBe(HANDLE.in)

    // Only the run policy is left in the settings.
    expect(settings.validations).toEqual({ onFailure: 'warn' })
  })

  it('keeps the quarantine outputs of a validations block', () => {
    const pipeline = {
      name: 'quarantine',
      input: { format: 'delta', path: 'bronze.pedidos' },
      validations: {
        on_failure: 'warn',
        outputs: {
          valid: { format: 'delta', path: 'silver.ok', mode: 'overwrite' },
          invalid: { format: 'delta', path: 'silver.quarentena', mode: 'overwrite' },
        },
        rules: [{ type: 'not_null', columns: ['id'] }],
      },
      output: { format: 'delta', path: 'silver.pedidos', mode: 'overwrite' },
    }

    const compiled = expectRoundTrip(pipeline)
    expect(compiled.validations?.outputs?.invalid.path).toBe('silver.quarentena')

    const { graph } = pipelineToGraph(pipeline)
    // Three destinations: the job's own, plus the two quarantine copies. The main
    // one is NOT fed through the quarantine — it still receives every row.
    const sinks = graph.nodes.filter((node) => node.data.kind === 'sink')
    expect(sinks).toHaveLength(3)

    const pathOfRole = (role: string) => {
      const node = sinks.find((sink) => sink.data.kind === 'sink' && sink.data.dqRole === role)
      return node?.data.kind === 'sink' ? node.data.path : undefined
    }
    expect(pathOfRole('valid')).toBe('silver.ok')
    expect(pathOfRole('invalid')).toBe('silver.quarentena')

    // Both are anchored to the validations they come out of, on the plain input
    // handle — an anchor the compiler reads nothing from, unlike the scope handle.
    // Neither ever feeds anything, so no edge may LEAVE them.
    const quarantineIds = sinks
      .filter((sink) => sink.data.kind === 'sink' && sink.data.dqRole !== undefined)
      .map((sink) => sink.id)
    expect(quarantineIds).toHaveLength(2)
    expect(graph.edges.some((edge) => quarantineIds.includes(edge.source))).toBe(false)
    const anchored = graph.edges.filter((edge) => quarantineIds.includes(edge.target))
    expect(anchored).toHaveLength(2)
    expect(anchored.every((edge) => edge.targetHandle === HANDLE.in)).toBe(true)

    // The main chain still ends at the job's own destination, untouched.
    expect(compiled.output?.path).toBe('silver.pedidos')
  })

  it('keeps an $include directive', () => {
    const pipeline = {
      name: 'shared',
      input: { format: 'csv', path: '/data/in.csv' },
      transformations: [
        { $include: 'shared/filter.json' },
        { type: 'drop_duplicates', columns: ['id'] },
      ],
      output: { format: 'csv', path: '/data/out' },
    }

    const compiled = expectRoundTrip(pipeline)
    expect(compiled.transformations?.[0]).toEqual({ $include: 'shared/filter.json' })

    const { graph } = pipelineToGraph(pipeline)
    const include = graph.nodes.find(
      (node) => node.data.kind === 'transform' && node.data.transform === '$include',
    )
    expect(include).toBeDefined()
  })

  it('imports an unknown transformation type verbatim', () => {
    const pipeline = {
      name: 'custom',
      input: { format: 'csv', path: '/data/in.csv' },
      transformations: [{ type: 'normalize_text', column: 'email', aggressive: true }],
      output: { format: 'csv', path: '/data/out' },
    }

    expectRoundTrip(pipeline)
  })
})

/* ---------------------------------------------------------------- compile */

describe('compileGraph', () => {
  it('leaves disabled nodes and notes out of the JSON', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        transformNode('t1', 'filter', { condition: 'id IS NOT NULL' }),
        transformNode('t2', 'select', { columns: ['id'] }, { disabled: true }),
        {
          id: 'note',
          type: 'note',
          position: { x: 0, y: 0 },
          data: { kind: 'note', text: 'todo', tone: 'info' },
        },
        sinkNode('out'),
      ],
      edges: [link('src', 't1'), link('t1', 't2'), link('t2', 'out')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(errorsOf(issues)).toEqual([])
    expect(pipeline?.transformations).toEqual([{ type: 'filter', condition: 'id IS NOT NULL' }])
  })

  it('writes skip_if_false at the top level of the transformation', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        transformNode(
          't1',
          'filter',
          { condition: "kind = '{kind}'" },
          { skipIfFalse: '{kind}' },
        ),
        sinkNode('out'),
      ],
      edges: [link('src', 't1'), link('t1', 'out')],
    }

    const { pipeline } = compileGraph(graph, SETTINGS)
    expect(Object.keys(pipeline?.transformations?.[0] ?? {})).toEqual([
      'type',
      'skip_if_false',
      'condition',
    ])
  })

  it('omits empty values', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        transformNode('t1', 'checkpoint', { method: '', eager: false }),
        sinkNode('out'),
      ],
      edges: [link('src', 't1'), link('t1', 'out')],
    }

    const { pipeline } = compileGraph(graph, SETTINGS)
    expect(pipeline?.transformations?.[0]).toEqual({ type: 'checkpoint', eager: false })
    expect(pipeline?.output).toEqual({ format: 'parquet', path: '/out', mode: 'overwrite' })
    expect(pipeline?.validations).toBeUndefined()
  })

  it('compiles nodes after the validation rules into the destination transformations', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        transformNode('t1', 'filter', { condition: '1 = 1' }),
        validationNode('val'),
        transformNode('t2', 'with_column', {
          column: 'value',
          expression: 'to_json(payload)',
        }),
        sinkNode('out'),
      ],
      edges: [link('src', 't1'), link('t1', 'val'), link('val', 't2'), link('t2', 'out')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(errorsOf(issues)).toEqual([])
    expect(pipeline?.transformations).toEqual([{ type: 'filter', condition: '1 = 1' }])
    expect(pipeline?.output?.transformations).toEqual([
      { type: 'with_column', column: 'value', expression: 'to_json(payload)' },
    ])
  })

  it('gives every destination its own copy of a node shared after the rules', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        validationNode('val'),
        transformNode('t1', 'with_column', {
          column: 'value',
          expression: 'to_json(payload)',
        }),
        sinkNode('out-a', 'parquet', '/out-a'),
        sinkNode('out-b', 'parquet', '/out-b'),
      ],
      edges: [link('src', 'val'), link('val', 't1'), link('t1', 'out-a'), link('t1', 'out-b')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(errorsOf(issues)).toEqual([])
    expect(pipeline?.transformations).toBeUndefined()
    expect(pipeline?.outputs?.[0].transformations).toHaveLength(1)
    expect(pipeline?.outputs?.[1].transformations).toHaveLength(1)
  })

  it('reports a job without a destination', () => {
    const graph: StudioGraph = { nodes: [sourceNode('src')], edges: [] }
    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(pipeline).toBeNull()
    expect(errorsOf(issues)).toEqual(['The job has no destination.'])
  })

  it('reports destinations reading from different sources', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src-a', 'csv', '/a'),
        sourceNode('src-b', 'csv', '/b'),
        sinkNode('out-a', 'parquet', '/out-a'),
        sinkNode('out-b', 'parquet', '/out-b'),
      ],
      edges: [link('src-a', 'out-a'), link('src-b', 'out-b')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(pipeline).toBeNull()
    expect(errorsOf(issues)).toContain('Every destination must read from the same source.')
  })

  it('reports a node with two incoming main connections', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src-a', 'csv', '/a'),
        sourceNode('src-b', 'csv', '/b'),
        transformNode('t1', 'distinct'),
        sinkNode('out'),
      ],
      edges: [link('src-a', 't1'), link('src-b', 't1'), link('t1', 'out')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(pipeline).toBeNull()
    expect(errorsOf(issues)).toContain('A node can only have one incoming main connection.')
  })

  it('compiles a run of rule nodes into one validations block, in canvas order', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        transformNode('t1', 'filter', { condition: '1 = 1' }),
        validationNode('v1', 'not_null', { columns: ['id'] }),
        validationNode('v2', 'unique', { columns: ['id'] }),
        validationNode('v3', 'row_count', { min: 1 }),
        sinkNode('out'),
      ],
      edges: [
        link('src', 't1'),
        link('t1', 'v1'),
        link('v1', 'v2'),
        link('v2', 'v3'),
        link('v3', 'out'),
      ],
    }

    const report = sinkNode('dq', 'csv', '/dq', 'report')
    report.data.mode = 'append'
    graph.nodes.push(report)

    const { pipeline, issues } = compileGraph(graph, {
      ...SETTINGS,
      validations: { onFailure: 'warn' },
    })
    expect(errorsOf(issues)).toEqual([])
    expect(pipeline?.validations).toEqual({
      on_failure: 'warn',
      report: { format: 'csv', path: '/dq', mode: 'append' },
      rules: [
        { type: 'not_null', columns: ['id'] },
        { type: 'unique', columns: ['id'] },
        { type: 'row_count', min: 1 },
      ],
    })
    // Rules are not transformations: the main array keeps only the filter.
    expect(pipeline?.transformations).toEqual([{ type: 'filter', condition: '1 = 1' }])
  })

  it('emits no validations key when no rule node is on the chain', () => {
    const graph: StudioGraph = {
      nodes: [sourceNode('src'), sinkNode('out')],
      edges: [link('src', 'out')],
    }

    const { pipeline } = compileGraph(graph, {
      ...SETTINGS,
      // Policy alone never produces a block — the framework needs rules.
      validations: { onFailure: 'warn' },
    })
    expect(pipeline?.validations).toBeUndefined()
  })

  it('leaves a muted rule out of the block', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        validationNode('v1', 'not_null', { columns: ['id'] }),
        validationNode('v2', 'unique', { columns: ['id'] }, { disabled: true }),
        sinkNode('out'),
      ],
      edges: [link('src', 'v1'), link('v1', 'v2'), link('v2', 'out')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(errorsOf(issues)).toEqual([])
    expect(pipeline?.validations?.rules).toEqual([{ type: 'not_null', columns: ['id'] }])
  })

  it('reports a transformation wedged between two rules', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        validationNode('v1'),
        transformNode('t1', 'filter', { condition: '1 = 1' }),
        validationNode('v2', 'unique', { columns: ['id'] }),
        sinkNode('out'),
      ],
      edges: [link('src', 'v1'), link('v1', 't1'), link('t1', 'v2'), link('v2', 'out')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(pipeline).toBeNull()
    expect(errorsOf(issues)).toContain('This node runs between two validation rules.')
  })

  it('reports validations placed after the chain diverges', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        transformNode('t1', 'filter', { condition: '1 = 1' }),
        validationNode('val'),
        sinkNode('out-a', 'parquet', '/out-a'),
        sinkNode('out-b', 'parquet', '/out-b'),
      ],
      edges: [link('src', 't1'), link('t1', 'val'), link('val', 'out-a'), link('t1', 'out-b')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(pipeline).toBeNull()
    expect(errorsOf(issues)).toContain(
      'Validations must run before the chain splits into several destinations.',
    )
  })

  it('reports a join without a second source', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        transformNode('j', 'join', { on: 'id', how: 'left' }),
        sinkNode('out'),
      ],
      edges: [link('src', 'j'), link('j', 'out')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(pipeline).toBeNull()
    expect(errorsOf(issues)).toContain('A `join` needs a second source on its right input.')
  })

  it('reports transformations wired into the second input of a union', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        sourceNode('src-side', 'parquet', '/side'),
        transformNode('t-side', 'filter', { condition: '1 = 1' }),
        transformNode('u', 'union', { allow_missing_columns: true }),
        sinkNode('out'),
      ],
      edges: [
        link('src', 'u'),
        link('src-side', 't-side'),
        link('t-side', 'u', HANDLE.inRight),
        link('u', 'out'),
      ],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(pipeline).toBeNull()
    expect(errorsOf(issues)).toContain(
      'A `union` ignores transformations placed on its second input.',
    )
  })
})

/* ---------------------------------------------------- one rule, many targets */

describe('targets', () => {
  const withTargets = (extra: Record<string, unknown> = {}) => ({
    name: 'docs',
    input: { format: 'delta', path: 'bronze.clientes' },
    validations: {
      on_failure: 'warn',
      rules: [
        {
          type: 'regex',
          targets: [
            { column: 'document', pattern: '^[0-9]{11}$' },
            { column: 'document2', pattern: '^[0-9]{12}$' },
          ],
        },
      ],
      ...extra,
    },
    output: { format: 'delta', path: 'silver.clientes' },
  })

  it('round-trips a multi-target rule as one node, unchanged', () => {
    // The library expands it at parse time; Studio keeps the authored shape, so the
    // file the user reads back is the file they wrote.
    const compiled = roundTrip(withTargets())
    expect(compiled.validations?.rules).toEqual([
      {
        type: 'regex',
        targets: [
          { column: 'document', pattern: '^[0-9]{11}$' },
          { column: 'document2', pattern: '^[0-9]{12}$' },
        ],
      },
    ])
  })

  it('draws a scope naming every target as an edge, and re-emits both codes', () => {
    const spec = withTargets({
      outputs: {
        invalid: {
          format: 'delta',
          path: 'dq.docs',
          mode: 'overwrite',
          annotate: 'dq_codes',
          rules: ['regex(document,^[0-9]{11}$)', 'regex(document2,^[0-9]{12}$)'],
        },
      },
    })

    const { graph } = pipelineToGraph(spec)
    const sink = graph.nodes.find((node) => node.data.kind === 'sink' && node.data.dqRole === 'invalid')
    const rule = graph.nodes.find((node) => node.data.kind === 'validation')
    // One edge for the node, not one per code — the same node twice would be a
    // duplicate connection on the canvas.
    const scoping = graph.edges.filter(
      (edge) => edge.target === sink?.id && edge.targetHandle === HANDLE.inScope,
    )
    expect(scoping).toHaveLength(1)
    expect(scoping[0]?.source).toBe(rule?.id)
    expect(sink?.data.kind === 'sink' ? sink.data.dqRules : undefined).toBeUndefined()

    const compiled = roundTrip(spec)
    expect(compiled.validations?.outputs?.invalid?.rules).toEqual([
      'regex(document,^[0-9]{11}$)',
      'regex(document2,^[0-9]{12}$)',
    ])
  })

  it('keeps a scope that names only SOME of the targets verbatim', () => {
    // An edge means the whole node, so drawing one here would silently widen the
    // scope to the other document on the next save. The code stays on the node.
    const spec = withTargets({
      outputs: {
        invalid: {
          format: 'delta',
          path: 'dq.docs',
          mode: 'overwrite',
          rules: ['regex(document,^[0-9]{11}$)'],
        },
      },
    })

    const { graph } = pipelineToGraph(spec)
    const sink = graph.nodes.find((node) => node.data.kind === 'sink' && node.data.dqRole === 'invalid')
    expect(sink?.data.kind === 'sink' ? sink.data.dqRules : undefined).toEqual([
      'regex(document,^[0-9]{11}$)',
    ])
    expect(
      graph.edges.filter((edge) => edge.target === sink?.id && edge.targetHandle === HANDLE.inScope),
    ).toHaveLength(0)

    const compiled = roundTrip(spec)
    expect(compiled.validations?.outputs?.invalid?.rules).toEqual(['regex(document,^[0-9]{11}$)'])
  })
})

/* ------------------------------------------------- quality destinations */

describe('quality destinations', () => {
  /** src → v1 → out, plus a standalone quality destination in the given role. */
  const withQualitySink = (role: ValidationSinkRole, sinkId = 'dq'): StudioGraph => ({
    nodes: [
      sourceNode('src'),
      validationNode('v1', 'not_null', { columns: ['id'] }),
      sinkNode('out', 'delta', 'silver.pedidos'),
      sinkNode(sinkId, 'delta', 'dq.rows', role),
    ],
    edges: [link('src', 'v1'), link('v1', 'out')],
  })

  it('compiles a quality sink into validations, never into outputs', () => {
    const { pipeline, issues } = compileGraph(withQualitySink('invalid'), SETTINGS)
    expect(errorsOf(issues)).toEqual([])
    expect(pipeline?.validations?.outputs).toEqual({
      invalid: { format: 'delta', path: 'dq.rows', mode: 'overwrite' },
    })
    // The job still writes ONE destination, and it is the one on the main chain.
    expect(pipeline?.outputs).toBeUndefined()
    expect(pipeline?.output?.path).toBe('silver.pedidos')
  })

  it('finds a quality sink wherever it sits, with no connection at all', () => {
    // Same graph, but the node is declared before the source and after the sink in
    // node order — position on the canvas carries no meaning for these.
    const graph: StudioGraph = {
      nodes: [
        sinkNode('dq', 'csv', '/dq/report', 'report'),
        sourceNode('src'),
        validationNode('v1', 'not_null', { columns: ['id'] }),
        sinkNode('out', 'delta', 'silver.pedidos'),
      ],
      edges: [link('src', 'v1'), link('v1', 'out')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(errorsOf(issues)).toEqual([])
    expect(pipeline?.validations?.report).toEqual({
      format: 'csv',
      path: '/dq/report',
      mode: 'overwrite',
    })
    // And it is never reported as an unreachable node, because it is not one.
    expect(issues.map((issue) => issue.message)).not.toContain(
      'This node does not reach a destination and was left out.',
    )
  })

  it('keeps the shared prefix intact: a quality sink is not a second chain', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        transformNode('t1', 'filter', { condition: '1 = 1' }),
        validationNode('v1', 'not_null', { columns: ['id'] }),
        sinkNode('out', 'delta', 'silver.pedidos'),
        sinkNode('dq', 'csv', '/dq', 'report'),
      ],
      edges: [link('src', 't1'), link('t1', 'v1'), link('v1', 'out')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(errorsOf(issues)).toEqual([])
    // The filter stays a MAIN transformation: had the quality sink been walked as a
    // chain, the shared prefix would have collapsed and pushed it onto the output.
    expect(pipeline?.transformations).toEqual([{ type: 'filter', condition: '1 = 1' }])
    expect(pipeline?.output?.transformations).toBeUndefined()
  })

  it('orders the quarantine keys valid then invalid, whatever the canvas order', () => {
    const graph = withQualitySink('invalid')
    graph.nodes.push(sinkNode('ok', 'delta', 'dq.ok', 'valid'))

    const { pipeline } = compileGraph(graph, SETTINGS)
    expect(Object.keys(pipeline?.validations?.outputs ?? {})).toEqual(['valid', 'invalid'])
  })

  it('does not let a quality sink stand in for the job destination', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        validationNode('v1', 'not_null', { columns: ['id'] }),
        sinkNode('dq', 'delta', 'dq.rows', 'report'),
      ],
      edges: [link('src', 'v1')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(pipeline).toBeNull()
    expect(errorsOf(issues)).toContain('The job has no destination.')
  })

  it('refuses two destinations in the same role, keeping the first', () => {
    const graph = withQualitySink('report')
    graph.nodes.push(sinkNode('dq2', 'csv', '/dq/other', 'report'))

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(pipeline).toBeNull()
    expect(errorsOf(issues)).toContain('Two quality destinations write the same dataset.')
    // The second one is the one reported: the first keeps the role, deterministically.
    const duplicate = issues.find(
      (issue) => issue.message === 'Two quality destinations write the same dataset.',
    )
    expect(duplicate?.nodeId).toBe('dq2')
  })

  it('refuses a quality sink in a job with no compiled rule', () => {
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        transformNode('t1', 'filter', { condition: '1 = 1' }),
        sinkNode('out'),
        sinkNode('dq', 'csv', '/dq', 'report'),
      ],
      edges: [link('src', 't1'), link('t1', 'out')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(pipeline).toBeNull()
    expect(errorsOf(issues)).toContain(
      'This quality destination has no validation rule to fill it.',
    )
  })

  it('refuses a quality sink whose only rule never compiles', () => {
    // The rule sits past the fan-out, so it is not part of the block.
    const graph: StudioGraph = {
      nodes: [
        sourceNode('src'),
        transformNode('t1', 'filter', { condition: '1 = 1' }),
        validationNode('v1', 'not_null', { columns: ['id'] }),
        sinkNode('a', 'csv', '/a'),
        sinkNode('b', 'csv', '/b'),
        sinkNode('dq', 'csv', '/dq', 'report'),
      ],
      edges: [link('src', 't1'), link('t1', 'v1'), link('v1', 'a'), link('t1', 'b')],
    }

    const { pipeline, issues } = compileGraph(graph, SETTINGS)
    expect(pipeline).toBeNull()
    expect(errorsOf(issues)).toContain(
      'This quality destination has no validation rule to fill it.',
    )
  })

  it('keeps the quality sinks out of the main row when laying the canvas out', () => {
    const { graph } = pipelineToGraph({
      name: 'q',
      input: { format: 'delta', path: 'bronze.p' },
      validations: {
        on_failure: 'warn',
        outputs: { invalid: { format: 'delta', path: 'dq.bad' } },
        rules: [{ type: 'not_null', columns: ['id'] }],
      },
      output: { format: 'delta', path: 'silver.p' },
    })

    const laid = autoLayout(graph)
    const side = laid.nodes.find(
      (node) => node.data.kind === 'sink' && node.data.dqRole === 'invalid',
    )
    const main = laid.nodes.filter((node) => node.id !== side?.id)
    const lowestMain = Math.max(...main.map((node) => node.position.y))
    const rule = laid.nodes.find((node) => node.data.kind === 'validation')
    // Below the whole diagram, in the last rule's column: near what fills it, and
    // clear of the row the data actually flows along.
    expect(side?.position.y).toBeGreaterThan(lowestMain)
    expect(side?.position.x).toBe(rule?.position.x)
    // The main destination stays on the row, to the right of the rule.
    const output = main.find((node) => node.data.kind === 'sink')
    expect(output?.position.y).toBe(rule?.position.y)
  })

  it('drops the quality destinations of a validations block with no usable rule', () => {
    const { graph, issues } = pipelineToGraph({
      name: 'q',
      input: { format: 'delta', path: 'bronze.p' },
      validations: {
        on_failure: 'warn',
        report: { format: 'csv', path: '/dq' },
        rules: [],
      },
      output: { format: 'delta', path: 'silver.p' },
    })

    expect(graph.nodes.filter((node) => node.data.kind === 'sink')).toHaveLength(1)
    expect(issues.map((issue) => issue.message)).toContain(
      'The validations "report" destination has no validation rule and was dropped.',
    )
  })

  it('drops a quarantine key the framework would not route to', () => {
    const { graph, issues } = pipelineToGraph({
      name: 'q',
      input: { format: 'delta', path: 'bronze.p' },
      validations: {
        on_failure: 'warn',
        outputs: { maybe: { format: 'csv', path: '/dq' } },
        rules: [{ type: 'not_null', columns: ['id'] }],
      },
      output: { format: 'delta', path: 'silver.p' },
    })

    expect(graph.nodes.filter((node) => node.data.kind === 'sink')).toHaveLength(1)
    expect(issues.map((issue) => issue.message)).toContain(
      '"validations.outputs.maybe" is not a quarantine key and was dropped.',
    )
  })
})

/* -------------------------------------------------------------- serialize */

describe('serializePipeline', () => {
  it('emits a stable, human-readable key order', () => {
    const pipeline: PipelineSpec = {
      output: { path: '/out', format: 'csv' },
      input: { path: '/in', format: 'csv' },
      validations: { rules: [{ columns: ['id'], type: 'not_null' }], on_failure: 'warn' },
      transformations: [{ condition: '1 = 1', type: 'filter', skip_if_false: '{go}' }],
      description: 'demo',
      name: 'demo',
    }

    const text = serializePipeline(pipeline)
    expect(text.startsWith('{\n  "name": "demo",')).toBe(true)
    const parsed = JSON.parse(text) as Record<string, unknown>
    expect(Object.keys(parsed)).toEqual([
      'name',
      'description',
      'input',
      'transformations',
      'validations',
      'output',
    ])
    expect(Object.keys(parsed.input as Record<string, unknown>)).toEqual(['format', 'path'])
    expect(Object.keys((parsed.transformations as Record<string, unknown>[])[0])).toEqual([
      'type',
      'skip_if_false',
      'condition',
    ])
    expect(Object.keys(parsed.validations as Record<string, unknown>)).toEqual([
      'on_failure',
      'rules',
    ])
  })
})

/* ------------------------------------------------------------ resilience */

describe('pipelineToGraph', () => {
  it('never throws on malformed input', () => {
    const cyclic: Record<string, unknown> = { name: 'loop' }
    cyclic.self = cyclic

    const inputs: unknown[] = [
      null,
      undefined,
      42,
      'not json',
      [],
      {},
      { name: 'x', input: 'nope', transformations: 'nope', output: 7 },
      { name: 'x', input: { format: 'csv', path: '/in' }, transformations: [null, {}, 3] },
      { name: 'x', input: { format: 'csv', path: '/in' }, transformations: [{ $include: 9 }] },
      {
        name: 'x',
        input: { format: 'csv', path: '/in' },
        validations: { on_failure: 'boom', rules: [{ nope: true }] },
        output: { format: 'csv', path: '/out' },
      },
      { name: 'x', input: { format: 'csv', path: '/in' }, transformations: [cyclic] },
    ]

    for (const input of inputs) {
      const result = pipelineToGraph(input)
      expect(Array.isArray(result.graph.nodes)).toBe(true)
      expect(Array.isArray(result.issues)).toBe(true)
      expect(() => compileGraph(result.graph, result.settings)).not.toThrow()
    }
  })

  it('drops with_transformations from a union so the imported graph still compiles', () => {
    const imported = pipelineToGraph({
      name: 'un',
      input: { format: 'csv', path: '/in' },
      transformations: [
        {
          type: 'union',
          input: { format: 'csv', path: '/other' },
          with_transformations: [{ type: 'select', columns: ['id'] }],
        },
      ],
      output: { format: 'csv', path: '/out' },
    })

    expect(errorsOf(imported.issues)).toEqual([])
    expect(imported.issues.map((issue) => issue.message)).toContain(
      'A `union` never applies "with_transformations".',
    )
    expect(imported.graph.nodes.filter((node) => node.data.kind === 'transform')).toHaveLength(
      1,
    )

    const compiled = compileGraph(imported.graph, imported.settings)
    expect(errorsOf(compiled.issues)).toEqual([])
    const union = compiled.pipeline?.transformations?.[0] as Record<string, unknown>
    expect(union.input).toEqual({ format: 'csv', path: '/other' })
    expect(union.with_transformations).toBeUndefined()
  })

  it('prefers outputs over output when both are present', () => {
    const { graph, settings } = pipelineToGraph({
      name: 'both',
      input: { format: 'csv', path: '/in' },
      output: { format: 'csv', path: '/ignored' },
      outputs: [{ format: 'csv', path: '/kept' }],
    })

    const { pipeline } = compileGraph(graph, settings)
    expect(pipeline?.output?.path).toBe('/kept')
  })

  it('warns about template params that are not defined', () => {
    const { graph, settings } = pipelineToGraph({
      name: 'templated',
      input: { format: 'delta', path: 'sales.{table}' },
      transformations: [{ type: 'filter', condition: "kind = '{kind}'" }],
      output: { format: 'csv', path: '/out' },
    })

    const { issues } = compileGraph(graph, settings, [
      { id: 'p1', key: 'kind', type: 'string', value: 'NC' },
    ])
    const messages = issues.map((issue) => issue.message)
    expect(messages).toContain(
      'The pipeline uses {table}, but no parameter with that name is defined.',
    )
    expect(messages.some((message) => message.includes('{kind}'))).toBe(false)
  })
})

/* ----------------------------------------------------------- real configs */

/**
 * The fixtures the Spark test executes for real (`fixtures/formats/`, six
 * formats, a write and a read each). They are the only configs in the project
 * that are pinned on both sides: this test says the Studio can open and compile
 * them back unchanged, and `server/test_formats_studio_spark.py` says the runner
 * really runs them. Neither half is worth much alone — a JSON that round-trips
 * perfectly and does not run is still broken.
 */
describe('the executed format fixtures', () => {
  const dir = fileURLToPath(new URL('../../../fixtures/formats/', import.meta.url))
  const formats = readdirSync(dir).sort()

  it('finds the six native formats', () => {
    expect(formats).toEqual(['csv', 'json', 'orc', 'parquet', 'txt', 'view'])
  })

  for (const format of formats) {
    for (const half of ['write.json', 'read.json']) {
      it(`round-trips ${format}/${half}`, () => {
        const original: unknown = JSON.parse(readFileSync(`${dir}${format}/${half}`, 'utf8'))
        const compiled = expectRoundTrip(original)
        expect(JSON.parse(serializePipeline(compiled))).toEqual(compiled)
      })
    }
  }
})

/**
 * Discovered, not listed: every `examples/*.json` the framework ships is
 * round-tripped. A hardcoded list rots in both directions — a new example goes
 * untested, and a deleted one turns into six unreadable ENOENTs instead of one
 * sentence telling you what happened.
 */
describe('the shipped example configs', () => {
  const dir = fileURLToPath(new URL('../../../../examples/', import.meta.url))
  const examples = readdirSync(dir).filter((name) => name.endsWith('.json')).sort()

  it('finds the examples directory', () => {
    expect(
      examples.length,
      `No .json found in ${dir}. The examples are fixtures for these tests — if they ` +
        'vanished from your working tree, restore them with `git restore examples`.',
    ).toBeGreaterThan(0)
  })

  for (const file of examples) {
    it(`round-trips ${file}`, () => {
      const original: unknown = JSON.parse(readFileSync(`${dir}${file}`, 'utf8'))
      const compiled = expectRoundTrip(original)
      expect(JSON.parse(serializePipeline(compiled))).toEqual(compiled)
    })
  }
})
