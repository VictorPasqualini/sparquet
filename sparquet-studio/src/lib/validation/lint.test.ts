import { describe, expect, it } from 'vitest'

import { TEMPLATES } from '@/data/templates'
import { pipelineToGraph } from '@/lib/compiler'
import { inferParams } from '@/lib/params'
import { lintJob } from '@/lib/validation/lint'
import { HANDLE } from '@/types/studio'
import type {
  ParamDefinition,
  SinkNodeData,
  SourceNodeData,
  StudioEdge,
  StudioNode,
  TransformNodeData,
  ValidationIssue,
  ValidationNodeData,
  JobSettings,
} from '@/types/studio'

const SETTINGS: JobSettings = { pipelineName: 'test', description: '', spark: {} }

const source = (id: string, patch: Partial<SourceNodeData> = {}): StudioNode => ({
  id,
  type: 'source',
  position: { x: 0, y: 0 },
  data: { kind: 'source', format: 'parquet', path: '/in', options: {}, ...patch },
})

const transform = (
  id: string,
  transformType: string,
  params: Record<string, unknown> = {},
  patch: Partial<TransformNodeData> = {},
): StudioNode => ({
  id,
  type: 'transform',
  position: { x: 0, y: 0 },
  data: { kind: 'transform', transform: transformType, params, ...patch },
})

const sink = (id: string, patch: Partial<SinkNodeData> = {}): StudioNode => ({
  id,
  type: 'sink',
  position: { x: 0, y: 0 },
  data: {
    kind: 'sink',
    format: 'parquet',
    path: '/out',
    mode: 'overwrite',
    partitionBy: [],
    columns: null,
    options: {},
    ...patch,
  },
})

const validation = (
  id: string,
  validator = 'not_null',
  params: Record<string, unknown> = { columns: ['id'] },
  patch: Partial<ValidationNodeData> = {},
): StudioNode => ({
  id,
  type: 'validation',
  position: { x: 0, y: 0 },
  data: { kind: 'validation', validator, params, ...patch },
})

const link = (from: string, to: string, handle: string = HANDLE.in): StudioEdge => ({
  id: `${from}->${to}:${handle}`,
  source: from,
  target: to,
  sourceHandle: HANDLE.out,
  targetHandle: handle,
})

const lint = (
  nodes: StudioNode[],
  edges: StudioEdge[],
  params: ParamDefinition[] = [],
  settings: JobSettings = SETTINGS,
): ValidationIssue[] => lintJob({ nodes, edges }, settings, params)

const idsOf = (issues: ValidationIssue[]): string[] => issues.map((issue) => issue.id)

const param = (key: string): ParamDefinition => ({
  id: `p-${key}`,
  key,
  type: 'string',
  value: 'x',
})

/** src → filter → not_null → sink; the reference graph every rule deviates from. */
const cleanGraph = (): { nodes: StudioNode[]; edges: StudioEdge[] } => ({
  nodes: [
    source('src'),
    transform('f', 'filter', { condition: 'valor > 0' }),
    validation('v'),
    sink('out'),
  ],
  edges: [link('src', 'f'), link('f', 'v'), link('v', 'out')],
})

describe('lintJob', () => {
  it('reports nothing for a complete pipeline', () => {
    const { nodes, edges } = cleanGraph()
    expect(lint(nodes, edges)).toEqual([])
  })

  it('gives every issue a stable, deduplicated id', () => {
    const nodes = [transform('f', 'filter', {})]
    const first = idsOf(lint(nodes, []))
    const second = idsOf(lint(nodes, []))
    expect(first).toEqual(second)
    expect(new Set(first).size).toBe(first.length)
  })

  describe('graph structure', () => {
    it('flags a pipeline without a sink', () => {
      const issues = lint(
        [source('src'), transform('f', 'filter', { condition: 'a > 1' })],
        [link('src', 'f')],
      )
      expect(idsOf(issues)).toContain('no-sink')
      expect(issues.find((issue) => issue.id === 'no-sink')?.severity).toBe('error')
    })

    it('accepts a pipeline that has a sink', () => {
      const { nodes, edges } = cleanGraph()
      expect(idsOf(lint(nodes, edges))).not.toContain('no-sink')
    })

    it('flags a pipeline without a source', () => {
      const issues = lint(
        [transform('f', 'filter', { condition: 'a > 1' }), sink('out')],
        [link('f', 'out')],
      )
      expect(idsOf(issues)).toContain('no-source')
    })

    it('accepts a pipeline that has a source', () => {
      const { nodes, edges } = cleanGraph()
      expect(idsOf(lint(nodes, edges))).not.toContain('no-source')
    })

    it('flags transforms and sinks with no incoming connection', () => {
      const issues = lint(
        [
          source('src'),
          transform('f', 'filter', { condition: 'a > 1' }),
          sink('out'),
          sink('lost', { path: '/lost' }),
        ],
        [link('src', 'out')],
      )
      expect(idsOf(issues)).toContain('orphan:f')
      expect(idsOf(issues)).toContain('orphan:lost')
    })

    it('accepts connected transforms and sinks', () => {
      const { nodes, edges } = cleanGraph()
      expect(idsOf(lint(nodes, edges)).filter((id) => id.startsWith('orphan:'))).toEqual([])
    })
  })

  describe('catalog fields', () => {
    it('flags a required transformation field left empty', () => {
      const issues = lint(
        [source('src'), transform('f', 'filter', {}), sink('out')],
        [link('src', 'f'), link('f', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'field:f:condition')
      expect(issue?.severity).toBe('error')
      expect(issue?.field).toBe('condition')
    })

    it('flags a required source path and sink format', () => {
      const issues = lint(
        [source('src', { path: '' }), sink('out', { format: '' })],
        [link('src', 'out')],
      )
      expect(idsOf(issues)).toContain('field:src:path')
      expect(idsOf(issues)).toContain('field:out:format')
    })

    it('flags a validation rule missing its required columns', () => {
      const nodes = [source('src'), validation('v', 'not_null', {}), sink('out')]
      const issues = lint(nodes, [link('src', 'v'), link('v', 'out')])
      expect(idsOf(issues)).toContain('field:v:columns')
    })

    it('flags a quality report node that has no format', () => {
      const issues = lint(
        [
          source('src'),
          validation('v'),
          sink('out'),
          sink('dq', { format: '', path: '/dq/report', dqRole: 'report' }),
        ],
        [link('src', 'v'), link('v', 'out')],
      )
      // A quality destination is a real write: same node-scoped rules as any sink.
      const issue = issues.find((entry) => entry.id === 'field:dq:format')
      expect(issue?.severity).toBe('error')
      expect(issue?.nodeId).toBe('dq')
    })

    it('accepts a complete report and quarantine beside a row-level rule', () => {
      const issues = lint(
        [
          source('src'),
          validation('v'),
          sink('out'),
          sink('dq', { format: 'csv', path: '/dq/report', dqRole: 'report' }),
          sink('ok', { format: 'delta', path: 'silver.ok', dqRole: 'valid' }),
          sink('bad', { format: 'delta', path: 'silver.bad', dqRole: 'invalid' }),
        ],
        [link('src', 'v'), link('v', 'out')],
      )
      expect(idsOf(issues).filter((id) => id.startsWith('dq-'))).toEqual([])
    })

    it('never reports an unconnected quality destination as an orphan', () => {
      const issues = lint(
        [
          source('src'),
          validation('v'),
          sink('out'),
          sink('dq', { format: 'csv', path: '/dq/report', dqRole: 'report' }),
        ],
        [link('src', 'v'), link('v', 'out')],
      )
      // It is a declaration, not a chain member: having no incoming link is correct.
      expect(idsOf(issues)).not.toContain('orphan:dq')
    })

    it('warns about a quarantine sink with only aggregate rules on the chain', () => {
      const issues = lint(
        [
          source('src'),
          validation('v', 'row_count', { min: 1 }),
          sink('out'),
          sink('bad', { format: 'delta', path: 'silver.bad', dqRole: 'invalid' }),
        ],
        [link('src', 'v'), link('v', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'dq-sink-no-row-rule:bad')
      expect(issue?.severity).toBe('warning')
      expect(issue?.nodeId).toBe('bad')
    })

    it('accepts a quarantine sink behind a row-level check metric', () => {
      const issues = lint(
        [
          source('src'),
          validation('v', 'check', {
            metric: 'missing_percent',
            column: 'cpf',
            must_be: '< 1%',
          }),
          sink('out'),
          sink('bad', { format: 'delta', path: 'silver.bad', dqRole: 'invalid' }),
        ],
        [link('src', 'v'), link('v', 'out')],
      )
      expect(idsOf(issues)).not.toContain('dq-sink-no-row-rule:bad')
    })

    it('warns that a quality report ignores a column projection', () => {
      const issues = lint(
        [
          source('src'),
          validation('v'),
          sink('out'),
          sink('dq', { format: 'csv', path: '/dq', columns: ['passed'], dqRole: 'report' }),
        ],
        [link('src', 'v'), link('v', 'out')],
      )
      expect(idsOf(issues)).toContain('dq-report-columns:dq')
    })

    it('flags a quality destination in a job whose only rule is muted', () => {
      const issues = lint(
        [
          source('src'),
          validation('v', 'not_null', { columns: ['id'] }, { disabled: true }),
          sink('out'),
          sink('dq', { format: 'csv', path: '/dq', dqRole: 'report' }),
        ],
        [link('src', 'v'), link('v', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'dq-sink-no-rules:dq')
      expect(issue?.severity).toBe('error')
    })

    it('flags a quality destination in a job with no rule at all', () => {
      const issues = lint(
        [source('src'), sink('out'), sink('dq', { format: 'csv', path: '/dq', dqRole: 'report' })],
        [link('src', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'dq-sink-no-rules:dq')
      expect(issue?.severity).toBe('error')
    })

    it('flags a second destination claiming the same dataset', () => {
      const issues = lint(
        [
          source('src'),
          validation('v'),
          sink('out'),
          sink('dq', { format: 'csv', path: '/dq/a', dqRole: 'report' }),
          sink('dq2', { format: 'csv', path: '/dq/b', dqRole: 'report' }),
        ],
        [link('src', 'v'), link('v', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'dq-sink-duplicate:dq2')
      expect(issue?.severity).toBe('error')
      // The first one keeps the role, so it is not flagged as well.
      expect(idsOf(issues)).not.toContain('dq-sink-duplicate:dq')
    })

    it('keeps the main chain intact around a quality destination', () => {
      const issues = lint(
        [
          source('src'),
          validation('v'),
          transform('t', 'filter', { condition: 'a > 1' }),
          sink('out'),
          sink('bad', { format: 'delta', path: 'silver.bad', dqRole: 'invalid' }),
        ],
        [link('src', 'v'), link('v', 't'), link('t', 'out')],
      )
      // The transform after the rule stays on the trunk: an unconnected quality
      // destination is not a second sink chain, so it must not shorten the prefix.
      expect(idsOf(issues)).not.toContain('orphan:t')
      expect(idsOf(issues)).not.toContain('validations-branch:v')
    })

    it('reports that "on failure" applies to no rule', () => {
      const issues = lint([source('src'), sink('out')], [link('src', 'out')], [], {
        ...SETTINGS,
        validations: { onFailure: 'warn' },
      })
      const issue = issues.find((entry) => entry.id === 'settings:on-failure-unused')
      expect(issue?.severity).toBe('info')
    })

    it('accepts filled catalog fields', () => {
      const nodes = [
        source('src'),
        transform('f', 'filter', { condition: 'valor > 0' }),
        validation('v'),
        sink('out'),
      ]
      const issues = lint(nodes, [link('src', 'f'), link('f', 'v'), link('v', 'out')])
      expect(idsOf(issues).filter((id) => id.startsWith('field:'))).toEqual([])
    })
  })

  describe('io formats', () => {
    it('accepts kafka as a source now that batch read is supported', () => {
      const issues = lint(
        [source('src', { format: 'kafka', path: 'topic' }), sink('out')],
        [link('src', 'out')],
      )
      // kafka gained a batch reader — it is no longer flagged as write-only.
      expect(idsOf(issues)).not.toContain('format-read:src')
    })

    it('accepts a readable source and a kafka sink', () => {
      const issues = lint(
        [
          source('src'),
          sink('out', {
            format: 'kafka',
            path: 'topic',
            mode: 'append',
            options: { bootstrap_servers: 'broker:9092' },
          }),
        ],
        [link('src', 'out')],
      )
      expect(idsOf(issues)).not.toContain('format-read:src')
      expect(idsOf(issues)).not.toContain('format-write:out')
    })

    it('flags merge on a format without merge support, and merge without keys', () => {
      const unsupported = lint(
        [source('src'), sink('out', { mode: 'merge' })],
        [link('src', 'out')],
      )
      expect(idsOf(unsupported)).toContain('merge-unsupported:out')

      const keyless = lint(
        [source('src'), sink('out', { format: 'delta', path: 'db.t', mode: 'merge' })],
        [link('src', 'out')],
      )
      expect(idsOf(keyless)).toContain('merge-keys:out')
    })

    it('blames the mode, not the keys, when an iceberg merge is not lowercase', () => {
      const issues = lint(
        [
          source('src'),
          sink('out', {
            format: 'iceberg',
            path: 'cat.db.t',
            mode: 'MERGE',
            options: { merge_keys: ['id'] },
          }),
        ],
        [link('src', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'merge-case:out')
      expect(issue?.severity).toBe('error')
      expect(issue?.field).toBe('mode')
      expect(idsOf(issues)).not.toContain('merge-keys:out')
    })

    it('accepts a lowercase iceberg merge and still blames a keyless delta merge', () => {
      const iceberg = lint(
        [
          source('src'),
          sink('out', {
            format: 'iceberg',
            path: 'cat.db.t',
            mode: 'merge',
            options: { merge_keys: ['id'] },
          }),
        ],
        [link('src', 'out')],
      )
      expect(idsOf(iceberg).filter((id) => id.startsWith('merge-'))).toEqual([])

      // The Delta writer lower-cases the mode, so "MERGE" really does merge there.
      const delta = lint(
        [source('src'), sink('out', { format: 'delta', path: 'db.t', mode: 'MERGE' })],
        [link('src', 'out')],
      )
      expect(idsOf(delta)).toContain('merge-keys:out')
      expect(idsOf(delta)).not.toContain('merge-case:out')
    })

    it('accepts a delta merge that declares its keys', () => {
      const issues = lint(
        [
          source('src'),
          sink('out', {
            format: 'delta',
            path: 'db.t',
            mode: 'merge',
            options: { merge_keys: ['id'] },
          }),
        ],
        [link('src', 'out')],
      )
      expect(idsOf(issues)).not.toContain('merge-unsupported:out')
      expect(idsOf(issues)).not.toContain('merge-keys:out')
    })
  })

  describe('combine nodes', () => {
    it('flags a join without its right-hand connection', () => {
      const issues = lint(
        [source('src'), transform('j', 'join', { on: 'id', how: 'inner' }), sink('out')],
        [link('src', 'j'), link('j', 'out')],
      )
      expect(idsOf(issues)).toContain('missing-right:j')
    })

    it('accepts a join whose right-hand handle is connected', () => {
      const issues = lint(
        [
          source('src'),
          source('right', { path: '/ref' }),
          transform('j', 'join', { on: 'id', how: 'inner' }),
          sink('out'),
        ],
        [link('src', 'j'), link('right', 'j', HANDLE.inRight), link('j', 'out')],
      )
      expect(idsOf(issues)).not.toContain('missing-right:j')
    })

    it('flags a union whose right chain carries transformations', () => {
      const issues = lint(
        [
          source('src'),
          source('right', { path: '/extra' }),
          transform('t', 'filter', { condition: 'a > 1' }),
          transform('u', 'union', { allow_missing_columns: true }),
          sink('out'),
        ],
        [
          link('src', 'u'),
          link('right', 't'),
          link('t', 'u', HANDLE.inRight),
          link('u', 'out'),
        ],
      )
      expect(idsOf(issues)).toContain('union-right-chain:u')
    })

    it('accepts a union fed directly by a source', () => {
      const issues = lint(
        [
          source('src'),
          source('right', { path: '/extra' }),
          transform('u', 'union', { allow_missing_columns: true }),
          sink('out'),
        ],
        [link('src', 'u'), link('right', 'u', HANDLE.inRight), link('u', 'out')],
      )
      expect(idsOf(issues)).not.toContain('union-right-chain:u')
    })

    it('warns when a union matches columns by position', () => {
      const issues = lint(
        [
          source('src'),
          source('right', { path: '/extra' }),
          transform('u', 'union', { allow_missing_columns: false }),
          sink('out'),
        ],
        [link('src', 'u'), link('right', 'u', HANDLE.inRight), link('u', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'union-positional:u')
      expect(issue?.severity).toBe('warning')
      expect(issue?.message).toMatch(/position/i)
    })

    it('accepts a union matching columns by name', () => {
      const issues = lint(
        [
          source('src'),
          source('right', { path: '/extra' }),
          transform('u', 'union', { allow_missing_columns: true }),
          sink('out'),
        ],
        [link('src', 'u'), link('right', 'u', HANDLE.inRight), link('u', 'out')],
      )
      expect(idsOf(issues)).not.toContain('union-positional:u')
    })
  })

  describe('validations placement', () => {
    it('flags rules that live in per-output branches', () => {
      const issues = lint(
        [
          source('src'),
          transform('cp', 'checkpoint', {}),
          validation('v1'),
          validation('v2', 'unique'),
          sink('out1', { path: '/a' }),
          sink('out2', { path: '/b' }),
        ],
        [
          link('src', 'cp'),
          link('cp', 'v1'),
          link('v1', 'out1'),
          link('cp', 'v2'),
          link('v2', 'out2'),
        ],
      )
      expect(idsOf(issues)).toContain('validations-branch:v1')
      expect(idsOf(issues)).toContain('validations-branch:v2')
    })

    it('accepts a run of rules on the main chain', () => {
      const issues = idsOf(
        lint(
          [
            source('src'),
            validation('v1'),
            validation('v2', 'unique'),
            validation('v3', 'row_count', { min: 1 }),
            sink('out'),
          ],
          [link('src', 'v1'), link('v1', 'v2'), link('v2', 'v3'), link('v3', 'out')],
        ),
      )
      expect(issues.filter((id) => id.startsWith('validations-'))).toEqual([])
    })

    it('flags a rule dropped on the canvas without an incoming connection', () => {
      const { nodes, edges } = cleanGraph()
      const issues = lint([...nodes, validation('loose', 'unique')], edges)
      expect(idsOf(issues)).toContain('orphan:loose')
    })

    it('flags a transformation wedged between two rules', () => {
      const issues = lint(
        [
          source('src'),
          validation('v1'),
          transform('f', 'filter', { condition: 'valor > 0' }),
          validation('v2', 'unique'),
          sink('out'),
        ],
        [link('src', 'v1'), link('v1', 'f'), link('f', 'v2'), link('v2', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'validations-split:f')
      expect(issue?.severity).toBe('error')
    })

    it('accepts a transformation placed after the whole run of rules', () => {
      const issues = lint(
        [
          source('src'),
          validation('v1'),
          validation('v2', 'unique'),
          transform('f', 'filter', { condition: 'valor > 0' }),
          sink('out'),
        ],
        [link('src', 'v1'), link('v1', 'v2'), link('v2', 'f'), link('f', 'out')],
      )
      expect(idsOf(issues)).not.toContain('validations-split:f')
    })

    it('keeps the main chain when one source feeds both the chain and a join right side', () => {
      const issues = lint(
        [
          source('src'),
          transform('stop', 'stop_if_empty', {}),
          transform('side', 'filter', { condition: 'status = 1' }),
          transform('j', 'join', { on: 'id', how: 'left' }),
          validation('v'),
          sink('out'),
        ],
        [
          link('src', 'stop'),
          link('stop', 'j'),
          link('src', 'side'),
          link('side', 'j', HANDLE.inRight),
          link('j', 'v'),
          link('v', 'out'),
        ],
      )
      expect(idsOf(issues)).not.toContain('validations-branch:v')
      expect(idsOf(issues)).not.toContain('stop-if-empty-branch:stop')
    })

    it('keeps the main chain when a probe hangs off it without reaching an output', () => {
      const issues = lint(
        [
          source('src'),
          transform('f', 'filter', { condition: 'valor > 0' }),
          transform('probe', 'debug', { actions: ['count'] }),
          validation('v'),
          sink('out'),
        ],
        [link('src', 'f'), link('f', 'probe'), link('f', 'v'), link('v', 'out')],
      )
      expect(idsOf(issues)).not.toContain('validations-branch:v')
    })

    it('still flags a rule inside a join right chain', () => {
      const issues = lint(
        [
          source('src'),
          source('right', { path: '/ref' }),
          validation('v'),
          transform('j', 'join', { on: 'id', how: 'inner' }),
          sink('out'),
        ],
        [
          link('src', 'j'),
          link('right', 'v'),
          link('v', 'j', HANDLE.inRight),
          link('j', 'out'),
        ],
      )
      expect(idsOf(issues)).toContain('validations-branch:v')
    })

    it('reports a pipeline with no validation rule', () => {
      const issues = lint([source('src'), sink('out')], [link('src', 'out')])
      const issue = issues.find((entry) => entry.id === 'no-validations')
      expect(issue?.severity).toBe('info')
    })

    it('stays quiet when a rule exists', () => {
      const { nodes, edges } = cleanGraph()
      expect(idsOf(lint(nodes, edges))).not.toContain('no-validations')
    })
  })

  describe('runtime variables', () => {
    it('warns when collect is not preceded by a checkpoint', () => {
      const issues = lint(
        [source('src'), transform('c', 'collect', { column: 'id', as: 'ids' }), sink('out')],
        [link('src', 'c'), link('c', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'collect-checkpoint:c')
      expect(issue?.severity).toBe('warning')
    })

    it('accepts a collect placed after a checkpoint', () => {
      const issues = lint(
        [
          source('src'),
          transform('cp', 'checkpoint', {}),
          transform('c', 'collect', { column: 'id', as: 'ids' }),
          sink('out'),
        ],
        [link('src', 'cp'), link('cp', 'c'), link('c', 'out')],
      )
      expect(idsOf(issues)).not.toContain('collect-checkpoint:c')
    })

    it('warns about a {{var}} no collect publishes', () => {
      const issues = lint(
        [
          source('src'),
          transform('f', 'filter', { condition: 'id IN ({{cessoes}})' }),
          sink('out'),
        ],
        [link('src', 'f'), link('f', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'runtime-var:f:cessoes')
      expect(issue?.severity).toBe('warning')
      expect(issue?.message).toContain('cessoes')
      expect(issue?.field).toBe('condition')
    })

    it('accepts a {{var}} published by a collect node', () => {
      const issues = lint(
        [
          source('src'),
          transform('cp', 'checkpoint', {}),
          transform('c', 'collect', { column: 'id_cessao', as: 'cessoes' }),
          transform('f', 'filter', { condition: 'id IN ({{cessoes}})' }),
          sink('out'),
        ],
        [link('src', 'cp'), link('cp', 'c'), link('c', 'f'), link('f', 'out')],
      )
      expect(idsOf(issues).filter((id) => id.startsWith('runtime-var:'))).toEqual([])
    })
  })

  describe('template params', () => {
    it('warns about a {param} that is not declared', () => {
      const issues = lint(
        [
          source('src'),
          transform('f', 'filter', { condition: "tipo = '{tipo_ativo}'" }),
          sink('out'),
        ],
        [link('src', 'f'), link('f', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'param-undeclared:tipo_ativo')
      expect(issue?.severity).toBe('warning')
      expect(issue?.nodeId).toBe('f')
    })

    it('accepts a declared param, and reports one that is never used', () => {
      const nodes = [
        source('src'),
        transform('f', 'filter', { condition: "tipo = '{tipo_ativo}'" }),
        sink('out'),
      ]
      const edges = [link('src', 'f'), link('f', 'out')]
      const issues = lint(nodes, edges, [param('tipo_ativo'), param('registradora')])
      expect(idsOf(issues)).not.toContain('param-undeclared:tipo_ativo')
      const unused = issues.find((entry) => entry.id === 'param-unused:registradora')
      expect(unused?.severity).toBe('info')
    })

    it('does not read a regex quantifier as a template param', () => {
      const rule = lint(
        [
          source('src'),
          validation('v', 'regex', { column: 'cpf', pattern: '^[0-9]{11}$' }),
          sink('out'),
        ],
        [link('src', 'v'), link('v', 'out')],
      )
      expect(idsOf(rule).filter((id) => id.startsWith('param-undeclared:'))).toEqual([])

      const expression = lint(
        [
          source('src'),
          transform('w', 'with_column', {
            column: 'doc',
            expression: "regexp_replace(doc, '[^0-9]{3}', '')",
          }),
          sink('out'),
        ],
        [link('src', 'w'), link('w', 'out')],
      )
      expect(idsOf(expression).filter((id) => id.startsWith('param-undeclared:'))).toEqual([])
    })

    it('scans the format and mode of a destination for params', () => {
      const nodes = [source('src'), sink('out', { mode: '{write_mode}' })]
      const edges = [link('src', 'out')]
      expect(idsOf(lint(nodes, edges, [param('write_mode')]))).not.toContain(
        'param-unused:write_mode',
      )
      expect(idsOf(lint(nodes, edges))).toContain('param-undeclared:write_mode')
    })

    it('does not read a runtime placeholder as a template param', () => {
      const issues = lint(
        [
          source('src'),
          transform('cp', 'checkpoint', {}),
          transform('c', 'collect', { column: 'id', as: 'ids' }),
          transform('f', 'filter', { condition: 'id IN ({{ids}})' }),
          sink('out'),
        ],
        [link('src', 'cp'), link('cp', 'c'), link('c', 'f'), link('f', 'out')],
      )
      expect(idsOf(issues)).not.toContain('param-undeclared:ids')
    })
  })

  describe('per-output chains', () => {
    it('warns about stop_if_empty inside a per-output chain', () => {
      const issues = lint(
        [
          source('src'),
          transform('cp', 'checkpoint', {}),
          transform('s', 'stop_if_empty', {}),
          sink('out1', { path: '/a' }),
          sink('out2', { path: '/b' }),
        ],
        [link('src', 'cp'), link('cp', 's'), link('s', 'out1'), link('cp', 'out2')],
      )
      const issue = issues.find((entry) => entry.id === 'stop-if-empty-branch:s')
      expect(issue?.severity).toBe('warning')
    })

    it('accepts stop_if_empty on the main chain', () => {
      const issues = lint(
        [source('src'), transform('s', 'stop_if_empty', {}), sink('out')],
        [link('src', 's'), link('s', 'out')],
      )
      expect(idsOf(issues)).not.toContain('stop-if-empty-branch:s')
    })

    it('warns when two sinks write to the same destination', () => {
      const issues = lint(
        [source('src'), sink('out1', { path: '/same' }), sink('out2', { path: '/same' })],
        [link('src', 'out1'), link('src', 'out2')],
      )
      const issue = issues.find((entry) => entry.id === 'duplicate-sink:out2')
      expect(issue?.severity).toBe('warning')
      expect(idsOf(issues)).not.toContain('duplicate-sink:out1')
    })

    it('accepts sinks with distinct destinations', () => {
      const issues = lint(
        [source('src'), sink('out1', { path: '/a' }), sink('out2', { path: '/b' })],
        [link('src', 'out1'), link('src', 'out2')],
      )
      expect(idsOf(issues).filter((id) => id.startsWith('duplicate-sink:'))).toEqual([])
    })
  })

  describe('output column projection', () => {
    it('warns about a projected column the chain cannot produce', () => {
      const issues = lint(
        [
          source('src'),
          transform('sel', 'select', { columns: ['id', 'to_json(payload) AS value'] }),
          sink('out', { columns: ['id', 'missing'] }),
        ],
        [link('src', 'sel'), link('sel', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'output-column:out:missing')
      expect(issue?.severity).toBe('warning')
      expect(issue?.field).toBe('columns')
    })

    it('accepts projected columns produced upstream', () => {
      const issues = lint(
        [
          source('src'),
          transform('sel', 'select', { columns: ['id', 'valor'] }),
          transform('wc', 'with_column', { column: 'total', expression: 'valor * 2' }),
          sink('out', { columns: ['id', 'total'] }),
        ],
        [link('src', 'sel'), link('sel', 'wc'), link('wc', 'out')],
      )
      expect(idsOf(issues).filter((id) => id.startsWith('output-column:'))).toEqual([])
    })

    it('stays silent when the column set is not statically knowable', () => {
      const issues = lint(
        [
          source('src'),
          transform('sel', 'select', { columns: ['id'] }),
          transform('q', 'sql', { query: 'SELECT * FROM _df', view_name: '_df' }),
          sink('out', { columns: ['anything'] }),
        ],
        [link('src', 'sel'), link('sel', 'q'), link('q', 'out')],
      )
      expect(idsOf(issues).filter((id) => id.startsWith('output-column:'))).toEqual([])
    })
  })

  describe('developer leftovers', () => {
    it('reports a debug node left in the pipeline', () => {
      const issues = lint(
        [
          source('src'),
          transform('d', 'debug', { label: 'after join', actions: ['count'] }),
          sink('out'),
        ],
        [link('src', 'd'), link('d', 'out')],
      )
      const issue = issues.find((entry) => entry.id === 'debug-node:d')
      expect(issue?.severity).toBe('info')
      expect(issue?.message).toContain('after join')
    })

    it('stays quiet without a debug node', () => {
      const { nodes, edges } = cleanGraph()
      expect(idsOf(lint(nodes, edges)).filter((id) => id.startsWith('debug-node:'))).toEqual([])
    })
  })

  describe('bundled templates', () => {
    for (const template of TEMPLATES) {
      it(`lints "${template.id}" without a single error`, () => {
        const { graph, settings } = pipelineToGraph(template.pipeline)
        const issues = lintJob(graph, settings, inferParams(template.pipeline))
        expect(issues.filter((issue) => issue.severity === 'error')).toEqual([])
      })
    }
  })
})
