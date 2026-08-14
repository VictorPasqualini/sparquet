import { describe, expect, it } from 'vitest'

import { getFormat } from '@/catalog'
import type {
  SinkNodeData,
  SourceNodeData,
  TransformNodeData,
  ValidationsNodeData,
} from '@/types/studio'

import { resolveIssueField, retainOptions } from './Inspector'

const sink = (patch: Partial<SinkNodeData> = {}): SinkNodeData => ({
  kind: 'sink',
  format: 'delta',
  path: 'catalog.schema.pedidos',
  mode: 'merge',
  partitionBy: [],
  columns: null,
  options: {},
  ...patch,
})

const source = (patch: Partial<SourceNodeData> = {}): SourceNodeData => ({
  kind: 'source',
  format: 'delta',
  path: 'catalog.schema.pedidos',
  options: {},
  ...patch,
})

const transform = (type: string, params: Record<string, unknown> = {}): TransformNodeData => ({
  kind: 'transform',
  transform: type,
  params,
})

const validations = (patch: Partial<ValidationsNodeData> = {}): ValidationsNodeData => ({
  kind: 'validations',
  onFailure: 'fail',
  rules: [{ type: 'not_null', columns: [] }],
  report: { format: 'csv', path: '', mode: 'overwrite' },
  ...patch,
})

describe('retainOptions', () => {
  it('drops keys the new format does not declare', () => {
    expect(retainOptions({ versionAsOf: '12' }, getFormat('view')?.readOptions)).toEqual({})
  })

  it('keeps the keys both formats share', () => {
    expect(
      retainOptions({ compression: 'gzip', sep: ';' }, getFormat('txt')?.writeOptions),
    ).toEqual({ compression: 'gzip' })
  })

  it('leaves the options of an unknown format alone', () => {
    const options = { anything: '1' }
    expect(retainOptions(options, getFormat('nope')?.writeOptions)).toEqual(options)
  })
})

describe('resolveIssueField', () => {
  it('maps an option path onto the option field anchor', () => {
    expect(resolveIssueField('n1', sink(), 'options.merge_keys')).toEqual({
      nodeId: 'n1',
      key: 'merge_keys',
    })
  })

  it('resolves an option key that contains dots', () => {
    const node = sink({ format: 'kafka', mode: 'append' })
    expect(resolveIssueField('n1', node, 'options.kafka.bootstrap.servers')).toEqual({
      nodeId: 'n1',
      key: 'kafka.bootstrap.servers',
    })
  })

  it('returns null for an option the current form hides', () => {
    expect(resolveIssueField('n1', sink({ mode: 'append' }), 'options.merge_keys')).toBeNull()
  })

  it('resolves the io controls rendered outside the field renderer', () => {
    const node = sink()
    expect(resolveIssueField('n1', node, 'format')).toEqual({ nodeId: 'n1', key: 'format' })
    expect(resolveIssueField('n1', node, 'mode')).toEqual({ nodeId: 'n1', key: 'mode' })
    expect(resolveIssueField('n1', node, 'columns')).toEqual({ nodeId: 'n1', key: 'columns' })
    expect(resolveIssueField('n1', node, 'partition_by')).toEqual({
      nodeId: 'n1',
      key: 'partition_by',
    })
  })

  it('returns null for a control the format does not render', () => {
    expect(resolveIssueField('n1', sink({ format: 'kafka' }), 'partition_by')).toBeNull()
  })

  it('walks an indexed or nested path up to the anchored field', () => {
    expect(resolveIssueField('n1', sink(), 'columns[0]')).toEqual({
      nodeId: 'n1',
      key: 'columns',
    })
    expect(resolveIssueField('t1', transform('group_by'), 'agg[1]')).toEqual({
      nodeId: 't1',
      key: 'agg',
    })
    expect(resolveIssueField('t1', transform('with_column'), 'columns.base')).toEqual({
      nodeId: 't1',
      key: 'columns',
    })
  })

  it('resolves source options and the run condition', () => {
    expect(resolveIssueField('s1', source(), 'options.versionAsOf')).toEqual({
      nodeId: 's1',
      key: 'versionAsOf',
    })
    expect(resolveIssueField('t1', transform('filter'), 'skip_if_false')).toEqual({
      nodeId: 't1',
      key: 'skip_if_false',
    })
    expect(resolveIssueField('t1', transform('sql'), 'view_name')).toEqual({
      nodeId: 't1',
      key: 'view_name',
    })
  })

  it('accepts an option reported without its scope', () => {
    expect(resolveIssueField('n1', sink(), 'merge_keys')).toEqual({
      nodeId: 'n1',
      key: 'merge_keys',
    })
  })

  it('returns null when no field renders the reported key', () => {
    expect(resolveIssueField('t1', transform('join'), 'with')).toBeNull()
    expect(resolveIssueField('s1', source(), 'mode')).toBeNull()
  })

  it('scopes rule and report paths to the form that renders them', () => {
    const node = validations()
    expect(resolveIssueField('v1', node, 'rules[0].columns')).toEqual({
      nodeId: 'v1-rule0',
      key: 'columns',
    })
    expect(resolveIssueField('v1', node, 'report.path')).toEqual({
      nodeId: 'v1-report',
      key: 'path',
    })
    expect(resolveIssueField('v1', node, 'rules[1].columns')).toBeNull()
  })

  it('returns null for report fields while no report is configured', () => {
    expect(resolveIssueField('v1', validations({ report: null }), 'report.path')).toBeNull()
  })
})
