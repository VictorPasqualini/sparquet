import { describe, expect, it } from 'vitest'

import { HANDLE, type StudioEdge, type StudioNode } from '@/types/studio'

import { freeTargetHandle } from './JobCanvas'

const transform = (id: string, type: string): StudioNode => ({
  id,
  type: 'transform',
  position: { x: 0, y: 0 },
  data: { kind: 'transform', transform: type, params: {} },
})

const edgeInto = (target: string, targetHandle: string): StudioEdge => ({
  id: `e-${target}-${targetHandle}`,
  source: 'upstream',
  target,
  targetHandle,
})

describe('freeTargetHandle', () => {
  it('always uses the single input of a one-input transform', () => {
    const node = transform('filter-1', 'filter')
    expect(freeTargetHandle(node, [])).toBe(HANDLE.in)
    expect(freeTargetHandle(node, [edgeInto('filter-1', HANDLE.in)])).toBe(HANDLE.in)
  })

  it('fills the left input of a join first', () => {
    expect(freeTargetHandle(transform('join-1', 'join'), [])).toBe(HANDLE.in)
  })

  it('spills into the right input once the left one is taken', () => {
    const edges = [edgeInto('join-1', HANDLE.in)]
    expect(freeTargetHandle(transform('join-1', 'join'), edges)).toBe(HANDLE.inRight)
    expect(freeTargetHandle(transform('union-1', 'union'), edges)).toBe(HANDLE.in)
  })

  it('reads an edge with no explicit handle as the left input', () => {
    const edges: StudioEdge[] = [{ id: 'e1', source: 'upstream', target: 'join-1' }]
    expect(freeTargetHandle(transform('join-1', 'join'), edges)).toBe(HANDLE.inRight)
  })
})
