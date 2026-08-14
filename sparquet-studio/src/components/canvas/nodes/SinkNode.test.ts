import { describe, expect, it } from 'vitest'

import { projectionLabel } from './SinkNode'

describe('projectionLabel', () => {
  it('says nothing when no projection is configured', () => {
    expect(projectionLabel(null)).toBeNull()
  })

  it('reports the full write for an empty list, which the compiler drops', () => {
    expect(projectionLabel([])).toBe('Writes every column')
  })

  it('counts the projected columns', () => {
    expect(projectionLabel(['id'])).toBe('Projects 1 column')
    expect(projectionLabel(['id', 'total'])).toBe('Projects 2 columns')
  })
})
