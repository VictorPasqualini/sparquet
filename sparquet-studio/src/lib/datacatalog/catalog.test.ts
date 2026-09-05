import { describe, expect, it } from 'vitest'

import { lineageExampleTemplates, templateToJob } from '@/data/templates'
import { buildLineage } from '@/lib/lineage'
import {
  buildCatalog,
  catalogStats,
  emptyAnnotation,
  isBlank,
  knownDomains,
  MAX_DESCRIPTION,
  normalizeAnnotation,
  orphanAnnotations,
  sanitizeAnnotations,
  withAnnotation,
  withoutAnnotation,
  type CatalogAnnotations,
} from './catalog'

const index = () => buildLineage(lineageExampleTemplates().map((t) => templateToJob(t, 'wf')))

const described = (key: string, patch = {}): CatalogAnnotations => ({
  [key]: normalizeAnnotation(key, { description: 'A table.', ...patch }),
})

describe('normalizeAnnotation', () => {
  it('trims, bounds and drops a classification it does not know', () => {
    const annotation = normalizeAnnotation('/t', {
      description: `  ${'x'.repeat(MAX_DESCRIPTION + 40)}  `,
      owner: '  data-platform  ',
      classification: 'top-secret' as never,
      tags: ['Core', 'core', '  ', 'pii'],
    })

    expect(annotation.description).toHaveLength(MAX_DESCRIPTION)
    expect(annotation.owner).toBe('data-platform')
    expect(annotation.classification).toBe('')
    // Tags are deduplicated case-insensitively and keep the case they were typed in.
    expect(annotation.tags).toEqual(['Core', 'pii'])
  })

  it('merges onto what is already there instead of replacing it', () => {
    const base = normalizeAnnotation('/t', { description: 'Orders.', owner: 'sales' })
    expect(normalizeAnnotation('/t', { owner: 'finance' }, base)).toMatchObject({
      description: 'Orders.',
      owner: 'finance',
    })
  })
})

describe('withAnnotation', () => {
  it('writes an entry and leaves the rest of the map alone', () => {
    const before = described('/a')
    const after = withAnnotation(before, '/b', { owner: 'me' })

    expect(Object.keys(after)).toEqual(['/a', '/b'])
    expect(before['/b']).toBeUndefined()
  })

  it('deletes the entry when the last field is cleared', () => {
    const before = described('/a')
    expect(withAnnotation(before, '/a', { description: '   ' })).toEqual({})
  })

  it('is a no-op for a key that was never annotated', () => {
    const before = described('/a')
    expect(withoutAnnotation(before, '/b')).toBe(before)
  })
})

describe('isBlank', () => {
  it('is what makes an empty entry not count as documented', () => {
    expect(isBlank(emptyAnnotation('/t'))).toBe(true)
    expect(isBlank(normalizeAnnotation('/t', { tags: ['pii'] }))).toBe(false)
  })
})

describe('sanitizeAnnotations', () => {
  it('keeps the stored timestamp instead of stamping the read', () => {
    const parsed = sanitizeAnnotations({
      '/t': { key: '/t', description: 'Orders.', updatedAt: 1700000000000 },
    })
    expect(parsed['/t'].updatedAt).toBe(1700000000000)
  })

  it('survives anything a hand-edited file can hold', () => {
    expect(sanitizeAnnotations(null)).toEqual({})
    expect(sanitizeAnnotations(['/t'])).toEqual({})
    expect(sanitizeAnnotations({ '/t': 'orders' })).toEqual({})
    // An entry with nothing in it is dropped rather than counted as documented.
    expect(sanitizeAnnotations({ '/t': { description: '' } })).toEqual({})
  })
})

describe('buildCatalog', () => {
  it('joins the annotations onto the datasets by address', () => {
    const entries = buildCatalog(index(), described('/lake/silver/orders', { owner: 'sales' }))
    const silver = entries.find((entry) => entry.dataset.key === '/lake/silver/orders')
    const gold = entries.find((entry) => entry.dataset.key === '/lake/gold/revenue_by_country')

    expect(silver?.documented).toBe(true)
    expect(silver?.owned).toBe(true)
    expect(gold?.annotation).toBeNull()
    expect(gold?.documented).toBe(false)
  })

  it('counts coverage over the datasets in use, not over the entries written', () => {
    const stats = catalogStats(buildCatalog(index(), described('/lake/silver/orders')))
    expect(stats).toMatchObject({ total: 9, documented: 1, owned: 0, classified: 0 })
    expect(stats.coverage).toBeCloseTo(1 / 9)
  })
})

describe('orphanAnnotations', () => {
  it('reports an entry whose address no Job mentions any more', () => {
    const annotations = {
      ...described('/lake/silver/orders'),
      ...described('/lake/silver/order'),
    }
    expect(orphanAnnotations(index(), annotations).map((a) => a.key)).toEqual([
      '/lake/silver/order',
    ])
  })
})

describe('knownDomains', () => {
  it('offers each domain once, sorted', () => {
    const annotations = {
      ...described('/a', { domain: 'sales' }),
      ...described('/b', { domain: 'finance' }),
      ...described('/c', { domain: 'sales' }),
    }
    expect(knownDomains(annotations)).toEqual(['finance', 'sales'])
  })
})
