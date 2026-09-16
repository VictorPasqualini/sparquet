import { describe, expect, it } from 'vitest'

import { lineageExampleTemplates, templateToJob } from '@/data/templates'
import { buildLineage } from '@/lib/lineage'
import {
  buildCatalog,
  catalogStats,
  columnAnnotationOf,
  columnKey,
  columnRaisesClassification,
  effectiveClassification,
  emptyAnnotation,
  isBlank,
  knownDomains,
  MAX_COLUMN_DESCRIPTION,
  MAX_CONNECTION,
  MAX_DESCRIPTION,
  normalizeAnnotation,
  normalizeColumns,
  orphanAnnotations,
  sanitizeAnnotations,
  withAnnotation,
  withColumnAnnotation,
  withoutAnnotation,
  type CatalogAnnotations,
} from './catalog'

const index = () => buildLineage(lineageExampleTemplates().map((t) => templateToJob(t, 'wf')))

const described = (key: string, patch = {}): CatalogAnnotations => ({
  [key]: normalizeAnnotation(key, { description: 'A table.', ...patch }),
})

describe('normalizeAnnotation', () => {
  it('keeps the connection as a bounded name and nothing more', () => {
    const annotation = normalizeAnnotation('/t', {
      connection: `  ${'p'.repeat(MAX_CONNECTION + 10)}  `,
    })

    expect(annotation.connection).toHaveLength(MAX_CONNECTION)
  })

  it('counts a connection on its own as something worth keeping', () => {
    // Naming the credential is a real statement about the dataset, so an
    // annotation that says only that must not be swept away as blank.
    expect(isBlank(normalizeAnnotation('/t', { connection: 'pg-prod' }))).toBe(false)
    expect(isBlank(normalizeAnnotation('/t', { connection: '   ' }))).toBe(true)
  })

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

describe('column annotations', () => {
  const withCpf = (patch = {}) =>
    withColumnAnnotation(described('/t'), '/t', 'CPF', {
      classification: 'restricted',
      tags: ['pii'],
      ...patch,
    })

  it('keys a column by its lower-cased name and keeps the spelling typed', () => {
    const columns = withCpf()['/t'].columns
    expect(Object.keys(columns)).toEqual(['cpf'])
    expect(columns.cpf.column).toBe('CPF')
    expect(columnKey('  Amount  ')).toBe('amount')
  })

  it('finds a column whatever case it is asked about', () => {
    const annotation = withCpf()['/t']
    expect(columnAnnotationOf(annotation, 'cpf')?.classification).toBe('restricted')
    expect(columnAnnotationOf(annotation, 'CpF')?.classification).toBe('restricted')
    expect(columnAnnotationOf(annotation, 'total')).toBeNull()
  })

  it('bounds the description like every other free text in the catalog', () => {
    const annotations = withColumnAnnotation(described('/t'), '/t', 'notes', {
      description: 'x'.repeat(MAX_COLUMN_DESCRIPTION + 40),
    })
    expect(annotations['/t'].columns.notes.description).toHaveLength(MAX_COLUMN_DESCRIPTION)
  })

  it('drops a column that says nothing rather than storing an empty row', () => {
    const annotations = withColumnAnnotation(withCpf(), '/t', 'cpf', {
      classification: '',
      tags: [],
    })
    expect(annotations['/t'].columns).toEqual({})
    // An entry whose columns all went away is still the table's own entry.
    expect(isBlank(annotations['/t'])).toBe(false)
  })

  it('an entry with a described column is not blank even with nothing else in it', () => {
    const bare = withColumnAnnotation({ '/t': emptyAnnotation('/t') }, '/t', 'cpf', {
      classification: 'restricted',
    })
    expect(isBlank(bare['/t'])).toBe(false)
    expect(isBlank(emptyAnnotation('/t'))).toBe(true)
  })

  it('refuses a column with no name at all', () => {
    const annotations = described('/t')
    expect(withColumnAnnotation(annotations, '/t', '   ', { classification: 'public' })).toBe(
      annotations,
    )
  })

  it('reads a stored map back through the same sanitizing as the rest', () => {
    const columns = normalizeColumns({
      CPF: {
        column: 'CPF',
        description: '  The document number.  ',
        classification: 'nonsense',
        tags: ['pii', 'pii'],
      },
      '': { column: '', description: 'x' },
    } as never)
    expect(Object.keys(columns)).toEqual(['cpf'])
    expect(columns.cpf).toMatchObject({
      description: 'The document number.',
      // A classification nobody defined is no classification, not a new one.
      classification: '',
      tags: ['pii'],
    })
  })
})

describe('effectiveClassification', () => {
  it('a table is as restricted as the most restricted column in it', () => {
    const annotations = withColumnAnnotation(
      { '/t': normalizeAnnotation('/t', { classification: 'internal' }) },
      '/t',
      'cpf',
      { classification: 'restricted' },
    )
    expect(effectiveClassification(annotations['/t'])).toBe('restricted')
    expect(annotations['/t'].classification).toBe('internal')
  })

  it('a column never lowers what the table itself says', () => {
    const annotations = withColumnAnnotation(
      { '/t': normalizeAnnotation('/t', { classification: 'confidential' }) },
      '/t',
      'id',
      { classification: 'public' },
    )
    expect(effectiveClassification(annotations['/t'])).toBe('confidential')
  })

  it('says nothing about a table nobody classified and whose columns are silent', () => {
    expect(effectiveClassification(emptyAnnotation('/t'))).toBe('')
    expect(effectiveClassification(null)).toBe('')
  })

  it('points at the column that raises the badge, and only at that one', () => {
    const annotation = normalizeAnnotation('/t', { classification: 'internal' })
    expect(
      columnRaisesClassification(annotation, {
        column: 'cpf',
        description: '',
        classification: 'restricted',
        tags: [],
      }),
    ).toBe(true)
    expect(
      columnRaisesClassification(annotation, {
        column: 'id',
        description: '',
        classification: 'public',
        tags: [],
      }),
    ).toBe(false)
    // A classified column inside an unclassified table raises it too: the table
    // said nothing, and nothing is not the same as "less than restricted".
    expect(
      columnRaisesClassification(emptyAnnotation('/t'), {
        column: 'cpf',
        description: '',
        classification: 'internal',
        tags: [],
      }),
    ).toBe(true)
  })
})

describe('catalogStats with columns', () => {
  it('counts the described columns across the whole catalog', () => {
    const annotations = withColumnAnnotation(
      withColumnAnnotation(described('/lake/silver/orders'), '/lake/silver/orders', 'cpf', {
        classification: 'restricted',
      }),
      '/lake/silver/orders',
      'total',
      { description: 'Order total, in cents.' },
    )
    // Described is not classified: the number beside the coverage counts the
    // columns somebody decided who may see, not the ones somebody explained.
    expect(catalogStats(buildCatalog(index(), annotations)).columnsClassified).toBe(1)
  })
})
