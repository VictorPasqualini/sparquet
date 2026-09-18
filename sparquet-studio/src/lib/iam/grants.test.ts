/**
 * The browser half of the access model, pinned against the runner's.
 *
 * Every case here has a twin in `server/test_grants.py`. That is the point of
 * the file: the two implementations answer the same question, and the failure
 * mode nobody notices is them drifting apart — the screen offers a button the
 * runner then refuses, or greys one out that would have worked.
 */

import { describe, expect, it } from 'vitest'

import {
  allows,
  ancestors,
  columnParents,
  columnResource,
  columnScopeChain,
  decide,
  parseColumnResource,
  effectiveOwner,
  mayAdminister,
  owns,
  sanitizeOwners,
  scopeChain,
  tagScopes,
  withoutOwner,
  withOwner,
  type Grant,
  type Identity,
  type Owner,
  type ResourceKind,
} from '@/lib/iam'

let counter = 0

function grant(
  resource: ResourceKind,
  resourceId: string,
  principalId: string,
  level: Grant['level'],
  effect: Grant['effect'] = 'allow',
  principalKind: Grant['principalKind'] = 'team',
): Grant {
  counter += 1
  return {
    id: `g${counter}`,
    resource,
    resourceId,
    principalKind,
    principalId,
    level,
    effect,
    updatedAt: counter,
  }
}

function owner(
  resource: ResourceKind,
  resourceId: string,
  principalId: string,
  principalKind: Owner['principalKind'] = 'team',
): Owner {
  return { resource, resourceId, principalKind, principalId, updatedAt: 1 }
}

const ANA: Identity = { userId: 'u1', username: 'ana', teamId: 't1' }
const BRUNO: Identity = { userId: 'u2', username: 'bruno', teamId: 't2' }

describe('ancestors', () => {
  it('walks a path upwards, nearest first, without the resource itself', () => {
    expect(ancestors('/lake/silver/orders')).toEqual(['/lake/silver', '/lake'])
  })

  it('treats a qualified name the way it treats a path', () => {
    expect(ancestors('main.sales.orders')).toEqual(['main.sales', 'main'])
  })

  it('keeps a URL scheme out of the chain', () => {
    // `s3:/bucket` is not the same bucket, and `s3:` is nothing anyone grants on.
    expect(ancestors('s3://bucket/main.db/orders')).toEqual(['s3://bucket/main.db', 's3://bucket'])
  })

  it('gives a single segment no ancestors at all', () => {
    expect(ancestors('orders')).toEqual([])
    expect(ancestors('*')).toEqual([])
  })
})

describe('scopeChain', () => {
  it('ends every kind with its wildcard', () => {
    expect(scopeChain('dataset', '/lake/silver/orders')).toEqual([
      ['dataset', '/lake/silver/orders'],
      ['dataset', '/lake/silver'],
      ['dataset', '/lake'],
      ['dataset', '*'],
    ])
  })

  it('appends the container a Job was told it lives in', () => {
    expect(scopeChain('job', 'j1', [['workflow', 'w1']])).toEqual([
      ['job', 'j1'],
      ['job', '*'],
      ['workflow', 'w1'],
      ['workflow', '*'],
    ])
  })
})

describe('inheritance', () => {
  it('lets a rule on the folder reach the table, and says where it came from', () => {
    const grants = [grant('dataset', '/lake/silver', 't1', 'read')]

    const decision = decide(grants, 'dataset', '/lake/silver/orders', ANA)

    expect(decision.governed).toBe(true)
    expect(decision.level).toBe('read')
    expect(decision.source).toBe('dataset//lake/silver')
  })

  it('keeps the best level anywhere in the chain', () => {
    const grants = [
      grant('dataset', '/lake', 't1', 'read'),
      grant('dataset', '/lake/silver/orders', 't1', 'write'),
    ]

    expect(decide(grants, 'dataset', '/lake/silver/orders', ANA).level).toBe('write')
  })

  it('lets a deny on the parent close the child', () => {
    const grants = [
      grant('dataset', '/lake/silver/orders', 't1', 'write'),
      grant('dataset', '/lake', 't1', 'read', 'deny'),
    ]

    const decision = decide(grants, 'dataset', '/lake/silver/orders', ANA)

    expect(decision.level).toBeNull()
    expect(decision.source).toBeNull()
  })

  it('leaves a resource ungoverned when only a sibling has a rule', () => {
    const grants = [grant('dataset', '/lake/gold/orders', 't1', 'read')]

    expect(decide(grants, 'dataset', '/lake/silver/orders', ANA).governed).toBe(false)
    expect(allows(grants, 'dataset', '/lake/silver/orders', ANA, 'read')).toBe(true)
  })

  it('lets a Job inherit from the Workflow it belongs to', () => {
    const grants = [grant('workflow', 'w1', 't1', 'write')]

    const decision = decide(grants, 'job', 'j1', ANA, [], [['workflow', 'w1']])

    expect(decision.level).toBe('write')
    expect(decision.source).toBe('workflow/w1')
  })
})

describe('ownership', () => {
  it('holds admin with no grant anywhere', () => {
    const decision = decide([], 'job', 'j1', ANA, [owner('job', 'j1', 't1')])

    expect(decision.owned).toBe(true)
    expect(decision.level).toBe('admin')
    expect(decision.source).toBe('job/j1')
  })

  it('does not let a deny reach the owner, and still lets it reach everyone else', () => {
    const owners = [owner('job', 'j1', 't1')]
    const grants = [
      grant('job', 'j1', '*', 'read', 'deny'),
      grant('job', 'j1', 't2', 'write'),
    ]

    expect(decide(grants, 'job', 'j1', ANA, owners).level).toBe('admin')
    expect(decide(grants, 'job', 'j1', BRUNO, owners).level).toBeNull()
  })

  it('owns the contents by owning the container', () => {
    const owners = [owner('workflow', 'w1', 't1')]

    expect(owns(owners, 'job', 'j1', ANA, [['workflow', 'w1']])).toBe('workflow/w1')
    expect(decide([], 'job', 'j1', ANA, owners, [['workflow', 'w1']]).level).toBe('admin')
  })

  it('recognises a user owner as well as a team one', () => {
    const owners = [owner('dataset', '/lake/silver/orders', 'u1', 'user')]

    expect(decide([], 'dataset', '/lake/silver/orders', ANA, owners).owned).toBe(true)
    expect(decide([], 'dataset', '/lake/silver/orders', BRUNO, owners).owned).toBe(false)
  })

  it('governs the resource for everyone else the moment an owner is named', () => {
    // Otherwise declaring an owner would leave the thing wide open, which is the
    // opposite of what naming one means.
    const owners = [owner('job', 'j1', 't1')]

    const decision = decide([], 'job', 'j1', BRUNO, owners)

    expect(decision.governed).toBe(true)
    expect(decision.level).toBeNull()
    expect(allows([], 'job', 'j1', BRUNO, 'read', true, owners)).toBe(false)
  })

  it('reads back the map shape the workspace may hold, and drops what is malformed', () => {
    const read = sanitizeOwners({
      'job:j1': { resource: 'job', resourceId: 'j1', principalKind: 'team', principalId: 't1' },
      'job:j2': { resource: 'nonsense', resourceId: 'j2', principalKind: 'team', principalId: 't1' },
      'job:j3': { resource: 'job', resourceId: '', principalKind: 'team', principalId: 't1' },
    })

    expect(read).toHaveLength(1)
    expect(read[0].resourceId).toBe('j1')
  })

  it('keeps one owner per resource when a second is recorded', () => {
    const first = withOwner([], owner('job', 'j1', 't1'))
    const second = withOwner(first, owner('job', 'j1', 't2'))

    expect(second).toHaveLength(1)
    expect(second[0].principalId).toBe('t2')
    expect(withoutOwner(second, 'job', 'j1')).toEqual([])
  })
})

describe('effectiveOwner', () => {
  it('prefers the record on the resource itself', () => {
    const owners = [owner('dataset', '/lake', 't2'), owner('dataset', '/lake/silver/orders', 't1')]

    const found = effectiveOwner(owners, 'dataset', '/lake/silver/orders')

    expect(found?.owner.principalId).toBe('t1')
    expect(found?.source).toBe('dataset//lake/silver/orders')
  })

  it('falls back to the nearest container, naming where it sits', () => {
    const owners = [owner('dataset', '/lake', 't2')]

    expect(effectiveOwner(owners, 'dataset', '/lake/silver/orders')?.source).toBe('dataset//lake')
  })

  it('answers null when nothing in the chain is owned', () => {
    expect(effectiveOwner([], 'dataset', '/lake/silver/orders')).toBeNull()
  })
})

describe('mayAdminister', () => {
  it('lets the owner write the rules without any platform role', () => {
    expect(mayAdminister([], [owner('job', 'j1', 't1')], 'job', 'j1', ANA)).toBe(true)
  })

  it('lets an admin grant do the same', () => {
    expect(mayAdminister([grant('job', 'j1', 't1', 'admin')], [], 'job', 'j1', ANA)).toBe(true)
  })

  it('refuses a write grant, and refuses an ungoverned resource', () => {
    expect(mayAdminister([grant('job', 'j1', 't1', 'write')], [], 'job', 'j1', ANA)).toBe(false)
    // Ungoverned is "the action-level policy decides", not "anybody may re-grant".
    expect(mayAdminister([], [], 'job', 'j1', ANA)).toBe(false)
  })
})

describe('tagScopes', () => {
  it('normalizes what somebody typed in the catalog', () => {
    // Twin of `test_tags_are_normalized_on_both_sides` in server/test_grants.py:
    // a tag typed with capitals has to meet a rule written in lower case.
    expect(tagScopes([' PII ', 'Finance'])).toEqual([
      ['tag', 'pii'],
      ['tag', 'finance'],
    ])
  })

  it('drops blanks, duplicates and the wildcard', () => {
    // `*` as a resource id means "every tag", which is a rule, not a label a
    // dataset may claim for itself.
    expect(tagScopes(['pii', 'PII', '', '  ', '*'])).toEqual([['tag', 'pii']])
  })

  it('carries classification and domain as tags of their own', () => {
    expect(tagScopes([], { classification: 'confidential', domain: 'sales' })).toEqual([
      ['tag', 'classification:confidential'],
      ['tag', 'domain:sales'],
    ])
    expect(tagScopes([], { classification: '', domain: null })).toEqual([])
  })
})

describe('grants written on a tag', () => {
  const tags = tagScopes(['pii'], { domain: 'sales' })

  it('reaches every dataset the catalog tags with it', () => {
    const rules = [grant('tag', 'pii', 't1', 'read')]

    expect(allows(rules, 'dataset', '/data/orders', ANA, 'read', true, [], tags)).toBe(true)
    // And no further: a table without the tag is not governed by that rule.
    expect(decide(rules, 'dataset', '/data/holidays', ANA, [], []).governed).toBe(false)
  })

  it('names the tag as the source, so a screen can say which rule answered', () => {
    const rules = [grant('tag', 'domain:sales', 't1', 'write')]

    expect(decide(rules, 'dataset', '/data/orders', ANA, [], tags).source).toBe('tag/domain:sales')
  })

  it('lets a tag deny close a table a path rule opened', () => {
    const rules = [
      grant('dataset', '/data', 't1', 'admin'),
      grant('tag', 'pii', 't1', 'read', 'deny'),
    ]

    expect(decide(rules, 'dataset', '/data/orders', ANA, [], tags).level).toBeNull()
    // The sibling without the tag keeps what the path rule gave it.
    expect(decide(rules, 'dataset', '/data/holidays', ANA, [], []).level).toBe('admin')
  })

  it('refuses to let a tag be owned', () => {
    // Ownership is undeniable admin. Handing it out over a label anybody may
    // type onto a table would be handing out admin over tables never seen.
    expect(sanitizeOwners([owner('tag', 'pii', 't1')])).toEqual([])
  })
})

describe('columns as securables', () => {
  const CPF = columnResource('/data/orders', 'cpf')

  it('addresses a column under its table, lower-cased', () => {
    expect(columnResource('main.silver.orders', 'CPF')).toBe('main.silver.orders#cpf')
    expect(columnResource('', 'cpf')).toBe('')
    expect(columnResource('/data/orders', '  ')).toBe('')
  })

  it('reads an address back into its two halves', () => {
    expect(parseColumnResource('s3://bucket/orders#cpf')).toEqual({
      key: 's3://bucket/orders',
      column: 'cpf',
    })
    // A table, a dangling separator and a name with nothing before it are all
    // "not a column", never "a column called nothing".
    expect(parseColumnResource('/data/orders')).toBeNull()
    expect(parseColumnResource('/data/orders#')).toBeNull()
    expect(parseColumnResource('#cpf')).toBeNull()
  })

  it('reaches the table above it without inventing a folder of columns', () => {
    const chain = scopeChain('column', '/lake/silver/orders#cpf')
    expect(chain[0]).toEqual(['column', '/lake/silver/orders#cpf'])
    expect(chain).toContainEqual(['column', '*'])
    expect(chain).toContainEqual(['dataset', '/lake/silver/orders'])
    expect(chain).toContainEqual(['dataset', '/lake/silver'])
    expect(chain.filter(([kind]) => kind === 'column').map(([, id]) => id)).toEqual([
      '/lake/silver/orders#cpf',
      '*',
    ])
  })

  it('carries the column tags first and the table tags after', () => {
    expect(
      columnParents({
        columnTags: ['pii'],
        columnClassification: 'restricted',
        datasetTags: ['pii', 'finance'],
        datasetClassification: 'internal',
        datasetDomain: 'sales',
      }),
    ).toEqual([
      ['tag', 'pii'],
      ['tag', 'classification:restricted'],
      ['tag', 'finance'],
      ['tag', 'classification:internal'],
      ['tag', 'domain:sales'],
    ])
  })

  it('closes one column of a table the reader may otherwise write', () => {
    const rules = [
      grant('dataset', '/data/orders', 't1', 'write'),
      grant('column', CPF, 't1', 'read', 'deny'),
    ]

    expect(allows(rules, 'dataset', '/data/orders', ANA, 'write')).toBe(true)
    expect(allows(rules, 'column', CPF, ANA, 'read')).toBe(false)
    // Every other column of the same table is untouched.
    expect(allows(rules, 'column', columnResource('/data/orders', 'total'), ANA, 'write')).toBe(
      true,
    )
  })

  it('lets a rule on the column tag close every column that wears it', () => {
    const rules = [
      grant('dataset', '/data/orders', 't1', 'write'),
      grant('tag', 'pii', 't1', 'read', 'deny'),
    ]
    const parents = columnParents({ columnTags: ['pii'] })

    expect(allows(rules, 'column', CPF, ANA, 'read', true, [], parents)).toBe(false)
    // And the chain that builds itself from the same tags says the same thing.
    expect(columnScopeChain('/data/orders', 'cpf', { columnTags: ['pii'] })).toContainEqual([
      'tag',
      'pii',
    ])
    // The table carries no such tag, so it stays open.
    expect(allows(rules, 'dataset', '/data/orders', ANA, 'write')).toBe(true)
  })

  it('does not let a column rule widen what the table refuses', () => {
    // Cumulative levels run one way, and a deny wins anywhere in the chain:
    // granting a column cannot open a table that is closed above it.
    const rules = [
      grant('dataset', '/data', 't1', 'read', 'deny'),
      grant('column', CPF, 't1', 'admin'),
    ]
    expect(allows(rules, 'column', CPF, ANA, 'read')).toBe(false)
  })

  it('refuses to let a column be owned', () => {
    // A column is handed over with its table; ownership is admin no deny can
    // reach, and nobody owns half a row.
    expect(sanitizeOwners([owner('column', CPF, 't1')])).toEqual([])
  })

  it('keeps another team out of a column granted to this one', () => {
    const rules = [grant('column', CPF, 't1', 'read')]
    expect(decide(rules, 'column', CPF, ANA).level).toBe('read')
    expect(decide(rules, 'column', CPF, BRUNO).level).toBeNull()
    expect(mayAdminister(rules, [], 'column', CPF, ANA)).toBe(false)
  })
})
