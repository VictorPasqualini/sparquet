import { describe, expect, it } from 'vitest'

import {
  ancestorIds,
  buildNamespaceTree,
  describeAsset,
  kindOfFormats,
  type CatalogAsset,
} from './namespace'

const asset = (key: string, ...formats: string[]) => describeAsset(key, formats)

describe('kindOfFormats', () => {
  it('reads the storage layer, not the file layout', () => {
    expect(kindOfFormats(['delta'])).toBe('table')
    expect(kindOfFormats(['iceberg'])).toBe('table')
    expect(kindOfFormats(['parquet'])).toBe('directory')
    expect(kindOfFormats(['kafka'])).toBe('topic')
    expect(kindOfFormats(['view'])).toBe('view')
    expect(kindOfFormats(['mongodb'])).toBe('collection')
    expect(kindOfFormats(['elasticsearch'])).toBe('index')
    // Nothing known about it: a directory is the safe guess.
    expect(kindOfFormats(['something-new'])).toBe('directory')
  })

  it('lets the table win when one dataset is named by two formats', () => {
    expect(kindOfFormats(['parquet', 'delta'])).toBe('table')
    expect(kindOfFormats(['delta', 'parquet'])).toBe('table')
  })
})

describe('describeAsset', () => {
  it('reads a dotted address as catalog, database, table', () => {
    expect(asset('analytics.gold.revenue_by_country', 'delta')).toMatchObject({
      kind: 'table',
      root: 'analytics',
      rootKind: 'catalog',
      namespace: ['gold'],
      name: 'revenue_by_country',
    })
  })

  it('puts a table with no catalog in front of it under default', () => {
    expect(asset('orders', 'iceberg')).toMatchObject({
      root: 'default',
      rootKind: 'catalog',
      namespace: [],
      name: 'orders',
    })
  })

  it('does NOT split the dots of a file — orders.csv is not a table called csv', () => {
    expect(asset('orders.csv', 'csv')).toMatchObject({
      kind: 'directory',
      rootLabel: '.',
      name: 'orders.csv',
    })
  })

  it('reads an absolute path as bucket and folders', () => {
    expect(asset('/lake/bronze/orders', 'parquet')).toMatchObject({
      kind: 'directory',
      root: '/lake',
      rootLabel: 'lake',
      rootKind: 'bucket',
      namespace: ['bronze'],
      name: 'orders',
    })
  })

  it('keeps a Delta table on a path a table, hanging off the bucket', () => {
    expect(asset('/lake/gold/revenue', 'delta')).toMatchObject({
      kind: 'table',
      root: '/lake',
      rootKind: 'bucket',
      namespace: ['gold'],
      name: 'revenue',
    })
  })

  it('reads an object-store URI as its own bucket', () => {
    expect(asset('s3://warehouse/bronze/orders', 'parquet')).toMatchObject({
      root: 's3://warehouse',
      rootLabel: 'warehouse',
      rootKind: 'bucket',
      scheme: 's3',
      namespace: ['bronze'],
      name: 'orders',
    })
    expect(asset('abfss://data@acct.dfs.core.windows.net/silver/x', 'parquet')).toMatchObject({
      rootKind: 'bucket',
      namespace: ['silver'],
      name: 'x',
    })
  })

  it('names the bucket itself when the URI stops there', () => {
    expect(asset('s3://warehouse', 'parquet')).toMatchObject({
      root: 's3://warehouse',
      namespace: [],
      name: 'warehouse',
    })
  })

  it('peels the stacked jdbc scheme and calls the server a catalog', () => {
    expect(asset('jdbc:postgresql://db:5432/app', 'postgresql')).toMatchObject({
      kind: 'table',
      root: 'jdbc:postgresql://db:5432',
      rootLabel: 'db:5432',
      rootKind: 'catalog',
      scheme: 'jdbc:postgresql',
      name: 'app',
    })
  })

  it('treats the dots in a topic name as part of the name', () => {
    expect(asset('orders.v1', 'kafka')).toMatchObject({
      kind: 'topic',
      root: 'kafka',
      rootKind: 'stream',
      namespace: [],
      name: 'orders.v1',
    })
  })

  it('parks a temporary view under the session it only lives in', () => {
    expect(asset('orders_clean', 'view')).toMatchObject({
      kind: 'view',
      root: 'session',
      rootKind: 'session',
      name: 'orders_clean',
    })
  })

  it('hangs a single-segment path off the filesystem root', () => {
    expect(asset('/orders', 'parquet')).toMatchObject({
      root: '/',
      rootLabel: '/',
      namespace: [],
      name: 'orders',
    })
  })
})

describe('buildNamespaceTree', () => {
  const assets: CatalogAsset[] = [
    asset('/lake/bronze/orders', 'parquet'),
    asset('/lake/silver/orders', 'parquet'),
    asset('/lake/silver/orders_backfill', 'parquet'),
    asset('analytics.gold.revenue', 'delta'),
    asset('orders.v1', 'kafka'),
  ]

  it('nests folders under their bucket and counts the whole subtree', () => {
    const [lake] = buildNamespaceTree(assets)
    expect(lake.label).toBe('lake')
    expect(lake.rootKind).toBe('bucket')
    expect(lake.count).toBe(3)
    expect(lake.children.map((child) => child.label)).toEqual(['bronze', 'silver'])
    expect(lake.children[1].assets.map((a) => a.name)).toEqual(['orders', 'orders_backfill'])
  })

  it('orders roots by kind first so a bucket never interleaves with a catalog', () => {
    expect(buildNamespaceTree(assets).map((node) => node.label)).toEqual([
      'lake',
      'analytics',
      'kafka',
    ])
  })

  it('gives every node an id unique to its full path', () => {
    const ids = buildNamespaceTree(assets).flatMap((root) => [
      root.id,
      ...root.children.map((child) => child.id),
    ])
    expect(ids).toContain('/lake/silver')
    expect(new Set(ids).size).toBe(ids.length)
  })
})

describe('ancestorIds', () => {
  it('lists the node ids a search has to expand to reveal an asset', () => {
    expect(ancestorIds(asset('/lake/silver/curated/orders', 'parquet'))).toEqual([
      '/lake',
      '/lake/silver',
      '/lake/silver/curated',
    ])
  })
})
