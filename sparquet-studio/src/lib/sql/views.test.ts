import { describe, expect, it } from 'vitest'

import { mentionedAliases, viewAlias } from './views'

describe('viewAlias', () => {
  it('keeps the layer, because the table name alone repeats across layers', () => {
    expect(viewAlias('/lake/silver/orders')).toBe('silver_orders')
    expect(viewAlias('/lake/bronze/orders')).toBe('bronze_orders')
  })

  it('drops schemes and drive letters, which name no folder', () => {
    expect(viewAlias('s3://bucket/raw/orders')).toBe('raw_orders')
    expect(viewAlias('E:/lake/gold/revenue')).toBe('gold_revenue')
  })

  it('replaces whatever Spark would not accept in a name', () => {
    expect(viewAlias('/lake/silver/order-items 2024')).toBe('silver_order_items_2024')
  })

  it('never starts with a digit', () => {
    expect(viewAlias('/lake/2024/orders')).toBe('_2024_orders')
  })

  it('resolves a collision instead of shadowing the first dataset', () => {
    const first = viewAlias('/lake/silver/orders')
    const second = viewAlias('s3://other/silver/orders', new Set([first]))
    expect(second).toBe('silver_orders_2')
  })

  it('falls back to a name when the address has nothing usable', () => {
    expect(viewAlias('///')).toBe('dataset')
  })
})

describe('mentionedAliases', () => {
  const aliases = ['silver_orders', 'bronze_orders', 'gold_revenue']

  it('returns only what the SQL names', () => {
    expect(mentionedAliases('select * from silver_orders', aliases)).toEqual(['silver_orders'])
  })

  it('matches whole words, so one alias does not drag in another', () => {
    expect(mentionedAliases('select * from orders_extra', aliases)).toEqual([])
  })

  it('ignores case, as SQL does', () => {
    expect(mentionedAliases('SELECT * FROM GOLD_REVENUE', aliases)).toEqual(['gold_revenue'])
  })

  it('finds every dataset a join names', () => {
    const sql = 'select * from silver_orders s join bronze_orders b on s.id = b.id'
    expect(mentionedAliases(sql, aliases)).toEqual(['silver_orders', 'bronze_orders'])
  })

  it('says nothing about an empty query', () => {
    expect(mentionedAliases('   ', aliases)).toEqual([])
  })
})
