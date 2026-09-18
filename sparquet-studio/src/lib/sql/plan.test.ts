import { describe, expect, it } from 'vitest'

import { countNodes, isPlanResult, parsePlan, toneOf } from './plan'

/** A physical plan as Spark prints one, indentation and connectors included. */
const PHYSICAL = `== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[day#12], functions=[sum(amount#8)])
   +- Exchange hashpartitioning(day#12, 200), ENSURE_REQUIREMENTS
      +- HashAggregate(keys=[day#12], functions=[partial_sum(amount#8)])
         +- Project [day#12, amount#8]
            +- FileScan parquet [day#12,amount#8] Batched: true`

describe('parsePlan', () => {
  it('turns the indentation back into a tree', () => {
    const [section] = parsePlan(PHYSICAL)
    expect(section.title).toBe('Physical Plan')
    expect(section.roots).toHaveLength(1)
    expect(countNodes(section.roots)).toBe(6)

    const root = section.roots[0]
    expect(root.operator).toBe('AdaptiveSparkPlan')
    expect(root.detail).toBe('isFinalPlan=false')

    const aggregate = root.children[0]
    expect(aggregate.operator).toBe('HashAggregate')
    expect(aggregate.children[0].operator).toBe('Exchange')

    // Every node has exactly one child here, so the deepest is the scan.
    let node = root
    while (node.children.length > 0) node = node.children[0]
    expect(node.operator).toBe('FileScan parquet')
  })

  it('keeps each plan of an EXPLAIN EXTENDED as its own section', () => {
    const sections = parsePlan(`== Parsed Logical Plan ==
Project [id#1]
+- Relation[id#1] parquet

== Physical Plan ==
FileScan parquet [id#1]`)
    expect(sections.map((section) => section.title)).toEqual([
      'Parsed Logical Plan',
      'Physical Plan',
    ])
    expect(sections[1].roots[0].operator).toBe('FileScan parquet')
  })

  it('folds a wrapped argument list into the node above it', () => {
    const [section] = parsePlan(`== Physical Plan ==
FileScan parquet [a#1,b#2]
PushedFilters: [IsNotNull(a)]`)
    expect(section.roots).toHaveLength(1)
    expect(section.roots[0].detail).toContain('PushedFilters')
  })

  it('gives back nothing for text that is not a plan', () => {
    expect(parsePlan('')).toEqual([])
  })
})

describe('toneOf', () => {
  it('marks the two operators a plan is read for', () => {
    expect(toneOf('Exchange')).toBe('shuffle')
    expect(toneOf('FileScan parquet')).toBe('scan')
    expect(toneOf('BroadcastExchange')).toBe('broadcast')
    expect(toneOf('SortMergeJoin')).toBe('join')
    expect(toneOf('Project')).toBeNull()
  })
})

describe('isPlanResult', () => {
  it('recognises the single cell of text an EXPLAIN answers with', () => {
    expect(isPlanResult(['plan'], [['== Physical Plan ==\nFileScan']])).toBe(true)
    expect(isPlanResult(['id'], [['1']])).toBe(false)
    expect(isPlanResult(['plan'], [['a'], ['b']])).toBe(false)
  })
})
