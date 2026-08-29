import { describe, expect, it } from 'vitest'

import {
  addTag,
  collectTags,
  hasTag,
  MAX_TAGS,
  MAX_TAG_LENGTH,
  normalizeTags,
  removeTag,
} from '@/lib/tags'

/**
 * These rules mirror the server's `normalize_tags` on purpose. A tag is what a
 * bill is aggregated by, so a Studio that stored `Prod` and `prod` as two tags
 * would split a month's spending in half for a reason invisible on the screen.
 */
describe('normalizeTags', () => {
  it('trims and drops what is not a tag', () => {
    expect(normalizeTags([' finance ', '', '   ', 7 as never, null as never])).toEqual([
      'finance',
    ])
  })

  it('deduplicates without caring about case, and keeps the case typed', () => {
    expect(normalizeTags(['Prod', 'prod', 'PROD'])).toEqual(['Prod'])
  })

  it('bounds how many tags one record carries', () => {
    const many = Array.from({ length: MAX_TAGS + 10 }, (_, index) => `tag-${index}`)
    expect(normalizeTags(many)).toHaveLength(MAX_TAGS)
  })

  it('bounds how long a tag is', () => {
    expect(normalizeTags(['x'.repeat(200)])[0]).toHaveLength(MAX_TAG_LENGTH)
  })

  it('answers with nothing for anything that is not a list', () => {
    expect(normalizeTags(undefined)).toEqual([])
    expect(normalizeTags(null)).toEqual([])
  })
})

describe('addTag', () => {
  it('adds a new tag', () => {
    expect(addTag(['finance'], 'nightly')).toEqual(['finance', 'nightly'])
  })

  it('returns the same list when nothing was added', () => {
    const tags = ['finance']
    expect(addTag(tags, 'FINANCE')).toBe(tags)
    expect(addTag(tags, '   ')).toBe(tags)
  })

  it('refuses to go past the ceiling', () => {
    const full = Array.from({ length: MAX_TAGS }, (_, index) => `tag-${index}`)
    expect(addTag(full, 'one-more')).toBe(full)
  })
})

describe('removeTag and hasTag', () => {
  it('matches the way tags are compared', () => {
    expect(removeTag(['Prod', 'finance'], 'prod')).toEqual(['finance'])
    expect(hasTag(['Prod'], ' prod ')).toBe(true)
    expect(hasTag(['Prod'], 'staging')).toBe(false)
  })
})

describe('collectTags', () => {
  it('ranks by use, then alphabetically', () => {
    expect(
      collectTags([
        { tags: ['finance', 'nightly'] },
        { tags: ['finance'] },
        { tags: ['alpha'] },
        {},
      ]),
    ).toEqual(['finance', 'alpha', 'nightly'])
  })

  it('counts one tag once however it was cased', () => {
    expect(collectTags([{ tags: ['Prod'] }, { tags: ['prod'] }])).toEqual(['Prod'])
  })
})
