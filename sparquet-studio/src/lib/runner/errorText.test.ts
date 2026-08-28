import { describe, expect, it } from 'vitest'

import { isErrorText, sameErrorText } from '@/lib/runner/errorText'

describe('isErrorText', () => {
  it('treats blank as no error at all', () => {
    expect(isErrorText('boom')).toBe(true)
    expect(isErrorText('   ')).toBe(false)
    expect(isErrorText('')).toBe(false)
    expect(isErrorText(null)).toBe(false)
    expect(isErrorText(undefined)).toBe(false)
  })
})

describe('sameErrorText', () => {
  it('matches the same failure quoted at a different level', () => {
    const step = 'Path does not exist: file:/data/in.csv'
    expect(sameErrorText(step, `Stage ingest failed: ${step}`)).toBe(true)
    expect(sameErrorText(`Stage ingest failed: ${step}`, step)).toBe(true)
  })

  it('ignores how the message was wrapped on the way up', () => {
    expect(sameErrorText('Path does\n  not exist', 'Path does not exist')).toBe(true)
  })

  it('keeps two different failures apart', () => {
    expect(sameErrorText('Path does not exist', 'Column orders.id is missing')).toBe(false)
  })

  it('never calls a missing message a match', () => {
    // Or a run with no error would silence the step that has one.
    expect(sameErrorText(null, 'boom')).toBe(false)
    expect(sameErrorText('boom', '')).toBe(false)
    expect(sameErrorText(null, null)).toBe(false)
  })
})
