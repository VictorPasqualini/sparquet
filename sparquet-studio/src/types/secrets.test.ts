import { describe, expect, it } from 'vitest'

import { secretRef, secretRefsIn } from './secrets'

/**
 * The grammar of a secret reference is the whole reason the framework did not
 * have to change. `apply_template` matches `(?<!\{)\{(\w+)\}(?!\})` and `\w`
 * covers neither `:` nor `/`, so `{secret:pg/password}` is invisible to it; the
 * transformation engine's `{{var}}` is a different shape again. These tests pin
 * that separation, because a later widening of the pattern would break it
 * silently — the reference would be substituted by the wrong layer, or dropped.
 */
describe('secretRef', () => {
  it('writes the reference a form stores', () => {
    expect(secretRef('pg-prod', 'password')).toBe('{secret:pg-prod/password}')
  })
})

describe('secretRefsIn', () => {
  it('finds every reference, in order', () => {
    const url = 'jdbc:postgresql://{secret:pg/host}/app?password={secret:pg/password}'

    expect(secretRefsIn(url)).toEqual([
      { name: 'pg', field: 'host' },
      { name: 'pg', field: 'password' },
    ])
  })

  it('ignores the other two template syntaxes', () => {
    expect(secretRefsIn('{env}/{{captured}}/{secret_like}')).toEqual([])
  })

  it('ignores a reference whose name or field is not a name', () => {
    // No empty halves, no path segments, no leading punctuation: a store key is
    // a name, and anything else is a typo that should stay visible in the value
    // rather than resolve to something unexpected.
    expect(secretRefsIn('{secret:/password}')).toEqual([])
    expect(secretRefsIn('{secret:pg/}')).toEqual([])
    expect(secretRefsIn('{secret:pg/a/b}')).toEqual([])
    expect(secretRefsIn('{secret:-pg/password}')).toEqual([])
  })

  it('accepts the punctuation a store name really uses', () => {
    expect(secretRefsIn('{secret:pg.prod-2_eu/jdbc.url}')).toEqual([
      { name: 'pg.prod-2_eu', field: 'jdbc.url' },
    ])
  })
})
