import { describe, expect, it } from 'vitest'

import { csvCell, timestampedName, toCsv } from './csv'

describe('csvCell', () => {
  it('quotes only what has to be quoted, and doubles a quote', () => {
    expect(csvCell('plain')).toBe('plain')
    expect(csvCell('a,b')).toBe('"a,b"')
    expect(csvCell('say "hi"')).toBe('"say ""hi"""')
    expect(csvCell('line\nbreak')).toBe('"line\nbreak"')
  })

  it('writes an absent value as an empty field', () => {
    expect(csvCell(null)).toBe('')
    expect(csvCell(undefined)).toBe('')
  })

  it('writes a structured value as JSON rather than as [object Object]', () => {
    expect(csvCell({ a: 1 })).toBe('"{""a"":1}"')
    expect(csvCell([1, 2])).toBe('"[1,2]"')
  })

  it('keeps numbers and booleans as they read', () => {
    expect(csvCell(0)).toBe('0')
    expect(csvCell(false)).toBe('false')
  })
})

describe('toCsv', () => {
  it('writes a header and one CRLF-terminated line per row', () => {
    expect(toCsv(['id', 'name'], [[1, 'ok'], [2, 'a,b']])).toBe('id,name\r\n1,ok\r\n2,"a,b"')
  })
})

describe('timestampedName', () => {
  it('slugs the stem and keeps the extension', () => {
    expect(timestampedName('Orders by day', 'csv')).toMatch(
      /^orders-by-day-\d{4}-\d{2}-\d{2}-\d{4}\.csv$/,
    )
  })

  it('falls back to a name when the stem slugs away to nothing', () => {
    expect(timestampedName('***', 'json')).toMatch(/^export-/)
  })
})
