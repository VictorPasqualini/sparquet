import { describe, expect, it } from 'vitest'

import { formatSql, tokenize } from './format'

describe('tokenize', () => {
  it('keeps a string literal whole, spaces and keywords inside it included', () => {
    const tokens = tokenize("SELECT 'from where' AS label")
    expect(tokens.map((token) => token.text)).toEqual(['SELECT', "'from where'", 'AS', 'label'])
  })

  it('reads a doubled quote as an escape, not as the end of the literal', () => {
    expect(tokenize("'it''s'").map((token) => token.text)).toEqual(["'it''s'"])
  })

  it('keeps a backtick identifier whole', () => {
    expect(tokenize('SELECT `odd name` FROM t')[1]).toEqual({ kind: 'quoted', text: '`odd name`' })
  })

  it('keeps comments as tokens', () => {
    const tokens = tokenize('SELECT 1 -- why\nFROM t /* and here */')
    expect(tokens.filter((token) => token.kind === 'comment').map((token) => token.text)).toEqual([
      '-- why',
      '/* and here */',
    ])
  })

  it('does not split a two-character operator', () => {
    expect(tokenize('a <= b').map((token) => token.text)).toEqual(['a', '<=', 'b'])
  })

  it('loses nothing from an unterminated string, so a half-typed buffer survives', () => {
    expect(tokenize("SELECT 'oops").map((token) => token.text)).toEqual(['SELECT', "'oops"])
  })
})

describe('formatSql', () => {
  it('puts every clause on its own line', () => {
    expect(formatSql('select a from t where a > 1 order by a')).toBe(
      ['SELECT a', 'FROM t', 'WHERE a > 1', 'ORDER BY a'].join('\n'),
    )
  })

  it('gives each select item a line of its own', () => {
    expect(formatSql('select a, b, c from t')).toBe(
      ['SELECT a,', '  b,', '  c', 'FROM t'].join('\n'),
    )
  })

  it('does not break the arguments of a function call', () => {
    expect(formatSql('select count(*), coalesce(a, b) from t')).toBe(
      ['SELECT count(*),', '  coalesce(a, b)', 'FROM t'].join('\n'),
    )
  })

  it('leaves a window clause inline', () => {
    expect(formatSql('select sum(x) over (partition by k order by d) from t')).toBe(
      ['SELECT sum(x) OVER (PARTITION BY k ORDER BY d)', 'FROM t'].join('\n'),
    )
  })

  it('indents a subquery and closes it on its own line', () => {
    expect(formatSql('select * from (select a from t) x')).toBe(
      ['SELECT *', 'FROM (', '  SELECT a', '  FROM t', ') x'].join('\n'),
    )
  })

  it('starts a line at each condition of a predicate', () => {
    expect(formatSql('select * from t where a = 1 and b = 2 or c = 3')).toBe(
      ['SELECT *', 'FROM t', 'WHERE a = 1', '  AND b = 2', '  OR c = 3'].join('\n'),
    )
  })

  it('keeps BETWEEN in one piece', () => {
    expect(formatSql('select * from t where d between 1 and 9')).toBe(
      ['SELECT *', 'FROM t', 'WHERE d BETWEEN 1 AND 9'].join('\n'),
    )
  })

  it('keeps a CASE expression in one piece', () => {
    expect(formatSql('select case when a and b then 1 else 0 end as flag from t')).toBe(
      ['SELECT CASE WHEN a AND b THEN 1 ELSE 0 END AS flag', 'FROM t'].join('\n'),
    )
  })

  it('starts a line at a join and at its condition', () => {
    expect(formatSql('select * from a left join b on a.id = b.id')).toBe(
      ['SELECT *', 'FROM a', 'LEFT JOIN b', '  ON a.id = b.id'].join('\n'),
    )
  })

  it('keeps a qualified name together', () => {
    expect(formatSql('select a.b.c from a.b')).toBe(['SELECT a.b.c', 'FROM a.b'].join('\n'))
  })

  it('uppercases keywords but never an identifier', () => {
    expect(formatSql('select Value as Label from Orders')).toBe(
      ['SELECT Value AS Label', 'FROM Orders'].join('\n'),
    )
  })

  it('leaves keyword case alone when asked to', () => {
    expect(formatSql('select a from t', { uppercaseKeywords: false })).toBe(
      ['select a', 'from t'].join('\n'),
    )
  })

  it('never touches what is inside a literal or a comment', () => {
    const formatted = formatSql("select 'a  ,  b' -- keep   this\nfrom t")
    expect(formatted).toContain("'a  ,  b'")
    expect(formatted).toContain('-- keep   this')
  })

  it('is idempotent — formatting formatted SQL changes nothing', () => {
    const sql = `with recent as (select id, sum(total) as total from orders where d >= '2026-01-01' group by id)
select r.id, r.total, c.name from recent r inner join customers c on r.id = c.id where r.total > 100 order by r.total desc limit 20`
    const once = formatSql(sql)
    expect(formatSql(once)).toBe(once)
  })

  it('loses no token: the statement is the same one, only laid out', () => {
    const sql = `select a, count(*) as n from t join u on t.id = u.id where a between 1 and 2 and b = 'x, y' group by a having n > 1`
    const before = tokenize(sql).map((token) => token.text.toUpperCase())
    const after = tokenize(formatSql(sql)).map((token) => token.text.toUpperCase())
    expect(after).toEqual(before)
  })

  it('returns an empty buffer untouched instead of eating a selection', () => {
    expect(formatSql('   ')).toBe('')
  })
})
