/**
 * A SQL formatter for the editor's Format button.
 *
 * Formatting is a layout question, not a dialect question — where a clause
 * starts, how far a subquery is indented, whether a select list runs across the
 * page — so it is answered here rather than asked of the runner. Syntax
 * *errors* are the opposite: those come from the parser that will run the
 * statement, because guessing them in Spark's dialect from the browser is how
 * an editor underlines valid SQL. The two sit side by side in the toolbar and
 * are answered by two different things on purpose.
 *
 * The formatter never rewrites the statement. It only decides whitespace and
 * the case of keywords it recognises: a string literal, an identifier, a
 * comment and a number come out byte for byte as they went in. That invariant
 * is what makes a Format button safe to press on somebody else's query, and it
 * is pinned by a test rather than by care.
 *
 * Line breaks happen where reading a query looks for them:
 *
 *   - before a clause (`SELECT`, `FROM`, `WHERE`, `GROUP BY`, a join, …)
 *   - after a comma in a select list, so one item is one line
 *   - before `AND` / `OR`, so a long predicate reads as a list of conditions
 *   - around a parenthesis that opens a subquery, and nowhere else — `count(*)`
 *     and `OVER (PARTITION BY …)` stay on their line, because they are one idea
 *     each and breaking them is what makes a formatted query longer to read
 *     than the one that was typed.
 */

const INDENT = '  '

/** Words that start a clause and therefore start a line. */
const CLAUSE = new Set([
  'SELECT',
  'FROM',
  'WHERE',
  'HAVING',
  'QUALIFY',
  'WINDOW',
  'LIMIT',
  'OFFSET',
  'UNION',
  'INTERSECT',
  'EXCEPT',
  'VALUES',
  'WITH',
  'INSERT',
  'DESCRIBE',
  'EXPLAIN',
  'SHOW',
])

/** Two-word clauses, keyed by the first word. */
const CLAUSE_PAIR: Record<string, string> = {
  GROUP: 'BY',
  ORDER: 'BY',
  CLUSTER: 'BY',
  DISTRIBUTE: 'BY',
  SORT: 'BY',
}

/** Words that may precede `JOIN` and belong on its line. */
const JOIN_PREFIX = new Set(['LEFT', 'RIGHT', 'FULL', 'INNER', 'OUTER', 'CROSS', 'NATURAL', 'ANTI', 'SEMI'])

/**
 * Keywords uppercased on the way out.
 *
 * Only the ones that are keywords everywhere: a column called `value` or a
 * table called `orders` keeps whatever case its author gave it, because that
 * case may be the one the storage uses.
 */
const KEYWORDS = new Set([
  ...CLAUSE,
  ...JOIN_PREFIX,
  ...Object.keys(CLAUSE_PAIR),
  'ALL',
  'AND',
  'AS',
  'ASC',
  'BETWEEN',
  'BY',
  'CASE',
  'CAST',
  'CUBE',
  'CURRENT',
  'DESC',
  'DISTINCT',
  'ELSE',
  'END',
  'EXISTS',
  'FALSE',
  'FILTER',
  'FIRST',
  'FOLLOWING',
  'ILIKE',
  'IN',
  'INTERVAL',
  'IS',
  'JOIN',
  'LAST',
  'LATERAL',
  'LIKE',
  'NOT',
  'NULL',
  'NULLS',
  'ON',
  'OR',
  'OVER',
  'PARTITION',
  'PIVOT',
  'PRECEDING',
  'RANGE',
  'RLIKE',
  'ROLLUP',
  'ROW',
  'ROWS',
  'SETS',
  'TABLE',
  'THEN',
  'TRUE',
  'UNBOUNDED',
  'UNPIVOT',
  'USING',
  'WHEN',
])

type TokenKind = 'word' | 'string' | 'quoted' | 'number' | 'comment' | 'punct'

export interface SqlToken {
  kind: TokenKind
  /** The text exactly as it was written. */
  text: string
}

const WORD_START = /[A-Za-z_$À-￿]/
const WORD_REST = /[A-Za-z0-9_$À-￿]/

/**
 * The statement as tokens, losing nothing.
 *
 * Whitespace is dropped — that is the one thing the formatter is allowed to
 * decide. Everything else, including an unterminated string at the end of a
 * half-typed statement, comes back as a token so that formatting a buffer
 * mid-edit cannot truncate it.
 */
export function tokenize(sql: string): SqlToken[] {
  const tokens: SqlToken[] = []
  let index = 0

  while (index < sql.length) {
    const char = sql[index]

    if (/\s/.test(char)) {
      index += 1
      continue
    }

    if (char === '-' && sql[index + 1] === '-') {
      const end = sql.indexOf('\n', index)
      const stop = end === -1 ? sql.length : end
      tokens.push({ kind: 'comment', text: sql.slice(index, stop) })
      index = stop
      continue
    }

    if (char === '/' && sql[index + 1] === '*') {
      const end = sql.indexOf('*/', index + 2)
      const stop = end === -1 ? sql.length : end + 2
      tokens.push({ kind: 'comment', text: sql.slice(index, stop) })
      index = stop
      continue
    }

    if (char === "'" || char === '"' || char === '`') {
      // Doubling is how SQL escapes the quote character inside its own literal;
      // a backslash escape is accepted too, since Spark reads one by default.
      let end = index + 1
      while (end < sql.length) {
        if (sql[end] === '\\') {
          end += 2
          continue
        }
        if (sql[end] === char) {
          if (sql[end + 1] === char) {
            end += 2
            continue
          }
          end += 1
          break
        }
        end += 1
      }
      tokens.push({ kind: char === "'" ? 'string' : 'quoted', text: sql.slice(index, Math.min(end, sql.length)) })
      index = end
      continue
    }

    if (/[0-9]/.test(char) || (char === '.' && /[0-9]/.test(sql[index + 1] ?? ''))) {
      let end = index
      while (end < sql.length && /[0-9.eE]/.test(sql[end])) {
        // The sign of an exponent, not an operator between two numbers.
        if ((sql[end] === 'e' || sql[end] === 'E') && /[+-]/.test(sql[end + 1] ?? '')) end += 1
        end += 1
      }
      tokens.push({ kind: 'number', text: sql.slice(index, end) })
      index = end
      continue
    }

    if (WORD_START.test(char)) {
      let end = index + 1
      while (end < sql.length && WORD_REST.test(sql[end])) end += 1
      tokens.push({ kind: 'word', text: sql.slice(index, end) })
      index = end
      continue
    }

    // Two-character operators, so `<=` and `||` are never split across a space.
    const pair = sql.slice(index, index + 2)
    if (['<=', '>=', '<>', '!=', '||', '::', '=>', '->'].includes(pair)) {
      tokens.push({ kind: 'punct', text: pair })
      index += 2
      continue
    }

    tokens.push({ kind: 'punct', text: char })
    index += 1
  }

  return tokens
}

function keywordOf(token: SqlToken | undefined): string | null {
  if (!token || token.kind !== 'word') return null
  const upper = token.text.toUpperCase()
  return KEYWORDS.has(upper) ? upper : null
}

/** A paren we are currently inside, and whether it broke across lines. */
interface Scope {
  /** Clauses and commas inside this scope start their own line. */
  broken: boolean
  /** Indent of the clause keywords inside it. */
  indent: number
}

export interface FormatOptions {
  /** Uppercase the keywords the formatter recognises. On by default. */
  uppercaseKeywords?: boolean
}

/**
 * `sql`, laid out.
 *
 * Returns the input unchanged when there is nothing to lay out, so formatting
 * an empty buffer is a no-op rather than a way to lose a selection.
 */
export function formatSql(sql: string, options: FormatOptions = {}): string {
  const uppercase = options.uppercaseKeywords !== false
  const tokens = tokenize(sql)
  if (tokens.length === 0) return sql.trim()

  const lines: string[] = []
  let line = ''
  let indent = 0
  const scopes: Scope[] = [{ broken: true, indent: 0 }]
  // `BETWEEN a AND b` is one condition: its `AND` must not start a line.
  let pendingBetween = 0
  let caseDepth = 0

  const top = () => scopes[scopes.length - 1]

  const flush = () => {
    if (line.trim()) lines.push(INDENT.repeat(indent) + line.trim())
    line = ''
  }

  const breakTo = (level: number) => {
    flush()
    indent = level
  }

  const add = (text: string, spaceBefore = true) => {
    line += line.length > 0 && spaceBefore ? ` ${text}` : text
  }

  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index]
    const previous = tokens[index - 1]
    const next = tokens[index + 1]
    const keyword = keywordOf(token)
    const text = keyword && uppercase ? keyword : token.text
    // `count(*)`, not `count( *)`: an opening parenthesis owns what follows it.
    const spaced = !(previous?.kind === 'punct' && previous.text === '(')

    if (token.kind === 'comment') {
      // A comment keeps its own line: trailing it onto the previous one moves
      // it away from whatever it was written above.
      breakTo(indent)
      add(text, false)
      breakTo(indent)
      continue
    }

    if (token.kind === 'punct') {
      if (token.text === '(') {
        const inner = keywordOf(next)
        const broken = inner === 'SELECT' || inner === 'WITH' || inner === 'VALUES'
        // A call's parenthesis belongs to the name in front of it; a grouping
        // one does not, and wants the space it was written with.
        const isCall = previous?.kind === 'word' && !keywordOf(previous)
        const isKeywordCall =
          previous?.kind === 'word' && ['CAST', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'].includes(previous.text.toUpperCase())
        add('(', !(isCall || isKeywordCall))
        if (broken) breakTo(indent + 1)
        scopes.push({ broken, indent: broken ? indent : top().indent })
        continue
      }

      if (token.text === ')') {
        const scope = scopes.length > 1 ? scopes.pop()! : top()
        if (scope.broken) breakTo(Math.max(0, scope.indent - 1))
        add(')', false)
        continue
      }

      if (token.text === ',') {
        add(',', false)
        if (top().broken) breakTo(top().indent + 1)
        continue
      }

      if (token.text === ';') {
        add(';', false)
        breakTo(0)
        continue
      }

      // Dots bind: `schema.table` is one name and reads as one.
      if (token.text === '.') {
        add('.', false)
        continue
      }
      if (previous?.kind === 'punct' && previous.text === '.') {
        add(token.text, false)
        continue
      }

      add(token.text, spaced)
      continue
    }

    if (previous?.kind === 'punct' && previous.text === '.') {
      add(text, false)
      continue
    }

    if (keyword === 'CASE') caseDepth += 1
    if (keyword === 'END' && caseDepth > 0) caseDepth -= 1
    if (keyword === 'BETWEEN') pendingBetween += 1

    if (top().broken) {
      const pairSecond = CLAUSE_PAIR[keyword ?? '']
      const startsPairClause = pairSecond !== undefined && keywordOf(next) === pairSecond
      const startsJoin =
        keyword === 'JOIN' && !JOIN_PREFIX.has(keywordOf(previous) ?? '') ||
        (keyword !== null &&
          JOIN_PREFIX.has(keyword) &&
          !JOIN_PREFIX.has(keywordOf(previous) ?? '') &&
          [next, tokens[index + 2], tokens[index + 3]].some((ahead) => keywordOf(ahead) === 'JOIN'))

      if (keyword !== null && (CLAUSE.has(keyword) || startsPairClause || startsJoin)) {
        breakTo(top().indent)
      } else if ((keyword === 'AND' || keyword === 'OR') && caseDepth === 0) {
        if (keyword === 'AND' && pendingBetween > 0) {
          pendingBetween -= 1
        } else {
          breakTo(top().indent + 1)
        }
      } else if (keyword === 'ON' || keyword === 'USING') {
        breakTo(top().indent + 1)
      }
    }

    add(text, spaced)
  }

  flush()
  return lines.join('\n')
}
