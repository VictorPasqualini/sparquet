/**
 * Spark's `EXPLAIN` output, as a tree.
 *
 * The runner returns a plan the way Spark prints it: one column, one row, and
 * inside it a block of text where the shape of the query is drawn with `+-`,
 * `:-` and three spaces of indentation per level. It is a tree already — it is
 * just a tree nobody can read past the third operator, because the eye has to
 * count spaces to know what feeds what.
 *
 * So this parses the drawing back into the structure it depicts. Nothing is
 * interpreted or renamed: an `Exchange` line stays an `Exchange` line, with its
 * whole argument list. What changes is that its children are its children, so a
 * subtree can be collapsed and a shuffle can be found without reading around it.
 *
 * `EXPLAIN EXTENDED` prints several plans under `== … ==` headings, and those
 * are kept as separate sections: the parsed, analysed, optimised and physical
 * plans are four answers to four different questions, and merging them would
 * produce a tree that describes no single thing.
 *
 * Anything this cannot read is handed back untouched — `EXPLAIN FORMATTED` and
 * `EXPLAIN COST` print shapes of their own, and a plan shown as plain text is
 * still the plan. Being wrong about a plan would be worse than being plain.
 */

export interface PlanNode {
  id: string
  /** The operator: `Exchange`, `FileScan parquet`, `HashAggregate`. */
  operator: string
  /** Everything the line said after the operator — keys, conditions, partitioning. */
  detail: string
  /** The whole line, minus the drawing characters. */
  text: string
  children: PlanNode[]
}

export interface PlanSection {
  /** `Physical Plan`, `Optimized Logical Plan`, … or `Plan` when unlabelled. */
  title: string
  roots: PlanNode[]
}

/** `== Physical Plan ==` */
const HEADING = /^==\s*(.+?)\s*==$/

/**
 * A drawn line: the indentation, the connector, and the operator.
 *
 * The prefix is made only of spaces and the `:` that continues a sibling's
 * line downward, so its length says how deep the node is — three characters per
 * level, which is what Spark's printer emits.
 */
const DRAWN = /^([ :]*)([+:]-)\s(.*)$/

/**
 * The operators whose format is part of their name.
 *
 * `FileScan parquet` and `FileScan json` are different operators to anybody
 * reading a plan — which reader ran is usually the first thing looked for — so
 * the second word is kept. Everywhere else the first word is the operator and
 * the rest is its arguments.
 */
const TWO_WORD = /^(FileScan|Scan|BatchScan|HiveTableScan|InMemoryTableScan)\s+([\w$.]+)/

function splitOperator(text: string): { operator: string; detail: string } {
  const two = TWO_WORD.exec(text)
  if (two) return { operator: `${two[1]} ${two[2]}`, detail: text.slice(two[0].length).trim() }
  const one = /^[\w$.]+/.exec(text)
  if (!one) return { operator: text, detail: '' }
  return { operator: one[0], detail: text.slice(one[0].length).trim() }
}

let seq = 0

function makeNode(text: string): PlanNode {
  seq += 1
  const { operator, detail } = splitOperator(text.trim())
  return { id: `p${seq}`, operator, detail, text: text.trim(), children: [] }
}

/**
 * The plan text as sections of trees.
 *
 * A line's depth comes from its prefix; a node is attached to the last node one
 * level above it. A line that is not drawn and follows a node — Spark wraps long
 * argument lists — is appended to that node's detail rather than becoming a
 * node of its own.
 */
export function parsePlan(text: string): PlanSection[] {
  const sections: PlanSection[] = []
  /** The last node seen at each depth, so a child knows its parent. */
  let stack: PlanNode[] = []

  const open = (title: string): PlanSection => {
    const opened: PlanSection = { title, roots: [] }
    sections.push(opened)
    stack = []
    return opened
  }

  for (const raw of text.split('\n')) {
    const line = raw.replace(/\s+$/, '')
    if (line.trim().length === 0) continue

    const heading = HEADING.exec(line.trim())
    if (heading) {
      open(heading[1])
      continue
    }

    const section = sections[sections.length - 1] ?? open('Plan')

    const drawn = DRAWN.exec(line)
    if (drawn) {
      const depth = Math.floor(drawn[1].length / 3) + 1
      const node = makeNode(drawn[3])
      const parent = stack[depth - 1]
      if (parent) parent.children.push(node)
      else section.roots.push(node)
      stack = stack.slice(0, depth)
      stack[depth] = node
      continue
    }

    // Not drawn. At the start of a section it is the root; anywhere else it is
    // the continuation of the line above.
    if (stack.length === 0 && section.roots.length === 0) {
      const node = makeNode(line)
      section.roots.push(node)
      stack = [node]
      continue
    }
    const last = stack[stack.length - 1]
    if (last) {
      last.detail = `${last.detail} ${line.trim()}`.trim()
      last.text = `${last.text} ${line.trim()}`.trim()
    }
  }

  return sections.filter((section) => section.roots.length > 0)
}

/** How many nodes a section holds, for the "12 operators" line. */
export function countNodes(nodes: PlanNode[]): number {
  return nodes.reduce((total, node) => total + 1 + countNodes(node.children), 0)
}

/**
 * The operators worth marking, and why.
 *
 * A plan is read looking for two things: where the data comes in, and where it
 * has to move. A shuffle is the expensive one — it writes every row to disk and
 * reads it back across the network — and a broadcast is the cheap one that turns
 * expensive when the side being broadcast is not small. Marking them makes the
 * two questions answerable at a glance instead of by reading every line.
 */
export type PlanTone = 'scan' | 'shuffle' | 'broadcast' | 'join' | 'aggregate' | null

export function toneOf(operator: string): PlanTone {
  if (/Scan|Relation/i.test(operator)) return 'scan'
  if (/^Exchange|ShuffleQueryStage|RepartitionBy|Repartition/i.test(operator)) return 'shuffle'
  if (/Broadcast/i.test(operator)) return 'broadcast'
  if (/Join|Cartesian/i.test(operator)) return 'join'
  if (/Aggregate|Window|Sort(?!MergeJoin)/i.test(operator)) return 'aggregate'
  return null
}

/**
 * Whether a result is a plan rather than rows.
 *
 * Spark answers `EXPLAIN` with a single cell of text, so the shape of the
 * result is the reliable signal — more reliable than the SQL, which may be a
 * comment away from starting with the word.
 */
export function isPlanResult(columns: string[], rows: unknown[][]): boolean {
  return (
    columns.length === 1 &&
    rows.length === 1 &&
    typeof rows[0]?.[0] === 'string' &&
    /^(plan|physical_plan|explain)$/i.test(columns[0] ?? '')
  )
}
