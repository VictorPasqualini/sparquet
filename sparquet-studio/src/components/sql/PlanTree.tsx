/**
 * An `EXPLAIN` plan, drawn as the tree it already is.
 *
 * Spark prints a plan bottom-up: the scans are at the bottom, and each line
 * feeds the one above it. That order is kept — re-sorting a plan would make
 * every plan anybody has ever read elsewhere unrecognisable — and what is added
 * is structure: a subtree folds away, the operator is separated from its
 * argument list, and the two lines somebody is actually looking for (where the
 * data is read, and where it is shuffled) are marked.
 *
 * The raw text stays one click away. A plan is evidence, and evidence gets
 * pasted into issues.
 */

import { ChevronRight, Copy } from 'lucide-react'
import { useCallback, useMemo, useState } from 'react'

import { Button } from '@/components/ui'
import { countNodes, parsePlan, toneOf, type PlanNode, type PlanTone } from '@/lib/sql/plan'
import { cn } from '@/lib/utils/cn'
import { copyText } from '@/lib/utils/download'

const TONES: Record<NonNullable<PlanTone>, string> = {
  scan: 'text-state-success',
  shuffle: 'text-state-warning',
  broadcast: 'text-state-info',
  join: 'text-brand-500',
  aggregate: 'text-content',
}

const TITLES: Record<NonNullable<PlanTone>, string> = {
  scan: 'Where the data is read.',
  shuffle: 'A shuffle: every row is written and read back across the network.',
  broadcast: 'One side is sent whole to every executor — cheap while it stays small.',
  join: 'A join.',
  aggregate: 'Rows are grouped or ordered.',
}

export function PlanTree({ text }: { text: string }) {
  const sections = useMemo(() => parsePlan(text), [text])
  const [raw, setRaw] = useState(false)
  const [collapsed, setCollapsed] = useState<Set<string>>(() => new Set())

  const toggle = useCallback((id: string) => {
    setCollapsed((current) => {
      const next = new Set(current)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }, [])

  // Nothing parseable — `EXPLAIN FORMATTED` and `EXPLAIN COST` print shapes of
  // their own. The plan is still the plan, so it is shown as it came.
  const readable = sections.length > 0 && !raw

  return (
    <div className="overflow-hidden rounded-xl border border-line bg-surface">
      <div className="flex items-center gap-2 border-b border-line bg-surface-sunken px-2.5 py-1.5">
        <span className="text-[11px] text-content-subtle">
          {sections.length > 0
            ? sections
                .map((section) => `${section.title}: ${countNodes(section.roots)} operators`)
                .join(' · ')
            : 'Plan'}
        </span>
        <span className="ml-auto flex items-center gap-1">
          {sections.length > 0 ? (
            <Button size="xs" variant="ghost" onClick={() => setRaw((value) => !value)}>
              {raw ? 'Tree' : 'Raw text'}
            </Button>
          ) : null}
          <Button
            size="xs"
            variant="ghost"
            onClick={() => void copyText(text)}
            icon={<Copy className="h-3 w-3" />}
          >
            Copy
          </Button>
        </span>
      </div>

      {readable ? (
        <div className="max-h-[26rem] overflow-auto p-2">
          {sections.map((section) => (
            <section key={section.title} className="mb-3 last:mb-0">
              {sections.length > 1 ? (
                <h4 className="px-1 pb-1 text-[11px] font-medium text-content-muted">
                  {section.title}
                </h4>
              ) : null}
              <ul>
                {section.roots.map((node) => (
                  <PlanRow
                    key={node.id}
                    node={node}
                    depth={0}
                    collapsed={collapsed}
                    onToggle={toggle}
                  />
                ))}
              </ul>
            </section>
          ))}
        </div>
      ) : (
        <pre className="max-h-[26rem] overflow-auto px-2.5 py-2 font-mono text-[11px] leading-relaxed text-content">
          {text}
        </pre>
      )}
    </div>
  )
}

function PlanRow({
  node,
  depth,
  collapsed,
  onToggle,
}: {
  node: PlanNode
  depth: number
  collapsed: Set<string>
  onToggle: (id: string) => void
}) {
  const open = !collapsed.has(node.id)
  const tone = toneOf(node.operator)
  const hasChildren = node.children.length > 0

  return (
    <li>
      <div
        className="flex items-start gap-1 rounded px-1 py-0.5 hover:bg-surface-sunken/60"
        style={{ paddingLeft: `${depth * 14 + 4}px` }}
      >
        {hasChildren ? (
          <button
            type="button"
            onClick={() => onToggle(node.id)}
            aria-expanded={open}
            aria-label={open ? `Collapse ${node.operator}` : `Expand ${node.operator}`}
            className="mt-0.5 flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded text-content-subtle hover:text-content"
          >
            <ChevronRight className={cn('h-3 w-3 transition-transform', open && 'rotate-90')} />
          </button>
        ) : (
          <span className="mt-0.5 h-3.5 w-3.5 shrink-0" />
        )}
        <span className="min-w-0 font-mono text-[11px] leading-relaxed">
          <span
            className={cn('font-medium', tone ? TONES[tone] : 'text-content')}
            title={tone ? TITLES[tone] : undefined}
          >
            {node.operator}
          </span>
          {node.detail ? (
            <span className="ml-1.5 break-all text-content-subtle">{node.detail}</span>
          ) : null}
          {!open && hasChildren ? (
            <span className="ml-1.5 text-content-subtle">
              (+{countNodes(node.children)} below)
            </span>
          ) : null}
        </span>
      </div>
      {open && hasChildren ? (
        <ul>
          {node.children.map((child) => (
            <PlanRow
              key={child.id}
              node={child}
              depth={depth + 1}
              collapsed={collapsed}
              onToggle={onToggle}
            />
          ))}
        </ul>
      ) : null}
    </li>
  )
}
