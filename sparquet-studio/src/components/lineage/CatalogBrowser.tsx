/**
 * The catalog, browsed the way Glue, Hive and Unity Catalog are browsed: a tree of
 * namespaces on the left, the assets they hold on the right.
 *
 * The hierarchy is not stored anywhere — it is read back out of the addresses by
 * `describeAsset`, so a bucket appears the moment a Job writes a path into it and
 * disappears when the last Job stops. Nothing here registers, resolves or grants
 * anything: Spark, Glue or Unity Catalog still own that at runtime. This is the
 * view, and the descriptions people attach to it.
 */

import {
  Boxes,
  ChevronDown,
  ChevronRight,
  CircleDashed,
  Database,
  Eye,
  Folder,
  FolderOpen,
  Layers,
  PaintBucket,
  Pencil,
  Radio,
  Table2,
} from 'lucide-react'
import { useCallback, useMemo, useState, type ReactNode } from 'react'

import { Badge, Button, type BadgeTone } from '@/components/ui'
import {
  buildNamespaceTree,
  describeAsset,
  type AssetKind,
  type CatalogAsset,
  type CatalogEntry,
  type CatalogNode,
  type DatasetSchema,
  type RootKind,
} from '@/lib/datacatalog'
import type { DatasetPlace } from '@/lib/lineage'

const ROOT_ICON: Record<RootKind, ReactNode> = {
  bucket: <PaintBucket className="h-3.5 w-3.5 text-brand-400" />,
  catalog: <Database className="h-3.5 w-3.5 text-brand-400" />,
  stream: <Radio className="h-3.5 w-3.5 text-brand-400" />,
  session: <CircleDashed className="h-3.5 w-3.5 text-content-subtle" />,
}

/** What the root is, in the word the platform it came from would use. */
const ROOT_HINT: Record<RootKind, string> = {
  bucket: 'A bucket or a filesystem root. What is under it are directories of files.',
  catalog: 'A catalog, a database or a server. What is under it are tables.',
  stream: 'A streaming cluster. What is under it are topics, not stored datasets.',
  session: 'Temporary views. They live in one SparkSession and nothing outside a run can read them.',
}

const ASSET_ICON: Record<AssetKind, ReactNode> = {
  table: <Table2 className="h-3.5 w-3.5 text-brand-400" />,
  view: <Eye className="h-3.5 w-3.5 text-content-subtle" />,
  directory: <FolderOpen className="h-3.5 w-3.5 text-content-muted" />,
  topic: <Radio className="h-3.5 w-3.5 text-content-muted" />,
  index: <Layers className="h-3.5 w-3.5 text-content-muted" />,
  collection: <Boxes className="h-3.5 w-3.5 text-content-muted" />,
}

const ASSET_HINT: Record<AssetKind, string> = {
  table: 'Delta, Iceberg, Hudi or a database table — the engine reads it as a table whether it is named by identifier or by path.',
  view: 'A temporary view. It exists only inside the run that creates it.',
  directory: 'A directory of files. Parquet, CSV, JSON — the reader takes the whole folder.',
  topic: 'A Kafka topic.',
  index: 'A search index.',
  collection: 'A document collection.',
}

const PLACE_LABEL: Record<DatasetPlace, string> = {
  external: 'External',
  intermediate: 'Handoff',
  terminal: 'Terminal',
  isolated: 'Isolated',
}

const PLACE_TONE: Record<DatasetPlace, BadgeTone> = {
  external: 'info',
  intermediate: 'brand',
  terminal: 'success',
  isolated: 'warning',
}

/** Every asset in this node and everything under it, in tree order. */
function assetsUnder(node: CatalogNode): CatalogAsset[] {
  return [...node.assets, ...node.children.flatMap(assetsUnder)]
}

function findNode(nodes: readonly CatalogNode[], id: string): CatalogNode | null {
  for (const node of nodes) {
    if (node.id === id) return node
    const found = findNode(node.children, id)
    if (found) return found
  }
  return null
}

function TreeRow({
  node,
  depth,
  isOpen,
  selected,
  onToggle,
  onSelect,
}: {
  node: CatalogNode
  depth: number
  isOpen: (id: string) => boolean
  selected: string | null
  onToggle: (id: string) => void
  onSelect: (id: string) => void
}) {
  const open = isOpen(node.id)
  const hasChildren = node.children.length > 0
  const active = selected === node.id

  return (
    <li>
      <div
        className={`group flex items-center gap-1 rounded-md pr-2 text-xs transition ${
          active ? 'bg-brand-500/10 text-content' : 'text-content-muted hover:bg-surface-raised'
        }`}
        style={{ paddingLeft: `${depth * 12 + 4}px` }}
      >
        <button
          type="button"
          aria-label={open ? `Collapse ${node.label}` : `Expand ${node.label}`}
          onClick={() => onToggle(node.id)}
          className={`shrink-0 rounded p-0.5 ${hasChildren ? 'hover:text-content' : 'invisible'}`}
        >
          {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
        </button>
        <button
          type="button"
          onClick={() => onSelect(node.id)}
          title={node.rootKind ? ROOT_HINT[node.rootKind] : node.id}
          className="flex min-w-0 flex-1 items-center gap-1.5 py-1 text-left"
        >
          <span className="shrink-0">
            {node.rootKind ? (
              ROOT_ICON[node.rootKind]
            ) : (
              <Folder className="h-3.5 w-3.5 text-content-subtle" />
            )}
          </span>
          <span className={`truncate ${node.rootKind ? 'font-medium text-content' : ''}`}>
            {node.label}
          </span>
          <span className="ml-auto shrink-0 tabular-nums text-[11px] text-content-subtle">
            {node.count}
          </span>
        </button>
      </div>
      {open && hasChildren ? (
        <ul>
          {node.children.map((child) => (
            <TreeRow
              key={child.id}
              node={child}
              depth={depth + 1}
              isOpen={isOpen}
              selected={selected}
              onToggle={onToggle}
              onSelect={onSelect}
            />
          ))}
        </ul>
      ) : null}
    </li>
  )
}

export interface CatalogBrowserProps {
  /** Already searched and filtered by the screen — the tree only shows what is left. */
  entries: CatalogEntry[]
  /** Columns derived from the canvas, by dataset address. */
  schemas: ReadonlyMap<string, DatasetSchema>
  /** True while a search is running: everything expands so matches are not hidden. */
  searching: boolean
  onOpenDataset: (key: string) => void
  workflowName: (id: string) => string
}

export function CatalogBrowser({
  entries,
  schemas,
  searching,
  onOpenDataset,
  workflowName,
}: CatalogBrowserProps) {
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(new Set())
  const [selected, setSelected] = useState<string | null>(null)

  const byKey = useMemo(
    () => new Map(entries.map((entry) => [entry.dataset.key, entry])),
    [entries],
  )

  const assets = useMemo(
    () => entries.map((entry) => describeAsset(entry.dataset.key, entry.dataset.formats)),
    [entries],
  )

  const tree = useMemo(() => buildNamespaceTree(assets), [assets])

  /**
   * Expansion is stored as what is CLOSED, not as what is open. The tree is rebuilt
   * whenever a Job changes an address, and a set of open ids would collapse every
   * node the rebuild renamed; a set of closed ids leaves the new ones open, which
   * is the harmless direction to be wrong in.
   */
  const isOpen = useCallback(
    (id: string) => searching || !collapsed.has(id),
    [collapsed, searching],
  )

  const toggle = useCallback((id: string) => {
    setCollapsed((current) => {
      const next = new Set(current)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }, [])

  const node = selected ? findNode(tree, selected) : null
  const shown = node ? assetsUnder(node) : assets
  const where = node?.id ?? 'everything in scope'

  if (entries.length === 0) return null

  return (
    <div className="grid gap-4 lg:grid-cols-[minmax(0,15rem)_minmax(0,1fr)]">
      <aside className="card max-h-[70vh] overflow-auto p-2">
        <div className="mb-1 flex items-center justify-between px-2 py-1">
          <span className="text-[11px] font-semibold uppercase tracking-wide text-content-subtle">
            Namespaces
          </span>
          {selected ? (
            <button
              type="button"
              onClick={() => setSelected(null)}
              className="text-[11px] text-content-muted hover:text-content"
            >
              Show all
            </button>
          ) : null}
        </div>
        <ul>
          {tree.map((root) => (
            <TreeRow
              key={root.id}
              node={root}
              depth={0}
              isOpen={isOpen}
              selected={selected}
              onToggle={toggle}
              onSelect={setSelected}
            />
          ))}
        </ul>
      </aside>

      <section className="card max-h-[70vh] overflow-auto p-0">
        <div className="sticky top-0 z-10 flex items-center gap-2 border-b border-line bg-surface px-4 py-2.5">
          <code className="min-w-0 flex-1 truncate font-mono text-xs text-content">{where}</code>
          <span className="shrink-0 text-[11px] text-content-subtle">
            {shown.length} {shown.length === 1 ? 'asset' : 'assets'}
          </span>
        </div>

        <ul className="divide-y divide-line">
          {shown.map((asset) => {
            const entry = byKey.get(asset.key)
            if (!entry) return null
            const { dataset, annotation } = entry
            // Only the part of the address the selected node does not already say.
            const prefix = asset.namespace.join('/')
            const schema = schemas.get(dataset.key)

            return (
              <li key={asset.key} className="px-4 py-3 transition hover:bg-surface-raised/50">
                <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5">
                  <span title={ASSET_HINT[asset.kind]} className="shrink-0">
                    {ASSET_ICON[asset.kind]}
                  </span>
                  <button
                    type="button"
                    onClick={() => onOpenDataset(dataset.key)}
                    title={dataset.key}
                    className="min-w-0 truncate text-left text-xs font-medium text-content hover:underline"
                  >
                    {asset.name}
                  </button>
                  {prefix ? (
                    <code className="min-w-0 truncate font-mono text-[11px] text-content-subtle">
                      {prefix}
                    </code>
                  ) : null}
                  <span title={ASSET_HINT[asset.kind]}>
                    <Badge tone="neutral">{asset.kind}</Badge>
                  </span>
                  <Badge tone={PLACE_TONE[dataset.place]}>{PLACE_LABEL[dataset.place]}</Badge>
                  {dataset.formats.map((format) => (
                    <Badge key={format} tone="neutral">
                      {format}
                    </Badge>
                  ))}
                  {schema && schema.fields.length > 0 ? (
                    <span
                      title={`${
                        schema.confidence === 'complete'
                          ? 'The whole schema, as the Job states it'
                          : 'The columns the Jobs name; the source supplies the rest'
                      }: ${schema.fields.map((field) => field.name).join(', ')}`}
                    >
                      <Badge tone={schema.confidence === 'complete' ? 'success' : 'warning'}>
                        {schema.fields.length} col
                      </Badge>
                    </span>
                  ) : null}
                  {dataset.workflowIds.length > 1 ? (
                    <span
                      title={`Touched by ${dataset.workflowIds.map(workflowName).join(', ')} — a change here crosses a workflow boundary.`}
                    >
                      <Badge tone="info">{dataset.workflowIds.length} workflows</Badge>
                    </span>
                  ) : null}
                  <Button
                    size="xs"
                    variant="ghost"
                    className="ml-auto"
                    onClick={() => onOpenDataset(dataset.key)}
                    title="Open the catalog entry"
                  >
                    <Pencil />
                    {annotation ? 'Edit' : 'Describe'}
                  </Button>
                </div>

                <p
                  className={`mt-2 break-words text-xs ${
                    annotation?.description
                      ? 'leading-relaxed text-content-muted'
                      : 'italic text-content-subtle'
                  }`}
                >
                  {annotation?.description || 'No description yet.'}
                </p>

                <div className="mt-1.5 flex flex-wrap items-center gap-x-1.5 gap-y-1 text-[11px] text-content-subtle">
                  <span>
                    {dataset.producers.length} writing · {dataset.consumers.length} reading
                  </span>
                  {annotation?.owner ? <span>· owner {annotation.owner}</span> : null}
                  {annotation?.domain ? <span>· {annotation.domain}</span> : null}
                  {annotation?.classification ? (
                    <Badge tone={annotation.classification === 'restricted' ? 'warning' : 'neutral'}>
                      {annotation.classification}
                    </Badge>
                  ) : null}
                  {annotation?.tags.map((tag) => (
                    <Badge key={tag} tone="neutral">
                      {tag}
                    </Badge>
                  ))}
                </div>
              </li>
            )
          })}
        </ul>
      </section>
    </div>
  )
}
