/**
 * The three-tier metastore browser, drawn once and used twice.
 *
 * Databricks says catalog / schema / table, Athena says catalog / database /
 * table, and a lake says bucket / prefix / dataset. They are the same tree with
 * different words, so this renders one tree and lets the caller decide what a
 * leaf does: the catalog screen selects a namespace and lists its assets in a
 * pane beside it, the SQL editor expands the leaf into its columns and inserts
 * whatever is clicked.
 *
 * Nothing here reads the library. It takes a tree already built by
 * `buildNamespaceTree`, which reads the hierarchy back out of the addresses —
 * there is no metastore to ask.
 */

import {
  Boxes,
  ChevronDown,
  ChevronRight,
  CircleDashed,
  Columns3,
  Database,
  Eye,
  Folder,
  FolderOpen,
  Layers,
  PaintBucket,
  Radio,
  Table2,
} from 'lucide-react'
import { useCallback, useState, type ReactNode } from 'react'

import type { AssetKind, CatalogAsset, CatalogNode, NodeTier, RootKind } from '@/lib/datacatalog'
import { cn } from '@/lib/utils/cn'

export const ROOT_ICON: Record<RootKind, ReactNode> = {
  bucket: <PaintBucket className="h-3.5 w-3.5 text-brand-400" />,
  catalog: <Database className="h-3.5 w-3.5 text-brand-400" />,
  stream: <Radio className="h-3.5 w-3.5 text-brand-400" />,
  session: <CircleDashed className="h-3.5 w-3.5 text-content-subtle" />,
}

/** What the root is, in the word the platform it came from would use. */
export const ROOT_HINT: Record<RootKind, string> = {
  bucket: 'A bucket or a filesystem root. What is under it are directories of files.',
  catalog: 'A catalog, a database or a server. What is under it are tables.',
  stream: 'A streaming cluster. What is under it are topics, not stored datasets.',
  session:
    'Temporary views. They live in one SparkSession and nothing outside a run can read them.',
}

export const ASSET_ICON: Record<AssetKind, ReactNode> = {
  table: <Table2 className="h-3.5 w-3.5 text-brand-400" />,
  view: <Eye className="h-3.5 w-3.5 text-content-subtle" />,
  directory: <FolderOpen className="h-3.5 w-3.5 text-content-muted" />,
  topic: <Radio className="h-3.5 w-3.5 text-content-muted" />,
  index: <Layers className="h-3.5 w-3.5 text-content-muted" />,
  collection: <Boxes className="h-3.5 w-3.5 text-content-muted" />,
}

export const ASSET_HINT: Record<AssetKind, string> = {
  table:
    'Delta, Iceberg, Hudi or a database table — the engine reads it as a table whether it is named by identifier or by path.',
  view: 'A temporary view. It exists only inside the run that creates it.',
  directory: 'A directory of files. Parquet, CSV, JSON — the reader takes the whole folder.',
  topic: 'A Kafka topic.',
  index: 'A search index.',
  collection: 'A document collection.',
}

/** The tier word, chosen by what the root turned out to be. */
export function tierLabel(tier: NodeTier, rootKind: RootKind | undefined): string {
  if (tier === 'folder') return 'folder'
  if (rootKind === 'bucket') return tier === 'catalog' ? 'bucket' : 'prefix'
  if (rootKind === 'stream') return tier === 'catalog' ? 'cluster' : 'namespace'
  if (rootKind === 'session') return 'session'
  return tier === 'catalog' ? 'catalog' : 'schema'
}

const TIER_HINT: Record<NodeTier, string> = {
  catalog: 'The top level: a catalog in a metastore, or a bucket on object storage.',
  schema: 'The middle level: a schema or database, which on a lake is the first prefix.',
  folder: 'A level below the schema. No metastore has a word for it; a lake calls it a folder.',
}

export interface NamespaceTreeProps {
  tree: CatalogNode[]
  /** True while a search runs: everything opens so no match hides in a closed node. */
  searching?: boolean
  /** The selected namespace, when the caller tracks one. */
  selectedNodeId?: string | null
  onSelectNode?: (id: string) => void
  /**
   * Assets are drawn inside the tree when this is set — the SQL editor wants the
   * leaves in the tree; the catalog screen lists them in a pane beside it.
   */
  onSelectAsset?: (asset: CatalogAsset) => void
  /** Marks a leaf as in use — the SQL editor lights the datasets the query names. */
  isAssetActive?: (asset: CatalogAsset) => boolean
  /** A badge for the leaf row: the format, a column count, whatever the caller has. */
  assetTrailing?: (asset: CatalogAsset) => ReactNode
  /** Rendered under an expanded leaf. The SQL editor puts its columns here. */
  assetChildren?: (asset: CatalogAsset) => ReactNode
  /** Shown when the tree has nothing in it. */
  empty?: ReactNode
}

interface RowProps extends NamespaceTreeProps {
  node: CatalogNode
  isOpen: (id: string) => boolean
  toggle: (id: string) => void
  openAsset: string | null
  setOpenAsset: (key: string | null) => void
}

function AssetRow({
  asset,
  depth,
  active,
  expanded,
  onToggle,
  onSelect,
  trailing,
  children,
}: {
  asset: CatalogAsset
  depth: number
  active: boolean
  expanded: boolean
  onToggle: (() => void) | null
  onSelect: () => void
  trailing: ReactNode
  children: ReactNode
}) {
  return (
    <li>
      <div
        className={cn(
          'group flex items-center gap-1 rounded-md pr-1.5 text-xs transition',
          active ? 'bg-brand-500/10 text-content' : 'text-content-muted hover:bg-surface-raised',
        )}
        style={{ paddingLeft: `${depth * 12 + 4}px` }}
      >
        <button
          type="button"
          onClick={onToggle ?? undefined}
          aria-label={expanded ? `Hide ${asset.name} columns` : `Show ${asset.name} columns`}
          aria-expanded={onToggle ? expanded : undefined}
          className={cn(
            'shrink-0 rounded p-0.5 text-content-subtle',
            onToggle ? 'hover:text-content' : 'invisible',
          )}
        >
          <ChevronRight className={cn('h-3 w-3 transition-transform', expanded && 'rotate-90')} />
        </button>
        <button
          type="button"
          onClick={onSelect}
          title={`${asset.key} — ${ASSET_HINT[asset.kind]}`}
          className="flex min-w-0 flex-1 items-center gap-1.5 py-1 text-left"
        >
          <span className="shrink-0">{ASSET_ICON[asset.kind]}</span>
          <span className="truncate font-mono text-2xs text-content">{asset.name}</span>
        </button>
        {trailing ? <span className="shrink-0">{trailing}</span> : null}
      </div>
      {expanded ? children : null}
    </li>
  )
}

function TreeRow(props: RowProps) {
  const {
    node,
    isOpen,
    toggle,
    selectedNodeId,
    onSelectNode,
    onSelectAsset,
    isAssetActive,
    assetTrailing,
    assetChildren,
    openAsset,
    setOpenAsset,
  } = props
  const open = isOpen(node.id)
  const showAssets = Boolean(onSelectAsset)
  const hasChildren = node.children.length > 0 || (showAssets && node.assets.length > 0)
  const active = selectedNodeId === node.id
  const tier = tierLabel(node.tier, node.rootKind)

  return (
    <li>
      <div
        className={cn(
          'group flex items-center gap-1 rounded-md pr-2 text-xs transition',
          active ? 'bg-brand-500/10 text-content' : 'text-content-muted hover:bg-surface-raised',
        )}
        style={{ paddingLeft: `${node.depth * 12 + 4}px` }}
      >
        <button
          type="button"
          aria-label={open ? `Collapse ${node.label}` : `Expand ${node.label}`}
          aria-expanded={hasChildren ? open : undefined}
          onClick={() => toggle(node.id)}
          className={cn('shrink-0 rounded p-0.5', hasChildren ? 'hover:text-content' : 'invisible')}
        >
          {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
        </button>
        <button
          type="button"
          onClick={() => onSelectNode?.(node.id)}
          title={`${tier} · ${node.rootKind ? ROOT_HINT[node.rootKind] : TIER_HINT[node.tier]}`}
          className="flex min-w-0 flex-1 items-center gap-1.5 py-1 text-left"
        >
          <span className="shrink-0">
            {node.rootKind ? (
              ROOT_ICON[node.rootKind]
            ) : node.tier === 'schema' ? (
              <Layers className="h-3.5 w-3.5 text-content-muted" />
            ) : (
              <Folder className="h-3.5 w-3.5 text-content-subtle" />
            )}
          </span>
          <span className={cn('truncate', node.rootKind && 'font-medium text-content')}>
            {node.label}
          </span>
          <span
            className="ml-1 hidden shrink-0 rounded bg-surface-sunken px-1 text-[10px] uppercase
              tracking-wide text-content-subtle group-hover:inline"
          >
            {tier}
          </span>
          <span className="ml-auto shrink-0 tabular-nums text-[11px] text-content-subtle">
            {node.count}
          </span>
        </button>
      </div>
      {open ? (
        <ul>
          {node.children.map((child) => (
            <TreeRow key={child.id} {...props} node={child} />
          ))}
          {showAssets
            ? node.assets.map((asset) => (
                <AssetRow
                  key={asset.key}
                  asset={asset}
                  depth={node.depth + 1}
                  active={isAssetActive?.(asset) ?? false}
                  expanded={openAsset === asset.key}
                  onToggle={
                    assetChildren
                      ? () => setOpenAsset(openAsset === asset.key ? null : asset.key)
                      : null
                  }
                  onSelect={() => onSelectAsset?.(asset)}
                  trailing={assetTrailing?.(asset)}
                >
                  <div style={{ paddingLeft: `${(node.depth + 2) * 12 + 4}px` }}>
                    <div className="mb-1 border-l border-line pl-2">
                      <p
                        className="flex items-center gap-1 py-0.5 text-[10px] uppercase
                          tracking-wide text-content-subtle"
                      >
                        <Columns3 className="h-3 w-3" />
                        columns
                      </p>
                      {assetChildren?.(asset)}
                    </div>
                  </div>
                </AssetRow>
              ))
            : null}
        </ul>
      ) : null}
    </li>
  )
}

export function NamespaceTree(props: NamespaceTreeProps) {
  const { tree, searching, empty } = props
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(new Set())
  const [openAsset, setOpenAsset] = useState<string | null>(null)

  /**
   * Expansion is stored as what is CLOSED, not as what is open. The tree is
   * rebuilt whenever a Job changes an address, and a set of open ids would
   * collapse every node the rebuild renamed; a set of closed ids leaves the new
   * ones open, which is the harmless direction to be wrong in.
   */
  const isOpen = useCallback(
    (id: string) => Boolean(searching) || !collapsed.has(id),
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

  if (tree.length === 0) return <>{empty ?? null}</>

  return (
    <ul>
      {tree.map((root) => (
        <TreeRow
          key={root.id}
          {...props}
          node={root}
          isOpen={isOpen}
          toggle={toggle}
          openAsset={openAsset}
          setOpenAsset={setOpenAsset}
        />
      ))}
    </ul>
  )
}
