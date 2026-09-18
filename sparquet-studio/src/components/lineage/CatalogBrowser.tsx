/**
 * The catalog, browsed the way Unity Catalog and Athena are browsed: three tiers
 * on the left — catalog, then schema (a database, or the first prefix of a
 * bucket), then the tables inside it — and the assets of whatever is selected on
 * the right.
 *
 * The hierarchy is not stored anywhere — it is read back out of the addresses by
 * `describeAsset`, so a bucket appears the moment a Job writes a path into it and
 * disappears when the last Job stops. Nothing here registers, resolves or grants
 * anything: Spark, Glue or Unity Catalog still own that at runtime. This is the
 * view, and the descriptions people attach to it.
 */

import { Crown, Pencil, ShieldCheck, Slash } from 'lucide-react'
import { useMemo, useState } from 'react'

import { ASSET_HINT, ASSET_ICON, NamespaceTree, tierLabel } from '@/components/catalog/NamespaceTree'
import { Badge, Button, type BadgeTone } from '@/components/ui'
import {
  buildNamespaceTree,
  describeAsset,
  effectiveClassification,
  type CatalogAsset,
  type CatalogEntry,
  type CatalogNode,
  type DatasetSchema,
} from '@/lib/datacatalog'
import type { DatasetGrant, Decision, Owner } from '@/lib/iam'
import { effectiveOwner, summarizeGrants } from '@/lib/iam'
import type { DatasetPlace } from '@/lib/lineage'

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

/**
 * What the person at the keyboard holds on a dataset, said the way the question
 * is asked — "can I read this?" — with where the answer came from in the title,
 * since a level with no source sends people to the wrong screen to change it.
 */
function standing(
  decision: Decision | undefined,
  key: string,
): { tone: BadgeTone; label: string; title: string } {
  const own = `dataset/${key}`
  if (!decision || !decision.governed) {
    return {
      tone: 'neutral',
      label: 'ungoverned',
      title:
        'No rule names this dataset or any path above it. Access to it is whatever the runner already allows — naming an owner is enough to start governing it.',
    }
  }
  if (decision.owned) {
    return {
      tone: 'success',
      label: 'you own it',
      title:
        decision.source === own
          ? 'You own this dataset: every level on it, and a deny cannot reach you.'
          : `You own ${decision.source}, and ownership reaches everything under it.`,
    }
  }
  if (!decision.level) {
    return {
      tone: 'danger',
      label: 'no access',
      title: 'Governed, and no rule here reaches you. The owner is who to ask.',
    }
  }
  return {
    tone: decision.level === 'admin' ? 'success' : 'brand',
    label: `you: ${decision.level}`,
    title:
      decision.source === own
        ? `Granted ${decision.level} on this dataset.`
        : `Granted ${decision.level} on ${decision.source} — inherited from there, so that is where to change it.`,
  }
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

/** The chain of nodes from the root down to `id`, for the breadcrumb. */
function pathTo(nodes: readonly CatalogNode[], id: string): CatalogNode[] {
  for (const node of nodes) {
    if (node.id === id) return [node]
    const below = pathTo(node.children, id)
    if (below.length > 0) return [node, ...below]
  }
  return []
}

export interface CatalogBrowserProps {
  /** Already searched and filtered by the screen — the tree only shows what is left. */
  entries: CatalogEntry[]
  /** Columns derived from the canvas, by dataset address. */
  schemas: ReadonlyMap<string, DatasetSchema>
  /** Access rules per dataset address, so a card can say who may read it. */
  grants: ReadonlyMap<string, DatasetGrant[]>
  /** Every ownership record, of any kind — a dataset inherits from its path. */
  owners: readonly Owner[]
  /** What the current identity holds per dataset address, decided by the screen. */
  decisions: ReadonlyMap<string, Decision>
  /** True while a search is running: everything expands so matches are not hidden. */
  searching: boolean
  onOpenDataset: (key: string) => void
  workflowName: (id: string) => string
}

export function CatalogBrowser({
  entries,
  schemas,
  grants,
  owners,
  decisions,
  searching,
  onOpenDataset,
  workflowName,
}: CatalogBrowserProps) {
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

  const node = selected ? findNode(tree, selected) : null
  const crumbs = selected ? pathTo(tree, selected) : []
  const shown = node ? assetsUnder(node) : assets

  if (entries.length === 0) return null

  return (
    <div className="grid gap-4 lg:grid-cols-[minmax(0,16rem)_minmax(0,1fr)]">
      <aside className="card max-h-[70vh] overflow-auto p-2">
        <div className="mb-1 flex items-center justify-between px-2 py-1">
          <span
            className="text-[11px] font-semibold uppercase tracking-wide text-content-subtle"
            title="Catalog, then schema or database, then the tables inside it — the same three
              levels Unity Catalog and Athena show. Read out of the addresses, not registered."
          >
            Catalog · Schema · Table
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
        <NamespaceTree
          tree={tree}
          searching={searching}
          selectedNodeId={selected}
          onSelectNode={setSelected}
        />
      </aside>

      <section className="card max-h-[70vh] overflow-auto p-0">
        <div className="sticky top-0 z-10 flex items-center gap-2 border-b border-line bg-surface px-4 py-2.5">
          {crumbs.length > 0 ? (
            <nav
              aria-label="Namespace"
              className="flex min-w-0 flex-1 flex-wrap items-center gap-1 text-xs"
            >
              <button
                type="button"
                onClick={() => setSelected(null)}
                className="rounded px-1 py-0.5 text-content-subtle transition hover:bg-surface-raised hover:text-content"
              >
                all
              </button>
              {crumbs.map((crumb) => (
                <span key={crumb.id} className="flex min-w-0 items-center gap-1">
                  <Slash className="h-3 w-3 shrink-0 -rotate-12 text-content-subtle/60" />
                  <button
                    type="button"
                    onClick={() => setSelected(crumb.id)}
                    title={tierLabel(crumb.tier, crumb.rootKind)}
                    className="min-w-0 truncate rounded px-1 py-0.5 font-mono text-content transition
                      hover:bg-surface-raised"
                  >
                    {crumb.label}
                  </button>
                  <span className="shrink-0 rounded bg-surface-sunken px-1 text-[10px] uppercase tracking-wide text-content-subtle">
                    {tierLabel(crumb.tier, crumb.rootKind)}
                  </span>
                </span>
              ))}
            </nav>
          ) : (
            <code className="min-w-0 flex-1 truncate font-mono text-xs text-content">
              everything in scope
            </code>
          )}
          <span className="shrink-0 text-[11px] text-content-subtle">
            {shown.length} {shown.length === 1 ? 'asset' : 'assets'}
          </span>
        </div>

        <ul className="divide-y divide-line">
          {shown.map((asset) => {
            const entry = byKey.get(asset.key)
            if (!entry) return null
            const { dataset, annotation } = entry
            const schema = schemas.get(dataset.key)
            const access = summarizeGrants(grants.get(dataset.key) ?? [])
            // Not the annotation's `owner`, which is a name somebody typed. This one
            // holds every privilege on the dataset and may re-grant it.
            const held = effectiveOwner(owners, 'dataset', dataset.key)
            const decision = decisions.get(dataset.key)
            const mine = standing(decision, dataset.key)

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
                  <span title={ASSET_HINT[asset.kind]}>
                    <Badge tone="neutral">{asset.kind}</Badge>
                  </span>
                  <Badge tone={PLACE_TONE[dataset.place]}>{PLACE_LABEL[dataset.place]}</Badge>
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

                {/*
                  Two rows, because they answer two different questions and mixing
                  them made the card unreadable: what the data IS — formats,
                  columns, who writes it — and who may TOUCH it.
                */}
                <dl className="mt-2 grid gap-2 sm:grid-cols-2">
                  <div className="min-w-0 rounded-md border border-line/70 bg-surface-sunken/60 px-2 py-1.5">
                    <dt className="mb-1 text-[10px] uppercase tracking-wide text-content-subtle">
                      Shape
                    </dt>
                    <dd className="flex flex-wrap items-center gap-x-1.5 gap-y-1 text-[11px] text-content-subtle">
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
                      ) : (
                        <span className="italic">no columns derived</span>
                      )}
                      <span>
                        {dataset.producers.length} writing · {dataset.consumers.length} reading
                      </span>
                      {dataset.workflowIds.length > 1 ? (
                        <span
                          title={`Touched by ${dataset.workflowIds
                            .map(workflowName)
                            .join(', ')} — a change here crosses a workflow boundary.`}
                        >
                          <Badge tone="info">{dataset.workflowIds.length} workflows</Badge>
                        </span>
                      ) : null}
                    </dd>
                  </div>

                  <div className="min-w-0 rounded-md border border-line/70 bg-surface-sunken/60 px-2 py-1.5">
                    <dt className="mb-1 flex items-center gap-1 text-[10px] uppercase tracking-wide text-content-subtle">
                      <ShieldCheck className="h-3 w-3" />
                      Access
                    </dt>
                    <dd className="flex flex-wrap items-center gap-x-1.5 gap-y-1 text-[11px] text-content-subtle">
                      <span title={mine.title}>
                        <Badge tone={mine.tone}>{mine.label}</Badge>
                      </span>
                      {held ? (
                        <span
                          title={
                            held.source === `dataset/${dataset.key}`
                              ? 'Owner of this dataset: holds every level on it and may re-grant it.'
                              : `Inherited from ${held.source}: whoever owns the path owns what is under it.`
                          }
                        >
                          <Badge
                            tone={held.source === `dataset/${dataset.key}` ? 'success' : 'neutral'}
                            icon={<Crown />}
                          >
                            {held.owner.principalLabel ?? held.owner.principalId}
                          </Badge>
                        </span>
                      ) : null}
                      {annotation?.owner ? (
                        <span>owner {annotation.owner}</span>
                      ) : held ? null : (
                        <span className="italic">no owner</span>
                      )}
                      {annotation?.domain ? <span>· {annotation.domain}</span> : null}
                      {/*
                        The effective classification, not the one typed on the
                        table: a dataset is as restricted as the most restricted
                        column in it, and a badge that says `internal` over a
                        `restricted` column is worse than no badge at all.
                      */}
                      {effectiveClassification(annotation) ? (
                        <Badge
                          tone={
                            effectiveClassification(annotation) === 'restricted'
                              ? 'warning'
                              : 'neutral'
                          }
                        >
                          {effectiveClassification(annotation)}
                        </Badge>
                      ) : null}
                      {/*
                        Only the rules written against this address. The badge above
                        already answers the reader's own case, inheritance included —
                        saying "0 granted" beside it would read as "nobody", when an
                        inherited rule may be granting the whole team.
                      */}
                      {access.total > 0 ? (
                        <>
                          <Badge tone="brand">{access.allowed} granted</Badge>
                          {access.denied > 0 ? (
                            <Badge tone="danger">{access.denied} denied</Badge>
                          ) : null}
                        </>
                      ) : null}
                      {annotation?.tags.map((tag) => (
                        <Badge key={tag} tone="neutral">
                          {tag}
                        </Badge>
                      ))}
                    </dd>
                  </div>
                </dl>
              </li>
            )
          })}
        </ul>
      </section>
    </div>
  )
}
