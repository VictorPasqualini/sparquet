/**
 * Pure graph helpers shared by the compiler.
 *
 * Nothing here knows about pipeline JSON: it only answers structural questions
 * about the canvas (who feeds whom, which chain ends at this node).
 */

import { nanoid } from 'nanoid'

import type {
  NodeKind,
  NoteNode,
  SinkNode,
  SourceNode,
  StudioEdge,
  StudioGraph,
  StudioNode,
  TransformNode,
  ValidationNode,
} from '@/types/studio'
import { HANDLE } from '@/types/studio'

/* ------------------------------------------------------------- type guards */

export function isSourceNode(node: StudioNode): node is SourceNode {
  return node.data.kind === 'source'
}

export function isTransformNode(node: StudioNode): node is TransformNode {
  return node.data.kind === 'transform'
}

/** One rule of the `validations` block; a run of them compiles into that block. */
export function isValidationNode(node: StudioNode): node is ValidationNode {
  return node.data.kind === 'validation'
}

export function isSinkNode(node: StudioNode): node is SinkNode {
  return node.data.kind === 'sink'
}

export function isNoteNode(node: StudioNode): node is NoteNode {
  return node.data.kind === 'note'
}

/** Muted nodes stay on the canvas but never reach the compiled JSON. */
export function isDisabled(node: StudioNode): boolean {
  return 'disabled' in node.data && node.data.disabled === true
}

/** Nodes the compiler walks through: everything but notes and muted nodes. */
export function isCompilable(node: StudioNode): boolean {
  return !isNoteNode(node) && !isDisabled(node)
}

/* ---------------------------------------------------------------- lookups */

export function nodeById(graph: StudioGraph, id: string): StudioNode | undefined {
  return graph.nodes.find((node) => node.id === id)
}

/** Edges drawn without an explicit handle id are treated as main-input edges. */
function targetHandleOf(edge: StudioEdge): string {
  return edge.targetHandle ?? HANDLE.in
}

function parentsOn(graph: StudioGraph, nodeId: string, handle: string): StudioNode[] {
  const seen = new Set<string>()
  const parents: StudioNode[] = []
  for (const edge of graph.edges) {
    if (edge.target !== nodeId || targetHandleOf(edge) !== handle) continue
    if (seen.has(edge.source)) continue
    seen.add(edge.source)
    const parent = nodeById(graph, edge.source)
    if (parent) parents.push(parent)
  }
  return parents
}

/** Every node wired into the main input — more than one is a modelling error. */
export function primaryParents(graph: StudioGraph, nodeId: string): StudioNode[] {
  return parentsOn(graph, nodeId, HANDLE.in)
}

export function primaryParent(graph: StudioGraph, nodeId: string): StudioNode | undefined {
  return parentsOn(graph, nodeId, HANDLE.in)[0]
}

/** Nodes fed by this node's output through their main input. */
export function primaryChildren(graph: StudioGraph, nodeId: string): StudioNode[] {
  const seen = new Set<string>()
  const children: StudioNode[] = []
  for (const edge of graph.edges) {
    if (edge.source !== nodeId || targetHandleOf(edge) !== HANDLE.in) continue
    if (seen.has(edge.target)) continue
    seen.add(edge.target)
    const child = nodeById(graph, edge.target)
    if (child) children.push(child)
  }
  return children
}

/** The right-hand side of a `join` / `union`. */
export function sideParent(graph: StudioGraph, nodeId: string): StudioNode | undefined {
  return parentsOn(graph, nodeId, HANDLE.inRight)[0]
}

/* ------------------------------------------------------------------ chains */

export type ChainProblemCode = 'multiple-parents' | 'cycle'

export interface ChainProblem {
  code: ChainProblemCode
  nodeId: string
}

export interface ChainResult {
  /** Ordered from the head of the chain to `endId`, both included. */
  nodes: StudioNode[]
  problem: ChainProblem | null
}

/**
 * Walks backwards along main-input edges from `endId`. Used for sink chains and,
 * with a join as the end node, for the right-hand sub-chain.
 */
export function chainToSink(graph: StudioGraph, endId: string): ChainResult {
  const end = nodeById(graph, endId)
  if (!end) return { nodes: [], problem: null }

  const reversed: StudioNode[] = [end]
  const visited = new Set<string>([end.id])
  let cursor = end

  for (;;) {
    const parents = primaryParents(graph, cursor.id)
    if (parents.length === 0) break
    if (parents.length > 1) {
      return { nodes: [], problem: { code: 'multiple-parents', nodeId: cursor.id } }
    }
    const parent = parents[0]
    if (visited.has(parent.id)) {
      return { nodes: [], problem: { code: 'cycle', nodeId: parent.id } }
    }
    visited.add(parent.id)
    reversed.push(parent)
    cursor = parent
  }

  reversed.reverse()
  return { nodes: reversed, problem: null }
}

/** Longest prefix shared by every list. An empty input yields an empty prefix. */
export function longestCommonPrefix<T>(
  lists: readonly (readonly T[])[],
  isEqual: (a: T, b: T) => boolean = (a, b) => a === b,
): T[] {
  if (lists.length === 0) return []
  const [first, ...rest] = lists
  const prefix: T[] = []
  for (let index = 0; index < first.length; index += 1) {
    const candidate = first[index]
    const shared = rest.every((list) => index < list.length && isEqual(list[index], candidate))
    if (!shared) break
    prefix.push(candidate)
  }
  return prefix
}

/* -------------------------------------------------------------- factories */

export function newNodeId(kind: NodeKind): string {
  return `${kind}-${nanoid(8)}`
}

export function makeEdge(
  sourceId: string,
  targetId: string,
  targetHandle: string = HANDLE.in,
): StudioEdge {
  return {
    id: `e-${sourceId}-${targetHandle}-${targetId}`,
    source: sourceId,
    target: targetId,
    sourceHandle: HANDLE.out,
    targetHandle,
  }
}
