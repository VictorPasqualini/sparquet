/**
 * The editing surface.
 *
 * Everything here is a thin wire between React Flow and the editor store: the
 * store owns the graph, this component owns pointer affordances (drag & drop,
 * quick add, connection rules) and the canvas chrome.
 *
 * A `<ReactFlowProvider>` must sit above this component — the editor screen
 * provides it so panels outside the canvas can use the same instance.
 */

import {
  Background,
  BackgroundVariant,
  ConnectionLineType,
  Controls,
  MiniMap,
  ReactFlow,
  useReactFlow,
  type Connection,
  type DefaultEdgeOptions,
  type Node,
  type XYPosition,
} from '@xyflow/react'
import {
  LayoutTemplate,
  Link2,
  Plus,
  Search,
  Sparkles,
  Workflow,
  type LucideIcon,
} from 'lucide-react'
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type DragEvent,
  type KeyboardEvent,
  type MouseEvent,
} from 'react'
import { useNavigate } from 'react-router-dom'

import {
  getTransformation,
  READABLE_FORMATS,
  searchCatalog,
  WRITABLE_FORMATS,
  type NodeAccent,
} from '@/catalog'
import { Button, EmptyState, Input, Kbd } from '@/components/ui'
import { cn } from '@/lib/utils/cn'
import { useEditorStore } from '@/store/editor'
import { useSettingsStore } from '@/store/settings'
import {
  HANDLE,
  type NodeKind,
  type StudioEdge,
  type StudioNode,
  type StudioNodeData,
} from '@/types/studio'

import { catalogIcon } from './icons'
import { edgeTypes, nodeTypes } from './nodeTypes'
import { cancelConnect, startConnect, useConnectSource } from './NodeShell'

/** Payload key the palette writes into `dataTransfer`. */
export const NODE_DND_MIME = 'application/sparquet-node'

interface NodeDropPayload {
  kind: NodeKind
  /** Transformation registry key, for `kind: 'transform'`. */
  type?: string
  /** IO format id, for `kind: 'source' | 'sink'`. */
  format?: string
}

const GRID: [number, number] = [16, 16]
const PRO_OPTIONS = { hideAttribution: false }
/**
 * `minZoom` keeps node labels readable on long chains: fitting a 12-node
 * pipeline into the viewport would otherwise shrink it to an unreadable map.
 * The user pans from there.
 */
const FIT_VIEW_OPTIONS = { padding: 0.25, maxZoom: 1, minZoom: 0.55 }
const DEFAULT_EDGE_OPTIONS: DefaultEdgeOptions = { type: 'pipeline' }
/** Half a node, so a drop lands centred under the pointer. */
const DROP_OFFSET = { x: 132, y: 30 }
const QUICK_ADD_SIZE = { width: 288, height: 336 }

const ACCENT_COLOR: Record<NodeAccent, string> = {
  input: 'rgb(var(--node-input))',
  transform: 'rgb(var(--node-transform))',
  combine: 'rgb(var(--node-combine))',
  control: 'rgb(var(--node-control))',
  inspect: 'rgb(var(--node-inspect))',
  validate: 'rgb(var(--node-validate))',
  output: 'rgb(var(--node-output))',
}

export function FlowCanvas() {
  const nodes = useEditorStore((state) => state.nodes)
  const edges = useEditorStore((state) => state.edges)
  const onNodesChange = useEditorStore((state) => state.onNodesChange)
  const onEdgesChange = useEditorStore((state) => state.onEdgesChange)
  const onConnect = useEditorStore((state) => state.onConnect)
  const select = useEditorStore((state) => state.select)
  const addSource = useEditorStore((state) => state.addSource)
  const addSink = useEditorStore((state) => state.addSink)
  const addTransform = useEditorStore((state) => state.addTransform)
  const addValidations = useEditorStore((state) => state.addValidations)
  const addNote = useEditorStore((state) => state.addNote)
  const togglePanel = useEditorStore((state) => state.togglePanel)

  const canvas = useSettingsStore((state) => state.canvas)

  const navigate = useNavigate()
  const { screenToFlowPosition } = useReactFlow()
  const wrapperRef = useRef<HTMLDivElement>(null)

  const [quickAdd, setQuickAdd] = useState<QuickAddAnchor | null>(null)

  const addFromPayload = useCallback(
    (payload: NodeDropPayload, position: XYPosition) => {
      switch (payload.kind) {
        case 'source':
          addSource(position, payload.format)
          break
        case 'sink':
          addSink(position, payload.format)
          break
        case 'transform':
          if (payload.type) addTransform(payload.type, position)
          break
        case 'validations':
          addValidations(position)
          break
        case 'note':
          addNote(position)
          break
      }
    },
    [addNote, addSink, addSource, addTransform, addValidations],
  )

  /* ------------------------------------------------------------ drag & drop */

  const onDragOver = useCallback((event: DragEvent<HTMLDivElement>) => {
    if (!event.dataTransfer.types.includes(NODE_DND_MIME)) return
    event.preventDefault()
    // Must stay compatible with the palette's `effectAllowed = 'copy'`, or the
    // browser cancels the drop before `onDrop` ever fires.
    event.dataTransfer.dropEffect = 'copy'
  }, [])

  const onDrop = useCallback(
    (event: DragEvent<HTMLDivElement>) => {
      const raw = event.dataTransfer.getData(NODE_DND_MIME)
      if (!raw) return
      event.preventDefault()
      const payload = parsePayload(raw)
      if (!payload) return
      const point = screenToFlowPosition({ x: event.clientX, y: event.clientY })
      addFromPayload(payload, { x: point.x - DROP_OFFSET.x, y: point.y - DROP_OFFSET.y })
    },
    [addFromPayload, screenToFlowPosition],
  )

  /* --------------------------------------------------------- quick add menu */

  const closeQuickAdd = useCallback(() => setQuickAdd(null), [])

  const openQuickAdd = useCallback(
    (event: { clientX: number; clientY: number }) => {
      const bounds = wrapperRef.current?.getBoundingClientRect()
      if (!bounds) return
      const flow = screenToFlowPosition({ x: event.clientX, y: event.clientY })
      setQuickAdd({
        left: clamp(event.clientX - bounds.left, 8, bounds.width - QUICK_ADD_SIZE.width - 8),
        top: clamp(event.clientY - bounds.top, 8, bounds.height - QUICK_ADD_SIZE.height - 8),
        flow: { x: flow.x - DROP_OFFSET.x, y: flow.y - DROP_OFFSET.y },
      })
    },
    [screenToFlowPosition],
  )

  const handleDoubleClick = useCallback(
    (event: MouseEvent<HTMLDivElement>) => {
      // Only the empty pane opens the menu; nodes and edges keep their own behaviour.
      const target = event.target as HTMLElement
      if (!target.classList.contains('react-flow__pane')) return
      openQuickAdd(event)
    },
    [openQuickAdd],
  )

  /* -------------------------------------------------------------- selection */

  const handlePaneClick = useCallback(() => {
    select(null)
    closeQuickAdd()
  }, [closeQuickAdd, select])

  const handleNodeClick = useCallback(
    (_event: MouseEvent, node: StudioNode) => {
      select(node.id)
      closeQuickAdd()
    },
    [closeQuickAdd, select],
  )

  /* ------------------------------------------------------------ connections */

  const isValidConnection = useCallback(
    (connection: StudioEdge | Connection) => {
      const { source, target } = connection
      if (!source || !target || source === target) return false
      const sourceNode = nodes.find((node) => node.id === source)
      const targetNode = nodes.find((node) => node.id === target)
      if (!sourceNode || !targetNode) return false
      // Notes are annotations, and a source reads from storage — neither takes input.
      if (sourceNode.data.kind === 'note' || targetNode.data.kind === 'note') return false
      if (targetNode.data.kind === 'source') return false
      return !reaches(edges, target, source)
    },
    [edges, nodes],
  )

  /* --------------------------------------------------- keyboard connections */

  const connectSource = useConnectSource()

  // The pending source lives outside React, so leaving the editor has to clear it.
  useEffect(() => cancelConnect, [])

  /**
   * Handles are pointer-only, so `C` is the keyboard equivalent of dragging one:
   * press it on the focused source node, tab to the target, press it again.
   */
  const handleKeyDown = useCallback(
    (event: KeyboardEvent<HTMLDivElement>) => {
      if (event.key === 'Escape' && connectSource !== null) {
        event.preventDefault()
        cancelConnect()
        return
      }
      if (event.key.toLowerCase() !== 'c') return
      if (event.metaKey || event.ctrlKey || event.altKey) return

      const element = event.target as HTMLElement
      if (!element.classList.contains('react-flow__node')) return
      const nodeId = element.getAttribute('data-id')
      if (!nodeId) return
      const node = nodes.find((item) => item.id === nodeId)
      if (!node) return

      if (connectSource === null) {
        if (node.data.kind === 'sink' || node.data.kind === 'note') return
        event.preventDefault()
        startConnect(nodeId)
        return
      }

      event.preventDefault()
      if (connectSource === nodeId) {
        cancelConnect()
        return
      }
      onConnect({
        source: connectSource,
        target: nodeId,
        sourceHandle: HANDLE.out,
        targetHandle: freeTargetHandle(node, edges),
      })
      cancelConnect()
      select(nodeId)
    },
    [connectSource, edges, nodes, onConnect, select],
  )

  /* ------------------------------------------------------------------ empty */

  const addFirstSource = useCallback(() => {
    const bounds = wrapperRef.current?.getBoundingClientRect()
    const point = bounds
      ? screenToFlowPosition({
          x: bounds.left + bounds.width / 2,
          y: bounds.top + bounds.height / 2,
        })
      : { x: 0, y: 0 }
    addSource({ x: point.x - DROP_OFFSET.x, y: point.y - DROP_OFFSET.y })
  }, [addSource, screenToFlowPosition])

  return (
    <div
      ref={wrapperRef}
      className="relative h-full w-full"
      onDrop={onDrop}
      onDragOver={onDragOver}
      onDoubleClick={handleDoubleClick}
      onKeyDown={handleKeyDown}
    >
      <ReactFlow<StudioNode, StudioEdge>
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        proOptions={PRO_OPTIONS}
        fitView
        fitViewOptions={FIT_VIEW_OPTIONS}
        defaultEdgeOptions={DEFAULT_EDGE_OPTIONS}
        connectionLineType={ConnectionLineType.SmoothStep}
        snapToGrid={canvas.snapToGrid}
        snapGrid={GRID}
        isValidConnection={isValidConnection}
        onNodeClick={handleNodeClick}
        onPaneClick={handlePaneClick}
        onMoveStart={closeQuickAdd}
        zoomOnDoubleClick={false}
        deleteKeyCode={['Backspace', 'Delete']}
        minZoom={0.2}
        maxZoom={2}
        className="bg-canvas"
      >
        {canvas.showGrid && (
          <Background
            variant={BackgroundVariant.Dots}
            gap={20}
            size={1.4}
            color="rgb(var(--grid-dot))"
          />
        )}
        <Controls
          showInteractive={false}
          className="rounded-lg border border-line shadow-card"
        />
        {canvas.showMinimap && (
          <MiniMap
            pannable
            zoomable
            ariaLabel="Pipeline overview"
            nodeColor={miniMapColor}
            nodeStrokeWidth={2}
            nodeBorderRadius={4}
            bgColor="rgb(var(--surface))"
            maskColor="rgb(var(--canvas) / 0.6)"
            className="rounded-xl border border-line shadow-card"
          />
        )}
      </ReactFlow>

      {nodes.length === 0 && !quickAdd && (
        <div className="pointer-events-none absolute inset-0 z-10 flex items-center justify-center p-6">
          <div className="pointer-events-auto w-full max-w-md rounded-2xl border border-line bg-surface/95 shadow-card animate-fade-in">
            <EmptyState
              icon={<Workflow />}
              title="Nothing on the canvas yet"
              description="Every pipeline starts with one input. Drag a node in from the palette, or pick a starting point below."
              action={
                <div className="flex flex-wrap items-center justify-center gap-2">
                  <Button variant="primary" size="sm" icon={<Plus />} onClick={addFirstSource}>
                    Add a source
                  </Button>
                  <Button
                    size="sm"
                    icon={<LayoutTemplate />}
                    onClick={() => navigate('/templates')}
                  >
                    Start from a template
                  </Button>
                  <Button size="sm" icon={<Sparkles />} onClick={() => togglePanel('ai', true)}>
                    Ask the AI
                  </Button>
                </div>
              }
            />
            <p className="border-t border-line px-6 py-2.5 text-center text-2xs text-content-subtle">
              Tip: double-click anywhere on the canvas to search for a node. Focus a node and
              press <Kbd>c</Kbd> to wire it to another one.
            </p>
          </div>
        </div>
      )}

      {connectSource !== null && (
        <div
          role="status"
          className="pointer-events-none absolute inset-x-0 bottom-4 z-20 flex justify-center px-4"
        >
          <p className="flex flex-wrap items-center justify-center gap-1.5 rounded-full border border-brand-500/40 bg-surface-overlay px-3 py-1.5 text-2xs text-content shadow-pop">
            <Link2 className="h-3.5 w-3.5 text-brand-600 dark:text-brand-400" aria-hidden />
            <span>Choose the node to connect to —</span>
            <Kbd>tab</Kbd>
            to move,
            <Kbd>c</Kbd>
            to connect,
            <Kbd>esc</Kbd>
            to cancel
          </p>
        </div>
      )}

      {quickAdd && (
        <QuickAdd
          anchor={quickAdd}
          onClose={closeQuickAdd}
          onPick={(payload) => {
            addFromPayload(payload, quickAdd.flow)
            closeQuickAdd()
          }}
        />
      )}
    </div>
  )
}

/* ------------------------------------------------------------------ quick add */

interface QuickAddAnchor {
  left: number
  top: number
  flow: XYPosition
}

interface QuickAddOption {
  id: string
  label: string
  hint: string
  icon: LucideIcon
  accent: NodeAccent
  payload: NodeDropPayload
}

function QuickAdd({
  anchor,
  onClose,
  onPick,
}: {
  anchor: QuickAddAnchor
  onClose: () => void
  onPick: (payload: NodeDropPayload) => void
}) {
  const [query, setQuery] = useState('')
  const [active, setActive] = useState(0)
  const activeRef = useRef<HTMLButtonElement>(null)

  const options = useMemo(() => buildOptions(query), [query])
  const current = options[Math.min(active, options.length - 1)]

  useEffect(() => setActive(0), [query])
  useEffect(() => {
    activeRef.current?.scrollIntoView({ block: 'nearest' })
  }, [active])

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault()
      if (options.length === 0) return
      const step = event.key === 'ArrowDown' ? 1 : -1
      setActive((index) => (index + step + options.length) % options.length)
      return
    }
    if (event.key === 'Enter' && current) {
      event.preventDefault()
      onPick(current.payload)
    }
  }

  return (
    <div
      role="dialog"
      aria-label="Add a node"
      // Escape lives on the wrapper: it is the only way out of the popup, so it
      // must fire wherever focus happens to be inside it.
      onKeyDown={(event) => {
        if (event.key !== 'Escape') return
        event.preventDefault()
        onClose()
      }}
      className="absolute z-20 w-72 rounded-xl border border-line bg-surface-overlay p-2 shadow-pop animate-slide-up"
      style={{ left: anchor.left, top: anchor.top }}
    >
      <Input
        autoFocus
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        onKeyDown={onKeyDown}
        placeholder="Search nodes…"
        aria-label="Search nodes"
        role="combobox"
        aria-expanded
        aria-controls="quick-add-options"
        aria-activedescendant={current ? `quick-add-${current.id}` : undefined}
        leading={<Search />}
        className="h-9 py-0 text-xs"
      />

      <ul
        id="quick-add-options"
        role="listbox"
        aria-label="Nodes"
        className="scroll-area mt-1.5 max-h-64 space-y-0.5"
      >
        {options.map((option, index) => {
          const Icon = option.icon
          const selected = option === current
          return (
            <li key={option.id}>
              <button
                ref={selected ? activeRef : null}
                id={`quick-add-${option.id}`}
                role="option"
                aria-selected={selected}
                // The combobox above drives this list with aria-activedescendant,
                // so focus must never leave the input for an option.
                tabIndex={-1}
                type="button"
                onMouseEnter={() => setActive(index)}
                onClick={() => onPick(option.payload)}
                className={cn(
                  'flex w-full items-center gap-2 rounded-lg px-2 py-1.5 text-left transition-colors',
                  selected ? 'bg-brand-500/12 text-content' : 'text-content-muted',
                )}
              >
                <Icon
                  className={cn('h-3.5 w-3.5 shrink-0', ACCENT_TEXT[option.accent])}
                  aria-hidden
                />
                <span className="min-w-0 flex-1 truncate text-xs">{option.label}</span>
                <span className="shrink-0 text-2xs text-content-subtle">{option.hint}</span>
              </button>
            </li>
          )
        })}
        {options.length === 0 && (
          <li className="px-2 py-6 text-center text-2xs text-content-subtle">
            Nothing matches “{query}”.
          </li>
        )}
      </ul>

      <p className="mt-1.5 flex items-center justify-center gap-1.5 border-t border-line pt-1.5 text-2xs text-content-subtle">
        <Kbd>↑</Kbd>
        <Kbd>↓</Kbd>
        to browse
        <Kbd>↵</Kbd>
        to add
        <Kbd>esc</Kbd>
        to close
      </p>
    </div>
  )
}

const ACCENT_TEXT: Record<NodeAccent, string> = {
  input: 'text-node-input',
  transform: 'text-node-transform',
  combine: 'text-node-combine',
  control: 'text-node-control',
  inspect: 'text-node-inspect',
  validate: 'text-node-validate',
  output: 'text-node-output',
}

/** Sources first: an empty query is usually someone looking for a starting point. */
function buildOptions(query: string): QuickAddOption[] {
  const found = searchCatalog(query)
  const options: QuickAddOption[] = []

  for (const format of READABLE_FORMATS) {
    if (!found.formats.includes(format)) continue
    options.push({
      id: `source-${format.id}`,
      label: `Read ${format.label}`,
      hint: 'source',
      icon: catalogIcon(format.icon),
      accent: 'input',
      payload: { kind: 'source', format: format.id },
    })
  }

  for (const transformation of found.transformations) {
    options.push({
      id: `transform-${transformation.type}`,
      label: transformation.label,
      hint: transformation.type,
      icon: catalogIcon(transformation.icon),
      accent: transformation.accent,
      payload: { kind: 'transform', type: transformation.type },
    })
  }

  for (const format of WRITABLE_FORMATS) {
    if (!found.formats.includes(format)) continue
    options.push({
      id: `sink-${format.id}`,
      label: `Write ${format.label}`,
      hint: 'output',
      icon: catalogIcon(format.icon),
      accent: 'output',
      payload: { kind: 'sink', format: format.id },
    })
  }

  if (found.validators.length > 0 || matches(query, 'validations quality rules')) {
    options.push({
      id: 'validations',
      label: 'Validations',
      hint: 'quality',
      icon: catalogIcon('ShieldCheck'),
      accent: 'validate',
      payload: { kind: 'validations' },
    })
  }

  if (matches(query, 'note sticky comment')) {
    options.push({
      id: 'note',
      label: 'Note',
      hint: 'canvas only',
      icon: catalogIcon('StickyNote'),
      accent: 'inspect',
      payload: { kind: 'note' },
    })
  }

  return options.slice(0, 40)
}

function matches(query: string, haystack: string): boolean {
  const term = query.trim().toLowerCase()
  return term === '' || haystack.includes(term)
}

/* ---------------------------------------------------------------- utilities */

function parsePayload(raw: string): NodeDropPayload | null {
  try {
    const parsed: unknown = JSON.parse(raw)
    if (!parsed || typeof parsed !== 'object') return null
    const candidate = parsed as Partial<NodeDropPayload>
    if (!candidate.kind) return null
    return { kind: candidate.kind, type: candidate.type, format: candidate.format }
  } catch {
    return null
  }
}

/**
 * Where a keyboard connection lands. Two-input transforms (join, union) fill the
 * left handle first and spill into the right one, so a second `C` on the same
 * node adds the other side instead of replacing what is already there.
 */
export function freeTargetHandle(node: StudioNode, edges: StudioEdge[]): string {
  const dual =
    node.data.kind === 'transform' &&
    getTransformation(node.data.transform)?.secondaryInput === true
  if (!dual) return HANDLE.in
  const leftTaken = edges.some(
    (edge) => edge.target === node.id && (edge.targetHandle ?? HANDLE.in) === HANDLE.in,
  )
  return leftTaken ? HANDLE.inRight : HANDLE.in
}

/** Can `from` already reach `goal`? A new edge closing that walk would be a cycle. */
function reaches(edges: StudioEdge[], from: string, goal: string): boolean {
  const adjacency = new Map<string, string[]>()
  for (const edge of edges) {
    const targets = adjacency.get(edge.source) ?? []
    targets.push(edge.target)
    adjacency.set(edge.source, targets)
  }

  const stack = [from]
  const seen = new Set<string>()
  while (stack.length > 0) {
    const current = stack.pop()
    if (current === undefined) break
    if (current === goal) return true
    if (seen.has(current)) continue
    seen.add(current)
    stack.push(...(adjacency.get(current) ?? []))
  }
  return false
}

function miniMapColor(node: Node): string {
  const data = node.data as StudioNodeData
  switch (data.kind) {
    case 'source':
      return ACCENT_COLOR.input
    case 'transform':
      return ACCENT_COLOR[getTransformation(data.transform)?.accent ?? 'transform']
    case 'validations':
      return ACCENT_COLOR.validate
    case 'sink':
      return ACCENT_COLOR.output
    default:
      return 'rgb(var(--line-strong))'
  }
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), Math.max(min, max))
}
