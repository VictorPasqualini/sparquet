/**
 * Inspector — the form for the selected node.
 *
 * Everything the catalog knows about a node is rendered here: its fields, the traps
 * worth knowing before running it, and examples. Writes always go through the editor
 * store (`updateNodeData` / `updateNodeParam`) so a single undo covers every edit
 * made from this panel.
 */

import {
  AlertTriangle,
  ChevronRight,
  CircleAlert,
  Copy,
  CopyPlus,
  Database,
  Eye,
  EyeOff,
  Info,
  Lightbulb,
  MessageSquare,
  MoreHorizontal,
  MousePointerClick,
  Settings2,
  ShieldCheck,
  SlidersHorizontal,
  StickyNote,
  Trash2,
  Wand2,
} from 'lucide-react'
import * as LucideIcons from 'lucide-react'
import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ComponentType,
  type ReactNode,
} from 'react'
import { toast } from 'sonner'

import {
  getFormat,
  getTransformation,
  getValidationSink,
  getValidator,
  READABLE_FORMATS,
  WRITABLE_FORMATS,
  type CatalogExample,
  type FieldSpec,
  type NodeAccent,
} from '@/catalog'
import {
  Badge,
  Button,
  EmptyState,
  Field,
  IconButton,
  Input,
  Kbd,
  Menu,
  MenuContent,
  MenuItem,
  MenuSeparator,
  MenuTrigger,
  Popover,
  PopoverContent,
  PopoverTrigger,
  renderInlineCode,
  Segmented,
  Select,
  Textarea,
  Toggle,
  useConfirm,
} from '@/components/ui'
import { useValidationSinkRole } from '@/components/canvas/nodes/SinkNode'
import { cn } from '@/lib/utils/cn'
import { useEditorStore } from '@/store/editor'
import type {
  NoteNodeData,
  SinkNodeData,
  SourceNodeData,
  StudioNodeData,
  TransformNodeData,
  ValidationNodeData,
} from '@/types/studio'

import { fieldAnchorId, FieldRenderer, focusField } from './fields/FieldRenderer'
import { JsonField, StringListField } from './fields/widgets'

type IconComponent = ComponentType<{ className?: string }>

/** The catalog stores lucide names; this resolves them without a second registry. */
const ICONS = LucideIcons as unknown as Record<string, IconComponent | undefined>

function iconByName(name: string | undefined, fallback: IconComponent): IconComponent {
  return (name ? ICONS[name] : undefined) ?? fallback
}

const ACCENT_BADGE: Record<NodeAccent, string> = {
  input: 'bg-node-input/12 text-node-input',
  transform: 'bg-node-transform/12 text-node-transform',
  combine: 'bg-node-combine/12 text-node-combine',
  control: 'bg-node-control/12 text-node-control',
  inspect: 'bg-node-inspect/12 text-node-inspect',
  validate: 'bg-node-validate/12 text-node-validate',
  output: 'bg-node-output/12 text-node-output',
}

interface NodeDescriptor {
  icon: IconComponent
  accent: NodeAccent
  /** Short type chip, e.g. `filter` or `delta · sink`. */
  type: string
  fallbackLabel: string
}

function describeNode(data: StudioNodeData): NodeDescriptor {
  if (data.kind === 'transform') {
    const def = getTransformation(data.transform)
    return {
      icon: iconByName(def?.icon, Wand2),
      accent: def?.accent ?? 'transform',
      type: data.transform,
      fallbackLabel: def?.label ?? data.transform,
    }
  }
  if (data.kind === 'source' || data.kind === 'sink') {
    const format = getFormat(data.format)
    return {
      icon: iconByName(format?.icon, Database),
      accent: data.kind === 'source' ? 'input' : 'output',
      type: `${data.format} · ${data.kind === 'source' ? 'input' : 'output'}`,
      fallbackLabel: format?.label ?? data.format,
    }
  }
  if (data.kind === 'validation') {
    const def = getValidator(data.validator)
    return {
      icon: iconByName(def?.icon, ShieldCheck),
      accent: 'validate',
      type: `${data.validator} · rule`,
      fallbackLabel: def?.label ?? data.validator,
    }
  }
  return { icon: StickyNote, accent: 'control', type: 'note', fallbackLabel: 'Note' }
}

/** Pipeline-shaped JSON for the node, for pasting into a config by hand. */
function nodeJson(data: StudioNodeData): unknown {
  if (data.kind === 'transform') {
    return {
      type: data.transform,
      ...(data.skipIfFalse ? { skip_if_false: data.skipIfFalse } : {}),
      ...data.params,
    }
  }
  if (data.kind === 'source') {
    return {
      format: data.format,
      path: data.path,
      ...(Object.keys(data.options).length > 0 ? { options: data.options } : {}),
    }
  }
  if (data.kind === 'sink') {
    return {
      format: data.format,
      path: data.path,
      mode: data.mode,
      ...(data.partitionBy.length > 0 ? { partition_by: data.partitionBy } : {}),
      ...(data.columns ? { columns: data.columns } : {}),
      ...(Object.keys(data.options).length > 0 ? { options: data.options } : {}),
    }
  }
  if (data.kind === 'validation') {
    return { type: data.validator, ...data.params }
  }
  return { text: data.text, tone: data.tone }
}

/* ------------------------------------------------------------- field focus */

interface FocusRequest {
  /** Anchor scope the field was rendered under. */
  nodeId: string
  key: string
  /** Bumped per click, so jumping twice to the same field is still a new request. */
  seq: number
}

const FocusRequestContext = createContext<FocusRequest | null>(null)

/** Runs the handler once per jump request — sections use it to open themselves. */
function useFocusRequest(handle: (request: FocusRequest) => void): void {
  const request = useContext(FocusRequestContext)
  const handled = useRef(0)
  useEffect(() => {
    if (!request || handled.current === request.seq) return
    handled.current = request.seq
    handle(request)
  })
}

interface AnchorScope {
  nodeId: string
  /** Part of the issue path that precedes the field key, e.g. `options.`. */
  prefix: string
  keys: string[]
}

function anchorKeys(
  fields: readonly FieldSpec[] | undefined,
  params: Record<string, unknown>,
): string[] {
  return (fields ?? [])
    .filter((field) => !field.visibleWhen || field.visibleWhen(params))
    .map((field) => field.key)
}

/** Every field this node's form actually renders an anchor for, by issue-path prefix. */
function anchorScopes(nodeId: string, data: StudioNodeData): AnchorScope[] {
  if (data.kind === 'source') {
    const format = getFormat(data.format)
    return [
      { nodeId, prefix: 'options.', keys: anchorKeys(format?.readOptions, data.options) },
      { nodeId, prefix: '', keys: ['format', 'path'] },
    ]
  }
  if (data.kind === 'sink') {
    const format = getFormat(data.format)
    return [
      {
        nodeId,
        prefix: 'options.',
        keys: anchorKeys(format?.writeOptions, { ...data.options, mode: data.mode }),
      },
      {
        nodeId,
        prefix: '',
        keys: [
          'format',
          'path',
          'mode',
          ...(format?.supportsPartitioning ? ['partition_by'] : []),
          'columns',
        ],
      },
    ]
  }
  if (data.kind === 'transform') {
    const keys = anchorKeys(getTransformation(data.transform)?.fields, data.params)
    return [{ nodeId, prefix: '', keys: [...keys, SKIP_FIELD.key] }]
  }
  if (data.kind === 'validation') {
    return [
      { nodeId, prefix: '', keys: anchorKeys(getValidator(data.validator)?.fields, data.params) },
    ]
  }
  return []
}

/** Longest prefix of a JSON path that a field anchor exists for. */
function anchoredKey(path: string, keys: readonly string[]): string | null {
  let candidate = path
  while (candidate !== '') {
    if (keys.includes(candidate)) return candidate
    const cut = Math.max(candidate.lastIndexOf('.'), candidate.lastIndexOf('['))
    if (cut <= 0) return null
    candidate = candidate.slice(0, cut)
  }
  return null
}

/**
 * Issues carry JSON paths (`options.merge_keys`, `rules[0].columns`, `agg[1]`) while
 * anchors are per field. Resolves a path to the field the panel can actually focus,
 * or null when nothing is reachable — a dead link is worse than plain text.
 */
export function resolveIssueField(
  nodeId: string,
  data: StudioNodeData,
  field: string,
): { nodeId: string; key: string } | null {
  const scopes = anchorScopes(nodeId, data)
  for (const scope of scopes) {
    if (!field.startsWith(scope.prefix)) continue
    const key = anchoredKey(field.slice(scope.prefix.length), scope.keys)
    if (key) return { nodeId: scope.nodeId, key }
  }
  // Also accept a bare key where the path is reported unscoped, e.g. `merge_keys`.
  for (const scope of scopes) {
    const key = anchoredKey(field, scope.keys)
    if (key) return { nodeId: scope.nodeId, key }
  }
  return null
}

/* ------------------------------------------------------------------ shell */

export function Inspector() {
  const node = useEditorStore(
    (state) => state.nodes.find((item) => item.id === state.selectedNodeId) ?? null,
  )
  const [focus, setFocus] = useState<FocusRequest | null>(null)

  useEffect(() => {
    if (!focus) return
    // The section holding the field opens itself in its own effect, so the anchor
    // only exists once that update has been flushed — hence the deferred lookup.
    const frame = requestAnimationFrame(() => focusField(focus.nodeId, focus.key))
    return () => cancelAnimationFrame(frame)
  }, [focus])

  if (!node) return <InspectorEmpty />

  const data = node.data
  const jump = (scopeId: string, key: string) =>
    setFocus((previous) => ({ nodeId: scopeId, key, seq: (previous?.seq ?? 0) + 1 }))
  // A request belongs to the node it was raised on; selecting another must not replay it.
  const request = focus && focus.nodeId.startsWith(node.id) ? focus : null

  return (
    <FocusRequestContext.Provider value={request}>
      <div className="flex h-full min-h-0 flex-col">
        <InspectorHeader key={`header-${node.id}`} id={node.id} data={data} />
        <div key={`body-${node.id}`} className="scroll-area flex-1 space-y-4 px-3 py-3">
          <NodeIssues nodeId={node.id} data={data} onJump={jump} />
          {(data.kind === 'source' || data.kind === 'sink') && (
            <IoBody id={node.id} data={data} />
          )}
          {data.kind === 'transform' && <TransformBody id={node.id} data={data} />}
          {data.kind === 'validation' && <ValidationBody id={node.id} data={data} />}
          {data.kind === 'note' && <NoteBody id={node.id} data={data} />}
        </div>
      </div>
    </FocusRequestContext.Provider>
  )
}

const EMPTY_SHORTCUTS: { keys: string[]; label: string }[] = [
  { keys: ['⌘', 'D'], label: 'Duplicate the selected node' },
  { keys: ['Del'], label: 'Remove the selected node' },
  { keys: ['⌘', 'Z'], label: 'Undo the last change' },
]

function InspectorEmpty() {
  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex h-11 shrink-0 items-center gap-2 border-b border-line px-3">
        <SlidersHorizontal className="h-3.5 w-3.5 text-content-subtle" aria-hidden />
        <span className="text-xs font-medium text-content">Inspector</span>
      </div>
      <EmptyState
        className="flex-1"
        icon={<MousePointerClick />}
        title="No node selected"
        description="Click a node on the canvas to edit it here. Every change lands in the pipeline JSON as you type."
        action={
          <ul className="w-full max-w-xs space-y-1.5 rounded-xl border border-line bg-surface-sunken p-2.5 text-left">
            {EMPTY_SHORTCUTS.map((shortcut) => (
              <li key={shortcut.label} className="flex items-center justify-between gap-3">
                <span className="text-2xs text-content-muted">{shortcut.label}</span>
                <span className="flex shrink-0 items-center gap-0.5">
                  {shortcut.keys.map((key) => (
                    <Kbd key={key}>{key}</Kbd>
                  ))}
                </span>
              </li>
            ))}
          </ul>
        }
      />
    </div>
  )
}

function InspectorHeader({ id, data }: { id: string; data: StudioNodeData }) {
  const updateNodeData = useEditorStore((state) => state.updateNodeData)
  const duplicateNode = useEditorStore((state) => state.duplicateNode)
  const toggleDisabled = useEditorStore((state) => state.toggleDisabled)
  const removeNodes = useEditorStore((state) => state.removeNodes)
  const [confirm, confirmDialog] = useConfirm()

  const descriptor = describeNode(data)
  const Icon = descriptor.icon
  const [draft, setDraft] = useState(data.label ?? '')
  useEffect(() => setDraft(data.label ?? ''), [data.label])

  const commitLabel = () => {
    const trimmed = draft.trim()
    if (trimmed === (data.label ?? '')) return
    updateNodeData(id, { label: trimmed === '' ? undefined : trimmed })
  }

  const copyJson = async () => {
    await navigator.clipboard.writeText(JSON.stringify(nodeJson(data), null, 2))
    toast.success('Node JSON copied')
  }

  const remove = async () => {
    const confirmed = await confirm({
      title: 'Delete node',
      message:
        'The node and its connections are removed from the canvas. Undo brings them back.',
      confirmLabel: 'Delete',
    })
    if (confirmed) removeNodes([id])
  }

  // Transformations and validation rules are the two kinds a run can leave out.
  const canMute = data.kind === 'transform' || data.kind === 'validation'
  const muted = canMute && data.disabled === true

  return (
    <div className="shrink-0 border-b border-line px-3 py-2.5">
      <div className="flex items-center gap-2">
        <span
          className={cn(
            'flex h-7 w-7 shrink-0 items-center justify-center rounded-lg',
            ACCENT_BADGE[descriptor.accent],
          )}
        >
          <Icon className="h-4 w-4" />
        </span>

        <Input
          value={draft}
          placeholder={descriptor.fallbackLabel}
          aria-label="Node label"
          onChange={(event) => setDraft(event.target.value)}
          onBlur={commitLabel}
          onKeyDown={(event) => {
            if (event.key === 'Enter') event.currentTarget.blur()
            if (event.key === 'Escape') {
              setDraft(data.label ?? '')
              event.currentTarget.blur()
            }
          }}
          className="h-7 min-w-0 flex-1 border-transparent bg-transparent px-1.5 text-sm font-semibold hover:border-line focus:bg-surface-sunken"
        />

        <Menu>
          <MenuTrigger asChild>
            <IconButton label="Node actions" size="sm">
              <MoreHorizontal />
            </IconButton>
          </MenuTrigger>
          <MenuContent>
            <MenuItem icon={<CopyPlus />} shortcut="⌘D" onSelect={() => duplicateNode(id)}>
              Duplicate
            </MenuItem>
            {canMute && (
              <MenuItem icon={muted ? <Eye /> : <EyeOff />} onSelect={() => toggleDisabled(id)}>
                {muted ? 'Unmute' : 'Mute'}
              </MenuItem>
            )}
            <MenuItem icon={<Copy />} onSelect={() => void copyJson()}>
              Copy node JSON
            </MenuItem>
            <MenuSeparator />
            <MenuItem danger icon={<Trash2 />} shortcut="Del" onSelect={() => void remove()}>
              Delete
            </MenuItem>
          </MenuContent>
        </Menu>
      </div>

      <div className="mt-1.5 flex items-center gap-1.5 pl-9">
        <span className="chip font-mono">{descriptor.type}</span>
        {muted && <Badge tone="warning">Muted</Badge>}
      </div>
      {confirmDialog}
    </div>
  )
}

function NodeIssues({
  nodeId,
  data,
  onJump,
}: {
  nodeId: string
  data: StudioNodeData
  onJump: (scopeId: string, key: string) => void
}) {
  const issues = useEditorStore((state) => state.issues)
  const mine = useMemo(
    () => issues.filter((issue) => issue.nodeId === nodeId),
    [issues, nodeId],
  )

  if (mine.length === 0) return null

  return (
    <ul className="space-y-1">
      {mine.map((issue) => {
        const tone =
          issue.severity === 'error'
            ? 'text-state-danger'
            : issue.severity === 'warning'
              ? 'text-state-warning'
              : 'text-state-info'

        const body = (
          <>
            <CircleAlert className={cn('mt-px h-3.5 w-3.5 shrink-0', tone)} aria-hidden />
            <span className="min-w-0">
              <span className="block text-2xs leading-relaxed text-content">
                {issue.message}
              </span>
              {issue.hint && (
                <span className="block text-2xs leading-relaxed text-content-subtle">
                  {issue.hint}
                </span>
              )}
            </span>
          </>
        )

        const target = issue.field ? resolveIssueField(nodeId, data, issue.field) : null

        return (
          <li key={issue.id}>
            {target ? (
              <button
                type="button"
                onClick={() => onJump(target.nodeId, target.key)}
                className="flex w-full items-start gap-2 rounded-lg border border-line bg-surface-sunken px-2 py-1.5 text-left transition-colors hover:border-line-strong"
              >
                {body}
              </button>
            ) : (
              <div className="flex items-start gap-2 rounded-lg border border-line bg-surface-sunken px-2 py-1.5">
                {body}
              </div>
            )}
          </li>
        )
      })}
    </ul>
  )
}

/* --------------------------------------------------------------- building blocks */

function Section({
  title,
  count,
  defaultOpen = true,
  action,
  opensFor,
  children,
}: {
  title: string
  count?: number
  defaultOpen?: boolean
  action?: ReactNode
  /** Opens the section when an issue jumps to a field rendered inside it. */
  opensFor?: (request: FocusRequest) => boolean
  children: ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)

  useFocusRequest((request) => {
    if (opensFor?.(request)) setOpen(true)
  })

  return (
    <section className="rounded-xl border border-line bg-surface-raised">
      <div className="flex items-center gap-1 pr-2">
        <button
          type="button"
          aria-expanded={open}
          onClick={() => setOpen((value) => !value)}
          className="flex flex-1 items-center gap-2 rounded-xl px-2.5 py-2 text-left"
        >
          <ChevronRight
            className={cn(
              'h-3.5 w-3.5 shrink-0 text-content-subtle transition-transform',
              open && 'rotate-90',
            )}
            aria-hidden
          />
          <span className="text-2xs font-semibold uppercase tracking-wider text-content-subtle">
            {title}
          </span>
          {count !== undefined && count > 0 && <span className="chip">{count}</span>}
        </button>
        {action}
      </div>
      {open && <div className="space-y-3 border-t border-line px-2.5 py-3">{children}</div>}
    </section>
  )
}

function Callout({
  tone = 'info',
  icon,
  children,
}: {
  tone?: 'info' | 'warning'
  icon?: ReactNode
  children: ReactNode
}) {
  return (
    <div
      className={cn(
        'flex items-start gap-2 rounded-lg border p-2.5 text-2xs leading-relaxed',
        tone === 'info'
          ? 'border-state-info/25 bg-state-info/5 text-content-muted'
          : 'border-state-warning/25 bg-state-warning/5 text-content-muted',
      )}
    >
      <span
        className={cn(
          'mt-px shrink-0 [&_svg]:h-3.5 [&_svg]:w-3.5',
          tone === 'info' ? 'text-state-info' : 'text-state-warning',
        )}
      >
        {icon ?? <Info />}
      </span>
      <div className="min-w-0">{children}</div>
    </div>
  )
}

const GOTCHA_PREVIEW = 3

function GotchaList({ items }: { items: string[] }) {
  const [expanded, setExpanded] = useState(false)
  if (items.length === 0) return null

  const shown = expanded ? items : items.slice(0, GOTCHA_PREVIEW)

  return (
    <div className="rounded-xl border border-state-warning/25 bg-state-warning/5 p-2.5">
      <p className="mb-1.5 flex items-center gap-1.5 text-2xs font-semibold uppercase tracking-wider text-state-warning">
        <AlertTriangle className="h-3 w-3" aria-hidden />
        Worth knowing
      </p>
      <ul className="space-y-1.5">
        {shown.map((item) => (
          <li key={item} className="flex gap-1.5 text-2xs leading-relaxed text-content-muted">
            <span
              className="mt-1.5 h-1 w-1 shrink-0 rounded-full bg-state-warning/70"
              aria-hidden
            />
            <span className="min-w-0">{renderInlineCode(item)}</span>
          </li>
        ))}
      </ul>
      {items.length > GOTCHA_PREVIEW && (
        <button
          type="button"
          onClick={() => setExpanded((value) => !value)}
          className="mt-1.5 text-2xs font-medium text-brand-600 transition-colors hover:text-brand-500 dark:text-brand-400"
        >
          {expanded ? 'Show less' : `Show ${items.length - GOTCHA_PREVIEW} more`}
        </button>
      )}
    </div>
  )
}

function ExamplesPopover({ examples }: { examples: CatalogExample[] }) {
  if (examples.length === 0) return null

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button size="xs" variant="ghost" icon={<Lightbulb />}>
          Examples
        </Button>
      </PopoverTrigger>
      <PopoverContent className="max-h-[24rem] w-[22rem] space-y-3 overflow-y-auto p-3">
        {examples.map((example) => (
          <div key={example.title} className="space-y-1">
            <div className="flex items-start justify-between gap-2">
              <p className="text-2xs font-medium leading-relaxed text-content">
                {example.title}
              </p>
              <IconButton
                size="xs"
                label={`Copy example: ${example.title}`}
                onClick={() => {
                  void navigator.clipboard.writeText(example.json)
                  toast.success('Example copied')
                }}
              >
                <Copy />
              </IconButton>
            </div>
            <pre className="overflow-x-auto rounded-lg border border-line bg-surface-sunken p-2 font-mono text-2xs leading-relaxed text-content-muted">
              {example.json}
            </pre>
          </div>
        ))}
      </PopoverContent>
    </Popover>
  )
}

function CommentSection({ id, comment }: { id: string; comment: string | undefined }) {
  const updateNodeData = useEditorStore((state) => state.updateNodeData)

  return (
    <Section title="Notes" defaultOpen={Boolean(comment)}>
      <Textarea
        rows={3}
        value={comment ?? ''}
        aria-label="Node notes"
        placeholder="Why this node exists, what to watch for…"
        onChange={(event) => updateNodeData(id, { comment: event.target.value })}
      />
      <p className="text-2xs text-content-subtle">
        Kept in Studio only — never compiled into the pipeline JSON.
      </p>
    </Section>
  )
}

/* ------------------------------------------------------------------ source / sink */

const MODE_HINTS: Record<string, string> = {
  overwrite: 'Replaces what is at the destination.',
  append: 'Adds rows and keeps what is already there.',
  merge: 'Upsert on the merge keys — Delta and Iceberg only.',
  ignore: 'Skips the write when the destination already exists.',
  error: 'Fails when the destination already exists.',
}

/**
 * Keys the new format does not declare have no editor left, yet they would keep
 * compiling into `options` unseen — the switch drops them and keeps shared ones.
 * An unknown format declares nothing because the registries are extensible at
 * runtime, so its options are left untouched.
 */
export function retainOptions(
  options: Record<string, unknown>,
  specs: readonly FieldSpec[] | undefined,
): Record<string, unknown> {
  if (!specs) return options
  const allowed = new Set(specs.map((spec) => spec.key))
  return Object.fromEntries(Object.entries(options).filter(([key]) => allowed.has(key)))
}

function IoBody({ id, data }: { id: string; data: SourceNodeData | SinkNodeData }) {
  const updateNodeData = useEditorStore((state) => state.updateNodeData)
  const sink = data.kind === 'sink' ? data : null
  const sideRole = useValidationSinkRole(id)
  const sideDef = sink && sideRole ? getValidationSink(sideRole) : null
  const format = getFormat(data.format)
  const formats = sink ? WRITABLE_FORMATS : READABLE_FORMATS
  const optionSpecs = (sink ? format?.writeOptions : format?.readOptions) ?? []
  const mainOptions = optionSpecs.filter((spec) => spec.group !== 'advanced')
  const advancedOptions = optionSpecs.filter((spec) => spec.group === 'advanced')

  /** Write options branch on the output mode, which lives outside `options`. */
  const optionParams = useMemo(
    () => (sink ? { ...data.options, mode: sink.mode } : { ...data.options }),
    [data.options, sink],
  )

  const pathSpec = useMemo<FieldSpec>(
    () => ({
      key: 'path',
      label: format?.pathLabel ?? 'Path',
      type: 'text',
      required: true,
      placeholder: format?.pathPlaceholder,
      help: format?.pathHelp,
      supportsRuntimeVars: true,
      validate: (value) =>
        typeof value === 'string' && value.trim() !== ''
          ? null
          : `Set the ${(format?.pathLabel ?? 'path').toLowerCase()}.`,
    }),
    [format],
  )

  const setOption = (key: string, value: unknown) => {
    const options = { ...data.options }
    if (value === undefined || value === null || value === '') delete options[key]
    else options[key] = value
    updateNodeData(id, { options })
  }

  return (
    <div className="space-y-4">
      {sideDef && (
        <section
          aria-label={`${sideDef.label} — validation side output`}
          className="space-y-1.5 rounded-lg border border-state-info/25 bg-state-info/5 p-2.5"
        >
          <p className="text-2xs font-semibold text-content">
            {sideDef.label} · <span className="font-mono">{sideDef.jsonKey}</span>
          </p>
          <p className="text-2xs leading-relaxed text-content-muted">{sideDef.summary}</p>
          <p className="text-2xs leading-relaxed text-content-muted">{sideDef.caveat}</p>
        </section>
      )}

      <div id={fieldAnchorId(id, 'format')} className="scroll-mt-4">
        <Field label="Format" help={format?.summary} htmlFor={`${id}-format`}>
          <Select
            id={`${id}-format`}
            ariaLabel="Format"
            value={data.format}
            onValueChange={(next) => {
              // A format only accepts some write modes; keep the sink on a valid one.
              const nextFormat = getFormat(next)
              const keepsMode =
                !sink || !nextFormat || nextFormat.modes.some((mode) => mode === sink.mode)
              updateNodeData(id, {
                format: next,
                options: retainOptions(
                  data.options,
                  sink ? nextFormat?.writeOptions : nextFormat?.readOptions,
                ),
                ...(keepsMode ? {} : { mode: nextFormat?.modes[0] ?? 'overwrite' }),
              })
            }}
            options={formats.map((item) => ({
              value: item.id,
              label: item.label,
              hint: item.summary,
            }))}
          />
        </Field>
      </div>

      <FieldRenderer
        field={pathSpec}
        value={data.path}
        params={{}}
        nodeId={id}
        onChange={(value) =>
          updateNodeData(id, { path: typeof value === 'string' ? value : '' })
        }
      />

      {sink && (
        <>
          <div id={fieldAnchorId(id, 'mode')} className="scroll-mt-4">
            <Field
              label="Write mode"
              help={MODE_HINTS[String(sink.mode)]}
              htmlFor={`${id}-mode`}
            >
              <Select
                id={`${id}-mode`}
                ariaLabel="Write mode"
                value={String(sink.mode)}
                onValueChange={(next) => updateNodeData(id, { mode: next })}
                options={(format?.modes ?? []).map((mode) => ({
                  value: mode,
                  label: mode,
                  hint: MODE_HINTS[mode],
                }))}
              />
            </Field>
          </div>

          {format?.supportsPartitioning && (
            <div id={fieldAnchorId(id, 'partition_by')} className="scroll-mt-4">
              <Field
                label="Partition by"
                help="Columns used to lay the data out on disk. They keep their values but leave the written files."
              >
                <StringListField
                  value={sink.partitionBy}
                  placeholder="dt_ref"
                  onChange={(next) => updateNodeData(id, { partitionBy: next })}
                />
              </Field>
            </div>
          )}

          <div id={fieldAnchorId(id, 'columns')} className="scroll-mt-4 space-y-2">
            <Toggle
              checked={sink.columns !== null}
              onCheckedChange={(checked) =>
                updateNodeData(id, { columns: checked ? [] : null })
              }
              label="Project specific columns"
              description={
                sideRole === 'report'
                  ? 'The report builds its own fixed schema and is written without a select, so a projection here is ignored.'
                  : 'Off writes every column, including the auto-added ingestion_ts.'
              }
            />
            {sink.columns !== null && (
              <StringListField
                value={sink.columns}
                placeholder="id"
                onChange={(next) => updateNodeData(id, { columns: next })}
              />
            )}
          </div>
        </>
      )}

      {optionSpecs.length > 0 && (
        <Section
          title="Options"
          count={Object.keys(data.options).length}
          defaultOpen={Object.keys(data.options).length > 0}
          opensFor={(request) => optionSpecs.some((spec) => spec.key === request.key)}
        >
          {mainOptions.map((spec) => (
            <FieldRenderer
              key={spec.key}
              field={spec}
              value={data.options[spec.key]}
              params={optionParams}
              nodeId={id}
              onChange={(value) => setOption(spec.key, value)}
            />
          ))}
          {advancedOptions.length > 0 && (
            <Section
              title="Advanced"
              count={advancedOptions.length}
              defaultOpen={false}
              opensFor={(request) => advancedOptions.some((spec) => spec.key === request.key)}
            >
              {advancedOptions.map((spec) => (
                <FieldRenderer
                  key={spec.key}
                  field={spec}
                  value={data.options[spec.key]}
                  params={optionParams}
                  nodeId={id}
                  onChange={(value) => setOption(spec.key, value)}
                />
              ))}
            </Section>
          )}
        </Section>
      )}

      {format && (
        <div className="flex justify-end">
          <ExamplesPopover examples={format.examples} />
        </div>
      )}
      {format && <GotchaList items={format.gotchas} />}
      <CommentSection id={id} comment={data.comment} />
    </div>
  )
}

/* --------------------------------------------------------------------- transform */

const SKIP_FIELD: FieldSpec = {
  key: 'skip_if_false',
  label: 'skip_if_false',
  type: 'text',
  placeholder: '{aplicar_filtro}',
  help: 'Leave empty to always run this node.',
  supportsRuntimeVars: true,
  docs: [
    'Evaluated after `{param}` substitution, and only then:',
    '',
    'An empty string skips the node — that is what a `false` boolean, an empty list or a',
    'missing param becomes.',
    '',
    "A value that parses as a boolean expression over literals (`'REGISTRO' in ('EMISSAO')`)",
    'skips the node when it is false. It sees literals only, never the DataFrame columns.',
    '',
    'Any other non-empty value runs the node.',
  ].join('\n'),
}

const SKIP_CASES = [
  'Empty after substitution — a false param, an empty list, a missing key → skipped.',
  'A boolean expression over literals → skipped when it evaluates to false.',
  'Any other non-empty value → the node runs.',
]

function skipCaseIndex(raw: string): number {
  const text = raw.trim()
  if (text === '') return -1
  if (/[=<>]|\bin\b|\band\b|\bor\b|\bnot\b/i.test(text)) return 1
  if (text.includes('{')) return 0
  return 2
}

function TransformBody({ id, data }: { id: string; data: TransformNodeData }) {
  const def = getTransformation(data.transform)
  const updateNodeData = useEditorStore((state) => state.updateNodeData)
  const updateNodeParam = useEditorStore((state) => state.updateNodeParam)

  const mainFields = (def?.fields ?? []).filter((field) => field.group !== 'advanced')
  const advancedFields = (def?.fields ?? []).filter((field) => field.group === 'advanced')
  const skip = data.skipIfFalse ?? ''
  const activeCase = skipCaseIndex(skip)

  return (
    <div className="space-y-4">
      {def && (
        <div className="flex items-start justify-between gap-2">
          <p className="min-w-0 text-2xs leading-relaxed text-content-muted">
            {renderInlineCode(def.summary)}
          </p>
          <ExamplesPopover examples={def.examples} />
        </div>
      )}

      {!def && (
        <>
          <Callout tone="warning" icon={<AlertTriangle />}>
            <span className="font-medium text-content">
              {renderInlineCode(`\`${data.transform}\` is not in the catalog.`)}
            </span>{' '}
            Studio cannot draw a form for it, so its parameters are edited as raw JSON.
          </Callout>
          <Field label="Parameters" help="Written into the transformation exactly as typed.">
            <JsonField
              rows={10}
              value={data.params}
              onChange={(value) =>
                updateNodeData(id, {
                  params:
                    value && typeof value === 'object' && !Array.isArray(value)
                      ? (value as Record<string, unknown>)
                      : {},
                })
              }
            />
          </Field>
        </>
      )}

      {def?.secondaryInput && (
        <Callout>
          The second source comes from the node wired into this one&apos;s lower input handle,
          not from a field here.
          {def.supportsSubPipeline
            ? ' The chain feeding that handle runs before the join.'
            : ''}
        </Callout>
      )}

      {def?.emitsRuntimeVar && (
        <Callout>
          Publishes a runtime variable that later nodes read as{' '}
          <code className="font-mono text-brand-600 dark:text-brand-400">{'{{name}}'}</code>.
          Place it after a checkpoint so the collect does not recompute the chain.
        </Callout>
      )}

      {mainFields.map((field) => (
        <FieldRenderer
          key={field.key}
          field={field}
          value={data.params[field.key]}
          params={data.params}
          nodeId={id}
          onChange={(value) => updateNodeParam(id, field.key, value)}
        />
      ))}

      {advancedFields.length > 0 && (
        <Section
          title="Advanced"
          count={advancedFields.length}
          defaultOpen={false}
          opensFor={(request) => advancedFields.some((field) => field.key === request.key)}
        >
          {advancedFields.map((field) => (
            <FieldRenderer
              key={field.key}
              field={field}
              value={data.params[field.key]}
              params={data.params}
              nodeId={id}
              onChange={(value) => updateNodeParam(id, field.key, value)}
            />
          ))}
        </Section>
      )}

      <Section
        title="Run condition"
        defaultOpen={skip !== ''}
        opensFor={(request) => request.key === SKIP_FIELD.key}
      >
        <FieldRenderer
          field={SKIP_FIELD}
          value={skip}
          params={data.params}
          nodeId={id}
          onChange={(value) =>
            updateNodeData(id, {
              skipIfFalse: typeof value === 'string' && value.trim() !== '' ? value : undefined,
            })
          }
        />
        <ul className="space-y-1">
          {SKIP_CASES.map((text, index) => (
            <li
              key={text}
              className={cn(
                'flex gap-1.5 text-2xs leading-relaxed',
                index === activeCase ? 'text-content' : 'text-content-subtle',
              )}
            >
              <span
                className={cn(
                  'mt-1.5 h-1 w-1 shrink-0 rounded-full',
                  index === activeCase ? 'bg-brand-500' : 'bg-line-strong',
                )}
                aria-hidden
              />
              <span>{text}</span>
            </li>
          ))}
        </ul>
        {activeCase === -1 && (
          <p className="text-2xs text-content-subtle">No guard set — this node always runs.</p>
        )}
      </Section>

      {def && <GotchaList items={def.gotchas} />}
      <CommentSection id={id} comment={data.comment} />
    </div>
  )
}

/* -------------------------------------------------------------------- validation */

function ValidationBody({ id, data }: { id: string; data: ValidationNodeData }) {
  const def = getValidator(data.validator)
  const updateNodeData = useEditorStore((state) => state.updateNodeData)
  const updateNodeParam = useEditorStore((state) => state.updateNodeParam)
  const togglePanel = useEditorStore((state) => state.togglePanel)

  const mainFields = (def?.fields ?? []).filter((field) => field.group !== 'advanced')
  const advancedFields = (def?.fields ?? []).filter((field) => field.group === 'advanced')

  return (
    <div className="space-y-4">
      {def && (
        <div className="flex items-start justify-between gap-2">
          <p className="min-w-0 text-2xs leading-relaxed text-content-muted">
            {renderInlineCode(def.summary)}
          </p>
          <ExamplesPopover examples={def.examples} />
        </div>
      )}

      {!def && (
        <>
          <Callout tone="warning" icon={<AlertTriangle />}>
            <span className="font-medium text-content">
              {renderInlineCode(`\`${data.validator}\` is not a built-in validator.`)}
            </span>{' '}
            Register it before the run, or Studio can only edit its parameters as raw JSON.
          </Callout>
          <Field label="Parameters" help="Written into the rule exactly as typed.">
            <JsonField
              rows={10}
              value={data.params}
              onChange={(value) =>
                updateNodeData(id, {
                  params:
                    value && typeof value === 'object' && !Array.isArray(value)
                      ? (value as Record<string, unknown>)
                      : {},
                })
              }
            />
          </Field>
        </>
      )}

      {mainFields.map((field) => (
        <FieldRenderer
          key={field.key}
          field={field}
          value={data.params[field.key]}
          params={data.params}
          nodeId={id}
          onChange={(value) => updateNodeParam(id, field.key, value)}
        />
      ))}

      {advancedFields.length > 0 && (
        <Section
          title="Advanced"
          count={advancedFields.length}
          defaultOpen={false}
          opensFor={(request) => advancedFields.some((field) => field.key === request.key)}
        >
          {advancedFields.map((field) => (
            <FieldRenderer
              key={field.key}
              field={field}
              value={data.params[field.key]}
              params={data.params}
              nodeId={id}
              onChange={(value) => updateNodeParam(id, field.key, value)}
            />
          ))}
        </Section>
      )}

      <Callout icon={<ShieldCheck />}>
        <p>
          Rules measure the data without changing it — use a transformation when rows
          must actually go. Every rule on the chain compiles into one{' '}
          <span className="font-mono">validations</span> block.
        </p>
        <Button
          size="xs"
          variant="ghost"
          className="mt-1.5 -ml-1.5"
          icon={<Settings2 />}
          onClick={() => togglePanel('settings', true)}
        >
          What happens on failure
        </Button>
      </Callout>

      {def && <GotchaList items={def.gotchas} />}
      <CommentSection id={id} comment={data.comment} />
    </div>
  )
}

/* --------------------------------------------------------------------------- note */

const NOTE_TONES: { value: NoteNodeData['tone']; label: string }[] = [
  { value: 'brand', label: 'Brand' },
  { value: 'neutral', label: 'Neutral' },
  { value: 'info', label: 'Info' },
  { value: 'warning', label: 'Warning' },
]

function NoteBody({ id, data }: { id: string; data: NoteNodeData }) {
  const updateNodeData = useEditorStore((state) => state.updateNodeData)

  return (
    <div className="space-y-4">
      <Field
        label="Text"
        help="Canvas-only. Notes are never compiled into the pipeline JSON."
        htmlFor={`${id}-note-text`}
      >
        <Textarea
          id={`${id}-note-text`}
          rows={6}
          value={data.text}
          placeholder="What this part of the pipeline does…"
          onChange={(event) => updateNodeData(id, { text: event.target.value })}
        />
      </Field>

      <Field label="Tone">
        <Segmented
          size="sm"
          value={data.tone}
          onChange={(value) => updateNodeData(id, { tone: value })}
          options={NOTE_TONES}
        />
      </Field>

      <Callout icon={<MessageSquare />}>
        Use notes to explain a decision the JSON cannot carry — why a join is a{' '}
        <span className="font-mono">leftanti</span>, or which upstream table is the slow one.
      </Callout>
    </div>
  )
}
