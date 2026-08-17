import type { XYPosition } from '@xyflow/react'
import {
  ArrowUpDown,
  Box,
  Braces,
  Bug,
  ChevronRight,
  CircleSlash,
  Columns3,
  Combine,
  CopyMinus,
  Database,
  Droplet,
  FileCode2,
  FileSpreadsheet,
  FileSymlink,
  FileText,
  Filter,
  Fingerprint,
  Flag,
  GitMerge,
  Hash,
  Layers,
  Mountain,
  OctagonX,
  PenLine,
  Radio,
  Regex,
  Ruler,
  Search,
  SearchX,
  ShieldCheck,
  Sigma,
  StickyNote,
  Table2,
  Terminal,
  Trash2,
  Type,
  Variable,
  X,
  type LucideIcon,
} from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type { KeyboardEvent as ReactKeyboardEvent, ReactNode } from 'react'

import {
  READABLE_FORMATS,
  WRITABLE_FORMATS,
  searchCatalog,
  type FormatDef,
  type NodeAccent,
  type NodeFamily,
  type TransformationDef,
} from '@/catalog'
import { EmptyState, IconButton, Input, Kbd } from '@/components/ui'
import { cn } from '@/lib/utils/cn'
import { useEditorStore } from '@/store/editor'
import type { StudioNode } from '@/types/studio'

import { PaletteItem, type PaletteDragPayload } from './PaletteItem'

/* ----------------------------------------------------------------- icons */

/** Catalog entries name their lucide icon; this resolves the name to a component. */
const ICONS: Record<string, LucideIcon> = {
  ArrowUpDown,
  Braces,
  Bug,
  CircleSlash,
  Columns3,
  Combine,
  CopyMinus,
  Database,
  Droplet,
  FileCode2,
  FileSpreadsheet,
  FileSymlink,
  FileText,
  Filter,
  Fingerprint,
  Flag,
  GitMerge,
  Hash,
  Layers,
  Mountain,
  OctagonX,
  PenLine,
  Radio,
  Regex,
  Ruler,
  Sigma,
  Table2,
  Terminal,
  Trash2,
  Type,
  Variable,
}

function iconFor(name: string): ReactNode {
  const Icon = ICONS[name] ?? Box
  return <Icon className="h-3.5 w-3.5" aria-hidden />
}

/* -------------------------------------------------------------- placement */

const FIRST_POSITION: XYPosition = { x: 120, y: 160 }
const NODE_GAP = 72
const FALLBACK_NODE_WIDTH = 240

/**
 * A free spot for a clicked node: just right of the rightmost node, aligned with it
 * so a chain keeps pipelineing left to right.
 */
function nextPosition(nodes: StudioNode[]): XYPosition {
  if (nodes.length === 0) return FIRST_POSITION
  let right = -Infinity
  let y = FIRST_POSITION.y
  for (const node of nodes) {
    const width = node.measured?.width ?? node.width ?? FALLBACK_NODE_WIDTH
    const edge = node.position.x + width
    if (edge > right) {
      right = edge
      y = node.position.y
    }
  }
  return { x: right + NODE_GAP, y }
}

/* --------------------------------------------------------------- sections */

interface PaletteEntry {
  key: string
  icon: ReactNode
  label: string
  summary: string
  accent: NodeAccent
  payload: PaletteDragPayload
}

interface PaletteSection {
  id: string
  title: string
  entries: PaletteEntry[]
  /** Extra context under the entries, e.g. which rules a search matched. */
  hint?: string
}

const TRANSFORM_FAMILIES: NodeFamily[] = ['shape', 'compute']
const QUALITY_TERMS = ['validation', 'validations', 'quality', 'rule', 'check', 'assert', 'dq']
const NOTE_TERMS = ['note', 'sticky', 'annotation', 'comment', 'label', 'text', 'docs']

const matchesTerms = (query: string, terms: string[]): boolean =>
  query === '' || terms.some((term) => term.includes(query))

function transformEntry(def: TransformationDef): PaletteEntry {
  return {
    key: `transform:${def.type}`,
    icon: iconFor(def.icon),
    label: def.label,
    summary: def.summary,
    accent: def.accent,
    payload: { kind: 'transform', type: def.type },
  }
}

function formatEntry(def: FormatDef, kind: 'source' | 'sink'): PaletteEntry {
  return {
    key: `${kind}:${def.id}`,
    icon: iconFor(def.icon),
    label: def.label,
    summary: def.summary,
    accent: kind === 'source' ? 'input' : 'output',
    payload:
      kind === 'source' ? { kind: 'source', format: def.id } : { kind: 'sink', format: def.id },
  }
}

/* ----------------------------------------------------------------- panel */

/** Left rail: every node the editor can create, searchable, draggable and clickable. */
export function NodePalette() {
  const [query, setQuery] = useState('')
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({})
  const inputRef = useRef<HTMLInputElement>(null)
  const rootRef = useRef<HTMLDivElement>(null)

  const addTransform = useEditorStore((s) => s.addTransform)
  const addSource = useEditorStore((s) => s.addSource)
  const addSink = useEditorStore((s) => s.addSink)
  const addValidations = useEditorStore((s) => s.addValidations)
  const addNote = useEditorStore((s) => s.addNote)

  const add = useCallback(
    (payload: PaletteDragPayload) => {
      // Read nodes on demand: subscribing would re-render the rail on every drag frame.
      const position = nextPosition(useEditorStore.getState().nodes)
      switch (payload.kind) {
        case 'transform':
          addTransform(payload.type, position)
          break
        case 'source':
          addSource(position, payload.format)
          break
        case 'sink':
          addSink(position, payload.format)
          break
        case 'validations':
          addValidations(position)
          break
        case 'note':
          addNote(position)
          break
      }
    },
    [addTransform, addSource, addSink, addValidations, addNote],
  )

  const sections = useMemo<PaletteSection[]>(() => {
    const term = query.trim().toLowerCase()
    const found = searchCatalog(query)
    const matchedFormats = new Set(found.formats.map((format) => format.id))

    const byFamily = (families: NodeFamily[]): PaletteEntry[] =>
      found.transformations
        .filter((def) => families.includes(def.family))
        .map((def) => transformEntry(def))

    const formats = (list: FormatDef[], kind: 'source' | 'sink'): PaletteEntry[] =>
      list.filter((def) => matchedFormats.has(def.id)).map((def) => formatEntry(def, kind))

    const qualityVisible =
      matchesTerms(term, QUALITY_TERMS) || (term !== '' && found.validators.length > 0)

    const all: PaletteSection[] = [
      { id: 'sources', title: 'Sources', entries: formats(READABLE_FORMATS, 'source') },
      { id: 'transform', title: 'Transform', entries: byFamily(TRANSFORM_FAMILIES) },
      { id: 'combine', title: 'Combine', entries: byFamily(['combine']) },
      { id: 'aggregate', title: 'Aggregate', entries: byFamily(['aggregate']) },
      { id: 'control', title: 'Control', entries: byFamily(['control']) },
      { id: 'inspect', title: 'Inspect', entries: byFamily(['inspect']) },
      {
        id: 'quality',
        title: 'Quality',
        entries: qualityVisible
          ? [
              {
                key: 'validations',
                icon: <ShieldCheck className="h-3.5 w-3.5" aria-hidden />,
                label: 'Validations',
                summary: 'Run data-quality rules on the result before anything is written.',
                accent: 'validate',
                payload: { kind: 'validations' },
              },
            ]
          : [],
        hint:
          term !== '' && found.validators.length > 0
            ? `Rules: ${found.validators.map((rule) => rule.label).join(', ')}`
            : undefined,
      },
      { id: 'destinations', title: 'Destinations', entries: formats(WRITABLE_FORMATS, 'sink') },
      {
        id: 'annotations',
        title: 'Annotations',
        entries: matchesTerms(term, NOTE_TERMS)
          ? [
              {
                key: 'note',
                icon: <StickyNote className="h-3.5 w-3.5" aria-hidden />,
                label: 'Note',
                summary: 'Sticky note for canvas context. Never compiled into the pipeline.',
                accent: 'inspect',
                payload: { kind: 'note' },
              },
            ]
          : [],
      },
    ]

    return all.filter((section) => section.entries.length > 0)
  }, [query])

  const searching = query.trim().length > 0

  const changeQuery = (value: string) => {
    // Starting a search re-opens every section so no match hides in a collapsed one.
    if (value.trim() && !searching) setCollapsed({})
    setQuery(value)
  }

  /* '/' focuses the search box from anywhere on the page. */
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== '/' || event.metaKey || event.ctrlKey || event.altKey) return
      const target = event.target as HTMLElement | null
      if (
        target &&
        (target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName))
      ) {
        return
      }
      event.preventDefault()
      inputRef.current?.focus()
      inputRef.current?.select()
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  /** Visible items, in DOM order — collapsed sections are unmounted, so this is exact. */
  const items = (): HTMLButtonElement[] =>
    Array.from(
      rootRef.current?.querySelectorAll<HTMLButtonElement>('[data-palette-item]') ?? [],
    )

  const move = (delta: 1 | -1) => {
    const list = items()
    if (list.length === 0) return
    const current = list.indexOf(document.activeElement as HTMLButtonElement)
    const next =
      current === -1
        ? delta === 1
          ? 0
          : list.length - 1
        : (current + delta + list.length) % list.length
    const target = list[next]
    target?.focus()
    target?.scrollIntoView({ block: 'nearest' })
  }

  const handleKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    if (event.key === 'ArrowDown') {
      event.preventDefault()
      move(1)
    } else if (event.key === 'ArrowUp') {
      event.preventDefault()
      move(-1)
    } else if (event.key === 'Escape' && searching) {
      event.preventDefault()
      setQuery('')
    } else if (event.key === 'Enter' && event.target === inputRef.current) {
      // Enter from the search box commits the first result.
      const first = items()[0]
      if (first) {
        event.preventDefault()
        first.click()
      }
    }
  }

  return (
    <div
      ref={rootRef}
      onKeyDown={handleKeyDown}
      className="flex h-full min-h-0 w-full flex-col"
    >
      <div className="shrink-0 p-2">
        <div className="relative">
          <Input
            ref={inputRef}
            value={query}
            onChange={(event) => changeQuery(event.target.value)}
            placeholder="Search nodes"
            aria-label="Search nodes"
            spellCheck={false}
            leading={<Search />}
            className="h-9 py-0 pr-9 text-xs"
          />
          {searching ? (
            <IconButton
              label="Clear search"
              size="sm"
              onClick={() => {
                setQuery('')
                inputRef.current?.focus()
              }}
              className="absolute right-1 top-1/2 -translate-y-1/2"
            >
              <X />
            </IconButton>
          ) : (
            <span className="pointer-events-none absolute right-2 top-1/2 -translate-y-1/2">
              <Kbd>/</Kbd>
            </span>
          )}
        </div>
      </div>

      <div className="scroll-area min-h-0 flex-1 px-2 pb-3">
        {sections.length === 0 ? (
          <EmptyState
            icon={<SearchX />}
            title="No matches"
            description="Try a format, a transformation name or what you want to do."
            className="py-10"
          />
        ) : (
          <div className="space-y-1">
            {sections.map((section) => {
              const open = !collapsed[section.id]
              return (
                <section key={section.id} className="animate-fade-in">
                  <button
                    type="button"
                    aria-expanded={open}
                    onClick={() => setCollapsed((state) => ({ ...state, [section.id]: open }))}
                    className={cn(
                      'flex w-full items-center gap-1.5 rounded-md px-1.5 py-1.5',
                      'text-content-subtle transition-colors hover:text-content',
                    )}
                  >
                    <ChevronRight
                      className={cn('h-3 w-3 transition-transform', open && 'rotate-90')}
                      aria-hidden
                    />
                    <span className="text-2xs font-semibold uppercase tracking-wider">
                      {section.title}
                    </span>
                    <span className="ml-auto text-2xs tabular-nums text-content-subtle">
                      {section.entries.length}
                    </span>
                  </button>

                  {open && (
                    <div className="space-y-0.5 pb-1 pl-1">
                      {section.entries.map((entry) => (
                        <PaletteItem
                          key={entry.key}
                          icon={entry.icon}
                          label={entry.label}
                          summary={entry.summary}
                          accent={entry.accent}
                          dragPayload={entry.payload}
                          onAdd={() => add(entry.payload)}
                        />
                      ))}
                      {section.hint && (
                        <p className="px-2 pt-0.5 text-2xs leading-relaxed text-content-subtle">
                          {section.hint}
                        </p>
                      )}
                    </div>
                  )}
                </section>
              )
            })}
          </div>
        )}
      </div>

      <div className="shrink-0 border-t border-line px-3 py-2">
        <p className="text-2xs leading-relaxed text-content-subtle">
          Drag onto the canvas, or click to add after the last node.
        </p>
      </div>
    </div>
  )
}
