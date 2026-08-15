/**
 * Inspector field widgets.
 *
 * Every widget is controlled through the same `value` / `onChange` pair. The list-like
 * ones keep their rows in local state, keyed by a generated id: rebuilding the rows
 * from the incoming value on each render would remount the inputs and drop the caret,
 * so props are only re-read when they differ from what the widget itself last emitted
 * (an undo, or an edit made from somewhere else).
 */

import { AlertCircle, ArrowRight, GripVertical, Plus, Trash2, X } from 'lucide-react'
import {
  useEffect,
  useId,
  useRef,
  useState,
  type DragEvent,
  type KeyboardEvent,
  type ReactNode,
} from 'react'

import { getFormat, READABLE_FORMATS, type FieldOption, type FieldSpec } from '@/catalog'
import { Button, Field, IconButton, Input, Select, Textarea } from '@/components/ui'
import { fieldLabelId } from '@/components/ui/Field'
import { cn } from '@/lib/utils/cn'

/* ------------------------------------------------------------------ shared */

export interface WidgetProps<T> {
  /** Id of the primary control, so a label or an issue link can target it. */
  id?: string
  value: T
  onChange: (value: T) => void
  placeholder?: string
  disabled?: boolean
  invalid?: boolean
}

let rowSequence = 0
const nextRowId = (): string => `row${(rowSequence += 1)}`

const REORDER_HINT = 'Drag to reorder, or focus this handle and use the arrow keys'

interface EditableRows<TRow> {
  rows: TRow[]
  commit: (rows: TRow[]) => void
}

/**
 * Rows live in state; the incoming value is only re-read when it stops matching what
 * this widget emitted, which is what keeps focus alive across keystrokes.
 */
function useEditableRows<TRow, TValue>(
  value: TValue,
  parse: (value: TValue) => TRow[],
  build: (rows: TRow[]) => TValue,
  onChange: (value: TValue) => void,
): EditableRows<TRow> {
  const latest = useRef({ value, parse, build, onChange })
  latest.current = { value, parse, build, onChange }

  const [rows, setRows] = useState<TRow[]>(() => parse(value))

  // The incoming value, in the exact shape this widget would have emitted for it.
  const signature = JSON.stringify(build(parse(value)) ?? null)
  const emitted = useRef(signature)

  useEffect(() => {
    if (signature === emitted.current) return
    emitted.current = signature
    setRows(latest.current.parse(latest.current.value))
  }, [signature])

  const commit = (next: TRow[]) => {
    setRows(next)
    const built = latest.current.build(next)
    emitted.current = JSON.stringify(built ?? null)
    latest.current.onChange(built)
  }

  return { rows, commit }
}

function moveItem<T>(items: T[], from: number, to: number): T[] {
  if (from === to || from < 0 || to < 0 || from >= items.length || to >= items.length) {
    return items
  }
  const next = items.slice()
  const [item] = next.splice(from, 1)
  next.splice(to, 0, item)
  return next
}

/** Handle-driven drag reordering, with the drop zone on the whole row. */
function useDragReorder(onMove: (from: number, to: number) => void) {
  const from = useRef<number | null>(null)

  return {
    handleProps: (index: number) => ({
      draggable: true,
      onDragStart: (event: DragEvent<HTMLElement>) => {
        from.current = index
        event.dataTransfer.effectAllowed = 'move'
        event.dataTransfer.setData('text/plain', String(index))
      },
      onDragEnd: () => {
        from.current = null
      },
    }),
    zoneProps: (index: number) => ({
      onDragOver: (event: DragEvent<HTMLElement>) => {
        if (from.current === null) return
        event.preventDefault()
        event.dataTransfer.dropEffect = 'move'
      },
      onDrop: (event: DragEvent<HTMLElement>) => {
        if (from.current === null) return
        event.preventDefault()
        onMove(from.current, index)
        from.current = null
      },
    }),
  }
}

const HANDLE_CLASS = cn(
  'shrink-0 cursor-grab rounded text-content-subtle transition-colors',
  'hover:text-content focus-visible:text-content active:cursor-grabbing',
)

function asStringRecord(value: unknown): Record<string, string> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {}
  const out: Record<string, string> = {}
  for (const [key, entry] of Object.entries(value)) {
    out[key] = typeof entry === 'string' ? entry : JSON.stringify(entry)
  }
  return out
}

/* ------------------------------------------------------------ string list */

interface TextRow {
  id: string
  text: string
}

/** Commas, semicolons, tabs and newlines all split a pasted list. */
const SPLITTERS = /[,;\t\n]/

const toTextRows = (value: string[]): TextRow[] =>
  (Array.isArray(value) ? value : []).map((text) => ({ id: nextRowId(), text: String(text) }))

const fromTextRows = (rows: TextRow[]): string[] =>
  rows.map((row) => row.text.trim()).filter((text) => text !== '')

const splitList = (text: string): string[] =>
  text
    .split(SPLITTERS)
    .map((part) => part.trim())
    .filter((part) => part !== '')

/** Ordered list of short values — column names, partition keys, merge keys. */
export function StringListField({
  id,
  value,
  onChange,
  placeholder,
  disabled,
  invalid,
}: WidgetProps<string[]>) {
  const { rows, commit } = useEditableRows(value, toTextRows, fromTextRows, onChange)
  const [draft, setDraft] = useState('')
  const drag = useDragReorder((from, to) => commit(moveItem(rows, from, to)))

  const append = (text: string) => {
    const parts = splitList(text)
    if (parts.length === 0) return
    commit([...rows, ...parts.map((part) => ({ id: nextRowId(), text: part }))])
  }

  const editRow = (index: number, text: string) => {
    if (!SPLITTERS.test(text)) {
      commit(rows.map((row, i) => (i === index ? { ...row, text } : row)))
      return
    }
    // A pasted list lands in one row: expand it in place, keeping the row's id first.
    const parts = splitList(text)
    const next = rows.slice()
    next.splice(
      index,
      1,
      ...parts.map((part, offset) => ({
        id: offset === 0 ? rows[index].id : nextRowId(),
        text: part,
      })),
    )
    commit(next)
  }

  const onHandleKey = (event: KeyboardEvent<HTMLButtonElement>, index: number) => {
    if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
      event.preventDefault()
      commit(moveItem(rows, index, index - 1))
    }
    if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
      event.preventDefault()
      commit(moveItem(rows, index, index + 1))
    }
  }

  return (
    <div
      className={cn(
        'flex flex-wrap items-center gap-1.5 rounded-lg border border-line bg-surface-sunken px-2 py-1.5',
        'focus-within:border-brand-500 focus-within:bg-surface focus-within:ring-2 focus-within:ring-brand-500/25',
        invalid && 'border-state-danger',
        disabled && 'pointer-events-none opacity-60',
      )}
    >
      {rows.map((row, index) => (
        <span
          key={row.id}
          className="inline-flex items-center gap-1 rounded-md border border-line bg-surface py-0.5 pl-1 pr-0.5"
          {...drag.zoneProps(index)}
        >
          <button
            type="button"
            aria-label={`Reorder ${row.text || `entry ${index + 1}`}`}
            title={REORDER_HINT}
            className={HANDLE_CLASS}
            onKeyDown={(event) => onHandleKey(event, index)}
            {...drag.handleProps(index)}
          >
            <GripVertical className="h-3 w-3" />
          </button>
          <input
            value={row.text}
            size={Math.max(row.text.length, 3)}
            aria-label={`Entry ${index + 1}`}
            onChange={(event) => editRow(index, event.target.value)}
            className="bg-transparent font-mono text-2xs text-content outline-none"
          />
          <button
            type="button"
            aria-label={`Remove ${row.text || `entry ${index + 1}`}`}
            onClick={() => commit(rows.filter((_, i) => i !== index))}
            className="rounded p-0.5 text-content-subtle transition-colors hover:bg-surface-sunken hover:text-state-danger"
          >
            <X className="h-3 w-3" />
          </button>
        </span>
      ))}

      <input
        id={id}
        value={draft}
        aria-label="Add entry"
        placeholder={rows.length > 0 ? 'Add…' : (placeholder ?? 'Type and press Enter')}
        onChange={(event) => {
          const text = event.target.value
          if (!SPLITTERS.test(text)) {
            setDraft(text)
            return
          }
          append(text)
          setDraft('')
        }}
        onKeyDown={(event) => {
          if (event.key === 'Enter') {
            event.preventDefault()
            append(draft)
            setDraft('')
          }
          if (event.key === 'Backspace' && draft === '' && rows.length > 0) {
            commit(rows.slice(0, -1))
          }
        }}
        onBlur={() => {
          if (draft.trim() === '') return
          append(draft)
          setDraft('')
        }}
        className="min-w-[7rem] flex-1 bg-transparent text-xs text-content outline-none placeholder:text-content-subtle"
      />
    </div>
  )
}

/* --------------------------------------------------------------- sql list */

export interface SqlListFieldProps extends WidgetProps<string[]> {
  /** Height of each expression box. */
  rows?: number
  addLabel?: string
}

/** One SQL expression per row — select projections, aggregate expressions. */
export function SqlListField({
  id,
  value,
  onChange,
  placeholder,
  disabled,
  invalid,
  rows: boxRows = 2,
  addLabel = 'Add expression',
}: SqlListFieldProps) {
  const { rows, commit } = useEditableRows(value, toTextRows, fromTextRows, onChange)
  const drag = useDragReorder((from, to) => commit(moveItem(rows, from, to)))

  const onHandleKey = (event: KeyboardEvent<HTMLButtonElement>, index: number) => {
    if (event.key === 'ArrowUp') {
      event.preventDefault()
      commit(moveItem(rows, index, index - 1))
    }
    if (event.key === 'ArrowDown') {
      event.preventDefault()
      commit(moveItem(rows, index, index + 1))
    }
  }

  return (
    <div className={cn('space-y-1.5', disabled && 'pointer-events-none opacity-60')}>
      {rows.map((row, index) => (
        <div key={row.id} className="flex items-start gap-1" {...drag.zoneProps(index)}>
          <button
            type="button"
            aria-label={`Reorder expression ${index + 1}`}
            title={REORDER_HINT}
            className={cn(HANDLE_CLASS, 'mt-2')}
            onKeyDown={(event) => onHandleKey(event, index)}
            {...drag.handleProps(index)}
          >
            <GripVertical className="h-3.5 w-3.5" />
          </button>
          <Textarea
            id={index === 0 ? id : undefined}
            mono
            rows={boxRows}
            value={row.text}
            placeholder={placeholder}
            invalid={invalid}
            aria-label={`Expression ${index + 1}`}
            onChange={(event) =>
              commit(
                rows.map((item, i) =>
                  i === index ? { ...item, text: event.target.value } : item,
                ),
              )
            }
            className="min-w-0 flex-1 px-2 py-1.5"
          />
          <IconButton
            size="xs"
            label={`Remove expression ${index + 1}`}
            className="mt-1"
            onClick={() => commit(rows.filter((_, i) => i !== index))}
          >
            <Trash2 />
          </IconButton>
        </div>
      ))}

      <Button
        // With no rows yet there is no input to label, so the button carries the id.
        id={rows.length === 0 ? id : undefined}
        size="xs"
        variant="ghost"
        icon={<Plus />}
        onClick={() => commit([...rows, { id: nextRowId(), text: '' }])}
      >
        {addLabel}
      </Button>
    </div>
  )
}

/* -------------------------------------------------------------- pair maps */

interface PairRow {
  id: string
  key: string
  value: string
}

const toPairRows = (value: Record<string, string>): PairRow[] =>
  Object.entries(asStringRecord(value)).map(([key, entry]) => ({
    id: nextRowId(),
    key,
    value: entry,
  }))

const fromPairRows = (rows: PairRow[]): Record<string, string> => {
  const out: Record<string, string> = {}
  for (const row of rows) {
    const key = row.key.trim()
    if (key !== '') out[key] = row.value
  }
  return out
}

interface PairEditorProps extends WidgetProps<Record<string, string>> {
  keyPlaceholder: string
  valuePlaceholder: string
  addLabel: string
  monoValue?: boolean
  note?: ReactNode
}

/**
 * Ordered key → value rows. Order is load-bearing for `rename` (sequential renames)
 * and for `with_column` maps (later expressions read earlier ones), so rows keep the
 * position the user gave them instead of being re-sorted from the object.
 */
function PairEditor({
  id,
  value,
  onChange,
  disabled,
  invalid,
  keyPlaceholder,
  valuePlaceholder,
  addLabel,
  monoValue,
  note,
}: PairEditorProps) {
  const { rows, commit } = useEditableRows(value, toPairRows, fromPairRows, onChange)
  const drag = useDragReorder((from, to) => commit(moveItem(rows, from, to)))

  const patch = (index: number, patchRow: Partial<PairRow>) =>
    commit(rows.map((row, i) => (i === index ? { ...row, ...patchRow } : row)))

  const onHandleKey = (event: KeyboardEvent<HTMLButtonElement>, index: number) => {
    if (event.key === 'ArrowUp') {
      event.preventDefault()
      commit(moveItem(rows, index, index - 1))
    }
    if (event.key === 'ArrowDown') {
      event.preventDefault()
      commit(moveItem(rows, index, index + 1))
    }
  }

  return (
    <div className={cn('space-y-1.5', disabled && 'pointer-events-none opacity-60')}>
      {rows.map((row, index) => (
        <div key={row.id} className="flex items-center gap-1" {...drag.zoneProps(index)}>
          <button
            type="button"
            aria-label={`Reorder row ${index + 1}`}
            title={REORDER_HINT}
            className={HANDLE_CLASS}
            onKeyDown={(event) => onHandleKey(event, index)}
            {...drag.handleProps(index)}
          >
            <GripVertical className="h-3.5 w-3.5" />
          </button>
          <Input
            id={index === 0 ? id : undefined}
            mono
            value={row.key}
            placeholder={keyPlaceholder}
            invalid={invalid}
            aria-label={`Key ${index + 1}`}
            onChange={(event) => patch(index, { key: event.target.value })}
            className="h-8 min-w-0 flex-1 px-2 py-1"
          />
          <ArrowRight className="h-3 w-3 shrink-0 text-content-subtle" aria-hidden />
          <Input
            mono={monoValue}
            value={row.value}
            placeholder={valuePlaceholder}
            aria-label={`Value ${index + 1}`}
            onChange={(event) => patch(index, { value: event.target.value })}
            className="h-8 min-w-0 flex-[1.3] px-2 py-1 text-xs"
          />
          <IconButton
            size="xs"
            label={`Remove row ${index + 1}`}
            onClick={() => commit(rows.filter((_, i) => i !== index))}
          >
            <Trash2 />
          </IconButton>
        </div>
      ))}

      <div className="flex items-center justify-between gap-2">
        <Button
          // With no rows yet there is no input to label, so the button carries the id.
          id={rows.length === 0 ? id : undefined}
          size="xs"
          variant="ghost"
          icon={<Plus />}
          onClick={() => commit([...rows, { id: nextRowId(), key: '', value: '' }])}
        >
          {addLabel}
        </Button>
        {note && <p className="text-2xs leading-relaxed text-content-subtle">{note}</p>}
      </div>
    </div>
  )
}

export interface PairFieldProps extends WidgetProps<Record<string, string>> {
  keyPlaceholder?: string
  valuePlaceholder?: string
}

/** Ordered map of plain strings — renames, casts, options. */
export function KeyValueField({
  keyPlaceholder = 'key',
  valuePlaceholder = 'value',
  ...props
}: PairFieldProps) {
  return (
    <PairEditor
      {...props}
      keyPlaceholder={keyPlaceholder}
      valuePlaceholder={valuePlaceholder}
      addLabel="Add pair"
      note="Applied top to bottom — the order is part of the behavior."
    />
  )
}

/** Ordered map of column → SQL expression. */
export function ExpressionMapField({
  keyPlaceholder = 'new_column',
  valuePlaceholder = 'valor * 1.1',
  ...props
}: PairFieldProps) {
  return (
    <PairEditor
      {...props}
      monoValue
      keyPlaceholder={keyPlaceholder}
      valuePlaceholder={valuePlaceholder}
      addLabel="Add column"
      note="Runs top to bottom — a later expression can use a column defined above it."
    />
  )
}

/* ------------------------------------------------------------- multi-select */

export interface MultiSelectFieldProps extends WidgetProps<string[]> {
  options: FieldOption[]
}

/** Checkbox list. The emitted order always follows the catalog order. */
export function MultiSelectField({
  id,
  value,
  onChange,
  options,
  disabled,
}: MultiSelectFieldProps) {
  const selected = new Set(Array.isArray(value) ? value : [])

  const toggle = (option: string) => {
    const next = new Set(selected)
    if (next.has(option)) next.delete(option)
    else next.add(option)
    onChange(options.filter((item) => next.has(item.value)).map((item) => item.value))
  }

  return (
    // `<label for>` only binds to labelable elements, so the caption names this
    // wrapper as a group instead of pointing at a div that can never own it.
    <div
      role="group"
      aria-labelledby={id ? fieldLabelId(id) : undefined}
      className={cn(
        'space-y-0.5 rounded-lg border border-line bg-surface-sunken p-1.5',
        disabled && 'pointer-events-none opacity-60',
      )}
    >
      {options.map((option) => {
        const checked = selected.has(option.value)
        return (
          <label
            key={option.value}
            className={cn(
              'flex cursor-pointer items-start gap-2 rounded-md px-1.5 py-1 transition-colors',
              checked ? 'bg-surface' : 'hover:bg-surface/60',
            )}
          >
            <input
              type="checkbox"
              checked={checked}
              onChange={() => toggle(option.value)}
              className="mt-0.5 h-3.5 w-3.5 shrink-0 rounded border-line accent-brand-500"
            />
            <span className="min-w-0">
              <span className="block font-mono text-2xs text-content">{option.label}</span>
              {option.hint && (
                <span className="block text-2xs leading-relaxed text-content-subtle">
                  {option.hint}
                </span>
              )}
            </span>
          </label>
        )
      })}
    </div>
  )
}

/* ------------------------------------------------------------------- json */

export interface JsonFieldProps extends WidgetProps<unknown> {
  rows?: number
}

function stringifyJson(value: unknown): string {
  if (value === undefined) return ''
  return JSON.stringify(value, null, 2)
}

/**
 * Small JSON box for object/scalar values (struct fields, pivots, fill values).
 * Deliberately a textarea: the inspector must stay light, Monaco lives in the JSON panel.
 */
export function JsonField({
  id,
  value,
  onChange,
  placeholder,
  disabled,
  invalid,
  rows = 6,
}: JsonFieldProps) {
  const errorId = useId()
  const incoming = stringifyJson(value)
  const [text, setText] = useState(incoming)
  const [error, setError] = useState<string | null>(null)
  const emitted = useRef(incoming)

  useEffect(() => {
    if (incoming === emitted.current) return
    emitted.current = incoming
    setText(incoming)
    setError(null)
  }, [incoming])

  const edit = (next: string) => {
    setText(next)
    if (next.trim() === '') {
      setError(null)
      emitted.current = ''
      onChange(undefined)
      return
    }
    try {
      const parsed: unknown = JSON.parse(next)
      setError(null)
      emitted.current = stringifyJson(parsed)
      onChange(parsed)
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'This is not valid JSON.')
    }
  }

  return (
    <div className={cn('space-y-1.5', disabled && 'pointer-events-none opacity-60')}>
      <Textarea
        id={id}
        mono
        rows={rows}
        value={text}
        spellCheck={false}
        placeholder={placeholder}
        invalid={invalid || Boolean(error)}
        aria-describedby={error ? errorId : undefined}
        onChange={(event) => edit(event.target.value)}
        className="px-2.5 py-2"
      />
      {error ? (
        <p
          id={errorId}
          role="alert"
          className="flex items-start gap-1.5 text-2xs text-state-danger"
        >
          <AlertCircle className="mt-px h-3 w-3 shrink-0" aria-hidden />
          <span>{error}</span>
        </p>
      ) : (
        text.trim() !== '' && (
          <div className="flex justify-end">
            <Button size="xs" variant="ghost" onClick={() => setText(stringifyJson(value))}>
              Format
            </Button>
          </div>
        )
      )}
    </div>
  )
}

/* ----------------------------------------------------------------- source */

export interface SourceValue {
  format?: string
  path?: string
  options?: Record<string, unknown>
}

export interface SourceFieldProps extends WidgetProps<SourceValue> {
  /** Renders one option of the chosen format; without it options fall back to JSON. */
  renderOption?: (
    spec: FieldSpec,
    optionValue: unknown,
    onOptionChange: (next: unknown) => void,
  ) => ReactNode
}

/** Nested `{ format, path, options }` sub-form. */
export function SourceField({ id, value, onChange, disabled, renderOption }: SourceFieldProps) {
  const source = value ?? {}
  const format = getFormat(source.format ?? '')
  const options = source.options ?? {}

  const patch = (next: Partial<SourceValue>) => onChange({ ...source, ...next })

  const setOption = (key: string, next: unknown) => {
    const nextOptions = { ...options }
    if (next === undefined || next === null || next === '') delete nextOptions[key]
    else nextOptions[key] = next
    patch({ options: nextOptions })
  }

  return (
    <div
      className={cn(
        'space-y-3 rounded-lg border border-line bg-surface p-2.5',
        disabled && 'pointer-events-none opacity-60',
      )}
    >
      <Field label="Format" htmlFor={id}>
        <Select
          id={id}
          ariaLabel="Source format"
          value={source.format ?? ''}
          onValueChange={(next) => patch({ format: next })}
          options={READABLE_FORMATS.map((item) => ({
            value: item.id,
            label: item.label,
            hint: item.summary,
          }))}
        />
      </Field>

      <Field
        label={format?.pathLabel ?? 'Path'}
        help={format?.pathHelp}
        htmlFor={id ? `${id}-path` : undefined}
      >
        <Input
          id={id ? `${id}-path` : undefined}
          mono
          value={source.path ?? ''}
          placeholder={format?.pathPlaceholder}
          onChange={(event) => patch({ path: event.target.value })}
        />
      </Field>

      {format && format.readOptions.length > 0 && (
        <div className="space-y-3 border-t border-line pt-3">
          {renderOption ? (
            format.readOptions.map((spec) =>
              renderOption(spec, options[spec.key], (next) => setOption(spec.key, next)),
            )
          ) : (
            <Field label="Options" help="Reader options, as a JSON object.">
              <JsonField
                rows={4}
                value={options}
                onChange={(next) =>
                  patch({
                    options:
                      next && typeof next === 'object' && !Array.isArray(next)
                        ? (next as Record<string, unknown>)
                        : {},
                  })
                }
              />
            </Field>
          )}
        </div>
      )}
    </div>
  )
}
