/**
 * Maps a catalog `FieldSpec` to its widget.
 *
 * The catalog decides what a field is; this module decides how it looks and how its
 * value travels back. Nothing here knows about nodes — the caller owns the value and
 * the write, which is what lets the same renderer drive transformations, IO options,
 * validation rules and nested sources.
 */

import { Braces, Variable } from 'lucide-react'
import { useMemo, type ReactNode } from 'react'

import type { FieldSpec } from '@/catalog'
import {
  Field,
  Input,
  renderInlineCode,
  Select,
  Textarea,
  Toggle,
  Tooltip,
} from '@/components/ui'

import {
  ExpressionMapField,
  JsonField,
  KeyValueField,
  MultiSelectField,
  SourceField,
  SqlListField,
  StringListField,
  type SourceValue,
} from './widgets'

/** Sentinel for "no value" — Radix Select cannot hold an empty string. */
const UNSET = '__unset__'

export interface FieldRendererProps {
  field: FieldSpec
  value: unknown
  /** Sibling values, for `visibleWhen` / `validate` and nothing else. */
  params: Record<string, unknown>
  onChange: (value: unknown) => void
  /** Scopes the generated DOM ids so several forms can coexist in one panel. */
  nodeId: string
}

/** Id of the wrapper an issue link scrolls to. */
export function fieldAnchorId(nodeId: string, key: string): string {
  return `field-${nodeId}-${key.replace(/[^\w-]/g, '_')}`
}

/** Scrolls the inspector to a field and focuses its first control. */
export function focusField(nodeId: string, key: string): void {
  const anchor = document.getElementById(fieldAnchorId(nodeId, key))
  if (!anchor) return
  anchor.scrollIntoView({ block: 'center', behavior: 'smooth' })
  const control = anchor.querySelector<HTMLElement>('input, textarea, select, button')
  control?.focus({ preventScroll: true })
}

/* ---------------------------------------------------------- placeholders */

const RUNTIME_PATTERN = /\{\{\s*([\w.]+)\s*\}\}/g
const PARAM_PATTERN = /\{\s*([\w.]+)\s*\}/g

function collectStrings(value: unknown, out: string[]): void {
  if (typeof value === 'string') {
    out.push(value)
    return
  }
  if (Array.isArray(value)) {
    for (const item of value) collectStrings(item, out)
    return
  }
  if (value && typeof value === 'object') {
    for (const [key, item] of Object.entries(value)) {
      out.push(key)
      collectStrings(item, out)
    }
  }
}

function detectPlaceholders(value: unknown): { runtime: string[]; params: string[] } {
  const strings: string[] = []
  collectStrings(value, strings)

  const runtime = new Set<string>()
  const params = new Set<string>()
  for (const text of strings) {
    for (const match of text.matchAll(RUNTIME_PATTERN)) runtime.add(match[1])
    // `{{a}}` also matches `{a}`, so runtime hits are removed before scanning params.
    for (const match of text.replace(RUNTIME_PATTERN, ' ').matchAll(PARAM_PATTERN)) {
      params.add(match[1])
    }
  }
  return { runtime: [...runtime], params: [...params] }
}

const CHIP_CLASS =
  'inline-flex items-center gap-1 rounded-md border border-brand-500/30 bg-brand-500/10 px-1.5 py-0.5 font-mono text-2xs text-brand-600 dark:text-brand-400'

/** Surfaces the `{{runtime}}` and `{param}` placeholders hiding inside a value. */
export function PlaceholderHints({ value }: { value: unknown }) {
  const found = useMemo(() => detectPlaceholders(value), [value])
  if (found.runtime.length === 0 && found.params.length === 0) return null

  return (
    <div className="flex flex-wrap items-center gap-1 pt-0.5">
      {found.runtime.map((name) => (
        <Tooltip
          key={`runtime-${name}`}
          content="Runtime variable — resolved while the pipeline runs, from the collect node that publishes it."
        >
          <span className={CHIP_CLASS}>
            <Variable className="h-3 w-3" aria-hidden />
            {`{{${name}}}`}
          </span>
        </Tooltip>
      ))}
      {found.params.map((name) => (
        <Tooltip
          key={`param-${name}`}
          content="Template parameter — replaced from the workflow params before the JSON is parsed."
        >
          <span className={CHIP_CLASS}>
            <Braces className="h-3 w-3" aria-hidden />
            {`{${name}}`}
          </span>
        </Tooltip>
      ))}
    </div>
  )
}

/* ------------------------------------------------------------- coercion */

const asText = (value: unknown): string => {
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  return ''
}

const asStringList = (value: unknown): string[] =>
  Array.isArray(value)
    ? value.map((item) => (typeof item === 'string' ? item : String(item)))
    : []

const asRecord = (value: unknown): Record<string, unknown> =>
  value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {}

const asStringMap = (value: unknown): Record<string, string> => {
  const out: Record<string, string> = {}
  for (const [key, entry] of Object.entries(asRecord(value))) {
    out[key] = typeof entry === 'string' ? entry : JSON.stringify(entry)
  }
  return out
}

/* -------------------------------------------------------------- renderer */

export function FieldRenderer({ field, value, params, onChange, nodeId }: FieldRendererProps) {
  const visible = field.visibleWhen ? field.visibleWhen(params) : true
  const error = useMemo(
    () => (visible && field.validate ? field.validate(value, params) : null),
    [visible, field, value, params],
  )

  if (!visible) return null

  const anchorId = fieldAnchorId(nodeId, field.key)
  const controlId = `${anchorId}-control`
  const invalid = Boolean(error)
  const help = field.help ? renderInlineCode(field.help) : undefined

  // Booleans read better as a switch on the label row than as a control below it.
  if (field.type === 'boolean') {
    const checked = typeof value === 'boolean' ? value : field.default === true
    return (
      <div id={anchorId} className="scroll-mt-4">
        <Field
          label={field.label}
          help={help}
          docs={field.docs}
          error={error}
          required={field.required}
          action={<Toggle checked={checked} onCheckedChange={(next) => onChange(next)} />}
        >
          {null}
        </Field>
      </div>
    )
  }

  const control = renderControl({ field, value, params, onChange, controlId, invalid, nodeId })

  return (
    <div id={anchorId} className="scroll-mt-4">
      <Field
        label={field.label}
        help={help}
        docs={field.docs}
        error={error}
        required={field.required}
        htmlFor={controlId}
      >
        {control}
        <PlaceholderHints value={value} />
      </Field>
    </div>
  )
}

interface ControlProps extends FieldRendererProps {
  controlId: string
  invalid: boolean
}

function renderControl({
  field,
  value,
  onChange,
  controlId,
  invalid,
  nodeId,
}: ControlProps): ReactNode {
  switch (field.type) {
    case 'textarea':
    case 'sql':
      return (
        <Textarea
          id={controlId}
          mono={field.type === 'sql'}
          rows={field.rows ?? 3}
          value={asText(value)}
          placeholder={field.placeholder}
          invalid={invalid}
          spellCheck={field.type === 'sql' ? false : undefined}
          onChange={(event) => onChange(event.target.value)}
          className={field.type === 'sql' ? 'px-2.5 py-2' : undefined}
        />
      )

    case 'number':
      return (
        <Input
          id={controlId}
          type="number"
          value={asText(value)}
          placeholder={field.placeholder}
          invalid={invalid}
          onChange={(event) => {
            const raw = event.target.value
            if (raw === '') {
              onChange(undefined)
              return
            }
            const parsed = Number(raw)
            onChange(Number.isFinite(parsed) ? parsed : raw)
          }}
        />
      )

    case 'select': {
      const options = field.options ?? []
      const current = asText(value)
      return (
        <Select
          id={controlId}
          ariaLabel={field.label}
          invalid={invalid}
          value={current === '' ? (field.required ? '' : UNSET) : current}
          onValueChange={(next) => onChange(next === UNSET ? undefined : next)}
          options={
            field.required
              ? options
              : [
                  { value: UNSET, label: 'Not set', hint: 'Leaves the key out of the JSON' },
                  ...options,
                ]
          }
        />
      )
    }

    case 'multi-select':
      return (
        <MultiSelectField
          id={controlId}
          options={field.options ?? []}
          value={
            value === undefined && Array.isArray(field.default)
              ? asStringList(field.default)
              : asStringList(value)
          }
          onChange={(next) => onChange(next)}
        />
      )

    case 'string-list':
      return (
        <StringListField
          id={controlId}
          value={asStringList(value)}
          placeholder={field.placeholder}
          invalid={invalid}
          onChange={(next) => onChange(next.length > 0 ? next : undefined)}
        />
      )

    case 'sql-list':
      return (
        <SqlListField
          id={controlId}
          rows={field.rows ?? 2}
          value={asStringList(value)}
          placeholder={field.placeholder}
          invalid={invalid}
          onChange={(next) => onChange(next.length > 0 ? next : undefined)}
        />
      )

    case 'key-value':
      return (
        <KeyValueField
          id={controlId}
          value={asStringMap(value)}
          keyPlaceholder={field.placeholder}
          invalid={invalid}
          onChange={(next) => onChange(Object.keys(next).length > 0 ? next : undefined)}
        />
      )

    case 'expression-map':
      return (
        <ExpressionMapField
          id={controlId}
          value={asStringMap(value)}
          invalid={invalid}
          onChange={(next) => onChange(Object.keys(next).length > 0 ? next : undefined)}
        />
      )

    case 'json':
      return (
        <JsonField
          id={controlId}
          rows={field.rows ?? 6}
          value={value}
          placeholder={field.placeholder}
          invalid={invalid}
          onChange={(next) => onChange(next)}
        />
      )

    case 'source': {
      const record = asRecord(value)
      const source: SourceValue = {
        format: typeof record.format === 'string' ? record.format : undefined,
        path: typeof record.path === 'string' ? record.path : undefined,
        options: asRecord(record.options),
      }
      return (
        <SourceField
          id={controlId}
          value={source}
          onChange={(next) => onChange(next)}
          renderOption={(spec, optionValue, onOptionChange) => (
            <FieldRenderer
              key={spec.key}
              field={spec}
              value={optionValue}
              params={asRecord(source.options)}
              onChange={onOptionChange}
              nodeId={`${nodeId}-${field.key}`}
            />
          )}
        />
      )
    }

    default:
      return (
        <Input
          id={controlId}
          mono={field.key === 'path' || field.supportsRuntimeVars}
          value={asText(value)}
          placeholder={field.placeholder}
          invalid={invalid}
          onChange={(event) => onChange(event.target.value)}
        />
      )
  }
}
