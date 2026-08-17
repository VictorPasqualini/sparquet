import * as RadixSelect from '@radix-ui/react-select'
import * as RadixSwitch from '@radix-ui/react-switch'
import { AlertCircle, Check, ChevronDown, Info } from 'lucide-react'
import {
  createContext,
  forwardRef,
  useContext,
  useId,
  type InputHTMLAttributes,
  type ReactNode,
  type TextareaHTMLAttributes,
} from 'react'

import { cn } from '@/lib/utils/cn'

import { InfoPopover } from './Popover'

/* ------------------------------------------------------------------ Field */

/**
 * Id of the paragraph that explains the surrounding `Field` — its error if it has
 * one, otherwise its help. Controls read it so `aria-describedby` is wired without
 * every call site repeating the plumbing.
 */
const FieldDescriptionContext = createContext<string | undefined>(undefined)

export function useFieldDescription(): string | undefined {
  return useContext(FieldDescriptionContext)
}

/** Id of the `<label>` a `Field` renders, for controls that are groups, not inputs. */
export function fieldLabelId(htmlFor: string): string {
  return `${htmlFor}-label`
}

export interface FieldProps {
  label?: string
  /** Rendered under the control. */
  help?: ReactNode
  /** Long-form markdown-ish content behind an info button next to the label. */
  docs?: string
  error?: string | null
  required?: boolean
  htmlFor?: string
  children: ReactNode
  className?: string
  /** Right-aligned adornment on the label row (e.g. a mode switch). */
  action?: ReactNode
}

export function Field({
  label,
  help,
  docs,
  error,
  required,
  htmlFor,
  children,
  className,
  action,
}: FieldProps) {
  const generatedId = useId()
  const base = htmlFor ?? generatedId
  const errorId = `${base}-error`
  const helpId = `${base}-help`
  const describedBy = error ? errorId : help ? helpId : undefined

  return (
    <div className={cn('space-y-1.5', className)}>
      {(label || action) && (
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1">
            {label && (
              <label
                id={fieldLabelId(base)}
                htmlFor={htmlFor}
                className="text-xs font-medium text-content-muted"
              >
                {label}
                {required && <span className="ml-0.5 text-state-danger">*</span>}
              </label>
            )}
            {docs && (
              <InfoPopover title={label ?? 'Details'} content={docs}>
                <button
                  type="button"
                  aria-label={`About ${label ?? 'this field'}`}
                  className="text-content-subtle transition-colors hover:text-brand-600 dark:hover:text-brand-400"
                >
                  <Info className="h-3.5 w-3.5" />
                </button>
              </InfoPopover>
            )}
          </div>
          {action}
        </div>
      )}
      <FieldDescriptionContext.Provider value={describedBy}>
        {children}
      </FieldDescriptionContext.Provider>
      {error ? (
        <p
          id={errorId}
          role="alert"
          className="flex items-start gap-1.5 text-2xs text-state-danger"
        >
          <AlertCircle className="mt-px h-3 w-3 shrink-0" aria-hidden />
          <span>{error}</span>
        </p>
      ) : help ? (
        <p id={helpId} className="text-2xs leading-relaxed text-content-subtle">
          {help}
        </p>
      ) : null}
    </div>
  )
}

/* ------------------------------------------------------------------ Input */

export interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
  invalid?: boolean
  mono?: boolean
  leading?: ReactNode
}

export const Input = forwardRef<HTMLInputElement, InputProps>(function Input(
  { className, invalid, mono, leading, 'aria-describedby': describedBy, ...props },
  ref,
) {
  const fieldDescription = useFieldDescription()
  const control = (
    <input
      ref={ref}
      aria-invalid={invalid || undefined}
      aria-describedby={describedBy ?? fieldDescription}
      className={cn(
        'input-base',
        mono && 'font-mono text-xs',
        invalid && 'border-state-danger focus:border-state-danger focus:ring-state-danger/25',
        leading && 'pl-8',
        className,
      )}
      {...props}
    />
  )

  if (!leading) return control

  return (
    <div className="relative">
      <span className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-content-subtle [&_svg]:h-4 [&_svg]:w-4">
        {leading}
      </span>
      {control}
    </div>
  )
})

export interface TextareaProps extends TextareaHTMLAttributes<HTMLTextAreaElement> {
  invalid?: boolean
  mono?: boolean
}

export const Textarea = forwardRef<HTMLTextAreaElement, TextareaProps>(function Textarea(
  { className, invalid, mono, rows = 3, 'aria-describedby': describedBy, ...props },
  ref,
) {
  const fieldDescription = useFieldDescription()
  return (
    <textarea
      ref={ref}
      rows={rows}
      aria-invalid={invalid || undefined}
      aria-describedby={describedBy ?? fieldDescription}
      className={cn(
        'input-base resize-y leading-relaxed',
        mono && 'font-mono text-xs',
        invalid && 'border-state-danger focus:border-state-danger focus:ring-state-danger/25',
        className,
      )}
      {...props}
    />
  )
})

/* ----------------------------------------------------------------- Switch */

export interface ToggleProps {
  checked: boolean
  onCheckedChange: (checked: boolean) => void
  label?: string
  description?: string
  disabled?: boolean
  className?: string
}

export function Toggle({
  checked,
  onCheckedChange,
  label,
  description,
  disabled,
  className,
}: ToggleProps) {
  const id = useId()
  return (
    <div className={cn('flex items-start justify-between gap-3', className)}>
      {(label || description) && (
        <div className="space-y-0.5">
          {label && (
            <label htmlFor={id} className="text-sm text-content">
              {label}
            </label>
          )}
          {description && (
            <p className="text-2xs leading-relaxed text-content-subtle">{description}</p>
          )}
        </div>
      )}
      <RadixSwitch.Root
        id={id}
        checked={checked}
        onCheckedChange={onCheckedChange}
        disabled={disabled}
        className={cn(
          'relative h-5 w-9 shrink-0 rounded-full border border-line bg-surface-sunken transition-colors',
          'data-[state=checked]:border-brand-500 data-[state=checked]:bg-brand-500',
          'disabled:cursor-not-allowed disabled:opacity-50',
        )}
      >
        <RadixSwitch.Thumb
          className={cn(
            'block h-3.5 w-3.5 translate-x-0.5 rounded-full bg-content-muted shadow transition-transform',
            'data-[state=checked]:translate-x-[18px] data-[state=checked]:bg-black',
          )}
        />
      </RadixSwitch.Root>
    </div>
  )
}

/* ----------------------------------------------------------------- Select */

export interface SelectOption {
  value: string
  label: string
  hint?: string
  disabled?: boolean
}

export interface SelectProps {
  value: string
  onValueChange: (value: string) => void
  options: SelectOption[]
  placeholder?: string
  disabled?: boolean
  invalid?: boolean
  className?: string
  id?: string
  ariaLabel?: string
}

export function Select({
  value,
  onValueChange,
  options,
  placeholder = 'Select…',
  disabled,
  invalid,
  className,
  id,
  ariaLabel,
}: SelectProps) {
  const fieldDescription = useFieldDescription()
  return (
    <RadixSelect.Root value={value} onValueChange={onValueChange} disabled={disabled}>
      <RadixSelect.Trigger
        id={id}
        aria-label={ariaLabel}
        aria-invalid={invalid || undefined}
        aria-describedby={fieldDescription}
        className={cn(
          'input-base flex items-center justify-between gap-2 text-left',
          invalid && 'border-state-danger',
          className,
        )}
      >
        <RadixSelect.Value placeholder={placeholder} />
        <RadixSelect.Icon>
          <ChevronDown className="h-4 w-4 text-content-subtle" />
        </RadixSelect.Icon>
      </RadixSelect.Trigger>
      <RadixSelect.Portal>
        <RadixSelect.Content
          position="popper"
          sideOffset={6}
          className="z-50 max-h-72 w-[var(--radix-select-trigger-width)] overpipeline-hidden rounded-xl border border-line bg-surface-overlay shadow-pop animate-slide-up"
        >
          <RadixSelect.Viewport className="p-1">
            {options.map((option) => (
              <RadixSelect.Item
                key={option.value}
                value={option.value}
                disabled={option.disabled}
                className={cn(
                  'relative flex cursor-pointer select-none flex-col gap-0.5 rounded-lg px-2.5 py-2 pr-8 text-sm outline-none',
                  'text-content-muted',
                  // A wash alone is ~1.1:1 against the overlay; the ring is what
                  // actually marks the row Enter would commit.
                  'data-[highlighted]:bg-brand-500/15 data-[highlighted]:text-content',
                  'data-[highlighted]:ring-1 data-[highlighted]:ring-inset',
                  'data-[highlighted]:ring-brand-600 dark:data-[highlighted]:ring-brand-400',
                  'data-[disabled]:pointer-events-none data-[disabled]:opacity-50',
                )}
              >
                <RadixSelect.ItemText>{option.label}</RadixSelect.ItemText>
                {option.hint && (
                  <span className="text-2xs text-content-subtle">{option.hint}</span>
                )}
                <RadixSelect.ItemIndicator className="absolute right-2.5 top-2.5">
                  <Check className="h-3.5 w-3.5 text-brand-500" />
                </RadixSelect.ItemIndicator>
              </RadixSelect.Item>
            ))}
          </RadixSelect.Viewport>
        </RadixSelect.Content>
      </RadixSelect.Portal>
    </RadixSelect.Root>
  )
}
