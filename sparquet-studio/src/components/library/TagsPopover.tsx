/**
 * The tag editor for one library record, in a popover.
 *
 * The same control on a Job, a Pipeline and a Workflow, because a tag means the
 * same thing on all three: the runner reads them when it charges a run — a Job's
 * own, plus what it inherits from its Pipeline and its Workflow — and freezes
 * them on the ledger entry. That is what makes Billing able to answer "what does
 * `finance` cost" without anybody having to reconcile spreadsheets.
 *
 * Tagging the Workflow is the cheap way to tag everything inside it. Repeating a
 * cost centre on forty Jobs guarantees that one of them ends up missing it and
 * quietly untagged on the invoice.
 */

import { Tag, X } from 'lucide-react'
import { useState, type KeyboardEvent } from 'react'

import { Badge, Button, IconButton, Input, Popover, PopoverContent, PopoverTrigger } from '@/components/ui'
import { addTag, hasTag, MAX_TAGS, MAX_TAG_LENGTH, removeTag } from '@/lib/tags'

export interface TagsPopoverProps {
  tags: string[]
  onChange: (tags: string[]) => void
  /** Tags already used elsewhere in the library, offered before a new one is typed. */
  suggestions?: string[]
  /** What is being tagged, for the labels a screen reader reads out. */
  subject?: string
}

export function TagsPopover({
  tags,
  onChange,
  suggestions = [],
  subject = 'this record',
}: TagsPopoverProps) {
  const [draft, setDraft] = useState('')

  const full = tags.length >= MAX_TAGS
  const unused = suggestions.filter((tag) => !hasTag(tags, tag)).slice(0, 12)

  const commit = (value: string) => {
    const next = addTag(tags, value)
    if (next !== tags) onChange(next)
    setDraft('')
  }

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter' || event.key === ',') {
      event.preventDefault()
      commit(draft)
      return
    }
    // Backspace on an empty box removes the last tag: the usual gesture, and it
    // saves reaching for a two-pixel × on every correction.
    if (event.key === 'Backspace' && !draft && tags.length) {
      onChange(tags.slice(0, -1))
    }
  }

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button size="xs" variant="ghost" icon={<Tag />}>
          {tags.length ? `${tags.length} tag${tags.length === 1 ? '' : 's'}` : 'Tags'}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[20rem] space-y-3 p-3">
        <div className="space-y-1">
          <p className="text-2xs font-semibold text-content">Tags</p>
          <p className="text-2xs leading-relaxed text-content-muted">
            Billing groups spending by these. A run is billed under the tags of its Job,
            its Pipeline and its Workflow together.
          </p>
        </div>

        {tags.length > 0 && (
          <ul className="flex flex-wrap gap-1">
            {tags.map((tag) => (
              <li key={tag}>
                <Badge tone="neutral" className="pr-0.5">
                  {tag}
                  <IconButton
                    size="xs"
                    label={`Remove the tag ${tag} from ${subject}`}
                    onClick={() => onChange(removeTag(tags, tag))}
                  >
                    <X />
                  </IconButton>
                </Badge>
              </li>
            ))}
          </ul>
        )}

        <Input
          value={draft}
          onChange={(event) => setDraft(event.target.value.slice(0, MAX_TAG_LENGTH))}
          onKeyDown={onKeyDown}
          onBlur={() => commit(draft)}
          disabled={full}
          aria-label={`Add a tag to ${subject}`}
          placeholder={full ? `${MAX_TAGS} tags is the limit` : 'Type a tag, press Enter'}
          className="h-8"
        />

        {unused.length > 0 && !full && (
          <div className="space-y-1">
            <p className="text-2xs text-content-subtle">Already used elsewhere</p>
            <ul className="flex flex-wrap gap-1">
              {unused.map((tag) => (
                <li key={tag}>
                  <button
                    type="button"
                    onClick={() => commit(tag)}
                    className="rounded-full border border-line px-2 py-0.5 text-2xs text-content-muted hover:border-brand-400 hover:text-content"
                  >
                    {tag}
                  </button>
                </li>
              ))}
            </ul>
          </div>
        )}
      </PopoverContent>
    </Popover>
  )
}
