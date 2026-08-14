import {
  ArrowLeft,
  ArrowRight,
  Check,
  Clock,
  Copy,
  LayoutTemplate,
  Lightbulb,
  ListChecks,
} from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { toast } from 'sonner'

import {
  Badge,
  Button,
  EmptyState,
  IconButton,
  renderInlineCode,
  SectionTitle,
} from '@/components/ui'
import { LESSONS, type Lesson, type LessonSection } from '@/data/lessons'
import { TEMPLATES } from '@/data/templates'
import { cn } from '@/lib/utils/cn'
import { copyText } from '@/lib/utils/download'
import { LEVEL_TONE, useLessonProgress } from '@/screens/Learn'

export function LessonDetail() {
  const { lessonId } = useParams<{ lessonId: string }>()
  const navigate = useNavigate()
  const { progressOf, toggleChecklistItem, setCompleted } = useLessonProgress()

  const index = LESSONS.findIndex((entry) => entry.id === lessonId)
  const lesson = index >= 0 ? LESSONS[index] : null
  const rootRef = useRef<HTMLDivElement>(null)

  // Prev/next must start the next lesson at the top, wherever the app scrolls.
  useEffect(() => {
    window.scrollTo({ top: 0 })
    for (let node = rootRef.current?.parentElement; node; node = node.parentElement) {
      if (node.scrollHeight > node.clientHeight + 1) {
        node.scrollTop = 0
        break
      }
    }
  }, [lessonId])

  if (!lesson) {
    return (
      <div className="mx-auto w-full max-w-3xl px-6 py-8">
        <div className="card">
          <EmptyState
            icon={<ListChecks />}
            title="This lesson does not exist"
            description="It may have been renamed. The index has all six."
            action={
              <Button size="sm" variant="secondary" onClick={() => navigate('/learn')}>
                Back to lessons
              </Button>
            }
          />
        </div>
      </div>
    )
  }

  const progress = progressOf(lesson.id)
  const previous = index > 0 ? LESSONS[index - 1] : null
  const next = index < LESSONS.length - 1 ? LESSONS[index + 1] : null
  const template = lesson.templateId
    ? (TEMPLATES.find((entry) => entry.id === lesson.templateId) ?? null)
    : null

  return (
    <div ref={rootRef} className="mx-auto w-full max-w-5xl px-6 py-8 animate-fade-in">
      <Button
        size="xs"
        variant="ghost"
        icon={<ArrowLeft />}
        className="-ml-2 mb-4"
        onClick={() => navigate('/learn')}
      >
        All lessons
      </Button>

      <div className="grid gap-8 lg:grid-cols-[minmax(0,1fr)_15rem] lg:items-start">
        <article className="min-w-0">
          <header className="space-y-2">
            <h1 className="text-sm font-semibold leading-snug text-content">{lesson.title}</h1>
            <div className="flex flex-wrap items-center gap-2">
              <Badge tone={LEVEL_TONE[lesson.level]}>{lesson.level}</Badge>
              <span className="flex items-center gap-1 text-2xs text-content-subtle">
                <Clock className="h-3 w-3" aria-hidden />
                {lesson.minutes} min
              </span>
              <span className="text-2xs tabular-nums text-content-subtle">
                Lesson {index + 1} of {LESSONS.length}
              </span>
              {progress.completed && <Badge tone="success">Completed</Badge>}
            </div>
            <p className="text-xs leading-relaxed text-content-muted">{lesson.summary}</p>
          </header>

          <div className="mt-6 space-y-6">
            {lesson.sections.map((section, sectionIndex) => (
              <Section
                key={section.heading}
                section={section}
                id={sectionId(lesson, sectionIndex)}
              />
            ))}
          </div>

          <section className="card mt-8 p-4">
            <SectionTitle
              action={
                <span className="text-2xs tabular-nums text-content-subtle">
                  {progress.checked.length} of {lesson.checklist.length} done
                </span>
              }
            >
              Try it on your own data
            </SectionTitle>
            <ul className="mt-2 space-y-0.5">
              {lesson.checklist.map((item, itemIndex) => (
                <li key={item}>
                  <ChecklistItem
                    label={item}
                    checked={progress.checked.includes(itemIndex)}
                    onToggle={() =>
                      toggleChecklistItem(lesson.id, itemIndex, lesson.checklist.length)
                    }
                  />
                </li>
              ))}
            </ul>
          </section>

          <nav
            className="mt-6 flex flex-wrap items-center gap-2"
            aria-label="Lesson navigation"
          >
            {previous && (
              <Button
                size="sm"
                variant="secondary"
                icon={<ArrowLeft />}
                onClick={() => navigate(`/learn/${previous.id}`)}
              >
                {previous.title}
              </Button>
            )}
            {next && (
              <Button
                size="sm"
                variant="secondary"
                className="ml-auto"
                trailing={<ArrowRight className="h-3.5 w-3.5" />}
                onClick={() => navigate(`/learn/${next.id}`)}
              >
                {next.title}
              </Button>
            )}
          </nav>
        </article>

        <aside className="space-y-4 lg:sticky lg:top-6">
          <section className="card p-4">
            <SectionTitle>On this page</SectionTitle>
            <ul className="mt-2 space-y-0.5">
              {lesson.sections.map((section, sectionIndex) => (
                <li key={section.heading}>
                  <button
                    type="button"
                    onClick={() => reveal(sectionId(lesson, sectionIndex))}
                    className={cn(
                      'w-full rounded-md px-2 py-1 text-left text-2xs leading-relaxed text-content-muted',
                      'transition-colors hover:bg-surface-sunken hover:text-content',
                    )}
                  >
                    {section.heading}
                  </button>
                </li>
              ))}
            </ul>
          </section>

          <section className="card space-y-2 p-4">
            <Button
              size="sm"
              fullWidth
              variant={progress.completed ? 'secondary' : 'primary'}
              icon={<Check />}
              onClick={() => setCompleted(lesson.id, !progress.completed)}
            >
              {progress.completed ? 'Mark as unread' : 'Mark as complete'}
            </Button>

            {template && (
              <>
                <Button
                  size="sm"
                  fullWidth
                  variant="secondary"
                  icon={<LayoutTemplate />}
                  onClick={() => navigate('/templates', { state: { templateId: template.id } })}
                >
                  Open the matching template
                </Button>
                <p className="text-2xs leading-relaxed text-content-subtle">
                  {template.name} — {template.summary}
                </p>
              </>
            )}
          </section>
        </aside>
      </div>
    </div>
  )
}

/* ------------------------------------------------------------------ section */

function Section({ section, id }: { section: LessonSection; id: string }) {
  return (
    <section id={id} className="scroll-mt-6">
      <h2 className="text-xs font-semibold text-content">{section.heading}</h2>
      <Prose text={section.body} className="mt-2" />

      {section.tip && (
        <p className="mt-3 flex items-start gap-2 rounded-lg border border-brand-500/25 bg-brand-500/10 p-2.5 text-2xs leading-relaxed text-content-muted">
          <Lightbulb
            className="mt-px h-3.5 w-3.5 shrink-0 text-brand-600 dark:text-brand-400"
            aria-hidden
          />
          <span>{renderRich(section.tip)}</span>
        </p>
      )}

      {section.code && <CodeBlock code={section.code} label={section.heading} />}
    </section>
  )
}

function CodeBlock({ code, label }: { code: string; label: string }) {
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    if (!copied) return
    const timer = window.setTimeout(() => setCopied(false), 1400)
    return () => window.clearTimeout(timer)
  }, [copied])

  const copy = useCallback(() => {
    void (async () => {
      if (await copyText(code)) setCopied(true)
      else toast.error('Could not copy to the clipboard')
    })()
  }, [code])

  return (
    <figure className="mt-3 overflow-hidden rounded-lg border border-line bg-surface-sunken">
      <figcaption className="flex items-center gap-2 border-b border-line px-2.5 py-1.5">
        <span className="font-mono text-2xs text-content-subtle">json</span>
        <IconButton
          size="sm"
          label={`Copy the snippet from “${label}”`}
          onClick={copy}
          className="ml-auto"
        >
          {copied ? <Check className="text-state-success" /> : <Copy />}
        </IconButton>
      </figcaption>
      {/* Focusable so the snippet can be scrolled from the keyboard. */}
      <pre
        tabIndex={0}
        aria-label={`JSON snippet from “${label}”`}
        className="scroll-area max-h-96 overflow-x-auto p-3 font-mono text-2xs leading-relaxed text-content-muted"
      >
        {code}
      </pre>
    </figure>
  )
}

function ChecklistItem({
  label,
  checked,
  onToggle,
}: {
  label: string
  checked: boolean
  onToggle: () => void
}) {
  return (
    <label
      className={cn(
        'flex cursor-pointer items-start gap-2.5 rounded-lg px-2 py-1.5',
        'transition-colors hover:bg-surface-sunken',
      )}
    >
      <input type="checkbox" checked={checked} onChange={onToggle} className="peer sr-only" />
      <span
        aria-hidden
        className={cn(
          'mt-px flex h-4 w-4 shrink-0 items-center justify-center rounded border transition-colors',
          'peer-focus-visible:ring-2 peer-focus-visible:ring-brand-500/70',
          checked
            ? 'border-brand-500 bg-brand-500 text-black'
            : 'border-line-strong bg-surface',
        )}
      >
        {checked && <Check className="h-3 w-3" />}
      </span>
      <span
        className={cn(
          'text-xs leading-relaxed',
          checked ? 'text-content-subtle line-through' : 'text-content-muted',
        )}
      >
        {renderInlineCode(label)}
      </span>
    </label>
  )
}

/* ------------------------------------------------------------------- markup */

/** Lesson bodies are markdown-lite: paragraphs, `- ` bullets, **bold** and `code`. */
function Prose({ text, className }: { text: string; className?: string }) {
  const blocks = useMemo(() => text.split('\n\n').flatMap(toChunks), [text])

  return (
    <div className={cn('space-y-2.5 text-xs leading-relaxed text-content-muted', className)}>
      {blocks.map((block, index) =>
        block.kind === 'list' ? (
          <ul key={index} className="space-y-1.5">
            {block.items.map((item) => (
              <li key={item} className="flex gap-2">
                <span
                  aria-hidden
                  className="mt-[0.4rem] h-1 w-1 shrink-0 rounded-full bg-brand-500"
                />
                <span>{renderRich(item)}</span>
              </li>
            ))}
          </ul>
        ) : (
          <p key={index}>{renderRich(block.text)}</p>
        ),
      )}
    </div>
  )
}

type Chunk = { kind: 'text'; text: string } | { kind: 'list'; items: string[] }

/** Splits a paragraph into runs of prose and runs of consecutive bullet lines. */
function toChunks(block: string): Chunk[] {
  const chunks: Chunk[] = []

  for (const raw of block.split('\n')) {
    const line = raw.trim()
    if (!line) continue
    const last = chunks[chunks.length - 1]

    if (/^[-*]\s+/.test(line)) {
      const item = line.replace(/^[-*]\s+/, '')
      if (last?.kind === 'list') last.items.push(item)
      else chunks.push({ kind: 'list', items: [item] })
      continue
    }

    if (last?.kind === 'text') last.text = `${last.text} ${line}`
    else chunks.push({ kind: 'text', text: line })
  }

  return chunks
}

/** `code` spans come from the shared helper; **bold** is handled here. */
function renderRich(text: string): ReactNode[] {
  return text.split(/(\*\*[^*]+\*\*)/g).map((part, index) => {
    if (part.startsWith('**') && part.endsWith('**') && part.length > 4) {
      return (
        <strong key={index} className="font-semibold text-content">
          {renderInlineCode(part.slice(2, -2))}
        </strong>
      )
    }
    return <span key={index}>{renderInlineCode(part)}</span>
  })
}

/* ------------------------------------------------------------------ helpers */

function sectionId(lesson: Lesson, index: number): string {
  return `${lesson.id}-section-${index}`
}

function reveal(id: string): void {
  document.getElementById(id)?.scrollIntoView({ behavior: 'smooth', block: 'start' })
}
