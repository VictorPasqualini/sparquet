import { ArrowRight, Check, Clock, LayoutTemplate, RotateCcw } from 'lucide-react'
import { useCallback, useMemo, useSyncExternalStore } from 'react'
import { useNavigate } from 'react-router-dom'

import { Badge, Button, SectionTitle, useConfirm, type BadgeTone } from '@/components/ui'
import { LESSONS, type Lesson } from '@/data/lessons'
import { cn } from '@/lib/utils/cn'

export const LEVEL_TONE: Record<Lesson['level'], BadgeTone> = {
  starter: 'success',
  intermediate: 'info',
  advanced: 'warning',
}

/* ----------------------------------------------------------------- progress */

export const LESSON_STORAGE_KEY = 'sparquet-studio:lessons'

export interface LessonProgress {
  completed: boolean
  /** Indexes of the ticked checklist items. */
  checked: number[]
}

export type LessonProgressMap = Record<string, LessonProgress>

const EMPTY: LessonProgress = { completed: false, checked: [] }

// One module-level snapshot keeps the index and a lesson page in sync without a
// store: useSyncExternalStore needs a stable object, so it is cached until a write.
let snapshot: LessonProgressMap | null = null
const listeners = new Set<() => void>()

function readSnapshot(): LessonProgressMap {
  if (!snapshot) snapshot = parseProgress(window.localStorage.getItem(LESSON_STORAGE_KEY))
  return snapshot
}

function emit(): void {
  for (const listener of listeners) listener()
}

function writeSnapshot(next: LessonProgressMap): void {
  snapshot = next
  try {
    window.localStorage.setItem(LESSON_STORAGE_KEY, JSON.stringify(next))
  } catch {
    // Private-mode quota errors must not break the lesson UI.
  }
  emit()
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener)
  const onStorage = (event: StorageEvent) => {
    if (event.key !== null && event.key !== LESSON_STORAGE_KEY) return
    snapshot = null
    emit()
  }
  window.addEventListener('storage', onStorage)
  return () => {
    listeners.delete(listener)
    window.removeEventListener('storage', onStorage)
  }
}

/** Hand-written parser: anything malformed degrades to "nothing completed". */
function parseProgress(raw: string | null): LessonProgressMap {
  if (!raw) return {}
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    return {}
  }
  if (typeof parsed !== 'object' || parsed === null) return {}

  const map: LessonProgressMap = {}
  for (const [id, value] of Object.entries(parsed as Record<string, unknown>)) {
    if (typeof value !== 'object' || value === null) continue
    const entry = value as { completed?: unknown; checked?: unknown }
    map[id] = {
      completed: entry.completed === true,
      checked: Array.isArray(entry.checked)
        ? entry.checked.filter((index): index is number => Number.isInteger(index))
        : [],
    }
  }
  return map
}

export interface LessonProgressApi {
  map: LessonProgressMap
  progressOf: (lessonId: string) => LessonProgress
  /** Ticks or unticks one checklist item; ticking the last one completes the lesson. */
  toggleChecklistItem: (lessonId: string, index: number, total: number) => void
  setCompleted: (lessonId: string, completed: boolean) => void
  resetAll: () => void
}

export function useLessonProgress(): LessonProgressApi {
  const map = useSyncExternalStore(subscribe, readSnapshot, readSnapshot)

  const progressOf = useCallback((lessonId: string) => map[lessonId] ?? EMPTY, [map])

  const toggleChecklistItem = useCallback(
    (lessonId: string, index: number, total: number) => {
      const current = map[lessonId] ?? EMPTY
      const checked = current.checked.includes(index)
        ? current.checked.filter((value) => value !== index)
        : [...current.checked, index].sort((a, b) => a - b)
      writeSnapshot({
        ...map,
        [lessonId]: { completed: current.completed || checked.length >= total, checked },
      })
    },
    [map],
  )

  const setCompleted = useCallback(
    (lessonId: string, completed: boolean) => {
      const current = map[lessonId] ?? EMPTY
      writeSnapshot({ ...map, [lessonId]: { ...current, completed } })
    },
    [map],
  )

  const resetAll = useCallback(() => writeSnapshot({}), [])

  return { map, progressOf, toggleChecklistItem, setCompleted, resetAll }
}

/* -------------------------------------------------------------------- index */

export function Learn() {
  const navigate = useNavigate()
  const { progressOf, resetAll } = useLessonProgress()
  const [confirm, confirmDialog] = useConfirm()

  const completed = useMemo(
    () => LESSONS.filter((lesson) => progressOf(lesson.id).completed).length,
    [progressOf],
  )
  const next = useMemo(
    () => LESSONS.find((lesson) => !progressOf(lesson.id).completed) ?? null,
    [progressOf],
  )
  const minutesLeft = useMemo(
    () =>
      LESSONS.filter((lesson) => !progressOf(lesson.id).completed).reduce(
        (total, lesson) => total + lesson.minutes,
        0,
      ),
    [progressOf],
  )

  const reset = useCallback(() => {
    void (async () => {
      const ok = await confirm({
        title: 'Reset learning progress',
        message: 'Every completed lesson and ticked checklist item is cleared. Lessons stay.',
        confirmLabel: 'Reset progress',
      })
      if (ok) resetAll()
    })()
  }, [confirm, resetAll])

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8 animate-fade-in">
      <header className="mb-6 space-y-1">
        <h1 className="text-sm font-semibold text-content">Learn Sparquet</h1>
        <p className="max-w-2xl text-xs leading-relaxed text-content-muted">
          Six short lessons, in order, from a first pipeline to a parameterized job in
          production. Each one ends with a checklist you can run against your own data.
        </p>
      </header>

      <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_17rem] lg:items-start">
        <ol className="space-y-3">
          {LESSONS.map((lesson, index) => (
            <li key={lesson.id}>
              <LessonCard
                lesson={lesson}
                index={index}
                progress={progressOf(lesson.id)}
                onOpen={() => navigate(`/learn/${lesson.id}`)}
              />
            </li>
          ))}
        </ol>

        <aside className="space-y-4 lg:sticky lg:top-6">
          <section className="card p-4">
            <SectionTitle>Your progress</SectionTitle>
            <p className="mt-2 text-sm font-semibold text-content">
              {completed} of {LESSONS.length} completed
            </p>
            <ProgressBar value={completed} total={LESSONS.length} />
            <p className="mt-2 text-2xs leading-relaxed text-content-subtle">
              {completed === LESSONS.length
                ? 'The whole path is done. Revisit any lesson whenever a pipeline gets stubborn.'
                : `About ${minutesLeft} minutes of reading left.`}
            </p>

            {next && (
              <Button
                size="sm"
                variant="primary"
                fullWidth
                className="mt-3"
                trailing={<ArrowRight className="h-3.5 w-3.5" />}
                onClick={() => navigate(`/learn/${next.id}`)}
              >
                {completed === 0 ? 'Start the path' : 'Continue'}
              </Button>
            )}

            {completed > 0 && (
              <Button
                size="sm"
                variant="ghost"
                fullWidth
                className="mt-1.5"
                icon={<RotateCcw />}
                onClick={reset}
              >
                Reset progress
              </Button>
            )}
          </section>

          <section className="card p-4">
            <SectionTitle>Learn by reading code</SectionTitle>
            <p className="mt-2 text-2xs leading-relaxed text-content-muted">
              Every lesson has a matching template. Open one, put the canvas and the JSON panel
              side by side, and change something.
            </p>
            <Button
              size="sm"
              variant="secondary"
              fullWidth
              className="mt-3"
              icon={<LayoutTemplate />}
              onClick={() => navigate('/templates')}
            >
              Browse templates
            </Button>
          </section>
        </aside>
      </div>

      {confirmDialog}
    </div>
  )
}

/* ------------------------------------------------------------------- pieces */

interface LessonCardProps {
  lesson: Lesson
  index: number
  progress: LessonProgress
  onOpen: () => void
}

function LessonCard({ lesson, index, progress, onOpen }: LessonCardProps) {
  const ticked = progress.checked.length
  const started = ticked > 0 || progress.completed

  return (
    <article
      className={cn(
        'card flex items-start gap-4 p-4 transition-colors hover:border-line-strong',
        progress.completed && 'border-state-success/30',
      )}
    >
      <span
        aria-hidden
        className={cn(
          'mt-0.5 flex h-7 w-7 shrink-0 items-center justify-center rounded-lg border text-2xs font-semibold tabular-nums',
          progress.completed
            ? 'border-state-success/40 bg-state-success/12 text-state-success'
            : 'border-line bg-surface-sunken text-content-subtle',
        )}
      >
        {progress.completed ? <Check className="h-3.5 w-3.5" /> : index + 1}
      </span>

      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-sm font-semibold leading-snug text-content">{lesson.title}</h2>
          {progress.completed && <Badge tone="success">Completed</Badge>}
        </div>

        <div className="mt-1.5 flex flex-wrap items-center gap-2">
          <Badge tone={LEVEL_TONE[lesson.level]}>{lesson.level}</Badge>
          <span className="flex items-center gap-1 text-2xs text-content-subtle">
            <Clock className="h-3 w-3" aria-hidden />
            {lesson.minutes} min
          </span>
          <span className="text-2xs tabular-nums text-content-subtle">
            {ticked} of {lesson.checklist.length} steps
          </span>
        </div>

        <p className="mt-2 text-xs leading-relaxed text-content-muted">{lesson.summary}</p>
      </div>

      <Button
        size="sm"
        variant={progress.completed ? 'ghost' : 'secondary'}
        className="shrink-0"
        trailing={<ArrowRight className="h-3.5 w-3.5" />}
        onClick={onOpen}
      >
        {progress.completed ? 'Review' : started ? 'Continue' : 'Start'}
      </Button>
    </article>
  )
}

export function ProgressBar({ value, total }: { value: number; total: number }) {
  const percent = total === 0 ? 0 : Math.round((value / total) * 100)
  return (
    <div
      className="mt-2 h-1.5 w-full overflow-hidden rounded-full bg-surface-sunken"
      role="progressbar"
      aria-valuenow={value}
      aria-valuemin={0}
      aria-valuemax={total}
      aria-label="Lessons completed"
    >
      {/* Width is data, not styling — the only value that cannot live in a class. */}
      <div
        className="h-full rounded-full bg-brand-500 transition-[width] duration-300"
        style={{ width: `${percent}%` }}
      />
    </div>
  )
}
