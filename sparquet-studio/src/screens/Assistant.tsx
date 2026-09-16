/**
 * The front door of the Studio: a conversation.
 *
 * Everything else here starts from something you already have — a canvas, a
 * workflow, a directory of confs. That is the wrong first screen for the person
 * who arrives knowing what they want the data to do and not which of eleven
 * transformations does it. A question is the cheapest way in, so `/` asks for
 * one.
 *
 * It is deliberately **not** the canvas assistant. That panel edits the job you
 * have open: it ships the pipeline, the selection and the lint issues with every
 * request and answers with a proposal to apply. This one has no job and sends no
 * pipeline — nothing about your data leaves the browser beyond what you type. It
 * answers about Sparquet itself: which format reads that, what `stop_if_empty`
 * does, why a join is dropping rows.
 *
 * The provider, model and key are the same ones Settings already holds, so this
 * screen adds no second place to configure anything and no second bill.
 *
 * With the local runner as the provider it can also *look*: read the formats
 * this installation registered, validate a config against the framework. What
 * it still cannot do is *write* — create the job it just described, run it. That
 * is the next step in `BACKLOG.md`, and the transcript shape below is already
 * the one it needs.
 */

import {
  FileJson,
  GitMerge,
  KeyRound,
  Radio,
  Send,
  ServerCog,
  ShieldCheck,
  Sparkles,
  Square,
  Trash2,
  Wrench,
  type LucideIcon,
} from 'lucide-react'
import { nanoid } from 'nanoid'
import { useEffect, useRef, useState, type KeyboardEvent } from 'react'
import { Link } from 'react-router-dom'

import logoMark from '@/assets/logo.png'
import { PageHeader, PageShell } from '@/components/layout/PageShell'
import { Button, IconButton, Spinner, Textarea, renderInlineCode } from '@/components/ui'
import { sendAiRequest } from '@/lib/ai/client'
import { buildSystemPrompt, buildUserPrompt } from '@/lib/ai/prompt'
import { AI_PROVIDER_INFO } from '@/lib/ai/providers'
import { getAssistantInfo } from '@/lib/runner/assistant'
import { cn } from '@/lib/utils/cn'
import { useSettingsStore } from '@/store/settings'
import type { AssistantInfo } from '@/types/assistant'

/** Six rows of composer, in pixels — past this the textarea scrolls. */
const COMPOSER_MAX_HEIGHT = 152

/**
 * Characters of transcript sent in one request (~4 chars per token). Without a
 * cap a long conversation grows until the provider rejects it, and the rejection
 * arrives after the call was billed.
 */
const HISTORY_BUDGET = 40_000

/**
 * Openings that show what the screen is for without making anyone guess. Four
 * different shapes of question on purpose — a "show me one", a "which of these",
 * a "why is mine wrong" and a "how do I" — so the range is legible at a glance.
 */
const SUGGESTIONS: { icon: LucideIcon; title: string; prompt: string }[] = [
  {
    icon: FileJson,
    title: 'Show me a pipeline',
    prompt: 'What does a Sparquet pipeline JSON look like, end to end?',
  },
  {
    icon: Radio,
    title: 'Read from Kafka',
    prompt: 'Which formats can Sparquet read from Kafka, and what do I have to set?',
  },
  {
    icon: GitMerge,
    title: 'My join lost rows',
    prompt: 'My join is dropping rows. What usually causes that here?',
  },
  {
    icon: ShieldCheck,
    title: 'Quarantine bad rows',
    prompt: 'How do I keep the rows that fail a validation instead of only counting them?',
  },
]

interface Message {
  id: string
  role: 'user' | 'assistant'
  content: string
  /** Set when the request failed; the bubble then explains instead of answering. */
  error?: string
  /**
   * Tools the runner ran while writing this answer. Shown because an answer that
   * checked the installed formats is worth more than one that recalled them, and
   * the reader cannot tell the two apart from the prose.
   */
  tools?: string[]
}

/** The last turns that fit the budget, oldest dropped first. */
function budgeted(messages: Message[]): { role: 'user' | 'assistant'; content: string }[] {
  const kept: Message[] = []
  let size = 0
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index]
    if (message.error) continue
    size += message.content.length
    if (size > HISTORY_BUDGET && kept.length > 0) break
    kept.unshift(message)
  }
  return kept.map(({ role, content }) => ({ role, content }))
}

export function Assistant() {
  const ai = useSettingsStore((state) => state.ai)
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const provider = AI_PROVIDER_INFO[ai.provider]
  const needsKey = provider.requiresKey && !ai.apiKey.trim()
  const usingRunner = ai.provider === 'runner'

  const [messages, setMessages] = useState<Message[]>([])
  const [draft, setDraft] = useState('')
  const [busy, setBusy] = useState(false)
  const [assistant, setAssistant] = useState<AssistantInfo | null>(null)
  // Told apart from `assistant: null` on purpose: no runner at all is a
  // different thing to fix than a runner with no model behind it.
  const [runnerDown, setRunnerDown] = useState(false)
  const abortRef = useRef<AbortController | null>(null)
  const composerRef = useRef<HTMLTextAreaElement>(null)
  const endRef = useRef<HTMLDivElement>(null)

  // Only the tail matters while an answer streams in; jumping on every token is
  // what makes a chat readable instead of a thing you chase.
  useEffect(() => {
    endRef.current?.scrollIntoView({ block: 'end' })
  }, [messages])

  useEffect(() => () => abortRef.current?.abort(), [])

  // Asked once per provider change, not per question: the answer only moves when
  // somebody restarts the runner, and a screen that probes a port on every
  // keystroke is a screen that logs a failed connection on every keystroke.
  useEffect(() => {
    if (!usingRunner) {
      setAssistant(null)
      setRunnerDown(false)
      return
    }
    const controller = new AbortController()
    getAssistantInfo(runnerUrl, runnerToken, controller.signal)
      .then((info) => {
        setAssistant(info)
        setRunnerDown(false)
      })
      .catch(() => {
        if (controller.signal.aborted) return
        setAssistant(null)
        setRunnerDown(true)
      })
    return () => controller.abort()
  }, [usingRunner, runnerUrl, runnerToken])

  function grow(element: HTMLTextAreaElement): void {
    element.style.height = 'auto'
    element.style.height = `${Math.min(element.scrollHeight, COMPOSER_MAX_HEIGHT)}px`
  }

  async function send(text: string): Promise<void> {
    const question = text.trim()
    if (!question || busy) return

    const user: Message = { id: nanoid(), role: 'user', content: question }
    const answerId = nanoid()
    const history = budgeted(messages)

    setMessages((previous) => [
      ...previous,
      user,
      { id: answerId, role: 'assistant', content: '' },
    ])
    setDraft('')
    if (composerRef.current) {
      composerRef.current.style.height = 'auto'
    }
    setBusy(true)

    const controller = new AbortController()
    abortRef.current = controller

    try {
      await sendAiRequest({
        settings: ai,
        system: buildSystemPrompt(),
        // No pipeline, no issues, no selected node: this screen has no job open,
        // and an empty context is the honest way to say so.
        messages: [...history, { role: 'user', content: buildUserPrompt('chat', question, {}) }],
        signal: controller.signal,
        runner: { baseUrl: runnerUrl, token: runnerToken },
        onTool: (call) =>
          setMessages((previous) =>
            previous.map((message) =>
              message.id === answerId
                ? { ...message, tools: [...(message.tools ?? []), call.name] }
                : message,
            ),
          ),
        onToken: (chunk) =>
          setMessages((previous) =>
            previous.map((message) =>
              message.id === answerId
                ? { ...message, content: message.content + chunk }
                : message,
            ),
          ),
      })
    } catch (caught) {
      const stopped = controller.signal.aborted
      const reason = caught instanceof Error ? caught.message : String(caught)
      setMessages((previous) =>
        previous.map((message) =>
          message.id === answerId
            ? {
                ...message,
                // A stopped answer keeps whatever arrived; a failed one says why.
                error: stopped ? undefined : reason,
                content: message.content || (stopped ? 'Stopped.' : ''),
              }
            : message,
        ),
      )
    } finally {
      abortRef.current = null
      setBusy(false)
      composerRef.current?.focus()
    }
  }

  function onComposerKeyDown(event: KeyboardEvent<HTMLTextAreaElement>): void {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault()
      void send(draft)
    }
  }

  const started = messages.length > 0

  return (
    <PageShell width="default" className="flex min-h-full flex-col">
      {/* The header is the running conversation's chrome. On an empty screen it
          would be a second title above the one in the middle, so it waits. */}
      {started && (
        <PageHeader
          icon={<Sparkles />}
          title="Assistant"
          description="Ask about Sparquet — the pipeline JSON, a format's options, why a run
            behaved the way it did. Nothing from your workflows is sent: only what you type."
          className="mb-5"
          actions={
            <IconButton
              label="Clear conversation"
              size="sm"
              disabled={busy}
              onClick={() => setMessages([])}
            >
              <Trash2 />
            </IconButton>
          }
        />
      )}

      {usingRunner && runnerDown && (
        <div className="mb-4 flex items-start gap-2.5 rounded-lg border border-line bg-surface-sunken p-3 text-xs text-content-muted">
          <ServerCog className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          <p>
            No local model is answering yet. Start the{' '}
            <Link
              to="/settings"
              className="text-brand-600 hover:underline dark:text-brand-400"
            >
              local runner
            </Link>{' '}
            and it answers here for free, or install{' '}
            <a
              href="https://ollama.com"
              target="_blank"
              rel="noreferrer"
              className="text-brand-600 hover:underline dark:text-brand-400"
            >
              Ollama
            </a>{' '}
            and run <code className="font-mono">ollama pull qwen2.5-coder:7b</code> — the
            Studio picks either up on its own. A provider with a key works too.
          </p>
        </div>
      )}

      {usingRunner && assistant && !assistant.available && (
        <div className="mb-4 flex items-start gap-2.5 rounded-lg border border-line bg-surface-sunken p-3 text-xs text-content-muted">
          <ServerCog className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          <p>
            The runner cannot answer yet. {assistant.error || ''}{' '}
            {assistant.hint || (
              <>
                Choose another provider in{' '}
                <Link
                  to="/settings"
                  className="text-brand-600 hover:underline dark:text-brand-400"
                >
                  Settings
                </Link>
                .
              </>
            )}
          </p>
        </div>
      )}

      {needsKey && (
        <div className="mb-4 flex items-start gap-2.5 rounded-lg border border-line bg-surface-sunken p-3 text-xs text-content-muted">
          <KeyRound className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          <p>
            {provider.label} needs an API key before it will answer. Add one in{' '}
            <Link to="/settings" className="text-brand-600 hover:underline dark:text-brand-400">
              Settings
            </Link>
            . It stays in this browser and requests go straight from here to the provider.
          </p>
        </div>
      )}

      <div className={cn('flex-1', started ? 'space-y-4' : 'flex flex-col justify-center')}>
        {started ? (
          messages.map((message) => <Bubble key={message.id} message={message} />)
        ) : (
          <Welcome busy={busy} onPick={(prompt) => void send(prompt)} />
        )}
        <div ref={endRef} />
      </div>

      <div className="sticky bottom-0 -mx-6 mt-5 border-t border-line bg-canvas px-6 py-3">
        <div className="flex items-end gap-2">
          <Textarea
            ref={composerRef}
            rows={1}
            value={draft}
            placeholder="Ask about a pipeline, a format, an error…"
            className="max-h-[152px] resize-none"
            onChange={(event) => {
              setDraft(event.target.value)
              grow(event.target)
            }}
            onKeyDown={onComposerKeyDown}
          />
          {busy ? (
            <Button
              size="sm"
              variant="secondary"
              icon={<Square className="h-3.5 w-3.5" />}
              onClick={() => abortRef.current?.abort()}
            >
              Stop
            </Button>
          ) : (
            <Button
              size="sm"
              variant="primary"
              icon={<Send className="h-3.5 w-3.5" />}
              disabled={!draft.trim()}
              onClick={() => void send(draft)}
            >
              Send
            </Button>
          )}
        </div>
        <p className="mt-1.5 text-2xs text-content-subtle">
          Enter sends, Shift+Enter breaks the line. Answers come from{' '}
          {usingRunner && assistant?.model ? `${provider.label} (${assistant.model})` : provider.label}
          {usingRunner && assistant?.local ? ', run on the runner’s machine at no cost,' : ''} and
          can be wrong — check the JSON before you run it.
        </p>
      </div>
    </PageShell>
  )
}

/* ------------------------------------------------------------------ welcome */

/**
 * What fills the screen before anyone has typed. A blank page and a text box is
 * the honest layout and the least inviting one: it asks a question without
 * saying what kind of answer it is any good at. The mark, one sentence and four
 * openings do that in the space the transcript will take over anyway.
 */
function Welcome({ busy, onPick }: { busy: boolean; onPick: (prompt: string) => void }) {
  return (
    <div className="flex flex-col items-center gap-6 py-6 text-center">
      <div className="relative">
        {/* A soft glow behind the mark, so it reads as a greeting rather than as
            a logo someone forgot to put in a corner. */}
        <div
          className="absolute inset-0 -z-10 scale-[2.2] rounded-full bg-brand-500/15 blur-2xl dark:bg-brand-400/15"
          aria-hidden
        />
        <img src={logoMark} alt="" width={72} height={72} className="h-[72px] w-[72px] drop-shadow-sm" />
      </div>

      <div className="space-y-1.5">
        <h1 className="text-lg font-semibold text-content">What should the data do?</h1>
        <p className="mx-auto max-w-md text-xs leading-relaxed text-content-muted">
          Describe it in your own words and I will answer in Sparquet&apos;s — the pipeline
          JSON, the format that reads it, the transformation that shapes it. Nothing from
          your workflows is sent: only what you type.
        </p>
      </div>

      <div className="grid w-full max-w-2xl gap-2 sm:grid-cols-2">
        {SUGGESTIONS.map((suggestion) => (
          <button
            key={suggestion.prompt}
            type="button"
            disabled={busy}
            onClick={() => onPick(suggestion.prompt)}
            className="group flex items-start gap-2.5 rounded-xl border border-line bg-surface-raised p-3 text-left transition-colors hover:border-brand-500/40 hover:bg-surface-overlay disabled:opacity-50"
          >
            <suggestion.icon
              className="mt-0.5 h-4 w-4 shrink-0 text-content-subtle transition-colors group-hover:text-brand-600 dark:group-hover:text-brand-400"
              aria-hidden
            />
            <span className="space-y-0.5">
              <span className="block text-xs font-medium text-content">{suggestion.title}</span>
              <span className="block text-2xs leading-relaxed text-content-subtle">
                {suggestion.prompt}
              </span>
            </span>
          </button>
        ))}
      </div>
    </div>
  )
}

/* ------------------------------------------------------------------- bubble */

/** Paragraphs, inline `code` and fenced blocks — the subset models actually emit. */
function Bubble({ message }: { message: Message }) {
  const mine = message.role === 'user'

  return (
    <div className={cn('flex', mine ? 'justify-end' : 'justify-start')}>
      <div
        className={cn(
          'max-w-[85%] rounded-xl px-3 py-2 text-xs leading-relaxed',
          mine
            ? 'bg-brand-500/12 text-content'
            : 'border border-line bg-surface-raised text-content-muted',
        )}
      >
        {message.tools && message.tools.length > 0 && (
          <p className="mb-1.5 flex flex-wrap items-center gap-1 text-2xs text-content-subtle">
            <Wrench className="h-3 w-3" aria-hidden />
            {/* Named, not counted: "checked the installed formats" and "validated
                the config" are different claims about how much to trust this. */}
            {[...new Set(message.tools)].join(', ')}
          </p>
        )}
        {message.error ? (
          <p className="text-state-danger">{message.error}</p>
        ) : message.content ? (
          <Body content={message.content} />
        ) : (
          <Spinner className="h-3.5 w-3.5" />
        )}
      </div>
    </div>
  )
}

function Body({ content }: { content: string }) {
  // Fences are kept verbatim; everything between them is prose with inline code.
  const parts = content.split(/```/)

  return (
    <div className="space-y-2">
      {parts.map((part, index) =>
        index % 2 === 1 ? (
          <pre
            key={index}
            className="overflow-x-auto rounded-lg border border-line bg-surface-sunken p-2 font-mono text-[11px] text-content"
          >
            {part.replace(/^[a-z]*\n/i, '')}
          </pre>
        ) : (
          part
            .split(/\n{2,}/)
            .filter((paragraph) => paragraph.trim().length > 0)
            .map((paragraph, paragraphIndex) => (
              <p key={`${index}-${paragraphIndex}`} className="whitespace-pre-wrap break-words">
                {renderInlineCode(paragraph.trim())}
              </p>
            ))
        ),
      )}
    </div>
  )
}
