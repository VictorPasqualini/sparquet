/**
 * The assistant, as one conversation rendered in two places.
 *
 * There used to be two: a panel beside the canvas that knew the open job and
 * could propose a pipeline to apply, and a screen at `/` that could only chat.
 * Two transcripts, two composers, two sets of quick prompts - and the person
 * who asked the front door for a pipeline got prose, while the person who asked
 * the panel got something they could click. Which one you were talking to was
 * not a thing anybody chose; it was where you happened to be standing.
 *
 * So it is one component, and the only thing that varies is what it stands next
 * to:
 *
 * * On the canvas (`layout="panel"`) it reads the open job - pipeline, lint
 *   issues, selected node - and an accepted proposal rebuilds that canvas.
 * * At the front door (`layout="page"`) there is no job, so nothing about one is
 *   sent, and an accepted proposal becomes a **new** Job: the same button, and
 *   the only honest meaning it has where no canvas is open.
 *
 * Everything else - intents, the tools the runner reports calling, the proposal
 * card, the history budget, retry - is the same code in both, because a
 * capability that exists in one of two chat boxes is a capability the user has
 * to know the floor plan to find.
 */

import {
  AlertTriangle,
  BookOpen,
  Check,
  Copy,
  FileText,
  KeyRound,
  Paperclip,
  Plus,
  RefreshCw,
  Send,
  ServerCog,
  Settings2,
  Sparkles,
  Square,
  Wand2,
  Wrench,
  Zap,
  type LucideIcon,
} from 'lucide-react'
import { nanoid } from 'nanoid'
import { useEffect, useRef, useState, type KeyboardEvent, type ReactNode } from 'react'
import { useNavigate } from 'react-router-dom'
import { toast } from 'sonner'

import {
  Badge,
  Button,
  EmptyState,
  IconButton,
  Kbd,
  Modal,
  renderInlineCode,
  Spinner,
  Textarea,
} from '@/components/ui'
import { sendAiRequest } from '@/lib/ai/client'
import { extractProposalFor } from '@/lib/ai/parse'
import { buildSystemPrompt, buildUserPrompt, type AiPromptContext } from '@/lib/ai/prompt'
import { AI_PROVIDER_INFO } from '@/lib/ai/providers'
import { getAssistantInfo } from '@/lib/runner/assistant'
import { cn } from '@/lib/utils/cn'
import { ProposalCard } from '@/components/panels/ProposalCard'
import { useSettingsStore } from '@/store/settings'
import type { AiIntent, AiMessage } from '@/types/ai'
import type { AssistantInfo } from '@/types/assistant'

import { CreateJobFromProposal } from './CreateJobFromProposal'

/** Six rows of the composer, in pixels — beyond this the textarea scrolls. */
const COMPOSER_MAX_HEIGHT = 152

/**
 * Characters of prior conversation kept in one request (~4 chars per token). Every
 * proposal reply carries a full pipeline, so an untrimmed transcript grows without
 * bound until the provider rejects it — and Retry would replay the same oversized call.
 */
const HISTORY_BUDGET = 60_000

/** Lint issues embedded in one request; the rest would only pad the prompt. */
export const MAX_CONTEXT_ISSUES = 40

interface IntentOption {
  id: AiIntent
  label: string
  icon: LucideIcon
  placeholder: string
}

const INTENTS: IntentOption[] = [
  {
    id: 'generate',
    label: 'Generate',
    icon: Sparkles,
    placeholder: 'Describe the pipeline you want…',
  },
  {
    id: 'modify',
    label: 'Modify',
    icon: Wand2,
    placeholder: 'Describe the change to make…',
  },
  {
    id: 'explain',
    label: 'Explain',
    icon: BookOpen,
    placeholder: 'Ask anything about this pipeline…',
  },
  {
    id: 'fix',
    label: 'Fix issues',
    icon: Wrench,
    placeholder: 'Describe the problem, or send to fix the reported issues…',
  },
  {
    id: 'optimize',
    label: 'Optimize',
    icon: Zap,
    placeholder: 'What should be faster or cheaper?',
  },
  {
    id: 'document',
    label: 'Document',
    icon: FileText,
    placeholder: 'What should the documentation cover?',
  },
]

interface QuickPrompt {
  text: string
  intent: AiIntent
  icon: LucideIcon
}

const QUICK_PROMPTS: QuickPrompt[] = [
  {
    text: 'Read a CSV, drop duplicates and write Parquet partitioned by date',
    intent: 'generate',
    icon: Sparkles,
  },
  {
    text: 'Ingest a Delta table, join it with customers on customer_id and keep the last event per id',
    intent: 'generate',
    icon: Sparkles,
  },
  {
    text: 'Add a data quality block that fails on nulls in id',
    intent: 'modify',
    icon: Wand2,
  },
  {
    text: 'Convert the output to a Delta merge on id',
    intent: 'modify',
    icon: Wand2,
  },
  {
    text: 'Explain what this pipeline does',
    intent: 'explain',
    icon: BookOpen,
  },
  {
    text: 'Review this pipeline for performance and suggest where to checkpoint',
    intent: 'optimize',
    icon: Zap,
  },
]

export interface AiConversationProps {
  /** `panel` is the rail beside the canvas; `page` is the screen at `/`. */
  layout: 'panel' | 'page'
  /**
   * The open job, read when a question is sent rather than handed over as a
   * value: the canvas moves between typing and sending, and what travels has to
   * be what is on screen the moment the request leaves.
   *
   * Absent means there is no job - the front door - and then nothing about one
   * is sent at all.
   */
  readContext?: () => AiPromptContext
  /** Lint issues on the open job: what the "Fix the issues" chip acts on. */
  issueCount?: number
  /**
   * Applies an accepted proposal where there is a canvas to apply it to. Absent
   * and the proposal offers to become a new Job instead.
   */
  onApply?: (pipeline: unknown) => void
  /**
   * What fills the screen before anybody has typed, in place of the default
   * empty state and its quick prompts. It is handed `ask` so its own openings
   * send a turn like any other - they are quick prompts, not decoration.
   */
  welcome?: (ask: (prompt: string) => void) => ReactNode
}

export function AiConversation({
  layout,
  readContext,
  issueCount = 0,
  onApply,
  welcome,
}: AiConversationProps) {
  const navigate = useNavigate()
  const page = layout === 'page'

  const ai = useSettingsStore((state) => state.ai)
  const setAi = useSettingsStore((state) => state.setAi)
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)

  const [messages, setMessages] = useState<AiMessage[]>([])
  /** A proposal waiting on the dialog that turns it into a Job. */
  const [creating, setCreating] = useState<AiMessage | null>(null)
  /** What the runner says about itself, asked once per provider change. */
  const [assistant, setAssistant] = useState<AssistantInfo | null>(null)
  // Told apart from `assistant: null` on purpose: no runner at all is a
  // different thing to fix than a runner with no model behind it.
  const [runnerDown, setRunnerDown] = useState(false)
  /** Non-fatal remarks about a turn, keyed by assistant message id. */
  const [notes, setNotes] = useState<Record<string, string>>({})
  const [input, setInput] = useState('')
  const [intent, setIntent] = useState<AiIntent>('generate')
  const [streamingId, setStreamingId] = useState<string | null>(null)
  const [viewingId, setViewingId] = useState<string | null>(null)

  const abortRef = useRef<AbortController | null>(null)
  const composerRef = useRef<HTMLTextAreaElement>(null)
  const scrollRef = useRef<HTMLDivElement>(null)
  /** False once the reader scrolls up, so streaming does not yank them back down. */
  const stickRef = useRef(true)

  const provider = AI_PROVIDER_INFO[ai.provider]
  const needsKey = provider.requiresKey && ai.apiKey.trim() === ''
  const usingRunner = ai.provider === 'runner'
  const streaming = streamingId !== null
  const active = INTENTS.find((option) => option.id === intent) ?? INTENTS[0]
  const draft = resolveText(input, intent, issueCount)
  const canSend = draft.length > 0 && !streaming && !needsKey
  const viewing = messages.find((message) => message.id === viewingId) ?? null

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
    let live = true
    getAssistantInfo(runnerUrl, runnerToken)
      .then((info) => {
        if (!live) return
        setAssistant(info)
        setRunnerDown(false)
      })
      .catch(() => {
        if (!live) return
        setAssistant(null)
        setRunnerDown(true)
      })
    return () => {
      live = false
    }
  }, [usingRunner, runnerUrl, runnerToken])

  useEffect(() => {
    const element = composerRef.current
    if (!element) return
    element.style.height = 'auto'
    element.style.height = `${Math.min(element.scrollHeight, COMPOSER_MAX_HEIGHT)}px`
  }, [input])

  useEffect(() => {
    const element = scrollRef.current
    if (!element || !stickRef.current) return
    element.scrollTop = element.scrollHeight
  }, [messages])

  /** Runs one assistant turn. `base` must end with the user message being answered. */
  async function deliver(text: string, turnIntent: AiIntent, base: AiMessage[]) {
    const assistantId = nanoid(8)
    const placeholder: AiMessage = {
      id: assistantId,
      role: 'assistant',
      content: '',
      intent: turnIntent,
      createdAt: Date.now(),
    }
    stickRef.current = true
    setMessages([...base, placeholder])
    setStreamingId(assistantId)

    const controller = new AbortController()
    abortRef.current = controller

    // Context off means nothing about the job leaves the browser: lint messages
    // and node params quote table paths, column names and raw SQL just as the
    // JSON does. No `readContext` at all reaches the same wire from the other
    // side - the front door has no job to share.
    const share = ai.shareJobContext && readContext !== undefined
    const context: AiPromptContext = share
      ? readContext()
      : { pipeline: null, issues: [], selectedNode: null }

    // The trailing user turn carries the raw text; the model gets the built prompt.
    const { turns: history, dropped } = budgetHistory(base.slice(0, -1))

    try {
      const response = await sendAiRequest({
        settings: ai,
        system: buildSystemPrompt(),
        messages: [
          ...history,
          { role: 'user', content: buildUserPrompt(turnIntent, text, context) },
        ],
        signal: controller.signal,
        runner: { baseUrl: runnerUrl, token: runnerToken },
        // Named as they are called: an answer that went and read the installed
        // formats is worth more than one that recalled them, and the prose alone
        // does not let the reader tell those two apart.
        onTool: (call) =>
          setMessages((previous) =>
            previous.map((message) =>
              message.id === assistantId
                ? { ...message, tools: [...(message.tools ?? []), call.name] }
                : message,
            ),
          ),
        onToken: (chunk) =>
          setMessages((previous) =>
            previous.map((message) =>
              message.id === assistantId
                ? { ...message, content: message.content + chunk }
                : message,
            ),
          ),
      })

      // The extractor never throws, and only proposal intents are scanned at all:
      // `pipeline` is null when the reply carried none.
      const found = extractProposalFor(turnIntent, response.text)
      const proposal =
        found.pipeline === null
          ? undefined
          : {
              pipeline: found.pipeline,
              summary: found.summary.trim() || 'A pipeline ready to apply to the canvas.',
              applied: false,
            }

      const note = [
        dropped > 0
          ? `${dropped} older ${dropped === 1 ? 'message' : 'messages'} left out of this request to stay within the size limit.`
          : '',
        !proposal && found.error && response.text.trim()
          ? `Nothing to apply: ${found.error}`
          : '',
      ]
        .filter(Boolean)
        .join(' ')
      if (note) setNotes((previous) => ({ ...previous, [assistantId]: note }))

      setMessages((previous) =>
        previous.map((message) =>
          message.id === assistantId
            ? {
                ...message,
                content: response.text || message.content,
                usage: response.usage,
                proposal,
              }
            : message,
        ),
      )
    } catch (error) {
      if (controller.signal.aborted) {
        // Keep whatever streamed in; drop the bubble when nothing arrived.
        setMessages((previous) =>
          previous.filter(
            (message) => message.id !== assistantId || message.content.trim().length > 0,
          ),
        )
        return
      }
      setMessages((previous) =>
        previous.map((message) =>
          message.id === assistantId ? { ...message, error: describeError(error) } : message,
        ),
      )
    } finally {
      abortRef.current = null
      setStreamingId(null)
    }
  }

  function send(raw: string, turnIntent: AiIntent) {
    const text = resolveText(raw, turnIntent, issueCount)
    if (!text || streaming || needsKey) return
    const userMessage: AiMessage = {
      id: nanoid(8),
      role: 'user',
      content: text,
      intent: turnIntent,
      createdAt: Date.now(),
    }
    setInput('')
    // Quick prompts unmount on send, so parking focus in the composer keeps the flow.
    composerRef.current?.focus()
    void deliver(text, turnIntent, [...messages, userMessage])
  }

  function retry(messageId: string) {
    if (streaming) return
    const index = messages.findIndex((message) => message.id === messageId)
    if (index < 0) return
    const base = messages.slice(0, index)
    const question = [...base].reverse().find((message) => message.role === 'user')
    if (!question) return
    void deliver(question.content, question.intent ?? 'chat', base)
  }

  function apply(message: AiMessage) {
    if (!message.proposal) return
    setViewingId(null)
    // No canvas to apply to: the same button, and the only honest thing it can
    // mean where no job is open is a job that does not exist yet.
    if (!onApply) {
      setCreating(message)
      return
    }
    onApply(message.proposal.pipeline)
    markApplied(message.id)
  }

  function markApplied(messageId: string) {
    setMessages((previous) =>
      previous.map((entry) =>
        entry.id === messageId && entry.proposal
          ? { ...entry, proposal: { ...entry.proposal, applied: true } }
          : entry,
      ),
    )
  }

  function onComposerKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key !== 'Enter' || event.shiftKey || event.nativeEvent.isComposing) return
    event.preventDefault()
    send(input, intent)
  }

  // Only reached with the toggle on, where the whole context travels; the issue
  // count is the number actually sent, not the number on the canvas.
  const sentIssues = Math.min(issueCount, MAX_CONTEXT_ISSUES)
  const contextParts = [
    'pipeline JSON',
    sentIssues ? `${sentIssues} ${sentIssues === 1 ? 'issue' : 'issues'}` : null,
  ].filter((part): part is string => part !== null)

  return (
    <section
      className={cn('flex h-full min-h-0 flex-col bg-surface', page && 'flex-1')}
      aria-label="AI assistant"
    >
      <header className="flex items-center gap-2 border-b border-line px-2.5 py-2">
        <span
          className="flex h-6 w-6 items-center justify-center rounded-lg bg-brand-500/15 text-brand-600 dark:text-brand-400"
          aria-hidden
        >
          <Sparkles className="h-3.5 w-3.5" />
        </span>
        <div className="min-w-0 flex-1">
          <p className="truncate text-sm font-medium text-content">Assistant</p>
          <p className="truncate text-2xs text-content-subtle">
            {provider.label} · {ai.model || assistant?.model || 'no model set'}
          </p>
        </div>
        <IconButton
          label="New chat"
          size="sm"
          disabled={messages.length === 0 || streaming}
          onClick={() => {
            setMessages([])
            setNotes({})
            setViewingId(null)
          }}
        >
          <Plus />
        </IconButton>
        <IconButton label="AI settings" size="sm" onClick={() => navigate('/settings')}>
          <Settings2 />
        </IconButton>
      </header>

      <div
        ref={scrollRef}
        role="log"
        aria-label="Conversation"
        className="scroll-area flex-1 space-y-3 p-2.5"
        onScroll={(event) => {
          const element = event.currentTarget
          const distance = element.scrollHeight - element.scrollTop - element.clientHeight
          stickRef.current = distance < 80
        }}
      >
        {needsKey && <SetupCard onOpenSettings={() => navigate('/settings')} />}

        {usingRunner && runnerDown && (
          <Notice
            title="The runner is not answering"
            detail={`Nothing at ${runnerUrl}. Start it, or point the Studio somewhere else in Settings.`}
            onOpenSettings={() => navigate('/settings')}
          />
        )}

        {usingRunner && !runnerDown && assistant && !assistant.available && (
          <Notice
            title="The runner has no model to answer with"
            detail={assistant.error || 'The assistant backend is off.'}
            hint={assistant.hint}
            onOpenSettings={() => navigate('/settings')}
          />
        )}

        {messages.length === 0 ? (
          <div className={cn('animate-fade-in', page && 'mx-auto w-full max-w-2xl')}>
            {welcome ? (
              welcome((prompt) => send(prompt, intent))
            ) : (
              <>
                <EmptyState
                  className="px-0 py-6"
                  icon={<Sparkles />}
                  title="Describe the pipeline"
                  description={
                    readContext
                      ? 'The assistant reads your canvas and answers with a pipeline you can apply in one click.'
                      : 'Describe what you need and the assistant answers with a pipeline you can turn into a Job in one click.'
                  }
                />
                <div className="space-y-1.5">
                  {QUICK_PROMPTS.map((prompt) => (
                    <button
                      key={prompt.text}
                      type="button"
                      disabled={needsKey}
                      onClick={() => {
                        setIntent(prompt.intent)
                        send(prompt.text, prompt.intent)
                      }}
                      className={cn(
                        'flex w-full items-start gap-2 rounded-lg border border-line bg-surface-raised',
                        'px-2.5 py-2 text-left text-xs leading-relaxed text-content-muted transition-colors',
                        'hover:border-line-strong hover:bg-surface-sunken hover:text-content',
                        'disabled:pointer-events-none disabled:opacity-50',
                      )}
                    >
                      <prompt.icon
                        className="mt-0.5 h-3.5 w-3.5 shrink-0 text-brand-500"
                        aria-hidden
                      />
                      <span>{prompt.text}</span>
                    </button>
                  ))}
                </div>
              </>
            )}
          </div>
        ) : (
          <div className={cn('space-y-3', page && 'mx-auto w-full max-w-2xl')}>
            {messages.map((message) => (
              <MessageRow
                key={message.id}
                message={message}
                note={notes[message.id]}
                streaming={streamingId === message.id}
                applyLabel={onApply ? 'Apply to canvas' : 'Create a Job'}
                appliedLabel={onApply ? 'Applied' : 'Job created'}
                onApply={() => apply(message)}
                onView={() => setViewingId(message.id)}
                onRetry={() => retry(message.id)}
              />
            ))}
          </div>
        )}
      </div>

      <div
        className={cn(
          'w-full space-y-2 border-t border-line px-2.5 py-2.5',
          page && 'mx-auto max-w-2xl',
        )}
      >
        <div className="flex flex-wrap gap-1">
          {INTENTS.map((option) => (
            <button
              key={option.id}
              type="button"
              aria-pressed={option.id === intent}
              onClick={() => setIntent(option.id)}
              className={cn(
                'inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-2xs font-medium transition-colors',
                option.id === intent
                  ? 'border-brand-500/40 bg-brand-500/15 text-brand-600 dark:text-brand-400'
                  : 'border-line bg-surface-sunken text-content-muted hover:text-content',
              )}
            >
              <option.icon className="h-3 w-3" aria-hidden />
              {option.label}
            </button>
          ))}
        </div>

        <div className="relative">
          <Textarea
            ref={composerRef}
            rows={1}
            value={input}
            onChange={(event) => setInput(event.target.value)}
            onKeyDown={onComposerKeyDown}
            placeholder={active.placeholder}
            aria-label="Message the assistant"
            className="min-h-[3.5rem] resize-none py-2 pr-11 text-xs"
          />
          <div className="absolute bottom-2 right-2">
            {streaming ? (
              <IconButton
                label="Stop generating"
                size="sm"
                variant="secondary"
                onClick={() => abortRef.current?.abort()}
              >
                <Square />
              </IconButton>
            ) : (
              <IconButton
                label="Send message"
                size="sm"
                variant="primary"
                disabled={!canSend}
                onClick={() => send(input, intent)}
              >
                <Send />
              </IconButton>
            )}
          </div>
        </div>

        <div className="flex items-center justify-between gap-2">
          {readContext ? (
            <button
              type="button"
              aria-pressed={ai.shareJobContext}
              title="Toggle job context"
              onClick={() => setAi({ shareJobContext: !ai.shareJobContext })}
              className={cn(
                'inline-flex min-w-0 items-center gap-1 rounded-md px-1 py-0.5 text-2xs transition-colors',
                ai.shareJobContext
                  ? 'text-content-muted hover:text-content'
                  : 'text-content-subtle hover:text-content-muted',
              )}
            >
              <Paperclip className="h-3 w-3 shrink-0" aria-hidden />
              <span className="truncate">
                {ai.shareJobContext ? contextParts.join(' · ') : 'Context off'}
              </span>
            </button>
          ) : (
            // Not a toggle here: there is no job open, so there is nothing the
            // switch could send either way. Saying so beats a dead control.
            <span className="inline-flex min-w-0 items-center gap-1 px-1 py-0.5 text-2xs text-content-subtle">
              <Paperclip className="h-3 w-3 shrink-0" aria-hidden />
              <span className="truncate">No job open</span>
            </span>
          )}
          <p className="shrink-0 text-2xs text-content-subtle">
            <Kbd>Enter</Kbd> send · <Kbd>⇧↵</Kbd> newline
          </p>
        </div>
      </div>

      {viewing?.proposal && (
        <Modal
          open
          onOpenChange={(open) => {
            if (!open) setViewingId(null)
          }}
          title="Proposed pipeline"
          description={
            onApply
              ? 'Read-only preview. Applying it rebuilds the canvas from this JSON.'
              : 'Read-only preview. Accepting it creates a Job built from this JSON.'
          }
          size="lg"
          footer={
            <>
              <Button variant="ghost" onClick={() => setViewingId(null)}>
                Close
              </Button>
              <Button
                variant="primary"
                disabled={viewing.proposal.applied}
                onClick={() => apply(viewing)}
              >
                {onApply ? 'Apply to canvas' : 'Create a Job'}
              </Button>
            </>
          }
        >
          <pre className="scroll-area max-h-[60vh] overflow-x-auto rounded-lg border border-line bg-surface-sunken p-3 font-mono text-2xs leading-relaxed text-content">
            {safeJson(viewing.proposal.pipeline)}
          </pre>
        </Modal>
      )}

      {creating?.proposal && (
        <CreateJobFromProposal
          pipeline={creating.proposal.pipeline}
          summary={creating.proposal.summary}
          onClose={() => setCreating(null)}
          onCreated={() => {
            markApplied(creating.id)
            setCreating(null)
          }}
        />
      )}
    </section>
  )
}

/** Something is wrong with the backend, said where the answer would have been. */
function Notice({
  title,
  detail,
  hint,
  onOpenSettings,
}: {
  title: string
  detail: string
  hint?: string
  onOpenSettings: () => void
}) {
  return (
    <div className="animate-fade-in rounded-xl border border-state-warn/40 bg-state-warn/10 p-3">
      <p className="flex items-start gap-1.5 text-xs font-medium text-content">
        <ServerCog className="mt-0.5 h-3.5 w-3.5 shrink-0 text-state-warn" aria-hidden />
        {title}
      </p>
      <p className="mt-1 text-2xs leading-relaxed text-content-muted">{detail}</p>
      {hint && (
        <pre className="mt-1.5 overflow-x-auto rounded-md bg-surface-sunken px-2 py-1 font-mono text-2xs text-content-muted">
          {hint}
        </pre>
      )}
      <Button size="xs" variant="secondary" className="mt-2" onClick={onOpenSettings}>
        Open settings
      </Button>
    </div>
  )
}

/* ---------------------------------------------------------------- messages */

interface MessageRowProps {
  message: AiMessage
  /** Non-fatal remark about the turn: trimmed history, unusable proposal JSON. */
  note?: string
  streaming: boolean
  /** What accepting the proposal does here: rebuild the canvas, or make a Job. */
  applyLabel: string
  appliedLabel: string
  onApply: () => void
  onView: () => void
  onRetry: () => void
}

function MessageRow({
  message,
  note,
  streaming,
  applyLabel,
  appliedLabel,
  onApply,
  onView,
  onRetry,
}: MessageRowProps) {
  const isUser = message.role === 'user'
  const failed = Boolean(message.error)

  return (
    <div className={cn('flex flex-col gap-1 animate-slide-up', isUser && 'items-end')}>
      <div className="flex items-center gap-1.5 px-0.5 text-2xs text-content-subtle">
        <span>{isUser ? 'You' : 'Assistant'}</span>
        {message.intent && message.intent !== 'chat' && (
          <span className="chip py-0">{intentLabel(message.intent)}</span>
        )}
        {streaming ? (
          <Spinner className="h-3 w-3" />
        ) : (
          <span>{formatTime(message.createdAt)}</span>
        )}
      </div>

      {message.tools && message.tools.length > 0 && (
        // What it went and read, rather than recalled. Named because "checked
        // the installed formats" and "wrote a plausible list" read the same.
        <div className="flex flex-wrap items-center gap-1 px-0.5">
          {message.tools.map((tool, index) => (
            <span
              key={`${tool}-${index}`}
              className="inline-flex items-center gap-1 rounded-full border border-line bg-surface-sunken px-1.5 py-0.5 font-mono text-2xs text-content-subtle"
            >
              <Wrench className="h-2.5 w-2.5" aria-hidden />
              {tool}
            </span>
          ))}
        </div>
      )}

      <div
        className={cn(
          'rounded-xl border px-3 py-2 text-xs leading-relaxed text-content',
          isUser
            ? 'max-w-[92%] border-brand-500/25 bg-brand-500/10'
            : 'w-full border-line bg-surface-raised',
          failed && 'border-state-danger/40 bg-state-danger/10',
        )}
      >
        {failed ? (
          <div className="space-y-2">
            <p className="flex items-start gap-1.5 text-content">
              <AlertTriangle
                className="mt-0.5 h-3.5 w-3.5 shrink-0 text-state-danger"
                aria-hidden
              />
              <span className="min-w-0 break-words">{message.error}</span>
            </p>
            <Button
              size="xs"
              variant="secondary"
              icon={<RefreshCw className="h-3 w-3" />}
              onClick={onRetry}
            >
              Retry
            </Button>
          </div>
        ) : streaming && !message.content ? (
          <p className="text-content-subtle">Thinking…</p>
        ) : (
          <MessageBody content={message.content} hideJson={Boolean(message.proposal)} />
        )}
      </div>

      {note && !failed && (
        <p className="px-0.5 text-2xs leading-relaxed text-content-subtle">{note}</p>
      )}

      {message.proposal && (
        <div className="w-full">
          <ProposalCard
            proposal={message.proposal}
            applied={message.proposal.applied}
            applyLabel={applyLabel}
            appliedLabel={appliedLabel}
            onApply={onApply}
            onView={onView}
          />
        </div>
      )}
    </div>
  )
}

/** Paragraphs, inline `code` and fenced blocks — the subset models actually emit. */
function MessageBody({ content, hideJson }: { content: string; hideJson: boolean }) {
  const blocks = parseBlocks(content)
  // The JSON that became a proposal is already summarized by the card below.
  const visible = hideJson ? blocks.filter((block) => !isPipelineBlock(block)) : blocks
  const hasText = visible.some((block) =>
    block.kind === 'code' ? true : block.text.trim().length > 0,
  )

  if (!hasText) {
    return (
      <p className="text-content-muted">
        {hideJson
          ? 'Proposed a pipeline — review it below.'
          : 'The provider returned an empty reply.'}
      </p>
    )
  }

  return (
    <div className="space-y-2">
      {visible.map((block, index) =>
        block.kind === 'code' ? (
          <CodeBlock key={index} code={block.code} lang={block.lang} />
        ) : (
          block.text
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

function CodeBlock({ code, lang }: { code: string; lang: string }) {
  const [copied, setCopied] = useState(false)

  async function copy() {
    try {
      await navigator.clipboard.writeText(code)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1600)
    } catch {
      toast.error('Clipboard is not available')
    }
  }

  return (
    <div className="overflow-hidden rounded-lg border border-line bg-surface-sunken">
      <div className="flex items-center justify-between gap-2 border-b border-line px-2 py-1">
        <span className="truncate font-mono text-2xs text-content-subtle">
          {lang || 'code'}
        </span>
        <IconButton label="Copy code" size="xs" onClick={() => void copy()}>
          {copied ? <Check /> : <Copy />}
        </IconButton>
      </div>
      <pre className="scroll-area max-h-64 overflow-x-auto px-2 py-1.5 font-mono text-2xs leading-relaxed text-content">
        {code.replace(/\n+$/, '')}
      </pre>
    </div>
  )
}

/* ------------------------------------------------------------------- setup */

function SetupCard({ onOpenSettings }: { onOpenSettings: () => void }) {
  const providers = Object.values(AI_PROVIDER_INFO)

  return (
    <div className="rounded-xl border border-brand-500/30 bg-surface-raised p-3 shadow-card">
      <div className="flex items-start gap-2">
        <span
          className="mt-0.5 flex h-6 w-6 shrink-0 items-center justify-center rounded-lg bg-brand-500/15 text-brand-600 dark:text-brand-400"
          aria-hidden
        >
          <KeyRound className="h-3.5 w-3.5" />
        </span>
        <div className="min-w-0 space-y-1">
          <p className="text-xs font-medium text-content">Connect a model to start</p>
          <p className="text-2xs leading-relaxed text-content-muted">
            Studio calls the provider straight from this browser. Your key stays on this device
            — it is never sent to a Sparquet server.
          </p>
        </div>
      </div>

      <div className="mt-2.5 flex flex-wrap gap-1">
        {providers.map((info) => (
          <Badge key={info.id} tone="neutral">
            {info.label}
          </Badge>
        ))}
      </div>

      <Button
        className="mt-2.5"
        size="xs"
        variant="primary"
        fullWidth
        icon={<Settings2 className="h-3 w-3" />}
        onClick={onOpenSettings}
      >
        Add an API key in Settings
      </Button>
    </div>
  )
}

/* ------------------------------------------------------------------ helpers */

type Block = { kind: 'text'; text: string } | { kind: 'code'; lang: string; code: string }

function parseBlocks(content: string): Block[] {
  const blocks: Block[] = []
  // The closing fence is optional so a half-streamed block still renders.
  const fence = /```([\w-]*)[ \t]*\n?([\s\S]*?)(?:```|$)/g
  let cursor = 0
  let match = fence.exec(content)

  while (match) {
    if (match.index > cursor) {
      blocks.push({ kind: 'text', text: content.slice(cursor, match.index) })
    }
    blocks.push({ kind: 'code', lang: match[1] ?? '', code: match[2] ?? '' })
    cursor = fence.lastIndex
    match = fence.exec(content)
  }

  if (cursor < content.length) blocks.push({ kind: 'text', text: content.slice(cursor) })
  return blocks
}

/** A fenced block holding the proposal: tagged json, or an untagged JSON object. */
function isPipelineBlock(block: Block): boolean {
  if (block.kind !== 'code') return false
  const lang = block.lang.toLowerCase()
  if (lang === 'json' || lang === 'jsonc') return true
  return lang === '' && block.code.trim().startsWith('{')
}

interface BudgetedHistory {
  turns: { role: AiMessage['role']; content: string }[]
  /** Turns left out because the transcript no longer fits the budget. */
  dropped: number
}

/** Keeps the most recent turns that fit `HISTORY_BUDGET`, oldest dropped first. */
function budgetHistory(messages: AiMessage[]): BudgetedHistory {
  const usable = messages.filter(
    (message) => !message.error && message.content.trim().length > 0,
  )
  const turns: BudgetedHistory['turns'] = []
  let used = 0
  for (let index = usable.length - 1; index >= 0; index -= 1) {
    const message = usable[index]
    used += message.content.length
    if (used > HISTORY_BUDGET) break
    turns.unshift({ role: message.role, content: message.content })
  }
  return { turns, dropped: usable.length - turns.length }
}

function resolveText(raw: string, intent: AiIntent, issueCount: number): string {
  const text = raw.trim()
  if (text) return text
  // The "Fix issues" chip is actionable on its own once the linter has something.
  if (intent === 'fix' && issueCount > 0) return 'Fix the issues reported for this pipeline.'
  return ''
}

function intentLabel(intent: AiIntent): string {
  return INTENTS.find((option) => option.id === intent)?.label ?? intent
}

function describeError(error: unknown): string {
  if (error instanceof Error && error.message.trim()) return error.message
  return 'The provider did not return a response.'
}

function formatTime(timestamp: number): string {
  return new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return String(value)
  }
}
