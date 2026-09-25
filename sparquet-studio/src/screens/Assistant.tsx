/**
 * The front door of the Studio: a conversation.
 *
 * Everything else here starts from something you already have — a canvas, a
 * workflow, a directory of confs. That is the wrong first screen for the person
 * who arrives knowing what they want the data to do and not which of eleven
 * transformations does it. A question is the cheapest way in, so `/` asks for
 * one.
 *
 * It used to be a second, smaller assistant: its own transcript, its own
 * composer, no intents and no proposal to apply, so the same question answered
 * here and in the canvas panel came back as two different kinds of thing. It is
 * now the same `AiConversation` the panel renders, and the only difference is
 * the one the situation forces: there is no job open, so nothing about a
 * pipeline is sent, and accepting a proposal creates a Job instead of rebuilding
 * a canvas that is not there.
 *
 * The provider, model and key are the same ones Settings already holds, so this
 * screen adds no second place to configure anything and no second bill.
 */

import { FileJson, GitMerge, Radio, ShieldCheck, type LucideIcon } from 'lucide-react'

import logoMark from '@/assets/logo.png'
import { AiConversation } from '@/components/ai/AiConversation'
import { PageShell } from '@/components/layout/PageShell'

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

export function Assistant() {
  return (
    // No PageHeader: the conversation brings its own, and two titles saying
    // "Assistant" on one screen is the seam showing.
    <PageShell width="default" className="flex min-h-full flex-col">
      <div className="flex min-h-0 flex-1 overflow-hidden rounded-xl border border-line">
        <AiConversation layout="page" welcome={(ask) => <Welcome onPick={ask} />} />
      </div>
    </PageShell>
  )
}

export default Assistant

/* ------------------------------------------------------------------ welcome */

/**
 * What fills the screen before anyone has typed. A blank page and a text box is
 * the honest layout and the least inviting one: it asks a question without
 * saying what kind of answer it is any good at. The mark, one sentence and four
 * openings do that in the space the transcript will take over anyway.
 */
function Welcome({ onPick }: { onPick: (prompt: string) => void }) {
  return (
    <div className="flex flex-col items-center gap-6 py-6 text-center">
      <div className="relative">
        {/* A soft glow behind the mark, so it reads as a greeting rather than as
            a logo someone forgot to put in a corner. */}
        <div
          className="absolute inset-0 -z-10 scale-[2.2] rounded-full bg-brand-500/15 blur-2xl dark:bg-brand-400/15"
          aria-hidden
        />
        <img
          src={logoMark}
          alt=""
          width={72}
          height={72}
          className="h-[72px] w-[72px] drop-shadow-sm"
        />
      </div>

      <div className="space-y-1.5">
        <h1 className="text-lg font-semibold text-content">What should the data do?</h1>
        <p className="mx-auto max-w-md text-xs leading-relaxed text-content-muted">
          Describe it in your own words and I will answer in Sparquet&apos;s — the pipeline
          JSON, the format that reads it, the transformation that shapes it. Ask for a pipeline
          and you can turn the answer into a Job in one click.
        </p>
      </div>

      <div className="grid w-full max-w-2xl gap-2 sm:grid-cols-2">
        {SUGGESTIONS.map((suggestion) => (
          <button
            key={suggestion.prompt}
            type="button"
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
