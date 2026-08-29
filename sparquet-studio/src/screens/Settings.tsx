/**
 * Settings — appearance, AI assistant, local runner, data and about.
 *
 * Every value on this screen is local to the browser: preferences live in the
 * settings store, the library lives in IndexedDB. The two places data can leave
 * the page are the AI provider configured below and the runner the user starts.
 */

import * as RadixSlider from '@radix-ui/react-slider'
import {
  AlertTriangle,
  Bot,
  CircleCheck,
  Copy,
  Coins,
  Database,
  Download,
  ExternalLink,
  Eye,
  EyeOff,
  Info,
  KeyRound,
  Moon,
  Palette,
  Plug,
  ShieldAlert,
  ShieldCheck,
  Sun,
  Terminal,
  Trash2,
  Upload,
  type LucideIcon,
} from 'lucide-react'
import { useEffect, useId, useRef, useState, type ChangeEvent, type ReactNode } from 'react'
import { toast } from 'sonner'

import {
  Badge,
  Button,
  Field,
  IconButton,
  Input,
  Modal,
  Segmented,
  Select,
  Spinner,
  Toggle,
} from '@/components/ui'
import { sendAiRequest } from '@/lib/ai/client'
import { AccessPanel } from '@/components/auth/AccessPanel'
import { RolesPanel } from '@/components/auth/RolesPanel'
import { TeamsPanel } from '@/components/auth/TeamsPanel'
import { CreditsPanel } from '@/components/credits/CreditsPanel'
import { AI_PROVIDER_INFO } from '@/lib/ai/providers'
import {
  checkRunnerHealth,
  RUNNER_INSTALL_COMMAND,
  RUNNER_START_COMMAND,
} from '@/lib/runner/client'
import { clearAll, exportAll, importAll } from '@/lib/storage/db'
import { cn } from '@/lib/utils/cn'
import { copyText, downloadText } from '@/lib/utils/download'
import { useLibraryStore } from '@/store/library'
import { useSettingsStore, type CanvasPreferences, type Theme } from '@/store/settings'
import type { AiProviderId } from '@/types/ai'

/** Matches the `version` field of package.json. */
const STUDIO_VERSION = '0.1.0'
const REPO_URL = 'https://github.com/VictorPasqualini/sparquet'
const DOCS_URL = 'https://github.com/VictorPasqualini/sparquet/tree/main/docs'

const CUSTOM_MODEL = '__custom__'
const RESET_PHRASE = 'RESET'
const PROBE_TIMEOUT_MS = 20_000

const INSTALL_COMMAND = RUNNER_INSTALL_COMMAND
const START_COMMAND = RUNNER_START_COMMAND

type SectionId = 'appearance' | 'ai' | 'runner' | 'access' | 'billing' | 'data' | 'about'

interface SectionMeta {
  id: SectionId
  label: string
  title: string
  description: string
  icon: LucideIcon
}

const SECTIONS: SectionMeta[] = [
  {
    id: 'appearance',
    label: 'Appearance',
    title: 'Appearance',
    description: 'Theme and the canvas defaults applied to every job you open.',
    icon: Palette,
  },
  {
    id: 'ai',
    label: 'AI assistant',
    title: 'AI assistant',
    description: 'Bring your own key. Studio calls the provider straight from this browser.',
    icon: Bot,
  },
  {
    id: 'runner',
    label: 'Local runner',
    title: 'Local runner',
    description: 'Optional bridge that executes pipelines against Spark on your machine.',
    icon: Terminal,
  },
  {
    id: 'access',
    label: 'Access & IAM',
    title: 'Access & IAM',
    description:
      'Who can sign in to this runner, which team they belong to, and what each role permits.',
    icon: ShieldCheck,
  },
  {
    id: 'billing',
    label: 'Billing',
    title: 'Billing',
    description: 'Execution credits: what this team has, what it has spent, and on what.',
    icon: Coins,
  },
  {
    id: 'data',
    label: 'Data',
    title: 'Data',
    description: 'Your workflows and jobs, stored in this browser only.',
    icon: Database,
  },
  {
    id: 'about',
    label: 'About',
    title: 'About',
    description: 'Version, links and how Studio keeps your work.',
    icon: Info,
  },
]

/** By id, so adding a section never shifts what another one renders. */
const SECTION = Object.fromEntries(SECTIONS.map((meta) => [meta.id, meta])) as Record<
  SectionId,
  SectionMeta
>

const CANVAS_PREFERENCES: {
  key: keyof CanvasPreferences
  label: string
  description: string
}[] = [
  {
    key: 'snapToGrid',
    label: 'Snap to grid',
    description: 'Dropped nodes align to the grid instead of landing a few pixels off.',
  },
  {
    key: 'showGrid',
    label: 'Show grid',
    description: 'Draws the dotted background that makes alignment visible.',
  },
  {
    key: 'showMinimap',
    label: 'Show minimap',
    description: 'Keeps a map of the whole graph in the corner of the canvas.',
  },
  {
    key: 'animateEdges',
    label: 'Animate edges',
    description: 'Dashes travel along connections. Turn it off on very large graphs.',
  },
  {
    key: 'liveLint',
    label: 'Live linting',
    description: 'Re-checks the job while you edit, not only when you run it.',
  },
]

const THEME_OPTIONS = [
  { value: 'dark' as Theme, label: <ThemeLabel icon={<Moon />} text="Dark" /> },
  { value: 'light' as Theme, label: <ThemeLabel icon={<Sun />} text="Light" /> },
]

type Probe<T> =
  | { status: 'idle' }
  | { status: 'busy' }
  | { status: 'ok'; value: T }
  | { status: 'error'; message: string }

type RunnerHealthValue = Awaited<ReturnType<typeof checkRunnerHealth>>

const IDLE = { status: 'idle' } as const

/* ------------------------------------------------------------------ screen */

export function Settings() {
  const active = useActiveSection()

  return (
    <div className="mx-auto w-full max-w-5xl px-6 py-8 animate-fade-in">
      <header className="space-y-1">
        <h1 className="text-lg font-semibold text-content">Settings</h1>
        <p className="text-xs text-content-muted">
          Preferences, keys and data for this browser. Nothing here syncs to a server.
        </p>
      </header>

      <div className="mt-8 flex items-start gap-10">
        <SectionNav active={active} />

        <div className="min-w-0 flex-1 space-y-10">
          <AppearanceSection />
          <AiSection />
          <RunnerSection />
          <AccessSection />
          <BillingSection />
          <DataSection />
          <AboutSection />
        </div>
      </div>
    </div>
  )
}

/* --------------------------------------------------------------- sub-nav */

const domId = (id: SectionId): string => `settings-${id}`

/** Highlights the section nearest the top of the scroll viewport. */
function useActiveSection(): SectionId {
  const [active, setActive] = useState<SectionId>('appearance')

  useEffect(() => {
    const visible = new Set<string>()
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) visible.add(entry.target.id)
          else visible.delete(entry.target.id)
        }
        const first = SECTIONS.find((section) => visible.has(domId(section.id)))
        if (first) setActive(first.id)
      },
      // Reading band: just under any app header, down to 45% of the viewport.
      { rootMargin: '-96px 0px -55% 0px' },
    )

    for (const section of SECTIONS) {
      const element = document.getElementById(domId(section.id))
      if (element) observer.observe(element)
    }
    return () => observer.disconnect()
  }, [])

  return active
}

function SectionNav({ active }: { active: SectionId }) {
  return (
    <nav aria-label="Settings sections" className="sticky top-6 hidden w-44 shrink-0 lg:block">
      <ul className="space-y-0.5">
        {SECTIONS.map((section) => {
          const Icon = section.icon
          const current = section.id === active
          return (
            <li key={section.id}>
              <button
                type="button"
                aria-current={current ? 'true' : undefined}
                onClick={() =>
                  document
                    .getElementById(domId(section.id))
                    ?.scrollIntoView({ behavior: 'smooth', block: 'start' })
                }
                className={cn(
                  'flex w-full items-center gap-2 rounded-lg px-2.5 py-1.5 text-xs transition-colors',
                  current
                    ? 'bg-brand-500/10 font-medium text-brand-600 dark:text-brand-400'
                    : 'text-content-muted hover:bg-surface-sunken hover:text-content',
                )}
              >
                <Icon className="h-3.5 w-3.5" aria-hidden />
                {section.label}
              </button>
            </li>
          )
        })}
      </ul>
    </nav>
  )
}

function Section({ meta, children }: { meta: SectionMeta; children: ReactNode }) {
  const Icon = meta.icon
  return (
    <section
      id={domId(meta.id)}
      aria-labelledby={`${domId(meta.id)}-title`}
      className="scroll-mt-24"
    >
      <div className="flex items-start gap-3">
        <span className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-line bg-surface-sunken text-content-muted">
          <Icon className="h-4 w-4" aria-hidden />
        </span>
        <div className="space-y-0.5">
          <h2 id={`${domId(meta.id)}-title`} className="text-sm font-semibold text-content">
            {meta.title}
          </h2>
          <p className="text-xs leading-relaxed text-content-muted">{meta.description}</p>
        </div>
      </div>
      <div className="card mt-4 space-y-5 p-5">{children}</div>
    </section>
  )
}

function ThemeLabel({ icon, text }: { icon: ReactNode; text: string }) {
  return (
    <span className="flex items-center gap-1.5 [&_svg]:h-3.5 [&_svg]:w-3.5">
      {icon}
      {text}
    </span>
  )
}

/* ------------------------------------------------------------- appearance */

function AppearanceSection() {
  const theme = useSettingsStore((state) => state.theme)
  const setTheme = useSettingsStore((state) => state.setTheme)
  const canvas = useSettingsStore((state) => state.canvas)
  const setCanvas = useSettingsStore((state) => state.setCanvas)

  return (
    <Section meta={SECTION.appearance}>
      <Field label="Theme" help="Follows your system until you pick one. The choice is remembered on this device.">
        <Segmented value={theme} onChange={setTheme} options={THEME_OPTIONS} />
      </Field>

      <div>
        <p className="text-xs font-medium text-content-muted">Canvas</p>
        <div className="mt-1 divide-y divide-line">
          {CANVAS_PREFERENCES.map((preference) => (
            <div key={preference.key} className="py-3 first:pt-2 last:pb-0">
              <Toggle
                checked={canvas[preference.key]}
                onCheckedChange={(value) => setCanvas({ ...canvas, [preference.key]: value })}
                label={preference.label}
                description={preference.description}
              />
            </div>
          ))}
        </div>
      </div>
    </Section>
  )
}

/* ---------------------------------------------------------- ai assistant */

function AiSection() {
  const ai = useSettingsStore((state) => state.ai)
  const setAi = useSettingsStore((state) => state.setAi)
  const persistApiKey = useSettingsStore((state) => state.persistApiKey)
  const setPersistApiKey = useSettingsStore((state) => state.setPersistApiKey)

  const ids = {
    provider: useId(),
    model: useId(),
    customModel: useId(),
    baseUrl: useId(),
    apiKey: useId(),
    maxTokens: useId(),
  }

  const [showKey, setShowKey] = useState(false)
  const [customModel, setCustomModel] = useState(false)
  const [probe, setProbe] = useState<Probe<string>>(IDLE)
  const abortRef = useRef<AbortController | null>(null)

  useEffect(() => () => abortRef.current?.abort(), [])

  const info = AI_PROVIDER_INFO[ai.provider]
  const providers = Object.values(AI_PROVIDER_INFO)
  const knownModel = info.models.some((model) => model.id === ai.model)
  const usingCustomModel = customModel || !knownModel

  function changeProvider(next: string) {
    const target = AI_PROVIDER_INFO[next as AiProviderId]
    if (!target) return
    setCustomModel(target.models.length === 0)
    setProbe(IDLE)
    setAi({
      provider: target.id,
      model: target.defaultModel,
      baseUrl: target.defaultBaseUrl,
    })
  }

  function changeModel(next: string) {
    if (next === CUSTOM_MODEL) {
      setCustomModel(true)
      return
    }
    setCustomModel(false)
    setAi({ model: next })
  }

  async function testConnection() {
    abortRef.current?.abort()
    const controller = new AbortController()
    abortRef.current = controller
    const timer = window.setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS)
    setProbe({ status: 'busy' })

    try {
      // One token is enough to prove the key, the model id and the endpoint.
      const response = await sendAiRequest({
        settings: { ...ai, maxTokens: 1 },
        system: 'Reply with the single word OK.',
        messages: [{ role: 'user', content: 'ping' }],
        signal: controller.signal,
      })
      // A newer probe superseded this one; its result is the one that counts.
      if (abortRef.current !== controller) return
      const used = response.usage?.inputTokens
      setProbe({
        status: 'ok',
        value: `${info.label} answered as ${ai.model || info.defaultModel}${
          used ? ` (${used} prompt tokens billed)` : ''
        }.`,
      })
    } catch (error) {
      if (abortRef.current !== controller) return
      setProbe({
        status: 'error',
        message: controller.signal.aborted
          ? `No answer within ${PROBE_TIMEOUT_MS / 1000} seconds. Check the base URL and your network.`
          : messageOf(error),
      })
    } finally {
      window.clearTimeout(timer)
    }
  }

  const modelOptions = [
    ...info.models.map((model) => ({ value: model.id, label: model.label, hint: model.hint })),
    {
      value: CUSTOM_MODEL,
      label: 'Custom model',
      hint: 'Type any model id the provider accepts.',
    },
  ]

  return (
    <Section meta={SECTION.ai}>
      <div className="grid gap-5 sm:grid-cols-2">
        <Field label="Provider" htmlFor={ids.provider} help={info.docsNote}>
          <Select
            id={ids.provider}
            value={ai.provider}
            onValueChange={changeProvider}
            options={providers.map((provider) => ({
              value: provider.id,
              label: provider.label,
            }))}
          />
        </Field>

        <Field
          label="Model"
          htmlFor={ids.model}
          help={
            usingCustomModel
              ? 'Model ids are free text — anything the endpoint serves works.'
              : undefined
          }
        >
          <Select
            id={ids.model}
            value={usingCustomModel ? CUSTOM_MODEL : ai.model}
            onValueChange={changeModel}
            options={modelOptions}
            placeholder="Choose a model"
          />
        </Field>
      </div>

      {usingCustomModel && (
        <Field label="Model id" htmlFor={ids.customModel}>
          <Input
            id={ids.customModel}
            mono
            spellCheck={false}
            value={ai.model}
            placeholder={info.defaultModel || 'llama3.1:8b'}
            onChange={(event) => setAi({ model: event.target.value })}
          />
        </Field>
      )}

      <Field
        label="Base URL"
        htmlFor={ids.baseUrl}
        help="Point this at a gateway or a self-hosted endpoint to route requests elsewhere."
      >
        <Input
          id={ids.baseUrl}
          mono
          spellCheck={false}
          value={ai.baseUrl}
          placeholder={info.defaultBaseUrl || 'http://localhost:11434/v1'}
          onChange={(event) => setAi({ baseUrl: event.target.value })}
        />
      </Field>

      <Field
        label="API key"
        htmlFor={ids.apiKey}
        help={
          info.requiresKey
            ? 'Sent as a header on each request and nowhere else.'
            : 'Optional — leave empty for endpoints that do not authenticate.'
        }
        action={
          info.keyUrl ? (
            <a
              href={info.keyUrl}
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-1 text-2xs text-content-subtle transition-colors hover:text-brand-500"
            >
              <KeyRound className="h-3 w-3" aria-hidden />
              Get a key
            </a>
          ) : undefined
        }
      >
        <div className="relative">
          <Input
            id={ids.apiKey}
            mono
            className="pr-10"
            type={showKey ? 'text' : 'password'}
            autoComplete="off"
            spellCheck={false}
            value={ai.apiKey}
            placeholder={info.requiresKey ? 'sk-…' : 'none'}
            onChange={(event) => setAi({ apiKey: event.target.value })}
          />
          <IconButton
            size="sm"
            label={showKey ? 'Hide API key' : 'Show API key'}
            onClick={() => setShowKey((previous) => !previous)}
            className="absolute right-1 top-1/2 -translate-y-1/2"
          >
            {showKey ? <EyeOff /> : <Eye />}
          </IconButton>
        </div>
      </Field>

      <div className="rounded-lg border border-line bg-surface-sunken p-3">
        <Toggle
          checked={persistApiKey}
          onCheckedChange={setPersistApiKey}
          label="Remember key in this browser"
          description="Off keeps the key in memory only — you retype it after a reload. On writes it to this browser's local storage, where any script on the page could read it. Leave it off on shared machines."
        />
      </div>

      <div className="grid gap-5 sm:grid-cols-2">
        <Field
          label="Temperature"
          help="Low values keep the generated JSON predictable."
          action={
            <span className="font-mono text-2xs text-content-muted">
              {ai.temperature.toFixed(2)}
            </span>
          }
        >
          <SliderControl
            label="Temperature"
            value={ai.temperature}
            min={0}
            max={1}
            step={0.05}
            onChange={(value) => setAi({ temperature: value })}
          />
        </Field>

        <Field
          label="Max tokens"
          htmlFor={ids.maxTokens}
          help="Upper bound on a single reply. Pipelines with many nodes need room."
        >
          <Input
            id={ids.maxTokens}
            type="number"
            min={1}
            max={200000}
            step={500}
            value={ai.maxTokens}
            onChange={(event) => {
              const parsed = Number.parseInt(event.target.value, 10)
              if (Number.isFinite(parsed)) setAi({ maxTokens: parsed })
            }}
            onBlur={() => setAi({ maxTokens: clamp(ai.maxTokens, 1, 200000) })}
          />
        </Field>
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <Button
          variant="secondary"
          size="sm"
          icon={<Plug className="h-3.5 w-3.5" />}
          loading={probe.status === 'busy'}
          onClick={() => void testConnection()}
        >
          Test connection
        </Button>
        <ProbeResult
          probe={probe}
          busyLabel="Sending a one-token ping…"
          render={(value) => value}
        />
      </div>

      <Note icon={<ShieldAlert className="h-3.5 w-3.5 text-state-info" />} tone="info">
        Requests go straight from this browser to {info.label}. Nothing is sent to a Sparquet
        server — there is no Sparquet server. Your key travels only in the request headers and
        is never written to logs.
      </Note>
    </Section>
  )
}

/* --------------------------------------------------------------- runner */

function RunnerSection() {
  const runnerUrl = useSettingsStore((state) => state.runnerUrl)
  const setRunnerUrl = useSettingsStore((state) => state.setRunnerUrl)
  const runnerToken = useSettingsStore((state) => state.runnerToken)
  const setRunnerToken = useSettingsStore((state) => state.setRunnerToken)
  const runAs = useSettingsStore((state) => state.runAs)
  const setRunAs = useSettingsStore((state) => state.setRunAs)

  const baseUrlId = useId()
  const tokenId = useId()
  const runAsId = useId()
  const [showToken, setShowToken] = useState(false)
  const [probe, setProbe] = useState<Probe<RunnerHealthValue>>(IDLE)
  const abortRef = useRef<AbortController | null>(null)

  useEffect(() => () => abortRef.current?.abort(), [])

  async function check() {
    abortRef.current?.abort()
    const controller = new AbortController()
    abortRef.current = controller
    const timer = window.setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS)
    setProbe({ status: 'busy' })

    try {
      const health = await checkRunnerHealth(runnerUrl.trim(), controller.signal)
      // A newer probe superseded this one; its result is the one that counts.
      if (abortRef.current !== controller) return
      setProbe({ status: 'ok', value: health })
    } catch (error) {
      if (abortRef.current !== controller) return
      setProbe({
        status: 'error',
        message: controller.signal.aborted
          ? `No answer within ${PROBE_TIMEOUT_MS / 1000} seconds.`
          : messageOf(error),
      })
    } finally {
      window.clearTimeout(timer)
    }
  }

  return (
    <Section meta={SECTION.runner}>
      <Field
        label="Runner base URL"
        htmlFor={baseUrlId}
        help="Studio works fully offline without the runner; it only powers the Run button."
      >
        <Input
          id={baseUrlId}
          mono
          spellCheck={false}
          value={runnerUrl}
          placeholder="http://127.0.0.1:8787"
          onChange={(event) => setRunnerUrl(event.target.value)}
        />
      </Field>

      <Field
        label="Runner token"
        htmlFor={tokenId}
        help="The runner prints a token in its terminal on startup — paste it here, and every run and validation carries it. Set SPARQUET_STUDIO_TOKEN before starting the runner to pin one that survives restarts."
      >
        <div className="relative">
          <Input
            id={tokenId}
            mono
            className="pr-10"
            type={showToken ? 'text' : 'password'}
            autoComplete="off"
            spellCheck={false}
            value={runnerToken}
            placeholder="Paste the token from the runner terminal"
            onChange={(event) => setRunnerToken(event.target.value)}
          />
          <IconButton
            size="sm"
            label={showToken ? 'Hide runner token' : 'Show runner token'}
            onClick={() => setShowToken((previous) => !previous)}
            className="absolute right-1 top-1/2 -translate-y-1/2"
          >
            {showToken ? <EyeOff /> : <Eye />}
          </IconButton>
        </div>
      </Field>

      <Field
        label="Run as"
        htmlFor={runAsId}
        help="Name recorded on every run you start from here. Leave it empty to record the runner's own OS account. This is a label for the history, not a permission — the runner authenticates the token, not a person."
      >
        <Input
          id={runAsId}
          spellCheck={false}
          value={runAs}
          placeholder={'The runner’s OS user'}
          onChange={(event) => setRunAs(event.target.value)}
        />
      </Field>

      <div className="flex flex-wrap items-center gap-3">
        <Button
          variant="secondary"
          size="sm"
          icon={<Plug className="h-3.5 w-3.5" />}
          loading={probe.status === 'busy'}
          disabled={runnerUrl.trim().length === 0}
          onClick={() => void check()}
        >
          Check connection
        </Button>
        <ProbeResult
          probe={probe}
          busyLabel="Contacting the runner…"
          render={(health) => (
            <span className="flex flex-wrap items-center gap-2">
              <span>
                Runner {health.version || 'unknown'} responded ({health.status}).
              </span>
              <Badge tone={health.sparkAvailable ? 'success' : 'warning'}>
                {health.sparkAvailable ? 'Spark available' : 'Spark not importable'}
              </Badge>
              <Badge tone={health.authRequired ? 'neutral' : 'warning'}>
                {health.authRequired ? 'Token enforced' : 'Token not enforced'}
              </Badge>
              {health.frameworkVersion && (
                <Badge tone="neutral">framework {health.frameworkVersion}</Badge>
              )}
              <span>
                {health.authRequired
                  ? 'It requires the token on runs and validations.'
                  : 'This build accepts unauthenticated runs — update the runner.'}
              </span>
            </span>
          )}
        />
      </div>

      <div className="space-y-2">
        <p className="text-xs font-medium text-content-muted">Start it</p>
        <Command command={INSTALL_COMMAND} />
        <Command command={START_COMMAND} />
        <p className="text-2xs leading-relaxed text-content-subtle">
          Run both from the{' '}
          <code className="font-mono text-content-muted">sparquet-studio</code> directory; the
          service adds the repository root to <code className="font-mono text-content-muted">sys.path</code>{' '}
          so <code className="font-mono text-content-muted">sparquet</code> resolves. A working{' '}
          <code className="font-mono text-content-muted">JAVA_HOME</code> is required for Spark.
        </p>
      </div>

      <Note icon={<AlertTriangle className="h-3.5 w-3.5 text-state-warning" />} tone="warning">
        The runner executes the Spark jobs you build — arbitrary SQL and arbitrary input and
        output paths, with your user's permissions. The token is what stops any page you happen
        to visit from driving it, so treat it as a password. Keep the runner bound to localhost
        (127.0.0.1, the default) and never expose it through a tunnel, a reverse proxy, or a
        public address.
      </Note>
    </Section>
  )
}

/* ----------------------------------------------------------------- data */

type ImportMode = 'merge' | 'replace'

interface ImportCandidate {
  name: string
  bundle: unknown
  workflows: number
  jobs: number
}

function AccessSection() {
  return (
    <Section meta={SECTION.access}>
      <AccessPanel />
      <TeamsPanel />
      <RolesPanel />
    </Section>
  )
}

function BillingSection() {
  return (
    <Section meta={SECTION.billing}>
      <CreditsPanel />
    </Section>
  )
}

function DataSection() {
  const workflowCount = useLibraryStore((state) => state.workflows.length)
  const jobCount = useLibraryStore((state) => state.jobs.length)
  const nodeCount = useLibraryStore((state) =>
    state.jobs.reduce((total, job) => total + job.graph.nodes.length, 0),
  )
  const reload = useLibraryStore((state) => state.load)

  const fileRef = useRef<HTMLInputElement>(null)
  const [exporting, setExporting] = useState(false)
  const [candidate, setCandidate] = useState<ImportCandidate | null>(null)
  const [importMode, setImportMode] = useState<ImportMode>('merge')
  const [importing, setImporting] = useState(false)
  const [resetOpen, setResetOpen] = useState(false)

  async function exportEverything() {
    setExporting(true)
    try {
      const bundle = await exportAll()
      const stamp = new Date().toISOString().slice(0, 10)
      downloadText(`sparquet-studio-${stamp}.json`, JSON.stringify(bundle, null, 2))
      toast.success('Workspace exported')
    } catch (error) {
      toast.error(messageOf(error))
    } finally {
      setExporting(false)
    }
  }

  async function pickFile(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0]
    // Reset the input so picking the same file twice still fires a change.
    event.target.value = ''
    if (!file) return

    try {
      const bundle: unknown = JSON.parse(await file.text())
      setImportMode('merge')
      setCandidate({
        name: file.name,
        bundle,
        workflows: countIn(bundle, 'workflows'),
        jobs: countIn(bundle, 'jobs'),
      })
    } catch {
      toast.error(`${file.name} is not valid JSON.`)
    }
  }

  async function runImport() {
    if (!candidate) return
    setImporting(true)
    try {
      const summary = await importAll(candidate.bundle, { merge: importMode === 'merge' })
      await reload()
      setCandidate(null)
      toast.success(
        `Imported ${summary.workflows} ${plural(summary.workflows, 'workflow')} and ` +
          `${summary.jobs} ${plural(summary.jobs, 'job')}` +
          (summary.skipped > 0 ? ` — ${summary.skipped} unreadable records skipped.` : '.'),
      )
    } catch (error) {
      toast.error(messageOf(error))
    } finally {
      setImporting(false)
    }
  }

  async function resetWorkspace() {
    try {
      await clearAll()
      await reload()
      setResetOpen(false)
      toast.success('Workspace cleared')
    } catch (error) {
      toast.error(messageOf(error))
    }
  }

  return (
    <Section meta={SECTION.data}>
      <dl className="grid grid-cols-3 gap-3">
        <Stat label="Workflows" value={workflowCount} />
        <Stat label="Jobs" value={jobCount} />
        <Stat label="Nodes" value={nodeCount} />
      </dl>

      <ActionRow
        title="Export everything"
        description="One JSON bundle with every workflow and job. Keys and preferences stay out of it."
        action={
          <Button
            size="sm"
            variant="secondary"
            icon={<Download className="h-3.5 w-3.5" />}
            loading={exporting}
            onClick={() => void exportEverything()}
          >
            Export
          </Button>
        }
      />

      <ActionRow
        title="Import a bundle"
        description="Restore an export on this machine or another one. You choose whether to merge or replace."
        action={
          <>
            {/*
              `hidden`, not `sr-only`: sr-only is position:absolute, and with no
              positioned ancestor this input anchored to the document and stretched
              it to its own offset, which made every in-page jump scroll the page
              behind the app. A display:none input still opens on .click().
            */}
            <input
              ref={fileRef}
              type="file"
              accept="application/json,.json"
              className="hidden"
              aria-hidden
              tabIndex={-1}
              onChange={(event) => void pickFile(event)}
            />
            <Button
              size="sm"
              variant="secondary"
              icon={<Upload className="h-3.5 w-3.5" />}
              onClick={() => fileRef.current?.click()}
            >
              Choose file
            </Button>
          </>
        }
      />

      <ActionRow
        title="Reset workspace"
        description="Deletes every workflow and job in this browser. Export first — this cannot be undone."
        action={
          <Button
            size="sm"
            variant="danger"
            icon={<Trash2 className="h-3.5 w-3.5" />}
            onClick={() => setResetOpen(true)}
          >
            Reset
          </Button>
        }
      />

      <ImportModal
        candidate={candidate}
        mode={importMode}
        busy={importing}
        onMode={setImportMode}
        onCancel={() => setCandidate(null)}
        onConfirm={() => void runImport()}
      />
      {/* Remounted on every open so the typed confirmation never survives a close. */}
      <ResetModal
        key={resetOpen ? 'reset-open' : 'reset-closed'}
        open={resetOpen}
        onOpenChange={setResetOpen}
        onConfirm={() => void resetWorkspace()}
      />
    </Section>
  )
}

function ImportModal({
  candidate,
  mode,
  busy,
  onMode,
  onCancel,
  onConfirm,
}: {
  candidate: ImportCandidate | null
  mode: ImportMode
  busy: boolean
  onMode: (mode: ImportMode) => void
  onCancel: () => void
  onConfirm: () => void
}) {
  return (
    <Modal
      open={candidate !== null}
      onOpenChange={(open) => {
        if (!open) onCancel()
      }}
      title="Import workspace"
      description={
        candidate
          ? `${candidate.name} — ${candidate.workflows} ${plural(candidate.workflows, 'workflow')}, ${candidate.jobs} ${plural(candidate.jobs, 'job')}.`
          : undefined
      }
      footer={
        <>
          <Button variant="ghost" onClick={onCancel}>
            Cancel
          </Button>
          <Button
            variant={mode === 'replace' ? 'danger' : 'primary'}
            loading={busy}
            onClick={onConfirm}
          >
            {mode === 'replace' ? 'Replace workspace' : 'Merge into workspace'}
          </Button>
        </>
      }
    >
      <div className="space-y-4">
        <Segmented<ImportMode>
          value={mode}
          onChange={onMode}
          options={[
            { value: 'merge', label: 'Merge' },
            { value: 'replace', label: 'Replace' },
          ]}
        />
        <p className="text-xs leading-relaxed text-content-muted">
          {mode === 'merge'
            ? 'Records are added, and anything with a matching id is overwritten by the bundle. Everything else in this browser is kept.'
            : 'Every workflow and job in this browser is deleted first, then the bundle is written. A copy of the current library is kept in a backup key.'}
        </p>
      </div>
    </Modal>
  )
}

function ResetModal({
  open,
  onOpenChange,
  onConfirm,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  onConfirm: () => void
}) {
  const [phrase, setPhrase] = useState('')
  const inputId = useId()
  const armed = phrase.trim().toUpperCase() === RESET_PHRASE

  function close() {
    setPhrase('')
    onOpenChange(false)
  }

  return (
    <Modal
      open={open}
      onOpenChange={(next) => {
        if (!next) close()
      }}
      title="Reset workspace"
      description="Every workflow and job in this browser is deleted. Settings and the AI key are untouched."
      size="sm"
      footer={
        <>
          <Button variant="ghost" onClick={close}>
            Cancel
          </Button>
          <Button variant="danger" disabled={!armed} onClick={onConfirm}>
            Delete everything
          </Button>
        </>
      }
    >
      <Field
        label={`Type ${RESET_PHRASE} to confirm`}
        htmlFor={inputId}
        help="There is no undo, and no copy on any server."
      >
        <Input
          id={inputId}
          mono
          autoComplete="off"
          spellCheck={false}
          value={phrase}
          placeholder={RESET_PHRASE}
          onChange={(event) => setPhrase(event.target.value)}
        />
      </Field>
    </Modal>
  )
}

/* ---------------------------------------------------------------- about */

function AboutSection() {
  return (
    <Section meta={SECTION.about}>
      <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3">
        <Stat label="Studio version" value={STUDIO_VERSION} />
        <Stat label="License" value="MIT" />
        <Stat label="Storage" value="This browser" />
      </dl>

      <div className="flex flex-wrap gap-2">
        <LinkButton href={REPO_URL}>Sparquet framework</LinkButton>
        <LinkButton href={DOCS_URL}>Documentation</LinkButton>
      </div>

      <div className="space-y-2">
        <p className="text-xs font-medium text-content-muted">How Studio works</p>
        <p className="text-xs leading-relaxed text-content-muted">
          Studio is a static front end with no accounts and no backend. Workflows, jobs and
          preferences live in this browser — IndexedDB, falling back to local storage — and the
          canvas compiles to the same pipeline JSON the framework runs. Clearing site data or
          moving to another browser loses your work, so export a bundle before you do either.
          Requests leave the page in exactly two cases: the AI provider you configure, and the
          local runner you start yourself.
        </p>
      </div>
    </Section>
  )
}

function LinkButton({ href, children }: { href: string; children: ReactNode }) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer"
      className="inline-flex items-center gap-1.5 rounded-lg border border-line bg-surface-raised px-2.5 py-1.5 text-xs text-content transition-colors hover:border-line-strong hover:bg-surface-sunken"
    >
      {children}
      <ExternalLink className="h-3.5 w-3.5 text-content-subtle" aria-hidden />
    </a>
  )
}

/* -------------------------------------------------------------- widgets */

function SliderControl({
  label,
  value,
  min,
  max,
  step,
  onChange,
}: {
  label: string
  value: number
  min: number
  max: number
  step: number
  onChange: (value: number) => void
}) {
  return (
    <RadixSlider.Root
      className="relative flex h-9 w-full touch-none select-none items-center"
      value={[value]}
      min={min}
      max={max}
      step={step}
      onValueChange={(values) => {
        const [next] = values
        if (typeof next === 'number') onChange(next)
      }}
    >
      <RadixSlider.Track className="relative h-1 w-full grow rounded-full bg-surface-sunken">
        <RadixSlider.Range className="absolute h-full rounded-full bg-brand-500" />
      </RadixSlider.Track>
      <RadixSlider.Thumb
        aria-label={label}
        className="block h-4 w-4 rounded-full border-2 border-brand-500 bg-surface shadow-card transition-colors hover:bg-brand-500/20"
      />
    </RadixSlider.Root>
  )
}

function ProbeResult<T>({
  probe,
  busyLabel,
  render,
}: {
  probe: Probe<T>
  busyLabel: string
  render: (value: T) => ReactNode
}) {
  // The region is always mounted so screen readers announce each result.
  return (
    <span
      role="status"
      aria-live="polite"
      className={cn(
        'flex items-start gap-2 text-xs leading-relaxed',
        probe.status === 'error' ? 'text-state-danger' : 'text-content-muted',
      )}
    >
      {probe.status === 'busy' && (
        <>
          <Spinner className="mt-0.5 h-3.5 w-3.5" />
          {busyLabel}
        </>
      )}
      {probe.status === 'ok' && (
        <>
          <CircleCheck className="mt-0.5 h-3.5 w-3.5 shrink-0 text-state-success" aria-hidden />
          {render(probe.value)}
        </>
      )}
      {probe.status === 'error' && (
        <>
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          {probe.message}
        </>
      )}
    </span>
  )
}

function Command({ command }: { command: string }) {
  return (
    <div className="flex items-center gap-2 rounded-lg border border-line bg-surface-sunken px-3 py-2">
      <code className="min-w-0 flex-1 overflow-x-auto whitespace-pre font-mono text-2xs text-content">
        {command}
      </code>
      <IconButton
        size="sm"
        label={`Copy command: ${command}`}
        onClick={() => {
          void copyText(command).then((copied) => {
            if (copied) toast.success('Command copied')
            else toast.error('Could not reach the clipboard')
          })
        }}
      >
        <Copy />
      </IconButton>
    </div>
  )
}

function Note({
  icon,
  tone,
  children,
}: {
  icon: ReactNode
  tone: 'info' | 'warning'
  children: ReactNode
}) {
  return (
    <div
      className={cn(
        'flex items-start gap-2 rounded-lg border p-3 text-xs leading-relaxed',
        tone === 'warning'
          ? 'border-state-warning/30 bg-state-warning/10 text-content-muted'
          : 'border-state-info/30 bg-state-info/10 text-content-muted',
      )}
    >
      <span className="mt-0.5 shrink-0" aria-hidden>
        {icon}
      </span>
      <p>{children}</p>
    </div>
  )
}

function Stat({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="rounded-lg border border-line bg-surface-sunken px-3 py-2">
      <dt className="text-2xs uppercase tracking-wide text-content-subtle">{label}</dt>
      <dd className="mt-0.5 text-sm font-medium text-content">{value}</dd>
    </div>
  )
}

function ActionRow({
  title,
  description,
  action,
}: {
  title: string
  description: string
  action: ReactNode
}) {
  return (
    <div className="flex items-start justify-between gap-4 border-t border-line pt-4">
      <div className="space-y-0.5">
        <p className="text-sm text-content">{title}</p>
        <p className="max-w-md text-2xs leading-relaxed text-content-subtle">{description}</p>
      </div>
      <div className="flex shrink-0 items-center gap-2">{action}</div>
    </div>
  )
}

/* -------------------------------------------------------------- helpers */

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max)
}

function plural(count: number, word: string): string {
  return count === 1 ? word : `${word}s`
}

function countIn(bundle: unknown, key: 'workflows' | 'jobs'): number {
  if (typeof bundle !== 'object' || bundle === null) return 0
  const value = (bundle as Record<string, unknown>)[key]
  return Array.isArray(value) ? value.length : 0
}
