/**
 * The assistant's prompts, generated from the catalog.
 *
 * Nothing about the pipeline language is spelled out twice: the transformation
 * list, the formats and the validators are rendered from the same data that
 * drives the palette and the linter, so the model cannot be told about a node
 * that does not exist.
 */

import { FORMATS, TRANSFORMATIONS, VALIDATORS } from '@/catalog'
import type { FieldSpec, FormatDef, TransformationDef, ValidatorDef } from '@/catalog/types'
import type { AiIntent } from '@/types/ai'
import type { StudioNode, ValidationIssue } from '@/types/studio'

const keyList = (fields: FieldSpec[], required: boolean): string[] =>
  fields.filter((field) => Boolean(field.required) === required).map((field) => field.key)

const join = (values: string[]): string => (values.length ? values.join(', ') : 'none')

function transformationLine(def: TransformationDef): string {
  const required = keyList(def.fields, true)
  const optional = keyList(def.fields, false)
  // `with` never appears in the catalog fields: on the canvas the right-hand
  // source is a second incoming edge, but in JSON it is a required key.
  if (def.secondaryInput) required.unshift('with')

  const flags: string[] = []
  if (def.supportsSubPipeline) flags.push('accepts "with_transformations"')
  if (def.emitsRuntimeVar) flags.push('publishes a {{runtime}} variable')
  if (def.canHalt) flags.push('can end the run')
  if (def.sideEffectFree) flags.push('never changes the DataFrame')

  const head = `- ${def.type} — required: ${join(required)} — optional: ${join(optional)}`
  const tail = flags.length ? ` [${flags.join('; ')}]` : ''
  const notes = [def.summary, def.gotchas[0] ? `Gotcha: ${def.gotchas[0]}` : '']
    .filter(Boolean)
    .join(' ')
  return `${head}${tail}\n  ${notes}`
}

function formatLine(def: FormatDef): string {
  const io = [def.canRead ? 'read' : '', def.canWrite ? 'write' : ''].filter(Boolean).join('+')
  const modes = def.canWrite ? ` — modes: ${join([...def.modes])}` : ''
  const merge = def.supportsMerge ? ' — supports merge' : ''
  const partition = def.supportsPartitioning ? ' — supports partition_by' : ''
  return `- ${def.id} (${io})${modes}${merge}${partition} — path is the ${def.pathLabel.toLowerCase()}. ${def.summary}`
}

function validatorLine(def: ValidatorDef): string {
  const required = keyList(def.fields, true)
  const optional = keyList(def.fields, false)
  return `- ${def.type} — required: ${join(required)} — optional: ${join(optional)} — ${def.summary}`
}

const SCHEMA_SKELETON = `{
  "name": "string (required)",
  "description": "string (optional)",
  "spark": { "app_name": "string", "master": "string", "configs": { "spark.sql.x": "value" } },
  "input": { "format": "delta", "path": "schema.table", "options": {} },
  "transformations": [ { "type": "filter", "condition": "status = 'ATIVO'" } ],
  "validations": {
    "on_failure": "fail | warn | skip",
    "report": { "format": "csv", "path": "/dq/report", "mode": "overwrite" },
    "rules": [ { "type": "not_null", "columns": ["id"] } ]
  },
  "outputs": [
    {
      "format": "delta",
      "path": "schema.target",
      "mode": "append",
      "partition_by": ["dt"],
      "columns": ["id", "total"],
      "transformations": [],
      "options": {}
    }
  ]
}`

const HARD_RULES = [
  'Emit exactly ONE JSON object per reply — the complete pipeline, never a diff, never a patch, never several alternatives.',
  '"name" and "input" are mandatory. "output" (single object) or "outputs" (array) is mandatory; prefer "outputs" when there is more than one destination.',
  'Only use the transformation types, formats and validator types listed above. Never invent a key or a type.',
  'Transformations run top to bottom over one DataFrame; each one sees the columns produced by the previous ones.',
  '"select" entries are SQL expressions parsed with F.expr, so alias them ("to_json(payload) AS value") and backtick names with spaces or dots.',
  '"group_by" takes "by" (list of column names) and "agg" as a LIST OF SQL STRINGS ("sum(valor) as total"), never an object.',
  '"cast", "rename" and "sort" take plain column names — they do not parse SQL expressions.',
  '"with_column" uses either column + expression, or the "columns" map (name → expression, applied in key order); whenever the "columns" key is present it wins and the single-column keys are ignored, so never emit both.',
  '"struct" builds a nested column: string values are SQL expressions, object values are nested structs, and dotted keys such as "data.nc.issuerName" auto-nest.',
  'kafka is write-only — never use it as an input. It needs the brokers as either the bootstrap_servers option or the Spark-native kafka.bootstrap.servers option, plus a topic (the path doubles as the topic).',
  'mode "merge" only exists for delta and iceberg, and requires options.merge_keys; options.merge_condition is extra SQL using the T. (target) and S. (source) aliases.',
  '"skip_if_false" is a TOP-LEVEL key of any transformation, never nested inside its parameters. After substitution, an empty string skips the step and a boolean expression skips it when false.',
  '{param} is substituted in the raw JSON before parsing (values come from the job parameters); {{runtime}} is resolved during execution from a value captured by a "collect" step. Never mix the two syntaxes.',
  'Use "checkpoint" before "collect" so the collected values do not recompute the whole lineage, and before fanning out to several outputs.',
  'Validations report on the data, they do not change it — filter rows with "filter", do not use a validator to drop them.',
  'Every value is a string, number, boolean, list or object of plain JSON: no comments, no trailing commas, no expressions outside strings.',
]

export function buildSystemPrompt(): string {
  const transformations = TRANSFORMATIONS.map(transformationLine).join('\n')
  const readable = FORMATS.filter((format) => format.canRead)
    .map((format) => format.id)
    .join(', ')
  const writable = FORMATS.filter((format) => format.canWrite)
    .map((format) => format.id)
    .join(', ')

  return [
    'You are the Sparquet Studio assistant. Sparquet is a Python/PySpark framework driven by JSON: a pipeline reads one input, applies an ordered list of transformations, optionally validates the result, and writes one or more outputs. You author and edit those JSON pipelines.',
    '',
    '# Pipeline schema',
    '',
    '```json',
    SCHEMA_SKELETON,
    '```',
    '',
    '# Transformations (exhaustive)',
    '',
    transformations,
    '',
    '# Formats',
    '',
    `Readable: ${readable}. Writable: ${writable}.`,
    '',
    FORMATS.map(formatLine).join('\n'),
    '',
    '# Validators',
    '',
    'They live under "validations.rules"; "on_failure" is fail, warn or skip.',
    '',
    VALIDATORS.map(validatorLine).join('\n'),
    '',
    '# Hard rules',
    '',
    HARD_RULES.map((rule, index) => `${index + 1}. ${rule}`).join('\n'),
    '',
    '# Style',
    '',
    'Prefer the smallest pipeline that satisfies the request. Keep existing node order and existing names when editing. Filter early, build heavy structs late. Answer in English.',
  ].join('\n')
}

/* ---------------------------------------------------------------- user prompt */

export interface AiPromptContext {
  /** Current pipeline JSON, when the user shares the job. */
  pipeline?: unknown
  /** Lint issues currently reported for the job. */
  issues?: ValidationIssue[]
  /** Node selected on the canvas, when the request is scoped to it. */
  selectedNode?: StudioNode | null
}

const INTENT_BRIEF: Record<AiIntent, string> = {
  generate: 'Build a new pipeline from the request below.',
  modify: 'Change the current pipeline as requested, keeping everything else untouched.',
  explain: 'Explain the current pipeline, or the part the user asks about.',
  fix: 'Fix the reported problems in the current pipeline without changing what it does otherwise.',
  optimize:
    'Improve the current pipeline (fewer steps, earlier filters, checkpoints where they pay off) without changing its output.',
  document:
    'Improve the naming and the description of the pipeline and of its steps, without changing any behaviour.',
  chat: 'Answer the question about Sparquet pipelines.',
}

/** Intents that must return a full pipeline; explain/chat answer in prose only. */
const PROPOSAL_INTENTS: ReadonlySet<AiIntent> = new Set<AiIntent>([
  'generate',
  'modify',
  'fix',
  'optimize',
  'document',
])

function describeNode(node: StudioNode): string {
  const data = node.data
  switch (data.kind) {
    case 'source':
      return `source node "${data.label ?? data.format}" — format ${data.format}, path ${data.path || '(empty)'}`
    case 'transform':
      return `transformation node "${data.label ?? data.transform}" — type ${data.transform}, params ${JSON.stringify(data.params)}`
    case 'validation':
      return `validation rule node — type ${data.validator}, params ${JSON.stringify(data.params)}`
    case 'sink':
      return `output node "${data.label ?? data.format}" — format ${data.format}, path ${data.path || '(empty)'}, mode ${data.mode}`
    case 'note':
      return `sticky note — ${data.text}`
    default: {
      const unreachable: never = data
      return String(unreachable)
    }
  }
}

function issueLine(issue: ValidationIssue): string {
  const hint = issue.hint ? ` (${issue.hint})` : ''
  const where = issue.field ? ` [field: ${issue.field}]` : ''
  return `- ${issue.severity}: ${issue.message}${where}${hint}`
}

export function buildUserPrompt(
  intent: AiIntent,
  userText: string,
  context: AiPromptContext = {},
): string {
  const sections: string[] = [INTENT_BRIEF[intent], '', '## Request', '', userText.trim()]

  if (context.pipeline !== undefined && context.pipeline !== null) {
    sections.push(
      '',
      '## Current pipeline JSON',
      '',
      '```json',
      JSON.stringify(context.pipeline, null, 2),
      '```',
    )
  }

  if (context.issues && context.issues.length > 0) {
    sections.push(
      '',
      '## Problems reported by the editor',
      '',
      context.issues.map(issueLine).join('\n'),
    )
  }

  if (context.selectedNode) {
    sections.push('', '## Selected node', '', describeNode(context.selectedNode))
  }

  sections.push('', '## Answer format', '')
  if (PROPOSAL_INTENTS.has(intent)) {
    sections.push(
      [
        'Return the FULL updated pipeline JSON — every key, not only the parts you changed — inside a single ```json fenced block.',
        'Emit exactly one fenced block and nothing else inside it.',
        'Outside the block, add one short paragraph of plain English explaining what you changed and why. No bullet lists, no second code block.',
      ].join('\n'),
    )
  } else {
    sections.push(
      'Answer in plain English. Do not return a JSON block unless the user explicitly asks for one; quote at most a short snippet inline when it helps.',
    )
  }

  return sections.join('\n')
}
