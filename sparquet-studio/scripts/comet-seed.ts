/**
 * Seeds the two Jobs the Comet deck is shot from, straight into a runner's workspace.
 *
 * The pair is the whole point: the same pipeline JSON twice, differing only in
 * the Spark configs that turn the plugin on. Run both from the Studio and the
 * run panel shows the gain in the place a person actually works, instead of in
 * a terminal nobody puts in a post.
 *
 * Written as a script and not by hand because a Job holds a laid-out graph, and
 * the honest way to get one is the Studio's own importer — `pipelineToGraph`
 * plus `autoLayout`, exactly what `createJob` does when it imports a JSON.
 *
 *   npx vite-node scripts/comet-seed.ts -- --url http://localhost:8788 --token dev-local-token
 *
 * Needs a runner with a workspace configured; it writes through
 * `PUT /workspace/{kind}/{id}`, the same call the Studio's own backend makes.
 */

import process from 'node:process'

import { autoLayout } from '@/lib/compiler/layout'
import { compileGraph } from '@/lib/compiler/toJson'
import { pipelineToGraph } from '@/lib/compiler/toGraph'
import type { Job, Workflow } from '@/types/studio'

const args = process.argv.slice(2)
const readArg = (flag: string, fallback: string) => {
  const index = args.indexOf(flag)
  return index >= 0 ? args[index + 1] : fallback
}

const URL_BASE = readArg('--url', 'http://localhost:8788').replace(/\/+$/, '')
const TOKEN = readArg('--token', process.env.SPARQUET_STUDIO_TOKEN ?? '')
const DATA = readArg('--data', '/tmp/sparquet-bench-comet/linhas-40000000.parquet')

const WORKFLOW_ID = 'comet-demo'
const NOW = Date.now()

/** What the benchmark measures, as the framework's own pipeline JSON. */
function pipeline(name: string, comet: boolean) {
  const configs: Record<string, string> = {
    'spark.sql.shuffle.partitions': '8',
    'spark.ui.enabled': 'false',
    'spark.sql.adaptive.enabled': 'true',
  }
  if (comet) {
    // The plugin is off unless every one of these is set. Off-heap is not
    // optional: the native operators allocate outside the JVM heap.
    configs['spark.plugins'] = 'org.apache.spark.CometPlugin'
    configs['spark.shuffle.manager'] =
      'org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager'
    configs['spark.memory.offHeap.enabled'] = 'true'
    configs['spark.memory.offHeap.size'] = '4g'
  }
  return {
    name,
    spark: { app_name: name, master: 'local[4]', configs },
    input: { format: 'parquet', path: DATA },
    transformations: [
      { type: 'filter', condition: 'valor > 0' },
      {
        type: 'group_by',
        by: ['nome', 'categoria'],
        agg: ['sum(valor) as total', 'count(1) as linhas'],
      },
    ],
    // `cache: false`, for the reason the benchmark states: the view writer
    // caches and counts by default, so a second run would read memory and
    // measure Spark's cache instead of the engine underneath it.
    output: { format: 'view', path: name.replace(/-/g, '_'), options: { cache: 'false' } },
  }
}

function jobOf(id: string, name: string, description: string, comet: boolean): Job {
  const imported = pipelineToGraph(pipeline(id, comet))
  return {
    id,
    workflowId: WORKFLOW_ID,
    name,
    description,
    tags: comet ? ['comet'] : ['spark'],
    settings: imported.settings,
    graph: autoLayout(imported.graph),
    params: [],
    createdAt: NOW,
    updatedAt: NOW,
    revision: 1,
  }
}

async function put(kind: string, id: string, record: unknown, config: unknown) {
  const response = await fetch(`${URL_BASE}/workspace/${kind}/${encodeURIComponent(id)}`, {
    method: 'PUT',
    headers: {
      'content-type': 'application/json',
      ...(TOKEN ? { 'x-sparquet-token': TOKEN } : {}),
    },
    body: JSON.stringify({ record, config }),
  })
  if (!response.ok) {
    throw new Error(`${kind}/${id}: ${response.status} ${await response.text()}`)
  }
  console.log(`wrote ${kind}/${id}`)
}

async function main() {
  const workflow: Workflow = {
    id: WORKFLOW_ID,
    name: 'DataFusion Comet',
    description: 'A mesma agregação, com e sem o plugin nativo.',
    accent: 'brand',
    tags: ['benchmark'],
    createdAt: NOW,
    updatedAt: NOW,
  }
  await put('workflow', WORKFLOW_ID, workflow, null)

  const jobs = [
    jobOf('agregacao-spark', 'Agregação — Spark puro', '40M linhas, JVM, sem plugin.', false),
    jobOf('agregacao-comet', 'Agregação — DataFusion Comet', '40M linhas, operadores nativos.', true),
  ]
  for (const job of jobs) {
    const compiled = compileGraph(job.graph, job.settings, job.params)
    await put('job', job.id, job, compiled.pipeline)
  }
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
