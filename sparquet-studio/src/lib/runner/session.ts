/**
 * The Spark settings a read needs, taken from the Jobs that already do it.
 *
 * A connector like Delta or Iceberg is not a library the reader imports at the
 * moment it reads: it is a jar the SparkSession loads and an extension the
 * session installs, and both are honoured only when the session is CREATED. The
 * runner keeps one session for the whole process, so whatever the first request
 * built it with is what every later request gets — which is why reading a Delta
 * table from the SQL editor failed with
 *
 *   org.apache.spark.SparkClassNotFoundException: [DATA_SOURCE_NOT_FOUND]
 *   Failed to find the data source: delta.
 *
 * on a runner that runs Delta Jobs perfectly well: the Job carries
 * `spark.jars.packages` and `spark.sql.extensions` in its settings, and the ad
 * hoc query carried nothing.
 *
 * So the query sends the settings too, and the runner rebuilds the session when
 * the live one is missing what the query needs. They are not invented here —
 * they are the union of what the Jobs that touch these very datasets already
 * declare, which keeps one place to maintain: a Job that reads a table is the
 * statement of how that table is opened.
 */

import { datasetKey, lineageOfJob } from '@/lib/lineage'
import type { Job } from '@/types/studio'
import type { SparkSettings } from '@/types/pipeline'

/** Every dataset address one Job names, read or written, keyed as the catalog keys it. */
function addressesOf(job: Job): Set<string> {
  const lineage = lineageOfJob(job)
  const out = new Set<string>()
  for (const endpoint of [...lineage.reads, ...lineage.writes]) {
    const key = datasetKey(endpoint.address)
    if (key) out.add(key)
  }
  return out
}

/**
 * The `spark` block to send with a query or a schema probe over `keys`.
 *
 * `undefined` when there is nothing to say — the common case, and the one where
 * the runner must not be asked to consider restarting anything.
 *
 * Only `configs` are merged. `app_name` and `master` are the runner's own
 * business: a query has no opinion on where the cluster is, and sending one
 * would let an ad hoc SELECT move somebody else's session to `local[*]`.
 *
 * When two Jobs disagree on the same key, the last one wins by address order,
 * which is arbitrary — and deliberately so. The realistic disagreement is two
 * Delta versions, and no merge rule saves that; the runner reports what it
 * built the session with, and the honest fix is for the two Jobs to agree.
 */
export function sparkForDatasets(
  jobs: readonly Job[],
  keys: readonly string[],
): SparkSettings | undefined {
  const wanted = new Set(keys.map(datasetKey).filter(Boolean))
  if (wanted.size === 0) return undefined

  const configs: Record<string, string> = {}
  for (const job of jobs) {
    const declared = job.settings?.spark?.configs
    if (!declared || Object.keys(declared).length === 0) continue
    const touched = addressesOf(job)
    let relevant = false
    for (const key of wanted) {
      if (touched.has(key)) {
        relevant = true
        break
      }
    }
    if (!relevant) continue
    for (const [key, value] of Object.entries(declared)) {
      if (typeof value === 'string' && value.trim() !== '') configs[key] = value
    }
  }

  return Object.keys(configs).length > 0 ? { configs } : undefined
}
