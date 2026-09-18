/**
 * A query turned into pipeline JSON, so an exploration can become a Job.
 *
 * The SQL editor is read-only by construction — the runner refuses anything that
 * is not a SELECT — and that is right: writing data is a decision that should be
 * reviewed, not something a query does by accident. But the work of finding the
 * statement is the same work either way, and retyping it into a canvas is how a
 * good query stays an exploration forever. This is the bridge: the statement
 * survives verbatim, and the only thing left to decide is where the result lands.
 *
 * The trick that keeps the SQL untouched is the view name. In the editor each
 * dataset is opened under an alias (`silver_orders`); in a Job the `sql`
 * transformation registers the incoming DataFrame under whatever `view_name` it
 * is given. Setting that name to the alias the query already uses means the
 * statement compiles across without a single edit — no rewriting, no regex over
 * somebody's SQL, and the Job reads exactly like the query that was tested.
 *
 * Only the FIRST dataset can travel that way: a pipeline has one `input`, and
 * the others have to be brought in with a join or a union, which needs keys
 * nothing here can guess. Rather than invent them, the extra datasets come back
 * in `unattached` so the caller can say so out loud, before the Job is created
 * rather than when it fails to resolve a view name at run time.
 */

import type { SparkSettings } from '@/types/pipeline'

/** A dataset the query names, as the SQL editor knows it. */
export interface QueryDataset {
  /** The address, as the catalog stores it. */
  key: string
  /** The view name the SQL calls it by. */
  alias: string
  /** The reader format the catalog resolved for the address. */
  format: string
}

export interface JobFromQuery {
  /** Pipeline JSON, ready for `createJob({ pipeline })`. */
  pipeline: Record<string, unknown>
  /** The dataset that became the job's `input`. */
  attached: QueryDataset | null
  /** Datasets the query names that a join or a union still has to bring in. */
  unattached: QueryDataset[]
}

export interface JobFromQueryInput {
  /** The job name, which is also the pipeline name in the JSON. */
  name: string
  sql: string
  /** Every dataset the query names. The first becomes the input. */
  datasets: readonly QueryDataset[]
  /** Connector jars and SQL extensions, as the editor resolved them. */
  spark?: SparkSettings
  description?: string
}

/** A trailing semicolon is fine in an editor and a syntax error in `spark.sql`. */
function statementOf(sql: string): string {
  // Trimmed twice on purpose: dropping the semicolon can expose the space that
  // was sitting in front of it.
  return sql.trim().replace(/;\s*$/, '').trim()
}

/**
 * Pipeline JSON for `name` that runs `sql` over the datasets it names.
 *
 * The destination is deliberately left blank. A format and a path guessed from
 * the input would be a write nobody chose, and the linter already says what a
 * missing destination means — which makes the empty sink the one thing the
 * author must answer before this can run.
 */
export function pipelineFromQuery({
  name,
  sql,
  datasets,
  spark,
  description,
}: JobFromQueryInput): JobFromQuery {
  const statement = statementOf(sql)
  const [trunk, ...rest] = datasets

  const pipeline: Record<string, unknown> = {
    name,
    ...(description ? { description } : {}),
    ...(spark && Object.keys(spark).length > 0 ? { spark } : {}),
    input: trunk
      ? { format: trunk.format, path: trunk.key }
      : // A query over no catalog dataset is a query over literals or over
        // tables the session already has. It still becomes a Job; the input is
        // the one thing its author has to fill in.
        { format: '', path: '' },
    transformations: [
      {
        type: 'sql',
        query: statement,
        // The alias, not `_df`: that is what makes the statement travel verbatim.
        ...(trunk ? { view_name: trunk.alias } : {}),
      },
    ],
    output: { format: '', path: '', mode: 'overwrite' },
  }

  return { pipeline, attached: trunk ?? null, unattached: rest }
}
