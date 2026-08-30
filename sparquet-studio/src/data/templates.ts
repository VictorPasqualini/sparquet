/**
 * Starter templates — the pipelines a user can open on day one.
 *
 * Every `pipeline` value is real Sparquet JSON: it is fed straight to
 * `pipelineToGraph`, so anything invalid here surfaces as import issues on the
 * canvas instead of a runtime error in Spark.
 */

import { nanoid } from 'nanoid'

import { autoLayout, pipelineToGraph } from '@/lib/compiler'
import { inferParams } from '@/lib/params'
import type { PipelineSpec } from '@/types/pipeline'
import type { Job, JobTemplate } from '@/types/studio'

export const TEMPLATES: JobTemplate[] = [
  {
    id: 'csv-to-parquet',
    name: 'CSV to Parquet',
    summary:
      'Read a CSV drop, keep the rows that matter, fix the types and land partitioned Parquet.',
    highlights: [
      'The four transformations you will use every day: filter, select, cast, with_column',
      'Transformations run strictly in order — filter before select, or the filtered column is already gone',
      'Parquet writes a directory of part files; partition_by splits it by column value',
    ],
    level: 'starter',
    tags: ['csv', 'parquet', 'ingestion'],
    pipeline: {
      name: 'csv_to_parquet',
      description: 'Landing CSV cleaned, typed and written as partitioned Parquet.',
      input: {
        format: 'csv',
        path: '/data/landing/orders',
        options: { header: 'true', inferSchema: 'true', sep: ',' },
      },
      transformations: [
        { type: 'filter', condition: "status = 'CONFIRMED'" },
        {
          type: 'select',
          columns: ['order_id', 'customer_id', 'country', 'amount', 'ordered_at'],
        },
        {
          type: 'cast',
          columns: { order_id: 'long', amount: 'decimal(18,2)', ordered_at: 'timestamp' },
        },
        { type: 'with_column', column: 'order_date', expression: 'to_date(ordered_at)' },
      ],
      output: {
        format: 'parquet',
        path: '/data/curated/orders',
        mode: 'overwrite',
        partition_by: ['country'],
      },
    } satisfies PipelineSpec,
  },

  {
    id: 'ingestion-data-quality',
    name: 'Ingestion with data quality',
    summary:
      'Clean a customer feed and measure it: five validation rules plus a persisted quality report.',
    highlights: [
      'Transformations change the data; validations only report on it',
      'on_failure "warn" keeps the pipeline running so the report is actually written',
      'The report has a fixed schema: pipeline, rule_type, passed, failed_count, message, validated_at',
    ],
    level: 'starter',
    tags: ['csv', 'validations', 'data-quality'],
    pipeline: {
      name: 'ingestion_with_data_quality',
      description:
        'Customer ingestion with cleansing, five quality rules and a persisted validation report.',
      input: { format: 'csv', path: '/data/landing/customers' },
      transformations: [
        { type: 'filter', condition: "status = 'active'" },
        {
          type: 'select',
          columns: ['id', 'name', 'email', 'age', 'country', 'created_at'],
        },
        { type: 'cast', columns: { age: 'integer', created_at: 'timestamp' } },
        { type: 'rename', mappings: { created_at: 'creation_date' } },
        { type: 'with_column', column: 'loaded_at', expression: 'current_timestamp()' },
        { type: 'drop_duplicates', columns: ['id'] },
      ],
      validations: {
        on_failure: 'warn',
        report: {
          format: 'csv',
          path: '/data/quality/customers_report',
          mode: 'append',
        },
        rules: [
          { type: 'not_null', columns: ['id', 'email'] },
          { type: 'unique', columns: ['id'] },
          { type: 'range', column: 'age', min: 0, max: 150 },
          { type: 'regex', column: 'email', pattern: '^[^@\\s]+@[^@\\s]+\\.[a-z]{2,}$' },
          { type: 'row_count', min: 1 },
        ],
      },
      output: {
        format: 'parquet',
        path: '/data/curated/customers',
        mode: 'overwrite',
        partition_by: ['country'],
      },
    } satisfies PipelineSpec,
  },

  {
    id: 'join-runtime-pushdown',
    name: 'Join and runtime pushdown',
    summary:
      'Enrich orders from a large event table by pushing the key list down into the read as a literal IN (...).',
    highlights: [
      'checkpoint materializes the working set so the following collect is cheap',
      'collect publishes a {{runtime}} variable; the join reads it inside with_transformations',
      'Inside with_transformations the right side has no alias yet — use bare column names there',
    ],
    level: 'intermediate',
    tags: ['delta', 'join', 'pushdown', 'performance'],
    pipeline: {
      name: 'join_with_runtime_pushdown',
      description:
        'Order enrichment with predicate pushdown: checkpoint, collect the keys, filter the right side by IN (...).',
      input: { format: 'delta', path: 'sales.orders' },
      transformations: [
        {
          type: 'filter',
          condition: "order_date >= '2026-01-01' AND status = 'APPROVED'",
        },
        {
          type: 'stop_if_empty',
          message: 'No approved orders in the window — nothing to enrich',
        },
        { type: 'checkpoint', method: 'localCheckpoint', eager: true },
        { type: 'collect', column: 'customer_id', as: 'active_customers' },
        {
          type: 'join',
          input: { format: 'delta', path: 'sales.bronze_customer_events' },
          with_transformations: [
            { type: 'filter', condition: 'customer_id IN ({{active_customers}})' },
            { type: 'select', columns: ['customer_id', 'credit_score', 'segment'] },
            { type: 'distinct' },
          ],
          on: 'customer_id',
          how: 'left',
        },
        { type: 'fill_na', value: 'UNSEGMENTED', columns: ['segment'] },
        { type: 'debug', label: 'after enrichment', actions: ['count', 'print_schema'] },
      ],
      output: { format: 'delta', path: 'sales.orders_enriched', mode: 'overwrite' },
    } satisfies PipelineSpec,
  },

  {
    id: 'nested-payload-multi-output',
    name: 'Nested payload and multi-output',
    summary:
      'Build a nested payload with dot-path fields, then write two different shapes of the same DataFrame.',
    highlights: [
      'struct dot-paths ("issuer.name") auto-nest, so the payload reads like a flat table',
      'Each output starts from the same DataFrame and can reshape it on its own',
      'checkpoint before the outputs so the lineage is computed once, not once per destination',
    ],
    level: 'intermediate',
    tags: ['delta', 'struct', 'multi-output', 'payload'],
    pipeline: {
      name: 'nested_payload_multi_output',
      description:
        'Nested payload built with struct dot-paths and written in two shapes: flattened JSON and typed columns.',
      input: { format: 'delta', path: 'contracts.silver_contract' },
      transformations: [
        { type: 'filter', condition: "status = 'READY'" },
        { type: 'stop_if_empty', message: 'No contracts ready to publish' },
        {
          type: 'with_column',
          columns: {
            total_amount: 'cast(principal_amount + interest_amount as double)',
            issued_on: 'cast(issued_at as date)',
          },
        },
        {
          type: 'struct',
          column: 'payload',
          fields: {
            external_id: 'contract_id',
            'issuer.name': 'issuer_name',
            'issuer.document': "lpad(cast(issuer_document as string), 14, '0')",
            'amounts.principal': 'cast(principal_amount as double)',
            'amounts.total': 'total_amount',
            schedule: {
              issued_on: 'cast(issued_on as string)',
              due_on: 'cast(due_date as string)',
            },
          },
        },
        { type: 'checkpoint' },
      ],
      outputs: [
        {
          format: 'parquet',
          path: '/data/events/contracts_json',
          mode: 'append',
          transformations: [
            {
              type: 'with_column',
              columns: { value: 'to_json(payload)', published_at: 'current_timestamp()' },
            },
          ],
          columns: ['contract_id', 'value', 'published_at'],
        },
        {
          format: 'delta',
          path: 'contracts.gold_contract',
          mode: 'append',
          transformations: [
            {
              type: 'with_column',
              columns: {
                issuer_document: 'payload.issuer.document',
                total: 'payload.amounts.total',
              },
            },
          ],
          columns: ['contract_id', 'issuer_document', 'total'],
        },
      ],
    } satisfies PipelineSpec,
  },

  {
    id: 'delta-merge-upsert',
    name: 'Delta merge (upsert)',
    summary: 'Aggregate an incremental drop and upsert it into a Delta table with MERGE INTO.',
    highlights: [
      'mode "merge" requires options.on and options.actions — either one missing and the write raises',
      'The ON is the whole predicate, using T. for the target and S. for the source; the WHEN clauses run in the order given',
      'A unique rule on the merge key guards against Delta\'s "multiple source rows matched" error',
    ],
    level: 'intermediate',
    tags: ['delta', 'merge', 'upsert', 'group_by'],
    pipeline: {
      name: 'delta_merge_upsert',
      description:
        'Incremental customer summary upserted into Delta with MERGE INTO, guarded by a uniqueness rule.',
      input: { format: 'parquet', path: '/data/incremental/orders' },
      transformations: [
        {
          type: 'cast',
          columns: { order_id: 'long', amount: 'double', order_date: 'date' },
        },
        { type: 'filter', condition: 'amount > 0' },
        {
          type: 'group_by',
          by: ['customer_id'],
          agg: [
            'sum(amount) as total_amount',
            'count(*) as order_count',
            'max(order_date) as last_order_date',
          ],
        },
        { type: 'with_column', column: 'updated_at', expression: 'current_timestamp()' },
      ],
      validations: {
        on_failure: 'fail',
        rules: [
          { type: 'not_null', columns: ['customer_id'] },
          { type: 'unique', columns: ['customer_id'] },
        ],
      },
      output: {
        format: 'delta',
        path: 'analytics.customer_summary',
        mode: 'merge',
        options: {
          on: 'T.customer_id = S.customer_id AND T.updated_at < S.updated_at',
          actions: [
            'WHEN MATCHED THEN UPDATE SET *',
            'WHEN NOT MATCHED THEN INSERT *',
          ],
        },
      },
    } satisfies PipelineSpec,
  },

  {
    id: 'aggregation-group-by-pivot',
    name: 'Aggregation with group_by and pivot',
    summary: 'Revenue by region and product line, pivoted into one column per month.',
    highlights: [
      'agg entries are complete SQL expressions — the alias lives inside the string',
      'Listing the pivot values saves Spark an extra scan to discover them',
      'group_by drops every column not named in "by" or produced by "agg"',
    ],
    level: 'intermediate',
    tags: ['parquet', 'group_by', 'pivot', 'analytics'],
    pipeline: {
      name: 'monthly_revenue_pivot',
      description:
        'Revenue mart: net amount aggregated by region and product line, pivoted by month.',
      input: { format: 'parquet', path: '/data/curated/orders' },
      transformations: [
        {
          type: 'filter',
          condition: "order_date BETWEEN '2026-01-01' AND '2026-12-31'",
        },
        {
          type: 'with_column',
          columns: {
            order_month: "date_format(order_date, 'MMM')",
            net_amount: 'cast(amount - coalesce(discount, 0) as decimal(18,2))',
          },
        },
        {
          type: 'group_by',
          by: ['region', 'product_line'],
          agg: [
            'sum(net_amount) as revenue',
            'count(distinct order_id) as orders',
            'avg(net_amount) as avg_ticket',
          ],
          pivot: {
            column: 'order_month',
            values: [
              'Jan',
              'Feb',
              'Mar',
              'Apr',
              'May',
              'Jun',
              'Jul',
              'Aug',
              'Sep',
              'Oct',
              'Nov',
              'Dec',
            ],
          },
        },
        { type: 'fill_na', value: 0 },
        { type: 'sort', columns: ['region', 'product_line'], ascending: [true, true] },
      ],
      output: {
        format: 'parquet',
        path: '/data/marts/revenue_by_month',
        mode: 'overwrite',
      },
    } satisfies PipelineSpec,
  },

  {
    id: 'union-and-dedupe',
    name: 'Union of two feeds',
    summary:
      'Merge two regional CSV feeds into one dataset, aligning columns by name before dedupe.',
    highlights: [
      'allow_missing_columns switches union from positional to by-name matching',
      'Select first: the framework adds an ingestion_ts column that the second feed does not have',
      'sort before drop_duplicates when it matters which duplicate survives',
    ],
    level: 'intermediate',
    tags: ['csv', 'union', 'dedupe'],
    pipeline: {
      name: 'union_regional_feeds',
      description:
        'Two regional CSV feeds unioned by name, typed, ordered and de-duplicated by order id.',
      input: {
        format: 'csv',
        path: '/data/landing/orders_eu',
        options: { sep: ';', header: 'true', inferSchema: 'false' },
      },
      transformations: [
        {
          type: 'select',
          columns: ['order_id', 'customer_id', 'region', 'amount', 'order_date'],
        },
        {
          type: 'union',
          input: {
            format: 'csv',
            path: '/data/landing/orders_us',
            options: { sep: ',', header: 'true', inferSchema: 'false' },
          },
          allow_missing_columns: true,
        },
        { type: 'cast', columns: { amount: 'decimal(18,2)', order_date: 'date' } },
        { type: 'sort', columns: ['order_date', 'order_id'], ascending: [false, true] },
        { type: 'drop_duplicates', columns: ['order_id'] },
      ],
      output: {
        format: 'parquet',
        path: '/data/curated/orders_all',
        mode: 'overwrite',
      },
    } satisfies PipelineSpec,
  },

  {
    id: 'staging-view-handoff',
    name: 'Staging view handoff',
    summary:
      'Aggregate with raw SQL, check the result with sql, and publish it as a temp view for the next pipeline.',
    highlights: [
      'The sql step registers the DataFrame under view_name and the query must read from that name',
      'sql passes when the query returns true — write the invariant, not the violation',
      'A view output is session-scoped: the next fw.run() in the same session can read it as an input',
    ],
    level: 'advanced',
    tags: ['delta', 'sql', 'view', 'validations'],
    pipeline: {
      name: 'staging_to_view_handoff',
      description:
        'Open balance per customer: SQL aggregation, runtime pushdown on the dimension, published to a temp view and a Delta table.',
      input: { format: 'delta', path: 'billing.silver_invoice' },
      transformations: [
        { type: 'filter', condition: "status = 'PENDING'" },
        { type: 'stop_if_empty', message: 'No pending invoices — staging not refreshed' },
        {
          type: 'sql',
          view_name: 'invoices',
          query:
            'SELECT customer_id, count(*) AS invoice_count, sum(amount) AS open_amount, min(due_date) AS next_due_date FROM invoices GROUP BY customer_id',
        },
        { type: 'checkpoint' },
        { type: 'collect', column: 'customer_id', as: 'staged_customers' },
        {
          type: 'join',
          input: { format: 'delta', path: 'crm.dim_customer' },
          with_transformations: [
            { type: 'filter', condition: 'customer_id IN ({{staged_customers}})' },
            { type: 'select', columns: ['customer_id', 'customer_name', 'account_manager'] },
          ],
          on: 'customer_id',
          how: 'left',
        },
      ],
      validations: {
        on_failure: 'warn',
        report: { format: 'delta', path: 'quality.validation_log', mode: 'append' },
        rules: [
          { type: 'not_null', columns: ['customer_id'] },
          {
            type: 'sql',
            query: 'SELECT count(*) = 0 FROM _validation_df WHERE open_amount <= 0',
            error_message: 'Customers staged with a non-positive open amount',
          },
        ],
      },
      outputs: [
        {
          format: 'view',
          path: 'open_balance_staging',
          mode: 'overwrite',
          options: { cache: 'true' },
        },
        {
          format: 'delta',
          path: 'billing.gold_open_balance',
          mode: 'overwrite',
        },
      ],
    } satisfies PipelineSpec,
  },

  {
    id: 'kafka-publication',
    name: 'Kafka publication',
    summary:
      'Turn rows into a JSON event with headers and publish them to a topic, keeping an audit copy.',
    highlights: [
      'The Kafka sink keeps only key, value, topic, partition, timestamp and headers — everything else is dropped',
      'value_column must name a real column; the code defaults are payload / header, not value / key',
      'Per-output transformations let the same DataFrame become an event stream and an audit table',
    ],
    level: 'advanced',
    tags: ['kafka', 'struct', 'streaming', 'multi-output'],
    pipeline: {
      name: 'publish_orders_to_kafka',
      description:
        'Approved orders serialized to JSON, published to Kafka with a header, and stored as an audit trail in Delta.',
      input: { format: 'delta', path: 'sales.orders_enriched' },
      transformations: [
        { type: 'filter', condition: "status = 'APPROVED'" },
        { type: 'stop_if_empty', message: 'No approved orders to publish' },
        {
          type: 'struct',
          column: 'payload',
          fields: {
            eventId: 'uuid()',
            eventType: "'ORDER_APPROVED'",
            'order.id': 'cast(order_id as string)',
            'order.customerId': 'cast(customer_id as string)',
            'order.amount': 'cast(amount as double)',
            'order.approvedAt': "date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')",
          },
        },
        { type: 'checkpoint' },
      ],
      outputs: [
        {
          format: 'kafka',
          path: 'orders.approved.v1',
          mode: 'append',
          transformations: [
            { type: 'drop_duplicates', columns: ['order_id'] },
            {
              type: 'struct',
              column: 'header_event',
              fields: { key: "'event-type'", value: "cast('ORDER_APPROVED' as binary)" },
            },
            {
              type: 'with_column',
              columns: {
                value: 'to_json(payload)',
                key: 'cast(order_id as string)',
                headers: 'array(header_event)',
              },
            },
          ],
          columns: ['key', 'value', 'headers'],
          options: {
            bootstrap_servers: 'broker-1:9092,broker-2:9092',
            value_column: 'value',
            key_column: 'key',
            'kafka.max.request.size': '4194304',
          },
        },
        {
          format: 'delta',
          path: 'sales.published_orders',
          mode: 'append',
          transformations: [
            {
              type: 'with_column',
              columns: { payload: 'to_json(payload)', published_at: 'current_timestamp()' },
            },
          ],
          columns: ['order_id', 'payload', 'published_at'],
          options: { mergeSchema: 'true' },
        },
      ],
    } satisfies PipelineSpec,
  },

  {
    id: 'parameterized-pipeline',
    name: 'Parameterized pipeline',
    summary:
      'One JSON, many runs: {param} placeholders drive the filters, and skip_if_false turns whole steps on and off.',
    highlights: [
      'A false boolean or an empty list becomes an empty string, which is what makes skip_if_false skip',
      'A parameter missing from params stays literal in the JSON — absent is not the same as false',
      'skip_if_false only sees literals, never DataFrame columns, so it branches on parameters alone',
    ],
    level: 'advanced',
    tags: ['params', 'skip_if_false', 'delta', 'reusable'],
    pipeline: {
      name: 'parameterized_regional_load',
      description:
        'Regional load driven by params: optional product filter, optional enrichment, replaceWhere overwrite.',
      input: { format: 'delta', path: 'sales.orders' },
      transformations: [
        { type: 'filter', condition: "region = '{region}'" },
        {
          type: 'filter',
          skip_if_false: '{product_ids}',
          condition: 'product_id IN ({product_ids})',
        },
        { type: 'stop_if_empty', message: 'No rows for the requested region' },
        {
          type: 'select',
          columns: ['order_id', 'customer_id', 'region', 'amount', 'order_date'],
        },
        {
          type: 'join',
          skip_if_false: '{enrich_customer}',
          input: { format: 'delta', path: 'sales.dim_customer' },
          with_transformations: [{ type: 'select', columns: ['customer_id', 'customer_tier'] }],
          on: 'customer_id',
          how: 'left',
        },
        {
          type: 'with_column',
          skip_if_false: "'{load_mode}' in ('FULL', 'BACKFILL')",
          column: 'reloaded_at',
          expression: 'current_timestamp()',
        },
      ],
      output: {
        format: 'delta',
        path: 'sales.orders_by_region',
        mode: 'overwrite',
        partition_by: ['region'],
        options: { replaceWhere: "region = '{region}'" },
      },
    } satisfies PipelineSpec,
  },
]

export function getTemplate(id: string): JobTemplate | undefined {
  return TEMPLATES.find((template) => template.id === id)
}

export function templateToJob(template: JobTemplate, workflowId: string): Job {
  const imported = pipelineToGraph(template.pipeline)
  const now = Date.now()

  return {
    id: nanoid(10),
    workflowId,
    name: template.name,
    description: template.summary,
    tags: [...template.tags],
    settings: imported.settings,
    graph: autoLayout(imported.graph),
    params: inferParams(template.pipeline),
    createdAt: now,
    updatedAt: now,
    revision: 1,
  }
}
