/**
 * "Make this query a Job": the one dialog between an exploration and a pipeline.
 *
 * It asks the two things nothing can infer — which workflow owns the result and
 * what to call it — and says plainly what the Job will and will not contain:
 * the statement verbatim, the first dataset as the input, no destination. The
 * blank destination is the point. A guessed write is a write nobody chose, so
 * the Job lands on the canvas with the one decision still to make, and the
 * linter already knows how to say so.
 */

import { ArrowRight, Database } from 'lucide-react'
import { useId, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import { Button, Field, Input, Modal, Select } from '@/components/ui'
import { usePermission } from '@/lib/auth/usePermission'
import { pipelineFromQuery, type QueryDataset } from '@/lib/sql/toJob'
import { useLibraryStore } from '@/store/library'
import type { SparkSettings } from '@/types/pipeline'

const NEW_WORKFLOW = '__new__'

export interface CreateJobFromQueryProps {
  /** The statement as it stands — the selection when there is one, else the buffer. */
  sql: string
  /** Every dataset the statement names. The first becomes the job's input. */
  datasets: readonly QueryDataset[]
  /** Connector jars and SQL extensions the editor resolved for these datasets. */
  spark?: SparkSettings
  /** The query's name, used as the default job name. */
  suggestedName: string
  onClose: () => void
}

export function CreateJobFromQuery({
  sql,
  datasets,
  spark,
  suggestedName,
  onClose,
}: CreateJobFromQueryProps) {
  const navigate = useNavigate()
  const workflows = useLibraryStore((state) => state.workflows)
  const createWorkflow = useLibraryStore((state) => state.createWorkflow)
  const createJob = useLibraryStore((state) => state.createJob)
  const mayWrite = usePermission('workspace:Write')

  const formId = useId()
  const workflowFieldId = `${formId}-workflow`
  const workflowNameFieldId = `${formId}-workflow-name`
  const nameFieldId = `${formId}-name`

  const [workflowId, setWorkflowId] = useState(() => workflows[0]?.id ?? NEW_WORKFLOW)
  const [workflowName, setWorkflowName] = useState('My pipelines')
  const [name, setName] = useState(suggestedName)
  const [busy, setBusy] = useState(false)
  const [failure, setFailure] = useState('')

  const creatingWorkflow = workflowId === NEW_WORKFLOW
  const valid = name.trim().length > 0 && (!creatingWorkflow || workflowName.trim().length > 0)

  const workflowOptions = useMemo(
    () => [
      ...workflows.map((workflow) => ({ value: workflow.id, label: workflow.name })),
      { value: NEW_WORKFLOW, label: 'New workflow…' },
    ],
    [workflows],
  )

  const [trunk, ...rest] = datasets

  const submit = () => {
    if (!valid || busy) return
    setBusy(true)
    void (async () => {
      try {
        const owner = creatingWorkflow
          ? (await createWorkflow({ name: workflowName.trim() })).id
          : workflowId
        const { pipeline } = pipelineFromQuery({
          name: name.trim(),
          sql,
          datasets,
          spark,
        })
        const job = await createJob({
          workflowId: owner,
          name: name.trim(),
          description: 'Created from a SQL editor query.',
          pipeline,
        })
        navigate(`/jobs/${job.id}`)
      } catch (error) {
        setBusy(false)
        setFailure(error instanceof Error ? error.message : String(error))
      }
    })()
  }

  return (
    <Modal
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      title="Create a Job from this query"
      description="The statement travels across unchanged — it runs as a SQL transformation over
        the dataset it already names."
      size="sm"
      footer={
        <>
          <Button variant="ghost" onClick={onClose} disabled={busy}>
            Cancel
          </Button>
          <Button
            type="submit"
            form={formId}
            variant="primary"
            loading={busy}
            disabled={!valid || !mayWrite}
            title={mayWrite ? undefined : 'Your role does not allow workspace:Write'}
          >
            Create job
          </Button>
        </>
      }
    >
      <form
        id={formId}
        className="space-y-3"
        onSubmit={(event) => {
          event.preventDefault()
          submit()
        }}
      >
        <Field label="Workflow" htmlFor={workflowFieldId}>
          <Select
            id={workflowFieldId}
            value={workflowId}
            onValueChange={setWorkflowId}
            options={workflowOptions}
            ariaLabel="Workflow"
          />
        </Field>

        {creatingWorkflow && (
          <Field
            label="New workflow name"
            htmlFor={workflowNameFieldId}
            help="Workflows group related jobs — one per domain works well."
          >
            <Input
              id={workflowNameFieldId}
              value={workflowName}
              onChange={(event) => setWorkflowName(event.target.value)}
              placeholder="My pipelines"
            />
          </Field>
        )}

        <Field label="Job name" htmlFor={nameFieldId} required>
          <Input
            id={nameFieldId}
            autoFocus
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="Orders by month"
          />
        </Field>

        <div className="space-y-1.5 rounded-lg border border-line bg-surface-sunken px-3 py-2.5 text-2xs leading-relaxed text-content-muted">
          {trunk ? (
            <p className="flex items-center gap-1.5">
              <Database className="h-3 w-3 shrink-0" aria-hidden />
              <span className="min-w-0 truncate">
                Input: <span className="text-content">{trunk.key}</span>
                <span className="text-content-subtle"> as {trunk.alias}</span>
              </span>
            </p>
          ) : (
            <p>
              The query names no catalog dataset, so the Job opens without an input — set one
              on the canvas.
            </p>
          )}

          {rest.length > 0 && (
            <p className="text-state-warning">
              {rest.map((dataset) => dataset.alias).join(', ')}{' '}
              {rest.length === 1 ? 'is' : 'are'} also queried. A pipeline has one input, so bring{' '}
              {rest.length === 1 ? 'it' : 'them'} in with a join or a union — registered under the
              same view {rest.length === 1 ? 'name' : 'names'} the query already uses.
            </p>
          )}

          <p className="flex items-center gap-1.5">
            <ArrowRight className="h-3 w-3 shrink-0" aria-hidden />
            No destination is set. Writing is the decision this dialog will not make for you.
          </p>
        </div>

        {failure && <p className="text-2xs text-state-danger">{failure}</p>}
      </form>
    </Modal>
  )
}
