/**
 * "Create a Job from this proposal": what Apply means with no canvas open.
 *
 * The panel beside the canvas applies a proposal to the job already on screen.
 * The front door has no such job, and the two ways out of that are to hide the
 * button — leaving the answer as prose nobody can run — or to ask the two things
 * nothing can infer: which workflow owns the job, and what to call it. This asks.
 *
 * The pipeline travels across unchanged. It is the assistant's JSON, untrusted
 * until `pipelineToGraph` reads it, which is why the failure is caught and shown
 * here rather than left to a blank canvas: a proposal that does not compile is a
 * thing to say out loud, not a job with nothing in it.
 */

import { useId, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import { Button, Field, Input, Modal, Select } from '@/components/ui'
import { usePermission } from '@/lib/auth/usePermission'
import { useLibraryStore } from '@/store/library'

const NEW_WORKFLOW = '__new__'

export interface CreateJobFromProposalProps {
  /** The proposed pipeline JSON, exactly as the model wrote it. */
  pipeline: unknown
  /** The one-line summary shown on the proposal card. */
  summary: string
  onClose: () => void
  /** Called once the job exists, before navigating away. */
  onCreated: () => void
}

export function CreateJobFromProposal({
  pipeline,
  summary,
  onClose,
  onCreated,
}: CreateJobFromProposalProps) {
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
  const [name, setName] = useState(() => suggestName(pipeline))
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

  const submit = () => {
    if (!valid || busy) return
    setBusy(true)
    void (async () => {
      try {
        const owner = creatingWorkflow
          ? (await createWorkflow({ name: workflowName.trim() })).id
          : workflowId
        const job = await createJob({
          workflowId: owner,
          name: name.trim(),
          description: summary || 'Created from an assistant proposal.',
          pipeline,
        })
        onCreated()
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
      title="Create a Job from this proposal"
      description="The proposed pipeline becomes a new Job on the canvas, where the linter reads it
        like any other."
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

        {summary && (
          <p className="rounded-lg border border-line bg-surface-sunken px-3 py-2.5 text-2xs leading-relaxed text-content-muted">
            {summary}
          </p>
        )}

        {failure && <p className="text-2xs text-state-danger">{failure}</p>}
      </form>
    </Modal>
  )
}

/** The pipeline's own `name`, when it wrote one — it is a better guess than ours. */
function suggestName(pipeline: unknown): string {
  if (pipeline && typeof pipeline === 'object' && 'name' in pipeline) {
    const name = (pipeline as { name?: unknown }).name
    if (typeof name === 'string' && name.trim()) return name.trim()
  }
  return 'Assistant pipeline'
}
