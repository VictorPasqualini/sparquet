/** Public surface of the pipeline modules: describing a Job, and Pipelines. */

export { describeJob } from '@/lib/pipeline/describe'
export type {
  JobDescription,
  JobEndpoint,
  JobStep,
  JobStepKind,
} from '@/lib/pipeline/describe'

export {
  linkRejection,
  newPipeline,
  newLink,
  newStage,
  planPipelineRun,
  resolvePipeline,
  stageRowPosition,
  wouldCreateCycle,
} from '@/lib/pipeline/pipeline'
export type {
  PipelineRunPlan,
  PipelineRunStage,
  LinkRejection,
  ResolvedPipeline,
  ResolvedStage,
} from '@/lib/pipeline/pipeline'
