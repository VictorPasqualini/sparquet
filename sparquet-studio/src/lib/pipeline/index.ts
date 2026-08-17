/** Public surface of the pipeline modules: the derived file map and pipelines. */

export { deriveInferredPipeline, describeJob, linkBetween } from '@/lib/pipeline/inferredPipeline'
export type {
  JobDescription,
  JobLink,
  JobSummary,
  JobEndpoint,
  JobLinkVia,
  JobStep,
  JobStepKind,
  InferredPipeline,
} from '@/lib/pipeline/inferredPipeline'

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
