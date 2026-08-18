/** Public surface of the graph ⇄ JSON compiler. */

export {
  chainToSink,
  isCompilable,
  isDisabled,
  isLastValidationOfRun,
  isNoteNode,
  isSinkNode,
  isSourceNode,
  isTransformNode,
  isValidationNode,
  isValidationSink,
  longestCommonPrefix,
  makeEdge,
  newNodeId,
  nodeById,
  primaryChildren,
  primaryParent,
  primaryParents,
  sideParent,
  validationSinkLink,
} from '@/lib/compiler/graph'
export type {
  ChainProblem,
  ChainProblemCode,
  ChainResult,
  ValidationSinkLink,
} from '@/lib/compiler/graph'

export { compileGraph, serializePipeline } from '@/lib/compiler/toJson'
export type { CompileResult } from '@/lib/compiler/toJson'

export { pipelineToGraph } from '@/lib/compiler/toGraph'
export type { DecompileResult } from '@/lib/compiler/toGraph'

export { autoLayout, NODE_RENDER_SIZE, NOTE_RENDER_SIZE } from '@/lib/compiler/layout'
export type { LayoutOptions } from '@/lib/compiler/layout'
