/**
 * React Flow re-mounts every node when the `nodeTypes` object identity changes,
 * so both maps are module constants — never build them inside a component.
 */

import type { EdgeTypes, NodeTypes } from '@xyflow/react'

import { PipelineEdge } from './edges/PipelineEdge'
import { NoteNode } from './nodes/NoteNode'
import { SinkNode } from './nodes/SinkNode'
import { SourceNode } from './nodes/SourceNode'
import { TransformNode } from './nodes/TransformNode'
import { ValidationsNode } from './nodes/ValidationsNode'

export const nodeTypes: NodeTypes = {
  source: SourceNode,
  transform: TransformNode,
  validations: ValidationsNode,
  sink: SinkNode,
  note: NoteNode,
}

export const edgeTypes: EdgeTypes = {
  pipeline: PipelineEdge,
}
