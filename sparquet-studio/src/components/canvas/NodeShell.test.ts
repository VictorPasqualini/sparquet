import { afterEach, describe, expect, it, vi } from 'vitest'

import { HANDLE } from '@/types/studio'

import {
  cancelConnect,
  readConnectSource,
  startConnect,
  subscribeConnectSource,
} from './NodeShell'

afterEach(() => cancelConnect())

describe('keyboard connect source', () => {
  it('starts empty and remembers the node a connection is leaving', () => {
    expect(readConnectSource()).toBeNull()
    startConnect('node-a')
    expect(readConnectSource()).toEqual({ nodeId: 'node-a', handle: HANDLE.out })
    cancelConnect()
    expect(readConnectSource()).toBeNull()
  })

  it('remembers which output the connection leaves from', () => {
    startConnect('rule-1', HANDLE.outInvalid)
    expect(readConnectSource()).toEqual({ nodeId: 'rule-1', handle: HANDLE.outInvalid })
  })

  it('treats the same node on a different handle as a change', () => {
    const listener = vi.fn()
    const unsubscribe = subscribeConnectSource(listener)

    startConnect('rule-1')
    startConnect('rule-1', HANDLE.outReport)
    expect(listener).toHaveBeenCalledTimes(2)

    unsubscribe()
  })

  it('notifies subscribers on every change, and never for a no-op', () => {
    const listener = vi.fn()
    const unsubscribe = subscribeConnectSource(listener)

    startConnect('node-a')
    startConnect('node-a')
    expect(listener).toHaveBeenCalledTimes(1)

    startConnect('node-b')
    expect(listener).toHaveBeenCalledTimes(2)

    cancelConnect()
    cancelConnect()
    expect(listener).toHaveBeenCalledTimes(3)

    unsubscribe()
    startConnect('node-c')
    expect(listener).toHaveBeenCalledTimes(3)
  })
})
