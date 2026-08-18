import { afterEach, describe, expect, it, vi } from 'vitest'

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
    // Every node has exactly one output, so the id is the whole pending source.
    expect(readConnectSource()).toBe('node-a')
    cancelConnect()
    expect(readConnectSource()).toBeNull()
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
