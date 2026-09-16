import { describe, expect, it } from 'vitest'

import { historyScope } from './history'

describe('historyScope', () => {
  it('asks for the file when the buffer has one', () => {
    expect(historyScope('qz9', 'tab-1')).toEqual({ savedQueryId: 'qz9' })
  })

  it('asks for the tab while the buffer has no file', () => {
    expect(historyScope(null, 'tab-1')).toEqual({ tab: 'tab-1' })
  })

  it('never sends both: the file is the identity once there is one', () => {
    expect(historyScope('qz9', 'tab-1')).not.toHaveProperty('tab')
  })
})
