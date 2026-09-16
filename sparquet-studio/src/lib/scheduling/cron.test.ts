import { describe, expect, it } from 'vitest'

import { describeCron, validateCron } from './cron'

describe('validateCron', () => {
  it('takes the everyday shapes', () => {
    expect(validateCron('0 6 * * *')).toBeNull()
    expect(validateCron('*/15 * * * *')).toBeNull()
    expect(validateCron('0 9-17 * * 1-5')).toBeNull()
    expect(validateCron('30 3 1 jan-mar mon')).toBeNull()
    expect(validateCron('0 0 * * 7')).toBeNull()
  })

  it('refuses a six-field expression instead of guessing at it', () => {
    // Reading `0 0 6 * * *` as minute-zero-of-hour-zero would run a daily job
    // every minute, which is the one wrong answer that costs money.
    expect(validateCron('0 0 6 * * *')).toMatch(/five fields/)
  })

  it('refuses macros rather than ignoring them', () => {
    expect(validateCron('@daily')).toMatch(/five fields/)
  })

  it('names the field it is complaining about', () => {
    expect(validateCron('0 25 * * *')).toMatch(/^hour:/)
    expect(validateCron('0 6 * * funday')).toMatch(/^day of week:/)
    expect(validateCron('0 6 0 * *')).toMatch(/^day of month:/)
  })

  it('catches a range that ends before it starts', () => {
    expect(validateCron('0 17-9 * * *')).toMatch(/ends before it starts/)
  })

  it('catches a step that is not a number', () => {
    expect(validateCron('*/x * * * *')).toMatch(/is not a step/)
    expect(validateCron('*/0 * * * *')).toMatch(/is not a step/)
  })

  it('catches an empty item in a list', () => {
    expect(validateCron('0,,30 * * * *')).toMatch(/empty item/)
  })

  it('asks for something when given nothing', () => {
    expect(validateCron('   ')).toMatch(/Write a schedule/)
  })
})

describe('describeCron', () => {
  it('says the common shapes in words', () => {
    expect(describeCron('0 6 * * *')).toBe('Every day at 06:00')
    expect(describeCron('0 6 * * *', 'America/Sao_Paulo')).toBe(
      'Every day at 06:00 (America/Sao_Paulo)',
    )
    expect(describeCron('30 8 * * mon')).toBe('Every Mon at 08:30')
    expect(describeCron('0 7 * * 1-5')).toBe('Every weekday at 07:00')
    expect(describeCron('0 0 1 * *')).toBe('On day 1 of every month at 00:00')
    expect(describeCron('15 * * * *')).toBe('Every hour at minute 15')
    expect(describeCron('* * * * *')).toBe('Every minute')
  })

  it('leaves the local zone unsaid, because that is the default', () => {
    expect(describeCron('0 6 * * *', 'local')).toBe('Every day at 06:00')
    expect(describeCron('0 6 * * *', '')).toBe('Every day at 06:00')
  })

  it('shows an expression it does not recognise as written', () => {
    expect(describeCron('0 6 1,15 */2 *')).toBe('0 6 1,15 */2 *')
  })

  it('shows an unreadable expression as written rather than paraphrasing it', () => {
    expect(describeCron('0 99 * * *')).toBe('0 99 * * *')
    expect(describeCron('')).toBe('No schedule')
  })
})
