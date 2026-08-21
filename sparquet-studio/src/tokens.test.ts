/**
 * Contrast budget for the design tokens.
 *
 * The tokens in `index.css` are the only place the app names a colour, so this is
 * where a regression would land. Body copy renders at `text-2xs` (11px), which is
 * normal text under WCAG — 4.5:1, not 3:1.
 */

import { readFileSync } from 'node:fs'
import { fileURLToPath, URL } from 'node:url'

import { describe, expect, it } from 'vitest'

type Rgb = [number, number, number]
type Tokens = Record<string, Rgb>

const css = readFileSync(fileURLToPath(new URL('./index.css', import.meta.url)), 'utf8')

function parseBlock(selector: string): Tokens {
  const start = css.indexOf(selector)
  if (start === -1) throw new Error(`no ${selector} block in index.css`)
  const open = css.indexOf('{', start)
  const end = css.indexOf('\n  }', open)
  const body = css.slice(open, end)
  const tokens: Tokens = {}
  for (const match of body.matchAll(/--([\w-]+):\s*(\d+)\s+(\d+)\s+(\d+);/g)) {
    tokens[match[1]] = [Number(match[2]), Number(match[3]), Number(match[4])]
  }
  return tokens
}

const light = parseBlock(':root {')
const dark = { ...light, ...parseBlock("[data-theme='dark'] {") }

function channel(value: number): number {
  const c = value / 255
  return c <= 0.04045 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4)
}

function luminance([r, g, b]: Rgb): number {
  return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b)
}

export function contrast(a: Rgb, b: Rgb): number {
  const [high, low] = [luminance(a), luminance(b)].sort((x, y) => y - x)
  return (high + 0.05) / (low + 0.05)
}

/** Tailwind's `/12` modifier: the token painted at 12% over whatever is behind it. */
function wash(fg: Rgb, bg: Rgb, alpha: number): Rgb {
  return [
    fg[0] * alpha + bg[0] * (1 - alpha),
    fg[1] * alpha + bg[1] * (1 - alpha),
    fg[2] * alpha + bg[2] * (1 - alpha),
  ]
}

const SURFACES = [
  'canvas',
  'surface',
  'surface-raised',
  'surface-sunken',
  'surface-overlay',
  'rail',
  'rail-sunken',
]
const FOREGROUNDS = ['content', 'content-muted', 'content-subtle']
const STATES = ['success', 'warning', 'danger', 'info']

const THEMES: [string, Tokens][] = [
  ['light', light],
  ['dark', dark],
]

describe.each(THEMES)('%s theme', (_name, tokens) => {
  it.each(FOREGROUNDS)('%s reads on every surface', (fg) => {
    for (const surface of SURFACES) {
      expect(contrast(tokens[fg], tokens[surface])).toBeGreaterThanOrEqual(4.5)
    }
  })

  it.each(STATES)('%s reads on a plain surface and on its own badge wash', (state) => {
    const surface = tokens.surface
    expect(contrast(tokens[state], surface)).toBeGreaterThanOrEqual(4.5)
    // Badge is `bg-state-x/12`; the canvas issue chip is `/15`.
    for (const alpha of [0.12, 0.15]) {
      const onWash = contrast(tokens[state], wash(tokens[state], surface, alpha))
      expect(onWash).toBeGreaterThanOrEqual(4.5)
    }
  })

  it('labels solid danger and success buttons with the inverted content token', () => {
    expect(contrast(tokens['content-inverted'], tokens.danger)).toBeGreaterThanOrEqual(4.5)
    expect(contrast(tokens['content-inverted'], tokens.success)).toBeGreaterThanOrEqual(4.5)
  })

  it('puts white on the brand button', () => {
    expect(contrast([255, 255, 255], tokens['brand-500'])).toBeGreaterThanOrEqual(4.5)
  })
})

describe('brand foreground', () => {
  it('reads on the surface and on the brand wash it usually sits on', () => {
    for (const [tokens, key] of [
      [light, 'brand-600'],
      [dark, 'brand-400'],
    ] as const) {
      expect(contrast(tokens[key], tokens.surface)).toBeGreaterThanOrEqual(4.5)
      const brandWash = wash(tokens['brand-500'], tokens.surface, 0.12)
      expect(contrast(tokens[key], brandWash)).toBeGreaterThanOrEqual(4.5)
    }
  })

  it('carries a focus ring that stands out from the canvas it is offset against', () => {
    expect(contrast(light['brand-600'], light.canvas)).toBeGreaterThanOrEqual(3)
    expect(contrast(dark['brand-400'], dark.canvas)).toBeGreaterThanOrEqual(3)
  })
})

describe('dark theme token coverage', () => {
  it('redefines every content token instead of inheriting a light-theme value', () => {
    const darkOnly = parseBlock("[data-theme='dark'] {")
    for (const fg of FOREGROUNDS) {
      expect(darkOnly[fg]).toBeDefined()
      expect(darkOnly[fg]).not.toEqual(light[fg])
    }
  })
})
