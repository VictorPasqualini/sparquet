/**
 * Captures the screens the promo PDF uses, framed: each React Flow graph gets a
 * fit-view click before the shot. One browser session — the example data lives
 * in IndexedDB, which a fresh profile would not have.
 */

import { copyFileSync, existsSync, mkdirSync } from 'node:fs'
import process from 'node:process'

import puppeteer from 'puppeteer-core'

const args = process.argv.slice(2)
const readArg = (flag, fallback) => {
  const index = args.indexOf(flag)
  return index >= 0 ? args[index + 1] : fallback
}

const BASE_URL = readArg('--url', 'http://localhost:5273')
const OUT = readArg('--out', 'promo-shots')
const PROFILE = readArg('--profile', '')

const CHROME_CANDIDATES = [
  process.env.CHROME_PATH,
  'C:/Program Files/Google/Chrome/Application/chrome.exe',
  'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe',
  `${process.env.LOCALAPPDATA ?? ''}/Google/Chrome/Application/chrome.exe`,
].filter(Boolean)

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

async function clickText(page, text, selector = 'button') {
  const clicked = await page.evaluate(
    (needle, query) => {
      const hit = [...document.querySelectorAll(query)].find((node) =>
        (node.textContent ?? '').trim().includes(needle),
      )
      if (!hit) return false
      hit.click()
      return true
    },
    text,
    selector,
  )
  if (!clicked) console.log(`  (no "${text}" in ${selector})`)
  return clicked
}

async function zoomIn(page, times) {
  for (let index = 0; index < times; index += 1) {
    await page.evaluate(() => document.querySelector('.react-flow__controls-zoomin')?.click())
    await wait(500)
  }
}

async function fit(page) {
  const clicked = await page.evaluate(() => {
    const button = document.querySelector('.react-flow__controls-fitview')
    if (!button) return false
    button.click()
    return true
  })
  await wait(1200)
  console.log('  fit:', clicked)
  return clicked
}

/**
 * Picks a value in a Radix select. Radix opens on pointerdown, so a synthetic
 * `.click()` from inside the page does nothing — the click has to come from
 * puppeteer, which needs a selector, hence the marker attribute.
 */
async function pickOption(page, triggerText, optionText) {
  const marked = await page.evaluate((needle) => {
    const trigger = [...document.querySelectorAll('[role="combobox"]')].find((node) =>
      (node.textContent ?? '').includes(needle),
    )
    if (!trigger) return false
    trigger.setAttribute('data-shot', 'select')
    return true
  }, triggerText)
  if (!marked) {
    console.log(`  (no select showing "${triggerText}")`)
    return false
  }
  await page.click('[data-shot="select"]')
  await wait(700)
  const option = await page.evaluate((needle) => {
    const hit = [...document.querySelectorAll('[role="option"]')].find((node) =>
      (node.textContent ?? '').includes(needle),
    )
    if (!hit) return false
    hit.setAttribute('data-shot', 'option')
    return true
  }, optionText)
  if (!option) {
    console.log(`  (no option "${optionText}")`)
    return false
  }
  await page.click('[data-shot="option"]')
  await wait(1200)
  return true
}

async function main() {
  const executablePath = CHROME_CANDIDATES.find((path) => existsSync(path))
  if (!executablePath) throw new Error('Chrome not found; set CHROME_PATH')
  if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true })

  // The cover uses the app's own lockup. It is not a screenshot, so nothing
  // else would put it next to the shots, and the deck would open with a broken
  // image where the logo goes.
  copyFileSync('src/assets/lockup-light.png', `${OUT}/lockup-light.png`)
  console.log('copied lockup-light')

  const browser = await puppeteer.launch({
    executablePath,
    headless: true,
    args: ['--no-sandbox'],
    ...(PROFILE ? { userDataDir: PROFILE } : {}),
    defaultViewport: { width: 1600, height: 1000, deviceScaleFactor: 2 },
  })
  const page = await browser.newPage()
  const resize = async (height) => {
    await page.setViewport({ width: 1600, height, deviceScaleFactor: 2 })
    await wait(800)
  }
  const shot = async (name) => {
    await page.screenshot({ path: `${OUT}/${name}.png` })
    console.log(`captured ${name}`)
  }

  await page.goto(BASE_URL, { waitUntil: 'networkidle2' })
  await page.evaluate((theme) => {
    localStorage.setItem('sparquet-studio:theme', theme)
    const raw = localStorage.getItem('sparquet-studio:settings')
    const parsed = raw ? JSON.parse(raw) : { state: {}, version: 1 }
    parsed.state = { ...parsed.state, theme }
    localStorage.setItem('sparquet-studio:settings', JSON.stringify(parsed))
  }, 'dark')
  await page.reload({ waitUntil: 'networkidle2' })
  await wait(1500)

  await page.goto(`${BASE_URL}/#/catalog`, { waitUntil: 'networkidle2' })
  await wait(1500)
  await clickText(page, 'Load example')
  await wait(4000)
  await page.reload({ waitUntil: 'networkidle2' })
  await wait(2500)

  await shot('catalog-list')

  await clickText(page, 'Lineage')
  await wait(2000)
  await resize(760)
  await fit(page)
  await shot('catalog-graph')

  await resize(1000)
  await clickText(page, 'Catalog')
  await wait(1500)
  // The starter workflow ships one of the medallion Jobs too, so the library
  // holds the same Job twice and every column trail states each hop twice.
  // Scoping to the example is what the screen is for, and it reads as intended.
  const scoped = await pickOption(page, 'All workflows', 'Medallion example')
  console.log('scoped to the example:', scoped)
  const opened = await page.evaluate(() => {
    const hit = document.querySelector('button[title="/lake/silver/orders"]')
    if (!hit) return false
    hit.click()
    return true
  })
  console.log('dataset opened:', opened)
  await wait(1800)
  await shot('dataset-sheet')

  await page.evaluate(() => {
    const hit = [...document.querySelectorAll('td button')].find(
      (node) => (node.textContent ?? '').trim() === 'amount',
    )
    hit?.click()
  })
  await wait(1200)
  await shot('dataset-column')

  // The sheet links back to the Job that writes the dataset.
  const toJob = await clickText(page, 'Bronze to silver', 'button, a')
  console.log('job opened:', toJob)
  await page.waitForSelector('.react-flow__node', { timeout: 20_000 }).catch(() => {})
  await wait(2500)
  await resize(720)
  await fit(page)
  await zoomIn(page, 2)
  await shot('canvas')
  await resize(1000)

  await page.evaluate(() => {
    const nodes = document.querySelectorAll('.react-flow__node')
    const node = nodes[3] ?? nodes[0]
    node?.dispatchEvent(new MouseEvent('click', { bubbles: true }))
  })
  await wait(1200)
  await shot('inspector')

  await page.keyboard.down('Control')
  await page.keyboard.press('KeyJ')
  await page.keyboard.up('Control')
  await wait(2000)
  await shot('json')

  await page.keyboard.down('Control')
  await page.keyboard.press('KeyJ')
  await page.keyboard.up('Control')
  await wait(1000)
  await page.keyboard.down('Control')
  await page.keyboard.press('Slash')
  await page.keyboard.up('Control')
  await wait(1500)
  await fit(page)
  await shot('ai')

  await page.goto(`${BASE_URL}/#/`, { waitUntil: 'domcontentloaded' })
  await wait(1800)
  await shot('overview')

  await page.goto(`${BASE_URL}/#/templates`, { waitUntil: 'domcontentloaded' })
  await wait(1800)
  await shot('templates')

  // SQL editor: click a dataset so the editor opens with a real query in it.
  await page.goto(`${BASE_URL}/#/sql`, { waitUntil: 'domcontentloaded' })
  await wait(2500)
  await page.evaluate(() => {
    const hit = document.querySelector('button[title^="/lake/silver/orders"]')
    hit?.click()
  })
  await wait(2000)
  await shot('sql')

  await browser.close()
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
