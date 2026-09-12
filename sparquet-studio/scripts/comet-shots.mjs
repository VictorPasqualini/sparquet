/**
 * Captures the Studio screens the DataFusion Comet deck uses.
 *
 * The deck's claim is a comparison, so the shots have to come from two runners:
 * `spark.plugins` is read when the SparkSession is created and the runner keeps
 * one session per process, so the plain side and the Comet side are two
 * processes on two ports. This script points the Studio at one, runs the Job
 * that belongs to it, then points it at the other — which is also the honest
 * way to shoot it, because that is what a person would have to do.
 *
 * Both runners share one workspace directory and one history database, so the
 * Runs tab at the end lists the pair next to each other.
 *
 *   node scripts/comet-shots.mjs --out ../docs/promo/comet \
 *     --plain http://localhost:8789 --comet http://localhost:8788 --token dev-local-token
 */

import { existsSync, mkdirSync } from 'node:fs'
import process from 'node:process'

import puppeteer from 'puppeteer-core'

const args = process.argv.slice(2)
const readArg = (flag, fallback) => {
  const index = args.indexOf(flag)
  return index >= 0 ? args[index + 1] : fallback
}

const BASE_URL = readArg('--url', 'http://localhost:5273')
const OUT = readArg('--out', 'comet-shots')
const PLAIN_URL = readArg('--plain', 'http://localhost:8789')
const COMET_URL = readArg('--comet', 'http://localhost:8788')
const TOKEN = readArg('--token', process.env.SPARQUET_STUDIO_TOKEN ?? '')
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

/**
 * Clicks the button whose whole label is `text`.
 *
 * The panel tab reading "Run" and the workspace tab reading "Runs" are both
 * buttons, and a substring match hits the wrong one — which silently shows the
 * history instead of the run panel.
 */
async function clickExact(page, text, selector = 'button') {
  const clicked = await page.evaluate(
    (needle, query) => {
      const hit = [...document.querySelectorAll(query)].find(
        (node) => (node.textContent ?? '').trim() === needle,
      )
      if (!hit) return false
      hit.click()
      return true
    },
    text,
    selector,
  )
  if (!clicked) console.log(`  (no ${selector} labelled exactly "${text}")`)
  return clicked
}

/**
 * Switches the middle pane.
 *
 * Scoped to `[role="tab"]` on purpose: the node palette holds a button labelled
 * exactly "JSON" too, and clicking that one adds a source node to the graph
 * instead of showing the pipeline JSON — it edits the Job being photographed.
 */
async function clickWorkspaceTab(page, text) {
  return clickExact(page, text, '[role="tab"]')
}

async function fit(page) {
  await page.evaluate(() => document.querySelector('.react-flow__controls-fitview')?.click())
  await wait(1200)
}

async function zoomIn(page, times) {
  for (let index = 0; index < times; index += 1) {
    await page.evaluate(() => document.querySelector('.react-flow__controls-zoomin')?.click())
    await wait(400)
  }
}

/** Frames the graph the way the deck wants it: short, fitted, then zoomed in. */
async function frameCanvas(page, resize) {
  await resize(760)
  await fit(page)
  await zoomIn(page, 2)
}

/**
 * Writes the runner the Studio talks to, then reloads.
 *
 * The library itself lives on that runner's workspace, so this also decides
 * which Jobs the Studio can see — here both runners share one directory, which
 * is why switching ports keeps the same two Jobs.
 */
async function useRunner(page, runnerUrl) {
  await page.evaluate(
    (url, token) => {
      const raw = localStorage.getItem('sparquet-studio:settings')
      const parsed = raw ? JSON.parse(raw) : { state: {}, version: 1 }
      parsed.state = { ...parsed.state, theme: 'dark', runnerUrl: url, runnerToken: token }
      localStorage.setItem('sparquet-studio:settings', JSON.stringify(parsed))
      localStorage.setItem('sparquet-studio:theme', 'dark')
    },
    runnerUrl,
    TOKEN,
  )
  await page.reload({ waitUntil: 'networkidle2' })
  await wait(2500)
  console.log(`runner: ${runnerUrl}`)
}

/**
 * Fires the run and waits for the panel to settle on a result.
 *
 * The side panel opens on the Inspector, so the Run tab has to be selected
 * before the button that starts anything exists in the DOM.
 */
async function runJob(page, timeoutMs = 180_000) {
  await clickExact(page, 'Run')
  await wait(1200)
  await clickText(page, 'Run pipeline')
  await wait(1500)
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const state = await page.evaluate(() => {
      const text = document.body.textContent ?? ''
      return { running: text.includes('Running…'), done: text.includes('Duration') }
    })
    if (!state.running && state.done) return true
    await wait(1000)
  }
  console.log('  (run did not settle before the timeout)')
  return false
}

async function main() {
  const executablePath = CHROME_CANDIDATES.find((path) => existsSync(path))
  if (!executablePath) throw new Error('Chrome not found; set CHROME_PATH')
  if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true })

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

  /* ---------------------------------------------------------- plain Spark */

  await useRunner(page, PLAIN_URL)
  await page.goto(`${BASE_URL}/#/jobs/agregacao-spark`, { waitUntil: 'networkidle2' })
  await page.waitForSelector('.react-flow__node', { timeout: 30_000 }).catch(() => {})
  await wait(2500)
  await frameCanvas(page, resize)
  await shot('canvas-spark')
  await resize(1000)

  await runJob(page)
  await wait(1200)
  await shot('run-spark')

  await clickWorkspaceTab(page, 'Runs')
  await wait(3000)
  await shot('runs-spark')
  await clickWorkspaceTab(page, 'Flow')
  await wait(1000)

  /* -------------------------------------------------------------- Comet */

  await useRunner(page, COMET_URL)
  await page.goto(`${BASE_URL}/#/jobs/agregacao-comet`, { waitUntil: 'networkidle2' })
  await page.waitForSelector('.react-flow__node', { timeout: 30_000 }).catch(() => {})
  await wait(2500)
  await frameCanvas(page, resize)
  await shot('canvas-comet')
  await resize(1000)

  // The JSON tab is the proof that the two Jobs differ only in Spark configs.
  await clickWorkspaceTab(page, 'JSON')
  await wait(2500)
  await shot('json-comet')

  await clickWorkspaceTab(page, 'Flow')
  await wait(1500)
  await runJob(page)
  await wait(1200)
  await shot('run-comet')

  // The Runs tab is per Job, so the pair is two shots — which is also the
  // fairer picture: each row is a real execution of that Job, not an average.
  await clickWorkspaceTab(page, 'Runs')
  await wait(3000)
  await shot('runs-comet')

  await browser.close()
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
