/**
 * Screenshot pass — captures the main screens in both themes for visual review.
 * Usage: node scripts/shots.mjs [--url http://localhost:5273] [--out ./shots]
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
const OUT = readArg('--out', 'shots')

const CHROME_CANDIDATES = [
  process.env.CHROME_PATH,
  'C:/Program Files/Google/Chrome/Application/chrome.exe',
  'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe',
  `${process.env.LOCALAPPDATA ?? ''}/Google/Chrome/Application/chrome.exe`,
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  '/usr/bin/google-chrome',
].filter(Boolean)

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

async function main() {
  const executablePath = CHROME_CANDIDATES.find((path) => existsSync(path))
  if (!executablePath) throw new Error('Chrome not found; set CHROME_PATH')
  if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true })

  const browser = await puppeteer.launch({
    executablePath,
    headless: true,
    args: ['--no-sandbox'],
    defaultViewport: { width: 1512, height: 900, deviceScaleFactor: 1 },
  })

  const page = await browser.newPage()
  await page.goto(BASE_URL, { waitUntil: 'networkidle2' })
  await page.evaluate(async () => {
    const databases = await indexedDB.databases()
    for (const database of databases) if (database.name) indexedDB.deleteDatabase(database.name)
    localStorage.clear()
  })
  await page.goto(BASE_URL, { waitUntil: 'networkidle2' })
  await page.waitForSelector('a[href*="jobs"]', { timeout: 20_000 })

  const shot = async (name) => {
    await page.screenshot({ path: `${OUT}/${name}.png` })
    console.log(`captured ${name}`)
  }

  const setTheme = async (theme) => {
    await page.evaluate((value) => {
      localStorage.setItem('sparquet-studio:theme', value)
      const raw = localStorage.getItem('sparquet-studio:settings')
      const parsed = raw ? JSON.parse(raw) : { state: {}, version: 1 }
      parsed.state = { ...parsed.state, theme: value }
      localStorage.setItem('sparquet-studio:settings', JSON.stringify(parsed))
      document.documentElement.dataset.theme = value
    }, theme)
    await page.reload({ waitUntil: 'networkidle2' })
    await wait(900)
  }

  const jobs = await page.$$eval('a[href*="jobs"]', (links) =>
    links.map((link) => link.getAttribute('href')),
  )
  const target = jobs[0]

  await shot('01-dashboard-dark')

  await page.goto(`${BASE_URL}/${target}`, { waitUntil: 'networkidle2' })
  await page.waitForSelector('.react-flow__edge-path', { timeout: 20_000 })
  await wait(900)
  await shot('02-editor-dark')

  // Select a node so the inspector fills in.
  await page.evaluate(() => {
    const nodes = document.querySelectorAll('.react-flow__node')
    const node = nodes[1] ?? nodes[0]
    node?.dispatchEvent(new MouseEvent('click', { bubbles: true }))
  })
  await wait(700)
  await shot('03-inspector-dark')

  // AI panel.
  await page.keyboard.down('Control')
  await page.keyboard.press('Slash')
  await page.keyboard.up('Control')
  await wait(900)
  await shot('04-ai-dark')

  // Run panel.
  await page.keyboard.down('Control')
  await page.keyboard.press('Enter')
  await page.keyboard.up('Control')
  await wait(1600)
  await shot('05-run-dark')

  // JSON panel.
  await page.keyboard.down('Control')
  await page.keyboard.press('KeyJ')
  await page.keyboard.up('Control')
  await wait(2200)
  await shot('06-json-dark')

  for (const [route, name] of [
    ['#/templates', '07-templates-dark'],
    ['#/learn', '08-learn-dark'],
    ['#/settings', '09-settings-dark'],
  ]) {
    await page.goto(`${BASE_URL}/${route}`, { waitUntil: 'networkidle2' })
    await wait(900)
    await shot(name)
  }

  await setTheme('light')
  await shot('10-settings-light')
  await page.goto(`${BASE_URL}/`, { waitUntil: 'networkidle2' })
  await wait(800)
  await shot('11-dashboard-light')
  await page.goto(`${BASE_URL}/${target}`, { waitUntil: 'networkidle2' })
  await page.waitForSelector('.react-flow__edge-path', { timeout: 20_000 })
  await wait(900)
  await shot('12-editor-light')

  await browser.close()
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
