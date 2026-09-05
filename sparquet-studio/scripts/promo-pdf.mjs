/**
 * Renders the promo deck (promo.html) to PDF. Page size matches the HTML's
 * 1080x1350 slides — the 4:5 that LinkedIn shows largest in the feed.
 */

import { existsSync } from 'node:fs'
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import process from 'node:process'

import puppeteer from 'puppeteer-core'

const args = process.argv.slice(2)
const readArg = (flag, fallback) => {
  const index = args.indexOf(flag)
  return index >= 0 ? args[index + 1] : fallback
}

const INPUT = resolve(readArg('--in', 'promo.html'))
const OUTPUT = resolve(readArg('--out', 'sparquet-studio.pdf'))

const CHROME_CANDIDATES = [
  process.env.CHROME_PATH,
  'C:/Program Files/Google/Chrome/Application/chrome.exe',
  'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe',
  `${process.env.LOCALAPPDATA ?? ''}/Google/Chrome/Application/chrome.exe`,
].filter(Boolean)

const executablePath = CHROME_CANDIDATES.find((path) => existsSync(path))
if (!executablePath) throw new Error('Chrome not found; set CHROME_PATH')

const browser = await puppeteer.launch({ executablePath, headless: true, args: ['--no-sandbox'] })
const page = await browser.newPage()
await page.goto(pathToFileURL(INPUT).href, { waitUntil: 'networkidle0' })
await page.pdf({
  path: OUTPUT,
  width: '1080px',
  height: '1350px',
  printBackground: true,
  preferCSSPageSize: false,
})
await browser.close()
console.log(`wrote ${OUTPUT}`)
