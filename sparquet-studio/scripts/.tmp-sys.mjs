import { existsSync } from 'node:fs'
import process from 'node:process'
import puppeteer from 'puppeteer-core'

const CHROME = [process.env.CHROME_PATH, 'C:/Program Files/Google/Chrome/Application/chrome.exe', `${process.env.LOCALAPPDATA ?? ''}/Google/Chrome/Application/chrome.exe`].filter(Boolean)
const browser = await puppeteer.launch({ executablePath: CHROME.find((p) => existsSync(p)), headless: true, args: ['--no-sandbox'] })

const read = (page) =>
  page.evaluate(() => ({
    theme: document.documentElement.dataset.theme,
    bg: getComputedStyle(document.body).backgroundColor,
    storedKey: localStorage.getItem('sparquet-studio:theme'),
    inSettings: JSON.parse(localStorage.getItem('sparquet-studio:settings') || '{}')?.state?.theme ?? null,
  }))

for (const scheme of ['dark', 'light']) {
  const page = await browser.newPage()
  await page.emulateMediaFeatures([{ name: 'prefers-color-scheme', value: scheme }])
  await page.setViewport({ width: 1280, height: 800 })
  await page.goto('http://localhost:5273', { waitUntil: 'networkidle2' })
  await page.evaluate(async () => {
    const dbs = await indexedDB.databases()
    for (const d of dbs) if (d.name) indexedDB.deleteDatabase(d.name)
    localStorage.clear()
  })
  await page.goto('http://localhost:5273', { waitUntil: 'networkidle2' })
  await new Promise((r) => setTimeout(r, 2200))
  console.log(`OS ${scheme}  fresh  `, JSON.stringify(await read(page)))

  // live switch of the OS preference, still with no explicit choice
  await page.emulateMediaFeatures([{ name: 'prefers-color-scheme', value: scheme === 'dark' ? 'light' : 'dark' }])
  await new Promise((r) => setTimeout(r, 800))
  console.log(`OS flip        `, JSON.stringify(await read(page)))

  // explicit choice must win and persist
  await page.evaluate(() => {
    const btn = [...document.querySelectorAll('button')].find((b) => b.textContent.trim() === 'Dark')
    btn?.click()
  })
  await page.goto('http://localhost:5273/#/settings', { waitUntil: 'networkidle2' })
  await new Promise((r) => setTimeout(r, 1200))
  await page.evaluate(() => {
    const btn = [...document.querySelectorAll('button')].find((b) => b.textContent.trim() === 'Dark')
    btn?.click()
  })
  await new Promise((r) => setTimeout(r, 600))
  await page.reload({ waitUntil: 'networkidle2' })
  await new Promise((r) => setTimeout(r, 1500))
  console.log(`after picking dark`, JSON.stringify(await read(page)))
  await page.close()
}
await browser.close()
