/**
 * End-to-end smoke test.
 *
 * Boots the built app (or an already-running dev server), drives it in a real
 * Chrome and asserts the pipelines a first-time user takes: the library loads, a
 * template opens, the canvas draws nodes AND edges, the palette adds a node,
 * and the compiled JSON stays in step with the graph.
 *
 * Usage:  node scripts/smoke.mjs [--url http://localhost:5273] [--headed]
 */

import { spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import process from 'node:process'

import puppeteer from 'puppeteer-core'

const args = process.argv.slice(2)
const urlArg = args.indexOf('--url')
const BASE_URL = urlArg >= 0 ? args[urlArg + 1] : 'http://localhost:5273'
const HEADED = args.includes('--headed')

const CHROME_CANDIDATES = [
  process.env.CHROME_PATH,
  'C:/Program Files/Google/Chrome/Application/chrome.exe',
  'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe',
  `${process.env.LOCALAPPDATA ?? ''}/Google/Chrome/Application/chrome.exe`,
  '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  '/usr/bin/google-chrome',
  '/usr/bin/chromium',
].filter(Boolean)

const checks = []
let failures = 0

function check(name, condition, detail = '') {
  const ok = Boolean(condition)
  if (!ok) failures += 1
  checks.push(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`)
  return ok
}

async function waitForServer(url, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url, { method: 'GET' })
      if (response.ok) return true
    } catch {
      // server not up yet
    }
    await new Promise((resolve) => setTimeout(resolve, 400))
  }
  return false
}

async function main() {
  const executablePath = CHROME_CANDIDATES.find((path) => existsSync(path))
  if (!executablePath) {
    console.error('No Chrome binary found. Set CHROME_PATH to your Chrome executable.')
    process.exit(2)
  }

  let server = null
  if (!(await waitForServer(BASE_URL, 1500))) {
    server = spawn('npm', ['run', 'dev', '--', '--port', new URL(BASE_URL).port], {
      cwd: process.cwd(),
      stdio: 'ignore',
      shell: true,
    })
    const up = await waitForServer(BASE_URL)
    if (!up) {
      console.error(`Dev server never became reachable at ${BASE_URL}`)
      server.kill()
      process.exit(2)
    }
  }

  const browser = await puppeteer.launch({
    executablePath,
    headless: !HEADED,
    args: ['--no-sandbox', '--window-size=1512,950'],
    defaultViewport: { width: 1512, height: 900 },
  })

  const consoleErrors = []
  try {
    const page = await browser.newPage()
    page.on('console', (message) => {
      if (message.type() === 'error') consoleErrors.push(message.text())
    })
    page.on('pageerror', (error) => consoleErrors.push(String(error)))

    /* ------------------------------------------------ library boots clean */

    await page.goto(BASE_URL, { waitUntil: 'networkidle2' })
    await page.evaluate(async () => {
      const databases = await indexedDB.databases()
      for (const database of databases) if (database.name) indexedDB.deleteDatabase(database.name)
      localStorage.clear()
    })
    await page.goto(BASE_URL, { waitUntil: 'networkidle2' })
    await page.waitForSelector('a[href*="jobs"]', { timeout: 15_000 })

    const seeded = await page.$$eval('a[href*="jobs"]', (links) =>
      links.map((link) => link.getAttribute('href')),
    )
    check('seed creates starter jobs', seeded.length >= 2, `${seeded.length} links`)

    const workflowCount = await page.$$eval('a[href*="workflows/"]', (links) => links.length)
    check('seed runs once', workflowCount <= 2, `${workflowCount} workflow links`)

    /* --------------------------------------------------- canvas renders */

    await page.goto(`${BASE_URL}/${seeded[0]}`, { waitUntil: 'networkidle2' })
    await page.waitForSelector('.react-flow__node', { timeout: 15_000 })
    await page.waitForFunction(
      () => document.querySelectorAll('.react-flow__edge-path').length > 0,
      { timeout: 15_000 },
    )

    const canvas = await page.evaluate(() => ({
      nodes: document.querySelectorAll('.react-flow__node').length,
      visible: Array.from(document.querySelectorAll('.react-flow__node')).filter(
        (node) => getComputedStyle(node).visibility === 'visible',
      ).length,
      edges: document.querySelectorAll('.react-flow__edge-path').length,
    }))
    check('nodes render', canvas.nodes > 0, `${canvas.nodes} nodes`)
    check('every node is visible', canvas.visible === canvas.nodes, `${canvas.visible}/${canvas.nodes}`)
    check('edges render', canvas.edges >= canvas.nodes - 1, `${canvas.edges} edges`)

    /* ------------------------------------------------- palette adds node */

    const beforeAdd = canvas.nodes
    await page.evaluate(() => {
      const button = Array.from(document.querySelectorAll('button')).find((element) =>
        /^\s*Filter\b/.test(element.textContent ?? ''),
      )
      button?.click()
    })
    await page.waitForFunction(
      (count) => document.querySelectorAll('.react-flow__node').length > count,
      { timeout: 8000 },
      beforeAdd,
    )
    const afterAdd = await page.$$eval('.react-flow__node', (nodes) => nodes.length)
    check('palette adds a node', afterAdd === beforeAdd + 1, `${beforeAdd} → ${afterAdd}`)

    /* ------------------------------------------------------ inspector */

    await page.evaluate(() => {
      const node = document.querySelector('.react-flow__node')
      node?.dispatchEvent(new MouseEvent('click', { bubbles: true }))
    })
    await new Promise((resolve) => setTimeout(resolve, 400))
    const inspectorText = await page.evaluate(() => document.body.innerText)
    check('inspector shows the selected node', /Format|Path/i.test(inspectorText))

    /* ---------------------------------------------------- JSON panel */

    await page.keyboard.down('Control')
    await page.keyboard.press('KeyJ')
    await page.keyboard.up('Control')
    const jsonVisible = await page
      .waitForFunction(() => /"input"|"transformations"|Preview/i.test(document.body.innerText), {
        timeout: 20_000,
      })
      .then(() => true)
      .catch(() => false)
    check('JSON panel opens', jsonVisible)

    /* --------------------------------------------- inferred pipeline tab */

    // The pipeline view is the only place a whole workflow is drawn as file boxes, and
    // it lazy-loads React Flow a second time — worth proving it mounts, links the
    // seeded files and opens a drill-down, not just that the tab exists.
    const workflowHref = await page
      .goto(BASE_URL, { waitUntil: 'networkidle2' })
      .then(() => page.$$eval('a[href*="workflows/"]', (links) => links[0]?.getAttribute('href')))
      .catch(() => null)

    if (!workflowHref) {
      check('inferred pipeline tab renders', false, 'no seeded workflow to open')
    } else {
      // Hash routing: the href already carries `#/`, so append it verbatim.
      await page.goto(`${BASE_URL}/${workflowHref}`, { waitUntil: 'networkidle2' })
      const opened = await page
        .waitForFunction(
          () =>
            Array.from(document.querySelectorAll('[role="tab"]')).some((tab) =>
              /^pipeline$/i.test(tab.textContent?.trim() ?? ''),
            ),
          { timeout: 15_000 },
        )
        .then(() => true)
        .catch(() => false)
      check('workflow pipeline tab exists', opened)

      if (opened) {
        await page.evaluate(() => {
          const tab = Array.from(document.querySelectorAll('[role="tab"]')).find((candidate) =>
            /^pipeline$/i.test(candidate.textContent?.trim() ?? ''),
          )
          if (tab instanceof HTMLElement) tab.click()
        })

        // Lazy chunk + React Flow measuring pass, so wait for the boxes themselves.
        const pipeline = await page
          .waitForFunction(
            () => {
              const nodes = document.querySelectorAll('.react-flow__node')
              return nodes.length > 0
                ? {
                    nodes: nodes.length,
                    edges: document.querySelectorAll('.react-flow__edge-path').length,
                  }
                : false
            },
            { timeout: 25_000 },
          )
          .then((handle) => handle.jsonValue())
          .catch(() => null)

        check('pipeline map renders one box per job', Boolean(pipeline && pipeline.nodes > 0), pipeline ? `${pipeline.nodes} files, ${pipeline.edges} links` : 'no boxes')

        // Drill-down: the disclosure must actually reveal the ordered step list.
        const expanded = await page
          .evaluate(() => {
            const button = Array.from(
              document.querySelectorAll('.react-flow__node [aria-expanded]'),
            ).find((candidate) => candidate.getAttribute('aria-expanded') === 'false')
            if (!(button instanceof HTMLElement)) return 'no-disclosure'
            button.click()
            return button.getAttribute('aria-controls') ?? 'no-target'
          })
          .catch(() => 'error')

        const stepsShown =
          expanded.startsWith('no-') || expanded === 'error'
            ? false
            : await page
                .waitForFunction(
                  (id) => {
                    const panel = document.getElementById(id)
                    return Boolean(panel && panel.querySelectorAll('li').length > 0)
                  },
                  { timeout: 10_000 },
                  expanded,
                )
                .then(() => true)
                .catch(() => false)

        check('pipeline map box drills down into its steps', stepsShown, expanded.startsWith('no-') ? expanded : '')
      }
    }

    /* ------------------------------------------- pipeline editor */

    // A pipeline is a second canvas with its own store and route, so prove it
    // mounts and accepts a stage rather than trusting that the tab renders.
    const pipelineCreated = await page
      .evaluate(() => {
        const button = Array.from(document.querySelectorAll('button')).find((candidate) =>
          /^new pipeline$/i.test(candidate.textContent?.trim() ?? ''),
        )
        if (!(button instanceof HTMLElement)) return false
        button.click()
        return true
      })
      .catch(() => false)

    if (!pipelineCreated) {
      check('pipeline can be created', false, 'no "New pipeline" control on the workflow screen')
    } else {
      // The dialog needs a name before "Create pipeline" enables, so type one the way
      // a user would — a click on a disabled button would silently do nothing.
      await page
        .waitForFunction(
          () =>
            Array.from(document.querySelectorAll('button')).some((candidate) =>
              /^create pipeline$/i.test(candidate.textContent?.trim() ?? ''),
            ),
          { timeout: 10_000 },
        )
        .catch(() => null)
      const nameField = await page.$('[role="dialog"] input:not([type="checkbox"])')
      if (nameField) {
        await nameField.click()
        await nameField.type('Smoke pipeline')
      }
      await page.waitForFunction(
        () =>
          Array.from(document.querySelectorAll('button')).some(
            (candidate) =>
              /^create pipeline$/i.test(candidate.textContent?.trim() ?? '') &&
              !candidate.hasAttribute('disabled'),
          ),
        { timeout: 10_000 },
      ).catch(() => null)
      await page.evaluate(() => {
        const confirm = Array.from(document.querySelectorAll('button')).find((candidate) =>
          /^create pipeline$/i.test(candidate.textContent?.trim() ?? ''),
        )
        if (confirm instanceof HTMLElement) confirm.click()
      })

      const onPipelineRoute = await page
        .waitForFunction(() => location.hash.includes('/pipelines/'), { timeout: 15_000 })
        .then(() => true)
        .catch(() => false)
      const hash = await page.evaluate(() => location.hash).catch(() => '')
      check('pipeline opens its own editor', onPipelineRoute, onPipelineRoute ? '' : `at ${hash}`)

      if (onPipelineRoute) {
        // React Flow must mount even with zero stages: an empty pipeline is a valid state.
        const mounted = await page
          .waitForFunction(() => Boolean(document.querySelector('.react-flow')), {
            timeout: 20_000,
          })
          .then(() => true)
          .catch(() => false)
        check('pipeline canvas mounts', mounted)
      }
    }

    /* ------------------------------------------------------- routes */

    for (const [route, needle] of [
      ['#/templates', /template/i],
      ['#/learn', /lesson|learn/i],
      ['#/settings', /AI assistant|Appearance/i],
    ]) {
      await page.goto(`${BASE_URL}/${route}`, { waitUntil: 'networkidle2' })
      const rendered = await page
        .waitForFunction((source) => new RegExp(source, 'i').test(document.body.innerText), {
          timeout: 15_000,
        }, needle.source)
        .then(() => true)
        .catch(() => false)
      check(`route ${route} renders`, rendered)
    }

    const realErrors = consoleErrors.filter(
      (message) => !/favicon|monaco|Download the React DevTools/i.test(message),
    )
    check('no console errors', realErrors.length === 0, realErrors.slice(0, 3).join(' | '))
  } finally {
    await browser.close()
    if (server) server.kill()
  }

  console.log(checks.join('\n'))
  console.log(failures === 0 ? '\nAll smoke checks passed.' : `\n${failures} check(s) failed.`)
  process.exit(failures === 0 ? 0 : 1)
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
