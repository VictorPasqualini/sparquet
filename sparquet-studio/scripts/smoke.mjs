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

/**
 * `DEFAULT_RUNNER_URL` in `src/lib/runner/client.ts`. The runner is optional, and
 * the editors ask it for run history as soon as they open, so a refused or
 * unauthorized read there is expected here — Studio handles it in the UI. Console
 * errors from any OTHER origin still fail the run.
 */
const RUNNER_URL = 'http://127.0.0.1:8787'

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
      if (message.type() === 'error') {
        consoleErrors.push({ text: message.text(), url: message.location()?.url ?? '' })
      }
    })
    page.on('pageerror', (error) => consoleErrors.push({ text: String(error), url: '' }))

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

    const canvas = await page.evaluate(() => {
      const nodes = Array.from(document.querySelectorAll('.react-flow__node'))
      return {
        nodes: nodes.length,
        visible: nodes.filter((node) => getComputedStyle(node).visibility === 'visible').length,
        // Quality destinations are declarations, not chain members: they carry no
        // link by design, so they are excluded before asking whether the chain that
        // remains is connected.
        declarations: nodes.filter((node) => /quality destination/i.test(node.textContent ?? ''))
          .length,
        edges: document.querySelectorAll('.react-flow__edge-path').length,
      }
    })
    const chained = canvas.nodes - canvas.declarations
    check('nodes render', canvas.nodes > 0, `${canvas.nodes} nodes`)
    check('every node is visible', canvas.visible === canvas.nodes, `${canvas.visible}/${canvas.nodes}`)
    check(
      'edges render',
      canvas.edges >= chained - 1,
      `${canvas.edges} edges for ${chained} chained node(s)`,
    )

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

    /* ------------------------------------------- pipeline editor */

    // A pipeline is a second canvas with its own store and route, so prove it
    // mounts and accepts a stage rather than trusting that the screen renders.
    // The library now loads asynchronously — the workspace on the runner first,
    // browser storage only if nothing answers — so the seeded workflow appears a
    // tick after the page is idle rather than with the first paint.
    const workflowHref = await page
      .goto(BASE_URL, { waitUntil: 'networkidle2' })
      .then(() => page.waitForSelector('a[href*="workflows/"]', { timeout: 15_000 }))
      .then(() => page.$$eval('a[href*="workflows/"]', (links) => links[0]?.getAttribute('href')))
      .catch(() => null)

    // Hash routing: the href already carries `#/`, so append it verbatim.
    if (workflowHref) {
      await page.goto(`${BASE_URL}/${workflowHref}`, { waitUntil: 'networkidle2' })
      await page
        .waitForFunction(
          () =>
            Array.from(document.querySelectorAll('button')).some((candidate) =>
              /^new pipeline$/i.test(candidate.textContent?.trim() ?? ''),
            ),
          { timeout: 15_000 },
        )
        .catch(() => null)
    }

    const pipelineCreated = !workflowHref
      ? false
      : await page
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
      check(
        'pipeline can be created',
        false,
        workflowHref
          ? 'no "New pipeline" control on the workflow screen'
          : 'no seeded workflow to open',
      )
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

        // A stage is a doorway into the job it runs, and double-click is the gesture
        // people try first — it crosses two stores and a route, so prove it end to end.
        const staged = !mounted
          ? false
          : await page
              .evaluate(() => {
                const add = Array.from(document.querySelectorAll('button')).find((candidate) =>
                  / as a stage$/.test(candidate.getAttribute('aria-label') ?? ''),
                )
                if (!(add instanceof HTMLElement)) return false
                add.click()
                return true
              })
              .catch(() => false)

        const stageBox = !staged
          ? false
          : await page
              .waitForSelector('.react-flow__node', { timeout: 15_000 })
              .then(() => true)
              .catch(() => false)

        if (!stageBox) {
          check('double-clicking a stage opens its job', false, 'no stage could be added')
        } else {
          await page.click('.react-flow__node', { clickCount: 2 }).catch(() => null)
          const onJobRoute = await page
            .waitForFunction(() => location.hash.includes('/jobs/'), { timeout: 15_000 })
            .then(() => true)
            .catch(() => false)
          const at = await page.evaluate(() => location.hash).catch(() => '')
          check('double-clicking a stage opens its job', onJobRoute, onJobRoute ? '' : `at ${at}`)
        }
      }
    }

    /* ------------------------------ validations write onto the canvas */

    // The data-quality template carries `validations.report`. Importing it must draw
    // that report as a standalone DESTINATION box — not hide it in a settings form —
    // and the job's own destination must survive next to it.
    await page.goto(`${BASE_URL}/#/templates`, { waitUntil: 'networkidle2' })
    const opened = await page
      .waitForFunction(
        () =>
          Array.from(document.querySelectorAll('article')).some((card) =>
            /data quality/i.test(card.textContent ?? ''),
          ),
        { timeout: 15_000 },
      )
      .then(() =>
        page.evaluate(() => {
          const card = Array.from(document.querySelectorAll('article')).find((candidate) =>
            /data quality/i.test(candidate.textContent ?? ''),
          )
          const use = Array.from(card?.querySelectorAll('button') ?? []).find((button) =>
            /^use template$/i.test(button.textContent?.trim() ?? ''),
          )
          if (!(use instanceof HTMLElement)) return false
          use.click()
          return true
        }),
      )
      .catch(() => false)

    const createdFromTemplate = !opened
      ? false
      : await page
          .waitForFunction(
            () =>
              Array.from(document.querySelectorAll('button')).some(
                (button) =>
                  /^create job$/i.test(button.textContent?.trim() ?? '') &&
                  !button.hasAttribute('disabled'),
              ),
            { timeout: 10_000 },
          )
          .then(() =>
            page.evaluate(() => {
              const confirm = Array.from(document.querySelectorAll('button')).find((button) =>
                /^create job$/i.test(button.textContent?.trim() ?? ''),
              )
              if (!(confirm instanceof HTMLElement)) return false
              confirm.click()
              return true
            }),
          )
          .catch(() => false)

    const onTemplateJob = !createdFromTemplate
      ? false
      : await page
          .waitForFunction(() => location.hash.includes('/jobs/'), { timeout: 15_000 })
          .then(() => true)
          .catch(() => false)

    if (!onTemplateJob) {
      check('data-quality template opens as a job', false, 'template flow did not reach a job')
    } else {
      await page.waitForSelector('.react-flow__node', { timeout: 15_000 }).catch(() => null)
      const drawn = await page
        .waitForFunction(
          () => {
            const nodes = Array.from(document.querySelectorAll('.react-flow__node'))
            const text = (node) => node.textContent ?? ''
            // The sink's own subtitle is what marks a quality destination now.
            const side = nodes.filter((node) => /quality destination/i.test(text(node)))
            if (side.length === 0) return false
            return {
              side: side.length,
              nodes: nodes.length,
              // A declaration, not a chain member: it renders no handle at all, so
              // nothing can be wired into it and nothing can leave it.
              handles: side.reduce(
                (total, node) => total + node.querySelectorAll('.react-flow__handle').length,
                0,
              ),
              // The job's own destination has to still be there, beside the report.
              mainOutput: nodes.some((node) => text(node).includes('/data/curated/customers')),
              reportOutput: side.some((node) => text(node).includes('customers_report')),
            }
          },
          { timeout: 15_000 },
        )
        .then((handle) => handle.jsonValue())
        .catch(() => null)

      check(
        'the quality report is drawn as a standalone destination box',
        Boolean(drawn && drawn.side === 1),
        drawn ? `${drawn.side} quality box(es) among ${drawn.nodes} nodes` : 'none found',
      )
      // It takes a link IN, so the canvas shows which validations write it, and never
      // one OUT — the block writes the dataset and nothing reads it downstream.
      check(
        'the quality destination is anchored to the validations',
        Boolean(drawn && drawn.handles >= 1),
        drawn ? `${drawn.handles} handle(s) on it` : 'nothing drawn',
      )

      // The report is drawn BESIDE the job's own destination, never in place of it.
      check(
        'the main destination survives next to the quality destination',
        Boolean(drawn && drawn.mainOutput && drawn.reportOutput),
        drawn ? `main=${drawn.mainOutput} report=${drawn.reportOutput}` : 'nothing drawn',
      )

      // The three datasets come from the Quality section of the palette now, added
      // by a click like every other node.
      const beforeQuality = drawn ? drawn.nodes : 0
      const addedFromPalette = await page.evaluate(() => {
        const button = Array.from(document.querySelectorAll('button[data-palette-item]')).find(
          (element) => /quarantine.*invalid/i.test(element.textContent ?? ''),
        )
        if (!(button instanceof HTMLElement)) return false
        button.click()
        return true
      })
      const qualityBoxes = !addedFromPalette
        ? 0
        : await page
            .waitForFunction(
              (count) =>
                document.querySelectorAll('.react-flow__node').length > count &&
                Array.from(document.querySelectorAll('.react-flow__node')).filter((node) =>
                  /quality destination/i.test(node.textContent ?? ''),
                ).length,
              { timeout: 8000 },
              beforeQuality,
            )
            .then((handle) => handle.jsonValue())
            .catch(() => 0)

      check(
        'the Quality palette section adds a quarantine destination',
        qualityBoxes === 2,
        addedFromPalette
          ? `${qualityBoxes} quality box(es) after the click`
          : 'no palette entry matched',
      )
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
      ({ text, url }) =>
        !/favicon|monaco|Download the React DevTools/i.test(text) &&
        !url.startsWith(RUNNER_URL),
    )
    check(
      'no console errors',
      realErrors.length === 0,
      realErrors
        .slice(0, 3)
        .map(({ text }) => text)
        .join(' | '),
    )
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
