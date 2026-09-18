/**
 * Where the framework's example pipelines are, for the tests that use them as
 * fixtures.
 *
 * The Studio's compiler is pinned to real pipeline JSON — the configs the
 * framework itself ships — and not only to JSON the Studio wrote. A test built
 * on its own output proves that the Studio agrees with itself, which is not the
 * property anyone wants from a compiler.
 *
 * That fixture set has to survive this project becoming a repository of its own,
 * where `../../../../examples/` is simply gone. So it is looked up in the order
 * that keeps working as the layout changes:
 *
 *   1. `SPARQUET_EXAMPLES_DIR`, for a CI job that already has them somewhere.
 *   2. The **installed** `sparquet` package, asked in Python. This is the one
 *      that matters after a split: the version pinned in the lockfile brings its
 *      own examples, so bumping the pin changes the fixtures, and a round-trip
 *      that stops holding says the compiler has fallen behind the framework.
 *   3. `examples/` next to this project, which is the monorepo today.
 *
 * When none answers, the tests skip rather than fail — a laptop with no Python
 * should still be able to run the suite — unless `SPARQUET_EXAMPLES_REQUIRED` is
 * set, which CI does: a skip there would be a green job proving nothing.
 */

import { execFileSync } from 'node:child_process'
import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

/** The interpreters worth trying, in the order a machine usually answers to. */
const PYTHONS = ['python', 'python3', 'py']

function isPopulated(dir: string): boolean {
  try {
    return statSync(dir).isDirectory() && readdirSync(dir).some((name) => name.endsWith('.json'))
  } catch {
    return false
  }
}

/** Asks the installed package where its examples are. Silent when there is none. */
function fromInstalledPackage(): string | null {
  for (const python of PYTHONS) {
    try {
      const printed = execFileSync(
        python,
        ['-c', 'import sparquet; print(sparquet.examples_path())'],
        { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], timeout: 20_000 },
      ).trim()
      if (printed && isPopulated(printed)) return printed
    } catch {
      // No such interpreter, no such package, or a version older than
      // `examples_path()`. Any of those simply means "not this way".
    }
  }
  return null
}

function fromRepository(): string | null {
  const dir = fileURLToPath(new URL('../../../examples/', import.meta.url))
  return isPopulated(dir) ? dir : null
}

export interface ExampleConfigs {
  /** The directory the configs were found in, for messages. */
  dir: string
  /** How it was found, so a failing test can say which fixture set it used. */
  source: 'env' | 'package' | 'repository'
  /** File names, sorted, `.json` only. */
  files: string[]
}

let resolved: ExampleConfigs | null | undefined

/**
 * The example configs, or `null` when this machine has none.
 *
 * Resolved once: the Python lookup spawns a process, and every test file that
 * wants the fixtures would otherwise pay for it again.
 */
export function exampleConfigs(): ExampleConfigs | null {
  if (resolved !== undefined) return resolved

  const fromEnv = process.env.SPARQUET_EXAMPLES_DIR?.trim()
  let dir: string | null = fromEnv && isPopulated(fromEnv) ? fromEnv : null
  let source: ExampleConfigs['source'] = 'env'

  if (!dir) {
    dir = fromInstalledPackage()
    source = 'package'
  }
  if (!dir) {
    dir = fromRepository()
    source = 'repository'
  }
  if (!dir) {
    resolved = null
    return resolved
  }

  resolved = {
    dir,
    source,
    files: readdirSync(dir)
      .filter((name) => name.endsWith('.json'))
      .sort(),
  }
  return resolved
}

/** Reads one of them. */
export function readExampleConfig(configs: ExampleConfigs, file: string): unknown {
  return JSON.parse(readFileSync(join(configs.dir, file), 'utf8'))
}

/** Whether a machine without the fixtures should fail instead of skipping. */
export function examplesRequired(): boolean {
  const flag = process.env.SPARQUET_EXAMPLES_REQUIRED?.trim().toLowerCase()
  return flag === '1' || flag === 'true' || flag === 'yes'
}

/** What to tell somebody whose machine has no fixtures at all. */
export const MISSING_EXAMPLES =
  'The framework example configs were not found. They are fixtures for these tests: ' +
  'install the pinned `sparquet` (pip install sparquet) so the package brings them, ' +
  'or point SPARQUET_EXAMPLES_DIR at a directory of example pipeline JSON.'
