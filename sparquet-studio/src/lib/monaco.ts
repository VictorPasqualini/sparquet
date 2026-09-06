/**
 * Monaco, bundled locally.
 *
 * @monaco-editor/react fetches the editor from a CDN by default, which breaks
 * the app offline and on air-gapped machines — exactly where a data tool tends
 * to run. Pointing the loader at the installed package keeps everything local;
 * the editor lands in its own chunk because this module is imported lazily.
 */

import { loader } from '@monaco-editor/react'
// The editor API plus the JSON contribution only: importing the `monaco-editor`
// barrel would pull every language grammar (~3 MB) for a JSON-only editor.
import * as monaco from 'monaco-editor/esm/vs/editor/editor.api'
import editorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker'
import jsonWorker from 'monaco-editor/esm/vs/language/json/json.worker?worker'
import 'monaco-editor/esm/vs/language/json/monaco.contribution'
// SQL is highlighting only — a basic language has no worker and no schema, so
// it costs one grammar rather than the whole language service.
import 'monaco-editor/esm/vs/basic-languages/sql/sql.contribution'
import 'monaco-editor/esm/vs/editor/contrib/folding/browser/folding.js'
import 'monaco-editor/esm/vs/editor/contrib/bracketMatching/browser/bracketMatching.js'
import 'monaco-editor/esm/vs/editor/contrib/find/browser/findController.js'
import 'monaco-editor/esm/vs/editor/contrib/comment/browser/comment.js'
import 'monaco-editor/esm/vs/editor/contrib/wordHighlighter/browser/wordHighlighter.js'
// The suggest widget is a contribution too, and the SQL editor completes table
// and column names out of the catalog — without these two the provider is
// registered and nothing ever shows it.
import 'monaco-editor/esm/vs/editor/contrib/suggest/browser/suggestController.js'
import 'monaco-editor/esm/vs/editor/contrib/snippet/browser/snippetController2.js'

let configured = false

export function configureMonaco(): void {
  if (configured) return
  configured = true

  window.MonacoEnvironment = {
    getWorker(_workerId: string, label: string) {
      return label === 'json' ? new jsonWorker() : new editorWorker()
    },
  }

  loader.config({ monaco })
}
