<div align="center">

# Sparquet Studio

**The visual editor for data pipelines that are just JSON.**

Design a Spark pipeline on a canvas, let an AI draft it for you, and run it on your machine — the file it produces is plain [Sparquet](../README.md) JSON your jobs already understand.

[Quickstart](#quickstart) · [Why](#why-this-exists) · [Features](#features) · [Your first pipeline](#your-first-pipeline) · [AI assistant](#ai-assistant) · [Local runner](#local-runner) · [Architecture](#architecture) · [Contributing](#contributing)

![Sparquet Studio editor](docs/images/editor.png)

</div>

---

## Why this exists

Sparquet turns a data pipeline into a JSON document: an `input`, a list of `transformations`, a `validations` block and one or more `outputs`. That is a great runtime format — versionable, diffable, parameterizable — and an awkward authoring format. A 300-line config with nested joins is hard to read, easy to typo, and impossible to explain in a review.

Studio is the other half: **a canvas for the same document.** Nodes are the entries of the JSON, edges are the order they run in, and every edit compiles straight back to the file. Nothing is hidden, nothing is generated twice — what you see is what Spark executes.

If you know n8n, you already know the idea. This is that, for data engineering.

## Features

| | |
|---|---|
| **Visual pipeline editor** | Drag sources, transformations, validations and destinations onto a canvas. Joins and unions take a second input, so branching pipelines read like a diagram instead of nested JSON. |
| **Every Sparquet feature, typed** | All 20 transformations, 7 IO formats and 6 validators, with per-field help, defaults and the gotchas that only live in the framework source (positional `union`, `merge_keys`, `{{runtime}}` pushdown, dot-path `struct`, …). |
| **AI that writes pipelines** | Describe what you need and get a complete, valid pipeline back — or ask it to modify, explain, optimize or fix the one on screen. Bring your own key for Anthropic, OpenAI, Google or any OpenAI-compatible endpoint. |
| **Live linting** | 20+ rules run as you type: unreachable nodes, a `merge` write without `merge_keys`, a `{{var}}` no `collect` publishes, a `{param}` you never declared, `collect` before `checkpoint`, two sinks fighting over one path. |
| **Round-trip JSON** | Import an existing config, edit it visually, export it byte-for-byte usable. The compiler is covered by tests that round-trip the framework's own example configs. |
| **Run it locally** | An optional Python service executes the compiled JSON with the real `SparkFramework` and streams back counters, validation results, a data preview and the framework's structured logs. |
| **Templates and lessons** | Ten working pipelines, from "CSV to Parquet" to Delta merge and runtime pushdown, plus six lessons that teach the language through the canvas. |
| **Local-first** | Projects and workflows live in your browser (IndexedDB). No account, no server, no telemetry. Export the whole workspace as one JSON file whenever you want. |

## Quickstart

```bash
git clone https://github.com/<your-org>/sparquet.git
cd sparquet/sparquet-studio
npm install
npm run dev
```

Open <http://localhost:5273>. The first launch seeds a **Getting Started** project with working pipelines — open one and start editing.

Requirements: Node 18.18+ (Node 22 recommended). No API key and no Python needed to design pipelines; both are optional add-ons.

### Build for production

```bash
npm run build
npm run preview
```

`dist/` is a static bundle — it deploys to GitHub Pages, S3, Netlify or any static host as-is (the app uses hash routing, so no server rewrites are required).

## Your first pipeline

1. **Create a project.** Projects group related pipelines; one per domain keeps names short.
2. **Add a source.** Drag *CSV* out of the palette, then set its path in the inspector on the right.
3. **Add transformations.** Drop a `filter`, then a `select`. Connect them left to right — the order on the canvas is the order in the `transformations` array.
4. **Add a destination.** Drag *Parquet* to the canvas, connect the last transformation into it, choose the write mode.
5. **Read the JSON.** Press <kbd>Ctrl/⌘</kbd>+<kbd>J</kbd>. That is exactly the file Sparquet runs — copy it, download it, or commit it next to your job.
6. **Run it.** Press <kbd>Ctrl/⌘</kbd>+<kbd>Enter</kbd> and start the [local runner](#local-runner) when prompted.

Prefer to learn by reading? **Learn** in the sidebar has six lessons that follow the same path, each linked to a template you can open.

### Keyboard

| Shortcut | Action |
|---|---|
| <kbd>Ctrl/⌘</kbd>+<kbd>K</kbd> | Command palette |
| <kbd>Ctrl/⌘</kbd>+<kbd>S</kbd> | Save now (edits autosave anyway) |
| <kbd>Ctrl/⌘</kbd>+<kbd>Z</kbd> / <kbd>⇧</kbd>+<kbd>Ctrl/⌘</kbd>+<kbd>Z</kbd> | Undo / redo |
| <kbd>Ctrl/⌘</kbd>+<kbd>Enter</kbd> | Run panel |
| <kbd>Ctrl/⌘</kbd>+<kbd>J</kbd> | JSON panel |
| <kbd>Ctrl/⌘</kbd>+<kbd>/</kbd> | AI assistant |
| <kbd>Ctrl/⌘</kbd>+<kbd>E</kbd> | Issues |
| <kbd>⇧</kbd>+<kbd>Ctrl/⌘</kbd>+<kbd>L</kbd> | Auto-layout |
| <kbd>Ctrl/⌘</kbd>+<kbd>D</kbd> / <kbd>Del</kbd> | Duplicate / delete selection |

## AI assistant

![AI assistant](docs/images/ai-assistant.png)

The assistant knows the pipeline language because its system prompt is generated **from the same catalog that drives the forms** — it cannot drift from what the framework supports.

Ask it to:

- *"Read a CSV, drop duplicates on id and write Parquet partitioned by date"* — get a full pipeline.
- *"Add a data quality block that fails on nulls in id"* — get a modified version of what is on the canvas.
- *"Explain what this pipeline does"* — get a plain-English walkthrough.
- *"Fix the issues"* — hand it the lint output and the current graph.

Every proposal arrives as a card you review before applying, and applying it is a single undo away.

### Setting it up

**Settings → AI assistant**, pick a provider and paste a key:

| Provider | Get a key | Default model |
|---|---|---|
| Anthropic | <https://console.anthropic.com/settings/keys> | `claude-sonnet-4-5` |
| OpenAI | <https://platform.openai.com/api-keys> | `gpt-4.1` |
| Google | <https://aistudio.google.com/apikey> | `gemini-2.5-pro` |
| OpenAI-compatible | your own gateway, Ollama, vLLM… | free text |

The key is used to call the provider **directly from your browser** and is never sent anywhere else — there is no Sparquet server in the loop. By default it is kept in memory for the session only; "Remember key in this browser" stores it in `localStorage`, which is convenient on a personal machine and a bad idea on a shared one.

## Local runner

Designing pipelines needs nothing but the browser. Executing them needs Spark, so Studio ships a small FastAPI service you run yourself.

```bash
cd sparquet-studio
pip install -r server/requirements.txt
uvicorn server.main:app --port 8787
```

It must run from `sparquet-studio/` — the module inserts the repository root into `sys.path` so `spark_framework` resolves without installing anything. `pyspark` and a working `JAVA_HOME` are required for real runs; on Windows you also need `winutils.exe` and `HADOOP_HOME`, or Spark will hang the first time it touches the filesystem ([details](server/README.md#windows)).

The Run panel then gives you: row counts, duration, per-rule validation results, a 50-row preview of the output DataFrame, and the framework's own structured logs.

### The token

On startup the runner prints a token to its terminal:

```
========================================================================
Sparquet Studio runner token (this session only):
    S3yhI-6191J6wu2xz7bCX9YpafB0GOLo
Send it as the 'x-sparquet-token' header on /run and /validate, or set
SPARQUET_STUDIO_TOKEN to keep the same token across restarts.
========================================================================
```

Copy that value into **Settings → Local runner → Runner token** (or into the card the Run panel shows the first time a run comes back refused) and every run and validation carries it. It is stored with your other Studio settings in this browser and sent to nothing but the runner URL.

`POST /run` and `POST /validate` reject requests without it (`401`) and requests whose `Origin` is outside the allow-list (`403`); `SPARQUET_STUDIO_ORIGINS` widens that list when Studio is served from somewhere other than `localhost:5273`. `GET /health` stays open, which is how Studio detects the runner before it has a token.

Set `SPARQUET_STUDIO_TOKEN` before starting the runner to pin a token that survives restarts, instead of pasting a fresh one after every start.

> **Security.** The runner executes arbitrary Spark work — arbitrary SQL, arbitrary reads and writes on whatever this machine can reach. Without the token, any web page you visit while it is up could drive Spark on your machine: a cross-origin `POST` still executes even when the browser withholds the response. Treat the token as a password, keep the runner on `127.0.0.1` (the default), and never expose it to a network.

Endpoints: `GET /health`, `POST /run`, `POST /validate`, `GET /capabilities` (the live registries, so custom transformations registered at runtime show up). See [`server/README.md`](server/README.md).

## Templates

![Templates](docs/images/templates.png)

Ten pipelines that are also documentation — each one is real, valid JSON with the reasoning attached:

`CSV to Parquet` · `Ingestion with data quality` · `Join and runtime pushdown` · `Nested payload and multi-output` · `Delta merge (upsert)` · `Aggregation with group_by and pivot` · `Union and dedupe` · `Staging view handoff` · `Kafka publication` · `Parameterized pipeline`

## Architecture

```
src/
├─ catalog/      the pipeline language: every transformation, format and validator
│                with its fields, defaults, docs and gotchas. Drives the palette,
│                the inspector forms, the linter and the AI system prompt.
├─ lib/
│  ├─ compiler/  graph ⇄ JSON. compileGraph() and pipelineToGraph() are inverses,
│  │             proven by round-trip tests over the framework's own examples.
│  ├─ validation/ lintWorkflow(): the rules that run as you type.
│  ├─ ai/        provider-agnostic streaming client, prompt builder, proposal parser.
│  ├─ runner/    typed client for the local FastAPI service.
│  └─ storage/   IndexedDB persistence, export/import, migrations.
├─ store/        zustand: editor (graph, history, autosave), library, settings.
├─ components/   canvas (React Flow), panels (inspector, JSON, AI, run, issues), ui.
├─ screens/      dashboard, project, editor, templates, learn, settings.
└─ data/         starter templates, lessons, keyboard map.
```

**The graph is the source of truth while editing**; JSON is compiled from it on demand. Two rules make that safe:

- The *main* `transformations` are the longest chain every destination shares. Once the graph forks, each branch becomes that destination's own `transformations`.
- A `join` / `union` takes its `with` source from its **second** input, and everything between that source and the node becomes `with_transformations`.

Stack: React 18, TypeScript (strict), Vite 6, Tailwind CSS, React Flow (@xyflow/react), zustand, Radix UI, Monaco (bundled locally, so the editor works offline).

## Development

```bash
npm run dev         # dev server on :5273
npm run typecheck   # tsc, zero errors expected
npm run test        # vitest — compiler, linter, storage, AI parsing, runner client
npm run lint        # eslint, zero warnings expected
npm run smoke       # end-to-end pass in a real Chrome (needs Chrome installed)
npm run shots       # screenshot every screen in both themes, for review
npm run build       # production bundle in dist/
```

`npm run smoke` boots the app, opens a seeded workflow and asserts that the canvas renders nodes **and** edges, the palette adds nodes, the inspector binds, the JSON panel compiles and every route loads without console errors. Run it before opening a PR that touches the canvas.

### Adding a transformation

When the framework gains a transformation, Studio needs one entry:

1. Add a `TransformationDef` to `src/catalog/transformations.core.ts` or `transformations.advanced.ts` — the exact `type` string, its fields, examples and gotchas.
2. That is it. The palette, the inspector form, the linter's required-field checks and the AI's system prompt all read from the catalog.

Nodes with unknown types still import and round-trip untouched, so a config using a custom registered transformation is never destroyed by opening it here.

## Contributing

Issues and pull requests are welcome. Please keep `npm run typecheck`, `npm run test` and `npm run lint` clean, and run `npm run smoke` for canvas changes. New user-facing copy is written in English.

## License

MIT — see [LICENSE](../LICENSE).
