# Moving Studio into its own repository

Studio lives inside the framework repository today. Nothing in it requires that:
the framework is a pinned dependency, the test fixtures are read from the
installed package, and the runner finds the framework the way any Python program
finds a library. This document is the procedure for the day the directory
becomes a repository, and the list of what was done so that day is a move rather
than a port.

The motivation is the user, not the developer: somebody who wants to design
pipelines should install one thing and get an editor, without cloning a Spark
framework they will never open.

## What already holds the two apart

| Seam | Where | What it does |
|---|---|---|
| Version pin | `server/requirements.txt` + `server/compat.py` | The framework is a dependency with a range, not a sibling directory. `/health` reports a version outside the range and Settings shows the sentence. |
| Framework lookup | `server/main.py` (`_framework_root`, `_bootstrap_sys_path`) | Import first, `sys.path` second. A repository root above this directory is used only when it actually holds a `sparquet/` package, and `SPARQUET_FRAMEWORK_PATH` overrides everything. |
| Test fixtures | `src/test/exampleConfigs.ts` | The compiler round-trips the framework's example pipelines. They are found through `SPARQUET_EXAMPLES_DIR`, then `sparquet.examples_path()` in the installed package, then `examples/` next to the checkout. Only the third is the monorepo. |
| Examples in the wheel | `pyproject.toml` in the framework, `tests/test_packaging.py` | `pip install sparquet` carries the example configs, which is what makes the second lookup possible at all. |
| CI | `.github/workflows/ci.yml` **in this directory** | Inert today (GitHub only reads workflows at a repository root) and complete: web layer plus runner tests, installing the pinned framework for the fixtures. |

## The move

```bash
# 1. A branch that contains only this directory, with its history.
cd /path/to/sparquet
git subtree split --prefix=sparquet-studio -b studio-only

# 2. A repository of its own.
mkdir ../sparquet-studio-repo && cd ../sparquet-studio-repo
git init -b main
git pull ../sparquet studio-only

# 3. Point it at its remote.
git remote add origin https://github.com/<org>/sparquet-studio.git
git push -u origin main
```

`git subtree split` keeps every commit that touched the directory, rewritten with
paths relative to it — `sparquet-studio/src/App.tsx` becomes `src/App.tsx`. The
history of `git log --follow` survives; the history of the framework does not
come along, which is the point.

## After the move

1. **Verify before announcing.** In the new repository, with no framework
   checkout anywhere near it:

   ```bash
   npm ci && npm run typecheck && npm run lint
   pip install -r server/requirements.txt
   SPARQUET_EXAMPLES_REQUIRED=1 npm run test   # fixtures must come from the package
   npm run build && npm run smoke
   for suite in server/test_*.py; do python "$suite"; done
   ```

   `SPARQUET_EXAMPLES_REQUIRED=1` is what proves the decoupling: without a
   framework installed, those tests fail instead of skipping.

2. **Remove the `studio` job** from the framework's `.github/workflows/ci.yml`,
   and the `sparquet-studio/` paths from anything else at the framework root
   (`.gitignore`, packaging excludes, `docs/`).

3. **Keep the catalog rule.** A new transformation, format or validator still
   needs an entry in `src/catalog/` here and a docs PR in `sparquet-web`. Across
   repositories that becomes a checklist item on the framework's PR template
   rather than a thing the same commit does.

4. **Raise the pin deliberately.** When the framework releases a minor, bump
   `MINIMUM`/`BELOW` and `REQUIREMENT` in `server/compat.py`, update
   `server/requirements.txt` to match (`test_compat.py` enforces the pair), and
   run the compiler suite against the new version. A green round-trip is the
   evidence for the bump; there is no other one.

5. **Leave a pointer** in the framework's README where the Studio directory was,
   so a clone of the framework finds the editor.

## What deliberately stays behind

The framework's `examples/` are not vendored here. They are the fixtures that
hold the compiler to the JSON the framework actually executes, and a copy would
stop being that the first time somebody edited one on this side. They arrive
through the pinned package, which is exactly the coupling that should exist:
the Studio compiles for a version, and the version brings its own examples.
