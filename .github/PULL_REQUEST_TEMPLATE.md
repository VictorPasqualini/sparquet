<!-- Keep it short. What changes, and why. Link the issue: Closes #123 -->

## What this changes

## Why

## Checklist

- [ ] The unit tests pass — every `tests/**/test_*.py` outside `case-of-success`, run as
      a script the way CI does: `PYTHONPATH=. python tests/io/test_connectors.py`, etc.
- [ ] Studio, if touched: `npm run typecheck`, `npm run test`, `npm run lint` are
      clean — plus `npm run smoke` for canvas changes.
- [ ] **JSON schema changed?** Catalog entry added or updated in
      `sparquet-studio/src/catalog/`, otherwise the editor cannot offer or describe it.
- [ ] **New capability?** An example pipeline under `examples/`, listed in
      `examples/README.md`.
- [ ] **Behaviour changed?** Docs pull request opened in
      [sparquet-web](https://github.com/VictorPasqualini/sparquet-web) (EN, PT and ES)
      — link it here: ______
- [ ] `CLAUDE.md` updated if the schema, the public API or a convention changed.
- [ ] Entry added under `## [Unreleased]` in `CHANGELOG.md`.
- [ ] `__version__` untouched — releases are cut separately
      ([docs/DEPLOY_PYPI.md](../docs/DEPLOY_PYPI.md)).

## Breaking changes

<!-- Any renamed key, changed default or removed field. Write "none" if there are
none, and say what a user has to edit in their JSON if there are. -->
