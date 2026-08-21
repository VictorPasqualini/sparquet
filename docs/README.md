# docs/

This folder holds the documents that belong next to the code and nowhere else.

| Document | What it covers |
|---|---|
| [DEPLOY_PYPI.md](DEPLOY_PYPI.md) | Building and publishing the framework to PyPI: version bump, `python -m build`, `twine`, the GitHub Actions release pipeline and Trusted Publishing. In Portuguese. |

Everything else lives outside this folder:

| Looking for | Go to |
|---|---|
| The full JSON schema, the public API, the conventions | [CLAUDE.md](../CLAUDE.md), at the repository root — the reference kept in sync with the code |
| Guides and reference for users, in English, Portuguese and Spanish | <https://sparquet.dev>, built from the separate [sparquet-web](https://github.com/VictorPasqualini/sparquet-web) repository |
| Working pipelines to copy | [examples/](../examples/) |
| What the test suite covers, and what it does not | [docs/TEST_PLAN.md](TEST_PLAN.md) |
| Studio: install, first Job, AI, local runner | [sparquet-studio/README.md](../sparquet-studio/README.md) |
| Data quality: checks, thresholds, the engine itself | [sparquet-cola](https://github.com/VictorPasqualini/sparquet-cola) — a separate package (`pip install sparquet-cola`, import `sparquet_cola`) that the framework depends on |
| What is planned | [BACKLOG.md](../BACKLOG.md) |
| Release history | [CHANGELOG.md](../CHANGELOG.md) |
| How to contribute | [CONTRIBUTING.md](../CONTRIBUTING.md) |

> This file used to carry a full schema reference of its own. It was removed rather
> than left to rot: the schema is now documented once in
> [CLAUDE.md](../CLAUDE.md) and once, for users, on <https://sparquet.dev>.
