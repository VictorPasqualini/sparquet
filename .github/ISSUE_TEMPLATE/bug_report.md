---
name: Bug report
about: Something in the framework or in Studio does not behave as documented
title: "[bug] "
labels: bug
---

<!--
Before filing: check the reference for the behaviour you expected —
https://sparquet.dev/docs or CLAUDE.md in this repository.

REDACT before pasting: paths, table names, hostnames, tokens, credentials.
A pipeline JSON is fine to trim, but keep it runnable — a config that cannot be
reproduced usually cannot be fixed.
-->

## What happened

<!-- One or two sentences. What you expected, and what you got instead. -->

## Pipeline JSON (redacted)

```json

```

<!-- If you pass params / input_df / columns / input_view to fw.run(), show that call too. -->

```python

```

## Full traceback or log output

<!-- The whole thing, not just the last line. Sparquet logs structured JSON: include
the lines around the failure. If the run failed without raising, paste
result.error and result.summary(). -->

```
```

## Versions

| | |
|---|---|
| Sparquet | <!-- python -c "import sparquet; print(sparquet.__version__)" --> |
| sparquet-cola | <!-- python -c "import sparquet_cola; print(sparquet_cola.__version__)" — only if the bug is in a validation --> |
| PySpark | <!-- python -c "import pyspark; print(pyspark.__version__)" --> |
| Python | <!-- python -V --> |
| Studio | <!-- only if the bug is in the editor: commit or version in sparquet-studio/package.json --> |

## Environment

<!-- Delete what does not apply, and add the detail that matters. -->

- [ ] Local (`master=local[*]`) — OS: ______ , JDK: ______
- [ ] Databricks — runtime version: ______
- [ ] EMR — release: ______
- [ ] Dataproc — image: ______
- [ ] Synapse
- [ ] Other: ______

Format/connector involved (`delta`, `postgresql`, `kafka`, …): ______
Driver/connector JARs on the classpath, if the format needs them: ______

## Where it shows up

- [ ] Running the framework directly (`fw.run(...)` or the `sparquet` CLI)
- [ ] Studio canvas / compiler / linter
- [ ] Studio local runner (`sparquet-studio/server`)

## Anything you already tried

<!-- Workarounds, a smaller config that still fails, the transformation you removed to
make it pass. This is usually the most useful part of the report. -->
