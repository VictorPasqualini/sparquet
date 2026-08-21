---
name: Feature request
about: A new transformation, connector, validation or Studio capability
title: "[feat] "
labels: enhancement
---

<!--
Check BACKLOG.md first — a fair amount is already planned there, with the reasoning
and the decisions already taken (for example: streaming stays out of scope for now,
Cassandra and ScyllaDB share one connector).

Anything that changes the JSON schema should start as an issue like this one, before
the code: the JSON is a published contract shared by the framework, the Studio
compiler, the linter and the docs site.
-->

## The problem

<!-- What you are trying to do, and what the language makes you do instead today.
Concrete beats abstract: the job you are writing, the SQL you had to hand-roll, the
step you cannot express. -->

## What you would like

<!-- If it changes the JSON, sketch it. This is the part that gets discussed. -->

```json
{
  "type": "",
}
```

## What it would take

<!-- Delete what does not apply. -->

- [ ] New transformation (`sparquet/transform/builtin.py` + engine registry)
- [ ] New IO format (a `BaseReader`/`BaseWriter` pair + the factory registry)
- [ ] New validation — note that the engine lives in the separate
      [`sparquet-cola`](https://github.com/VictorPasqualini/sparquet-cola) repository
- [ ] Change to an existing field, default or name (**breaking?** say so)
- [ ] Studio only (canvas, linter, AI, runner)
- [ ] Documentation only — the docs live in
      [`sparquet-web`](https://github.com/VictorPasqualini/sparquet-web) (EN/PT/ES)

Remember that a new transformation, format or validator also needs a catalog entry in
`sparquet-studio/src/catalog/` and an example under `examples/` — see
[CONTRIBUTING.md](../../CONTRIBUTING.md).

## Can it be done today with an extension?

<!-- register_transformation / register_reader / register_writer / register_validator
cover domain-specific needs without changing the core. If you tried that and it did
not fit, say why — that is a strong argument for the feature. -->

## Anything else

<!-- Prior art in dbt, SODA, Spark itself; a link to the connector's docs; whether you
are willing to send the pull request. -->
