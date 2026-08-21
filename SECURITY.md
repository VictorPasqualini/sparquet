# Security Policy

> **PLACEHOLDER — MUST BE REPLACED BEFORE THIS REPOSITORY GOES PUBLIC.**
> The reporting address below is `SECURITY-CONTACT-TODO@example.invalid`, which does not
> exist. Replace it with a real, monitored address, and enable GitHub private
> vulnerability reporting (Settings → Code security and analysis → Private vulnerability
> reporting) so the link in this document actually works.

## Supported versions

Sparquet is pre-1.0 and released from `main`. Fixes land in a new patch or minor
release; there are no maintenance branches for older lines.

| Version | Supported |
|---|---|
| The latest release on [PyPI](https://pypi.org/project/sparquet/) | Yes — fixes go into the next release |
| Any earlier `0.x` | No — upgrade; there is no backporting before 1.0 |
| Anything published as `spark-framework` (up to `0.2.3`) | No — that distribution is retired; move to `sparquet` |

The data-quality engine ships separately as
[`sparquet-cola`](https://github.com/VictorPasqualini/sparquet-cola) (import
`sparquet_cola`). Report anything specific to validation checks in that repository.

Sparquet Studio (`sparquet-studio/`) is not published as a package: it is used from a
checkout of this repository, so "supported" means the current `main`.

## Reporting a vulnerability

**Do not open a public issue, discussion or pull request for a security problem.**

Two private channels:

1. **GitHub private vulnerability reporting** — the *Report a vulnerability* button
   under the [Security tab](https://github.com/VictorPasqualini/sparquet/security)
   of this repository. Preferred: it keeps the report, the fix and the advisory in one
   place.
2. **Email** — **`SECURITY-CONTACT-TODO@example.invalid`** *(placeholder — replace with
   a real address)*.

Please include:

- what an attacker can do, and the worst case if it is exploited;
- the affected component (framework, a specific connector, the Studio app, the local
  runner) and the version or commit;
- a minimal reproduction — a pipeline JSON with paths and credentials **redacted**, and
  the commands you ran;
- your environment: Python, PySpark and JDK versions, and where Spark ran.

What to expect: an acknowledgement within a few days, an assessment and a fix plan, a
release, and credit in the advisory unless you prefer otherwise. Please give us a
reasonable window to ship a fix before disclosing publicly.

## The local runner executes arbitrary Spark work

This is the sharpest edge in the project, and it is by design.

The Studio local runner (`sparquet-studio/server/`) takes a pipeline definition over
HTTP and executes it with the real `Sparquet`. That definition carries **arbitrary SQL,
arbitrary input paths and arbitrary output paths**, which run with your user's
permissions, on your machine, against every data store your machine can reach. It is,
functionally, remote code execution as a feature.

Therefore:

- **Never expose the runner to a network.** Keep it bound to `127.0.0.1` (the default).
  No public address, no tunnel, no reverse proxy, no container port published to a LAN.
- `POST /run` and `POST /validate` require the runner's token in the
  `X-Sparquet-Token` header, and refuse an `Origin` outside
  `SPARQUET_STUDIO_ORIGINS` with a `403`. Both checks are load-bearing: CORS alone
  protects nothing, because a cross-origin `POST` without a custom header is sent with
  no preflight — the browser would withhold the *response* from the attacker after the
  pipeline had already run. Do not weaken either check.
- Treat the token like a password. It is what stands between any web page you visit and
  code execution on your machine. Only widen `SPARQUET_STUDIO_ORIGINS` to origins you
  control.

The details, including the token lifecycle, are in
[`sparquet-studio/server/README.md`](sparquet-studio/server/README.md#security).

## Notes on what is and is not a vulnerability

- **A pipeline JSON is executable input.** `sql`, `filter`, `with_column` and friends
  carry SQL that is evaluated by Spark; `$include` reads files from disk; `params` are
  substituted into the raw JSON before parsing. Running a config you do not trust is
  equivalent to running code you do not trust. Treat pipeline files as source code, and
  do not build a service that accepts them from untrusted users.
- **Credentials belong in `options` / `spark.configs` supplied by your environment**,
  not committed into a pipeline JSON. A connector that logs a secret *is* a bug worth
  reporting.
- Reports that only require the reporter to run a malicious pipeline on their own
  machine, or to expose the local runner deliberately, are documented behaviour rather
  than vulnerabilities. Anything that escapes those boundaries — a missing check on the
  runner, a secret leaking into logs or into the validation report, a path traversal
  through `$include` beyond the intended directory — is one.
