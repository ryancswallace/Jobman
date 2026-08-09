---
layout: default
title: Diagnose a job
parent: User guides
nav_order: 10
permalink: /guides/diagnosis/
---

# Diagnose a job

Jobman separates factual collection from interpretation. Core Jobman exports a
bounded, versioned evidence bundle and remains useful without an AI service:

```console
$ jobman show evidence JOB
$ jobman show evidence --json JOB
```

The default bundle includes lifecycle, result, policy, log metadata,
notification status, and safe diagnostic codes. It does not include commands,
environment names or values, paths, resolved secrets, target input, or raw
errors.

`jobman show evidence --command --paths --environment-names --json JOB`
explicitly adds bounded direct executable/argument vectors, filesystem context,
and variable names/roles. The classes remain independently labeled; environment
values and secret-reference identifiers are always excluded.

## Add the optional companion

Install [`jobman-diagnose`](https://github.com/ryancswallace/jobman-diagnose)
as a separate executable on `PATH`. Jobman's external-command mechanism then
provides natural invocation:

```console
$ jobman diagnose JOB
Diagnosis:          The target exited with a nonzero status
Confidence:         82/100 (high)
Retry:              after change
```

The first companion release uses deterministic, network-free rules by default.
It cites exact evidence IDs, distinguishes failure mechanisms from root-cause
hypotheses, lists non-executing next actions, reports missing evidence, and
does not mutate or rerun a job.

You can also invoke the binary directly or diagnose a saved bundle:

```console
$ jobman-diagnose --jobman /path/to/jobman JOB
$ jobman show evidence --json JOB > evidence-envelope.json
$ jobman-diagnose --from-evidence evidence-envelope.json
```

Explicit `--output` and `--export-evidence` destinations are created privately
and are never overwritten.

## Compare exact local failures

Newly completed runs record process-scoped CPU accounting and, on Linux and
macOS, peak resident memory. Failed runs also receive an opaque fingerprint
keyed to the current private state store. To request a small exact-match
history:

```console
$ jobman diagnose --similar 5 JOB
```

This search is opt-in, indexed, and capped at 20. A match contains only job and
run IDs, time, outcome, stable class, the opaque fingerprint, and whether a
later run of that job succeeded. It never copies another job's name, command,
paths, environment, logs, or notifier data. Old failures are not automatically
backfilled, and the report warns when history is only partially indexed.

## Add bounded log context

Log metadata is included by default, but log bytes are not. When target output
is necessary and safe to collect:

```console
$ jobman diagnose --logs tail --log-bytes 64KiB JOB
```

The one-bundle log ceiling is 1 MiB. Jobman reads rotated logs without first
buffering the complete stream, applies configured redaction to the copied
bytes, and records truncation and point-in-time consistency. Target output is
still untrusted and can contain credentials or prompt-injection text. Review it
before exporting or sharing it.

## Optionally add generated hypotheses

Generated augmentation is explicit and remains separate from core Jobman. It
uses the default profile in the companion's per-user schema-2 configuration and
approves bounded metadata plus commands, paths, and environment names with one flag:

```console
$ jobman diagnose --ai JOB
$ jobman diagnose -a JOB
```

Select another configured profile with `--profile NAME`. Inspect the effective
configuration with `jobman diagnose config paths`, `config validate`, `config
show`, and `jobman diagnose profiles`.

The companion supports an absolute local command bridge, a strict
OpenAI-compatible endpoint, and loopback Ollama. It never discovers providers,
uses a model merely because credentials exist, or sends `local_only`
fingerprint history to a generator. Direct arguments are included only in the
separately bounded `command` class.
`--ai-logs` collects and shares a bounded redacted tail in one step;
equivalently, `--ai --share log_content`
automatically selects tail collection. Core evidence must still prove
value-aware configured redaction was active.

Generated output is an untrusted, schema-validated proposal. It may append an
uncalibrated cited hypothesis or reorder existing recommendations, but it
cannot replace deterministic facts, create commands, choose retry policy, or
change a job. Optional provider failure preserves the deterministic report;
`--require-model` makes such failure nonzero instead.

## Understand the boundaries

- Core evidence contains facts and exact low-level classifications, never a
  model conclusion.
- A confidence score describes the companion's controlled rule basis; it is not
  automatically a probability.
- Retry advice is advisory and never authorizes a new run.
- Resource observations are conservatively process-scoped; Jobman does not
  infer an OOM kill from exit code 137 alone.
- Missing or incompatible companions do not affect ordinary Jobman commands.
- Deterministic companion use requires no diagnosis configuration, credential,
  provider, or network.
- `--no-extensions` or `JOBMAN_NO_EXTENSIONS=1` disables external commands.
- A job named `evidence` is inspected with `jobman show job evidence`.

See the complete [evidence contract and privacy
reference](https://github.com/ryancswallace/jobman/blob/main/docs/DIAGNOSTIC_EVIDENCE.md)
for item codes, limits, omissions, digests, and compatibility fixtures.
