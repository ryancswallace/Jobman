---
layout: default
title: Selectors, output, and exit status
parent: Reference
nav_order: 3
permalink: /reference/output/
---

# Selectors, output, and exit status

This page summarizes the stable CLI surfaces intended for automation. The
[compatibility contract]({{ site.baseurl }}/reference/compatibility/) is the
authoritative v1 policy.

## Job selectors

Commands that accept `JOB` resolve selectors in this order:

1. a complete canonical UUIDv7 job ID;
2. a unique ID prefix of at least eight characters; or
3. an unambiguous exact display name.

Scripts should retain the complete ID printed by `jobman run`. Names and
prefixes are conveniences for interactive use and can become ambiguous after
more jobs are submitted.

## Run selectors

Commands supporting a run selection accept a positive run number or negative
index. `-1` is the latest run, `-2` is the preceding run, and so on. Run number
zero is invalid.

Supported inspection forms include:

```console
$ jobman show JOB
$ jobman show job JOB
$ jobman show run JOB -1
$ jobman logs --run -1 JOB
```

## JSON envelope

Machine-readable inspection uses one object with a version and payload:

```json
{
  "schema_version": 1,
  "data": {}
}
```

Existing fields do not change meaning or type in compatible v1 patch and minor
releases. Minor releases may add fields, so consumers must ignore unknown
fields. Human wording and column alignment are not stable interfaces.

Times use UTC RFC3339 with nanosecond precision when needed. IDs are opaque
lowercase UUIDv7 strings.

Commands with JSON output include `list --json`, `status --json`,
`show --json`, `show evidence --json`, and `doctor --json`. `config show` emits
effective configuration as JSON and can include origins.

Diagnostic evidence adds a sealed value under `data.evidence`. Its nested
`kind`, `schema_version`, and `evidence_id` form an independently versioned
contract. See the [diagnostic evidence guide]({{ site.baseurl }}/guides/diagnosis/)
for selection, limits, and disclosure behavior.
Resource observations remain ordinary metadata. Store-local failure
fingerprints and exact-match summaries use the `local_only` disclosure class
and appear only when available or explicitly requested with `--similar`.

## Process exit status

| Status | Meaning |
| --- | --- |
| `0` | The command operation succeeded. |
| `1` | Internal or uncategorized failure. |
| `2` | Usage or validation failure. |
| `3` | No matching job or run. |
| `4` | Ambiguous selector. |
| `5` | Lifecycle or durable-state conflict. |
| `6` | Partial live-input delivery. |

The exit status of `wait`, `run --wait`, or `run --foreground` also reflects
the managed lifecycle contract. Consult command help when distinguishing a
Jobman operation error from a target job outcome.

## Standard streams

Normal results go to stdout. Diagnostics go to stderr. Structured output is
not mixed with progress decoration. Captured target logs are read explicitly
with `logs`; detached targets do not inherit the submitting terminal's output.

## Redaction

Configured redaction applies to Jobman diagnostics and structured output.
Captured target stdout and stderr remain raw. Secret references are persisted;
resolved secret values are not included in the immutable job specification.
Bounded evidence log tails are copied only when explicitly requested and are
redacted before the evidence bundle is sealed.
