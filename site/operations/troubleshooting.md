---
layout: default
title: Troubleshooting
parent: Operations
nav_order: 1
permalink: /operations/troubleshooting/
---

# Troubleshooting

Start with read-only evidence. Preserve the state directory and redact secrets
before sharing output.

## Collect a minimal report

```console
$ jobman --version
$ jobman config paths
$ jobman doctor --json
$ jobman list --all --json
$ jobman show --json JOB
$ jobman show evidence --json JOB
$ jobman show evidence --similar 5 --json JOB
```

The final form performs a bounded exact-match search over newly indexed local
failure fingerprints. It can distinguish a one-off failure from a recurring
factual pattern without exposing other jobs' commands or logs.

Include the operating system and architecture, exact command with secret values
removed, effective configuration source list, observed result, and expected
result in a bug report. Do not attach raw target logs unless you have reviewed
them for credentials and private data.

For a cited, read-only interpretation, install the independent
[`jobman-diagnose` companion](https://github.com/ryancswallace/jobman-diagnose)
and run `jobman diagnose JOB`. Its default deterministic mode is local and
network-free. Add `--logs tail` only after deciding that target output is
appropriate to include.

## Configuration fails to load

```console
$ jobman config validate
$ jobman config show --origins
```

Unknown and duplicate YAML keys are errors. Lists replace lower-precedence
lists rather than appending. A project `.jobman.yml` is ignored until its
canonical root is trusted by a user-controlled or explicit source.

Inspection, cancellation, `doctor`, `clean JOB`, `clean --all`, and explicit
`clean --older-than` remain available without valid configuration. Use them to
recover safely before repairing a configuration-authority command.

## A job remains queued or waiting

Inspect `show --json` for:

- unsatisfied dependency outcomes;
- wait-condition evaluations and diagnostics;
- `next_run_at` retry or repetition delay;
- concurrency admission and pool capacity;
- paused state; and
- whole-job timeout or cancellation intent.

Waiting prerequisites do not consume concurrency slots. A request larger than
its finite pool capacity is rejected rather than queued indefinitely.

## A job is `lost`

`lost` means Jobman cannot safely prove the target result or lifecycle owner.
It deliberately does not infer success after a crash boundary.

1. Preserve `doctor --json` and `show --json JOB` output.
2. Check whether the target process still exists outside Jobman's ownership.
3. Run `jobman doctor --repair` only after reviewing the report.
4. Submit a new job with `rerun` when repeating the command is safe.

Do not rewrite SQLite rows to force a preferred outcome.

## Logs are missing or incomplete

Check the run's log metadata in `show --json`: capture mode, availability,
integrity, recording health, sizes, pruning time, and diagnostic code.

- Capture may have been `none`, `stdout`, or `stderr`.
- Retention or rotation may have pruned bytes intentionally.
- A crash can leave an unindexed tail that Jobman will not present as valid
  ordered output.
- `clean` may have written a durable pruning tombstone.

## Pause, cancel, or input fails

Lifecycle operations revalidate process creation and boot identity. Failure can
mean the target already exited, the selector is ambiguous, the operation
conflicts with the current phase, or the native process primitive is no longer
available.

Live input additionally requires the active run to have been submitted with
`--stdin live`. Input is local, ephemeral, and never replayed to another run.

Review the [platform matrix]({{ site.baseurl }}/reference/platforms/) for native
differences.

## SQLite or filesystem errors

State must be private, local, non-symlinked, and compatible with SQLite WAL
locking. Jobman rejects known network and distributed filesystems. Correct
ownership or ACL problems on an offline copy; do not weaken the state root for
convenience.

For corruption or migration recovery, follow [Backup and recovery]({{ site.baseurl }}/operations/backup-recovery/).

## Ask for help

Use the [issue tracker](https://github.com/ryancswallace/jobman/issues) for
ordinary reproducible bugs. Report suspected vulnerabilities through the
[private security process]({{ site.baseurl }}/operations/security/#reporting-a-vulnerability).
