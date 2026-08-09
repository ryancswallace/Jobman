# v1 compatibility contract

This document defines the compatibility surface for the Jobman v1 release
line. Changes to this contract follow the deprecation and versioning rules
below and must update this file, the specification, command help, and contract
fixtures together.

## Supported public surfaces

The supported product API is the `jobman` command-line interface. The root Go
package intentionally exposes only:

- `NewCommand()`, which constructs an independent production Cobra tree;
- `Execute()`, the process-global CLI entry point; and
- `ExitCode(error)`, the stable error-to-process-status mapping.

The separate public `diagnostic` Go package is a standard-library-only data
contract for sealed evidence schema 1. Its concrete types and documented
codec functions are supported independently of Jobman's private backend and
store types.

Backend injection and internal model/store types are private implementation
seams. They are not a general-purpose Jobman SDK. A future SDK requires its own
documented, externally implementable types and a separate compatibility review.

The CLI contract includes documented command and flag names, selector order,
exit statuses, JSON schema version 1, configuration schema version 1,
notification event schema version 1, immutable job-spec schema version 2, and
forward migrations for the SQLite state schema. Diagnostic evidence has its
own `kind` and `schema_version` nested inside the CLI envelope.

## Command contract

- `show JOB`, `show job JOB`, and `show run JOB RUN` are supported. Negative
  run indexes count backward, with `-1` selecting the latest run.
- `cancel JOB` and `cancel job JOB` cancel the job. `cancel run JOB RUN`
  requires that RUN is active and, in v1, also cancels the owning job so no
  retry follows.
- `list` supports bounded phase, outcome, name, group, active/completed, and
  RFC3339 submission-time filters. `--all` means the documented store maximum
  of 1000, not unbounded memory use.
- Read/inspection and emergency operations do not require valid configuration
  and never apply store-wide settings. `config apply`, `run`, `rerun`, and
  policy-based `clean` are the explicit configuration-authority paths.
- `clean` is a dry run unless `--force` is present. `clean --all` selects every
  completed job and its logs independently of retention policy, while retaining
  dependency, notification, admission, and active-state safety checks. A job
  selector applies the same behavior to one completed job and may be combined
  with `--older-than`; selecting a nonterminal job returns a conflict error in
  both dry-run and forced modes.
- `doctor` is configuration-independent. `--repair` authorizes WAL checkpoint,
  stale lifecycle reconciliation, and due-notification recovery; `--backup`
  writes a new consistent SQLite snapshot.
- `show evidence JOB` returns a bounded factual snapshot. `--run` and
  `--all-runs` select run history; log content is absent unless `--logs tail`
  is explicit; direct command argument vectors, paths, and environment names
  are absent unless their respective collection flags are explicit. Environment
  values and secret-reference identifiers remain excluded. Evidence schema 1 item codes
  may grow additively, so consumers must ignore unknown codes while rejecting
  unsupported required schemas.
- An unknown lowercase command can resolve to the exact `jobman-NAME`
  executable on `PATH`. Built-ins always win. `--no-extensions` and
  `JOBMAN_NO_EXTENSIONS=1` disable dispatch, and help or completion never
  enumerates extensions. Extension protocol 1 is documented in ADR-0003.

## JSON and errors

Machine output is a single JSON object with `schema_version: 1` and `data`.
Existing fields do not change meaning or type in v1 patch/minor releases. Minor
releases may add fields. Consumers must ignore unknown fields. A breaking field
change requires a new envelope schema selected by a documented mechanism.

Times are UTC RFC3339 with nanosecond precision when needed. IDs remain opaque
lowercase UUIDv7 strings. Human wording and column spacing are not stable
machine interfaces.

Stable process statuses are:

| Status | Meaning |
| --- | --- |
| 0 | Success. |
| 1 | Internal or uncategorized failure. |
| 2 | Command usage or validation failure. |
| 3 | No matching job or run. |
| 4 | Ambiguous selector. |
| 5 | Lifecycle or state conflict. |
| 6 | Partial live-input delivery. |

Configured redaction applies to Jobman diagnostics and structured output.
Captured target stdout/stderr is intentionally raw and is never promised to be
secret-free. An explicitly requested evidence log tail is sanitized before its
evidence digest is sealed, but users must still review target output before
sharing it.

## State and upgrades

Jobman migrates supported older schemas forward and never silently downgrades.
Jobman v1.0 through v1.3 write database schema 7. The unreleased diagnostic
extension writes schema 8 and directly upgrades intact existing schemas 1
through 7. Schema 8 adds typed per-run diagnostic facts and one private local
fingerprint key; it does not backfill historical failures. Schema 0 represents
a new, uninitialized database rather than a supported historical store.
Releases before v0.6.0 were an unrelated prototype and have no supported
persisted-state migration. Opening a noncurrent supported schema first creates
a private, consistent backup under `STATE_DIR/backups/`; migration aborts if
backup creation fails. Once schema 8 is written, an older schema-7 binary
cannot open that state root. Restore is an explicit offline operator action
described in [UPGRADING.md](UPGRADING.md).

State roots must be local filesystems with working SQLite WAL locking. Known
network/distributed filesystems are rejected. Cross-host state sharing is not a
v1 feature.

## Deprecation policy

A v1 command or flag can be deprecated in a minor release but remains
functional for the rest of v1. Help and release notes identify the replacement.
Removal waits for the next major version. Security fixes may narrow unsafe
behavior sooner and must be called out prominently.
