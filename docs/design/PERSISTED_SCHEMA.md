# Persisted schema

Status: implemented, frozen v1 compatibility surface
Database schema version: 8
Job specification schema version: 2
Log index versions: 1 (unsegmented) and 2 (segmented)
Specification: [Persistence and concurrency](SPEC.md#7-persistence-and-concurrency)
Decision: [ADR-0002](adr/0002-sqlite-metadata-and-filesystem-logs.md)

This document records the current formats written by Jobman. It is an
implementation reference, not permission to edit state by hand. The migration
source and format encoders remain authoritative. An incompatible change uses a
new migration or format version; an applied migration is never rewritten in
place.

## State layout

The state root is selected by `--state-dir`, then `JOBMAN_STATE_DIR`, then the
platform default. Jobman resolves it to an absolute path. The normal layout is:

```text
<state-dir>/
  jobman.db
  jobman.db-wal
  jobman.db-shm
  logs/
    <job-uuidv7>/
      <run-number>/
        .active
        stdout.log
        stdout.000002.log
        stderr.log
        stderr.000002.log
        chunks.idx
```

Additional numbered stream files exist only when rotation is enabled. `.active`
exists only while a capture owns the directory. Cleanup atomically renames a
completed run directory with a `.deleting` suffix before removing recognized
private regular files; the suffix may remain after interruption and is resumed
fail-closed.

The WAL and shared-memory files exist only while SQLite requires them. The
database and sidecars form one storage unit and must not be copied independently
while Jobman processes are using the store.

On Unix-like systems, Jobman creates directories with mode `0700` and database,
marker, and log files with mode `0600`, and rejects unsafe ownership,
permissions, symlinks, or hard links where checked. On Windows, new paths use
protected current-user/SYSTEM/administrators ACLs and existing state rejects
broad principals or foreign ownership. See the
[platform capability record](PLATFORM_CAPABILITIES.md).

## SQLite identity and migrations

The database uses:

- `PRAGMA application_id = 0x4a4f424d` (`JOBM`);
- `PRAGMA user_version = 8`; and
- one checksum-bearing row per applied version in `schema_migrations`.

Startup rejects a foreign application ID, a newer schema, a missing migration,
or a checksum mismatch. Migrations are applied in order under the store's write
transaction and advance both `user_version` and the migration ledger. Each
process uses one pooled physical connection with foreign keys, WAL mode,
`synchronous=FULL`, immediate write transactions, and a bounded busy timeout of
five seconds by default. The bundled SQLite library must report version 3.51.3
or newer.

The store is supported only on a local filesystem with reliable SQLite locking
and shared-memory behavior. Platform adapters reject known remote,
distributed, and user-space filesystem types before SQLite opens. This is a
fail-fast guard, not a promise that every third-party synchronization driver is
detectable; operators must still choose an ordinary local state volume.

Schema 1 is the oldest accepted intermediate database from development of the
durable implementation. Tagged releases v0.6.0 through v1.3.0 create schema 7;
no tagged release used schemas 1 through 6 as its final format. The unreleased
diagnostic work advances current stores to schema 8. Schema 0 denotes a new,
uninitialized database rather than a released persisted format. Releases
before v0.6.0 were an unrelated prototype and have no supported persisted-state
migration. The migration suite upgrades schema 1 fixtures through every
immutable intermediate migration and creates a private backup before changing
an existing database.

### Migration 1: lifecycle snapshots and events

All tables are SQLite `STRICT` tables. Canonical IDs are lowercase, hyphenated
UUIDv7 strings. Columns ending in `_ns` contain nonnegative Unix nanosecond
timestamps and are normalized to UTC in the Go model. Revisions are positive
and increase on mutable snapshot transitions.

| Table | Purpose and principal constraints |
| --- | --- |
| `schema_migrations` | Records each positive migration version, application time, Jobman version, and 64-character checksum. |
| `jobs` | Stores immutable `spec_json` plus the current job phase/outcome/revision, lifecycle times, active run, supervisor, cancellation intent, diagnostics, and temporary launch-credential digest/deadline. A completed phase requires an outcome and completion time. The plaintext credential is never stored. |
| `runs` | Stores numbered target invocations, phase/outcome/revision, resolved executable, verified process identity, stop/exit observations, and log paths, sizes, index version, integrity, and recording health. `(job_id, run_number)` is unique and at most one run per job may be nonterminal. |
| `supervisors` | Stores one process identity and renewable lease per job. Release time is retained for history. |
| `state_events` | Stores append-only job, run, and supervisor transitions. `(entity_kind, entity_id, entity_revision)` is unique. An event and its updated snapshot commit together. |

Foreign keys restrict destructive deletion. SQL checks duplicate the principal
Go invariants: allowed phases and outcomes, paired nullable fields, nonnegative
sizes/times, and terminal-state consistency. Mutations compare the prior
revision and phase; a zero-row update is a conflict, not success.

### Migration 2: scheduler and policy state

| Table | Purpose and principal constraints |
| --- | --- |
| `job_runtime` | Stores repeated-run counters, next-run/wait reason, pause origin and elapsed pause time, collective prerequisite-completion time, private live-input endpoint, and durable per-run EOF intent. It has exactly one row per job. |
| `job_dependencies` | Stores selectors resolved to immutable job IDs, outcome predicates, and the observed terminal revision/outcome used to satisfy an edge. Self-dependencies are forbidden. |
| `wait_evaluations` | Stores each prerequisite's kind, last evaluation/satisfaction time, attempt count, and non-secret diagnostic code. |
| `concurrency_limits` | Stores the store-wide capacity and named-pool capacities with revisions. A null capacity means explicitly unlimited. |
| `admissions` | Stores one job's atomic global/optional-pool slot allocation, optional run binding, lease metadata, and idempotent release time. Lease expiry is liveness evidence; it does not by itself make occupied capacity reusable. |
| `notification_attempts` | Stores bounded structured delivery metadata by state-event ID, notifier, and attempt number. Response bodies, command output, notification payloads, and resolved secrets are not stored. |
| `job_tags` | Stores validated tags keyed by job ID and tag. |

### Migration 3: explicit dependency outcome sets

Migration 3 rebuilds `job_dependencies` so a canonical
`outcomes:OUTCOME[,OUTCOME...]` predicate can represent multiple accepted
terminal outcomes. Existing single predicates are copied unchanged and the
dependency-target index is recreated.

### Migration 4: durable admission fairness

`admission_requests` records a durable wait request with its job, optional
pool, slot count, enqueue time, and bounded bypass count. Initial requests use
the instant at which all dependency and wait prerequisites became satisfied;
retry requests use their new eligibility instant. Equal instants are ordered
by canonical job ID. Acquisition may bypass an older request only when that
request cannot currently fit; the durable counter prevents unbounded
starvation. A trigger removes queued requests when their job becomes terminal.

### Migration 5: durable log-pruning tombstones

`run_log_pruning` stores one completed run's prune time plus the number of
filesystem entries and bytes removed. The row is inserted only after guarded
filesystem cleanup succeeds and is idempotent for repeated cleanup. Run reads
join the tombstone into `LogMetadata`: the original internal paths remain part
of the historical run row, but availability becomes false and user-facing
inspection reports the prune time/counts without presenting removed paths as
available. Log reads reject a pruned run instead of treating it as empty or
recreating files.

The filesystem removal and SQLite tombstone cannot form one atomic transaction.
Cleanup first renames the directory and syncs a checksummed summary of the
entries and bytes it will remove. It retains that private claim after deleting
the log files, commits the pruning row with the recorded counts, and only then
removes the summary and empty directory. A retry can resume at every boundary,
including after filesystem removal but before the metadata commit; finalization
is idempotent.

### Migration 6: recoverable notification delivery queue

`notification_deliveries` stores one subscribed state-event/notifier pair
before external delivery begins. It preserves the state event's stable ID,
event/run identity and time, maximum/used attempt counts, next-attempt time,
and one of `pending`, `delivering`, `succeeded`, or `failed`. A delivering row
uses a UUID claim token and renewable expiry so a later per-job supervisor can
recover an abandoned attempt without allowing a stale worker to complete it.

Claiming the oldest ready item, recording its structured
`notification_attempts` row, and advancing or completing the queue entry use
transactional compare-and-swap updates. Destinations, payload bodies,
credentials, raw errors, response bodies, and command output are not stored in
the queue. Subscribed delivery rows are inserted in the same transaction as
their lifecycle snapshot and state event. Future retry times are serviced by
the job's supervisor; another supervisor opportunistically processes ready or
expired work left by a crash. There is no shared notification daemon, so
abandoned work may remain pending until a later Jobman supervisor starts.
Migration 6 does not backfill deliveries for historical pre-migration events.

### Migration 7: runtime-counter repair and deterministic admission order

Migration 7 recomputes each job's run, success, and failure counters from its
completed historical runs. This repairs runtime rows initialized with zero
counters by the original schema 1 to schema 2 upgrade without rewriting an
already published migration. It does not change run outcomes or immutable job
specifications.

The migration also replaces the admission-request ordering index with one on
enqueue time followed by canonical job ID. This makes the persisted index
match the scheduler's prerequisite-eligibility ordering and deterministic
tie-break rule.

### Migration 8: diagnostic facts and private failure fingerprints

`store_secrets` contains exactly one 32-byte
`failure_fingerprint_hmac_v1` key. Jobman creates it from `crypto/rand` after
the migration transaction and before the store becomes usable. If a process
stops between those steps, the next open safely creates the missing key while
no indexed fingerprint exists. A missing key alongside an indexed fingerprint
is treated as corruption rather than silently rotating identity. A current
store loads its existing key without taking a writer lock. Invalid or
additional key rows fail schema verification. The key is part of consistent
database backups but is never logged, placed in evidence, or exposed by a CLI.

`run_diagnostic_facts` contains optional version-1 post-wait facts for a
completed run: a JSON array of typed resource observations bounded to 16 KiB,
a stable
failure class, and an optional keyed fingerprint. Every resource records its
metric, integer value, unit, process/tree scope, native source, and
completeness. The initial supervisor records process-scoped user/system CPU
time everywhere and process-scoped peak resident memory on Linux and macOS.
Absence remains valid for old runs, targets that never started, and unsupported
native metrics.

Failure fingerprints use HMAC-SHA-256 with an independently versioned factual
input projection. The projection contains lifecycle outcome, stable failure
class, safe diagnostic/exit/timeout facts, policy disposition, and a separately
HMACed executable identity. Raw command text, arguments, paths, environment,
logs, prose, and model output are neither stored in the row nor included
directly in the exposed value. Successful or factually insufficient runs have
no fingerprint. The fingerprint is deliberately meaningful only inside its
own state store.

The partial `run_diagnostic_facts_fingerprint` index supports bounded,
deterministically ordered exact matches. Resource facts and the fingerprint
row commit in the same transaction as run completion and policy counters.
Historical schema-7 failures are not automatically backfilled; evidence marks
similarity as partially indexed when any older failure in the searched state
store lacks an indexed fingerprint.

The exact columns, checks, indexes, trigger, and immutable migration text are
defined by
[`internal/store/migrations.go`](https://github.com/ryancswallace/jobman/blob/main/internal/store/migrations.go).
Tests cover initialization, upgrades, concurrent initialization, application
and version headers, checksums, rollback, compare-and-swap conflicts, capacity,
fairness and deterministic tie-breaking, runtime-counter repair, pruning
tombstones, notification claims/retries, fingerprint-key recovery, atomic
diagnostic-fact persistence, indexed similarity privacy/bounds, and busy-error
classification.

## Canonical job specification JSON

`jobs.spec_json` contains strict canonical JSON. New submissions write schema
version 2; the decoder retains schema version 1 support by applying the original
single-run defaults. Unknown members, duplicate keys, trailing values, and
missing required collections are rejected.

The version 2 top-level shape is:

```json
{
  "schema_version": 2,
  "executable": "/usr/bin/example",
  "arguments": ["one", "two"],
  "working_directory": "/home/user/work",
  "environment": {
    "inheritance": "submission",
    "set": {"MODE": "batch"},
    "unset": []
  },
  "name": "example",
  "stop_policy": {
    "grace_period": "10s",
    "force_after_grace": true
  },
  "stdin_policy": "null",
  "execution_policy": {}
}
```

The empty `execution_policy` above is an illustration placeholder, not a valid
stored value. The persisted object always contains normalized fields for:

- completion limits and retry abort time;
- success/retry classification;
- independent failure and success delays;
- per-run and whole-job timeouts;
- wait mode/conditions and dependencies resolved to job IDs;
- global/named-pool slot requests;
- notification subscriptions and the complete non-secret notifier definitions
  required by a detached supervisor;
- tags, groups, and secret environment references;
- foreground/stdin-file policy metadata; and
- capture selection, rotation, segment limits, and completed-log retention.

Durations are canonical Go duration strings. Timestamps are RFC 3339 with
nanosecond precision. Lists and maps are normalized for deterministic encoding;
argument order is preserved. Resolvable secret references contain a provider
and locator, never a resolved value. The inherited submission environment is
not copied wholesale.

Process identity is strict JSON alongside a separately checked PID. The current
cross-platform shape is:

```json
{
  "PID": 1234,
  "Platform": "linux",
  "CreationID": "1234567",
  "BootID": "01234567-89ab-cdef-0123-456789abcdef",
  "TreeID": "1234"
}
```

Creation and boot identity reject a reused PID where the platform adapter can
provide the evidence; `TreeID` records the target boundary. Event details must
be valid JSON and currently contain structured transition diagnostics.

## Log files and chunk indexes

Raw stream segments are authoritative and byte-preserving. `chunks.idx`
records the order in which Jobman observed appends. Both index versions use
fixed 52-byte records:

| Byte range | Encoding | Meaning |
| --- | --- | --- |
| 0-3 | bytes | Magic `JMLI`. |
| 4 | unsigned byte | Index version: `1` for unsegmented, `2` for segmented. |
| 5 | unsigned byte | Stream: `1` for stdout, `2` for stderr. |
| 6-7 | little-endian `uint16` | Zero in version 1; positive per-stream segment number in version 2. |
| 8-15 | little-endian `uint64` | Positive contiguous global sequence number. |
| 16-23 | little-endian `uint64` | Contiguous offset within the selected raw stream segment. |
| 24-31 | little-endian `uint64` | Chunk length, from 1 byte through 16 MiB. |
| 32-39 | little-endian `int64` | Observation time, Unix seconds. |
| 40-47 | little-endian `int64` | Observation time, nanoseconds within the second. |
| 48-51 | little-endian `uint32` | CRC-32C of bytes 0 through 47. |

Rotation is selected before capture: a run without rotation uses version 1, while a
run with a positive segment-byte limit uses version 2 and segments numbered
from one independently for stdout and stderr. Capture never deletes an earlier
segment to make room. Reaching a finite segment count degrades log recording
and preserves everything already written.

For each chunk, Jobman writes and syncs raw bytes before writing and syncing the
index record. A crash can leave an unindexed raw tail but must not leave a valid
record that names nonexistent bytes. Readers discard a partial final record,
reject corruption in a complete record, preserve raw tails in individual
streams, and omit those tails from the combined view because their cross-stream
ordering is unknowable.

## Compatibility and safety rules

- Never change an applied migration or an emitted index format; add a migration
  or version.
- Do not infer lifecycle state from files when the database says otherwise.
- Do not remove or rewrite state events independently of their snapshots.
- Do not derive paths from display names or other user-controlled path text.
- Do not persist resolved secret values, input bytes, notification response
  bodies, command-notifier output, or plaintext launch credentials.
- Back up a live database only with a SQLite-supported consistent mechanism.
- Treat CLI JSON, YAML configuration, immutable spec JSON, notification JSON,
  and SQLite rows as independent versioned contracts.
- Cleanup must recheck terminal metadata, reject active markers and unknown
  entries, and remain inside the canonical state root.
