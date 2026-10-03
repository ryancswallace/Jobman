# Shared Control diagnostic evidence

Status: implemented public contract and collector; Control/Dashboard adapters
and downstream Diagnose support must be released separately before deployment.

The standard-library-only `github.com/ryancswallace/jobman/diagnostic` package
owns factual evidence acquisition. It provides `SharedCollector.Collect`, a
`SnapshotReader` interface for an authorized Control metadata transaction, and
a separate `LogReader` interface for authorized immutable log chunks. The
collector neither reads a local SQLite store nor executes processes, scheduler
commands, or model calls. It returns a verified `Evidence` document.

## Versions and compatibility

| Contract | Kind | Version |
| --- | --- | --- |
| Control metadata snapshot | `jobman.shared_diagnostic_snapshot` | 1 |
| Shared evidence | `jobman.diagnostic_evidence` | 2 |
| Existing local-store evidence | `jobman.diagnostic_evidence` | 1 |

`SchemaVersion` and `CollectorVersion` retain their stable schema-1 values.
`Seal` defaults to schema 1 when no version is supplied; callers deliberately
select `SharedSchemaVersion` for shared evidence. `SharedCollector` does that
automatically. `Decode`, `Verify`, and `Encode` understand both schemas. All
original schema-1 fixtures retain their exact bytes and semantic identities.
Existing consumers supporting only schema 1 must reject schema 2 explicitly.

The new snapshot's revisions, run numbers, offsets and lengths are decimal
JSON strings to avoid JavaScript integer rounding. The inherited evidence
fields keep their established JSON integer representation; consume evidence
with the public Go decoder or a lossless integer decoder, and project numeric
values to decimal strings in browser-facing APIs. Do not round-trip sealed
evidence through JavaScript `Number`.

## Authority and collection

`SharedSelection` contains deployment UUID, Control instance UUID, namespace
UUID and job UUID. It can also pin an expected job revision and a selected run
UUID. The collector rejects a returned snapshot that disagrees with any of
those pins. A reader must report a revision conflict rather than attaching the
requested revision to newer state. IDs are canonical lowercase, nonzero UUIDs.

Deployment identity is operator configuration, not an end-user claim. A
Dashboard adapter binds it to its configured source and checks the actual
Control instance identity. Each adapter must enforce current namespace
authorization on every metadata and log request. The interfaces deliberately
do not carry credentials or imply that a prior check remains authoritative.
Denied, revoked or unavailable access aborts collection without a partial
evidence result. Collectors propagate cancellation to both readers.

`SharedSnapshot` contains one transactionally consistent bounded selection of
job metadata, actual run UUIDs and durable run numbers, execution identities
when recorded, typed facts, omission/redaction notices and manifest references.
Run numbers are never fabricated from slice indices. Missing historical
execution IDs are valid and generate an explicit omission. Metadata adapters
must bound queries before constructing the snapshot. `ValidateSharedSnapshot`
checks the boundary and `DecodeSharedSnapshot` additionally bounds untrusted
JSON, rejects duplicate keys, unknown envelope fields and trailing values.

Source provenance records Control/contract versions and source kind `control`.
The generic source records the collecting core library version, collector
version and collection platform. `store_schema_version` is absent for schema
2. It must never contain a fabricated local SQLite schema number.

## Disclosure and factual observations

The default `metadata` profile contains only explicitly supported metadata
facts. Commands, paths, environment names/values, source code and arbitrary
unknown fact codes are excluded with omissions, including facts mislabeled as
metadata when their code is not on the allowlist. Supported scalar tokens are
bounded codes, not free-form command/error output. The shared collector does
not alter any existing local fact code's type or meaning.

Supported existing facts cover job/run phase, outcome, revision, lifecycle
timestamps, run exit code/signal, timeout/stop reason and log
availability/integrity/recording health/byte counts. New shared facts are:

| Code | Typed value |
| --- | --- |
| `control.job.desired_state` | State code; distinct from observed phase |
| `control.job.observation_confidence` | Confidence code; distinct from transport freshness |
| `control.scheduler.observation` | `SharedSchedulerObservation`: state/reason codes and observation time |
| `control.dependency.observation` | `SharedDependencyObservation`: prerequisite job, predicate, observed outcome, source-computed satisfaction and disposition |
| `control.lifecycle.event` | `SharedLifecycleEvent`: durable event identity/type, run/execution, phase/outcome and observed/recorded times |

Unknown future enum tokens remain factual values. Arbitrary scheduler messages
are not reason codes. Missing observations remain omissions; the collector
does not infer execution times from update times or memory exhaustion from an
exit code. Facts join this job/selected runs, or an explicitly identified
lifecycle event. Invalid joins fail collection. Snapshot items are sorted by
ID, runs by real number and manifest references by opaque ID.

`include_log_tail` is an explicit request profile. It requires a sanitizer that
implements both `Sanitizer` and `ValueRedactionReporter`, affirming a configured
value-aware policy. Without it, logs are omitted with
`configured_redaction_unavailable`; there is no raw-byte fallback. The reader
receives only an opaque manifest reference, source-qualified selection and byte
budget. It must validate chunk checksums and safe storage access. No path or URL
is accepted from the diagnostic request.

A tail must cover exactly the last `min(limit, manifest bytes)` bytes of its
pinned manifest. Changed manifests, wrong execution/run/stream, short reads,
unexpected ranges and access errors fail collection. Original byte offsets,
selected bytes and pre-redaction total length are retained. Content bytes and
SHA-256 describe the sanitized bytes, whose length can differ. Do not interpret
post-redaction string positions as original byte positions. Citation reads use
the sealed artifact only, never the current log at a similar offset.

## Bounds, consistency and identity

| Resource | Hard bound |
| --- | ---: |
| Snapshot JSON and sealed evidence JSON | 2 MiB each |
| JSON nesting | 32 |
| Metadata facts | 1,024 |
| Selected runs | 32 |
| Streams per run | 2: stdout and stderr |
| Requested and sanitized tail per stream | 64 KiB |

Oversized or invalid metadata fails explicitly. If base64 log bytes make the
sealed bundle too large, whole tails are omitted in deterministic reverse
reference order with `log_budget_exceeded`; references and source facts remain.
Readers cannot return arbitrary extra bytes or rely on the collector to bound
an unbounded database query. No artifact is silently cut after sanitization.

Metadata is `transactional_snapshot`. Logs are captured afterward: a completed
manifest is `stable`, unfinished streams are `point_in_time`, and a combination
is `mixed`. No collected bytes means `not_collected`. Active or unknown job
phases conservatively set `active_state_may_have_advanced`.

Schema-2 `shared` provenance is part of the evidence semantic hash: authority,
real run identities, execution IDs, selected disclosure profile and manifest
identities/revisions/lengths/completion flags all participate. Manifest state
is retained even for metadata-only evidence so downstream freshness and cache
checks can distinguish changed sources. Changing any of these invalidates the
seal. Capture wall time is excluded, as in schema 1. A digest proves integrity
of the supplied snapshot, not source authenticity; authentication remains the
adapter's responsibility.

The deterministic fixture
[`shared-control-failure-v2.json`](../diagnostic/testdata/shared-control-failure-v2.json)
supports downstream integration. Run `go test ./diagnostic` to verify both
generations of fixtures, identity/disclosure boundaries, source substitution,
revocation/cancellation, redaction/ranges and budget behavior. Regenerate a
reviewed shared fixture with `UPDATE_DIAGNOSTIC_FIXTURES=1 go test ./diagnostic
-run TestSharedEvidenceFixture`; do not regenerate original fixtures casually.
