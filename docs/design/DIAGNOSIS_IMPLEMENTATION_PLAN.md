# Diagnostic evidence and diagnosis companion implementation plan

Status: M0 through M5 implemented in coordinated repositories but unreleased.
M6 selected-artifact enrichment, offline evaluation, and support bundles are
implemented. Live hosted/self-hosted evaluation and the first signed release
remain open; system-event collection, semantic retrieval, and mutation remain
separately gated.
Design: [Diagnostic evidence and optional diagnosis companion](DIAGNOSIS.md)
Scope: Jobman core, the independent `jobman-diagnose` project, provider
adapters, evaluation tooling, documentation, packaging, and release evidence

## 1. Purpose

This plan turns the diagnosis design into reviewable, independently releasable
work. It is deliberately organized as vertical slices rather than as one large
core change followed by one large AI change. Each milestone must leave Jobman
useful, compatible, and releasable even if later work stops.

The complete program has two products and six independently versioned
contracts:

| Product or contract | Owner | Release unit |
| --- | --- | --- |
| Job execution, factual capture, and evidence collection | Jobman core | `jobman` binary and Go module |
| Deterministic and generated diagnosis | Companion project | `jobman-diagnose` binary and Go module |
| Diagnostic evidence schema | Jobman core | Public `diagnostic` Go package plus JSON fixtures |
| Diagnosis report schema | Companion | Public `diagnosis` Go package plus JSON fixtures |
| Structured-generation protocol | Companion | Provider-facing request and response schemas |
| External-command protocol | Jobman core | `jobman-NAME` process and environment contract |

Core Jobman never imports the companion. The companion consumes only the
public evidence package and the versioned core JSON command. Production code in
both projects remains Go. Optional Python tooling is confined to offline
evaluation.

## 2. Delivery principles

Every implementation pull request must follow these rules:

- preserve the frozen v1 commands and JSON envelopes unless an explicitly
  reviewed post-v1 compatibility note says otherwise;
- keep core changes independently useful without the companion;
- keep companion deterministic operation independently useful without a
  provider, network, credentials, or Python;
- add no model, prompt, embedding, or provider dependency to Jobman core;
- make every read, log excerpt, subprocess, request, response, and collection
  finite and cancellable;
- persist observations, not diagnoses, in Jobman's authoritative state;
- add a golden compatibility fixture before exposing a new machine contract;
- use typed codes and allowlisted attributes at persistence boundaries;
- ship deterministic behavior before generated augmentation; and
- require explicit approval for remote disclosure, unrelated-job history, and
  platform-event enrichment.

The normal change size is one work package or one coherent row within a work
package. Database migrations, public schema versions, and accepted ADRs receive
dedicated reviews rather than being incidental parts of feature pull requests.

## 3. Dependency map

```text
contract decisions and fixtures
    |
    +--> core diagnostic catalog
    |        |
    |        +--> evidence contract --> snapshot query --> collector --> show evidence
    |                                                                  |
    |                                                                  +--> offline companion
    |
    +--> external-command ADR --> extension dispatch ------------------+
    |
    +--> resource capture --> state migration --> fingerprints --> similar history
    |                                                   |
    |                                                   +--> richer analyzers
    |
    +--> companion report contract --> engine --> deterministic analyzers/actions/retry
                                                    |
                                                    +--> disclosure projection
                                                            |
                                                            +--> structured generators
                                                                    |
                                                                    +--> provider adapters

synthetic evaluation corpus begins with the first contract fixture and gates
deterministic, generated, calibration, and enrichment releases thereafter
```

The critical path to the first useful release is:

1. freeze evidence schema version 1 and diagnostic codes;
2. ship `jobman show evidence --json`;
3. ship deterministic `jobman-diagnose`; and
4. enable `jobman diagnose` through external dispatch.

Resource persistence, historical similarity, and model providers are not on
that path and must not delay the offline feature.

## 4. Decisions to record before implementation

The design establishes the product boundary. The following implementation
choices still need an explicit recorded answer. The recommended choice is the
default for this plan.

| Decision | Recommended choice | Record |
| --- | --- | --- |
| Companion repository and module path | Create a separate public repository and Go module named `jobman-diagnose`; do not use a nested module in this repository. | Companion bootstrap issue and README |
| Evidence type sharing | Publish a standard-library-only `diagnostic` package from the Jobman module. The companion imports it but still acquires live evidence through JSON and a process boundary. | Evidence package documentation |
| External commands | Accept an ADR for built-in precedence, direct `PATH` lookup, reserved flag handling, environment context, nested-dispatch prevention, signals, and exit status. | New Jobman ADR |
| `show evidence` selector collision | Keep `show evidence JOB`; document that a job literally named `evidence` is addressed as `show job evidence`, as already required for names colliding with `show job` and `show run`. Add a compatibility regression test. | CLI compatibility note |
| Default evidence disclosure | Collect lifecycle metadata and safe codes by default. Never collect environment values. Require explicit flags for log bytes and any future raw command or path content. | Evidence schema field policy |
| Evidence digest | Canonicalize the typed semantic value, including canonicalized `json.RawMessage` values, before SHA-256. Do not hash ordinary encoder output blindly. | Evidence contract tests |
| Post-seal redaction | Sanitize before sealing and use a dedicated evidence encoder. Do not pass sealed evidence through the generic JSON redactor, which could invalidate its digest. | Collector and CLI tests |
| Fingerprint key storage | Add a private database table with one 32-byte store key. Ensure it with `crypto/rand` when an upgraded store opens; include it in normal database backup and never expose it through evidence. | State-migration design review |
| Initial provider set | First implement the bounded command bridge for conformance testing, then one OpenAI-compatible structured HTTP adapter and one Ollama adapter. Add other vendor transports only after the engine contract is stable. | Companion provider roadmap |
| Supported schemas | Support every evidence and report schema released in the current compatibility window. Reject newer required semantics with an actionable error; tolerate documented additive fields. | Compatibility matrix |

The external-command ADR and evidence version-1 review are hard gates. Provider
selection and richer state migration are later gates and do not block the
offline companion.

## 5. Milestones and release slices

| Milestone | User-visible result | Core migration | Provider required |
| --- | --- | --- | --- |
| M0: contracts and foundations | Reviewed schemas, fixtures, ADRs, and separate companion skeleton | No | No |
| M1: core evidence | `jobman show evidence JOB --json` returns bounded factual evidence | No | No |
| M2: offline diagnosis | Direct `jobman-diagnose JOB` produces deterministic human and JSON reports | No | No |
| M3: natural invocation | Installed companion is available as `jobman diagnose JOB` | No | No |
| M4: richer evidence | Resource facts, versioned fingerprints, and opt-in same-fingerprint history | Yes | No; implemented, unreleased |
| M5: generated augmentation | Explicit `--ai` or `--profile NAME` adds validated generated hypotheses | No additional core migration | Yes, only when selected |
| M6: evaluation and enrichment | Offline metrics, selected-artifact enrichment, and support bundles are implemented; platform events and retrieval remain separately gated | Only if a later core fact requires it | Optional |

M1, M2, and M3 should be released before M4 and M5. That sequence exercises
the public boundary in production before increasing persistence or disclosure
risk.

## 6. M0: contracts and foundations

### M0.1 Freeze the core evidence vocabulary

Create a registry document and test fixtures for:

- diagnostic origins, categories, retry classes, and quality labels;
- evidence item codes and their JSON value types, units, and disclosure class;
- artifact roles, capture qualities, omission codes, and redaction notices;
- consistency states and capability facts;
- stable item-ID construction and ordering; and
- evidence schema versioning rules.

The registry must distinguish facts from heuristics. For example, an observed
signal and a confirmed platform resource-limit event are facts; an OOM guess
from exit code 137 is not.

Deliverables:

- version-1 JSON examples for a failed exit, start failure, timeout, active
  run, pruned logs, and successful target with notification failure;
- malformed and unsupported-version examples;
- a disclosure matrix stating which source fields may populate each evidence
  code; and
- initial byte, item, run, event, and artifact limits.

Exit gate: a reviewer can validate every field in a fixture against a current
Jobman snapshot or an explicit derivation rule without consulting prose model
output.

### M0.2 Record the external-command decision

Add an ADR that specifies:

- built-ins always win over extensions;
- accepted lowercase names and exact `jobman-NAME` lookup behavior;
- how `--state-dir` and an explicitly supplied `--config` are removed from the
  child arguments and represented in the versioned environment contract;
- that the extension inherits the caller environment, never a managed target's
  resolved environment, while Jobman replaces all reserved protocol variables;
- that no shell, current-directory fallback, download, or implicit discovery
  is used;
- how nested extension dispatch is disabled for the first protocol version;
- stream, context, interrupt, termination, and exit-status behavior on Unix and
  Windows;
- how a child nonzero status is returned without printing a duplicate Jobman
  error; and
- how `--no-extensions` and `JOBMAN_NO_EXTENSIONS=1` fail closed.

The implementation should set `JOBMAN_NO_EXTENSIONS=1` in the child
environment. This lets the companion call core built-ins while preventing an
accidental recursive extension chain.

Exit gate: table tests against a fake resolver and an assembled helper process
cover every rule before real `PATH` execution is enabled.

### M0.3 Bootstrap the companion repository

Create the separate module with:

```text
cmd/jobman-diagnose/
diagnosis/
provider/
internal/config/
internal/coreclient/
internal/enrichment/
internal/presentation/
testdata/
```

Add the same supported Go baseline and CGO-free release targets as Jobman where
practical. Establish formatting, lint, unit, race, vulnerability, cross-build,
license, SBOM, checksum, signature, and release-archive gates. The first binary
may expose only `--version` and a clear not-yet-implemented diagnosis error.

The companion must own its own changelog, security policy, configuration
reference, release process, and compatibility matrix. Its default dependency
graph must not require a provider SDK.

Exit gate: a clean checkout produces reproducible native archives and the
binary runs without Jobman configuration, provider configuration, Python, or a
network.

### M0.4 Establish cross-project fixtures

Store canonical core fixtures in `diagnostic/testdata/`. Copy released fixtures
into the companion with an origin manifest containing the Jobman release,
evidence schema, and SHA-256. Tests must never fetch fixtures from the network.

For each released evidence schema, retain:

- the oldest supported valid fixture;
- a newest additive-fields fixture;
- an unsupported-newer-schema fixture;
- a maximum-size boundary fixture; and
- a secret-canary fixture whose forbidden values must never appear in output.

Exit gate: both repositories validate the same semantic digest and the
companion can decode every evidence version in its published compatibility
window.

## 7. M1: Jobman core factual evidence

### C1. Add typed diagnostic records

Primary core locations:

```text
internal/model/diagnostic.go
internal/model/transition.go
internal/model/lifecycle.go
internal/executor/
internal/supervisor/
internal/policy/
internal/notify/
internal/store/
```

Implementation steps:

1. Define `DiagnosticCode`, `DiagnosticRecord`, origin, category, retry class,
   safe OS-error category, validation, and the initial catalog.
2. Keep existing snapshot string columns for v1 readers, but change new Go
   call sites to accept typed codes or records.
3. Encode the versioned record inside `state_events.details_json` while
   retaining legacy members such as `diagnostic_code`, `reason`, and
   `next_run_at` where existing readers require them.
4. Add a decoder that accepts legacy detail shapes and the new record. Unknown
   codes survive decoding as unknown facts rather than corrupting history.
5. Classify direct executable lookup, working-directory, permission, and start
   errors at the observation point. Do not discard the safe class after a
   start failure as the current supervisor path does.
6. Audit wait, dependency, admission, timeout, ownership, log, store, and
   notification paths. Replace new arbitrary strings with catalog constants;
   preserve existing stored values as legacy aliases where necessary.
7. Add a generated or table-driven catalog test that rejects duplicate codes,
   invalid names, unsafe attributes, and undocumented mappings.

No database migration is needed because current columns already hold codes and
event details are already JSON.

Focused tests:

- one positive and negative test for each classifier;
- `errors.Is` and platform-specific error mappings;
- legacy event decoding and new event round trips;
- tests proving raw error text, paths, arguments, and environment values do not
  enter structured records; and
- unchanged lifecycle and notification fixtures.

Exit gate: every failure code emitted by current production paths is either a
documented typed code or a documented legacy value, and core does not infer a
root cause from free-form target output.

### C2. Implement the public `diagnostic` package

Add a standard-library-only package:

```text
diagnostic/evidence.go
diagnostic/codes.go
diagnostic/validate.go
diagnostic/codec.go
diagnostic/digest.go
diagnostic/testdata/
```

The package owns concrete envelope, source, subject, consistency, item,
artifact, omission, redaction, capability, and limit types. It should expose a
small surface such as:

```go
func Validate(Evidence) error
func Seal(Evidence) (Evidence, error)
func Verify(Evidence) error
func Encode(io.Writer, Evidence) error
func Decode(io.Reader, DecodeLimits) (Evidence, error)
```

`Decode` must bound bytes and nesting, reject duplicate identifiers and
trailing values, validate `json.RawMessage` values, and reject unsupported
required semantics. `Seal` must sort all set-like collections, canonicalize
raw JSON values, compute limits, and calculate the digest without
`evidence_id` or collection wall time.

Do not expose internal model, store, configuration, Cobra, SQLite, or logstore
types through this package. Job and run IDs cross the public boundary as
validated strings unless a separate public ID package is deliberately adopted.

Focused tests:

- golden encoding and digest stability;
- randomized map and item insertion order;
- unknown additive fact codes;
- duplicate IDs, invalid values, unsupported versions, and inconsistent
  limits;
- fuzz decoding and canonicalization; and
- a package dependency test proving the package imports only the standard
  library.

Exit gate: a released companion can use the package without importing any
Jobman internal package or changing when Jobman's internal state types change.

### C3. Add a single-snapshot store query

Add an internal store result containing the job, ordered runs, runtime,
dependencies, wait evaluations, admission request and lease, applicable
capacity, notification deliveries and attempts, supervisors needed for
ownership facts, bounded lifecycle events, and existing diagnostic records.

Implementation steps:

1. Refactor current row decoders so they work with a `schemaQueryer` and can be
   reused inside one read-only transaction.
2. Add `GetDiagnosticSnapshot(ctx, selector, limits)` that resolves the
   selector and performs every metadata query on the same SQLite transaction.
3. Define stable ordering and hard limits for events, attempts, and runs.
4. Return explicit truncation counts rather than silently dropping rows.
5. Keep reconciliation outside this read transaction. The application service
   first performs the same bounded stale-submission and stale-ownership checks
   as `Inspect`, then captures a fresh snapshot.
6. Never read log files or wait for a process while the transaction is open.

The existing `Inspect` API need not change immediately. A later refactor may
use the same snapshot query if it preserves current output exactly.

Focused tests:

- a concurrent writer cannot produce mixed revisions in one snapshot;
- every row type preserves current corruption checks;
- ordering and truncation are deterministic;
- cancellation interrupts the query;
- selector errors translate to current application categories; and
- snapshot collection performs no writes after reconciliation.

Exit gate: one snapshot is sufficient to build all metadata evidence without
calling another store getter.

### C4. Implement bounded log artifacts

Extend `internal/logstore` with a range/tail API that understands both log
index versions and rotated segments. It must:

- read at most the requested bytes without first buffering the complete log;
- identify stream, segment or logical range, observed size, selected byte
  range, truncation, integrity, and point-in-time capture time;
- preserve binary bytes and compute the artifact digest over the sanitized
  bytes that are actually encoded;
- recheck file identity and private-file constraints through existing helpers;
- check context cancellation between bounded reads; and
- distinguish pruned, missing, corrupt, changing, and unsupported artifacts.

Add tests for rotation boundaries, active growth, a file shrinking during
read, binary and invalid UTF-8 content, torn indexes, pruned logs, short reads,
and the absolute one-diagnosis log ceiling.

Exit gate: no evidence option can cause a full-log read or allocate more than
its declared hard limit.

### C5. Implement the application evidence collector

Add `DiagnosticBackend` to `internal/app/api.go` without expanding the minimal
`Backend` interface. Implement it on `Service` in a dedicated file.

Collector sequence:

1. validate request limits and mutually exclusive selectors;
2. perform bounded inspection reconciliation;
3. capture the single metadata snapshot;
4. select the active, latest abnormal, explicitly selected, or bounded run
   history according to the documented rule;
5. map internal state to registered evidence codes;
6. derive only exact durations, policy dispositions, and low-level
   classifications;
7. read requested artifacts after the transaction closes;
8. sanitize every eligible field and byte artifact, recording notices and
   unavailable configured-redaction state;
9. add omissions for every requested but unavailable or truncated domain;
10. validate, sort, seal, and return the concrete evidence value.

The sanitizer interface should return sanitized bytes plus whether a change
occurred. The Jobman CLI adapts the existing configured redactor to this
interface. Invalid UTF-8 remains bytes; it must not be silently normalized
before digesting or citation.

Do not include environment values, resolved secret values, notification
destinations, target input, launch credentials, or raw Go error strings. In
the first evidence release, default to metadata and safe local-only facts.
Logs require `--logs tail`; raw command and path collection should remain out
of scope until an explicit collection flag and disclosure tests are approved.

Focused tests:

- every evidence domain and omission state;
- stable IDs and ordering;
- active versus terminal consistency;
- log and metadata budget exhaustion;
- configured and fallback redaction with secret canaries;
- unsupported platform capabilities;
- no post-reconciliation store mutations; and
- semantically identical snapshots produce the same evidence digest.

Exit gate: the collector can diagnose all current durable job outcomes using
metadata alone and produces a valid sealed bundle when logs are absent,
pruned, corrupt, or changing.

### C6. Add `jobman show evidence`

Extend `jobman/show.go` with the command and flags from the design. Reuse
existing run-selector parsing where possible, but give evidence options their
own validated request type.

CLI-specific work:

- type-assert `app.DiagnosticBackend` and return an actionable unsupported
  backend error for embedders that implement only the minimal interface;
- ensure `--run` and `--all-runs` are mutually exclusive;
- validate `--logs`, `--log-bytes`, and `--similar` before opening artifacts;
- retain the core version-1 JSON envelope with the evidence under
  `data.evidence`;
- add a dedicated encoder that wraps already sanitized, sealed evidence
  without applying generic JSON redaction again;
- make human output describe collection coverage, evidence ID, omissions, and
  safe next usage rather than pretending to be a diagnosis; and
- document the `show job evidence` escape for a job named `evidence`.

Focused tests:

- golden JSON and human output;
- digest verification after exact CLI encoding;
- stdout contains data only and stderr contains errors only;
- output and cancellation errors preserve current exit categories;
- all flag conflicts and selector collisions;
- completion, manpage, and shell-completion generation; and
- assembled binary collection from active and terminal jobs.

Exit gate: `jobman show evidence JOB --json` is a stable, model-free interface
that remains useful when no companion exists.

### M1 release gate

Before releasing core evidence:

- update the design, compatibility, security, troubleshooting, output, and
  configuration documentation;
- add evidence schema 1 to the persisted compatibility references without
  implying a database schema change;
- run focused package tests, fuzz seeds, race tests, assembled lifecycle tests,
  `make docs`, `make release-build`, and the complete `make check` gate;
- run native Linux, macOS, and Windows jobs on the release commit; and
- publish the evidence fixtures and supported-schema statement with the
  release.

## 8. M2: deterministic offline companion

### D1. Implement core evidence acquisition

`internal/coreclient` owns the only live Jobman interaction. It must support:

- extension invocation using validated `JOBMAN_EXTENSION_PROTOCOL` and the
  absolute `JOBMAN_EXECUTABLE` supplied by core;
- direct invocation using `--jobman`, then a documented safe default lookup;
- explicit forwarding of state directory and explicitly selected redaction
  configuration;
- direct `exec.CommandContext` argument vectors with no shell;
- bounded stdout, bounded stderr, deadline, cancellation, and core exit-code
  translation;
- strict decoding of the core JSON envelope and sealed evidence; and
- `--from-evidence` operation that performs no executable or store lookup.

The client must call only the built-in `show evidence` command. It must not
open Jobman's database, import internal packages, read logs directly, or use
ordinary Jobman configuration as authority.

Exit gate: direct, extension, and fixture inputs produce identical decoded
evidence, and a malicious core response cannot exceed memory or bypass strict
decoding.

### D2. Implement report and engine contracts

In the public `diagnosis` package, add concrete types and validation for:

- `FailureEvidence` and attributed enrichment;
- analyzer and generator descriptors;
- candidates, findings, hypotheses, alternatives, and provenance;
- confidence, calibration, citations, missing evidence, and warnings;
- action identifiers and execution classes;
- existing-policy state and retry verdict;
- disclosure manifests; and
- report identifiers and fingerprints.

Implement `Diagnostician`, `Analyzer`, `StructuredGenerator`, `Options`, and
`Engine`. The engine owns limits, analyzer ordering, candidate normalization,
deduplication, ranking, action resolution, retry advice, report validation, and
stable report fingerprinting.

Implementation decision: the stable companion API exports the concrete report
and `FailureEvidence` contracts plus the narrow `Diagnostician` and
`StructuredGenerator` seams. Concrete analyzer registration, options, and
engine orchestration remain internal until a second non-test consumer proves a
public extension API is needed. This prevents rule-ordering and safety-policy
internals from becoming an accidental compatibility surface while retaining
pluggable providers.

No analyzer or generator may return a complete trusted report. That rule keeps
ranking, retry, actions, and safety invariants in one place.

Exit gate: a fake analyzer set can produce a deterministic schema-1 report,
and randomized input order cannot change its semantic content or report
fingerprint.

### D3. Implement deterministic analyzers

Implement analyzers as small, versioned rules with registered output codes:

1. lifecycle and ownership consistency;
2. direct start and executable resolution;
3. exit, signal, and platform termination;
4. run timeout, job timeout, cancellation, and lost ownership;
5. retry history, backoff, exhaustion, and changing failure mode;
6. dependency, wait, and admission blockers;
7. log or state degradation that limits certainty;
8. notification-only failure separated from target outcome;
9. bounded traceback and structured error-chain extraction; and
10. opt-in same-fingerprint comparison when core supplies it.

Each rule specifies required evidence codes, disqualifying evidence,
supporting and contradicting citations, confidence method, stable score,
applicable actions, and test cases. Exact facts outrank heuristics. Text
signatures remain hypotheses unless a format is authoritative.

Exit gate: every current durable Jobman outcome has at least one useful
deterministic report or a precise missing-evidence result.

### D4. Implement action and retry policy

Create a closed action catalog. The first release permits prose and
allowlisted read-only Jobman argument vectors only. A deterministic resolver,
not an analyzer or generator, creates those vectors and revalidates them
against the selected core command version.

Implement retry advice independently from root-cause ranking. Cover:

- a retry already scheduled;
- current backoff and next eligible time;
- active dependency, wait, or admission blocking;
- remaining and exhausted run or failure budgets;
- cancellation and timeout scope;
- repeated identical versus changing fingerprints;
- persistent start failures; and
- unknown or incomplete evidence.

Exit gate: report retry state matches recorded Jobman policy fixtures and never
suggests reopening a terminal job instead of using a new `rerun` submission.

### D5. Add bounded enrichment

The first enrichment layer may inspect only evidence already collected:

- extract bounded tracebacks, compiler diagnostics, or structured error chains
  from selected artifact bytes;
- cite the core artifact and exact byte range;
- attach collector name, version, disclosure class, and quality; and
- retain the sealed core evidence unchanged.

Do not add system events, arbitrary file reads, dependency commands,
embeddings, or unrelated-job content in M2.

Exit gate: derived items are reproducible, bounded, and cannot cite bytes
outside the source artifact.

### D6. Implement the companion CLI and rendering

Implement the designed flags, mutual exclusions, and exit categories. Human
output should present, in order:

1. primary diagnosis and confidence meaning;
2. strongest supporting and contradicting evidence;
3. retry verdict and current policy state;
4. ordered safe actions;
5. alternatives; and
6. missing evidence, omissions, and warnings.

JSON output is the validated top-level companion report, not a Jobman core
envelope. Diagnosis success returns zero even when the subject failed.

Evidence and report export must use private creation, no-follow and regular-file
checks, write-sync-rename where durable output is requested, and no overwrite
without a separate explicit option. Standard output remains usable in a
pipeline without progress decoration.

Exit gate: human, JSON, import, export, active-job, failed-job, successful-job,
and no-model paths pass assembled tests on all supported platforms.

### M2 release gate

- deterministic operation has no network access and no required config file;
- the binary works directly with every core version in its compatibility
  matrix;
- all reports validate their citations and action vectors;
- fixture, fuzz, race, cross-build, SBOM, signature, and native package tests
  pass; and
- installation documentation states that Jobman and the companion are
  independently installed and upgraded.

## 9. M3: Jobman external-command dispatch

### X1. Add resolver and protocol types

Prefer a small internal extension package plus thin root integration:

```text
internal/extension/resolve.go
internal/extension/protocol.go
internal/extension/run_unix.go
internal/extension/run_windows.go
jobman/external.go
```

Inject executable lookup and child running for tests. Validate names before
lookup, canonicalize the dispatching Jobman executable, replace reserved
environment keys, and pass argument boundaries unchanged.

### X2. Integrate unknown-command handling

First add a focused Cobra behavior test or spike. Unknown commands must reach
the dispatcher only after built-in resolution and normal persistent-flag
parsing. The implementation must not reinterpret an unknown flag as an
extension or consume extension-local flags.

Capture whether `--config` was explicitly supplied rather than sending an
implicitly discovered config path. Resolve the effective state directory with
normal precedence before launching the child. Do not open the Jobman store.

### X3. Preserve streams, signals, and status

On Unix, process replacement may be used only if it preserves the documented
context and test behavior. Otherwise spawn and forward signals explicitly. On
Windows, use a bounded child lifecycle and the appropriate process-group
control.

The current executable boundary always prints a returned error. Add a typed
silent child-status result and a small main-boundary presentation helper so an
extension's nonzero exit does not produce a duplicate Jobman error line.
`ExitCode` must return the child status rather than collapsing it to one.

Focused tests:

- built-in precedence and every invalid name;
- exact `PATH` order, duplicate entries, missing binary, and current-directory
  behavior;
- flags before and after the extension name;
- argument, stdin, stdout, and stderr fidelity;
- reserved environment replacement with secret canaries;
- nested dispatch disabled;
- interrupt, termination, and nonzero or signaled child exit; and
- installed and missing `jobman-diagnose` assembled scenarios.

### M3 release gate

Accept the ADR, update security and shell-completion documentation, regenerate
CLI documentation, run native assembled tests, and verify that normal help and
completion never execute programs found on `PATH`.

## 10. M4: richer core facts, resources, and similarity

### R1. Define resource observation capabilities

Add portable resource types before adding persistence. Extend the supervisor's
post-wait observation path and platform adapters to report only facts the OS
can establish reliably:

- elapsed duration;
- user and system CPU time;
- peak resident memory where scope is known;
- I/O counters where scope and units are stable; and
- explicit platform or control-group resource-limit events.

Every metric records unit, process or tree scope, source, and completeness.
Process-only values must not be presented as tree totals. Unsupported metrics
produce capability facts, not zero values. Initial Unix support may be
process-scoped if that is all `ProcessState` establishes; Windows Job Object
accounting and Linux control-group events require separate native tests.

Exit gate: the platform capability document states exactly what each target
reports, and ambiguous exit conventions still do not become confirmed OOM
facts.

### R2. Design and apply the state migration

Use one reviewed forward migration after the resource and fingerprint row
shape is stable. The recommended schema adds:

- one private store-secret row for the local fingerprint key;
- one per-run diagnostic-facts row containing versioned resource observations
  and the factual fingerprint; and
- an index on fingerprint algorithm and value for bounded matching.

Ensure the 32-byte key with `crypto/rand` after schema migration and before the
store becomes usable. A crash between schema creation and key creation is
recoverable on the next open. Schema verification requires exactly one valid
key after initialization. The key remains in database backups and is never
logged, exported, or included in evidence.

Compute resource facts and the fingerprint before the terminal transaction and
persist them atomically with run completion when present. Absence remains
valid; lifecycle completion does not depend on a platform supporting a metric.

Migration tests must cover new, upgraded, interrupted, backed-up, restored,
read-only, corrupt-key, and concurrent-open stores. Release notes must warn
that an upgraded schema cannot be opened by an older Jobman binary.

### R3. Implement versioned factual fingerprints

Create a pure fingerprint builder whose inputs are typed safe facts. It must:

- exclude raw command, path, environment, log, prose, model, and analyzer data;
- HMAC any executable identity contribution with the local store key;
- sort and encode inputs canonically;
- include algorithm and input-schema versions; and
- return unavailable when facts are insufficient rather than creating a weak
  exit-code-only group.

Old runs do not receive a reconstructed fingerprint: doing so would risk
grouping facts captured under a weaker historical contract. Do not perform an
automatic backfill. Similar-history queries search indexed schema-8 rows and
return an omission when older rows were not indexed. A later explicit bounded
maintenance operation requires its own design, input-version choice, and
operator action.

### R4. Add bounded same-fingerprint history

Implement an opt-in query with a small hard maximum. It returns only safe
summary fields: job and run IDs, time, outcome, stable classes, fingerprint,
and whether a later rerun succeeded. It excludes names, specifications, paths,
environment, logs, notifier data, and unrelated diagnostic text.

Use a single indexed query with deterministic order. Add privacy tests with
unrelated secret-canary jobs and performance tests over a large synthetic
store.

### M4 release gate

- migration, backup, restore, native resource, fingerprint, query-plan, and
  privacy tests pass;
- the companion remains compatible with evidence bundles that omit every new
  fact;
- optional new fact codes do not require an evidence schema bump unless their
  semantics change a required invariant; and
- the persisted-schema and platform-capability references are updated.

## 11. M5: structured generated augmentation

Implementation status: G1 through G6 are implemented with offline conformance,
fake-server, helper-process, secret-canary, and failure-path tests. The live
hosted/self-hosted evaluation portion of the M5 release gate remains open
because it requires explicit external provider configuration and credentials.

### G1. Freeze the proposal protocol

Define versioned structured request and proposal-response schemas. A request
contains only the approved evidence projection, deterministic candidates,
allowed categories and action IDs, output limits, untrusted-data instructions,
and the response JSON schema.

A response may propose hypotheses, summaries, alternatives, missing evidence,
and IDs from the deterministic action catalog. It cannot contain a report,
executable argument vector, new action, retry verdict, lifecycle fact, or tool
request.

Keep the JSON Schema as a reviewed embedded artifact and test it against the Go
validator. Provider-native schema enforcement is preferred, but every response
is decoded and semantically validated again in Go.

### G2. Implement strict companion configuration

Add an independently versioned `diagnosis.yml` with a default and named profiles. A profile
selects provider, locality, endpoint or absolute command and arguments, model,
mandatory schema enforcement, disclosure classes, deadlines, input and output
limits, and secret references. Mode selection remains a CLI concern.

Use strict decoding and explicit precedence. Literal credentials are rejected.
Initial secret resolvers may support environment and private files; OS keychain
support is a later adapter. A malformed model file must not block
`--deterministic`.

### G3. Implement disclosure projection

Before invoking a generator:

1. start from the already sanitized failure evidence;
2. select only profile-allowed and invocation-approved disclosure classes,
   with metadata implied by AI activation and log content remaining explicit;
3. fail closed if requested log content lacks core's sealed value-aware
   redaction capability;
4. enforce per-class item and byte ceilings;
5. preserve typed values and convert invalid artifact bytes to explicit UTF-8
   replacement characters;
6. record item IDs, classes, counts, byte counts, provider locality, and
   redaction status in the disclosure manifest; and
7. compute the exact request digest.

The provider receives typed data, never a concatenated prompt containing
artifact text as instructions. No prompt, raw provider response, or credential
is written to Jobman's state.

### G4. Implement provider conformance and adapters

Build all adapters against one conformance suite covering descriptor
negotiation, schema support, deadlines, cancellation, size limits, malformed
responses, redirects, TLS, nonzero command exits, and provenance.

Recommended order:

1. bounded absolute-command bridge using one request on stdin and one response
   on stdout;
2. OpenAI-compatible structured HTTP adapter usable with an explicitly
   configured hosted or self-hosted endpoint;
3. Ollama adapter; and
4. additional hosted or self-hosted transports only when they add behavior not
   expressible through the prior adapters.

HTTP tests use local fake servers; live providers are never part of required
unit tests. Adapters must not discover endpoints, silently change locality,
follow unsafe redirects, remove TLS verification, or fall back from local to
hosted service.

### G5. Reconcile generated proposals

Extend the engine modes only after provider-independent proposal validation is
complete. Reconciliation must:

- reject invalid or undisclosed citations;
- retain observed and exact deterministic findings;
- record contradictions explicitly;
- resolve actions only through the deterministic catalog;
- ignore generated retry verdicts;
- merge equivalent candidates while retaining provenance;
- rank exact, calibrated, and uncalibrated results in the designed order; and
- turn optional provider failure into a warning and deterministic report.

`--require-model` converts provider failure to an operation failure. It does
not relax validation.

### G6. Complete the generated-output security review

Before general availability, test:

- prompt injection in every evidence and enrichment class;
- invented citations, paths, URLs, flags, commands, and identifiers;
- oversized, deeply nested, duplicate-key, non-finite, and trailing JSON;
- attempts to erase facts or request mutations;
- remote disclosure without explicit approval;
- proxy, redirect, DNS, TLS, and local-command boundary behavior;
- credential redaction from errors and debug output; and
- deterministic fallback during every provider failure mode.

### M5 release gate

At least one explicit hosted profile and one self-hosted profile must pass
recorded-response evaluation and adapter conformance before release.
Documentation states provider
data handling, exact disclosed classes, uncalibrated confidence semantics, and
the absence of automatic remediation. Live provider tests remain opt-in.

## 12. M6: evaluation and optional enrichment

### E1. Build the offline evaluation corpus continuously

Implementation status: a versioned synthetic corpus, Go runner, safety and
correctness metrics, deterministic CI gate, and explicit live-provider mode
are implemented. Live release-candidate runs still require external provider
configuration and retained evaluation evidence.

Begin with M0 fixtures and grow a checked-in synthetic, nonsecret corpus. Each
case records:

- evidence and enrichment input;
- accepted and forbidden primary or alternative codes;
- required and forbidden citations and actions;
- expected retry state;
- confidence bounds and calibration class; and
- the failure mode the case is intended to detect.

Measure deterministic and generated behavior separately. Required metrics are
primary-code precision, unsupported-claim rate, citation validity, safe-action
rate, retry-advice accuracy, abstention quality, deterministic stability, and
provider failure fallback.

Recorded provider responses permit offline regression tests. Live evaluation
is manual or scheduled with explicit credentials and budget; it never gates a
normal source pull request.

### E2. Add optional Python analysis tooling

If Go test reports become insufficient, add an isolated evaluation environment
for dataset analysis, calibration, provider comparison, and report generation.
Python outputs are advisory artifacts. A rule, threshold, prompt, or taxonomy
change enters production only after review and conversion to versioned Go-owned
code or fixtures.

### E3. Add system-event collectors

Design and implement one platform at a time. Collectors are opt-in, read-only,
unprivileged, time-windowed, allowlisted, and bounded. They report their query,
coverage, source, and limitations. Linux journal, macOS unified log, Windows
Event Log, and container events each require native fixtures and privacy review.

Do not parse unrelated system history, request elevation, or treat a missing
event as proof that an event did not occur.

### E4. Add support bundles

Implementation status: schema-1 deterministic private archives, dry-run
inventories, per-member hashes, build/capability metadata, no-overwrite
creation, and exclusion tests are implemented in the companion.

Build a deterministic, private archive containing selected evidence, diagnosis
report, version information, capability facts, disclosure manifest, and a
human-readable inventory. Exclude provider credentials, target environment,
unapproved logs, database files, and the fingerprint key. Refuse overwrite by
default and make every included file visible before creation through a dry-run
inventory.

### E5. Evaluate local semantic retrieval separately

Embeddings and a companion-local similarity index require a separate accepted
design covering source consent, storage permissions, deletion, model choice,
reindexing, portability, disclosure, and poisoning. They remain disabled by
default and must not replace the core factual fingerprint query.

### E6. Defer mutating workflows

Batch diagnosis, `run --wait --diagnose-on-failure`, report-driven rerun, and
automatic remediation each require separate CLI and authorization reviews.
The initial companion remains read-only. No model receives tools or authority
to signal, edit, clean, repair, rerun, or notify.

## 13. Cross-cutting test matrix

| Layer | Required evidence |
| --- | --- |
| Public schemas | Goldens, round trips, unsupported versions, additive fields, bounds, duplicate detection, fuzzing, canonical digests |
| Core classifications | Exact OS and policy mappings, legacy decoding, no raw-error persistence, platform distinctions |
| Snapshot and collector | Transaction consistency, truncation, cancellation, active logs, pruning, corruption, redaction, no unintended writes |
| Extension protocol | Built-in precedence, `PATH`, flags, streams, environment, nested dispatch, signals, child status, three platforms |
| Companion engine | Stable ranking, contradictory evidence, valid citations, action allowlist, retry matrix, injected clock |
| Enrichment | Exact byte ranges, bounded parsing, invalid UTF-8, hostile formats, provenance |
| Providers | Conformance fakes, schema negotiation, transport limits, TLS and redirect policy, command bridge, deterministic fallback |
| Security | Secret canaries, prompt injection, unsafe actions, oversized input/output, export permissions, no telemetry |
| Compatibility | Oldest supported core fixture with newest companion and newest compatible core fixture with oldest supported companion |
| Release | CGO-free cross-builds, native Linux/macOS/Windows tests, race detector, SBOM, signatures, installation smoke tests |

Tests must use injected clocks, fake generators, local HTTP servers, helper
processes, temporary stores, and synthetic logs. Required tests never use a
developer's home directory, real provider credentials, shared state, or an
external network.

## 14. Documentation and release work

### Jobman core

Update, as each milestone becomes real:

- command help, man pages, completions, and JSON output reference;
- compatibility and security documents;
- troubleshooting and support guidance;
- persisted-schema and platform-capability references when M4 lands;
- installation documentation explaining optional extensions;
- changelog and release notes; and
- the design status so proposed behavior is never described as implemented
  early.

### Companion

Maintain:

- installation and direct/extension invocation guides;
- evidence and report compatibility matrix;
- deterministic analyzer and confidence reference;
- provider profile, credential, locality, and disclosure documentation;
- privacy, threat model, support, and security-reporting guidance;
- machine-schema references and fixture provenance;
- changelog and migration notes; and
- checksums, signatures, SBOMs, and native package instructions.

Documentation examples that contain Jobman argument vectors must be checked
against the appropriate command tree. Provider examples must make network
disclosure visible and must not include real credentials.

## 15. Rollout, compatibility, and rollback

Recommended release order:

1. release Jobman with evidence schema 1 but no extension dispatch if those
   reviews complete separately;
2. release the deterministic companion for direct invocation;
3. release generic external dispatch and document natural invocation;
4. release richer core facts and the state migration;
5. release generated augmentation as explicit opt-in; and
6. promote generated support only after offline evaluation and security gates.

Compatibility rules:

- a companion never relies on matching Jobman semantic versions;
- explicit schema and capability fields govern behavior;
- the core never reads diagnosis reports;
- evidence schema additions remain optional within a version only when old
  consumers can safely ignore them;
- a changed required meaning increments the evidence or report schema;
- provider and analyzer changes record their own versions in the report; and
- old state events remain valid legacy evidence.

Rollback rules:

- M1 through M3 are additive and have no database migration; users may disable
  extensions or uninstall the companion without affecting jobs;
- provider profiles can be removed without affecting deterministic diagnosis;
- a model outage never affects core execution or evidence collection;
- M4's database migration follows Jobman's normal forward-only schema policy,
  so rollback requires restoring the pre-upgrade backup with a compatible
  older binary; and
- reports and exported evidence are advisory files and never need a core state
  rollback.

## 16. Program completion criteria

The initial complete feature comprises M0 through M5. It is complete only when:

1. Jobman emits sealed, bounded, versioned evidence for every durable outcome.
2. The companion produces a useful deterministic report with no network or
   configuration.
3. Every diagnosis and action has valid citations or an explicit external-
   knowledge label.
4. Retry advice is deterministic, agrees with recorded policy, and does not
   mutate state.
5. Installed direct and extension invocation have equivalent behavior.
6. Resource and fingerprint facts never overstate platform scope or certainty.
7. Similar-history evidence is explicit, indexed, bounded, and privacy tested.
8. Generated proposals cannot replace facts, create executable actions, own
   retry advice, or bypass disclosure policy.
9. Optional provider failure preserves a valid local report unless the user
   explicitly requires the provider.
10. Schema, migration, native platform, race, fuzz, security, documentation,
    packaging, and compatibility gates pass on release commits.
11. No core execution path, package, state, or configuration contains a model
    SDK, prompt, embedding, provider credential, or inference dependency.
12. `show`, `doctor`, notifications, lifecycle behavior, and frozen v1 JSON
    remain compatible.

M6 enrichments are separate post-completion improvements. None can weaken the
read-only, explicit-disclosure, bounded-operation, or deterministic-fallback
requirements.

## 17. First executable backlog

The recommended initial issue and pull-request order is:

1. Accept the evidence vocabulary and external-command ADR.
2. Add core diagnostic types and legacy event compatibility.
3. Add the public evidence package, goldens, decoder fuzz target, and digest.
4. Add the store diagnostic snapshot and transaction-consistency tests.
5. Add bounded log tails and artifact metadata.
6. Add the application collector and redaction tests.
7. Add `show evidence`, JSON goldens, assembled tests, and documentation.
8. Bootstrap the companion release repository and compatibility fixtures.
9. Add core evidence acquisition and offline import.
10. Add report contracts, engine, action catalog, and retry evaluator.
11. Add deterministic analyzers and bounded artifact enrichment.
12. Add companion CLI, rendering, private export, packages, and documentation.
13. Add core external dispatch and direct/extension parity tests.
14. Release and dogfood the offline path before starting the state migration.
15. Add resource observation types and native capability tests.
16. Add the reviewed migration, key lifecycle, fingerprints, and similarity.
17. Add proposal schemas, companion configuration, and disclosure projection.
18. Add provider conformance, command bridge, structured HTTP, and Ollama
    adapters.
19. Add generated reconciliation, security tests, and recorded-response
    evaluation.
20. Release generated augmentation as explicit opt-in, then prioritize M6 from
    measured gaps rather than feature speculation.
