# Diagnostic evidence and optional diagnosis companion

Status: implemented post-v1 extension; unreleased
Contract status: core evidence, deterministic companion, external dispatch,
resource observations, failure fingerprints, exact similar history, generated
augmentation, and the initial provider adapters are implemented in the
coordinated repositories. Selected-artifact exact-range enrichment, offline
evaluation, and private support bundles are also implemented. Live provider
release-candidate evaluation and the first signed release remain gates; none
of this changes the frozen v1 specification.
Last updated: 2026-08-09

Implementation sequencing, work packages, compatibility gates, and release
criteria are defined in the
[diagnosis implementation plan](DIAGNOSIS_IMPLEMENTATION_PLAN.md).

This document specifies deterministic diagnostic evidence in Jobman plus a
separately installed `jobman-diagnose` Go project for explaining why a managed
command failed, is blocked, or is behaving unexpectedly. Jobman's external
subcommand mechanism makes the companion available naturally as
`jobman diagnose`. The release, dependency, configuration, and trust boundaries
remain separate.

The design deliberately separates factual evidence produced by Jobman from
conclusions produced by deterministic rules or generative models. Core Jobman
contains no model SDK, prompt, provider configuration, embedding machinery, or
AI-specific runtime dependency.

The implemented slices build a stable diagnosis input from lifecycle, event,
policy, exit, log, wait, admission, notification, process resource, and exact
store-local failure-history records. Generated augmentation uses an explicit
companion-only profile and disclosure projection; it remains optional and
cannot alter core facts or lifecycle state.

The existing commands keep their meanings:

- `show` reports detailed recorded state;
- `doctor` checks the Jobman store and platform and performs only explicitly
  authorized conservative repair; and
- the optional `diagnose` companion interprets evidence about one job or run
  and is read-only.

## 1. Outcomes and constraints

The feature should answer these questions:

- What most likely happened?
- Which facts support or contradict that explanation?
- How confident is Jobman, and what does that confidence mean?
- What information is missing?
- Which safe next actions are available?
- Would the existing policy retry, and should the user rerun now, wait, make a
  change first, or not rerun?
- Did the target fail, did Jobman fail to manage it, or did an unrelated
  subsystem such as notification delivery fail?

The design MUST:

- work offline and remain useful without a model provider;
- produce a bounded, versioned, machine-readable evidence bundle and report;
- keep persisted lifecycle state authoritative and all diagnoses advisory;
- preserve redaction and minimize disclosure, especially to remote models;
- support active, waiting, queued, successful, failed, timed-out, cancelled,
  aborted, submission-failed, and lost jobs;
- remain deterministic for a fixed evidence bundle when only deterministic
  analyzers are selected;
- permit models and additional analyzers to be substituted in tests or by a
  host application;
- degrade to a deterministic report when an optional model is unavailable;
- allow the diagnosis component to be installed and upgraded independently;
- respect context cancellation and bound all file reads, process execution,
  model input, model output, and elapsed time; and
- preserve the single-binary, CGO-free, cross-platform core Jobman release.

The first release MUST NOT:

- let a model mutate state, signal a process, rerun a command, edit a file, or
  execute a suggested shell command;
- treat model output as a lifecycle transition, retry-policy decision, or
  security decision;
- silently send command text, paths, logs, environment values, or job metadata
  to a remote service;
- claim that an uncalibrated score is a probability;
- store raw model prompts or responses in the Jobman database;
- require network access, an account, an API key, or a background daemon; or
- use Go's dynamic `plugin` mechanism, which would undermine portability and
  create a compiler-version ABI dependency.

## 2. Terminology

- **Diagnostic record**: a stable code and safe structured attributes recorded
  by core Jobman when it observes a failure or abnormal condition.
- **Evidence item**: one factual, attributable observation in a diagnostic
  evidence bundle.
- **Evidence bundle**: a bounded, immutable core Jobman snapshot and the only
  data passed across the core/companion boundary.
- **Failure evidence**: a companion-owned read-only view containing one sealed
  core evidence bundle plus separately attributed optional enrichment.
- **Finding**: a directly supported statement, such as “run 3 exited with code
  127.”
- **Hypothesis**: an inferred possible cause that can be uncertain.
- **Diagnosis**: one ranked finding or hypothesis with confidence, supporting
  and contradicting evidence, and proposed actions.
- **Action**: an advisory next step. Only deterministic, allowlisted actions may
  contain an executable argument vector.
- **Retry advice**: a separate assessment of Jobman's current policy and the
  safety or usefulness of creating another run or job.
- **Analyzer**: a deterministic implementation that maps evidence to findings,
  hypotheses, and action identifiers.
- **Generator**: an optional generative model adapter that proposes additional
  hypotheses, explanations, and missing evidence through a strict protocol.
- **Projection**: the subset of an evidence bundle disclosed to one analyzer or
  generator.
- **Extension command**: a separately installed `jobman-NAME` executable that
  Jobman invokes for an otherwise unknown `jobman NAME` subcommand.

“Diagnostic” refers to factual data and infrastructure. “Diagnosis” refers to
the act or result of interpreting that data. This distinction prevents the
existing `doctor` command and ordinary standard-error diagnostics from being
confused with the new feature.

## 3. Architecture

The system has a hard process and project boundary:

```text
Jobman core repository and executable
  deterministic execution, stable failure records, fingerprints, evidence JSON
       |
       | jobman show evidence JOB --json
       v
jobman-diagnose companion repository and executable
  deterministic analyzers, evidence enrichment, report validation, prompts
       |
       | provider-neutral structured generation
       v
optional inference backend
  hosted API, Ollama, vLLM, llama.cpp, SGLang, or another adapter
```

Jobman owns facts and the evidence format. The companion owns inference and the
report format. The core never imports the companion. The companion may import a
small public Jobman evidence-contract package or consume the same stable JSON
through the CLI. It does not import `internal/app`, `internal/store`, or any
other Jobman internal package.

The companion is useful without a model: deterministic analyzers, action
selection, retry advice, evidence export, and report rendering all work
offline. Model providers are optional companion dependencies or separately
configured inference endpoints.

Recommended source layout in Jobman:

```text
diagnostic/
  evidence.go       evidence types, schema version, and validation
  codec.go          bounded strict decoding and canonical digest helpers

internal/model/diagnostic.go
                    stable core diagnostic records and code catalog
internal/app/diagnostic.go
                    evidence collector and application boundary
internal/store/events.go
                    transactionally consistent diagnostic snapshot queries
jobman/show.go      built-in evidence JSON command
jobman/external.go  external-subcommand resolution and direct execution
```

Recommended layout in the separate `jobman-diagnose` Go module:

```text
cmd/jobman-diagnose/
                    executable entry point and extension protocol handling
diagnosis/
  contract.go       diagnosis report and proposal contracts
  engine.go         orchestration, reconciliation, ranking, and limits
  analyzer.go       analyzer interface and built-in deterministic analyzers
  generator.go      narrow structured-generation interface
  actions.go        allowlisted action catalog and retry advice
provider/
                    optional hosted and self-hosted model adapters
internal/config/
                    companion-only profiles, disclosure, and credentials
```

Both projects may be implemented entirely in Go. Python is not embedded in the
core or companion binaries; it is appropriate for offline evaluation,
prompt/model benchmarking, failure-dataset analysis, and future ML experiments.

### 3.1 Application boundary

The existing minimal `app.Backend` interface should not grow. As with query,
lifecycle, cleanup, input, and doctor support, diagnosis uses an optional
capability:

```go
type DiagnosticBackend interface {
    DiagnosticEvidence(
        context.Context,
        diagnostic.EvidenceRequest,
        diagnostic.Sanitizer,
    ) (diagnostic.Evidence, error)
}
```

`EvidenceRequest` contains only collection choices: selector, selected run or
runs, explicit executable-identity inclusion, log policy, byte budgets, and
optional health facts. `Sanitizer` is a
narrow byte-oriented interface prepared by the CLI from the current redaction
policy; it does not expose the policy or its patterns. Neither argument contains
model or prompt options. This keeps evidence collection independent from how
the evidence will be interpreted.

The built-in evidence command performs this sequence:

1. resolve the selector and do the same bounded stale-state reconciliation as
   existing inspection;
2. collect a consistent metadata snapshot and bounded artifact snapshots;
3. apply field-aware redaction and attach disclosure classifications;
4. validate and seal the evidence bundle with a content digest;
5. encode the evidence in Jobman's stable JSON envelope; and
6. write it to standard output without invoking the companion or a model.

The companion invokes this command through the absolute core executable path
provided by the extension protocol. It receives JSON, not a backend callback.
This process boundary prevents the inference engine from reaching the live
store, process controls, configuration authority, or secret providers.

### 3.2 Consistency

The current inspection service loads the job and runs together, then loads
runtime, dependency, admission, wait, and notification records through
additional reads. A diagnosis could otherwise combine different revisions.
The store should add one read-transaction query that returns all metadata and
the last relevant lifecycle events from one SQLite snapshot.

Log bytes cannot share a transaction with SQLite. Each log artifact therefore
records the path role, selected run, byte range, observed size, digest,
truncation, and read time. A terminal run with valid finalized logs can be
marked `stable`; an active or changing log is `point_in_time`. The report MUST
not imply that a point-in-time excerpt is complete.

Evidence from an active job includes its captured revisions and an
`active_state_may_have_advanced` limitation. A later diagnosis naturally has a
different evidence digest.

### 3.3 Independent versioning

Jobman, `jobman-diagnose`, the evidence schema, the diagnosis report schema,
the structured-generation protocol, and the external-command protocol have
independent versions. A report records all versions that affected it.

The companion declares the evidence schema versions it accepts and fails with
an actionable compatibility error for a newer unsupported bundle. Core Jobman
does not inspect or migrate diagnosis reports. The companion does not infer
capability from a Jobman release string when the evidence bundle already
provides explicit schema and capability fields.

The companion can therefore ship providers, rules, prompts, and evaluation
improvements on its own schedule. A Jobman release changes only when factual
capture, the evidence contract, or generic external-command behavior changes.

### 3.4 External subcommands

Jobman should support independently installed extensions using the established
executable naming convention:

```text
jobman NAME ARG...  ->  jobman-NAME ARG...
```

Dispatch occurs only after built-in command resolution fails. A built-in name
always wins and can never be shadowed. `NAME` must be one lowercase command
token containing ASCII letters, digits, and internal hyphens; slashes, dots,
empty segments, and private names beginning with an underscore are rejected.
Jobman uses the operating system's normal `PATH` lookup for the exact
`jobman-NAME` executable and never invokes a shell.

Jobman's persistent `--state-dir` and `--config` options remain reserved even
when they appear around an external command. The dispatcher resolves them with
normal precedence, removes them from the extension argument vector, and sends
their effective nonsecret context through the protocol below. All other child
arguments retain their order and boundaries.

The child inherits the command's standard input, output, and error streams and
receives cancellation and termination signals. Unix implementations should
replace the client process when safe; other platforms wait for the child and
forward signals. Jobman returns the extension's exit status without translating
a diagnosis result into a core lifecycle status.

A small versioned environment contract supplies only nonsecret invocation
context:

- `JOBMAN_EXTENSION_PROTOCOL=1`;
- `JOBMAN_EXECUTABLE`, the absolute path of the dispatching core binary;
- `JOBMAN_VERSION`;
- `JOBMAN_STATE_DIR`, the effective canonical state directory; and
- `JOBMAN_CONFIG`, only when the user explicitly selected a configuration
  path.

The companion uses `JOBMAN_EXECUTABLE`, never a fresh `PATH` lookup, when it
calls `jobman show evidence`. It treats all environment context as untrusted
input and validates the protocol version before doing work. The protocol does
not carry configuration contents, credentials, inherited target environment,
or log data.

Extensions may implement a bounded `--jobman-extension-info` response for an
explicit future `jobman extensions` inspection command. Jobman does not execute
every discovered binary to populate ordinary root help or completions. It does
not download extensions, search the current directory unless it is explicitly
on `PATH`, or maintain a hidden plug-in directory.

`jobman NAME --help` is forwarded normally. An extension packages its own shell
completion files or implements the documented completion request; core Jobman
does not run every program on `PATH` during tab completion.

An extension executable is trusted code running with the user's authority.
The documentation must warn that an otherwise unknown command may resolve to a
`jobman-NAME` program on `PATH`. `--no-extensions` and
`JOBMAN_NO_EXTENSIONS=1` disable dispatch for restricted automation. Tests
cover name validation, built-in precedence, duplicate `PATH` entries, loops,
argument boundaries, signals, and exit-status preservation.

The first companion executable is `jobman-diagnose`. If it is installed on
`PATH`, users invoke it as `jobman diagnose JOB`; direct invocation remains
available for debugging and environments that disable extensions.

### 3.5 Distribution

`jobman-diagnose` has its own module, release version, changelog, checksums,
signatures, SBOM, and native archives. Installing Jobman does not install the
companion, and upgrading one does not silently upgrade the other. The companion
binary is named exactly `jobman-diagnose` and may publish packages for the same
platforms as Jobman without becoming part of Jobman's release matrix.

The companion exposes its own `--version` and reports the evidence versions it
accepts. Release notes maintain a tested compatibility matrix rather than
requiring equal semantic versions. Jobman's installation and troubleshooting
documentation may link to the companion, but core help must remain accurate
when it is absent.

## 4. Core diagnostic records

Current snapshots contain useful string codes such as
`supervisor_lease_expired`, and event details sometimes contain a reason or
diagnostic code. New code should stop passing arbitrary strings between core
packages. `internal/model/diagnostic.go` should define a validated string type,
constants, and a versioned safe detail record:

```go
type DiagnosticCode string

type DiagnosticRecord struct {
    SchemaVersion int               `json:"schema_version"`
    Code          DiagnosticCode    `json:"code"`
    Origin        DiagnosticOrigin  `json:"origin"`
    Operation     string            `json:"operation"`
    Category      DiagnosticCategory `json:"category"`
    RetryClass    RetryClass        `json:"retry_class"`
    Attributes    map[string]string `json:"attributes"`
}
```

The exact Go formatting may change during implementation, but the following
semantics are required:

- `Code` is stable, namespaced, lowercase snake case, and documented;
- `Origin` distinguishes `target`, `jobman`, `platform`, `policy`,
  `dependency`, `storage`, `logging`, and `notification`;
- `Category` supports grouping without parsing the code;
- `RetryClass` is an observed-condition classification, not a command to rerun;
- attributes come from a per-code allowlist and never contain raw errors,
  command text, environment values, log content, notification destinations, or
  unclassified paths; and
- an unknown code remains representable and never makes evidence decoding
  fail.

New events store this record in `state_events.details_json` while retaining the
existing snapshot code columns and legacy detail members required by v1
readers. The details decoder accepts both the current legacy shape and the new
versioned shape. Because the column already contains JSON, this first change
does not require a database migration.

The initial catalog should cover at least:

- supervisor launch, claim, lease, and identity failures;
- executable lookup, working-directory, permission, and process-start errors;
- exit, signal, run timeout, job timeout, cancellation, and lost ownership;
- retry exhaustion, abort deadlines, and retry scheduling;
- dependency mismatch, failed wait probes, and admission configuration;
- log open, write, sync, index, rotation, and recording degradation;
- state safety, database busy, disk capacity, and integrity problems; and
- notification transport, rejection, timeout, command exit, and exhaustion.

OS errors are reduced to stable categories such as `not_found`,
`permission_denied`, `resource_exhausted`, `temporarily_unavailable`, and
`unknown`. Raw `error.Error()` text remains an immediate redacted diagnostic,
not durable structured data.

### 4.1 Additional factual capture

The evidence collector should expose existing facts before adding persistence.
Later state-schema changes are justified only for facts that cannot be
reliably reconstructed. Useful additions include:

- target termination source, with an explicit distinction between an observed
  signal and a shell convention such as exit code 137;
- platform-provided resource termination facts, such as a Windows Job Object
  limit or Linux control-group out-of-memory event, when safely available;
- elapsed run time and timeout boundaries derived from recorded timestamps;
- retry counts remaining and the policy decision that followed each run;
- log recording degradation separately from target failure; and
- store free-space or write-failure category at the time Jobman observes it.

Jobman MUST NOT infer “out of memory” solely from exit code 137, infer “command
not found” solely from exit code 127 when the target was a shell, or infer a
network failure solely from arbitrary stderr text. Such clues may support a
lower-confidence hypothesis, but are not facts.

### 4.2 Deterministic core classification

Core Jobman should classify conditions that it can establish from direct
observations. Initial stable classes include:

- executable not found or not executable at direct process start;
- working directory missing or inaccessible;
- permission denied by a specific Jobman operation;
- run timeout, job timeout, and user cancellation;
- observed signal or platform termination reason;
- confirmed resource-limit or out-of-memory termination;
- supervisor ownership loss or unverifiable process identity;
- dependency predicate mismatch, wait abort, and admission rejection;
- retry budget exhaustion; and
- degraded or failed state and log recording.

These are low-level factual classes, not prose diagnoses. Each classification
cites the Jobman event or snapshot fields that establish it and states whether
it is `confirmed`, `derived_exact`, or `unknown`. Core code does not parse
arbitrary target logs to assign a root cause. Tool-specific text signatures,
cross-run reasoning, likely causes, confidence ranking, actions, and natural
language belong to the companion.

### 4.3 Resource observations

Where an operating system provides reliable data without continuous sampling,
Jobman should record bounded structured resource observations at run exit. The
portable shape includes a stable metric code, integer or decimal value, unit,
scope (`process` or `tree`), observation source, and completeness.

Candidate facts include elapsed time, user and system CPU time, peak resident
memory, bytes read or written, and a platform resource-limit event. Unsupported
metrics are omitted with a capability fact; zero is never used to mean
unavailable. Platform adapters must distinguish a process-only measurement
from a whole-tree measurement.

Confirmed Linux control-group memory events, container runtime reasons, macOS
process information, or Windows Job Object limit events may support an exact
resource classification. Exit-code conventions and free-form log text alone do
not. Adding durable resource columns or an observation table requires a new
state migration and upgrade tests; the evidence schema can represent these
facts before every platform implements them.

## 5. Diagnostic evidence contract

The evidence schema and report schema have independent integer versions. They
are also independent from the CLI JSON envelope, configuration schema, job
specification schema, notification schema, and database schema.

An evidence bundle has this conceptual shape:

```json
{
  "kind": "jobman.diagnostic_evidence",
  "schema_version": 1,
  "evidence_id": "sha256:...",
  "captured_at": "2026-08-08T14:30:00Z",
  "source": {
    "jobman_version": "1.2.0",
    "collector_version": "1.0.0",
    "store_schema_version": 7,
    "platform": "linux"
  },
  "subject": {
    "job_id": "01980f4c-7b2a-7a6f-8c10-0123456789ab",
    "job_revision": 9,
    "selected_runs": [3],
    "phase": "completed",
    "outcome": "failure"
  },
  "consistency": {
    "metadata": "transactional_snapshot",
    "artifacts": "stable",
    "active_state_may_have_advanced": false
  },
  "items": [
    {
      "id": "ev:run:3:exit",
      "code": "jobman.run.exit.code",
      "value": 2,
      "observed_at": "2026-08-08T14:29:58Z",
      "source": {
        "kind": "run_snapshot",
        "entity_id": "01980f4d-...",
        "revision": 3
      },
      "quality": "observed",
      "disclosure": "metadata"
    }
  ],
  "artifacts": [],
  "omissions": [
    {
      "code": "log_content_not_requested",
      "affects": ["run:3:stderr"]
    }
  ],
  "redaction_notices": [],
  "limits": {
    "item_count": 42,
    "encoded_bytes": 18320,
    "log_bytes": 0
  }
}
```

The final contract should use concrete typed Go fields rather than arbitrary
maps for envelope, subject, source, consistency, limits, artifacts, omissions,
and redaction notices. `EvidenceItem.Value` may be `json.RawMessage` so new
namespaced fact codes can be added without changing the envelope. Every registered fact
code documents its value type and units.

### 5.1 Required evidence domains

The collector creates items for:

- job identity, revision, phase, outcome, timestamps, and safe diagnostic
  records;
- selected run identity, revision, phase, outcome, timestamps, exit facts,
  resolved executable classification, and log health;
- core deterministic failure class, classification quality, resource
  observations, and normalized failure fingerprint when available;
- normalized completion, retry, delay, wait, timeout, and admission policy;
- run counters, next eligibility time, wait reason, and pause state;
- dependency predicates and observed terminal outcomes;
- wait-condition kind, attempt count, result, and safe diagnostic code;
- current admission request or lease and known capacity, without exposing
  unrelated jobs;
- notification failures as secondary findings, clearly separated from target
  outcome;
- relevant immutable lifecycle events in occurrence order;
- Jobman version, platform, and applicable capability facts; and
- requested bounded log artifacts and their capture quality.

Command arguments, environment values, paths, and log text are not ordinary
metadata. When locally collected, they receive a stricter disclosure class.
The evidence contract stores why a field is absent so an analyzer can
distinguish “not collected,” “redacted,” “pruned,” “unsupported,” and “not
applicable.”

### 5.2 Stable identity and encoding

- Timestamps are UTC RFC 3339 with nanosecond precision.
- Durations are canonical Go duration strings.
- Byte sizes and counters are nonnegative integers.
- Item and artifact arrays use a documented stable order.
- Item and artifact IDs are unique within a bundle and are the only valid
  citation targets.
- Maps used in a content digest have lexically ordered keys.
- The evidence digest excludes `evidence_id` itself and collection wall time,
  but includes source revisions, selected byte ranges, artifact digests,
  omissions, redaction notices, and every semantic item.
- Invalid UTF-8 log bytes are not coerced into text. They remain a bounded
  base64 artifact or are omitted from model projections.

Decoders reject an unsupported newer schema version, duplicate item IDs,
invalid citations, invalid JSON values, excessive nesting, oversized input,
non-finite numbers, trailing values, and inconsistent limits. Within a
supported schema, additive noncritical fields are ignored by older consumers.
A new required semantic or a changed meaning increments the schema version.

### 5.3 Collection budgets

Defaults should favor predictable local operation:

- metadata and event evidence: at most 1 MiB encoded;
- lifecycle history: all selected-run events plus a bounded surrounding job
  window;
- log mode: `metadata` by default;
- an explicitly requested tail: at most 64 KiB per selected stream by default;
- absolute log-content ceiling: 1 MiB per diagnosis;
- one artifact records its original size, selected range, truncation, and
  digest; and
- exceeding a budget creates an omission instead of silently dropping data.

The implementation should tune these numbers with tests, but all limits remain
finite and visible in output.

### 5.4 Previous attempts and similar failures

All runs of the selected job are eligible as bounded local evidence because
they share one immutable job specification. The default bundle includes compact
facts for previous attempts and artifacts only for the selected run. This makes
repeated exit codes, changing durations, retry exhaustion, and a changed
failure fingerprint visible without duplicating logs.

Evidence from unrelated jobs is opt-in. `EvidenceRequest` may request at most a
small fixed number of terminal runs with the same core fingerprint. Returned
matches contain job/run IDs, times, outcomes, stable classes, fingerprint, and
whether a later rerun succeeded; they do not contain names, command text,
paths, environment, or logs. The request never performs semantic or embedding
search in core Jobman.

The companion may enrich a bundle with additional user-approved local history
or a separately maintained similarity index. Embeddings, vector stores, and
model-based retrieval remain companion concerns, are disabled by default, and
must not cause unrelated job content to be disclosed to a remote model without
separate approval.

### 5.5 Companion enrichment

The companion may wrap, but never rewrite, the sealed core bundle with a
versioned analysis envelope. Additional items use a separate ID namespace and
record collector name, version, source, capture time, disclosure class, and
quality. The envelope retains the original `evidence_id`, so a consumer can
separate Jobman facts from companion observations and derivations.

Useful bounded enrichment includes:

- extracting tracebacks or structured error chains from already selected log
  ranges;
- comparing previous attempts and explicitly requested similar fingerprints;
- reading a small time-correlated window of user-accessible platform or
  container events; and
- adding local tool-version or dependency metadata through an allowlisted,
  non-shell collector.

System-event collection is opt-in, platform-specific, read-only, and never
requests elevated privileges. It records its query window and reports when
coverage is incomplete. An enrichment collector cannot execute a suggested
remediation, read arbitrary files, or expand the core log-byte budget. Derived
traceback or signature items cite the core artifact and exact byte range from
which they came.

## 6. Companion diagnosis package

The companion exposes a small domain interface that is independent from model
transport:

```go
type Diagnostician interface {
    Diagnose(
        context.Context,
        FailureEvidence,
    ) (Diagnosis, error)
}

type Analyzer interface {
    Descriptor() AnalyzerDescriptor
    Analyze(context.Context, EvidenceView) ([]Candidate, error)
}

type StructuredGenerator interface {
    Descriptor(context.Context) (GeneratorDescriptor, error)
    Generate(context.Context, StructuredRequest) (StructuredResponse, error)
}

type Options struct {
    Analyzers         []Analyzer
    Generator         StructuredGenerator
    Mode              Mode
    Limits            Limits
    Clock             Clock
    RequireGenerator  bool
}

func New(Options) (*Engine, error)
func (e *Engine) Diagnose(
    context.Context,
    FailureEvidence,
) (Diagnosis, error)
```

`FailureEvidence` retains the sealed `diagnostic.Evidence` plus attributed
enrichment items. `EvidenceView` is read-only and supplies typed helpers for
registered evidence codes. An analyzer returns candidates, not a finished
report. The engine owns validation, duplicate handling, ranking, action
resolution, retry advice, warnings, and final schema encoding.
`StructuredGenerator` is intentionally not a universal chat, tool, embedding,
streaming, or agent interface. It performs one bounded schema-directed
generation operation.

The implemented modes are `deterministic`, the default, and explicit generated
augmentation selected by `--ai`, `-a`, `--ai-logs`, or `--profile NAME`. An accepted proposal produces a
`mixed` report because deterministic findings, actions, and retry policy remain
authoritative. If an optional generator fails, the result remains a sealed
deterministic report with a warning. There is no mode that disables
deterministic safety checks or lets a generator own retry advice.

### 6.1 Deterministic analyzers

Built-in analyzers should be small, versioned, and independently testable.
Initial analyzers include:

- lifecycle and ownership consistency;
- start and executable-resolution failures;
- exit, signal, and platform termination facts;
- timeout and cancellation attribution;
- retry history, repeated failure fingerprint, and exhaustion;
- dependency, wait-condition, and admission blockers;
- logging or state failures that may obscure the target result;
- notification-only failures; and
- cross-run comparison for changing versus repeated failure modes;
- bounded traceback and structured error-chain extraction with exact artifact
  range citations; and
- opt-in comparison with core-supplied similar failure summaries.

Rules cite item IDs and emit stable diagnosis and action codes. They do not
scan unbounded logs. Text-signature rules are anchored, length-bounded, and
specific to a known tool or format; a match remains a hypothesis unless the
format is authoritative.

For a fixed bundle, analyzer set, versions, and options, results are semantically
deterministic. The engine sorts candidates by explicit precedence, score, code,
and citation IDs. An injected clock is used for output timestamps. A stable
report fingerprint excludes its generation time.

### 6.2 Generative stage

The generator receives a strict `StructuredRequest` containing:

- schema and protocol version;
- the approved evidence projection;
- deterministic findings and their citations;
- allowed diagnosis categories and action identifiers;
- explicit instructions to treat all artifact content as untrusted data;
- response size and count limits; and
- a response JSON schema.

Provider adapters require native JSON-Schema-constrained generation. Hosted
structured-output APIs and self-hosted
runtimes such as Ollama, vLLM, llama.cpp, and SGLang can enforce a schema around
an otherwise ordinary model. The companion does not implement token-level
constrained decoding itself.

A provider that cannot enforce the schema is not eligible. Regardless of
backend enforcement, the companion performs bounded JSON decoding and full Go
validation again, including semantic invariants, allowed taxonomy values,
unique identifiers, and valid evidence citations. Native enforcement improves
reliability; it is never a substitute for host validation.

It returns proposals rather than a report. A proposal may add a hypothesis,
plain-language explanation, alternative, missing-evidence request, or select
an existing deterministic action ID. It must cite projected evidence item IDs.

The engine rejects or downgrades generated content that:

- cites nonexistent or undisclosed evidence;
- contradicts an observed fact without identifying the contradiction;
- claims that an omitted field was observed;
- invents a Jobman command, path, URL, identifier, or option;
- requests automatic execution;
- exceeds output limits or the selected vocabulary; or
- fails strict decoding or validation.

The engine does not request or persist hidden chain-of-thought. It requests
short rationales tied to citations. One bounded generator call is the default;
multi-call debate or agent loops are outside the first release.

A generator timeout, cancellation, invalid response, or unavailable provider
becomes a report warning when the selected mode can still produce a
deterministic report. `RequireGenerator` converts it to a command failure for
users who explicitly need model output.

### 6.3 Reconciliation and ranking

The engine keeps these result classes distinct:

1. observed findings;
2. deterministic hypotheses;
3. generated hypotheses; and
4. alternatives contradicted by one or more facts.

Generated output cannot erase or lower the visibility of an observed finding.
Equivalent candidates are merged by stable diagnosis code and citation set,
while their provenance remains visible. The primary diagnosis is selected by
this order:

1. exact deterministic attribution;
2. calibrated deterministic heuristic;
3. calibrated generated proposal;
4. uncalibrated deterministic heuristic; and
5. uncalibrated generated proposal.

Score, stable code, and citation IDs break ties. A generated proposal with a
higher self-reported score cannot outrank contradictory exact evidence.

## 7. Diagnosis report contract

The report contains:

- report, core evidence, and enriched analysis-evidence identifiers;
- subject and selected-run summary;
- engine, analyzer, and optional generator provenance;
- one primary diagnosis and ranked alternatives;
- confidence metadata;
- supporting and contradicting evidence citations;
- safe, ordered actions;
- deterministic retry advice and current policy state;
- missing evidence, omissions, limitations, and warnings;
- a disclosure manifest for every generator invocation; and
- core and report fingerprints suitable for grouping repeated runs.

Conceptual JSON:

```json
{
  "kind": "jobman.diagnosis_report",
  "schema_version": 1,
  "report_id": "sha256:...",
  "core_evidence_id": "sha256:...",
  "analysis_evidence_id": "sha256:...",
  "generated_at": "2026-08-08T14:30:01Z",
  "provenance": {
    "jobman_version": "1.2.0",
    "companion_version": "0.1.0",
    "evidence_schema_version": 1,
    "analysis_schema_version": 1
  },
  "subject": {
    "job_id": "01980f4c-7b2a-7a6f-8c10-0123456789ab",
    "runs": [3]
  },
  "primary": {
    "code": "target_configuration_invalid",
    "kind": "deterministic_hypothesis",
    "summary": "The target rejected its configuration.",
    "confidence": {
      "score": 0.91,
      "level": "high",
      "meaning": "ranking_score",
      "method": "rule:known_exit_and_stderr_signature@1",
      "calibration": "uncalibrated"
    },
    "supporting_evidence": ["ev:run:3:exit", "artifact:run:3:stderr"],
    "contradicting_evidence": []
  },
  "alternatives": [],
  "fingerprints": {
    "core": "hmac-sha256-v1:...",
    "report": "sha256-v1:..."
  },
  "actions": [
    {
      "id": "inspect_target_configuration",
      "priority": 1,
      "risk": "read_only",
      "summary": "Validate the target configuration before rerunning.",
      "execution": "none"
    }
  ],
  "retry": {
    "verdict": "after_change",
    "existing_policy": "exhausted",
    "next_eligible_at": null,
    "confidence": {
      "score": 1.0,
      "level": "high",
      "meaning": "policy_fact",
      "method": "jobman_retry_policy@1",
      "calibration": "exact"
    },
    "reasons": ["retry_budget_exhausted", "same_input_likely_to_repeat"]
  },
  "missing_evidence": [],
  "warnings": [],
  "disclosure": {
    "generator_used": false,
    "classes_shared": []
  }
}
```

Core `jobman show evidence --json` retains Jobman's version-1 CLI envelope and
places the independently versioned evidence object under `data.evidence`.
`jobman-diagnose --json`, whether invoked directly or as `jobman diagnose`,
emits the companion-owned diagnosis object above as its top-level value. Its
`kind` and `schema_version` prevent it from being mistaken for core inspection
JSON. A report records the companion version, core Jobman version, evidence
schema, analyzer versions, and provider/model descriptor.

### 7.1 Confidence

Every diagnosis has a score in `[0,1]`, a display level, a method, and a
calibration label. The score is an ordering measure unless `meaning` explicitly
says it is an estimated correctness probability backed by a named evaluation.

Recommended display levels are:

- `high`: score at least 0.85;
- `medium`: score at least 0.60 and below 0.85; and
- `low`: score below 0.60.

These thresholds are presentation policy, not statistical calibration.
Exact facts and direct policy evaluations identify themselves as such. A model
self-assessment is labeled `provider_self_assessment` and `uncalibrated` unless
the selected provider supplies a compatible, versioned calibration record.

Calibration data should be built from checked-in, nonsecret diagnosis cases
with known causes. Scores and thresholds change only with an analyzer or engine
version change. The report always exposes the method so a consumer need not
compare unlike scores.

### 7.2 Evidence citations

Each diagnosis lists supporting and contradicting evidence separately. Human
output renders a safe summary of each citation. JSON uses stable item IDs and
may include a compact cited-item table so consumers do not need to join against
an omitted full bundle.

A generated sentence without a valid citation is commentary, not a diagnosis,
and should normally be discarded. General knowledge may be included only as an
explicitly labeled `external_knowledge` rationale; it cannot be presented as
an observed property of the job.

### 7.3 Actions

Actions have stable identifiers, priority, applicability, risk, rationale,
and execution class. The classes are:

- `none`: prose guidance only;
- `read_only_argv`: a direct argument vector for an allowlisted Jobman
  inspection command; and
- `mutating_argv`: reserved for a future explicit workflow and never produced
  by the first release.

Only a deterministic action resolver may create `read_only_argv`. A generator
selects an action identifier or supplies prose; it cannot provide executable
text. The resolver rechecks that the action applies to the cited evidence.
Displayed argument vectors preserve boundaries and are never joined into a
shell string.

Actions should say what new evidence or state would make them complete. Useful
initial actions include inspecting a selected log range, showing a dependency,
waiting until an existing backoff expires, checking a path or permission,
validating target configuration, increasing an explicitly exhausted resource,
and using `doctor` when the failure originates in Jobman's store.

### 7.4 Retry advice

Retry advice is computed deterministically after diagnosis and has two parts:

- `existing_policy`: whether Jobman already scheduled another run, is waiting,
  has exhausted its budget, classified the outcome as nonretryable, or has no
  applicable retry policy; and
- `verdict`: `now`, `after_delay`, `after_change`, `do_not_retry`, or `unknown`.

“Retry” is intentionally precise. Another run under the same job can occur
only through its recorded policy. After a terminal job, the user creates a new
job with `rerun`; the report should not imply that historical state can be
reopened.

The verdict accounts for active ownership, cancellation, timeout, dependency
state, admission, remaining policy budget, repeated fingerprints, and whether
the suspected cause is transient or persistent. A generator may offer a retry
opinion, but it appears only as a rationale or alternative and cannot replace
the deterministic verdict.

## 8. Model and analyzer extension points

Model and analyzer extensibility belongs entirely to `jobman-diagnose`. It is
unrelated to Jobman's external CLI-subcommand discovery after the companion has
started.

### 8.1 Go interfaces

Embedding programs pass `Analyzer` and `StructuredGenerator` implementations
to the companion engine. The initial generator capability descriptor includes
native JSON Schema support, locality (`local` or `remote`), and input/output
limits; the selected profile supplies the adapter and model identity.

The domain package does not read API keys or know provider request formats. A
provider adapter owns authentication and transport while still receiving only
the approved projection and strict structured request.

### 8.2 Provider adapters

Initial adapters may target:

- a hosted API with native structured output;
- Ollama;
- vLLM or another OpenAI-compatible self-hosted endpoint;
- llama.cpp or SGLang servers; and
- an explicitly configured local command bridge for providers that do not
  expose a suitable network API.

This list is an interoperability target, not a set of core dependencies or a
promise that every adapter ships in the first companion release. Provider
packages can have their own build tags or modules so installing the default
companion does not require every vendor SDK.

HTTP adapters use bounded request and response bodies, deadlines, explicit
redirect policy, and TLS verification for non-loopback endpoints. A profile
must explicitly identify a remote endpoint. The companion does not discover
servers on the network or silently fall back from a local model to a hosted
service.

The optional command bridge uses a configured absolute executable and argument
vector, launches directly without a shell, supplies a minimal environment, and
bounds standard output and error independently. It writes one versioned
structured request to standard input and reads one versioned response. A
nonzero exit, timeout, malformed response, or excess output is a provider
failure. The executable is trusted code with the user's authority and is never
discovered or selected automatically.

### 8.3 Companion configuration

The deterministic engine requires no configuration. Model configuration lives
in a strict schema-2 `diagnosis.yml`, not Jobman's configuration. AI mode uses
an explicit override or the platform per-user Jobman configuration directory;
there is no system, project, or working-directory search. A malformed companion file cannot block
`doctor`, `show`, evidence collection, or deterministic diagnosis because
deterministic mode does not open it.

The file has an independent schema version, a default profile, and named profiles. A profile
selects provider type, locality, an exact endpoint or absolute command argument
vector, model identifier, mandatory schema enforcement, time and size limits,
and allowed disclosure classes. It contains secret references only, never
literal credentials. Project-local model configuration is not implicitly
trusted.

Prompt templates, taxonomy versions, and provider-specific request settings
ship with the companion and are included in report provenance. None is stored
in Jobman's state or configuration.

The first model invocation is explicit through `--ai`, `-a`, `--ai-logs`, or
`--profile NAME`. AI activation approves bounded metadata, direct command
specifications (including ordered arguments), paths, and environment variable
names; log content requires
per-invocation `--ai-logs` or `--share log_content`. Merely having a profile
does not enable network disclosure. `--deterministic` ignores the model file
and remains available when it is missing or invalid.

## 9. Privacy and security

Evidence minimization happens before inference. Each evidence item and artifact
has one disclosure class:

- `metadata`: lifecycle enum, count, timestamp, stable code, or byte size;
- `command`: executable or argument content;
- `path`: working directory or filesystem path;
- `environment_name`: a variable name without its value;
- `log_content`: target-produced bytes;
- `sensitive`: data that cannot be safely disclosed; or
- `local_only`: useful to local rules but prohibited from provider projection.

No environment value, secret reference value, input bytes, launch credential,
notification credential, webhook body, SMTP body, provider credential, or raw
configuration is eligible for a bundle.

Default deterministic collection uses metadata and safe local-only facts.
Command, path, environment-name, and log content are opt-in at the core evidence
boundary. AI activation is an explicit approval for the first three; log
content remains separately opt-in. Approval is
recorded in the report's disclosure manifest with item counts and byte counts.

Configured field redaction occurs in core before evidence serialization. The
companion verifies the sealed content, converts invalid log bytes to explicit
UTF-8 replacement characters, and applies a final class/ID/byte projection
check before provider invocation. Redaction notices state which evidence IDs
changed without including original values. Because arbitrary target output may
contain unknown secrets, both projects warn that redaction cannot make log
disclosure risk-free.

Deterministic metadata diagnosis remains available without a model profile.
Schema 1 collects command, path, and environment-name context only when the
corresponding request controls are enabled. Environment values and secret
reference identifiers are never eligible. A projection of
`log_content` fails closed unless core sealed the
`configured_value_redaction_v1` capability, the profile permits bounded log
content, and the CLI independently supplies `--share log_content`.

Target output, platform events, historical summaries, and enrichment results
are untrusted and may contain prompt-injection text. They are placed in typed
artifact fields, never concatenated as model instructions. The model request
says that artifacts are data, the provider has no Jobman tools, and the
companion validates all citations and actions. These controls reduce risk but
do not make disclosure to an untrusted provider safe.

There is no telemetry, feedback upload, prompt logging, or report upload by
default. Explicitly exported evidence and reports use private files, atomic
write-sync-rename where durability matters, and fail if the destination would
overwrite an existing file unless the user separately authorizes replacement.

## 10. Command-line interface

### 10.1 Core evidence command

```text
jobman show evidence [OPTIONS] JOB
```

This extends the established `show` inspection boundary instead of introducing
an overlapping `inspect` command. It emits human-readable collection metadata
by default and the stable evidence bundle with `--json`. Useful options are:

| Option | Meaning |
| --- | --- |
| `--run N` | Select one run; negative indexing follows `show run`. |
| `--all-runs` | Include compact facts for the bounded run history. |
| `--command` | Explicitly include bounded direct executable and ordered argument vectors. |
| `--paths` | Explicitly include bounded working directories, file paths, and resolved executables. |
| `--environment-names` | Explicitly include environment variable names and roles, never values. |
| `--logs metadata\|tail\|none` | Select bounded target-log evidence. |
| `--log-bytes SIZE` | Set a value within the hard log-content ceiling. |
| `--similar N` | Opt in to at most N same-fingerprint failure summaries. |
| `--json` | Emit the versioned evidence bundle in the core CLI envelope. |

The command does not know whether its consumer is `jobman-diagnose`, another
extension, a support tool, or a user. It never invokes a model. Normal core
selector and exit-status rules apply.

### 10.2 Companion command

When `jobman-diagnose` is installed on `PATH`, these are equivalent:

```text
jobman diagnose [OPTIONS] JOB
jobman-diagnose [OPTIONS] JOB
```

Offline evidence remains supported:

```text
jobman diagnose [OPTIONS] --from-evidence PATH
```

Recommended companion options are:

| Option | Meaning |
| --- | --- |
| `--run N` | Diagnose one run; negative indexing follows `show run`. |
| `--all-runs` | Compare the bounded run history for repeated or changing failures. |
| `--similar N` | Opt in to bounded same-fingerprint history from core. |
| `--json` | Emit the versioned companion diagnosis object. |
| `--deterministic` | Use only built-in analyzers; this is the default. |
| `--ai`, `-a` | Use the configured default AI profile and approve bounded metadata, commands, paths, and environment names. |
| `--ai-logs` | Use AI and collect/share a bounded redacted target-log tail. |
| `--profile NAME` | Use a named AI profile instead of the configured default. |
| `--diagnosis-config PATH` | Override the platform per-user diagnosis configuration path. |
| `--require-model` | Fail instead of degrading when the selected generator fails. |
| `--logs metadata\|tail\|none` | Select bounded log evidence. |
| `--log-bytes SIZE` | Set a value within the hard log-content ceiling. |
| `--share CLASS[,CLASS...]` | Approve an additional class; live `log_content` implies tail collection. |
| `--from-evidence PATH` | Diagnose a saved evidence value without opening a live store. |
| `--export-evidence PATH` | Save the sealed core evidence. |
| `--jobman PATH` | Select a core executable during direct companion invocation. |
| `--state-dir PATH` | Select core state during direct companion invocation. |
| `--config PATH` | Select core redaction configuration without granting store authority. |
| `--output PATH` | Atomically write a private report instead of standard output. |

Exactly one of `JOB` and `--from-evidence` is required. The default selected run
is the latest failed or otherwise abnormal run. For an active job it is the
active run. A successful job can still be diagnosed to explain delays, retries,
degraded logging, or notification problems. The human view should lead with the
primary diagnosis, confidence, strongest evidence, next action, and retry
verdict, then show alternatives and limitations.

The first release diagnoses jobs that reached durable state, including a
durable `submission_failed` record. Validation, configuration, or store-open
errors that occur before a job ID exists remain ordinary CLI errors. A future
versioned command-error envelope could be accepted through a separate offline
input without weakening the job evidence contract.

Report creation succeeds with exit status 0 even when the subject job failed.
The status describes the diagnosis operation, not the target. Usage,
not-found, ambiguous-selector, conflict, and internal errors keep the existing
Jobman categories. An unavailable optional model still returns 0 with a
warning; `--require-model` makes it a runtime failure.

Core Jobman preserves the companion's process exit status. The companion should
use the matching Jobman categories for usage, selector, conflict, and runtime
errors when it can do so unambiguously.

Diagnosis authorizes no new Jobman-state mutation. Evidence collection may
perform the same bounded stale-state reconciliation already permitted for
inspection, and output flags may create explicitly selected private files. The
companion does not load ordinary Jobman configuration as store authority,
perform hidden cleanup, invoke `doctor --repair`, or rerun a target.

### 10.3 Useful follow-on workflows

The contract supports later additions without putting them in the first CLI:

- comparison of a job against its previous rerun source;
- a redacted support bundle containing evidence, report, version, and platform
  capability facts;
- `--at-event EVENT` for reproducible historical analysis when all required
  artifacts remain available;
- batch deterministic diagnosis of recent failures; and
- user-supplied local rule packs expressed as bounded data, not executable
  templates.

Automatic remediation, report-driven rerun, model tool use, remote log
retrieval, and background monitoring require separate designs and explicit
authorization. A future `run --wait --diagnose-on-failure` must resolve the
companion explicitly and remain opt-in; normal `run` never acquires an
extension dependency.

## 11. Failure fingerprints

Core Jobman produces a normalized factual fingerprint for a terminal failed
run when sufficient evidence exists. Its versioned input contains only stable
facts available to core, such as run outcome, diagnostic class, exact start
error category, observed signal or platform termination class, timeout scope,
and relevant policy disposition. It excludes raw command text, paths,
environment, log text, model output, prose, and analyzer versions.

When executable identity materially improves grouping, Jobman uses a keyed
digest rather than exposing the executable or a reversible low-entropy hash.
The key is random, private, and local to the state store. Consequently the
fingerprint supports grouping within one store and is deliberately unsuitable
as a cross-user or cross-host identifier.

The fingerprint record contains algorithm and input-schema versions. Jobman
only computes it from facts captured under that versioned contract; it does not
reconstruct or backfill old rows during evidence collection. Persisting and
indexing it for efficient similar-failure queries uses a forward database
migration; terminal lifecycle truth does not depend on fingerprint
availability.

The companion adds a separate report fingerprint based on the core fingerprint,
diagnosis code, deterministic analyzer version, and bounded signature IDs. A
model-generated phrase never affects either fingerprint. This lets the
companion say “three runs failed the same way,” “a matching rerun later
succeeded,” or “the failure mode changed after run 2” without treating
unrelated exit-code matches as proof of a common root cause.

Search across unrelated jobs is explicit and bounded through `--similar`; it is
never added to ordinary evidence by default.

## 12. Performance and availability

Core diagnostic support adds only bounded structured detail and, when enabled
by its migration, one factual fingerprint at a terminal transition. Evidence
collection and all companion analysis occur only when requested. Normal job
execution never loads the companion or contacts a model.

The design uses:

- one bounded metadata read transaction;
- no full-log reads;
- no evidence-collection database writes beyond inspection's existing bounded
  stale-state reconciliation;
- no unbounded regular expressions;
- a fixed maximum number of candidates, alternatives, actions, and missing
  evidence items;
- one optional model call with a default 30-second deadline; and
- a default provider response ceiling of 256 KiB.

The report records when budget limits may have reduced confidence. Context
cancellation stops collection, analyzer work, and provider execution. An
optional provider failure does not prevent local diagnosis.

## 13. Testing and evaluation

### 13.1 Contract tests

- Golden core evidence JSON in Jobman and golden report, request, and response
  JSON in the companion for every supported schema version.
- Strict bounds, duplicate detection, citation validation, unsupported-version
  behavior, unknown additive fields, and round trips.
- Fuzz decoding of evidence and provider responses.
- Canonical digest stability across map insertion order and repeated encoding.
- Compatibility fixtures proving the oldest supported companion can consume
  each compatible core evidence version.

### 13.2 Collector tests

- One SQLite snapshot across jobs, runs, runtime, dependencies, waits,
  admission, notifications, and events.
- Active-log growth, rotation, pruning, binary content, short reads, and
  truncation metadata.
- Every omission and consistency state.
- Secret canaries in names, arguments, paths, environment, logs, errors, and
  notifier metadata.
- Confirmed versus ambiguous signal and out-of-memory cases on each platform.
- Resource units, scope, unavailable metrics, fingerprint stability, and
  bounded same-fingerprint queries.
- No state mutation except the existing bounded stale-state reconciliation
  contract used by inspection.

### 13.3 External-command tests

- Built-in precedence, name validation, `PATH` order, duplicate executables,
  missing extensions, and `--no-extensions`.
- Exact argument and stream forwarding without a shell.
- Protocol environment fields without configuration or credential contents.
- Interrupt, termination, child exit-status preservation, and recursion-loop
  prevention on Linux, macOS, and Windows.
- `jobman diagnose` and direct `jobman-diagnose` parity.

### 13.4 Analyzer tests

- Table-driven cases for each exact fact and heuristic.
- Positive, negative, ambiguous, and contradictory evidence.
- Stable ranking after randomized evidence-item order.
- Retry advice across active policy, backoff, exhaustion, persistent start
  failure, timeout, cancellation, and unknown failure.
- Cross-run fingerprint and change-point behavior.
- Traceback extraction with exact artifact byte-range citations.
- Opt-in similar-history results that never expose unrelated command or log
  content.

### 13.5 Generator and security tests

- Fake generators for timeout, cancellation, nonzero process exit, oversized
  streams, malformed JSON, unsupported protocol, and invalid citations.
- Prompt-injection fixtures in stdout, stderr, arguments, names, and paths.
- Attempts to invent executable actions, override deterministic facts, or cite
  undisclosed evidence.
- Disclosure manifests and byte counts for every profile.
- Deterministic fallback when the provider is absent or fails.
- Native schema-enforcement capability negotiation and strict revalidation of
  responses that were already constrained by a backend.
- Hosted, loopback self-hosted, and command-provider transport boundaries.

### 13.6 Evaluation set

Maintain a checked-in, synthetic, nonsecret case corpus. Each case contains a
versioned evidence bundle, accepted primary and alternative codes, required and
forbidden actions, expected retry class, and confidence bounds. Deterministic
cases run in ordinary tests. Recorded model-response evaluation can run without
network access; live provider evaluation is opt-in and never a required unit
test.

Report precision, unsupported-claim rate, citation validity, safe-action rate,
and retry-advice accuracy separately. A model that produces more fluent text
but worse citations or unsafe actions is a regression.

Production core and companion code remains Go. Python tooling may be used in a
separate evaluation environment for dataset analysis, provider benchmarking,
calibration, and experiment reporting. Generated evaluation artifacts are not
runtime inputs unless reviewed and converted into versioned Go-owned rules,
taxonomies, prompts, or fixtures.

End-to-end tests cover missing and installed companions, `show evidence`,
`diagnose`, evidence export/import, JSON output, private file permissions, exit
categories, cancellation, and all three supported platforms. Documentation
tests ensure every suggested Jobman argument vector is valid for the relevant
core or companion command tree.

## 14. Delivery plan

Each phase is independently useful and keeps the v1 behavior intact.

### Phase 1: core factual boundary and extension dispatch

1. Define the diagnostic code type and safe structured record.
2. Replace new arbitrary diagnostic strings at failure sites with catalog
   values and exact low-level classifications while decoding legacy details.
3. Add the public `diagnostic` evidence contract, validation, and canonical
   digest without report or model types.
4. Add the transactionally consistent application collector and bounded event
   query.
5. Implement `jobman show evidence --json` with bounded optional log tails.
6. Record external command resolution in an ADR, then add direct dispatch,
   protocol context, disable controls, and cross-platform execution tests.

No database migration, companion installation, prompt, model configuration, or
AI dependency is required.

### Phase 2: offline companion

1. Create the independent `jobman-diagnose` Go module and executable.
2. Add `Diagnostician`, analyzer, report, action, confidence, and deterministic
   retry-advice contracts.
3. Implement deterministic analyzers, cross-run comparison, bounded traceback
   extraction, and human and JSON rendering.
4. Support extension and direct invocation plus evidence import/export.
5. Publish a core/companion evidence compatibility matrix.

This phase should ship before generative support so users receive reliable
offline value and the project boundary receives real use.

### Phase 3: richer core evidence

1. Add portable resource observation types and the measurements each platform
   can establish reliably.
2. Add a forward migration for versioned core fingerprints and efficient
   bounded matching.
3. Implement opt-in same-fingerprint summaries without unrelated content.
4. Add confirmed resource termination and fingerprint portability tests.

Unavailable platform facts remain explicit and do not delay the offline
companion.

### Phase 4: structured generated augmentation

Implementation status: complete in the companion repository; live hosted and
self-hosted evaluation remains a release gate.

1. Add `StructuredGenerator`, strict proposal schemas, and native JSON Schema
   capability negotiation in the companion.
2. Add companion-only profiles, disclosure projections, manifests, per-user
   configuration discovery, and explicit `--ai`/`--profile` selection.
3. Add at least one hosted structured-output adapter, one self-hosted adapter,
   and the bounded command bridge without adding dependencies to core Jobman.
4. Add proposal reconciliation, contradiction handling, model provenance,
   deterministic fallback, and `--require-model`.
5. Complete prompt-injection, privacy, transport, and unsafe-action review.

### Phase 5: evaluation and optional enrichment

Establish the offline evaluation corpus, calibration labels, provider
benchmarks, and optional Python analysis tooling. Then add opt-in system-event
collectors, companion-local similarity indexes, support bundles, and explicit
diagnose-on-failure workflows only when their privacy and authorization models
are tested.

## 15. Acceptance criteria

The initial complete feature is ready when:

- a fixed evidence bundle produces the same deterministic report across
  repeated runs and supported platforms, excluding documented platform facts;
- every diagnosis and action cites valid evidence or is explicitly labeled as
  external knowledge;
- retry advice agrees with the recorded Jobman policy and never mutates it;
- malformed or hostile model output cannot create executable actions or alter
  observed facts;
- a constrained backend response is still fully validated by the companion;
- deterministic diagnosis works with no configuration or network;
- core evidence collection works when the companion is absent or incompatible;
- missing, disabled, or incompatible extensions fail without changing a job;
- optional provider failure produces a useful local report unless explicitly
  required;
- all collection and provider inputs and outputs are bounded and cancellable;
- redaction and disclosure tests contain no secret-canary leaks;
- active-state and log consistency limitations are visible;
- similar-job evidence is opt-in, bounded, and contains no unrelated command or
  log content;
- exported files are private and safely created;
- no core package, configuration, or normal job execution path acquires a
  prompt, provider, embedding, network, or model dependency; and
- `show`, `doctor`, notifications, and the frozen v1 JSON contracts retain
  their existing behavior.

## 16. Principal decisions

1. Diagnosis is an independently installed read-only companion, not an
   expansion of `doctor` or an AI dependency in core Jobman.
2. Core Jobman owns facts, exact low-level classifications, fingerprints, and
   the public `diagnostic` evidence package; the companion owns inference.
3. A versioned, bounded JSON evidence bundle is the only data bridge between
   core and companion.
4. Generic `jobman-NAME` dispatch supplies natural invocation for optional
   extensions while preserving built-in precedence and direct execution.
5. Deterministic companion analysis is the default and fallback.
6. Generative models propose; the companion validates, reconciles, and decides.
7. Native JSON-Schema-constrained generation is preferred, and Go validation is
   always required afterward.
8. Retry advice remains deterministic and separate from root-cause inference.
9. Remote disclosure and unrelated-job history are independently opt-in and
   recorded in the report.
10. Diagnoses are not persisted as lifecycle truth, and no model can act on a
    job.
11. Production functionality remains Go; Python is limited to optional offline
    evaluation and experimentation.
12. The first core evidence phase requires no database migration or vendor SDK;
    richer resource and indexed fingerprint facts use an explicit later
    migration.
