# Jobman design

The [formal design specification](SPEC.md) records Jobman's v1 behavior,
architecture, requirements, implementation milestones, and product decisions.
It is the authoritative design contract. This page is a short overview; if the
two documents conflict, the formal specification controls.

Implementation is guided by the
[initial vertical-slice plan](IMPLEMENTATION_PLAN.md) and the indexed
[architecture decision records](adr/README.md).

A post-v1 extension for deterministic diagnostic evidence and an optional
independently installed diagnosis companion is described in the
[diagnostic evidence and diagnosis design](DIAGNOSIS.md). Jobman v1.4.0
released core evidence, external-command dispatch, resource observations,
failure fingerprints, and exact similarity history. Jobman Diagnose v0.1.0
released deterministic analysis and optional generated-model augmentation.
These additive facilities do not change the frozen v1 Jobman contract.
The corresponding
[diagnosis implementation plan](DIAGNOSIS_IMPLEMENTATION_PLAN.md) divides that
proposal into independently releasable core, companion, provider, evaluation,
and enrichment work packages.

The implementation's storage formats and measured portability are recorded
separately in the [persisted-schema reference](PERSISTED_SCHEMA.md) and
[platform capability record](PLATFORM_CAPABILITIES.md). The initial vertical
slice and the subsequent v1 policy expansion are tracked in the
[implementation plan](IMPLEMENTATION_PLAN.md). Those records distinguish the
implemented contract from evidence that must be repeated for each release.

## Product model

Jobman manages a command as a **job**. A job has an immutable identity and
specification plus one or more **runs**. Each run records its lifecycle, exit
status, timestamps, and output. A job may create another run when its retry
policy permits.

The CLI is daemonless: submitting, inspecting, following logs, stopping, and
cleaning jobs must not depend on a continuously running privileged service.
Background execution may use a detached worker process, but all durable state
must remain inspectable after the submitting terminal exits.

Per-job supervisors coordinate through the local transactional store. There is
no shared scheduler, recovery daemon, or remote-control listener; remote users
invoke Jobman through an existing channel such as SSH.

## Target commands

| Command | Purpose |
| --- | --- |
| `jobman run [OPTIONS] -- COMMAND [ARG...]` | Submit and execute a managed command. |
| `jobman list` | List jobs and their current state. |
| `jobman status JOB` | Show a concise current status. |
| `jobman show JOB` | Show a job and its run history. |
| `jobman logs JOB` | Read or follow recorded output. |
| `jobman cancel JOB` | Durably request cancellation of a job. |
| `jobman pause JOB` | Pause policy progress and best-effort suspend an active process tree. |
| `jobman resume JOB` | Resume a paused job. |
| `jobman input JOB` | Stream local standard input to an active run. |
| `jobman wait JOB` | Wait for a terminal job outcome. |
| `jobman rerun JOB` | Submit a new job from an existing effective specification. |
| `jobman clean` | Safely prune eligible completed logs and metadata. |
| `jobman doctor` | Verify state, create a backup, and perform explicit conservative recovery. |
| `jobman config` | Inspect configuration or explicitly apply durable settings. |

Stable commands must support machine-readable output and meaningful exit codes.
Identifiers accepted by destructive commands must be unambiguous; names that
match multiple jobs require an explicit selection policy or an error.

## Execution policy

A job specification may combine:

- wait conditions based on time, files, or executable probes;
- dependencies on another job's success, failure, selected outcome, or any
  terminal result;
- store-wide and named-pool integer concurrency limits;
- an abort deadline for waiting or retrying;
- accepted success and retryable-failure exit codes;
- maximum run, success, or failure counts;
- constant, linear, or exponential retry delay with bounded jitter;
- run-level and job-level timeouts;
- success, retry, and failure notification callbacks;
- completed-log cleanup limits by age, size, job count, or run count.

Concurrency admission is local and transactional. Every active run consumes a
configurable number of slots from the store-wide limit and, when selected, one
named pool. Groups remain descriptive labels rather than hidden queues. These
limits do not perform CPU/GPU discovery, resource placement, preemption, or
fair-share scheduling. Limits default to unlimited until configured.

Policy validation happens before background execution. Invalid combinations
must fail without creating partial state. Durations and timestamps use Go's
documented duration syntax and RFC 3339 unless a command explicitly documents
another representation.

## State and concurrency

The default store is local and per-user. Transactional metadata is stored in
SQLite, while raw stdout/stderr and their ordering index use private filesystem
files. Updates must be atomic, tolerate an interrupted writer, and coordinate
concurrent Jobman processes. State schema versions are recorded so migrations
can be explicit and testable. A simplified layout is:

```text
state/
  jobman.db
  jobman.db-wal
  jobman.db-shm
  logs/
    <job-id>/
      <run-number>/
        stdout.log
        stderr.log
        chunks.idx
```

Files containing commands, environment values, logs, or callback data may be
sensitive. New files must use user-only permissions by default, and diagnostic
output must not expose secret values.

## Process and signal behavior

- Managed commands run in their own process group where supported.
- Terminal hangup does not terminate a deliberately detached job.
- Stop requests target the process group and escalate only according to an
  explicit policy.
- Pause/resume operates on a verified process tree where the platform exposes a
  safe suspension mechanism; unsupported platforms report that limitation.
- Opted-in detached jobs accept bounded binary input through a private local
  supervisor channel: Unix-domain sockets on Unix-like systems and protected
  named pipes on Windows.
- Jobman forwards container and operating-system termination signals.
- State transitions remain valid if either Jobman or the managed command exits
  unexpectedly.
- Shell execution is opt-in and visible; argument-preserving execution is the
  safe default for untrusted input.

Platform-specific behavior must be isolated and covered by platform builds or
tests. Unsupported behavior should fail clearly rather than silently degrading.

## Logs and notifications

Standard output and standard error retain their stream identity and ordering as
far as the operating system permits. Following logs must terminate cleanly when
the run ends. Rotation and cleanup never delete state for an active run.

Notification callbacks receive a documented, versioned payload and execute with
bounded time and output. Callback failures are recorded but do not rewrite the
underlying job result.

## Configuration

The implemented schema, source discovery, environment bindings, defaults, and
security notes are documented in the
[configuration reference](../CONFIGURATION.md).

Precedence is explicit and testable:

1. command-line flags;
2. environment variables;
3. an explicitly selected configuration file;
4. an explicitly trusted project configuration;
5. the per-user configuration file;
6. the system configuration file; and
7. built-in defaults.

Unknown keys and invalid values produce actionable errors. Configuration
paths follow platform conventions, with `XDG_CONFIG_HOME` honored on Unix-like
systems. A schema change that cannot be interpreted compatibly requires release
notes and a migration path.

## Release acceptance criteria

Every supported release retains these gates:

- lifecycle transitions, retries, timeouts, and interruption recovery have
  deterministic unit and end-to-end coverage;
- concurrent commands cannot corrupt or observe partial state;
- permissions and secret-redaction behavior have security tests;
- Linux, macOS, and Windows builds pass, with documented feature differences;
- command help, man pages, completions, examples, and sample configuration
  agree;
- release archives, native packages, checksums, signatures, SBOMs, and container
  images can be installed and verified from a clean environment;
- upgrades from each supported state and configuration schema are tested.
