# Jobman design specification

Status: frozen v1 design contract
Target: v1 release line
Last updated: 2026-07-22

This document defines the supported v1 behavior and architecture of Jobman.
The implementation, tests, and user documentation have been reconciled to this
contract for the v1 release. Requirements use **MUST**, **SHOULD**, and **MAY**
in their usual normative senses.

## 1. Product definition

Jobman is a per-user, daemonless command-line job manager. It starts ordinary
commands outside the lifetime of the submitting terminal, records their state
and output, and applies optional dependency, local concurrency, waiting,
repetition, retry, timeout, retention, and notification policies.

Jobman is intended primarily for interactive terminal users, including
researchers and engineers who run long experiments or commands susceptible to
transient failure. Its human-oriented interface MUST remain convenient for
interactive use, while every inspection command MUST also provide stable,
machine-readable output for scripts.

### 1.1 Goals

Jobman MUST:

- require no privileged installation or continuously running global service;
- let a submitted job continue after the submitting Jobman process and terminal
  exit;
- retain inspectable job state, run history, standard output, and standard
  error;
- support dependencies, bounded local concurrency, delayed execution, retries
  or repeated runs, timeouts, cancellation, and notifications as composable
  policies;
- coordinate simultaneous Jobman invocations without corrupting state;
- detect and report uncertain process state rather than claiming a false
  result;
- ship as a single native executable on supported platforms; and
- give Linux, macOS, and Windows first-class consideration in API and test
  design.

### 1.2 Non-goals

The v1 release line does not:

- provide cron-like schedules or automatically resume jobs after reboot;
- distribute jobs across hosts;
- provide cluster scheduling, resource-aware placement, fair-share allocation,
  or resource accounting beyond local integer concurrency slots;
- replace an init system, container orchestrator, or workflow engine;
- provide a shared coordinator daemon, remote-control listener, or automatic
  supervisor restart or target adoption service;
- execute commands with elevated privileges on the user's behalf; or
- guarantee continued execution through host failure, reboot, or termination
  of the entire operating-system user session.

HTTP or S3 wait conditions, Slack/SMS integrations, and a system-wide
multi-user mode are possible future extensions, not v1 requirements. Remote
state stores and remote control are not planned; users may invoke Jobman over
their existing SSH or host-management channel.

### 1.3 Relationship to the prototype

Jobman releases v0.1.0 through v0.5.0 were an exploratory prototype and impose
no compatibility or architectural constraint on v1. Their state and
configuration do not have a supported migration path. The durable
implementation introduced in v0.6.0 is governed by the explicit versioned
schema and upgrade contracts referenced by this specification; behavior is
stable only when the v1 compatibility contract declares it so.

## 2. Terminology

- **Job**: A durable submission with an immutable ID, an execution
  specification, policies, and zero or more runs.
- **Run**: One invocation of the job's target command. A retry or requested
  repetition creates another run under the same job.
- **Supervisor**: The detached, per-job Jobman process that evaluates policies,
  launches runs, records state, and sends notifications. It is not a shared
  daemon.
- **Client**: A short-lived Jobman invocation used to submit or inspect jobs.
- **Selector**: A job ID, unambiguous ID prefix, or job name used to identify a
  job.
- **Wait condition**: A predicate that must be satisfied before the first run.
- **Dependency**: A predicate over another immutable job ID that gates a job's
  first run.
- **Concurrency pool**: A named local admission domain with a configured number
  of integer slots.
- **Admission**: A durable reservation of global and optional pool slots that
  permits one run to start.
- **Outcome**: A terminal result such as success, failure, timeout,
  cancellation, abort, or lost.
- **Store**: The transactional metadata database and associated log directory.

Names are labels and MAY be duplicated. IDs are opaque, globally unique, and
immutable. User-visible run numbers start at 1 and increase monotonically
within a job.

## 3. Representative workflows

### 3.1 Submit and inspect a job

```console
$ jobman run --name backup -- ./backup.sh /srv/data
01980f4c-7b2a-7a6f-8c10-0123456789ab
$ jobman status 01980f4c
01980f4c-7b2a-7a6f-8c10-0123456789ab  backup  running
$ jobman logs --follow 01980f4c
...
```

`run` returns after durable state has been created and the supervisor has
acknowledged responsibility for the job. It does not wait for the target
command to finish unless `--foreground` or `--wait` is requested.

### 3.2 Retry a transient failure

```console
$ jobman run --retries 3 --retry-delay 10s \
    --retry-backoff exponential -- ./download-data
```

This permits at most four runs: the initial run and three retries. Jobman stops
after the first success, a non-retryable failure, cancellation, a job timeout,
or exhaustion of the retry policy.

### 3.3 Wait for prerequisites

```console
$ jobman run --wait-file ./input.ready --wait-until 2026-07-15T08:00:00Z \
    --wait-mode all -- ./analyze ./input.dat
```

The supervisor evaluates the conditions without requiring a later interactive
Jobman invocation.

### 3.4 Scripted inspection

```console
$ jobman show --json 01980f4c
{"schema_version":1,"data":{"job":{...}}}
```

JSON goes to standard output. Diagnostics go to standard error. Color,
progress indicators, and terminal-width formatting MUST be disabled in JSON
mode.

### 3.5 Dependencies and local concurrency

```console
$ jobman run --name prepare -- ./prepare-data
01980f4c-7b2a-7a6f-8c10-0123456789ab
$ jobman run --name analyze --after-success 01980f4c \
    --pool experiments --slots 2 -- ./analyze
01980f4d-1234-7bcd-9e20-abcdef012345
```

The second job becomes eligible only after the first succeeds. Its supervisor
then waits until it can atomically reserve two slots from both the store-wide
limit and the `experiments` pool. Neither dependency waiting nor admission
requires a shared scheduler process.

## 4. Command-line interface

### 4.1 General conventions

- Long flags use kebab case, for example `--retry-max-delay`.
- `--` ends Jobman flag parsing and begins the target command.
- A direct executable plus an argument vector is the safe default. Jobman MUST
  NOT concatenate arguments and pass them through a shell implicitly.
- Shell evaluation is explicit through the user's shell command, such as
  `jobman run -- sh -c 'echo "$HOME"'`, or through a future `--shell` option.
- The root command without a subcommand displays help. Running a target always
  requires the explicit `run` subcommand.
- Commands that mutate or signal jobs MUST reject ambiguous selectors.
- Times in machine output use RFC 3339 with nanosecond precision and UTC.
- Durations accept Go duration units plus `d` for exactly 24 hours and `w` for
  exactly 7 days. Calendar months are not accepted because their length is
  ambiguous.
- Byte sizes accept IEC suffixes such as `KiB`, `MiB`, and `GiB`.

### 4.2 Common flags

The root command supports:

| Flag | Meaning |
| --- | --- |
| `--config PATH` | Use an explicit configuration file. |
| `--state-dir PATH` | Override the state directory for this invocation. |

Inspection commands expose `--json` where they have a versioned machine
representation: `list`, `status`, `show`, and `doctor`. `config show` emits a
versioned JSON representation without an additional format flag. Jobman v1
does not emit ANSI styling or progress decoration and therefore has no
`--no-color`, `--quiet`, or `--verbose` global flags. Application diagnostics
use standard error and MUST NOT be mixed into command data on standard output.

### 4.3 Job selectors

A selector is resolved in this order:

1. exact job ID;
2. unique job ID prefix of at least eight characters;
3. exact job name.

No match is an error. Multiple matches are an ambiguity error. Commands MAY
provide an explicit `--latest` option for choosing the most recently submitted
matching name, but MUST NOT silently choose it by default. Destructive commands
SHOULD display the selected ID in human-readable output.

### 4.4 `jobman run`

```text
jobman run [OPTIONS] -- COMMAND [ARG...]
```

`run` validates the effective specification, creates the job transactionally,
starts a supervisor, waits for its acknowledgement, prints the job ID, and
exits. A failure before supervisor acknowledgement MUST either leave no job or
leave a clearly terminal submission-failed record; it MUST NOT leave a job
appearing active without an owner.

Initial option groups are:

- identity: `--name`, repeatable `--group`, and optional tags;
- execution: working directory, explicit environment additions, stdin policy,
  and stop policy;
- prerequisites: job dependencies plus time, file, and executable-probe
  conditions;
- admission: an optional named concurrency pool and positive slot request;
- completion: run limits, success targets, failure limits, and retryability;
- delay: constant, linear, or exponential backoff with bounded jitter;
- timeout: per-run and whole-job deadlines;
- logging: stream capture, rotation, and retention overrides; and
- notification: named notifiers and transition subscriptions.

The command is required unless a named job specification or rerun source
provides it. Mutually exclusive sources are:

```text
COMMAND [ARG...]
--job-spec NAME
--rerun JOB
```

Rerunning copies the prior effective specification into a new job with a new
ID. It does not mutate or append runs to the historical job.
`jobman rerun JOB` is the convenience spelling of `jobman run --rerun JOB` and
has the same validation and configuration-authority behavior.

The v1 convenience options include:

| Flag | Semantics |
| --- | --- |
| `--retries N` | Permit N retries after the initial run. |
| `--retry-delay DURATION` | Base delay before another run. |
| `--retry-backoff constant\|linear\|exponential` | Delay algorithm. |
| `--retry-jitter DURATION` | Full width of symmetric bounded jitter. |
| `--retry-max-delay DURATION` | Cap the computed retry delay. |
| `--run-timeout DURATION` | Limit each target-command run. |
| `--job-timeout DURATION` | Limit the entire job, including waits and delays. |
| `--after-success JOB` | Require JOB to complete successfully first; repeatable. |
| `--after-finish JOB` | Require JOB to reach any terminal outcome first; repeatable. |
| `--after-failed JOB` | Require JOB to complete with outcome `failure`; repeatable. |
| `--after-outcome JOB=OUTCOME[,OUTCOME...]` | Require one of the listed terminal outcomes. |
| `--pool NAME` | Use the named local concurrency pool. |
| `--slots N` | Reserve N global and pool slots while a run is active; default 1. |
| `--wait` | Wait for the terminal job outcome before returning. |
| `--foreground` | Attach terminal input and output; implies `--wait`. |

Advanced completion options use the explicit names `--max-runs`,
`--success-target`, and `--failure-limit`. They accept positive integers;
limits that support no bound use the literal value `unlimited`, never a
negative sentinel.

### 4.5 `jobman list`

`list` shows known jobs, newest first by default. It supports filters for
phase, outcome, name, group, submission time, and active/completed status.
Human output is a table. JSON output is a complete array and is not affected by
terminal width.

Useful options SHOULD include `--all`, `--active`, `--completed`, `--limit`,
`--group`, `--name`, and `--show-runs`.

### 4.6 `jobman status`

`status JOB` produces a concise, one-line current status intended for quick
interactive checks and shell conditions. With `--json`, it returns the same
stable job summary object used by `list`.

### 4.7 `jobman show`

```text
jobman show job JOB
jobman show run JOB RUN
```

The human `show job` view is a concise operational summary of the job,
lifecycle, runs, log availability, and notification counts. `show job --json`
exposes the complete effective specification, process identity, log locations,
and notification record. `show run` displays one run. A negative run number
indexes backward from the latest run, so `-1` means the latest.

`jobman show JOB` MAY be shorthand for `jobman show job JOB`.

### 4.8 `jobman logs`

```text
jobman logs [OPTIONS] JOB
```

Options include:

- `--run N` for one run, with negative indexing;
- `--run all` for all runs in order;
- `--follow`, `-f`, to continue until the selected active run ends;
- `--lines N` for the last N logical lines, with `-1` meaning all;
- `--stream stdout|stderr|both`; and
- `--raw` to omit Jobman presentation prefixes.

Following a completed run exits immediately after existing output. Following
an active run MUST tolerate rotation and terminate after all recorded output is
drained. Exact ordering between stdout and stderr cannot be guaranteed; the
combined view preserves Jobman's observed sequence.

### 4.9 `jobman cancel`

```text
jobman cancel job JOB
jobman cancel run JOB RUN
```

Cancellation records intent durably before signaling. A job cancellation stops
the active run and prevents future runs. In v1, cancelling a selected active
run also cancels its owning job, so no retry follows.

`cancel` is the only command name. Jobman does not provide a `kill` alias.

### 4.10 `jobman pause` and `jobman resume`

```text
jobman pause JOB
jobman resume JOB
```

Pausing a job prevents new runs and policy progress. If a target is active,
Jobman also attempts to suspend its verified process tree. Resuming reverses
that operation and continues from the phase recorded when the pause was
accepted. Cancellation remains valid while paused and takes precedence over a
resume request.

Pausing an active process tree is a platform-dependent, best-effort feature.
Jobman MUST probe the capability and return a clear unsupported or partial
error rather than record a false paused state. Pausing a job that is waiting,
queued, or in backoff is portable because no target needs to be suspended.
During the narrow process-creation `starting` window, Jobman returns a
retryable state conflict rather than accept a pause that could race process
publication.

### 4.11 `jobman input`

```text
jobman input [--eof] JOB
```

`input` copies raw bytes from the client's standard input to the active run's
bounded supervisor-owned input channel. It does not accept input text as a
positional argument, because arguments are commonly exposed in process lists
and shell history. `--eof` closes the target's input after all accepted bytes
are delivered. When client stdin is an interactive terminal, `--eof` MUST NOT
wait for terminal input; it delivers no bytes and closes the target input
immediately. Piped and redirected bytes are still delivered before EOF. Input
is local-only, preserves bytes exactly, and reports partial delivery distinctly
from complete delivery.

This v1 command is not terminal reattachment: Jobman does not reproduce
terminal modes, signals, or a PTY.

### 4.12 `jobman clean`

`clean` removes completed state and logs eligible under retention policy. It
supports `--dry-run`, an optional job selector, and an `--older-than` override.
It MUST NOT delete an active job, an active run, or a file currently owned by a
supervisor. Any destructive invocation requires explicit `--force`; Jobman
does not prompt when input may be non-interactive.

`clean` is a policy-based dry run by default. `--force` applies the selected
plan without requiring the redundant `--dry-run=false`. `clean JOB` selects
that completed job and its logs independently of retention policy or
configuration, and `--older-than` may restrict that selection. If an explicit
job selector resolves to a nonterminal job, cleanup MUST return a conflict
error in both dry-run and forced modes. `--all` selects every completed job and
its logs independently of retention policy or configuration; it cannot be
combined with a job selector or `--older-than`. Dependency, notification,
admission, and active-state safety checks still apply.

### 4.13 `jobman config`

Initial subcommands are:

```text
jobman config show
jobman config paths
jobman config validate [PATH]
jobman config apply
```

`show` prints effective configuration and source information. Secret values are
redacted in every format. `paths` reports searched paths without requiring them
to exist. `validate` performs strict schema and semantic validation without
starting a job. `apply` explicitly writes store-wide concurrency limits without
submitting a job. `run`, `rerun`, and policy-based `clean` are also
configuration-authority operations. Read/inspection, emergency commands,
`doctor`, explicitly selected or age-based cleanup, and `clean --all` do not use
configuration as a store authority and remain usable when it is malformed.

### 4.14 `jobman doctor`

`doctor` checks state paths, local-filesystem support, database integrity,
foreign keys, schema version, and WAL health without requiring configuration.
`--backup PATH` writes a new consistent SQLite snapshot. `--repair` explicitly
authorizes a WAL checkpoint, stale lifecycle reconciliation, and recovery of
due notification work; it does not rewrite corrupt rows or adopt a target.

### 4.15 Exit status

The stable CLI uses these categories:

| Code | Meaning |
| ---: | --- |
| 0 | Requested operation succeeded. |
| 1 | Runtime or internal operation failed. |
| 2 | Invalid CLI usage or configuration. |
| 3 | Requested job or run was not found. |
| 4 | Selector was ambiguous. |
| 5 | Operation conflicts with current state. |
| 6 | Operation completed only partially. |

When `run --wait` is used, the default remains to return a Jobman category
rather than the target's raw exit code. A future `--propagate-exit-code` option
MAY request target exit propagation where representable.

## 5. Job and run lifecycle

Job state is represented by an operational **phase** and an optional terminal
**outcome**. Keeping them separate avoids ambiguous states such as whether a
timed-out process has actually stopped.

### 5.1 Job phases

```text
submitting
    -> waiting
    -> queued
    -> starting
    -> running
    -> backoff -> queued

waiting | queued | running | backoff -> paused -> recorded prior phase
starting | running | paused -> stopping
waiting | queued | running | backoff | stopping -> completed
```

- `submitting`: durable record exists, but no supervisor has acknowledged it.
- `waiting`: job dependencies or initial wait conditions are not yet satisfied.
- `queued`: prerequisites are satisfied, but required concurrency slots have
  not been admitted.
- `starting`: a run is being prepared or process creation is being committed.
- `running`: a target process is believed to be active.
- `backoff`: policy permits another run after a delay.
- `paused`: user intent has stopped policy progress and, when applicable, the
  verified active process tree; the prior phase is recorded for resume.
- `stopping`: cancellation or timeout was recorded and termination is in
  progress.
- `completed`: no further run may start.

The supervisor MAY move directly between applicable phases, such as
`submitting -> starting` or `running -> completed`.

### 5.2 Outcomes

Terminal job outcomes are:

- `success`: the configured success target was met;
- `failure`: a non-retryable failure occurred or retry capacity was exhausted;
- `timed_out`: the whole-job timeout expired;
- `cancelled`: an accepted cancellation prevented further work;
- `aborted`: a wait or retry deadline prevented a required run;
- `lost`: Jobman cannot prove a safe final result after supervisor or process
  identity loss; and
- `submission_failed`: a supervisor could not take ownership.

Run outcomes are `success`, `failure`, `timed_out`, `cancelled`, `start_failed`,
or `lost`. Exit code, signal or platform termination reason, and timestamps are
stored separately from the outcome.

An outcome is written exactly once unless an explicit recovery operation
changes `lost` to a provable result. Ordinary clients MUST NOT rewrite terminal
history.

### 5.3 Invariants

- At most one run of a job is active at a time in v1.
- A run cannot enter `starting` without a durable admission satisfying the
  store-wide and selected-pool limits.
- A paused active run retains its admission slots.
- A completed job can never start another run.
- Run numbers are never reused, including after a failed start.
- Counters and state transitions commit in the same transaction.
- A process is never considered owned based on PID alone.
- Cancellation and timeout intent is durable before a signal is sent.
- Wall-clock changes MUST NOT shorten or extend elapsed-duration policies;
  monotonic time is used while a supervisor remains alive.

## 6. Execution architecture

### 6.1 Per-job supervisor

Each accepted job has one detached supervisor implemented by the Jobman binary
through a private internal entry point. The submission client:

1. validates and resolves configuration;
2. writes the job and supervisor launch token in one transaction;
3. starts a detached copy of Jobman with only the job ID and one-time token;
4. waits for the supervisor to claim the job; and
5. prints the job ID and exits.

The supervisor owns dependency and wait evaluation, concurrency admission, run
creation, retry delays, timeout enforcement, pause handling, output capture,
live input, and notifications for that job. There is no shared always-running
Jobman process.

The internal entry point is not a supported user interface and MUST reject jobs
it cannot claim atomically. Tokens MUST be random, short-lived, stored with
user-only access, and removed after the claim.

### 6.2 Cleanup

Cleanup is an explicit `jobman clean` operation. It claims work
transactionally, bounds each request, and can be previewed before deletion.
Ordinary interactive commands do not perform hidden retention cleanup, and
there is no periodic reaper daemon.

### 6.3 Internal package boundaries

The intended Go architecture is:

```text
main.go                    process entry point only
jobman/                    public CLI construction and application facade
internal/model/            immutable specs, state, and transition validation
internal/store/            metadata transactions and migrations
internal/supervisor/       per-job orchestration
internal/executor/         platform-neutral process API
internal/platform/         OS-specific detach, identity, and signaling
internal/logstore/         stream capture, indexing, rotation, and follow
internal/input/            private local live-input transport
internal/policy/           waits, completion rules, delays, and timeouts
internal/notify/           notifier interfaces and built-ins
internal/config/           loading, merging, validation, and redaction
```

Core policy and state types MUST NOT depend on Cobra, Viper, SQLite, or OS
process types. Interfaces belong at boundaries with multiple implementations or
where deterministic testing requires substitution; internal code SHOULD avoid
single-implementation interface abstractions.

## 7. Persistence and concurrency

### 7.1 Storage choice

The default metadata store is SQLite accessed through a pure-Go driver so the
release remains a single CGO-free executable. SQLite is supported on every
target platform before that platform is declared stable.

The database stores specifications, state transitions, process identities,
counters, notification attempts, leases, and log metadata. Bulk stdout and
stderr content is stored in files rather than database rows.

SQLite configuration MUST include:

- foreign-key enforcement;
- a bounded busy timeout;
- transactions for every state transition;
- write-ahead logging where reliable on the local filesystem;
- explicit schema versions and ordered migrations; and
- durability settings documented and tested against abrupt process exit.

Network filesystems are not assumed safe. Jobman SHOULD detect common unsafe
store locations or locking failures and reject them with guidance rather than
silently weakening isolation.

### 7.2 Default paths

All paths are per-user and overridable. Defaults follow platform conventions:

- Linux and other XDG systems: `$XDG_STATE_HOME/jobman`, falling back to
  `~/.local/state/jobman`;
- macOS: `~/Library/Application Support/jobman`, with logs below its `logs`
  directory; and
- Windows: `%LOCALAPPDATA%\Jobman`.

Configuration uses the corresponding config location rather than the state
location. The store records canonical absolute paths needed by an already
submitted job, so later working-directory changes cannot redirect its files.

### 7.3 Permissions

Directories and files MUST be private to the current user by default. On Unix,
Jobman uses mode `0700` for directories and `0600` for files, subject only to
more restrictive behavior. On Windows, Jobman uses user-restricted ACLs and
does not rely on POSIX mode emulation.

Jobman refuses to use a store owned by another user or one with unsafe
permissions unless an explicit repair operation safely corrects it.

### 7.4 Process identity and leases

Supervisor and target identity includes PID plus platform-specific creation
identity. On Linux this includes `/proc/<pid>/stat` start time; macOS and
Windows implementations use their native process creation information or
handles where available. Command line and environment are diagnostic evidence,
not identity, because they can change or expose secrets.

Supervisors renew a database lease with wall-clock and monotonic observations.
A stale lease does not alone prove that a target exited. Reconciliation checks
the full process identity before declaring a run lost or attempting to signal
it.

## 8. Process execution

### 8.1 Direct execution

The target is stored as an executable and an ordered argument array. Jobman
uses the platform's direct process API. It does not perform shell splitting,
globbing, variable interpolation, pipeline construction, or redirection.

The job records:

- the submitted executable and argument vector;
- the resolved working directory;
- explicit environment overrides and inheritance policy;
- stdin policy;
- process-group or equivalent identity; and
- the resolution result used to start each run.

Executable lookup occurs for each run using the job's captured environment.
The resolved executable path is recorded in run metadata.

### 8.2 Environment

By default the supervisor inherits the submission environment in memory,
subject to configured exclusions. Jobman does not persist that inherited
environment wholesale. `--env NAME=VALUE` adds or replaces a value and
`--unset-env NAME` removes one. Environment ordering has no semantic meaning.

Explicit non-secret overrides may be persisted. Secrets are represented in the
specification by references to environment variables, user-private files, or a
credential provider. Resolved secret values exist only in supervisor memory for
as long as required and are not written to the metadata store. Reruns resolve
the references again and fail clearly if a referenced secret is unavailable.
Human output and diagnostics MUST redact configured secret names and patterns.

### 8.3 Standard input and live input

Detached jobs cannot safely retain the submitting terminal as stdin. The
default stdin is the null device. Supported policies are:

- `--stdin null` (default);
- `--stdin-file PATH`;
- `--stdin live`; and
- inherited stdin only in `--foreground` mode.

For a detached job using `--stdin live`, the supervisor retains a writable
pipe to the target and exposes a private local IPC endpoint. `jobman input`
streams bounded chunks through that endpoint. The endpoint and any capability
material MUST be user-private, derived from canonical job identity rather than
a display name, and removed when the run ends. No network listener is opened.

Backpressure is explicit: v1 clients wait for at most 30 seconds and receive a
partial-delivery error containing the accepted byte count. Concurrent input
clients are serialized in admission order. Input is not persisted for later
replay by default, and Jobman MUST NOT log input bytes or include them in
notifications. EOF is durable intent and may be accepted at most once per run.
Interactive terminal reattachment remains outside v1. A future release may
make the bounded client timeout configurable without changing delivery
semantics.

### 8.4 Process trees and signals

Each run receives an independent process group or closest safe platform
equivalent. Cancellation targets the group so children do not normally survive
their parent. The default stop policy is:

1. send the platform's graceful termination request;
2. wait a configurable grace duration; and
3. force termination if the exact process identity is still active.

The graceful request is the platform adapter's fixed safe default: `SIGTERM`
for Unix process groups and best-effort `CTRL_BREAK_EVENT` for Windows Job
Objects. Users configure the grace duration and whether forceful termination
follows, not a platform-specific signal name. Jobman MUST NOT silently claim a
graceful request succeeded when the platform rejected it.

Pause and resume operate on the verified process tree, not a bare PID. On
platforms without a safe tree-wide suspension mechanism, pausing a running job
is unsupported. Jobman may report partial failure if the tree changes during
the operation, but MUST retain enough state to allow cancellation and
reconciliation.

## 9. Prerequisites and local concurrency admission

### 9.1 Wait conditions

The first run may be gated by zero or more conditions. Conditions are combined
using `all` by default or `any` when requested. State records each condition's
last evaluation, last error, and satisfaction time.

Initial condition types are:

- `until`: an absolute RFC 3339 timestamp;
- `delay`: an elapsed duration after supervisor acceptance;
- `file-exists`: a path exists with optional required type; and
- `probe`: a direct executable returns exit code 0.

Probe executions have their own timeout, output limit, environment policy, and
poll interval. A probe error is recorded and treated as unsatisfied unless its
policy marks the error fatal. Polling uses bounded jitter to avoid synchronized
wakeups.

`wait-abort-at` prevents a first run whose start would occur after the given
timestamp. An elapsed whole-job timeout has the same effect with outcome
`timed_out` rather than `aborted`.

Jobman does not provide a “file is not open by any process” condition because
operating systems do not expose portable, race-free proof of that property. A
future version MAY provide a distinctly named cooperative advisory-lock
condition.

### 9.2 Job dependencies

Dependency flags are repeatable and all dependencies must be satisfied. Each
selector is resolved at submission to an immutable job ID and its required
terminal outcomes; names and prefixes are never reevaluated later. A dependency
must already exist, cannot refer to the submitted job, and cannot introduce a
cycle. Duplicate requirements are coalesced when equivalent and rejected when
contradictory.

`--after-success JOB` requires outcome `success`. `--after-failed JOB` requires
outcome `failure` exactly; timeout, cancellation, abort, loss, and submission
failure are not silently treated as failure. `--after-finish JOB` accepts every
terminal outcome. `--after-outcome` provides the explicit general form and
accepts only terminal job outcomes defined by the current schema.

If a dependency reaches a terminal outcome that cannot satisfy its predicate,
the dependent job completes as `aborted` with reason
`dependency_unsatisfied`; its target never starts. Dependency state and the
observed prerequisite revision are recorded for inspection. The whole-job
timeout includes dependency waiting. Cleaning MUST retain enough tombstone
metadata to evaluate dependencies that still reference the cleaned job.

Dependencies provide simple local sequencing, not arbitrary workflow graphs:
Jobman does not add output passing, conditional expressions, dynamic fan-out,
or cross-host dependencies in v1.

### 9.3 Concurrency limits

Jobman enforces two local layers of integer-slot admission:

1. a store-wide `max_active_slots` limit; and
2. an optional named pool capacity selected by `--pool`.

Every run requests `--slots N`, defaulting to one, and consumes that many slots
from both layers while it is starting, running, stopping, or paused. A job may
select at most one named pool in v1. Groups remain descriptive labels and do
not implicitly create pools. An unlimited store-wide limit or pool capacity
must use the explicit value `unlimited`.

The built-in store-wide limit is `unlimited`. A named pool must be defined
before submission; selecting an unknown pool is a validation error. This makes
concurrency control opt-in without imposing an arbitrary machine-wide default.

Capacities are synchronized by `config apply`, `run`, `rerun`, and policy-based
`clean`, not by read-only or emergency commands. `config apply` is the
administrative form that changes no other state; `run` synchronizes the
effective limits before accepting a job so submission cannot accidentally use
stale authority.
Submission fails if a finite capacity can never satisfy the requested slots.
Reducing a capacity below current use does not
stop admitted work; it prevents further admissions until usage falls within
the new limit.

Admission and release are atomic SQLite transitions with leases. A supervisor
may start a target only after committing an admission, and completion or
reconciliation releases it exactly once. Eligible jobs are considered by
prerequisite-satisfied time and then job ID, with bounded bypass permitted when
the oldest job cannot fit currently available slots. Starvation prevention and
the exact bypass bound are versioned policy, not an accident of process races.

There is no scheduler daemon. Supervisors use bounded jittered polling and
transactional compare-and-swap operations; clients may opportunistically wake
local supervisors, but correctness never depends on a notification. These
limits are for protecting one user's local machine, not CPU/GPU discovery,
memory accounting, priorities, preemption, or fair-share scheduling.

## 10. Completion, retry, and repetition policy

### 10.1 Classification

A run is classified using:

- success exit codes, default `{0}`;
- retryable failure exit codes or ranges, default all non-success exit codes
  when another run is permitted;
- retryable termination signals or platform reasons; and
- whether timeout, start failure, or cancellation is retryable.

Success and retryable-failure sets MUST NOT overlap. An unlisted nonzero exit is
a retryable failure by default; selecting one or more retryable exit codes
changes the policy to that explicit allowlist. Cancellation, process-creation
errors, and termination by signal or timeout are non-retryable unless
explicitly enabled.

### 10.2 Completion limits

The general policy can constrain:

- `max_runs`: maximum total runs, or `unlimited`;
- `success_target`: successful runs required for a successful job outcome, or
  `unlimited` when success count does not terminate the job;
- `failure_limit`: failed runs that produce a failed job outcome, or
  `unlimited`;
- a retry abort timestamp; and
- the whole-job timeout.

The CLI exposes these as `--max-runs`, `--success-target`, and
`--failure-limit`. The default is `max_runs=1`, `success_target=1`, and
`failure_limit=1`. `--retries N` is shorthand for one required success, at most
`N + 1` total runs, and a failure limit of `N + 1`, with retryable failures
eligible for another run.

These limits support, among other combinations:

- one ordinary run;
- retry until the first success, bounded or unbounded;
- stop after a configured number of failures;
- collect a configured number of successful runs;
- combine success, failure, total-run, timestamp, and elapsed-time bounds; and
- intentionally continue indefinitely until cancellation when every applicable
  limit is `unlimited`.

At least one reachable terminal condition is strongly recommended. A policy
with no finite limit is valid only when the user explicitly writes
`unlimited`; command help and policy documentation warn that such a job
requires cancellation or an external termination event.

After every run, Jobman evaluates terminal conditions in deterministic order:

1. cancellation or whole-job timeout;
2. finite success target reached;
3. non-retryable outcome;
4. failure or run limit reached;
5. retry deadline would be exceeded; or
6. schedule another run after the computed delay.

Successful runs below the target and retryable failed runs both proceed to the
next policy evaluation. A success does not stop the job unless its success
target has been reached. Success targets greater than one intentionally support
repeated successful runs, not merely retries after failure.

### 10.3 Delay

Let `n` be the number of completed runs before the upcoming delay, starting at
1, and `d` the nonnegative base delay.

- constant: `d`;
- linear: `d * n`; and
- exponential: `d * base^(n-1)`, where `base >= 1`.

The result is capped by `max_delay` when configured. Symmetric jitter of width
`j` then samples uniformly from `[delay-j/2, delay+j/2]` and clamps the result
to `[0, max_delay]`. Tests use an injected random source and clock.

## 11. Timeouts and cancellation

- A run timeout starts immediately before process creation and ends only after
  the process tree is reaped or declared lost.
- A job timeout starts when the supervisor accepts the job and includes initial
  waits, runs, backoff, and notification work needed before completion.
- Timeout first records durable intent, then invokes the configured stop policy.
- A timeout is not reported as complete while the target is still known alive;
  the phase remains `stopping`.
- Cancellation is idempotent. Repeated requests return the same accepted state.
- If process identity is uncertain, Jobman MUST refuse to signal the PID and
  mark the run for reconciliation.
- User-requested pause time does not consume elapsed run or job timeout budgets;
  their monotonic deadlines are extended on resume. Absolute timestamps, such
  as dependency or retry abort times, do not move. A timeout or cancellation
  already committed before a pause request takes precedence.

## 12. Logging

### 12.1 Captured data

Each run stores stdout and stderr as separate raw byte streams. A sequence index
records observed chunks with stream identity, byte offsets, wall timestamp, and
monotonic ordering. This permits separate or combined views without modifying
the target's bytes.

Timestamp and stream prefixes are presentation features; they are not inserted
into raw output. This preserves binary output and prevents partial-line
ambiguity. Jobman records its own supervisor diagnostics separately from target
output.

### 12.2 Rotation

Rotation may be limited by bytes per segment and segments per run. Segment
creation is atomic, and the index is committed only after the segment exists.
Readers tolerate a final actively written segment.

Retention may constrain:

- maximum age of completed jobs;
- maximum retained jobs;
- maximum runs per job;
- maximum log bytes per job; and
- maximum total log bytes.

Deletion proceeds oldest eligible content first, never removes active-run
content, and records what was removed so metadata does not claim logs still
exist. “Unlimited” is represented explicitly, never by overloaded negative
durations in persisted schemas.

By default, completed job metadata is retained indefinitely and completed-run
logs are retained for 30 days. There is no default byte or job-count cap. The
effective policy is shown by `config show`, and `list` and `doctor` report store
usage so operators can monitor disk capacity. Cleanup marks pruned log ranges
in retained metadata. Jobman MUST prominently document the 30-day log default
and return a visible error when an explicit cleanup operation fails.

### 12.3 Following

`logs --follow` uses bounded polling so behavior remains consistent across the
three supported operating systems and filesystems. It handles rotation,
truncation performed by Jobman, supervisor exit, and client interruption. Slow
readers do not block the target process; the supervisor writes to disk
independently of readers. A compatible future release may use native
filesystem notification as an optimization without changing these semantics.

## 13. Notifications

Initial notifier types are:

- command hook;
- HTTP webhook; and
- SMTP email.

A notifier subscribes to explicit events such as job started, run succeeded,
retry scheduled, job succeeded, job failed, timed out, cancelled, or lost.
Notification failure never changes the job or run outcome.

Every delivery has a durable record containing notifier identity, event ID,
attempt number, timestamps, non-secret response metadata, and result. Delivery
uses a configurable timeout and bounded retry policy. Event IDs are stable so
receivers can implement idempotency; delivery is at least once, not exactly
once.

Command hooks receive a versioned JSON event on stdin and a minimal documented
environment. Their stdout and stderr are bounded and recorded separately. HTTP
webhooks receive the same JSON schema and SHOULD support a secret-based request
signature. Email credentials MUST use the secret mechanism, not literal values
printed by `config show`.

The supervisor may mark the job completed before all notification retries
finish, but it remains responsible for bounded notification completion. The
retention system does not remove a job with pending notification work.

## 14. Configuration

### 14.1 Format and precedence

The normative configuration format is YAML. Supporting every syntax understood
by Viper is intentionally not part of the contract because multiple formats
produce inconsistent typing and merge behavior.

Precedence from highest to lowest is:

1. command-line flags;
2. `JOBMAN_` environment variables;
3. an explicitly selected `--config` file;
4. an explicitly trusted project configuration;
5. per-user configuration;
6. system configuration; and
7. built-in defaults.

Maps merge recursively. Scalars and lists replace lower-precedence values
unless a field explicitly documents additive behavior. Unknown keys,
duplicate keys, invalid types, contradictory settings, and unusable paths are
errors. Environment variable mapping is documented and reversible; for
example, `JOBMAN_CONCURRENCY_MAX_ACTIVE_SLOTS` maps to
`concurrency.max_active_slots`.

Jobman never automatically trusts a project file merely because it exists in
the current directory or an ancestor. A project configuration is loaded only
when selected with `--config` or when its canonical project root is present in
an explicit user-controlled trust allowlist. Trust management displays the
exact path and warns that the file can select probes, hooks, environment, and
target commands. System or project configuration cannot add a trusted root.

### 14.2 Named objects

Configuration may define:

- `job_specs`: reusable run specifications;
- `wait_conditions`: reusable conditions;
- `concurrency`: the store-wide slot limit and named pool capacities;
- `notifiers`: reusable notifier configurations;
- `retention`: global defaults; and
- `profiles`: explicit bundles of overrides.

Implicit configuration selected by job name, group, or command regular
expression is deferred until its precedence and observability can be made
unambiguous. The initial design favors explicit `--job-spec` and `--profile`
selection.

### 14.3 Schema evolution

Configuration and JSON output have independent integer schema versions. State
database migrations are ordered, transactional where SQLite permits, and
backed up before every supported forward conversion. A newer unsupported state schema is a
hard error. Downgrades that would write incompatible state are rejected.

## 15. Security and privacy

- Jobman does not create a security boundary around target commands. Targets
  run with the submitting user's privileges.
- Direct execution is the default to avoid accidental shell injection.
- State paths are canonicalized and created without following attacker-created
  symlinks where platform APIs permit.
- Log rotation and cleanup verify ownership and containment before modifying a
  path.
- Process signaling requires verified identity, not only PID.
- HTTP notifiers default to HTTPS; redirects and access to local or link-local
  destinations are governed by explicit policy.
- Error messages and structured output pass through field-aware redaction.
- Arbitrary regular-expression redaction MUST have bounded resource behavior.
- Jobman warns that secrets present in target output cannot be perfectly
  removed after they have already been written.

Commands, arguments, working directories, environment values, probe output,
and logs may all be sensitive. The threat model assumes another process running
as the same OS user can generally inspect or interfere with Jobman; defending
against a compromised account is not a v1 goal.

## 16. Fault tolerance and recovery

### 16.1 Supervisor failure

Every ordinary client performs bounded reconciliation of stale leases. If the
supervisor is gone:

- a terminal completion commit remains authoritative;
- a verified live target without a supervisor is marked `lost` and is not
  automatically adopted in v1;
- an unverifiable or reused PID is never signaled; and
- no target plus no provable result produces outcome `lost`.

Automatic supervisor restart is not required because reconstructing timers,
open pipes, and process ownership safely after a crash is platform-dependent.
Jobman does not provide a resident recovery service or automatically adopt an
orphaned target. A future explicit local recovery command may adopt only states
for which safety can be proven.

### 16.2 Abrupt writes

Tests MUST kill clients and supervisors at state-transition fault points. After
restart, the database is either at the previous valid transition or the next
valid transition. Log indexes may lag raw durable bytes but MUST be repairable
without fabricating data.

### 16.3 Reboot

Jobman stores a boot/session identity with active records. After reboot, stale
active records become `lost` or `aborted` according to provable process state;
they are not resumed. PID values from an earlier boot are never signaled.

## 17. Platform requirements

Platform-specific process code is isolated behind narrow concrete adapters and
build-tagged files.

- Linux uses sessions/process groups and `/proc` creation identity where
  available.
- macOS uses process groups and native process information; unsupported Linux
  assumptions are not emulated with shell commands.
- Windows uses detached process creation, creation timestamps or handles, Job
  Objects where appropriate, and native termination semantics.

A feature unavailable with safe equivalent semantics on a platform either
fails clearly or is documented as unsupported. It MUST NOT silently behave as
though its guarantee were met. The stable v1 release requires core run,
inspect, logging, timeout, and cancellation behavior, dependency evaluation,
local concurrency admission, and live input on Linux, macOS, and Windows.
Pause/resume remains an explicitly best-effort capability and may be
unsupported on a platform when safe process-tree suspension is unavailable;
best-effort feature parity remains the goal.

Survival of terminal closure and SSH disconnection is required and tested on
every supported platform. Survival after the entire OS user session ends is not
required and is not claimed.

## 18. Observability

Jobman's own diagnostic log is separate from target output and includes event
time, severity, component, job/run IDs, stable event name, and redacted fields.
Default logging is concise. Debug logging is opt-in and still redacted.

`jobman doctor` is the diagnostic command that checks paths,
permissions, database integrity, locking, platform capabilities, stale leases,
and notifier prerequisites without modifying jobs unless `--repair` is
explicitly supplied.

## 19. Testing and quality strategy

### 19.1 Unit and property tests

- Every legal and illegal state transition.
- Completion-policy invariants across generated outcome sequences.
- Retry delay bounds, jitter, overflow, and cancellation.
- Dependency satisfaction, contradiction, terminal mismatch, and immutable
  selector resolution.
- Concurrency admission safety, bounded bypass, starvation prevention, lease
  expiry, and exactly-once release.
- Pause/resume timeout accounting and transition precedence.
- Duration, size, timestamp, selector, and configuration parsing.
- Redaction properties and secret nonappearance in diagnostics.
- Log rotation and retention ordering.

Policy tests use injected clocks, random sources, and process fakes. They MUST
not depend on wall-clock sleeps.

### 19.2 Integration tests

- Real subprocess success, failure, signals, output, and process trees.
- Submission acknowledgement and terminal detachment.
- Concurrent readers, writers, cancellation, and cleanup.
- Run and job timeouts, including uncooperative children.
- Wait probes and retry sequences.
- Dependency chains and concurrent slot contention across the global limit and
  named pools.
- Pause/resume capability detection, partial process-tree suspension, and
  cancellation while paused on each platform.
- Binary live input, concurrent writers, backpressure, EOF, partial delivery,
  and abrupt supervisor exit.
- SQLite lock contention, migration, and abrupt supervisor termination.
- Log following through rotation.
- Notifier timeout, retry, duplicate delivery, and output bounds.

### 19.3 Robustness testing

- Go race detector on supported packages.
- Native Go fuzzing for configuration, selectors, state decoding, and log
  indexes.
- Property-based tests for state machines and retention.
- Regression fixtures for every production defect.
- Fault injection at durable transition boundaries.
- Cross-platform CI and architecture release builds.

End-to-end tests use generous eventually assertions rather than fragile fixed
sleeps, but every test has a hard upper deadline.

## 20. Compatibility policy

The v1 CLI and machine contracts are frozen in
[`docs/COMPATIBILITY.md`](../COMPATIBILITY.md). Throughout the v1 release line:

- documented CLI flags and JSON fields follow semantic-versioning compatibility;
- new JSON fields may be added in minor versions;
- field removal or semantic reinterpretation requires a major version;
- state migrations support every schema version documented as upgradeable; and
- generated help, man pages, completions, sample configuration, and this spec
  must agree.

## 21. Implementation milestones

The following milestones record the original standalone delivery sequence.
The supervisor and persistence choices are governed by
[ADR-0001](adr/0001-per-job-supervisor.md) and
[ADR-0002](adr/0002-sqlite-metadata-and-filesystem-logs.md).

### Milestone 1: core model and foreground execution

- immutable job/run specifications;
- state transition engine and completion policy;
- strict configuration types;
- direct foreground execution and raw stream capture; and
- deterministic unit/property tests.

### Milestone 2: durable local jobs

- SQLite store, migrations, and permissions;
- per-job detached supervisor and acknowledgement protocol;
- `run`, `list`, `status`, and `show`; and
- crash and concurrency tests.

### Milestone 3: process management and logs

- platform process groups/Job Objects and verified identity;
- cancellation and run/job timeouts;
- indexed logs, rotation, follow, and `logs`; and
- Linux, macOS, and Windows end-to-end coverage.

### Milestone 4: policies

- wait conditions, job dependencies, and probes;
- transactional global and named-pool concurrency admission;
- retry/repetition policies, delay, and jitter;
- retention and `clean`; and
- rerun and named job specifications.

### Milestone 5: interaction and notifications

- best-effort `pause` and `resume` with platform capability reporting;
- command, webhook, and email notifiers;
- admission fairness and dependency diagnostics; and
- cross-platform policy end-to-end coverage.

### Milestone 6: live input and hardening

- private local live-input endpoints and the `input` command;
- backpressure, partial-delivery, and EOF semantics;
- recovery, doctor, and fault injection;
- security and redaction review; and
- documentation and compatibility audit.

## 22. Resolved product decisions

The following decisions were accepted on 2026-07-14 and are reflected in the
normative sections above:

1. Jobman guarantees survival of terminal closure and SSH disconnection, not
   termination of the entire operating-system user session.
2. Linux, macOS, and Windows are v1 build targets, with best-effort parity and
   portable core behavior required for every stable release.
3. `cancel` is the sole canonical command; there is no `kill` alias.
4. The product supports a broad matrix of bounded or unbounded repeated-run,
   success-target, failure-limit, retry, deadline, and timeout semantics using
   explicit names.
5. Project configuration is never trusted merely because it is present. It
   requires explicit selection or a user-controlled trust allowlist.
6. A global “file is not open” wait is omitted because it cannot be implemented
   portably or without races. A future advisory-lock condition must be named
   according to its actual cooperative semantics.
7. Resolved secrets are not persisted. Specifications store re-resolvable
   references, while supervisors keep required values only in memory.
8. Running a target requires the explicit `run` subcommand; bare `jobman`
   displays help.
9. `status` remains the concise current-state command and `show` remains the
   detailed diagnostic command. Their JSON summaries share a schema.
10. Completed metadata is retained indefinitely by default, completed-run logs
    are retained for 30 days, and no byte or job-count cap applies by default.
11. Local concurrency uses a store-wide slot limit plus at most one named pool
    per job. Groups remain labels, and no resource-aware scheduler is implied.
12. Job dependencies resolve selectors to immutable IDs at submission and use
    explicit success, failure, finish, or outcome predicates.
13. Pause/resume is included as a platform-dependent best-effort capability;
    unsupported platforms report the limitation instead of fabricating state.
14. Binary live input through a private local supervisor channel was delivered
    in v1 and does not provide PTY reattachment.
15. Jobman will not add a shared recovery daemon, automatic target adoption, or
    a remote-control listener. Existing SSH and host-management tools remain
    the remote access mechanism.

Future changes to these decisions require an explicit specification revision
and, once the affected behavior is stable, the compatibility process in
Section 20.
