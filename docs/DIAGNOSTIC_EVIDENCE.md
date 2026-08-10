# Diagnostic evidence schema 1

Status: schema 1 released in v1.4.0; additive system-context collection is
implemented on `main`

Jobman can export a bounded, immutable snapshot of factual job observations
without interpreting target output or contacting a model:

```console
jobman show evidence JOB
jobman show evidence --json JOB
jobman show evidence --command --json JOB
jobman show evidence --system --json JOB
jobman show evidence --logs tail --log-bytes 64KiB --json JOB
jobman show evidence --similar 5 --json JOB
```

The public, standard-library-only Go contract is
`github.com/ryancswallace/jobman/diagnostic`. The JSON document kind is
`jobman.diagnostic_evidence`, and its independently versioned schema is `1`.
The optional [jobman-diagnose companion] consumes this interface, but core
evidence remains useful to scripts and support tooling on its own.

## Collection and selection

Metadata is read in one SQLite read transaction after bounded stale-lifecycle
reconciliation. Log files are read after that transaction and therefore carry
an explicit `stable`, `point_in_time`, `mixed`, or `not_collected` consistency
value. Evidence captured for an active job also sets
`active_state_may_have_advanced`.

Without a run flag, Jobman selects the active run, otherwise the latest
abnormal run, otherwise the latest run. `--run N` selects one positive run
number or negative index. `--all-runs` selects bounded history. A job literally
named `evidence` remains addressable as `jobman show job evidence`.

The current hard limits are:

| Domain | Default | Hard limit |
| --- | ---: | ---: |
| Selected history | one run | 100 runs |
| Lifecycle events | 500 | 500 |
| Notification deliveries | 200 | 200 |
| Notification attempts | 200 | 200 |
| Log content | not collected | 1 MiB across one bundle |
| Direct command | not collected | 128 KiB and 1,024 arguments per command item |
| Path | not collected | 4 KiB per path item |
| Environment names | not collected | 2,048 names per item; values are never collected |
| System context | not collected | one allowlisted point-in-time item |
| Requested tail | 64 KiB per selected stream | 1 MiB per selected stream, subject to the bundle limit |
| Similar histories | not requested | 20 exact store-local fingerprint matches |
| Public decoder input | 2 MiB | 2 MiB unless a caller selects a smaller positive limit |

`--logs metadata` is the default and includes only log availability, integrity,
recording health, and byte counts. `--logs tail` explicitly admits bounded raw
target bytes. `--logs none` excludes even log metadata. Every unavailable or
truncated domain produces an omission rather than silently disappearing.
`--command` separately opts in to immutable direct command specifications,
including ordered argument vectors. `--paths` opts into filesystem context,
and `--environment-names` opts into names and set/unset/secret-backed roles;
environment values and secret-reference identifiers are never collected.
`--system` opts into capacity for the filesystem containing Jobman's state and,
on Linux cgroup v2, allowlisted memory, PID, and cumulative OOM counters plus a
controlled container hint. It does not collect mount paths, cgroup paths,
hostnames, process lists, system logs, or arbitrary host configuration.

## Envelope and identity

The CLI retains Jobman's version-1 output envelope and places the sealed
evidence value under `data.evidence`:

```json
{
  "schema_version": 1,
  "data": {
    "evidence": {
      "kind": "jobman.diagnostic_evidence",
      "schema_version": 1,
      "evidence_id": "sha256:..."
    }
  }
}
```

`evidence_id` is a SHA-256 semantic identity over normalized typed content. It
excludes collection wall time, artifact capture wall time, and the encoded byte
count. Thus, collecting the same factual snapshot at another instant produces
the same ID, while changing a fact, omission, redaction notice, selected byte,
or capability changes it. `Encode` and `Decode` verify the ID and measured
limits. Decoding also rejects excessive nesting, duplicate JSON keys, duplicate
IDs, noncanonical item values, trailing values, and unsupported schema
versions.

Collections that behave as sets are sorted before sealing. Item and artifact
IDs are stable joins, not user-facing selectors. Current forms include:

```text
ev:job:FIELD
ev:run:00000000000000000001:FIELD
ev:runtime:FIELD
ev:dependency:000000:FIELD
ev:wait:000000:FIELD
ev:notification:delivery:000000:FIELD
ev:event:EVENT_ID
ev:similar:000000
artifact:run:00000000000000000001:stderr
```

Consumers must treat IDs as opaque citation keys and ignore unknown additive
item codes. A code's existing meaning or JSON value type does not change within
schema 1.

## Item registry

Ordinary facts have disclosure class `metadata`. Store-local fingerprints and
similar-failure summaries are `local_only`; explicitly requested target output
is `log_content`; direct executable/argv specifications are `command`;
filesystem locations are `path`; and value-free environment inventories are
`environment_name`. Timestamps are UTC RFC3339 JSON strings, durations are Go
duration strings, byte counts are nonnegative integers, and diagnostic records
are schema-1 objects containing only `code`, `origin`, `operation`, `category`,
`retry_class`, and allowlisted string `attributes`.

| Codes | JSON value | Direct source or exact derivation |
| --- | --- | --- |
| `jobman.source.context` | object | Jobman/collector versions, store schema, operating system, and architecture copied from the collector source |
| `jobman.job.phase`, `jobman.job.outcome`, `jobman.job.revision` | string, string, integer | Durable job snapshot |
| `jobman.job.submitted_at`, `claimed_at`, `started_at`, `completed_at` | timestamp | Durable job timestamps |
| `jobman.job.diagnostic_code` | diagnostic record | Durable safe diagnostic code; unrecognized legacy values become `legacy_unclassified` plus an omission |
| `jobman.job.cancellation.reason`, `requested_at` | string, timestamp | Durable cancellation intent |
| `jobman.job.name` | string | Sanitized immutable display name, when configured |
| `jobman.target.command`, `jobman.wait.command`, `jobman.notification.command` | `{executable, arguments}` | Immutable direct command specifications; disclosure is `command` and collection requires `--command` |
| `jobman.target.working_directory`, `stdin_path`, `jobman.wait.path`, `jobman.notification.working_directory` | string | Sanitized immutable paths; disclosure is `path` and collection requires `--paths` |
| `jobman.target.environment_names`, `jobman.wait.environment_names`, `jobman.notification.environment_names` | `{inheritance, set, unset, secret}` | Variable names and roles only; disclosure is `environment_name` and collection requires `--environment-names` |
| `jobman.policy.configuration` | object | Effective completion, classification, retry, timeout, wait, concurrency, logging, tag/group, stdin, and stop policy; excludes paths, commands, environment values, destinations, and secret references |
| `jobman.wait.configuration`, `jobman.notification.configuration` | object | Non-secret immutable prerequisite and notifier policy summaries |
| `jobman.run.phase`, `outcome`, `revision` | string, string, integer | Durable run snapshot |
| `jobman.run.reserved_at`, `started_at`, `completed_at` | timestamp | Durable run timestamps |
| `jobman.run.duration` | duration string | Exact difference between start and completion or capture time |
| `jobman.run.exit.code`, `signal`, `platform_reason` | integer, string, string | Observed platform exit record |
| `jobman.run.diagnostic_code` | diagnostic record | Durable safe diagnostic code |
| `jobman.run.timeout.scope` | `"run"` or `"job"` | Confirmed stop reason and diagnostic code |
| `jobman.run.resolved_executable` | string | Executable path resolved for a run; disclosure is `path` and collection requires `--paths` |
| `jobman.run.stop_reason` | string | Durable reason Jobman requested the target to stop |
| `jobman.log.available` | boolean | Exact durable log metadata derivation |
| `jobman.log.integrity`, `recording_health` | string | Durable log metadata |
| `jobman.log.diagnostic_code` | diagnostic record | Durable safe log diagnostic code |
| `jobman.log.stdout.bytes`, `stderr.bytes` | integer bytes | Durable nonnegative log sizes |
| `jobman.policy.run_count`, `success_count`, `failure_count` | integer | Durable policy runtime |
| `jobman.policy.next_run_at` | timestamp | Durable scheduled policy time |
| `jobman.policy.waiting_reason`, `paused_from` | string | Durable policy runtime |
| `jobman.policy.total_paused` | duration string | Durable accumulated pause duration |
| `jobman.dependency.job_id`, `predicate`, `observed_outcome` | string | Durable dependency identity and observation |
| `jobman.dependency.satisfied` | boolean | Exact presence of the satisfaction timestamp |
| `jobman.wait.kind`, `attempt_count` | string, integer | Durable wait evaluation |
| `jobman.wait.satisfied` | boolean | Exact presence of the satisfaction timestamp |
| `jobman.wait.diagnostic_code` | diagnostic record | Durable safe wait diagnostic code |
| `jobman.admission.pool`, `slots`, `lease_expires` | string, integer, timestamp | Durable active admission |
| `jobman.admission.released` | boolean | Exact presence of the release timestamp |
| `jobman.notification.status` | object | Durable notifier name, event type, status, retry schedule, transport result, and bounded attempt counts |
| `jobman.notification.diagnostic_code` | diagnostic record | Durable safe notification diagnostic code |
| `jobman.notification.retryable` | boolean | Durable transport disposition |
| `jobman.lifecycle.event` | object | Durable transition type, entity, phases, outcomes, optional run ID, and typed safe transition details |
| `jobman.failure.class` | `{class, scope}` | Deterministic low-level classification from durable result and safe diagnostic code |
| `jobman.resource.observation` | `{metric, value, unit, scope, source, completeness}` | Typed post-wait process accounting persisted atomically with run completion |
| `jobman.system.context` | `{scope, filesystem?, linux_cgroup?, container_hint?}` | Opt-in point-in-time constraints for the collector host; the filesystem scope is Jobman's state filesystem, Linux cgroup-v2 counters describe Jobman's containing cgroup, and disclosure is `metadata` |
| `jobman.failure.fingerprint` | `{algorithm, input_schema_version, value, scope}` | Opaque HMAC-SHA-256 grouping key over a versioned safe factual projection; disclosure is `local_only` |
| `jobman.failure.similar` | safe similar-run summary | Exact indexed fingerprint match containing only job/run IDs, run number, completion time, outcome, failure class, fingerprint, and whether a later run succeeded; disclosure is `local_only` |

Initial resource metrics are process-scoped user CPU time and system CPU time
in nanoseconds on all supported platforms, plus process-scoped peak resident
memory in bytes on Linux and macOS. Unsupported or inapplicable facts are
omitted, never represented as zero merely to fill a field. In particular,
Jobman does not call exit 137 an out-of-memory kill without a confirming
platform observation.

System context is deliberately different from per-run resource observations.
It is captured after the transactional job snapshot and has
`point_in_time` quality. A target normally inherits Jobman's cgroup, but the
group may contain other processes and `memory.events` counters are cumulative.
Those counters can support a hypothesis, but cannot by themselves prove that
the selected run caused an OOM event. The collector therefore does not turn
them into a deterministic OOM classification.

Fingerprints are keyed with one random 32-byte secret held in the private
SQLite store. The safe input projection includes outcome, stable failure
class, safe diagnostic/exit/timeout facts, policy disposition, and an HMACed
executable identity. It excludes arguments, environment, log content, prose,
and analyzer/model output. Values cannot be compared across independent state
stores, and the key is never evidence. Successful or insufficiently described
runs have no fingerprint.

`--similar N` opts into a single bounded indexed search for exact matches. The
metadata and matches come from the same SQLite snapshot. The selected run is
the anchor; matching summaries may represent other jobs in the same private
store but expose none of their names, specifications, paths, environments,
logs, or notifier data. Omission codes distinguish not requested, unavailable,
partially indexed historical rows, and truncation at the requested limit.

## Classification registry

Core classifications identify an observed failure mechanism, not an inferred
application root cause. Current `class` values are:

| Class | Quality | Meaning |
| --- | --- | --- |
| `executable_not_found` | confirmed | Direct executable resolution failed with not-found semantics. |
| `working_directory_missing` | confirmed | The configured working directory was directly unavailable. |
| `permission_denied` | confirmed | A direct target-start operation was denied. |
| `target_start_failed`, `submission_failed` | confirmed | Target creation failed without a narrower safe class. |
| `job_timeout`, `run_timeout`, `timeout` | confirmed | A configured timeout boundary was reached. |
| `user_cancellation` | confirmed | Durable user cancellation ended progress. |
| `supervisor_claim_expired`, `ownership_lost` | confirmed | Jobman could not establish or retain lifecycle ownership. |
| `wait_evaluation_error` | confirmed | A wait-condition evaluation failed. |
| `log_recording_degraded` | confirmed | Jobman's output recording path reported degradation. |
| `signal_termination` | confirmed | The operating system reported signal termination. |
| `nonzero_exit` | observed | The target returned a nonzero exit code. |
| `target_failure`, `job_failure_without_run` | observed | Durable failure exists but no narrower mechanism was established. |

Qualities are `observed`, `confirmed`, `derived_exact`, `point_in_time`, and
`unknown`. They describe provenance strength, not diagnosis confidence.

## Artifacts, privacy, and disclosure

The only schema-1 artifact role is `run_log_tail`. Artifacts preserve arbitrary
bytes as base64 JSON, identify the selected logical byte range, distinguish
selected source bytes from post-redaction content bytes, record truncation,
and digest the sanitized bytes actually encoded.

Evidence never includes environment values, secret-reference identifiers or
values, input bytes, notification destinations, launch credentials, or raw Go
error strings. Direct executable identities and ordered argument vectors are
omitted by default; `--command` collects bounded target, wait-probe, and command
notifier specifications and applies configured redaction to every field.
`--paths` independently collects bounded working directories, stdin/wait
paths, and per-run resolved executables. `--environment-names` collects only
variable names and whether each is set, unset, or secret-backed. `--system`
collects only the bounded allowlist described above and produces an explicit
not-requested or unavailable omission when absent. Redaction notices
identify affected evidence IDs and counts without retaining the original
values or matching patterns.

When at least one configured rule can redact a resolved literal value, core
adds source capability `configured_value_redaction_v1`. This is a narrowly
defined proof that value-aware configured redaction was active for the
collection; automatic field-name heuristics alone do not produce it. The
capability is intended as a fail-closed prerequisite for companions that might
disclose log content to a generator. It does not certify that arbitrary target
output is secret-free.

Target output is inherently untrusted and may contain secrets or adversarial
instructions. Review it before using `--logs tail` or sharing an exported
bundle. Core never sends evidence to a network service.

Disclosure classes are `metadata`, `command`, `path`, `environment_name`,
`log_content`, `sensitive`, and `local_only`. Schema 1 currently emits
`metadata`, store-local `local_only` items, explicitly requested `command`,
`path`, and `environment_name` items, and explicitly requested `log_content`
artifacts. Consumers must opt in
independently before disclosing any non-metadata class; a remote generator
should normally exclude fingerprints and similar-history summaries entirely.

The initial companion excludes `local_only` evidence from every generator
projection, including local models. Generated log analysis requires both
explicit profile/CLI approval and `configured_value_redaction_v1`.

## Compatibility fixtures

Canonical fixtures live in [`diagnostic/testdata/`](../diagnostic/testdata/).
They cover failed exit, start failure, timeout, active state, pruned logs,
successful target plus notification failure, an unknown additive item, a
secret canary, exact similar-fingerprint history, the one-bundle log ceiling,
and invalid/newer inputs. The
manifest records the semantic ID and exact file SHA-256. Consumers copy
released fixtures and run them without network access.

[jobman-diagnose companion]: https://github.com/ryancswallace/jobman-diagnose
