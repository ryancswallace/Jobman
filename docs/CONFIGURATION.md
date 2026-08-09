# Configuration reference

Status: stable v1 schema
Configuration schema: 1

Jobman uses strict, versioned YAML. Unknown or duplicate keys, invalid scalar
types, contradictory policies, and unresolved named references stop the command
before a job is submitted. Use these commands when editing configuration:

```console
jobman config paths
jobman config validate
jobman config validate ./jobman.yml
jobman config show
jobman config show --origins
```

`config show` emits JSON so values and their effective types are unambiguous.
`--origins` also reports the source of merged fields. It never resolves a
secret reference.

## Sources and precedence

Configuration is merged from low to high precedence:

1. built-in defaults;
2. the platform system file;
3. the per-user file;
4. a trusted project `.jobman.yml` found from the working directory upward;
5. the file explicitly selected by `--config PATH`;
6. documented `JOBMAN_` environment overrides; and
7. command flags for the submitted job.

Run `jobman config paths` for the concrete system and user paths on the current
platform. On Linux they normally are `/etc/jobman/jobman.yml` and
`${XDG_CONFIG_HOME:-$HOME/.config}/jobman/jobman.yml`.

Maps merge recursively. Scalars and lists replace lower-precedence values.
Every source may include `schema_version: 1`; an omitted source-level version
inherits the effective version.

Project discovery is opt-in. A canonical project root must be listed in
`trusted_project_roots` by the user configuration or an explicit file. System
and project files cannot grant themselves project trust. An explicitly selected
file does not require a trust entry.

## Scalar forms and defaults

- Durations in configuration and CLI flags use Go duration syntax plus `d` as
  exactly 24 hours and `w` as exactly 7 days; calendar months are unsupported.
- Slot and completion limits use a positive decimal integer; retention counts
  also accept zero. The string `unlimited` is accepted where allowed.
- Byte limits use a nonnegative integer number of bytes, an IEC value such as
  `64KiB` or `2GiB`, or `unlimited` where allowed.
- Absolute timestamps use RFC 3339.

The built-in global defaults are:

```yaml
schema_version: 1
concurrency:
  max_active_slots: unlimited
  pools: {}
retention:
  completed_metadata_max_age: unlimited
  completed_log_max_age: 30d
  max_jobs: unlimited
  max_runs_per_job: unlimited
  max_log_bytes_per_job: unlimited
  max_total_log_bytes: unlimited
```

An ordinary job defaults to one slot, one run, one required success, one
failure limit, exit code 0 as success, null standard input, a ten-second
graceful-stop window, both output streams captured without rotation, and the
global completed-log retention policy.

The environment override surface is intentionally small and reversible:

| Variable | YAML field |
| --- | --- |
| `JOBMAN_CONCURRENCY_MAX_ACTIVE_SLOTS` | `concurrency.max_active_slots` |
| `JOBMAN_RETENTION_COMPLETED_METADATA_MAX_AGE` | `retention.completed_metadata_max_age` |
| `JOBMAN_RETENTION_COMPLETED_LOG_MAX_AGE` | `retention.completed_log_max_age` |
| `JOBMAN_RETENTION_MAX_JOBS` | `retention.max_jobs` |
| `JOBMAN_RETENTION_MAX_RUNS_PER_JOB` | `retention.max_runs_per_job` |
| `JOBMAN_RETENTION_MAX_LOG_BYTES_PER_JOB` | `retention.max_log_bytes_per_job` |
| `JOBMAN_RETENTION_MAX_TOTAL_LOG_BYTES` | `retention.max_total_log_bytes` |

`JOBMAN_STATE_DIR` selects the runtime state directory separately from the
YAML merge.

## Durable configuration authority

Store-wide and named-pool capacities are durable scheduler state. Before a
submission, `jobman run` and `jobman rerun` validate their complete effective
configuration and atomically synchronize those capacities; a submission never
starts under stale limits merely because `config apply` was omitted. Use
`jobman config apply` to perform the same synchronization without submitting a
job.
Policy-based `jobman clean` also synchronizes the capacities because it already
uses the effective retention configuration as an administrative authority.

Read-only inspection, lifecycle emergency commands, `doctor`, `clean JOB`,
`clean --all`, and explicit `clean --older-than` do not apply store-wide
settings. They remain usable when the current configuration is malformed.
Removing a named pool through an authority command fails while active
admissions still reference that pool.

## Named objects

The top-level registries are:

- `job_specs`: reusable direct command and policy specifications;
- `wait_conditions`: reusable `until`, `delay`, `file-exists`, or direct
  executable `probe` prerequisites;
- `secrets`: re-resolvable `env:NAME` or `file:/absolute/path` references;
- `concurrency`: the store-wide slot limit and named pool capacities;
- `retention`: completed-log and completed-metadata selection limits;
- `notifiers`: bounded command, HTTP, or SMTP delivery definitions;
- `profiles`: explicit ordered overrides, optionally based on a named job spec;
  and
- `redaction`: extra sensitive field names and bounded RE2 patterns applied to
  Jobman diagnostics and structured output. Captured target logs remain raw;
  when a user explicitly collects a bounded evidence log tail, the configured
  patterns are applied to copied evidence bytes before sealing.

Select a job spec with `jobman run --job-spec NAME`. Repeat `--profile NAME` to
apply profiles in command-line order. A direct command after `--` may replace
the configured command while retaining the selected policy. `jobman run
--rerun JOB` instead clones the prior effective specification exactly; policy
flags are rejected for that source, while `--name` and `--wait` remain
available.

The packaged [sample configuration] contains commented examples for every
registry and policy group.

Retention limits are evaluated when `jobman clean` is invoked; there is no
resident cleanup service. `clean` first prunes selected completed-run logs and
writes durable pruning tombstones. A finite `completed_metadata_max_age` then
removes eligible completed history only after every log is pruned and while no
unresolved dependency, pending notification, or active admission needs it.

```console
jobman clean                         # policy-based dry run
jobman clean --force                 # apply the policy
jobman clean JOB                     # preview one completed job and its logs
jobman clean JOB --force             # remove one eligible completed job
jobman clean --all                   # preview all completed jobs and logs
jobman clean --all --force           # remove all eligible completed history
```

## Secrets and notifiers

Configuration stores references, never resolved secret values:

```yaml
secrets:
  api_token: env:JOBMAN_API_TOKEN
  smtp_password: file:/home/example/.config/jobman/smtp-password
```

File references must be clean absolute paths. On Unix, Jobman requires a
regular, non-symlink file without group or other permissions. Values are
resolved in supervisor memory when a target, probe, or notifier needs them and
the resolved bytes are not copied into the immutable specification.

Command notifier executables must be absolute and receive a versioned JSON
payload on standard input. HTTP defaults to HTTPS, rejects local/private hosts
unless explicitly allowed, and requires sensitive headers to use a named
secret. SMTP supports `starttls` and `implicit` TLS modes and requires a secret
reference when a username is configured. SMTP subjects identify the result,
optional job name, and short job ID; plain-text bodies contain a bounded
non-secret lifecycle summary without commands, environment values, or logs.
Delivery attempts are bounded and recorded for inspection; notification
failure never changes the job outcome.
Subscribed lifecycle events are queued in the same SQLite transaction as their
state snapshot and event record, before external delivery. A later per-job
supervisor can reclaim an expired delivery lease, but there is no resident
notification daemon to wake solely for abandoned work. `jobman doctor --repair`
provides an explicit configuration-independent recovery wake-up.

Notification attempts and their retry waits consume the remaining whole-job
timeout budget. If the next retry would begin at or after that deadline, Jobman
records the delivery as terminally failed instead of sleeping beyond the
budget. This still does not change the already determined job outcome.

Accepted notification subscription names are:

```text
job_started, run_started, run_succeeded, run_failed, run_timed_out,
run_cancelled, run_lost, retry_scheduled, job_succeeded, job_failed,
job_timed_out, job_cancelled, job_aborted, job_lost,
job_submission_failed
```

Schema migration 6 does not backfill notification deliveries for historical
events. Due or expired work is recovered opportunistically when another
per-job supervisor finishes or explicitly with `jobman doctor --repair`.

## Safety notes

- A project configuration can select commands, probes, environment changes,
  secret references, and notifier callbacks. Trust only roots you control.
- Target commands run with the invoking user's privileges; configuration is
  not a sandbox.
- Use `jobman config validate PATH` before deploying a shared configuration.
- Keep configuration and secret files user-private even though references,
  rather than resolved values, are persisted.
- Review `CHANGELOG.md` and [the upgrade guide](UPGRADING.md) before upgrading;
  do not edit the SQLite state database by hand.

[sample configuration]: ../etc/jobman/jobman.yml
