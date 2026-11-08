# Deployment configuration

Organization values belong in a separate deployment repository. Jobman supplies
neutral defaults, native schemas, validators, and rendering hooks. It never
loads a deployment Git checkout, interpolates shell expressions, fetches remote
configuration, or resolves credentials during validation.

## Native validation and inventory

```console
jobman config schema
jobman config validate --isolated /absolute/path/jobman.yml
jobman config show --origins
jobman-agent config schema
jobman-agent config validate /absolute/path/agent.yml
jobman-agent config show /absolute/path/agent.yml
```

Schema commands emit JSON Schema draft 2020-12 describing native YAML shapes.
Native validation is authoritative for cross-field and semantic constraints;
schema validation alone is insufficient. The isolated core validator reads only
the selected file over built-in defaults, without discovering system, user,
project, or environment settings. It does not open state, credentials, artifact
roots, or network connections. Agent validation likewise reads only the supplied
file. `show` includes configuration references and field origins, never resolved
credentials; configuration may still contain sensitive deployment metadata.

The existing core precedence is unchanged: built-in, system, user, trusted
project, explicit file, environment, then invocation flags. Maps merge recursively;
other values replace. System, project, and environment files cannot establish
project trust, and project files cannot select Control profiles. These are
operator defaults, not an authorization or enforcement boundary. Control and
agents must independently enforce deployment policy.

The complete core field inventory is emitted from the native types by
`jobman config schema`. Existing families are `trusted_project_roots`, `job_specs`,
`wait_conditions`, `secrets`, `concurrency`, `retention`, `notifiers`, `profiles`,
`shared`, `redaction`, and `presentation`. Refer to the configuration guide and
packaged `etc/jobman/jobman.yml` for examples. One run, exit code zero success,
null stdin, 10-second stop grace, no notifications, and 30-day completed-log
cleanup eligibility remain defaults. Global concurrency, metadata retention, and
byte limits remain unlimited until explicitly configured. Deployment profiles
should size those limits deliberately.

## Agent schema 1

Operator-facing `enroll`, `run`, `install-service`, `status`, and `bootstrap`
accept `--config PATH`, falling back to `JOBMAN_AGENT_CONFIG` only when the flag
is absent. The environment path must be clean and absolute. A selected missing
or malformed file fails closed. There is no implicit current-directory search.
Explicit command flags override file values, including explicit false booleans.
Internal execution and array runners use their immutable generated arguments
and never consult the environment configuration path.

The file is a flat YAML mapping with required `schema_version: 1`. Unknown or
duplicate fields, aliases, anchors, multiple documents, wrong scalar types,
symlinks, and Unix group/world-writable files are rejected. File size is bounded
at 1 MiB. No inline credential field exists. `token_file` is a reference; generated
agent certificates, session tokens, journals, and execution state stay outside
configuration repositories. Validation never opens the referenced token or CA.

```yaml
schema_version: 1
state_dir: /var/lib/example-agent
poll_interval: 2s
artifact_store: shared
artifact_store_version: 1
artifact_root: /srv/jobman/artifacts
max_log_bytes: 67108864
max_artifact_bytes: 1073741824
container_engine: podman
container_host_network: false
log_level: info
```

The same file can supply multiple operator commands. Each command consumes its
applicable settings; enrollment/bootstrap-only fields are not forwarded into
runner processes. The schema command lists every field and default:

| Family | Fields | Defaults |
| --- | --- | --- |
| Runtime | `state_dir`, `poll_interval`, `log_level` | empty, `2s`, `info` |
| Artifacts | `artifact_store`, `artifact_store_version`, `artifact_root` | empty, `1`, empty |
| S3 | `artifact_s3_bucket`, `artifact_s3_prefix`, `artifact_s3_region`, `artifact_s3_expected_owner`, `aws_cli` | empty except `aws` executable |
| Capture | `max_log_bytes`, `max_artifact_bytes` | 64 MiB per stream, 1 GiB per execution |
| Containers | `container_engine`, `container_runtime`, `container_host_network` | empty, empty, false |
| Slurm | `slurm`, `slurm_root`, `slurm_runner` | false, empty, empty |
| Enrollment | `server`, `target_generation`, `token_file`, `server_ca` | empty except `token_file: '-'` for stdin |
| Installation | `agent_binary`, `unit_dir` | empty; existing executable/platform discovery |
| Bootstrap | `host`, `ssh`, `remote_binary`, `remote_state_dir`, `expected_version` | empty, `ssh`, `.local/bin/jobman-agent`, `.local/state/jobman-agent`, empty |

Polling remains bounded to 250 ms through 1 minute; byte limits and store versions
must be positive. Slurm root/runner must appear together. An artifact store needs
exactly one filesystem root or S3 bucket. Existing runtime checks and probes still
apply when a command executes. `log_level` accepts debug, info, warn, or error for
`run` and `install-service`; bootstrap keeps its existing info-level service.

Installing with a selected configuration writes its absolute `--config` path and
only explicit runtime flag overrides into the systemd unit. File values therefore
remain effective on restart; they are not flattened into stale flag defaults.
Changes require a service restart; automatic reload is not implemented. Provision
the referenced file at a durable location readable by the service user. The unit
continues to use existing security restrictions and contains no enrollment token.

## Presentation and messages

Core accepts optional, empty-by-default presentation settings:

```yaml
schema_version: 1
presentation:
  organization: Example Organization
  display_name: Example Jobman
  documentation_url: https://docs.example.com/jobman/
  support_url: https://support.example.com/jobman/
  motd: |
    Planned maintenance information appears here.
```

Labels are bounded single-line plain text, links require HTTPS without embedded
credentials, and messages are bounded plain text without terminal control
sequences or bidirectional formatting characters. `jobman about` displays these
values explicitly; `jobman about --json` emits JSON only. Bare `jobman` includes
them before help only when stdout is an interactive terminal. Piped output,
ordinary job commands, emergency commands, and machine JSON receive no banners.
Malformed configuration never prevents bare help. A message is informational;
mandatory access consent belongs in the authentication or managed login layer.

Executable names, wire identifiers, public provenance, signatures, authorization
checks, redaction safeguards, and protocol constants remain product behavior.
Native configuration has no organization-specific endpoint, identity, branding,
or policy. A deployment renderer should pin public versions, emit native files,
run isolated validators, and install only after reviewing its rendered diff.
