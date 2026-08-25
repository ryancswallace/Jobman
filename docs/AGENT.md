<!-- cspell:ignore mTLS OIDC PostgreSQL workdir -->

# Jobman agent preview

`jobman-agent` is the pre-release data plane for the first shared-mode vertical
slices. It works with the separately deployed Jobman Control service. It does
not change the stable, daemonless `jobman` CLI or its standalone SQLite store.

## Implemented boundary

The agent can execute portable workloads through either a named-host
subprocess or a Slurm target, including an AWS ParallelCluster submit host.
Both paths require:

- placement selects the enrolled target generation;
- the runtime is `native` or a target-approved `container` adapter;
- the command is a direct executable plus an argument vector, not a shell;
- the working directory is under the logical `workspace:/` root;
- the workload declares no extensions, environment profile, secrets, or
  agent-side retries; declared artifacts, when present, use one approved
  local/NFS filesystem or S3 store and regular files only; and
- Jobman Control returns a launch authorization after durable acceptance.

The subprocess path does not accept resource controls. The Slurm path accepts
portable CPU, memory, GPU, node, task, and wall-time intent, plus an approved
partition. It translates those values to fixed `sbatch` arguments; arbitrary
Slurm flags, account/QOS overrides, reservations, constraints, and temporary
storage are not accepted.

The runner preserves argument boundaries, creates a private workspace and log
files, supplies a small allowlist of baseline environment variables plus the
workload's explicit values, manages the target process tree, and records a
stable terminal result for success, failure, timeout, cancellation, or runner
interruption.

For Slurm, the per-user agent runs on a Linux submit host as the Unix identity
that owns the NFS files and scheduler job. It writes a private immutable
execution bundle to NFS, calls `sbatch --parsable`, observes `squeue` and
`sacct`, and cancels with `scancel`. The same runner executes inside the
allocation and records its result and bounded logs in the bundle. Scheduler
state, cluster, reason, and native job ID are reported separately from the
portable Jobman lifecycle.

The pre-release [`jobman shared` client](SHARED_MODE.md) can submit, inspect,
wait for, cancel, and read or follow logs for this bounded workload path.
Filesystem-backed local/NFS and target-side S3 stores are implemented for logs
and declared regular-file artifacts. Docker/Podman subprocess containers and
Apptainer Slurm containers are supported under fail-closed target policy. SSH
bootstrap and hardened Linux systemd user-service installation are
implemented. Collections are independently accepted child executions;
compatible Slurm collections compile to one array allocation with per-task
identity and outcomes. Standalone Slurm CLI control, AWS provisioning and
environment acceptance, directories or archives, retries, and dependency
graphs are not implemented in this slice.

## Trust and durability model

The agent is an unprivileged, per-user service and executes work as its own
operating-system identity. Enrollment creates the private key locally and
sends only a certificate signing request. Steady-state execution APIs require
a short-lived mTLS client certificate; Jobman Control rechecks its agent,
target generation, key digest, expiry, revocation, and target state in
PostgreSQL.

The agent renews its opaque compatibility session and mTLS certificate before
expiry and sends an immutable capability/liveness observation at startup and
periodically thereafter. Control intersects that report with target policy;
an expired session, stale report, inactive target, or incompatible capability
prevents new assignments. Draining stops new work while preserving control of
already accepted executions. Agent silence marks their observations stale but
does not prove termination or permit automatic reassignment.

An assignment offer is inert. The agent first journals the exact offer in its
host-local SQLite spool, requests durable acceptance, and journals the returned
launch authorization before it starts a runner. A runner uses an exclusive
launch claim so a replay cannot deliberately launch a second process. Start and
completion observations remain in an outbox until Jobman Control receipts them.
Cancellation is likewise journaled before the local marker is written and
before acknowledgement.

While a process runs, the service copies newly captured stdout and stderr into
immutable, checksummed 256 KiB objects. It places each object before recording
its logical metadata and retains that metadata in the SQLite outbox until
Control receipts it. A restart resumes from the last journaled byte offset;
repeated object placement and metadata commits are content-checked and
idempotent. Terminal process state is recorded independently from log
completeness, although the agent publishes final stream manifests before
sending the terminal process event.

The control protocol is at least once; it does not promise exactly-once
external execution. If an agent restarts with a launch claim but no trustworthy
start or completion manifest, it eventually records the execution as `lost`
instead of guessing that a duplicate launch is safe.

Before `sbatch`, the Slurm agent durably records a unique Jobman job name and
attempt time. If submission returns ambiguously or the agent stops after Slurm
accepted the job but before its job ID was journaled, the agent searches
accounting for that exact identity. It never submits a second allocation while
the first outcome is uncertain. Zero or multiple matches remain visibly
`uncertain` for operator repair.

For a native array, the agent waits until every independently authorized child
binding is present, validates task indices, count, partition, concurrency, and
portable resources, then writes one immutable index manifest before invoking
`sbatch --array`. A crash after a possibly successful submission follows the
same name/accounting recovery boundary as a single job. Each child is observed
and cancelled through its `ARRAY_JOB_ID_TASK_INDEX`, and ordinary per-execution
runner, log, artifact, and event paths remain unchanged.

## Enroll and run

An authorized Jobman Control user first registers the host target and issues a
short-lived, single-use enrollment token pinned to the target generation and
expected operating-system user. Pass the token on standard input so it is not
exposed in the process argument list:

```sh
printf '%s\n' "$ENROLLMENT_TOKEN" | jobman-agent enroll \
  --state-dir /var/lib/jobman-agent-user \
  --server https://jobman-control.example.edu \
  --target-generation 55555555-5555-4555-8555-555555555555 \
  --server-ca /etc/jobman/control-ca.pem
```

For a Slurm target, run enrollment on its Linux submit host and add `--slurm`.
Enrollment probes `sbatch`, `squeue`, `sacct`, and `scancel` before advertising
the `slurm` backend and accounting capabilities:

```sh
printf '%s\n' "$ENROLLMENT_TOKEN" | jobman-agent enroll \
  --slurm \
  --state-dir /var/lib/jobman-agent-user \
  --server https://jobman-control.example.edu \
  --target-generation 66666666-6666-4666-8666-666666666666 \
  --server-ca /etc/jobman/control-ca.pem
```

The `--server-ca` option is unnecessary when the control certificate already
chains to a system trust root. After enrollment, run the long-lived polling
service under an ordinary user service manager:

```sh
jobman-agent run \
  --state-dir /var/lib/jobman-agent-user \
  --poll-interval 2s \
  --artifact-store department-nfs \
  --artifact-store-version 1 \
  --artifact-root /nfs/jobman-artifacts \
  --max-log-bytes 67108864 \
  --max-artifact-bytes 1073741824
```

For a private S3 mapping, omit `--artifact-root` and configure the physical
bucket only on the agent:

```sh
jobman-agent run \
  --state-dir /var/lib/jobman-agent-user \
  --artifact-store research-results \
  --artifact-store-version 4 \
  --artifact-s3-bucket department-jobman \
  --artifact-s3-prefix namespaces/research \
  --artifact-s3-region us-east-1 \
  --artifact-s3-expected-owner 123456789012
```

The adapter invokes AWS CLI v2 without a shell and uses its standard credential
chain. Prefer an instance role, short-lived STS credentials, or another
site-managed scoped provider; workloads and durable manifests never contain
credentials. Transfers enable S3 checksum validation, bound local
materialization, reject unexpected owners, and publish with an immutable
conditional write. An existing key is a replay only when its size and SHA-256
checksum match.

Enable a subprocess container adapter with `--container-engine docker` or
`podman`. Enable a Slurm container adapter with `--container-engine apptainer`;
the portable image must be an administrator-staged absolute `.sif` file and
use pull policy `never`. `--container-host-network` is an explicit target-side
policy opt-in and still requires matching workload intent.

The Slurm service additionally requires a private NFS bundle root and an
absolute `jobman-agent` binary path visible at the same path on compute nodes:

```sh
jobman-agent run \
  --state-dir /var/lib/jobman-agent-user \
  --artifact-store department-nfs \
  --artifact-store-version 1 \
  --artifact-root /nfs/jobman-artifacts \
  --slurm-root /nfs/home/researcher/.jobman/slurm \
  --slurm-runner /nfs/apps/jobman/jobman-agent
```

The same Slurm configuration applies on a stable ParallelCluster head or login
node. Register that deployment as an `aws-parallelcluster` Slurm target in
Control and create a new immutable generation when the cluster is recreated.
The agent uses Slurm and artifact APIs only; it does not create or resize the
cluster.

The logical name and version must match the target generation's `logStore`
policy and any selected `artifactStores` mapping in Jobman Control. The root
must be an existing clean absolute path and
may be either local storage or an NFS mount. Local storage does not become
remotely readable merely because its metadata is in Control. The filesystem
must support atomic same-directory hard links so the agent can publish a fully
synchronized chunk without exposing a partial object at its final key.

The state directory must be absolute, private, and host-local. Linux rejects
NFS, macOS requires a filesystem marked local, and Windows rejects remote or
unverifiable drives. This SQLite spool is a target-side recovery journal, not
the shared source of truth; PostgreSQL in Jobman Control remains authoritative
for shared state.

`--slurm-root` is deliberately different: it must be an absolute, normalized,
owner-private NFS directory reachable by the submit host and compute nodes.
The configured filesystem must provide reliable create-exclusively,
write/sync/rename, and directory-sync semantics. The runner path must be an
owner-executable regular file. Reliable Slurm accounting is required for
terminal and ambiguity reconciliation.

## Install or bootstrap the service

On a Linux host with systemd user services, install the enrolled agent without
granting system privileges:

```console
jobman-agent install-service --start \
  --state-dir /var/lib/jobman-agent-user \
  --artifact-store department-nfs \
  --artifact-store-version 1 \
  --artifact-root /nfs/jobman-artifacts
jobman-agent status --json --state-dir /var/lib/jobman-agent-user
```

The generated unit uses the exact binary and arguments, mode 0600, a private
umask, restart-on-failure, and systemd process/filesystem hardening. The user
manager must be configured to remain available when that is required by site
policy; Jobman does not enable lingering or modify login policy.

From a workstation, SSH can perform the initial Linux installation,
enrollment, service start, and local identity/version check:

```console
printf '%s\n' "$ENROLLMENT_TOKEN" | jobman-agent bootstrap \
  --host researcher@worker-a \
  --agent-binary ./jobman-agent-linux-amd64 \
  --server https://jobman-control.example.edu \
  --target-generation 55555555-5555-4555-8555-555555555555 \
  --artifact-store department-nfs \
  --artifact-store-version 1 \
  --artifact-root /nfs/jobman-artifacts
```

Add `--slurm`, `--slurm-root`, and `--slurm-runner` for a Slurm submit-host
target. Bootstrap uses the installed OpenSSH client and therefore preserves
the user's host aliases, jump hosts, authentication, and host-key policy. It
accepts only a matching Linux Go binary, verifies the transfer checksum,
passes the enrollment token through remote standard input, and is idempotent
only for the same existing enrollment. SSH is not used for assignment,
execution, logs, events, cancellation, or other steady-state control.

## Local files

The exact layout is private implementation state, but operators should expect:

```text
state-directory/
  credentials.json
  agent.db
  agent.db-wal
  agent.db-shm
  executions/<execution-id>/
    assignment.json
    authorization.json
    launch.claim
    started.json
    completed.json
    stdout.log
    stderr.log
    runner.log
    cancel.requested
    workspace/
  arrays/<collection-id>/
    slurm-array-attempt.json
    slurm-array-submission.json

slurm-root/
  executions/<execution-id>/
    assignment.json
    authorization.json
    jobman-slurm.sh
    started.json
    completed.json
    stdout.log
    stderr.log
    slurm.stdout.log
    slurm.stderr.log
    cancel.requested
    workspace/
  arrays/<collection-id>/
    array-manifest.json
    jobman-slurm.sh
    slurm-<array-job>_<task>.stdout.log
    slurm-<array-job>_<task>.stderr.log
```

Credential updates and enrollment recovery records use synchronized atomic
replacement. Files that can contain credentials, commands, environment data,
or logs use user-private permissions. The configured artifact root additionally
contains owner-private logical namespace/job/execution paths with immutable
stdout/stderr chunks and declared artifact keys. Input staging verifies an
optional checksum and rejects symlinks or special files. Output publication is
content-checked and immutable; metadata is sent with the terminal event and
exposed by `jobman shared artifacts`. Control stores checksums and manifests,
not bytes. Automated artifact retention is not part of this slice.
