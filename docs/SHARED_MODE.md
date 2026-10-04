<!-- cspell:ignore OIDC PostgreSQL workdir -->

# Shared-mode preview

Status: pre-release distributed execution lifecycle

Jobman's `shared` command group is the client for Jobman Control. It gives
authorized users a consistent PostgreSQL-backed job view from different
workstations without putting SQLite on NFS or distributing database
credentials. Shared jobs and standalone jobs are deliberately separate state
universes: commands such as `jobman list` continue to use local SQLite, while
`jobman shared list` reads Jobman Control.

This preview supports direct native or container commands on a named target
registered with the `subprocess` or `slurm` execution backend. It publishes
bounded logs and stages declared regular-file inputs and outputs through a
target-approved local/NFS filesystem or S3 store. AWS ParallelCluster is
registered as a Slurm provider and uses immutable target-generation rollover;
Jobman does not provision the cluster or call its management API to schedule
work. Portable collections create independent child jobs and compatible Slurm
collections compile to one native array. A Linux agent can be installed as a
hardened systemd user service directly or through a one-time OpenSSH bootstrap.
Immutable dependency graphs may coordinate jobs across targets, and an
explicit importer can copy quiescent completed standalone metadata into shared
history. Standalone Slurm CLI control, directory/archive artifacts, Jobman
retries, and live-history migration are not implemented.

## Configure a profile

Add a profile to the per-user configuration file shown by `jobman config
paths`. A shared profile selects exactly one HTTPS endpoint and namespace:

```yaml
shared:
  current_profile: department
  profiles:
    department:
      endpoint: https://jobman-control.example.edu
      namespace: research
      token_file: /home/example/.config/jobman/control-token
      ca_file: /home/example/.config/jobman/control-ca.pem
      artifact_roots:
        department-nfs:
          version: 1
          path: /nfs/jobman-artifacts
```

`token_file` contains an externally obtained OIDC bearer token. Jobman rereads
it for every request so a credential helper can replace it between commands;
interactive login and token refresh are not part of this slice. On Unix, the
token must be a regular non-symlink file inaccessible to group and other
users. `ca_file` is optional when the service certificate chains to a system
trust root.

`artifact_roots` maps a logical store name and immutable mapping version from
Control to this host's physical mount. The path may differ between Linux and
Windows profiles; for example, the same logical store could map to
`/nfs/jobman-artifacts` on Linux and `N:\jobman-artifacts` on Windows. These
paths are local deployment configuration and never enter a portable workload
or Control manifest. The root must already exist and must not be a symlink.
Its filesystem must support atomic same-directory hard links; Jobman stages and
synchronizes each chunk before linking it to the immutable published key. The
department NFS store is the intended shared deployment for this slice.

Endpoints must use HTTPS. Loopback HTTP is accepted only for the control
service's explicitly enabled development mode, and Jobman refuses to send a
token over HTTP. Credential paths must be clean absolute paths. Trusted project
configuration cannot select or replace a shared profile, which prevents a
checked-out project from redirecting a bearer token to another endpoint.

Select another configured profile explicitly when needed:

```console
jobman shared --profile aws target list
```

## Submit and inspect a job

List namespace-visible targets before submitting:

```console
jobman shared target list
jobman shared target show workstation-a
```

Submit a direct executable and argument vector. Jobman does not invoke a shell
or inherit the client environment:

```console
jobman shared run \
  --name analysis \
  --target workstation-a \
  --env MODE=batch \
  --timeout 20m \
  -- analyze --input sample.dat
```

The same portable command can target an enrolled on-premises Slurm cluster.
Portable resource flags are supported only by this Slurm slice:

```console
jobman shared run \
  --name gpu-analysis \
  --target onprem-slurm \
  --partition gpu \
  --cpu 8 \
  --memory 32GiB \
  --gpu 1 \
  --nodes 1 \
  --tasks 1 \
  --wall-time 2h \
  -- analyze --input sample.dat
```

The target policy resolves a default partition when `--partition` is omitted.
Jobman reports the Slurm job ID and normalized scheduler state, cluster, and
reason in `shared show` and JSON status output. Queueing does not become the
portable `running` phase until Slurm reports `RUNNING`.

When a job leaves the live queue, the agent checks accounting even if `squeue`
rejects the old ID. Accounting must identify the exact job or native array task;
an allocation's numeric `JobIDRaw` is not interchangeable with `ARRAY_JOB_ID_TASK_INDEX`.
The runner's durable workload result remains authoritative when the batch
wrapper itself exits successfully.

A cancellation acknowledgement records the request, not proof of terminal
execution. Some queued array tasks leave no individual accounting allocation.
Without a positive terminal scheduler or runner record, the agent preserves the
last observation and reports reconciliation failure. It does not infer a
cancelled outcome from absence. Other executions can still publish their
verified outcomes and logs, and the agent continues polling desired actions.

Use a target-approved container runtime by supplying portable image and policy
intent. Subprocess targets support Docker or Podman; Slurm targets support
Apptainer with an administrator-staged absolute `.sif` path and `never` pull
policy:

```console
jobman shared run \
  --name container-analysis \
  --target workstation-a \
  --container-image registry.example.edu/research/analyze@sha256:0123456789abcdef \
  --container-pull-policy if-not-present \
  --container-network restricted \
  -- analyze --input sample.dat
```

Containers run without a shell, preserve command argument boundaries, use a
read-only root filesystem, drop Linux capabilities, prohibit privilege
escalation, and bind only the private workspace. Host networking requires both
portable `host` intent and explicit target-side enablement. A container remains
subject to its host architecture, kernel, driver, filesystem, and scheduler
constraints.

The default logical working directory is `workspace:/`. In this slice it maps
to the private workspace created by the selected agent. The command may set a
different logical path below that root with `--working-directory`; client-local
absolute paths are not portable.

Use the returned shared job ID with the lifecycle commands:

```console
jobman shared list
jobman shared list --phase running --limit 50
jobman shared status JOB_ID
jobman shared show JOB_ID
jobman shared wait JOB_ID
jobman shared cancel JOB_ID
jobman shared logs JOB_ID
jobman shared logs --follow JOB_ID
jobman shared logs --stream stderr --lines 100 JOB_ID
jobman shared artifacts JOB_ID
```

`shared logs` obtains an authorized logical manifest from Control, reads the
immutable chunks directly through the selected profile's filesystem mapping,
and verifies every length and SHA-256 checksum before emitting bytes. `--json`
returns the manifest without reading object bytes. Stdout and stderr preserve
their exact individual byte order; combined display uses chunk capture times
and cannot reconstruct ordering within writes that the operating system
delivered concurrently.

### Stage regular-file artifacts

Portable workloads reference logical object URIs and sandbox paths; they never
contain a client or target mount path. The input object must already exist in
the configured immutable store. Declare inputs and outputs when submitting:

```console
jobman shared run \
  --name artifact-analysis \
  --target workstation-a \
  --input source=artifact://department-nfs/research/inputs/sample.dat=inputs:/sample.dat=sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --output result=outputs:/result.dat=artifact://department-nfs/research/results/result.dat=required \
  -- analyze inputs/sample.dat outputs/result.dat
```

The agent copies and verifies the input into the private workspace before
launch, rejects symlinks and special files, and publishes each output
immutably only after the process exits and its logs close. A missing required
output converts an otherwise successful execution to
`artifact_publish_failed`; a missing optional output is omitted. Publication
of an existing object is accepted only when its content is identical.

`jobman shared artifacts JOB_ID` returns authorized logical URIs, sizes, and
SHA-256 checksums. `--json` emits the versioned manifest. Control stores and
authorizes metadata but never proxies object bytes or returns physical mount
paths. The agent can resolve the logical store to local/NFS storage or to a
private S3 bucket and administrator prefix. S3 uses the standard AWS credential
chain, expected-owner checks, checksum-enabled transfers, conditional immutable
writes, and exact-key replay validation; credentials never enter the workload.
Use an instance role or other short-lived/scoped identity and a private S3
endpoint where site policy requires it. This slice supports one logical store
per workload and regular files only. The default aggregate staging/publication
limit is 1 GiB per execution.

Filesystem mappings let `shared logs` read bytes directly. For an S3 log store,
the current client can return the authorized JSON manifest but does not yet
download object bytes; an external authorized S3 client is required. `shared
artifacts` returns metadata for either store and likewise does not download
declared outputs.

## Submit a collection

`jobman shared collection submit FILE` accepts a canonical
`CollectionRequest`. Each item contains an explicit sealed workload and
placement; Jobman does not perform shell interpolation or an implicit parameter
matrix. The collection is accepted transactionally, has a bounded
`maxActive`, and uses either `continue` or `fail-fast` sibling policy:

```console
jobman shared collection submit sweep.json
jobman shared collection show COLLECTION_ID
jobman shared collection show --json COLLECTION_ID
```

`arrayPolicy` may be `never`, `prefer`, or `require`. Compatible collections
with at least two children, one Slurm target generation and partition, and
identical portable resources compile to a native Slurm array. `require` rejects
the whole request when those conditions are not met; `prefer` falls back to
ordinary child dispatch. The array concurrency limit uses `maxActive`.

Every array task remains an addressable Jobman job with its own effective
execution, native `ARRAY_JOB_ID_TASK_INDEX`, logs, artifacts, cancellation, and
terminal outcome. Fail-fast records durable cancellation for eligible siblings
without erasing outcomes that race to completion. Array task ordering is an
identity mapping, not an execution-order guarantee.

## Submit a dependency graph

`jobman shared graph submit FILE` accepts a canonical `GraphRequest`. Every
node contains one sealed workload and explicit placement, so a graph can gate
work across a workstation, an on-premises Slurm partition, and a
ParallelCluster target without delegating orchestration to any one scheduler:

```console
jobman shared graph submit pipeline.json
jobman shared graph show GRAPH_ID
jobman shared graph show --json GRAPH_ID
jobman shared graph cancel GRAPH_ID
```

Graph documents are immutable, contain between one and 10,000 nodes and at
most 100,000 edges, reject cycles and duplicate references, and use
`maxActive` to bound concurrent nodes. Each edge selects `success`, `failure`,
`any-terminal`, or an explicit `outcomes` list. `failure` includes failure,
timeout, aborted, and lost outcomes; it does not include cancellation.

When every upstream node is terminal but an edge predicate is unsatisfied,
the graph's `unsatisfiedPolicy` marks the downstream node `skipped`,
`cancelled`, or `blocked`. Those are durable terminal dispositions, not jobs
left indefinitely queued. Each node remains an ordinary addressable Jobman job
with its own logs, artifacts, target evidence, cancellation races, and outcome.
Control evaluates readiness transactionally; Slurm native dependencies are
not authoritative and are not used by this slice.

## Import completed standalone history

`jobman shared history import FILE` validates and imports one quiescent
terminal standalone record. Always validate a generated document first:

```console
jobman shared history import --dry-run completed-job.json
jobman shared history import completed-job.json
```

The `CompletedHistoryImport` document supplies namespace/name/labels, one
sealed portable workload and placement, the terminal outcome and completion
time, and SQLite source schema/job ID provenance. A successful import creates
a new shared job ID in terminal state and no run, execution, or assignment.
Replaying the same request is idempotent; trying to reuse the same source
provenance for different intent conflicts.

This is metadata migration only. Jobman never imports active jobs, retries,
process identities, log or artifact bytes, or SQLite primary keys. Copy and
verify retained bytes separately before referring users to the imported
history.

When a list response includes a next-page token, pass it unchanged to
`--page-token`. Add `--json` to lifecycle and target commands for versioned
machine-readable output. `wait` returns success only for a terminal `success`
outcome. Validation errors use status 2, missing resources status 3,
idempotency conflicts status 5, and other control or transport failures status
1.

## Recover an uncertain submission

Before sending a submission, the client writes the canonical request and its
idempotency key to a user-private local operation journal. If the response is
lost, the error identifies an operation ID. Retry the exact request safely with:

```console
jobman shared run --resume OPERATION_ID
```

The same idempotency key is reused, so Jobman Control either returns the
already-created job or creates it once. Completed operations also resolve and
return their recorded job. Inspect the local recovery records with:

```console
jobman shared operations
```

These operation files are client-side recovery aids, not authoritative job
state and not a synchronization mechanism. They live under the selected local
Jobman state directory and can contain command and environment values. Keep
that directory private. PostgreSQL behind Jobman Control remains the only
authoritative shared store.

## Current boundary

- `shared run` submits one direct-command native or container workload;
  collection and graph submissions accept up to 10,000 explicit child jobs or
  nodes, subject to the namespace's lower configured quota.
- Placement must name an already registered agent target using `subprocess` or
  `slurm`. Slurm requires a Linux submit-host agent, reliable accounting, and a
  private NFS execution-bundle root shared with compute nodes.
- Only explicit non-secret environment values are accepted by the client.
- Declared artifacts use exactly one target-approved local/NFS filesystem or
  S3 store. Inputs must be pre-populated by an external copy or site workflow;
  this slice does not yet provide a client upload/download command.
- Remote logs require the target generation, agent service, and client profile
  to agree on a logical store name and version. A local store is useful only to
  clients that can reach the same bytes; NFS supplies the cross-host behavior.
- Filesystem permissions remain an independent authorization boundary. This
  slice creates owner-private directories and objects, which supports the same
  OS identity across NFS-connected hosts. Cross-user namespace log access
  requires a future administered group/ACL mapping or download broker.
- Capture is bounded per stream (64 MiB by default). The manifest and CLI warn
  explicitly when later bytes were discarded.
- Jobman retries a mutating HTTP request at most once and only when the
  creation or cancellation request carries a stable idempotency key. A local
  operation remains pending after an uncertain single-job result until
  explicitly resumed.
- A Slurm submission with an unprovable native identity remains
  `scheduler.uncertain`; the agent reconciles its unique name through
  accounting and never automatically submits a duplicate allocation.
- Slurm resource intent maps to fixed scheduler arguments. Native arrays are
  selected only by the collection compiler. This slice does not accept
  arbitrary native flags, account/QOS or execution-user overrides, temporary
  storage, native dependencies, or requeue as a Jobman retry.
- ParallelCluster provider metadata and generation rollover are implemented,
  but cluster creation, IAM provisioning, private-network validation, elastic
  node/cost acceptance, and version compatibility remain deployment work.
- The control API, client JSON, and portable contracts are `v1alpha1` and may
  change before the first supported shared release.
- Control applies per-namespace queued/active/group limits and rotates
  namespace dispatch fairly before preserving FIFO order within a namespace.
  It does not replace Slurm priority, fair-share, or partition policy.
- `observationConfidence` distinguishes fresh agent evidence from `stale`,
  scheduler `uncertain`, or `lost` evidence. Silence never proves that an
  accepted process or scheduler job stopped, so Control does not automatically
  reassign it.

The complete target architecture and remaining delivery phases are described
in the [shared and distributed execution design](design/DISTRIBUTED_MODE.md).
