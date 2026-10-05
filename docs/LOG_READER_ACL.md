# Designated reader for filesystem log stores

Filesystem stores are private by default. An operator may opt one existing
Linux local/NFS store root into a designated numeric reader policy so a
Dashboard log broker can read immutable execution log chunks. This does not
change private runner capture files, agent state, standalone CLI logs, S3
objects, or artifact payloads.

## Provision and enable

Provision the storage server and producer identity before installing the policy.
The root and every existing shared prefix must have this exact access ACL and
default ACL (substitute the broker UID):

```text
user::rwx
user:21901:r-x
group::---
mask::r-x
other::---
default:user::rwx
default:user:21901:r-x
default:group::---
default:mask::r-x
default:other::---
```

Keep owning-group and other data access absent. Parent directories outside the
store root need operator-provisioned traversal; Jobman neither verifies nor
changes those ancestors. Preserve NFS `root_squash`. The operator owns UID
assignment and consistent mapping across the storage server, producer, and
broker. Never assign the broker the producer's owner UID.

Inside the root, create `.jobman-log-reader.json` as a new regular file with
mode `0600`, owned by the producer (the store owner), inheriting the default ACL:

```json
{"schema_version":1,"store_name":"lab-nfs","store_version":1,"reader_uid":21901}
```

The logical name and version must match `NewFilesystemStore`'s configured
mapping. The file is limited to 1 KiB; duplicate, missing, unknown, or trailing
JSON fields are rejected. The producer verifies it through a no-follow file
descriptor at initialization and before each write. Changes require a restart;
removal during an active policy-enabled process makes writes fail closed.
Restarting with no policy returns to private defaults.

Read-only clients use `NewFilesystemReader`, which validates the mapping and
verifies reads without loading the producer-only policy. The existing
`jobman shared logs` client uses this path. OS permissions still enforce
reader access, and the reader API exposes no publication operations.

Existing directories with masked reader access are rejected. Provision only
the canonical shared prefixes and existing log chunks before enabling the
policy; do not recursively grant the broker access to artifact payloads or
other files. The producer never installs ACL grants or changes existing modes.
Disabling the policy does not revoke previously granted access; the operator
must remove that reader's grants separately if revocation is intended.

## Shared path boundary

Only explicit log publication (`PutLog` / `PutLogImmutable`) at this exact
immutable object key receives mode `0640`. Ordinary `Put`, `PutImmutable`,
`Publish`, and `PutFileImmutable` remain private even for a log-shaped key:

```text
namespaces/<namespace>/jobs/<job-uuid>/executions/<execution-uuid>/logs/<stream>/<sequence>.chunk
```

The namespace is the existing lowercase protocol name (1–128 characters,
letters/digits at each end, periods, underscores, and hyphens inside). UUIDs
must be lowercase canonical UUID versions 1–5 with the RFC variant, matching the
shared execution protocol (Control currently generates version 4). Standalone
Jobman UUIDv7 identifiers are a separate contract. Stream is `stdout` or `stderr`.
Sequence is a positive signed 64-bit decimal integer padded to at least eight
digits, without additional leading zeros. A shared chunk is at most 256 KiB.

The root and canonical directories through the stream are created/validated
as `0750`. Those shared namespace/job/execution prefixes can be created by an
artifact write before a log arrives. Artifact-specific directories remain
`0700`, and all other payload files remain `0600`. New private nodes retain the
inherited named ACL entry under a zero mask, so that entry confers no access.
The group-class mode bits represent the ACL mask, not an owning-group grant.
See [Linux POSIX ACL inheritance and masks](https://man7.org/linux/man-pages/man5/acl.5.html).

Control must separately bind manifests to the authorized deployment, namespace,
execution, and store mapping. A store ACL grants the broker physical access;
it does not authorize a Dashboard end user or replace manifest-prefix checks.
The broker must not expose arbitrary paths or directory listings to clients.

## Supported ACL representations and failure behavior

Linux local POSIX ACLs must contain exactly owner, configured named reader,
owning group, mask, and other entries in the kernel xattr format. The raw named
reader permission is `r-x`; regular log files restrict effective permission to
`r--` with their mask. All directories require the exact default ACL above.
The parser accepts no additional users or groups, even if currently masked.

For NFSv4 clients without POSIX xattrs, Jobman supports the Linux server's
allow-only projection of that policy in `system.nfs4_acl`: four effective
entries in owner/numeric-reader/group/everyone order, plus four inherit-only
entries for directories. The broker principal must be the exact decimal UID.
Read-attribute/ACL and synchronize metadata rights are allowed in the server's
standard projection; only the owner can mutate data or ACLs. Named/domain
principal translation, reordered entries, deny/audit entries, additional
principals, unfamiliar flags or permissions, and ACLs over 4 KiB fail closed.
This deliberately supports a narrow verified projection rather than every
[NFSv4 ACL layout](https://man7.org/linux/man-pages/man5/nfs4_acl.5.html).
The representation follows the [Linux server ACL conversion](https://github.com/torvalds/linux/blob/master/fs/nfsd/nfs4acl.c).

Configured policy is unsupported on macOS and Windows and fails explicitly.
Missing ACL support or unexpected effective permissions on Linux also fails.
No mode-only fallback is available. A default private store works as before on
all supported platforms when no policy exists.

Policy-enabled writes use descriptor-relative no-follow directory and file
opens, bounded copying, exact ACL verification before payload writes, file
sync, an exclusive hard link from a fresh private staging directory, and
parent directory sync. Staging directories have mode `0700` and the exact
private ACL mask: readers cannot traverse to incomplete or aborted chunks.
The inherited default ACL supplies the final file grant without changing any
existing permission. A crash may leave an inaccessible staging directory for
operator cleanup. An
existing identical object is accepted only after its ACL and content are
verified; symlinks and changed content fail. ACLs on the file and all opened
ancestors are rechecked before publication. The owner and storage administrator
remain trusted: neither this feature nor Unix ACLs can stop an owner from
changing permissions concurrently or later. Parent paths outside the configured
root must likewise remain under trusted operator control.

## Verification

Portable tests include captured non-secret ACL bytes from the Jobman-Lab
NFSv4.2 mount, masked objects, malformed XDR, wrong identities, and unexpected
grants. Linux tests exercise real POSIX ACL inheritance, private artifact
isolation, immutable replay, symlinks, policy replacement, and missing or
broader grants.

For a live storage check, build the artifact test binary with the `integration`
build tag for the producer platform. As the ordinary producer user, set
`JOBMAN_TEST_LOG_READER_ROOT` to an **empty disposable root** provisioned with
the exact access/default ACL above and run only
`TestLogReaderProvisionedFilesystem`. It installs a synthetic `logs` version
1 policy, creates synthetic private and shared objects, and leaves their paths
for separate broker read, broker write denial, unrelated-user denial, and
root-squash checks. As the named reader, set
`JOBMAN_TEST_LOG_READER_READ_ROOT` to that same directory and run only
`TestLogReaderProvisionedRead` to verify the read-only API succeeds while
producer initialization and private-artifact reads fail. The operator then removes that disposable tree. Never point
this test at a production store root.
