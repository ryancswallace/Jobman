# Platform requirements and capabilities

Jobman v1 inherits the [Go 1.26 minimum operating-system requirements]: Linux
kernel 3.2 or later, macOS 12 Monterey or later, and Windows 10 or Windows
Server 2016 or later.

## v1 matrix

| Capability | Linux | macOS | Windows |
| --- | --- | --- | --- |
| Supervisor detachment | New session via `setsid`; assembled tests prove the target outlives the submitting process. | New session via `setsid`; the hosted native suite runs the same assembled detachment scenario. | Detached hidden process in a new process group; the hosted native suite proves the target outlives the submitter. |
| Target tree | Dedicated process group. | Dedicated process group. | The target starts suspended, is assigned to a named Job Object, then its initial thread is resumed. Descendants inherit Job Object membership. |
| Process identity | PID, `/proc` start ticks, and kernel boot ID. | PID, `KERN_PROC` start time, and kernel boot time. | PID, process-creation `FILETIME`, and system boot time from `NtQuerySystemInformation`. |
| Stop | Revalidated `SIGTERM`, then optional `SIGKILL`, to the process group. | Revalidated `SIGTERM`, then optional `SIGKILL`, to the process group. | Best-effort `CTRL_BREAK_EVENT`, followed after the configured grace period by guaranteed Job Object termination. |
| Pause/resume | Revalidated `SIGSTOP`/`SIGCONT` for the process group. | Revalidated `SIGSTOP`/`SIGCONT` for the process group. | Every current Job Object member is revalidated and suspended/resumed with the native process suspension API. |
| Live input | Private owner-only Unix-domain socket. | Private owner-only Unix-domain socket. | Private named pipe with a protected current-user, SYSTEM, and administrators DACL. |
| State privacy | UID ownership, mode, symlink, and hard-link checks. | UID ownership, mode, symlink, and hard-link checks. | New paths receive protected ACLs; existing state/database paths must be owned by the current user and must not grant broad-principal access. |
| Local storage | Known remote/distributed filesystems are rejected before SQLite is opened. | NFS, SMB, WebDAV, AFP, and FUSE state roots are rejected. | Remote drive roots are rejected. |
| Diagnostic resource observations | Post-wait user/system CPU time plus peak resident memory from wait resource usage; every value is conservatively process-scoped and complete at exit. Linux reports `Maxrss` in KiB and Jobman converts it to bytes with overflow checking. | Post-wait user/system CPU time plus process-scoped peak resident memory from wait resource usage. macOS reports `Maxrss` in bytes. | Post-wait process-scoped user/system CPU time. Peak resident memory is omitted until a reviewed Job Object accounting adapter can establish its scope. |
| Assembled-binary evidence | Full Linux lifecycle and crash-boundary suite. | Native detachment, output, tree cancellation, live input, pause/resume, and package tests. | Native detachment, output, Job Object cancellation, named-pipe input, pause/resume, ACL, and package tests. |
| Race evidence | Linux unit and assembled suites run with the race detector. | Platform, live-input, log, store, supervisor, and assembled-binary packages run natively with the race detector. | Platform, live-input, log, store, supervisor, and assembled-binary packages run natively with the race detector. |
| Architecture evidence | `amd64`, `arm64`, and `386` release-style builds. | `amd64` and `arm64` release-style builds. | `amd64`, `arm64`, and `386` release-style builds. |

Platform-neutral retry, dependency, wait, timeout, admission, cleanup, and
notification policy is exercised by the package test suite on every native CI
runner. Release-style builds remain `CGO_ENABLED=0`. Cross-architecture build
jobs prove compilation only; they do not replace native lifecycle or race
evidence.

Diagnostic evidence encoding, strict decoding, disclosure classes, HMAC
fingerprints, and configured value-redaction capability reporting are
platform-neutral. `configured_value_redaction_v1` is emitted only when a
resolved-value or literal-value configured rule was active; it is not a claim
that arbitrary target output is secret-free.

## Deliberate differences

- Windows console control delivery is inherently dependent on console
  attachment. Jobman treats it as best effort and relies on Job Object
  termination for the guaranteed forced phase.
- Unix process groups and Windows Job Objects are different primitives; their
  user-visible contract is tree-wide lifecycle control, not identical signals.
- Ending an entire operating-system user session may terminate jobs. The v1
  guarantee covers closing the submitting terminal or SSH connection.
- Resource observations never promote one process's accounting to a process-
  tree total. I/O counters, Job Object aggregate accounting, control-group
  limits, and confirmed resource-limit events are not currently collected.
  Exit code 137 alone is not treated as confirmed out-of-memory evidence.

[Go 1.26 minimum operating-system requirements]: https://go.dev/wiki/MinimumRequirements
