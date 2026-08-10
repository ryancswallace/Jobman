# ADR-0003: Discover optional commands as external executables

Status: Accepted
Date: 2026-08-09

## Context

Jobman needs optional capabilities, beginning with failure diagnosis, whose
dependencies, configuration, release cadence, and trust boundary do not belong
in the core binary. Go's dynamic plugin mechanism is compiler- and
platform-coupled, while importing optional components would make every Jobman
installation carry their dependencies.

Users should nevertheless be able to invoke an installed companion naturally
as `jobman diagnose`. The mechanism must preserve built-in command precedence,
argument boundaries, process status, automation safety, and Jobman's
cross-platform behavior. It must not search the network, execute a shell, or
turn ordinary help and completion into arbitrary code execution.

## Decision

After built-in Cobra command resolution fails, Jobman may resolve one unknown
command token `NAME` as an executable named `jobman-NAME` on the caller's
`PATH`.

The protocol has these rules:

1. A built-in command always wins and cannot be shadowed.
2. `NAME` contains only lowercase ASCII letters, digits, and internal hyphens.
   It cannot contain a slash, dot, empty hyphen-separated segment, or leading
   underscore.
3. Jobman uses direct platform executable lookup and process creation. It never
   invokes a shell, searches the network, downloads an extension, or uses an
   implicit plugin directory.
4. The current directory participates only when the platform's explicit
   `PATH` value includes it.
5. Jobman's persistent `--state-dir` and `--config` flags remain reserved. The
   dispatcher resolves and removes them while preserving all other child
   arguments exactly. An implicitly discovered configuration file is not sent
   as though the user explicitly selected it.
6. The child inherits the caller's standard streams and environment, never a
   managed target's resolved environment. Jobman replaces every reserved
   `JOBMAN_EXTENSION_*`, `JOBMAN_EXECUTABLE`, `JOBMAN_VERSION`,
   `JOBMAN_STATE_DIR`, and `JOBMAN_CONFIG` value with trusted protocol context.
7. Protocol version 1 supplies `JOBMAN_EXTENSION_PROTOCOL=1`, the absolute core
   executable, Jobman version, canonical state directory, and an explicitly
   selected configuration path when present.
8. Jobman sets `JOBMAN_NO_EXTENSIONS=1` for the child. Extensions may call core
   built-ins but cannot accidentally create a nested dispatch chain.
9. `--no-extensions` and `JOBMAN_NO_EXTENSIONS=1` disable resolution. A missing
   or disabled extension is an ordinary usage error and never opens the store.
10. The child receives cancellation and supported termination signals. Jobman
    preserves its exit status and does not print a second error after the child
    has reported its own failure.
11. Root help and completion never enumerate or execute programs from `PATH`.
    Completion may advertise an explicit allowlist of supported first-party
    companion names and values that core can resolve itself, such as job
    selectors for `diagnose`. `jobman NAME --help` is forwarded only after the
    user names the extension.

An extension is trusted native code running with the user's authority. The
protocol provides invocation context, not a sandbox. Extension installation
and provenance remain explicit user responsibilities.

## Consequences

- `jobman-diagnose` can be installed, upgraded, and removed independently while
  remaining available as `jobman diagnose`.
- The core binary retains no inference, provider, prompt, or companion
  dependency.
- Other carefully scoped optional commands can reuse the mechanism without a
  new plugin ABI.
- A typo can resolve a matching executable on `PATH`; documentation and the
  disable controls must make that trust implication visible.
- The main executable boundary needs a typed silent child-status result so
  Windows and non-replacing Unix implementations do not duplicate errors.
- Native signal and exit-status tests are required on Linux, macOS, and
  Windows.

## Rejected alternatives

- **Import the diagnosis package into Jobman:** couples dependencies and
  release schedules and makes optional inference part of every core build.
- **Go dynamic plugins:** introduce compiler ABI coupling and exclude supported
  platforms.
- **A private plugin directory:** creates a second installation and discovery
  system and hides normal executable trust semantics.
- **Shell command dispatch:** loses argument boundaries and creates injection
  risk.
- **Automatically enumerate extensions for help or completion:** executes or
  trusts arbitrary `PATH` entries during an otherwise read-only operation.
- **Permit nested extension dispatch:** increases recursion and confused-deputy
  risk without a first-release use case.
