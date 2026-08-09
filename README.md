<!-- markdownlint-disable MD041 -->
<!-- markdownlint-disable MD033 -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark-transparent.svg">
  <img alt="Jobman" src="assets/logo.svg" width="420">
</picture>
<!-- markdownlint-enable MD033 -->

[![Test](https://github.com/ryancswallace/jobman/actions/workflows/test.yml/badge.svg)](https://github.com/ryancswallace/jobman/actions/workflows/test.yml)
[![Codecov](https://codecov.io/gh/ryancswallace/jobman/branch/main/graph/badge.svg)](https://codecov.io/gh/ryancswallace/jobman)
[![CodeQL](https://github.com/ryancswallace/jobman/actions/workflows/codeql.yml/badge.svg)](https://github.com/ryancswallace/jobman/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/ryancswallace/jobman/badge)](https://scorecard.dev/viewer/?uri=github.com/ryancswallace/jobman)
[![Latest release](https://img.shields.io/github/v/release/ryancswallace/jobman?sort=semver)](https://github.com/ryancswallace/jobman/releases/latest)
[![Go version](https://img.shields.io/github/go-mod/go-version/ryancswallace/jobman)](https://github.com/ryancswallace/jobman/blob/main/go.mod)
[![Go Reference](https://pkg.go.dev/badge/github.com/ryancswallace/jobman/jobman.svg)](https://pkg.go.dev/github.com/ryancswallace/jobman/jobman)
[![Documentation](https://img.shields.io/badge/docs-jobman.tech-blue)](https://jobman.tech/)
[![OSS hosting by Cloudsmith](https://img.shields.io/badge/OSS%20hosting%20by-Cloudsmith-blue?logo=cloudsmith)](https://cloudsmith.com/)

Jobman is a daemonless CLI for running and managing local background jobs. It
adds retries, timeouts, durable logs, dependencies, concurrency controls, and
notifications without a resident service.

<!-- markdownlint-disable MD033 -->
<p align="center">
  <img src="docs/screencaps/gif/basic.gif"
       alt="A beginner Jobman demo"
       width="900">
</p>
<!-- markdownlint-enable MD033 -->

> [!TIP]
> **Read the [Jobman documentation](https://jobman.tech)** for a quickstart,
> install instructions, user guides, and command reference.

## Features

| Capability | Jobman provides... |
| --- | --- |
| Daemonless execution | A per-job supervisor that requires no shared service or privileged installation |
| Durable state | Inspectable job metadata and logs stored in a private per-user directory |
| Execution policies | Retries, repeated runs, backoff, delayed starts, and per-run or whole-job timeouts |
| Coordination | Dependencies, wait conditions, store-wide limits, and named concurrency pools |
| Lifecycle control | Status, wait, pause, resume, cancel, rerun, and live input |
| Logging | Separate stdout/stderr capture, following, rotation, and retention |
| Notifications | Named notifiers and event subscriptions with bounded delivery retries |
| Automation | Stable selectors, versioned JSON output, strict YAML configuration, and shell completions |
| Diagnostics | Bounded factual evidence plus an optional independently installed diagnosis companion |

> [!NOTE]
> Jobman is a local, per-user process manager—not a distributed scheduler or
> backup system. Jobs can survive a closed terminal or SSH connection, but may
> end with the operating-system user session. Back up important state and logs
> independently.

## Command overview

| Task | Commands |
| --- | --- |
| Submit work | `run`, `rerun` |
| Inspect work | `list`, `status`, `show`, `logs`, `wait` |
| Control work | `pause`, `resume`, `cancel`, `input` |
| Maintain Jobman | `clean`, `doctor`, `config` |

Job selectors accept a full ID, a unique ID prefix of at least eight
characters, or an unambiguous exact name. Run `jobman COMMAND --help` for all
options, or browse the [command reference].

`jobman show evidence JOB` exports a model-free diagnostic snapshot, with
opt-in direct command/argument vectors through `--command`, paths through
`--paths`, environment variable names through `--environment-names`, bounded
log tails, and exact store-local failure history through `--similar N`. When the separate
[`jobman-diagnose` companion] is installed on
`PATH`, the generic external-command mechanism also makes
`jobman diagnose JOB` available. Core Jobman remains deterministic and has no
model or provider dependency. The companion can optionally add
schema-validated generated hypotheses, but only after explicit profile and
per-class disclosure selection; deterministic facts and retry advice remain
authoritative.

See the [configuration reference], [sample configuration], and
[persisted-schema reference] for more info.

## Installation

Start with the package or archive that fits your operating system:

| Environment | Recommended for most users | Other available methods |
| --- | --- | --- |
| macOS | [Homebrew][install-homebrew]: `brew install ryancswallace/tap/jobman` | `.tar.gz`, Go toolchain, source |
| Fedora, RHEL, Rocky, AlmaLinux, Amazon Linux | [Signed RPM repository][install-rpm-repository], then `sudo dnf install jobman` | Downloaded `.rpm`, `.tar.gz`, Go toolchain, source |
| Debian or Ubuntu | [Downloaded `.deb`][install-deb], then `sudo apt install ./jobman_X.Y.Z_linux_amd64.deb` | `.tar.gz`, Go toolchain, source |
| Alpine Linux | [Downloaded `.apk`][install-apk], then `sudo apk add --allow-untrusted ./jobman_X.Y.Z_linux_amd64.apk` | `.tar.gz`, Go toolchain, source |
| Other Linux | [Portable `.tar.gz` release archive][install-archive] | Go toolchain, source |
| Windows | [Portable `.zip` release archive][install-windows] installed with PowerShell | Go toolchain, source |

Release artifacts include signed checksums, provenance, SBOMs, man pages, and
shell completions. Follow the [installation guide] for repository setup,
copy-paste installation and verification commands, architecture selection, and
upgrade instructions.

Jobman also publishes a Linux container image for containerized workloads. It
is not the recommended way to install the CLI on a host because detached jobs
cannot outlive their container.

Supported operating systems, architectures, lifecycle primitives, and known
platform differences are documented in the [platform support reference].

## Documentation

| Topic | Resource |
| --- | --- |
| Quickstart | [Getting started] and the [first-job guide] |
| Common workflows | [User guides] |
| CLI | [Command reference][command reference] |
| Configuration | [Configuration reference][configuration reference] |
| Platform support | [Platform support reference][platform support reference] |
| Compatibility | [Compatibility contract] |
| Containers | [Container guide][container guide] |
| Troubleshooting and recovery | [Operations guides] |
| Architecture | [Design documentation] |
| Diagnostic evidence | [Evidence schema and privacy reference] |
| Releases | [Release and artifact verification guide] |

Use the [issue tracker] for reproducible bugs and feature proposals. Report
suspected vulnerabilities privately according to the [security policy].

## Development

Use the included devcontainer or a local Go installation:

```console
make setup
make quick-check
make check
```

`make help` lists development, test, documentation, packaging, and container
targets.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution requirements.

[command reference]: https://jobman.tech/reference/commands/
[compatibility contract]: https://jobman.tech/reference/compatibility/
[configuration reference]: https://jobman.tech/reference/configuration/
[container guide]: https://jobman.tech/guides/containers/
[design documentation]: docs/design/README.md
[evidence schema and privacy reference]: docs/DIAGNOSTIC_EVIDENCE.md
[first-job guide]: https://jobman.tech/getting-started/first-job/
[getting started]: https://jobman.tech/getting-started/
[install-apk]: https://jobman.tech/getting-started/installation/#install-an-apk-package-on-alpine-linux
[install-archive]: https://jobman.tech/getting-started/installation/#install-a-portable-linux-or-macos-archive
[install-deb]: https://jobman.tech/getting-started/installation/#install-a-deb-package-on-debian-or-ubuntu
[install-homebrew]: https://jobman.tech/getting-started/installation/#install-with-homebrew-on-macos
[install-rpm-repository]: https://jobman.tech/getting-started/installation/#install-from-the-rpm-repository
[install-windows]: https://jobman.tech/getting-started/installation/#install-a-windows-zip
[installation guide]: https://jobman.tech/getting-started/installation/
[issue tracker]: https://github.com/ryancswallace/jobman/issues
[jobman-diagnose companion]: https://github.com/ryancswallace/jobman-diagnose
[operations guides]: https://jobman.tech/operations/
[persisted-schema reference]: docs/design/PERSISTED_SCHEMA.md
[platform support reference]: https://jobman.tech/reference/platforms/
[release and artifact verification guide]: RELEASE.md
[sample configuration]: etc/jobman/jobman.yml
[security policy]: SECURITY.md
[user guides]: https://jobman.tech/guides/
