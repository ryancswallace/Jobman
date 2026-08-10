---
layout: default
title: Installation
parent: Getting started
nav_order: 1
permalink: /getting-started/installation/
---

# Install and verify Jobman

Use the recommended package or archive for your operating system unless you
have a specific reason to build from source. Package-manager installations are
usually easiest because they also install Jobman's manual page, sample
configuration, and shell completions.

Pin an exact version in automation. Before opening an existing state directory
with a newer binary, review the release notes and the
[upgrade guide]({{ site.baseurl }}/operations/upgrading/).

## Choose an installation method

| Environment | Recommended for most users | Other supported methods |
| --- | --- | --- |
| macOS | [Homebrew](#install-with-homebrew-on-macos) | Portable `.tar.gz`, Go toolchain, source |
| Fedora, RHEL, CentOS Stream, Rocky Linux, AlmaLinux, Amazon Linux | [Cloudsmith RPM repository](#install-from-the-rpm-repository) | Downloaded `.rpm`, portable `.tar.gz`, Go toolchain, source |
| Debian or Ubuntu | [Cloudsmith DEB repository](#install-from-cloudsmith-on-debian-or-ubuntu) | Downloaded `.deb`, portable `.tar.gz`, Go toolchain, source |
| Alpine Linux | [Cloudsmith APK repository](#install-from-cloudsmith-on-alpine-linux) | Downloaded `.apk`, portable `.tar.gz`, Go toolchain, source |
| Other Linux distributions | [Portable `.tar.gz`](#install-a-portable-linux-or-macos-archive) | Go toolchain, source |
| Windows | [Portable `.zip`](#install-a-windows-zip) | Go toolchain, source |

The [container image](#container-image) is available for containerized
workloads, but it is not a normal host installation. A detached Jobman job
cannot outlive its container.

## Supported systems

Jobman v1 adopts the
[Go 1.26 minimum operating-system requirements](https://go.dev/wiki/MinimumRequirements):
Linux kernel 3.2 or later, macOS 12 Monterey or later, and Windows 10 or
Windows Server 2016 or later.

| Operating system | Release architectures | Release formats |
| --- | --- | --- |
| Linux | `amd64`, `arm64`, `386` | `.apk`, `.deb`, `.rpm`, `.tar.gz`; container for `amd64`/`arm64` |
| macOS | `amd64`, `arm64` | Homebrew formula, `.tar.gz` |
| Windows | `amd64`, `arm64`, `386` | `.zip` |

Every listed target receives a release-style compile check. Lifecycle and race
tests run natively on the current GitHub-hosted runner for each operating
system; cross-compiled architectures do not receive identical native evidence.
The state directory must be on a local filesystem with reliable SQLite WAL
locking. Jobs are scoped to the current operating-system user session even
though they can survive closing the submitting terminal or SSH connection.

## macOS

### Install with Homebrew on macOS

Homebrew is the recommended macOS installation method:

```sh
brew install ryancswallace/tap/jobman
jobman --version
```

The project-maintained formula selects the Intel or Apple Silicon release and
installs the binary, manual pages, sample configuration, and Bash and Zsh
completions. Start a new shell after installation so it discovers the
completions.

Upgrade or uninstall with:

```sh
brew upgrade ryancswallace/tap/jobman
brew uninstall jobman
```

The formula uses the same checksummed release archives described below. The
macOS executable is not yet Apple Developer ID signed or notarized, so
Gatekeeper may require explicit per-application approval. The formula does not
remove quarantine attributes or disable Gatekeeper.

If Gatekeeper blocks the first launch, verify the release, attempt the launch
once, then open **System Settings → Privacy & Security** and select **Open
Anyway** for Jobman. Authenticate and confirm the next launch. Do not bypass
organization policy or disable Gatekeeper globally. See
[Apple's guidance for safely opening an unnotarized app](https://support.apple.com/en-us/102445).

For an installation without Homebrew, use the
[portable archive](#install-a-portable-linux-or-macos-archive).

## Linux

Jobman publishes DEB, RPM, and APK packages for Linux 386, amd64, and arm64 in
the public [`jobman/stable` Cloudsmith repository](https://cloudsmith.io/~jobman/repos/stable/).
Cloudsmith signs repository metadata and packages for verification by the
native package manager. Configure the repository once with the setup script
for your package format, then install and upgrade Jobman normally.

### Install from the RPM repository

The signed Cloudsmith repository is recommended on Fedora, RHEL, CentOS
Stream, Rocky Linux, AlmaLinux, Amazon Linux, and other DNF-compatible systems.
Configure it once, then install Jobman:

```sh
curl -1sLf \
  'https://dl.cloudsmith.io/public/jobman/stable/cfg/setup/bash.rpm.sh' |
  sudo -E bash
sudo dnf install jobman
jobman --version
```

The setup script configures the distribution-neutral repository and its
signing key. If local policy prohibits piping a network response to a
privileged shell, download and review the script before running it. Install
future stable releases with:

```sh
sudo dnf upgrade jobman
```

### Install from Cloudsmith on Debian or Ubuntu

Configure the signed DEB repository, then install Jobman with APT:

```sh
curl -1sLf \
  'https://dl.cloudsmith.io/public/jobman/stable/cfg/setup/bash.deb.sh' |
  sudo -E bash
sudo apt update
sudo apt install jobman
jobman --version
```

Download and review the setup script first when local policy prohibits piping
a network response to a privileged shell. Upgrade future releases with
`sudo apt update && sudo apt upgrade jobman`.

### Install from Cloudsmith on Alpine Linux

Install the setup-script prerequisites, configure the signed APK repository,
and install Jobman:

```sh
sudo apk add --no-cache bash curl
curl -1sLf \
  'https://dl.cloudsmith.io/public/jobman/stable/cfg/setup/bash.alpine.sh' |
  sudo -E bash
sudo apk add jobman
jobman --version
```

Download and review the setup script first when required by local policy.
Upgrade future releases with `sudo apk upgrade jobman`.

### Install a downloaded DEB package

As an alternative to configuring Cloudsmith, download the `.deb` matching your
version and architecture from
[GitHub Releases](https://github.com/ryancswallace/jobman/releases), verify it
as described under [Verify a manual download](#verify-a-manual-download), then
run:

```sh
VERSION=X.Y.Z
ARCH=amd64
sudo apt install "./jobman_${VERSION}_linux_${ARCH}.deb"
jobman --version
```

Use `ARCH=arm64` or `ARCH=386` where appropriate. To upgrade, download the new
package, verify it, and run the same `apt install` command with its version.

### Install a downloaded APK package

Download the `.apk` matching your version and architecture from
[GitHub Releases](https://github.com/ryancswallace/jobman/releases), then
verify it as described under
[Verify a manual download](#verify-a-manual-download). The release package is
not signed with a locally trusted Alpine packaging key, so explicitly authorize
the verified local file:

```sh
VERSION=X.Y.Z
ARCH=amd64
sudo apk add --allow-untrusted "./jobman_${VERSION}_linux_${ARCH}.apk"
jobman --version
```

Use `ARCH=arm64` or `ARCH=386` where appropriate. To upgrade, verify the newer
package and install it with the same command.

### Install a downloaded RPM package

If you cannot configure the Cloudsmith repository, download and verify a
release RPM, then install the local package directly:

```sh
VERSION=X.Y.Z
ARCH=amd64
sudo dnf install "./jobman_${VERSION}_linux_${ARCH}.rpm"
jobman --version
```

Repository installation is preferable when available because `dnf upgrade`
can discover new Jobman versions automatically.

### What native Linux packages install

The `.apk`, `.deb`, and `.rpm` packages install:

- the `jobman` binary and manual page;
- Bash and Zsh completion scripts and their runtime dependencies;
- project and third-party license notices; and
- a preserved sample configuration at `/etc/jobman/jobman.yml`.

The packaged system configuration contains safe defaults. Per-user
configuration takes precedence, and Jobman's runtime state remains per-user by
default. Start a new shell after installation so it discovers the completion
scripts.

For Linux distributions without a matching package manager, use the
[portable archive](#install-a-portable-linux-or-macos-archive).

## Windows

### Install a Windows ZIP

The release ZIP is the recommended Windows installation. Download the ZIP,
checksum manifest, and Sigstore bundle for the desired version from
[GitHub Releases](https://github.com/ryancswallace/jobman/releases). Then open
PowerShell and set the version and architecture to match the downloaded file:

```powershell
$Version = 'X.Y.Z'
$Arch = 'amd64'
$Archive = "jobman_${Version}_windows_${Arch}.zip"
$Manifest = "jobman_${Version}_checksums.txt"
$Bundle = "${Manifest}.sigstore.json"
$InstallDir = Join-Path $env:LOCALAPPDATA 'Programs\Jobman'

cosign verify-blob `
    --bundle $Bundle `
    --certificate-identity `
      'https://github.com/ryancswallace/jobman/.github/workflows/release.yml@refs/heads/main' `
    --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' `
    $Manifest
if ($LASTEXITCODE -ne 0) { throw 'checksum signature verification failed' }

$ChecksumLine = @(Get-Content $Manifest | Where-Object {
    ($_ -split '\s+', 2)[1] -eq $Archive
})
if ($ChecksumLine.Count -ne 1) { throw 'archive is absent or duplicated in checksum manifest' }
$ExpectedHash = ($ChecksumLine[0] -split '\s+', 2)[0].ToLowerInvariant()
$ActualHash = (Get-FileHash -LiteralPath $Archive -Algorithm SHA256).Hash.ToLowerInvariant()
if ($ActualHash -ne $ExpectedHash) { throw 'archive checksum verification failed' }

New-Item -ItemType Directory -Force $InstallDir | Out-Null
Expand-Archive -Path $Archive -DestinationPath $InstallDir -Force

$UserPath = [Environment]::GetEnvironmentVariable('Path', 'User')
$PathEntries = @($UserPath -split ';' | Where-Object { $_ })
if ($PathEntries -notcontains $InstallDir) {
    [Environment]::SetEnvironmentVariable(
        'Path', (($PathEntries + $InstallDir) -join ';'), 'User'
    )
}
$env:Path = "$InstallDir;$env:Path"
jobman --version
```

The persistent `PATH` update applies to new terminals; the final assignment
makes Jobman available in the current PowerShell session. The ZIP does not
create a Windows service or machine-wide state. Its PowerShell completion file
is `docs\completions\powershell\jobman.ps1` inside the installation directory.

The Windows executable is not Authenticode signed. Its signed checksum and
provenance establish the release bytes, but they do not display a verified
Windows publisher. Microsoft Defender SmartScreen may show **Windows protected
your PC**, and Windows 11 Smart App Control or enterprise policy may block the
executable. Proceed only after verification and where local policy permits; do
not weaken a managed device's security controls. See
[Microsoft's SmartScreen reputation guidance](https://learn.microsoft.com/en-us/windows/apps/package-and-deploy/smartscreen-reputation).

## Manual release downloads

### Verify a manual download

Portable archives and downloaded Linux packages come from
[GitHub Releases](https://github.com/ryancswallace/jobman/releases). Each
release also includes a checksum manifest, Sigstore bundle, SBOMs, and
provenance. Before installing a downloaded artifact, verify the manifest and
the artifact checksum:

```console
$ cosign verify-blob \
    --bundle jobman_X.Y.Z_checksums.txt.sigstore.json \
    --certificate-identity \
      'https://github.com/ryancswallace/jobman/.github/workflows/release.yml@refs/heads/main' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    jobman_X.Y.Z_checksums.txt
$ sha256sum --check jobman_X.Y.Z_checksums.txt --ignore-missing
```

On macOS, use `shasum -a 256 <archive>` to compare an individual archive if
GNU `sha256sum` is unavailable. See the
[release process]({{ site.baseurl }}/project/releasing/#verifying-a-release)
for attestation and SLSA verification.

Release artifacts use names such as:

```text
jobman_X.Y.Z_linux_amd64.deb
jobman_X.Y.Z_linux_arm64.tar.gz
jobman_X.Y.Z_darwin_arm64.tar.gz
jobman_X.Y.Z_windows_amd64.zip
```

### Install a portable Linux or macOS archive

Set `VERSION`, `OS`, and `ARCH` to match the verified `.tar.gz`. Use
`OS=linux` or `OS=darwin` and a supported architecture from the table above:

```sh
VERSION=X.Y.Z
OS=linux
ARCH=amd64
ARCHIVE="jobman_${VERSION}_${OS}_${ARCH}.tar.gz"
EXTRACT_DIR="$(mktemp -d)"
tar -xzf "$ARCHIVE" -C "$EXTRACT_DIR"
install -d -m 0755 "$HOME/.local/bin"
install -m 0755 "$EXTRACT_DIR/jobman" "$HOME/.local/bin/jobman"
"$HOME/.local/bin/jobman" --version
```

Ensure the per-user binary directory is on `PATH`. Add this line to your shell
startup file, such as `~/.profile`, then start a new shell:

```sh
export PATH="$HOME/.local/bin:$PATH"
```

For a system-wide installation, replace the two `install` commands with:

```sh
sudo install -m 0755 "$EXTRACT_DIR/jobman" /usr/local/bin/jobman
```

The archive also contains the manual pages, shell completions, sample
configuration, changelog, project license, third-party notices, and citation
file. Unlike native packages and Homebrew, the binary-only commands above do
not install those supporting files automatically.

## Other installation methods

### Install with the Go toolchain

If Go 1.26.5 is already installed, install the latest tagged module without
downloading a release archive:

```sh
go install github.com/ryancswallace/jobman@latest
jobman --version
```

Ensure `$(go env GOPATH)/bin` or `GOBIN` is on `PATH`. This method builds the
binary locally and does not install manual pages, shell completions, or sample
configuration.

### Build from source

Source builds are intended for development or evaluation of the current
`main` branch. Reproducible project checks use the exact Go version recorded in
[`go.version`](https://github.com/ryancswallace/jobman/blob/main/go.version).
On Linux or macOS:

```console
$ git clone https://github.com/ryancswallace/jobman.git
$ cd jobman
$ make install
```

On Windows, clone the repository and build with the Go toolchain:

```powershell
git clone https://github.com/ryancswallace/jobman.git
Set-Location jobman
go build -o jobman.exe .
.\jobman.exe --version
```

### Container image

Jobman publishes `linux/amd64` and `linux/arm64` images to GitHub Container
Registry:

```console
$ docker pull ghcr.io/ryancswallace/jobman:vX.Y.Z
$ docker run --rm ghcr.io/ryancswallace/jobman:vX.Y.Z --version
```

The image contains Jobman and basic runtime utilities, not arbitrary target
commands. Read the
[container contract]({{ site.baseurl }}/guides/containers/) before submitting
work. A detached job cannot outlive a short-lived container.

## Verify the installation

Run these commands after installing Jobman by any method:

```console
$ jobman --version
$ jobman doctor
$ jobman config paths
```

`doctor` opens the selected state directory and reports database, filesystem,
and lifecycle health. `config paths` shows the concrete configuration files
for the current platform without resolving secrets.

Once `jobman --version` succeeds, continue with
[Your first job]({{ site.baseurl }}/getting-started/first-job/). The opening
three commands submit a managed job, inspect its durable result, and read its
captured output in about a minute.
