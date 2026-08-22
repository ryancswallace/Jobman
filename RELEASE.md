# Releasing jobman

Releases are automated from tested commits on `main`. Semantic-release chooses
the next version and creates the `vX.Y.Z` tag and GitHub release; GoReleaser then
builds, signs, and publishes the release artifacts.

## Release flow

1. Merge a pull request into `main` using Conventional Commit messages.
2. The `Test` workflow runs against the new `main` commit.
3. After that workflow succeeds, the `Release` workflow previews
   semantic-release without entering the protected environment. If a release
   is required, it waits for the Test, CodeQL, Fuzz, Docs site, Docs links, and
   OpenSSF Scorecard workflows to succeed on that exact commit.
4. After the `main` environment is approved, semantic-release creates the next
   tag and a draft GitHub release.
5. GoReleaser checks out that exact tag and stages binaries, archives, native
   Linux packages, SBOMs, checksums, a checksum signature, and signed immutable
   version tags for a multi-platform GHCR image.
6. An isolated SLSA generator signs provenance for every checksummed artifact
   and uploads `jobman.intoto.jsonl` to the draft GitHub release. After the
   remote checksum, signature, and provenance assets are verified, the workflow
   publishes the exact draft by numeric release ID and then moves the stable
   `latest` container alias to its already signed immutable image.
7. After a stable release is public, the workflow publishes its nine verified
   DEB, RPM, and APK packages to the public `jobman/stable` Cloudsmith
   repository and proposes the generated Homebrew formula to
   `ryancswallace/homebrew-tap`. Required tap checks validate and automatically
   merge the formula pull request. This ordering
   prevents either package repository from advertising assets that are still
   private in a draft release.
8. If there are no releasable commits, the workflow exits successfully without
   creating a tag or publishing artifacts.

Release jobs are serialized and are never cancelled in progress. The workflow
does not run independently on tag pushes, which prevents duplicate publication
when semantic-release creates a tag.

## Version selection

Semantic-release follows Conventional Commits:

| Commit | Release effect | Example |
| --- | --- | --- |
| `fix:` or `perf:` | Patch | `fix: preserve job output on retry` |
| `feat:` | Minor | `feat: add retry backoff policies` |
| `BREAKING CHANGE:` footer or `!` | Major | `feat!: change job file format` |
| `docs:`, `test:`, `ci:`, `chore:` | No release by themselves | `docs: clarify timeout behavior` |

Use squash-merge titles that follow this convention because the commits on
`main`, not pull-request labels, determine the version.

For the first stable release, the release-triggering squash commit must carry
an explicit major-version signal, such as `feat!: freeze the v1 public
contract` or a `BREAKING CHANGE:` footer. Confirm that semantic-release selects
`v1.0.0`; do not manually replace a calculated `v0.x` tag with a v1 tag.

## Published artifacts

Each GitHub release contains:

- `.tar.gz` archives for Linux and macOS and `.zip` archives for Windows;
- `amd64`, `arm64`, and supported `386` binaries;
- `.deb`, `.rpm`, and `.apk` Linux packages;
- generated man pages, Bash/Zsh completions, the sample configuration, license,
  third-party notices, README, and changelog inside the portable archives;
- SPDX JSON SBOMs for archives and native packages;
- a SHA-256 checksum manifest and a keyless Sigstore bundle for that manifest;
- a keyless, verifiable SLSA provenance bundle named `jobman.intoto.jsonl` for
  every file in the checksum manifest;
- GitHub-hosted, Sigstore-signed build provenance attestations for every file
  in the checksum manifest and every published container digest;
- keyless Sigstore signatures for the multi-platform container manifest and
  its platform-specific images.

GoReleaser also publishes `linux/amd64` and `linux/arm64` images to:

```text
ghcr.io/ryancswallace/jobman:<version>
ghcr.io/ryancswallace/jobman:v<version>
ghcr.io/ryancswallace/jobman:latest
```

The `latest` image is published only for stable versions. Images run as an
unprivileged `jobman` user and include Bash, CA certificates, and timezone data.
They are CLI runtimes, not persistent daemon containers: use `run --wait` or
`run --foreground`, mount `/home/jobman/.local/state/jobman`, and derive a
workload image containing target commands as described in
[docs/CONTAINERS.md](docs/CONTAINERS.md).

Immutable versioned image tags may become visible while the matching GitHub
release is still a draft. Verify the published release's digest signature and
attestation before use. The mutable `latest` alias is not moved during this
staging interval and is intentionally promoted only after the GitHub release
is public. If alias promotion then fails, the verified immutable release
remains public while `latest` remains on the preceding stable version; run the
**Repair stable container alias** workflow with the published tag to repair
that channel without rebuilding public artifacts.

After GoReleaser stages the release, Jobman's formula generator reads its
checksum manifest and creates a formula for the same Intel and Apple Silicon
archives. The formula, including installation rules for man pages, sample
configuration, and Bash/Zsh completions, is carried between jobs as a
short-lived workflow artifact and is pushed to `ryancswallace/homebrew-tap`
only after the matching GitHub release becomes public. The executable is not
Apple Developer ID signed or notarized, and the formula does not remove
quarantine attributes. The installation guide documents Apple's explicit
per-application approval for users who accept that limitation.

The portable Windows executables are likewise not Authenticode signed. Their
checksums, Sigstore bundle, attestations, and provenance authenticate release
bytes but are not a Windows publisher signature; SmartScreen, Smart App
Control, or enterprise policy may warn or block them. Add Authenticode signing
only with a protected signing identity, timestamping, signature verification,
and native install evidence in the release workflow.

## Repository configuration

The workflow uses GitHub's short-lived `GITHUB_TOKEN` for Jobman releases and
separate `CLOUDSMITH_API_KEY` and `HOMEBREW_TAP_TOKEN` secrets for RPM and
Homebrew publication. No GPG private key is required. Keep these workflow
permissions enabled:

- `contents: write` for tags, releases, and assets;
- `packages: write` for GHCR;
- `id-token: write` for keyless Sigstore signing;
- `attestations: write` and `artifact-metadata: write` for provenance.

Cloudsmith publication uses a personal API key because the free Cloudsmith Core
plan does not provide OIDC or service accounts. Create the key under **Personal
API Keys** for an account that can push to `jobman/stable`, then store it as the
`CLOUDSMITH_API_KEY` secret in the Jobman repository's protected `main`
environment. The release, staged-release recovery, and manual package repair jobs
all run under that environment and pass the secret only to the pinned
Cloudsmith CLI action. Never store or print the key in the repository; replace
the environment secret immediately whenever the key is refreshed or may have
been exposed. Keep the Cloudsmith repository public and classified as open
source.

Linux-package publication is idempotent. Before uploading, the shared
publication script requires an already-public stable GitHub release and
verifies its keyless signed checksum manifest, all nine package checksums, and
every GitHub artifact attestation. Each upload carries the original release
digest as a `source-sha256-*` tag, which remains comparable when Cloudsmith
re-signs an RPM. If publication fails after the GitHub release becomes public,
dispatch **Publish Linux packages** from `main` with the existing stable tag;
it verifies and publishes only missing packages.

Create `HOMEBREW_TAP_TOKEN` as a fine-grained personal access token restricted
to the `ryancswallace/homebrew-tap` repository with **Contents: read and
write** and **Pull requests: read and write**. Store it as a secret in the
Jobman repository's protected `main`
environment. Do not grant it access to the Jobman repository, releases,
packages, workflows, or administration. Every job that reads this secret uses
the `main` environment. The Homebrew publication job receives no write
permission through `GITHUB_TOKEN`; it runs only after the verified Jobman
release is public.

When creating or editing the token, select `ryancswallace` as its resource
owner, choose **Only select repositories**, explicitly select
`homebrew-tap`, and set **Repository permissions → Contents** and **Pull
requests** to **Read and write**. Replace the environment secret whenever the token is regenerated or
expires. The workflows query the repository API before checkout and fail early
unless the token reports repository write access. The workflow pushes only an
automation branch, opens a pull request, and requests auto-merge; protected
`main` requires strict online audit and installation tests for both formulas
on Intel and Apple Silicon runners.

If Homebrew publication fails after the GitHub release becomes public, fix the
token or repository rule and dispatch **Publish Homebrew formula** from `main`
with the existing stable tag. The repair workflow regenerates the formula only
from that public release's checksum manifest and does not rebuild or modify
release artifacts.

The isolated SLSA provenance job additionally receives `actions: read`,
`contents: write`, and `id-token: write`. The SLSA generator must be referenced
by a complete release tag such as `v2.1.0`: its verifier currently rejects a
commit-SHA reference. This is an intentional exception to the repository's
normal action-pinning policy. Dependabot monitors the tag for updates.

The **Publish staged release** recovery workflow receives `actions: read` and
`contents: write`. It can publish an already-staged stable draft but cannot
rebuild artifacts, sign new content, push packages, or move a container alias.
It runs under the protected `main` environment and shares
`devel/verify-publish-release.sh` with the normal release workflow.

In **Settings → Actions → General → Workflow permissions**, retain read-only
defaults and leave **Allow GitHub Actions to create and approve pull requests**
disabled. Post-release and scheduled repository maintenance publish only the
`automation/repository-maintenance` review branch and place its compare URL in
the workflow summary. A maintainer creates and reviews the pull request.

The workflow grants its token only the explicit write permissions listed
above. In the package settings, ensure this repository's workflow has write
access to the `jobman` GHCR package. This is required even when the workflow has
`packages: write`; an existing package can retain access settings from the
repository or token that first created it. A `403 Forbidden` while pushing
layers indicates package access, ownership, or organization-policy
configuration rather than a GoReleaser build failure.

Set the `jobman` GHCR package visibility to **Public**. Repository visibility
does not automatically make a container package public. The release workflow
runs its immutable-digest container smoke test with an empty Docker credential
store and fails before publication if the advertised anonymous pull contract
does not hold. To check the setting independently against an existing version:

```sh
anonymous_config=$(mktemp -d)
DOCKER_CONFIG="$anonymous_config" \
  docker pull ghcr.io/ryancswallace/jobman:v0.9.0
rm -rf "$anonymous_config"
```

Protect existing `v*` tags from update and deletion. Permit authorized writers
and the release workflow to create new tags; do not grant Actions a bypass that
would also allow existing release tags to be rewritten or deleted.

The release job uses the repository's `main` environment. Keep that environment
restricted to deployments from `main`, and retain required reviewers when
releases need a manual approval boundary. Manual recovery runs are rejected
unless the workflow itself is dispatched from `main`.

## Local validation

Install Go 1.26.6, GoReleaser 2.17, Syft, Cosign, SLSA verifier 2.7.1,
Docker with Buildx, and QEMU/binfmt for multi-platform container tests. Then
run:

```sh
make check
make docker-smoke
make snapshot
```

Snapshot mode writes artifacts to `dist/` and does not create a GitHub release.
Keyless checksum and container signing are skipped locally because they need
GitHub credentials and an Actions OIDC identity.
Docker Buildx may create local platform-suffixed images during snapshot
validation. `make snapshot` finishes by checking artifact counts, checksums,
required archive contents, absence of repository scaffolding, executable
version metadata, a tag-versioned changelog, dependency notices, and the
project-level `CITATION.cff`. The release workflow repeats the same artifact
check before generating attestations.

`make release-check` validates the release configuration without requiring a
post-release maintenance commit to have landed already. After a successful
Release workflow, repository maintenance runs automatically, verifies the
changelog section and Unreleased comparison link against the latest published
semantic version tag, and opens a reviewed pull request that updates those
records. Merge that pull request before preparing the next release candidate.

Run every target in `.github/workflows/fuzz.yml` for 30 seconds and complete a
race-enabled `make soaktest` run as recorded in the [dogfood runbook] before
merging the v1 release commit. The scheduled workflows remain the authoritative
long-running evidence for that exact commit.

GoReleaser includes the tracked `CITATION.cff` directly. It intentionally
describes the project rather than an individual release, so it does not contain
error-prone version or release-date fields. Confirm the file is present inside
at least one archive.

Before merging a release-triggering change, also confirm that generated assets
are non-empty and current:

```sh
make gen-manpage gen-completions
test -s docs/manpage/jobman.1
test -s docs/completions/bash/jobman
test -s docs/completions/zsh/_jobman
```

The Release workflow now verifies successful Test, CodeQL, Docs site, Docs
links, OpenSSF Scorecard, and all four Fuzz jobs on the exact commit before it
offers the `main` environment approval. Confirm that gate, review the Soak
workflow result and the manual evidence described in `docs/DOGFOOD.md`, and
verify the previewed version. The environment approval is the final human
boundary; do not approve merely because semantic-release calculated `v1.0.0`.

## Verifying a release

Download an artifact, the checksum manifest, and its `.sigstore.json` bundle
from the same GitHub release. Verify the signature first, then the checksum:

```sh
cosign verify-blob \
  --bundle jobman_<version>_checksums.txt.sigstore.json \
  --certificate-identity \
    'https://github.com/ryancswallace/Jobman/.github/workflows/release.yml@refs/heads/main' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  jobman_<version>_checksums.txt

sha256sum --check jobman_<version>_checksums.txt --ignore-missing

gh attestation verify \
  --owner ryancswallace \
  jobman_<version>_linux_amd64.tar.gz

slsa-verifier verify-artifact \
  --provenance-path jobman.intoto.jsonl \
  --source-uri github.com/ryancswallace/Jobman \
  --source-branch main \
  jobman_<version>_linux_amd64.tar.gz
```

The `sha256sum` command above is available on GNU/Linux. On stock macOS,
download the one artifact you intend to verify and check only its signed
manifest record with the system `shasum` utility:

```sh
artifact=jobman_<version>_darwin_arm64.tar.gz
awk -v artifact="$artifact" \
  '$2 == artifact { print; found = 1 } END { exit !found }' \
  jobman_<version>_checksums.txt |
  shasum -a 256 --check
```

The GitHub attestation and downloadable SLSA bundle independently bind the
artifact name and digest to the release workflow and source commit. The SLSA
bundle covers all files listed by the release checksum manifest, so the same
bundle verifies any archive, native package, or SBOM from that release. Inspect
an SBOM before installation when provenance or dependency review is required.
For containers, pull an immutable version tag rather than `latest`:

```sh
docker pull ghcr.io/ryancswallace/jobman:<version>
cosign verify \
  --certificate-identity \
    'https://github.com/ryancswallace/Jobman/.github/workflows/release.yml@refs/heads/main' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/ryancswallace/jobman:<version>
```

## Recovering or retrying a release

Do not delete and recreate tags as a first response. Open the `Release` workflow,
select the `main` branch, choose **Run workflow**, and enter the existing tag
(for example, `v1.2.3`). The workflow validates the tag, checks it out, rebuilds
from that exact commit, and creates a missing draft or replaces artifacts in an
existing draft. It rejects an already published release because build dates
are embedded in binaries and image metadata: rebuilding one public tag would
silently change its bytes, checksums, and signatures. Publish a new patch
release to correct any artifact that has already become public.

The recovery tag must identify the current `main` commit selected by the
dispatch, its GitHub release must be absent or still a draft, and all required
exact-commit workflow results must remain available and successful. This
constraint keeps artifact attestations bound to the workflow's actual source
commit, prevents an old recovery from moving `latest` backward, and prevents
the input from publishing an arbitrary repository tag.

This path also recovers the narrow failure window in which semantic-release
successfully pushed the tag but did not create the draft GitHub release. If
`main` has advanced since the tagged release attempt, do not use this
rebuilding workflow for the old tag.

If no release exists yet, GoReleaser creates the draft; otherwise, recovery
keeps the incomplete release private while replacing its asset set. Run it only
while the tag is still at current `main`. Never undraft an incomplete
replacement manually: first complete a successful retry and verify every
checksummed asset, the checksum signature, and provenance.

If **Build and stage release** and the complete SLSA provenance job succeeded,
but **Publish complete release** failed before making the draft public, retain
the tag, draft, assets, provenance, signatures, and versioned images. Do not
rebuild them after `main` advances. Land the workflow correction as a
non-releasing `ci:` commit, wait for its own `main` checks, and then:

1. Run **Publish staged release** from `main` with the retained stable tag and
   approve the protected environment.
2. Let it require successful exact-commit quality workflows, verify the signed
   checksum and every artifact, bind the SLSA provenance and container metadata
   to the tagged source commit, recheck the draft for concurrent changes, and
   publish that exact numeric release ID.
3. Run **Repair stable container alias** from `main` with the same tag. It
   verifies the immutable image again, promotes `latest`, and resumes
   post-release maintenance.

This publish-only path intentionally supports stable drafts, not prereleases.
It also rejects a tag that is not reachable from `main` or is older than an
already-published stable version. A rerun of the original failed job continues
to use the workflow definition from its original commit and therefore cannot
pick up a later workflow fix.

If the GitHub release is already public but only the mutable GHCR `latest`
alias failed to move, do not rebuild or redraft the release. Run the **Repair
stable container alias** workflow from `main` with the published tag. That
workflow requires the tag to be GitHub's latest stable release, verifies the
immutable image's release-workflow signature, promotes the alias, and confirms
both names resolve to the same manifest digest. It then resumes the
post-release repository-maintenance workflow that the failed publication job
could not trigger.

If GitHub has expired the tagged commit's workflow records, the automated
recovery intentionally fails closed. Rebuild on an isolated host for diagnosis,
then publish a new patch release from a newly tested `main` commit rather than
bypassing provenance or exact-commit gates for the old tag.

Before retrying, diagnose the failed publishing stage:

- GHCR failures usually indicate missing package write access;
- Cloudsmith authentication failures usually indicate that
  `CLOUDSMITH_API_KEY` is missing, revoked, or owned by an account without push
  access to `jobman/stable`;
- signing failures usually indicate missing `id-token: write` permission;
- a missing `jobman.intoto.jsonl` asset usually indicates that the isolated
  provenance job could not read the release or obtain its OIDC identity;
- an absent man page or completion file means the generator failed or produced
  an empty file;
- a duplicate asset error should be handled automatically by GoReleaser's
  `replace_existing_artifacts` setting.
- a `404 Not Found` from `GET /releases/tags/<tag>` while the release is still
  a draft means the published-release-only REST lookup was used. Drafts must be
  resolved through GraphQL and then read or updated by numeric release ID.

Only remove a tag or release when it points at the wrong commit or exposed an
artifact that must be revoked. Document that incident before publishing a new
version; released version numbers should not be reused for different source.

## Manual preflight for maintainers

Before relying on the first stable release:

- confirm the latest `Test` run on `main` is green;
- check that all release-worthy commits use Conventional Commit syntax;
- run the local snapshot commands above;
- verify GitHub Actions, GHCR, and tag-protection permissions;
- verify the `main` environment's deployment protection rules;
- confirm the `jobman` GHCR package grants this repository's Actions workflow
  write access, especially if the package already exists;
- review the generated GitHub release notes and all artifacts after publication;
- install at least one archive or native package and run the versioned
  container;
- review the v1 release-commit checklist in [SUPPORT.md](SUPPORT.md), including
  README warnings, security support, platform evidence, upgrades, changelog,
  every fuzz target, performance/soak results, and `make docker-smoke`.

[dogfood runbook]: docs/DOGFOOD.md
