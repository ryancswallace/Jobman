# Contributing to jobman

Thanks for considering a contribution.

## Before you start

For substantial changes, open an issue first so the problem and proposed
direction can be discussed. Small fixes and documentation improvements can go
directly to a pull request.

By participating, you agree to follow the [code of conduct](CODE_OF_CONDUCT.md).
Please report security vulnerabilities through [SECURITY.md](SECURITY.md), not a
public issue.

The project uses the Go version recorded in `go.version`. The included
devcontainer is the supported reproducible contributor environment; local Go
installations are equally welcome when they use the same version. `make setup`,
`make quick-check`, and `make check` fail early when the active patch version
does not match. The failure reports the exact `GOTOOLCHAIN` invocation that can
select the version recorded in `go.version`.
The full documentation and container checks also require a running Docker
daemon. ShellCheck is required when Docker is unavailable for script checks.

## Local checks

The normal pre-submission loop is:

```bash
make setup
make format
make check
```

`make setup` installs pinned development tools into `bin/` when they are not
already available and downloads the Go module graph. Run it once after cloning
or whenever tool versions change. Use `make help` to list the narrower workflows
available while iterating.

Documentation changes should pass `make docs`, and public API behavior changes
should update tests, docs, and [CHANGELOG.md](CHANGELOG.md) when users will
notice the change.

The published manual is assembled rather than maintained as a duplicate tree:

- author task-oriented pages under `site/`;
- update canonical contracts under `docs/` or at the repository root;
- update Cobra help for command and flag reference text; and
- run `make gen-site` to stage and validate the combined source in the ignored
  `site-build/` directory.

Never edit `site-build/` directly. Internal site links should use the
`{{ site.baseurl }}/getting-started/` form so local, project Pages, and
production paths remain consistent. The site generator rejects missing
internal destinations, missing assets, duplicate permalinks, legacy
Python-project instructions, and command reference drift.

The `Docs links` workflow separately checks published HTTPS destinations with
bounded concurrency and retries. Keep exclusions limited to deliberate example
endpoints; do not exclude a failing documentation provider merely to make the
check pass.

Useful focused checks include:

- `make quick-check` for the normal edit-test loop;
- `make lint` and `make format-check` for Go source quality;
- `make workflow-check shellcheck` for automation changes;
- `make vulncheck` for reachable Go vulnerabilities;
- `make unittest`, `make e2etest`, and `make perftest` for the distinct unit,
  assembled-binary, and performance tiers;
- `make fuzz` to run one selected fuzz target (CI matrices all targets; override
  `FUZZ_PACKAGE`, `FUZZ_TARGET`, `FUZZ_TIME`, or the resource-bounding
  `FUZZ_PARALLEL` worker count locally);
- `make soaktest SOAK_TIME=10m` for the opt-in race-enabled storage, logging,
  cleanup, and admission soak;
- `make release-build` to compile every supported release platform;
- `make release-check` to validate release configuration and tracked release
  records;
- `make snapshot` to build and verify archives, native packages, SBOMs,
  checksums, release metadata, and container images for release or packaging
  changes;
- `make docker-image` for runtime-image changes;
- `make docker-smoke` for persistent-state and derived-image behavior.

Generated man pages, completions, the staged site, and its command reference are
ignored in the working tree and created during validation or releases. Change
their generators under `devel/`, then run `make docs` to validate the generated
output and production-equivalent Pages build.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/) because commit
messages on `main` determine release versions. Common prefixes are `fix:`,
`feat:`, `docs:`, `test:`, `ci:`, and `chore:`. Mark breaking changes with `!`
or a `BREAKING CHANGE:` footer.

## Pull requests

Keep each pull request focused on one coherent change. In the description,
explain the problem, the chosen approach, compatibility impact, and verification
performed.

Do not include secrets, credentials, private logs, or personal data in commits,
issues, test fixtures, or workflow output. Pull requests from forks should not
require access to repository secrets.

Contributions are accepted under the project's [MIT License](LICENSE).

## Maintainer repository settings

Protect `main` with a repository ruleset that requires pull requests, successful
`Test` and `CodeQL` checks, resolved review conversations, and code-owner review
when another maintainer is available. Block force pushes and branch deletion.
Keep the default Actions token read-only and grant write permissions only in the
specific jobs that publish maintenance updates, Pages, releases, or security
results.
The maintenance workflow publishes an isolated automation branch and a compare
URL; it does not receive pull-request write or approval authority.

Enable private vulnerability reporting, Dependabot alerts and security updates,
secret scanning, and push protection when they are available for the repository.
The `main` release environment and `github-pages` environment should permit
deployments only from `main`; add required reviewers to the `main` environment
when a manual release approval boundary is desired.
