# Devcontainer

This devcontainer is the reproducible contributor environment for the project.

It provides:

* Go 1.26.6 on Debian Bookworm.
* Pinned Go editor and debugging tools plus the repository tools installed by
    `make bootstrap`.
* GitHub CLI for pull request and release workflows.
* Docker-outside-of-Docker for optional local container checks.
* VHS recording prerequisites (`ffmpeg`, `ttyd`, and `zsh`) for the tracked
    terminal screencasts.
* VS Code recommendations for Go, Markdown, CSpell, GitHub Actions, containers,
    YAML, and Makefile.

Docker-outside-of-Docker exposes the host Docker socket inside the container,
which means the devcontainer should be treated as a trusted development
environment. Keep local secrets in your editor, Codespaces secrets, or ignored
shell environment files. The shared configuration has no required host mounts
or environment files, so a clean clone can start without local preparation.
Container setup runs `make screencast-deps-check` before downloading the module
graph so a missing recording dependency fails during setup rather than during a
render.

## Local Runtime Options

Runtime settings that depend on one developer's machine should stay out of the
shared `devcontainer.json`. This section describes how to configure three common
developer-specific local patches to `devcontainer.json`:

* local secrets;
* host Git configuration;
* a relaxed seccomp profile.

### Preliminary: managing a local patch

If you choose to edit the tracked configuration locally, keep that patch out of
commits. As an optional convenience, tell Git to suppress routine status output:

```bash
git update-index --skip-worktree .devcontainer/devcontainer.json
```

Remember that `skip-worktree` can hide upstream changes. Re-enable normal
tracking before pulling or intentionally editing the shared configuration:

```bash
git update-index --no-skip-worktree .devcontainer/devcontainer.json
```

### Developer-specific settings

Add your machine-specific settings to `.devcontainer/devcontainer.json`. The
following snippet mounts your host `.gitconfig`, mounts in a local environment
variable file, a Codex auth file, and sets `seccomp=unconfined` (e.g., for Codex
sandboxing).

```json
"mounts": [
    "source=${localEnv:HOME}/.gitconfig,target=/home/vscode/.gitconfig,type=bind,consistency=cached",
    "source=${localEnv:HOME}/.local/share/devcontainer-bin,target=/home/vscode/.local/share/host-bin,type=bind,consistency=cached",
    "source=${localEnv:HOME}/.codex-devcontainer,target=/home/vscode/.codex,type=bind,consistency=cached"
],
"runArgs": [
    "--env-file",
    "${localWorkspaceFolder}/.devcontainer/.env.local",
    "--security-opt",
    "seccomp=unconfined"
]
```

If you use the `--env-file` option in `runArgs`, be sure to create
`.devcontainer/.env.local` with your local-only values. For example:

```dotenv
GH_TOKEN=github_pat_example
```

The `.env.local` file is ignored by Git.
