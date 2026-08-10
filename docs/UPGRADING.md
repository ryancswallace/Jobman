# Upgrading and restoring Jobman

Jobman upgrades the per-user SQLite schema automatically on first open. Before
an upgrade transaction begins, it writes a consistent private snapshot under
`STATE_DIR/backups/`. If that snapshot cannot be written or validated, Jobman
does not migrate the database.

v1 supports forward upgrades within the v1 release line. Downgrades are not
supported after a newer binary has migrated the database. Patch and minor
releases preserve the CLI, JSON, configuration, and persisted-state contracts
described in `docs/COMPATIBILITY.md`; release notes identify any newly added
fields or migrations.

Jobman v1.0 through v1.3 write database schema 7. Jobman v1.4 and newer write
schema 8 and directly upgrade intact existing schemas 1 through 7. Schema 8
adds a private store-local fingerprint key and typed
per-run diagnostic facts. Schema 0 represents a new, uninitialized database
rather than a supported historical store. If `doctor` reports a foreign
application ID, an unsupported schema, a migration checksum mismatch, or
corruption, stop and preserve the state directory instead of forcing an
upgrade.

After a schema-8 migration, binaries that support only schema 7 refuse to open
the state root. Keep the automatic schema-7 backup until the new release is
accepted. Restoring it is an offline rollback and also discards all state
written after that snapshot; do not copy only the database over a live WAL.

## Upgrade from v0.6.0 or later to v1.0

The durable Jobman implementation and its versioned SQLite store were first
released in v0.6.0. Tagged releases v0.6.0 through v0.9.0 create schema 7, which
v1.0 opens without a schema conversion; follow this procedure to validate the
stricter stable contract and preserve a rollback copy. Jobman also accepts
intact intermediate schemas 1 through 6 created during development of the
durable implementation, although no tagged release used one of those as its
final schema.

Releases v0.1.0 through v0.5.0 were an unrelated prototype and have no
supported persisted-state or configuration migration to v1. Do not point a v1
binary at prototype state. Preserve any data that matters as an offline
archive, translate required command settings manually, and start v1 with a new
state directory.

The v1 configuration schema remains version 1, but parsing is strict and old
experimental keys are not silently ignored. Stage the v1 binary under a
different filename and validate configuration before it opens the existing
state root:

```console
/path/to/jobman-v1 config validate
/path/to/jobman-v1 config show --origins
```

Then use the installed v0.6.0-or-later binary to inspect and back up the store
before replacement:

```console
jobman --state-dir "$STATE_DIR" doctor --json
jobman --state-dir "$STATE_DIR" doctor \
  --backup "$HOME/jobman-before-v1.db"
```

Stop other Jobman clients, supervisors, and targets associated with that state
root before replacing the binary. Run the v1 binary's `doctor` command first;
that open performs any required migration and creates an automatic backup only
when a migration is needed. A schema-7 database from a tagged v0.6.0–v0.9.0
release is opened without that additional backup. Complete the representative
inspection and disposable-job checks below before resuming normal use. Keep the
old binary, the operator-selected backup, and any automatic migration backup
until the upgrade has been accepted, but do not run the old binary against a
state root that v1 has migrated.

## Before upgrading

1. Finish or cancel important active jobs.
2. Record `jobman --version` and `jobman --state-dir PATH doctor --json`.
3. Create an operator-selected snapshot:

   ```console
   jobman --state-dir "$STATE_DIR" doctor --backup "$HOME/jobman-before-upgrade.db"
   ```

4. Stop invoking Jobman from other terminals while replacing the binary.
5. Install the new binary and run `jobman --state-dir "$STATE_DIR" doctor`.
6. Inspect representative jobs and logs, then run one disposable managed job
   before returning the state root to normal use.

When a migration is required, its automatic backup remains under
`STATE_DIR/backups/`; retain the command output and record that path with the
upgrade evidence.

## Offline restore

Restore only while no Jobman client, supervisor, or target associated with the
state root is running. Keep the failed database and WAL sidecars for diagnosis.

1. Move the entire failed state directory to a timestamped quarantine path.
2. Create a new owner-private state directory.
3. Copy the selected backup to `jobman.db` in that directory without copying
   the old `jobman.db-wal` or `jobman.db-shm` files.
4. Ensure the directory is accessible only to the current user and the database
   is not a symlink or hard link.
5. Run the same or newer Jobman binary with `--state-dir NEW_STATE doctor`.
6. Inspect `list --all`, representative `show` output, and retained logs before
   resuming normal use.

Do not edit SQLite rows by hand. If `doctor` reports integrity or foreign-key
failures, preserve the quarantined state and use SQLite recovery tooling on a
copy; Jobman's `--repair` deliberately does not invent lifecycle state or
rewrite corrupt rows.

## Platform or architecture changes

Portable state may be copied only while no Jobman process owns it. Moving a
state root between supported architectures of the same operating system is
expected to preserve SQLite metadata and log bytes, but active process
identities are host-specific and must not be resumed. Run `doctor`, inspect all
nonterminal jobs, and submit a disposable test job on the destination host.

Moving state between Linux, macOS, and Windows is not a supported v1 migration
because path syntax, process identity, ACLs, and local IPC differ. Preserve the
source state as an offline archive and start with a new state root instead.

## Validate an upgrade before rollout

Before an important rollout, rehearse the oldest schema in use against a copy
of representative state on each deployed operating system. Retain the
pre-upgrade database digest, automatic migration backup, `doctor --json` before
and after output, job/log spot checks, and an offline restore result. A
successful cross-compile is not upgrade evidence.
