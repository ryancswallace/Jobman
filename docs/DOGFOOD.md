# v1 dogfood and release-candidate runbook

Use this runbook for evidence that automated tests cannot provide: terminal
disconnects, native packages, network filesystems, remote notifiers, upgrades,
and long-running resource behavior.

Test the exact release-candidate commit and artifacts on disposable Linux,
macOS, and Windows accounts. Never use production jobs, credentials,
configuration, endpoints, or state.

## Release-wide gate

Run once from the release commit:

```sh
make check
make snapshot
make e2etest
make perftest
make soaktest SOAK_TIME=10m
```

All commands must pass. Then run the applicable OS procedures below against
the unpacked or installed artifact—not `go run`.

For every host, record:

- OS version, architecture, filesystem, shell, and local/SSH access;
- release commit, artifact filename, checksum result, and `jobman --version`;
- operator and UTC start/end times; and
- commands, job IDs, JSON results, and deviations.

Use a fresh local-disk state directory.

## Linux and macOS

Linux and macOS share the same CLI and POSIX shell procedure. Platform-specific
artifact and host checks are called out where they differ.

### 1. Start a campaign

In every terminal, use the same `CAMPAIGN`, `STATE_DIR`, and `JOBMAN_BIN`.
Replace the first two paths:

```sh
cd /path/to/jobman-release-commit
export JOBMAN_BIN=/absolute/path/to/release-candidate/jobman
export CAMPAIGN="$HOME/jobman-dogfood-$(date -u +%Y%m%dT%H%M%SZ)"
export STATE_DIR="$CAMPAIGN/state"
export EVIDENCE_DIR="$CAMPAIGN/evidence"
export CAPTURE="$PWD/devel/dogfood/capture.sh"
export TREE_HELPER="$PWD/devel/dogfood/unix-process-tree.sh"
mkdir -m 700 -p "$STATE_DIR" "$EVIDENCE_DIR"
jm() { "$JOBMAN_BIN" --state-dir "$STATE_DIR" "$@"; }
capture() { "$CAPTURE" "$EVIDENCE_DIR" "$@"; }
printf '%s\n' "CAMPAIGN=$CAMPAIGN" "STATE_DIR=$STATE_DIR" \
  "JOBMAN_BIN=$JOBMAN_BIN"
```

`capture LABEL COMMAND ...` records arguments, stdout, stderr, UTC times, and
exit status. Labels must be unique. Never pass real secrets to `capture`.

Initial checks:

```sh
capture version "$JOBMAN_BIN" --version
capture config "$JOBMAN_BIN" --state-dir "$STATE_DIR" config validate
capture origins "$JOBMAN_BIN" --state-dir "$STATE_DIR" config show --origins
capture doctor "$JOBMAN_BIN" --state-dir "$STATE_DIR" doctor --json
{
  uname -a
  uname -m
  id
  df "$STATE_DIR"
} >"$EVIDENCE_DIR/platform.txt"
git rev-parse HEAD >"$EVIDENCE_DIR/release-commit.txt"
```

On Linux, also record `cat /etc/os-release` and `df -T "$STATE_DIR"`. On macOS:

```sh
sw_vers >>"$EVIDENCE_DIR/platform.txt"
diskutil info "$(df "$STATE_DIR" | awk 'END {print $1}')" \
  >>"$EVIDENCE_DIR/platform.txt"
```

Check:

- version and commit are the intended release candidate;
- configuration is valid;
- schema is supported, store health is good, and foreign-key violations are
  zero; and
- state permissions are private.

Mount a disposable network filesystem and confirm rejection:

```sh
NETWORK_STATE_DIR=/mounted/network/path/jobman-dogfood
if "$JOBMAN_BIN" --state-dir "$NETWORK_STATE_DIR" doctor --json; then
  echo 'FAIL: network state directory accepted' >&2
  exit 1
fi
```

### 2. Verify the artifact

Download the checksum manifest, Sigstore bundle, provenance, and artifact into
an empty directory. Replace `<version>`, OS, and architecture:

```sh
cosign verify-blob \
  --bundle "jobman_<version>_checksums.txt.sigstore.json" \
  --certificate-identity \
    'https://github.com/ryancswallace/Jobman/.github/workflows/release.yml@refs/heads/main' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "jobman_<version>_checksums.txt"
gh attestation verify --owner ryancswallace \
  "jobman_<version>_<os>_<arch>.tar.gz"
slsa-verifier verify-artifact \
  --provenance-path jobman.intoto.jsonl \
  --source-uri github.com/ryancswallace/Jobman \
  --source-branch main \
  "jobman_<version>_<os>_<arch>.tar.gz"
```

Linux checksum:

```sh
sha256sum --check "jobman_<version>_checksums.txt" --ignore-missing
```

macOS checksum:

```sh
grep "jobman_<version>_darwin_<arch>.tar.gz" \
  "jobman_<version>_checksums.txt" |
  shasum -a 256 -c
```

Extract and inspect:

```sh
mkdir "$CAMPAIGN/archive"
tar -xzf "jobman_<version>_<os>_<arch>.tar.gz" -C "$CAMPAIGN/archive"
"$CAMPAIGN/archive/jobman" --version
test -s "$CAMPAIGN/archive/docs/manpage/jobman.1"
test -s "$CAMPAIGN/archive/docs/completions/bash/jobman"
test -s "$CAMPAIGN/archive/docs/completions/zsh/_jobman"
"$CAMPAIGN/archive/jobman" config validate \
  "$CAMPAIGN/archive/etc/jobman/jobman.yml"
```

### 3. Lifecycle and terminal disconnect

In terminal A:

```sh
GATE="$CAMPAIGN/disconnect"
rm -f "$GATE"
DISCONNECT_ID=$(jm run -- sh -c '
  printf "stdout-line\nstdout-fragment"
  printf "stderr-line\nstderr-fragment" >&2
  while [ ! -e "$1" ]; do sleep 1; done
' dogfood "$GATE")
printf 'DISCONNECT_ID=%s\n' "$DISCONNECT_ID"
exit
```

Close terminal A. In terminal B, restore the campaign variables, set the
recorded ID, then run:

```sh
DISCONNECT_ID=replace-with-job-id
GATE="$CAMPAIGN/disconnect"
jm status "$DISCONNECT_ID"
touch "$GATE"
jm wait "$DISCONNECT_ID"
jm show --json "$DISCONNECT_ID" >"$EVIDENCE_DIR/disconnect.json"
jm logs --stream stdout --raw "$DISCONNECT_ID" >"$CAMPAIGN/actual.stdout"
jm logs --stream stderr --raw "$DISCONNECT_ID" >"$CAMPAIGN/actual.stderr"
printf 'stdout-line\nstdout-fragment' | cmp - "$CAMPAIGN/actual.stdout"
printf 'stderr-line\nstderr-fragment' | cmp - "$CAMPAIGN/actual.stderr"
```

Repeat through SSH. Kill or disconnect the first SSH client without ending the
remote OS user session. Release the gate from an independent SSH connection.

Check:

- the job survives terminal and SSH disconnects;
- no process retains the closed terminal;
- status reaches `success`; and
- raw output is byte-for-byte exact.

Exercise terminal outcomes:

```sh
OK_ID=$(jm run -- sh -c 'exit 0')
FAIL_ID=$(jm run -- sh -c 'exit 23')
CANCEL_ID=$(jm run -- sleep 300)
TIMEOUT_ID=$(jm run --run-timeout 1s -- sleep 300)
START_ID=$(jm run -- /definitely/missing/jobman-command)
FRAGMENT_ID=$(jm run -- sh -c 'printf final-fragment')
jm cancel "$CANCEL_ID"
for id in "$OK_ID" "$FAIL_ID" "$CANCEL_ID" "$TIMEOUT_ID" \
  "$START_ID" "$FRAGMENT_ID"; do
  jm wait "$id" || :
  jm show --json "$id" >"$EVIDENCE_DIR/lifecycle-$id.json"
done
test "$(jm logs --stream stdout --raw "$FRAGMENT_ID")" = final-fragment
```

Check each documented outcome, reason, exit fact, and final fragment.

### 4. Process-tree control

Graceful cancellation, pause, and resume:

```sh
TREE_CASE="$CAMPAIGN/tree-graceful"
mkdir "$TREE_CASE"
TREE_ID=$(jm run --stop-grace 5s -- \
  "$TREE_HELPER" graceful "$TREE_CASE/pids" "$TREE_CASE/progress")
for attempt in 1 2 3 4 5; do
  test -f "$TREE_CASE/pids" &&
    test "$(wc -l <"$TREE_CASE/pids")" -eq 3 && break
  sleep 1
done
if ! test -f "$TREE_CASE/pids" ||
    ! test "$(wc -l <"$TREE_CASE/pids")" -eq 3; then
  cat "$TREE_CASE/pids" 2>/dev/null || :
  jm cancel "$TREE_ID"
  jm wait "$TREE_ID"
  echo 'FAIL: helper did not record three processes' >&2
  exit 1
fi
jm pause "$TREE_ID"
sleep 2
PAUSED_LINES=$(wc -l <"$TREE_CASE/progress")
sleep 3
test "$(wc -l <"$TREE_CASE/progress")" -eq "$PAUSED_LINES"
jm resume "$TREE_ID"
sleep 3
test "$(wc -l <"$TREE_CASE/progress")" -gt "$PAUSED_LINES"
jm cancel "$TREE_ID"
jm wait "$TREE_ID"
while read -r role pid; do
  ! kill -0 "$pid" 2>/dev/null ||
    { echo "FAIL: $role PID $pid survived" >&2; exit 1; }
done <"$TREE_CASE/pids"
```

Forced escalation and cancellation while paused:

```sh
for mode in stubborn cancel-paused; do
  TREE_CASE="$CAMPAIGN/tree-$mode"
  mkdir "$TREE_CASE"
  TREE_ID=$(jm run --stop-grace 2s -- \
    "$TREE_HELPER" stubborn "$TREE_CASE/pids" "$TREE_CASE/progress")
  sleep 3
  test "$mode" != cancel-paused || jm pause "$TREE_ID"
  jm cancel "$TREE_ID"
  jm wait "$TREE_ID"
  while read -r role pid; do
    ! kill -0 "$pid" 2>/dev/null ||
      { echo "FAIL: $role PID $pid survived" >&2; exit 1; }
  done <"$TREE_CASE/pids"
  jm show --json "$TREE_ID" >"$EVIDENCE_DIR/tree-$mode.json"
done
```

Check the parent, child, and grandchild all stop. Use recorded process identity,
not PID alone, to rule out signalling a reused PID.

### 5. Scheduling, dependencies, and concurrency

Run this minimum matrix:

- retryable exit, start failure, timeout, and nonretryable exit;
- constant, linear, and exponential backoff; maximum delay and jitter bounds;
- run, success, and failure limits;
- every dependency predicate, multiple prerequisites, and an impossible
  dependency;
- time, delay, file, and executable waits, including abort deadlines;
- global and named-pool limits, multi-slot jobs, FIFO/bounded bypass;
- pause/resume while waiting, queued, and backing off; and
- `config apply` plus removal of an unused pool.

Core recipes:

```sh
RETRY_ID=$(jm run --retries 3 --retryable-exit-code 17 \
  --retry-delay 250ms --retry-backoff exponential --retry-max-delay 1s -- \
  sh -c 'exit 17')
TIMEOUT_RETRY_ID=$(jm run --retries 1 --retry-timeouts \
  --run-timeout 250ms --retry-delay 100ms -- sleep 5)
START_RETRY_ID=$(jm run --retries 1 --retry-start-failures \
  --retry-delay 100ms -- /definitely/missing/jobman-command)
for id in "$RETRY_ID" "$TIMEOUT_RETRY_ID" "$START_RETRY_ID"; do
  jm wait "$id" || :
  jm show --json "$id" >"$EVIDENCE_DIR/retry-$id.json"
done

SUCCESS_ID=$(jm run -- sh -c 'exit 0')
FAILED_ID=$(jm run -- sh -c 'exit 9')
jm wait "$SUCCESS_ID"
jm wait "$FAILED_ID" || :
for predicate in success failed finish; do
  id=$(jm run "--after-$predicate" \
    "$(test "$predicate" = success && echo "$SUCCESS_ID" || echo "$FAILED_ID")" \
    -- sh -c 'exit 0')
  jm wait "$id"
done
IMPOSSIBLE_ID=$(jm run --after-success "$FAILED_ID" -- sh -c 'exit 0')
jm wait "$IMPOSSIBLE_ID" || :
jm show --json "$IMPOSSIBLE_ID" >"$EVIDENCE_DIR/dependency-impossible.json"
```

Named-pool admission:

```sh
POLICY_CONFIG="$CAMPAIGN/policy.yml"
cat >"$POLICY_CONFIG" <<'YAML'
schema_version: 1
concurrency:
  max_active_slots: 3
  pools:
    dogfood: 2
YAML
"$JOBMAN_BIN" --state-dir "$STATE_DIR" --config "$POLICY_CONFIG" config apply
POOL_A=$("$JOBMAN_BIN" --state-dir "$STATE_DIR" --config "$POLICY_CONFIG" \
  run --pool dogfood --slots 2 -- sleep 10)
POOL_B=$("$JOBMAN_BIN" --state-dir "$STATE_DIR" --config "$POLICY_CONFIG" \
  run --pool dogfood --slots 1 -- sleep 10)
sleep 2
jm show --json "$POOL_A" >"$EVIDENCE_DIR/pool-active.json"
jm show --json "$POOL_B" >"$EVIDENCE_DIR/pool-queued.json"
jm cancel "$POOL_A"
jm wait "$POOL_B"
```

Concurrent readers and cancellation:

```sh
RACE_ID=$(jm run -- sleep 300)
sleep 2
jm list --json >"$EVIDENCE_DIR/concurrent-list.json" & p1=$!
jm show --json "$RACE_ID" >"$EVIDENCE_DIR/concurrent-show.json" & p2=$!
jm doctor --json >"$EVIDENCE_DIR/concurrent-doctor.json" & p3=$!
jm cancel "$RACE_ID" >"$EVIDENCE_DIR/concurrent-cancel.txt" & p4=$!
wait "$p1" "$p2" "$p3" "$p4"
jq -e . "$EVIDENCE_DIR"/concurrent-*.json >/dev/null
```

Any invalid JSON, impossible transition, database-busy loop, or reader-induced
mutation blocks release.

### 6. Logs, live input, configuration, and redaction

Binary logs and rotation:

```sh
dd if=/dev/urandom of="$CAMPAIGN/expected.bin" bs=1024 count=16
LOG_ID=$(jm run --log-segment-bytes 1024 --log-segments 32 -- \
  cat "$CAMPAIGN/expected.bin")
jm wait "$LOG_ID"
jm logs --stream stdout --raw "$LOG_ID" >"$CAMPAIGN/actual.bin"
cmp "$CAMPAIGN/expected.bin" "$CAMPAIGN/actual.bin"

FOLLOW_ID=$(jm run -- sh -c \
  'for i in 1 2 3 4 5; do echo "line-$i"; sleep 1; done')
jm logs --follow --stream stdout --raw "$FOLLOW_ID" \
  >"$EVIDENCE_DIR/follow.txt"
test "$(wc -l <"$EVIDENCE_DIR/follow.txt")" -eq 5
```

Live input from two clients:

```sh
dd if=/dev/urandom of="$CAMPAIGN/a.bin" bs=4096 count=8
dd if=/dev/urandom of="$CAMPAIGN/b.bin" bs=4096 count=8
INPUT_ID=$(jm run --stdin live -- cat)
jm input "$INPUT_ID" <"$CAMPAIGN/a.bin" & a=$!
jm input "$INPUT_ID" <"$CAMPAIGN/b.bin" & b=$!
wait "$a" "$b"
find "$STATE_DIR" -type s -ls >"$EVIDENCE_DIR/input-sockets.txt" || :
jm input --eof "$INPUT_ID"
! jm input --eof "$INPUT_ID"
jm wait "$INPUT_ID"
jm logs --stream stdout --raw "$INPUT_ID" >"$CAMPAIGN/input.bin"
cat "$CAMPAIGN/a.bin" "$CAMPAIGN/b.bin" >"$CAMPAIGN/ab.bin"
cat "$CAMPAIGN/b.bin" "$CAMPAIGN/a.bin" >"$CAMPAIGN/ba.bin"
cmp "$CAMPAIGN/input.bin" "$CAMPAIGN/ab.bin" ||
  cmp "$CAMPAIGN/input.bin" "$CAMPAIGN/ba.bin"
```

Check socket ownership and mode; group/other access must be absent. Confirm no
TCP listener appears. Rerun the job and verify it receives a fresh endpoint.

Malformed configuration must fail validation without disabling emergency
commands:

```sh
cat >"$CAMPAIGN/bad.yml" <<'YAML'
schema_version: 1
unknown_v1_key: reject-me
YAML
! "$JOBMAN_BIN" --state-dir "$STATE_DIR" --config "$CAMPAIGN/bad.yml" \
  config validate
for command in list doctor; do
  "$JOBMAN_BIN" --state-dir "$STATE_DIR" --config "$CAMPAIGN/bad.yml" "$command"
done
for command in status show logs; do
  "$JOBMAN_BIN" --state-dir "$STATE_DIR" --config "$CAMPAIGN/bad.yml" \
    "$command" "$OK_ID"
done
EMERGENCY_ID=$(jm run -- sleep 300)
"$JOBMAN_BIN" --state-dir "$STATE_DIR" --config "$CAMPAIGN/bad.yml" \
  cancel "$EMERGENCY_ID"
```

Also verify:

- CLI, environment, explicit file, user file, and defaults have documented
  precedence and origins;
- trusted-project restrictions and unknown-key rejection;
- `config apply` affects `run`, `rerun`, and policy cleanup; and
- `clean --all` and `clean --older-than` do not gain configuration authority.

Use unique fake values for `token`, `password`, and a redaction pattern.
Confirm they never occur in diagnostics, JSON, notifier diagnostics, or errors:

```sh
export JOBMAN_DOGFOOD_TOKEN=dogfood-token-7f8f6018
export JOBMAN_DOGFOOD_PASSWORD=dogfood-password-c24ac7b1
export JOBMAN_DOGFOOD_PATTERN=private-8675309
cat >"$CAMPAIGN/redaction.yml" <<'YAML'
schema_version: 1
secrets:
  token: env:JOBMAN_DOGFOOD_TOKEN
  password: env:JOBMAN_DOGFOOD_PASSWORD
redaction:
  names: [token, password]
  patterns: ['private-[0-9]+']
YAML
REDACT_ID=$("$JOBMAN_BIN" --state-dir "$STATE_DIR" \
  --config "$CAMPAIGN/redaction.yml" run \
  --name "$JOBMAN_DOGFOOD_PATTERN" \
  --secret-env TOKEN=token --secret-env PASSWORD=password -- \
  printf %s "$JOBMAN_DOGFOOD_PATTERN")
jm wait "$REDACT_ID"
"$JOBMAN_BIN" --state-dir "$STATE_DIR" --config "$CAMPAIGN/redaction.yml" \
  show --json "$REDACT_ID" >"$EVIDENCE_DIR/redaction.json"
grep -F '[REDACTED]' "$EVIDENCE_DIR/redaction.json" >/dev/null
! grep -R -F -e "$JOBMAN_DOGFOOD_TOKEN" \
  -e "$JOBMAN_DOGFOOD_PASSWORD" -e "$JOBMAN_DOGFOOD_PATTERN" "$EVIDENCE_DIR"
test "$(jm logs --stream stdout --raw "$REDACT_ID")" = \
  "$JOBMAN_DOGFOOD_PATTERN"
```

Raw target logs intentionally remain outside diagnostic redaction.

### 7. Notifications and crash recovery

Use the checked-in command notifier first:

```sh
NOTIFIER="$PWD/devel/dogfood/command-notifier.sh"
EVENTS="$CAMPAIGN/notifier-events"
mkdir "$EVENTS"
cat >"$CAMPAIGN/notifiers.yml" <<YAML
schema_version: 1
notifiers:
  audit:
    type: command
    events: [job_succeeded]
    timeout: 5s
    retry: {max_attempts: 1, delay: 10ms, max_delay: 10ms}
    command:
      command: ["$NOTIFIER", success, "$EVENTS/audit"]
      output_limit: 4KiB
  failing:
    type: command
    events: [job_succeeded]
    timeout: 1s
    retry: {max_attempts: 2, delay: 100ms, max_delay: 100ms}
    command:
      command: ["$NOTIFIER", failure, "$EVENTS/failing"]
      output_limit: 32B
YAML
for notifier in audit failing; do
  id=$("$JOBMAN_BIN" --state-dir "$STATE_DIR" \
    --config "$CAMPAIGN/notifiers.yml" run --notify "$notifier" -- \
    sh -c 'exit 0')
  jm wait "$id"
  sleep 3
  jm show --json "$id" >"$EVIDENCE_DIR/notifier-$notifier.json"
done
for payload in "$EVENTS"/*/*.event.json; do jq -e . "$payload" >/dev/null; done
```

Check one successful attempt, two bounded failures, 32-byte diagnostic
truncation, valid v1 payloads, unique event IDs, and unchanged job outcomes.

Then test recovery:

1. Configure `command-notifier.sh slow-once` with a five-minute timeout.
2. Submit a successful job.
3. Wait for its `.claimed` marker.
4. Kill the recorded supervisor and notifier PIDs.
5. Wait longer than the 15-second lease.
6. Run `jm doctor --repair`.
7. Confirm one durable completed attempt and stable idempotency key.

Repeat success, timeout, retry, and non-success response cases against
operator-controlled SMTP and HTTPS capture services. Record sanitized headers,
timestamps, statuses, truncation, and attempt counts. Never use production
credentials or endpoints.

### 8. Cleanup, backup, and upgrade

Cleanup must default to dry-run:

```sh
CLEAN_ID=$(jm run -- sh -c 'printf retained-log')
jm wait "$CLEAN_ID"
jm clean "$CLEAN_ID" --older-than 0s >"$EVIDENCE_DIR/clean-dry-run.txt"
test "$(jm logs --stream stdout --raw "$CLEAN_ID")" = retained-log
jm clean "$CLEAN_ID" --older-than 0s --dry-run=false --force \
  >"$EVIDENCE_DIR/clean-forced.txt"
! jm logs --stream stdout --raw "$CLEAN_ID"
! jm show --json "$CLEAN_ID"
! jm list --all | grep -F "$CLEAN_ID"
```

Also confirm active admissions, unresolved dependencies, and pending
notifications block deletion. Verify log age, log tombstones, and metadata
retention.

Create a backup:

```sh
BACKUP_DB="$CAMPAIGN/pre-upgrade.db"
jm doctor --backup "$BACKUP_DB"
chmod 600 "$BACKUP_DB"
if command -v sha256sum >/dev/null; then
  sha256sum "$BACKUP_DB" >"$EVIDENCE_DIR/pre-upgrade.sha256"
else
  shasum -a 256 "$BACKUP_DB" >"$EVIDENCE_DIR/pre-upgrade.sha256"
fi
```

For every supported prior schema fixture and the latest released binary:

1. Copy it to a fresh state directory.
2. Record the old binary's `doctor --json`.
3. Create representative jobs, logs, dependencies, and notifications.
4. Open the state with the release candidate.
5. Confirm a pre-migration backup, healthy store, matching counts, and intact
   logs.
6. Restore into a third directory using [UPGRADING.md](UPGRADING.md).
7. Make `backups/` unwritable on another copy; confirm migration fails and the
   database digest is unchanged.

Never restore over the original state.

### 9. Native installation and endurance

Test only packages native to the disposable host.

Linux:

```sh
sudo apt install ./jobman_<version>_linux_<arch>.deb
# or: sudo dnf install ./jobman_<version>_linux_<arch>.rpm
# or: sudo apk add --allow-untrusted ./jobman_<version>_linux_<arch>.apk
jobman --version
jobman config validate /etc/jobman/jobman.yml
man jobman
```

Also install through each public Cloudsmith repository on disposable hosts
with the matching package manager:

```sh
# Debian or Ubuntu
curl -1sLf \
  'https://dl.cloudsmith.io/public/jobman/stable/cfg/setup/bash.deb.sh' |
  sudo -E bash
sudo apt update
sudo apt install jobman

# Fedora, RHEL, Rocky Linux, AlmaLinux, or Amazon Linux
curl -1sLf \
  'https://dl.cloudsmith.io/public/jobman/stable/cfg/setup/bash.rpm.sh' |
  sudo -E bash
sudo dnf install jobman

# Alpine Linux
sudo apk add --no-cache bash curl
curl -1sLf \
  'https://dl.cloudsmith.io/public/jobman/stable/cfg/setup/bash.alpine.sh' |
  sudo -E bash
sudo apk add jobman
```

On Fedora 44 or later, Cloudsmith's generated repository must not force the
retired `/etc/pki/tls/certs/ca-bundle.crt` path. Treat that generator defect as
an external publishing blocker; do not disable TLS verification.

macOS:

```sh
brew install ryancswallace/tap/jobman
jobman --version
jobman config validate "$(brew --prefix)/etc/jobman/jobman.yml"
man jobman
```

For every package:

- start a new Bash and Zsh session; `jobman ru<Tab>` completes to `jobman run`;
- verify man pages, sample configuration, and private writable user state;
- upgrade, uninstall, and reinstall with the native package manager;
- confirm uninstall preserves user state and follows configuration-preservation
  rules; and
- on macOS, record any Gatekeeper behavior. Never bypass quarantine as an
  acceptance step.

Verify the signed container on Linux:

```sh
DOCKER_CONFIG="$CAMPAIGN/docker-config" docker pull \
  "ghcr.io/ryancswallace/jobman:<version>"
cosign verify \
  --certificate-identity \
    'https://github.com/ryancswallace/Jobman/.github/workflows/release.yml@refs/heads/main' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "ghcr.io/ryancswallace/jobman:<version>"
docker run --rm --init "ghcr.io/ryancswallace/jobman:<version>" --version
```

Check anonymous pull, unprivileged identity, persistent state, signal handling,
and a derived image containing a target command.

Run a 24-hour soak on each OS:

```sh
JOBMAN_BIN="$JOBMAN_BIN" ./devel/dogfood/soak.sh \
  "$CAMPAIGN/soak-state" "$EVIDENCE_DIR/soak" 86400
```

Sample CPU, memory, process tree, file descriptors, database/WAL/log sizes, and
command latency at fixed intervals. Exercise retries, timeouts, rotation, live
input, notifications, cleanup dry-runs, and `doctor`. Unbounded growth, leaked
processes, or unexplained errors block release.

## Windows

Run this procedure in native PowerShell—not WSL, MSYS2, or a Linux container.

### 1. Start a campaign

```powershell
Set-Location C:\path\to\jobman-release-commit
$JobmanBin = 'C:\absolute\path\to\jobman.exe'
$Campaign = Join-Path $HOME (
  'jobman-dogfood-' + (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
)
$StateDir = Join-Path $Campaign 'state'
$EvidenceDir = Join-Path $Campaign 'evidence'
New-Item -ItemType Directory -Force $StateDir, $EvidenceDir | Out-Null
function jm { & $JobmanBin --state-dir $StateDir @args }
Write-Host "Campaign=$Campaign"
Write-Host "StateDir=$StateDir"
```

Initial checks:

```powershell
& $JobmanBin --version |
  Tee-Object (Join-Path $EvidenceDir 'version.txt')
jm config validate |
  Tee-Object (Join-Path $EvidenceDir 'config.txt')
jm config show --origins |
  Tee-Object (Join-Path $EvidenceDir 'origins.json')
jm doctor --json |
  Tee-Object (Join-Path $EvidenceDir 'doctor.json')
Get-ComputerInfo | Out-File (Join-Path $EvidenceDir 'platform.txt')
Get-Volume | Out-File (Join-Path $EvidenceDir 'volumes.txt')
git rev-parse HEAD | Out-File (Join-Path $EvidenceDir 'release-commit.txt')
```

Check version, schema, health, foreign keys, ACLs, and local filesystem. Map a
disposable network share and confirm `doctor --json` rejects it.

### 2. Verify the ZIP

Verify the checksum manifest, Sigstore bundle, attestation, and provenance as
in the Unix artifact procedure, using
`jobman_<version>_windows_<arch>.zip`. Then:

```powershell
$Archive = 'C:\path\to\jobman_<version>_windows_<arch>.zip'
$Extracted = Join-Path $Campaign 'archive'
Expand-Archive $Archive $Extracted
& (Join-Path $Extracted 'jobman.exe') --version
& (Join-Path $Extracted 'jobman.exe') config validate `
  (Join-Path $Extracted 'etc\jobman\jobman.yml')
Test-Path (Join-Path $Extracted 'docs\completions\powershell\jobman.ps1')
```

Dot-source the packaged completion in a disposable PowerShell process and test
completion. Confirm extraction creates no machine-wide state; Windows releases
are portable ZIPs, not MSI installers.

### 3. Lifecycle and terminal disconnect

In PowerShell window A:

```powershell
$Gate = Join-Path $Campaign 'disconnect'
$Id = jm run --wait-file $Gate -- powershell.exe -NoProfile -Command `
  '[Console]::Out.Write("windows-stdout"); [Console]::Error.Write("windows-stderr")'
Write-Host "DISCONNECT_ID=$Id"
```

Close window A. In window B, restore the same campaign values:

```powershell
$Id = 'replace-with-job-id'
$Gate = Join-Path $Campaign 'disconnect'
jm status $Id
New-Item -ItemType File -Force $Gate | Out-Null
jm wait $Id
jm show --json $Id |
  Tee-Object (Join-Path $EvidenceDir 'disconnect.json')
jm logs --stream stdout --raw $Id
jm logs --stream stderr --raw $Id
```

Repeat through native OpenSSH using two independent clients. Check exact
stdout/stderr, successful completion, and no retained terminal handles.

Also run and inspect:

- exit 0 and nonzero;
- cancellation;
- run timeout;
- missing executable; and
- output without a trailing newline.

### 4. Process-tree control

```powershell
$TreeId = jm run --stop-grace 2s -- powershell.exe -NoProfile -Command `
  '$p = Start-Process powershell.exe -ArgumentList "-NoProfile","-Command","Start-Sleep 300" -PassThru; Write-Output $PID; Write-Output $p.Id; Wait-Process $p.Id'
Start-Sleep 2
$PidFile = Join-Path $EvidenceDir 'tree-pids.txt'
jm logs --stream stdout --raw $TreeId | Tee-Object $PidFile
jm pause $TreeId
Start-Sleep 3
jm resume $TreeId
jm cancel $TreeId
jm wait $TreeId
Get-Content $PidFile | ForEach-Object {
  if (Get-Process -Id ([int]$_) -ErrorAction SilentlyContinue) {
    throw "PID $_ survived cancellation"
  }
}
```

Repeat while paused and with a descendant that ignores graceful termination.
Using Process Explorer or an approved equivalent, record Job Object membership
and handle counts. Best-effort console break may fail; forced Job Object
termination must stop the entire tree after the grace period.

### 5. Functional matrix

Run the same behavioral matrix as Linux/macOS:

- retry and backoff modes, limits, dependency predicates, waits, and aborts;
- global/named-pool admission, multiple slots, FIFO/bounded bypass;
- pause/resume in waiting, queued, running, and backoff states;
- concurrent `list`, `show`, `logs`, `doctor`, and `cancel`;
- binary/rotated/interleaved logs and follow-to-completion;
- multi-client binary live input, EOF, repeated EOF, and rerun;
- configuration precedence, strict validation, emergency commands, and
  `config apply`;
- redaction canaries;
- command, SMTP, and HTTPS notifiers, including retry and crash recovery;
- cleanup dry-run/force, retention blockers, backup, restore, and upgrade.

Use native equivalents:

- `Start-Sleep` instead of `sleep`;
- `Get-Content -Raw FILE | ConvertFrom-Json` to validate JSON;
- `Get-FileHash -Algorithm SHA256` for digests;
- `Compare-Object` or byte arrays for exact comparisons;
- named-pipe ACL inspection instead of Unix socket modes; and
- `Get-NetTCPConnection -State Listen` to prove live input opens no TCP port.

Every final job must have the documented state, reason, attempts, and exit
facts. Invalid JSON, impossible transitions, database-busy loops, leaked
handles, or reader-induced mutations block release.

### 6. Endurance

Run the native 24-hour driver:

```powershell
.\devel\dogfood\soak.ps1 -JobmanBin $JobmanBin `
  -StateDir (Join-Path $Campaign 'soak-state') `
  -EvidenceDir (Join-Path $EvidenceDir 'soak') `
  -DurationSeconds 86400
```

At fixed intervals record CPU, memory, process tree, handles,
database/WAL/log sizes, and command latency. Unbounded growth, leaked
processes/handles, or unexplained errors block release.

## Final evidence and release decision

On Linux/macOS:

```sh
capture doctor-final "$JOBMAN_BIN" --state-dir "$STATE_DIR" doctor --json
find "$EVIDENCE_DIR" -type f -print | sort >"$EVIDENCE_DIR/manifest.txt"
grep -R -n -E '(token|password|authorization|secret)' "$EVIDENCE_DIR" \
  >"$CAMPAIGN/evidence-secret-review.txt" || :
tar -czf "$CAMPAIGN/evidence-review-required.tar.gz" \
  -C "$CAMPAIGN" evidence
```

On Windows:

```powershell
jm doctor --json | Out-File (Join-Path $EvidenceDir 'doctor-final.json')
Get-ChildItem -Recurse -File $EvidenceDir |
  Select-Object -ExpandProperty FullName |
  Out-File (Join-Path $EvidenceDir 'manifest.txt')
Get-ChildItem -Recurse -File $EvidenceDir |
  Select-String -Pattern 'token|password|authorization|secret' |
  Out-File (Join-Path $Campaign 'evidence-secret-review.txt')
Compress-Archive $EvidenceDir `
  (Join-Path $Campaign 'evidence-review-required.zip')
```

The secret scan is a human-review queue, not proof that evidence is safe. Do
not archive the state directory by default; job specifications and raw logs can
contain target data.

Release only when:

- every required automated and manual check passed on all supported OSes;
- artifact identity, signatures, provenance, and native installation passed;
- upgrades preserve state and failed migrations preserve the original;
- no process, handle, descriptor, database, or storage leak remains;
- notification failure never changes the job outcome;
- all deviations have resolved issue links; and
- an operator has reviewed the evidence for secrets.
