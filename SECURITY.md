# Security policy

## Supported versions

The newest patch of the current stable minor line receives all security fixes.
The immediately previous stable minor line receives Critical and High severity
fixes for 90 days after the next minor release. This limited overlap gives
operators time to upgrade without creating an open-ended backport obligation
for the maintainer.

| Version | Supported |
| --- | --- |
| Latest stable minor, newest patch | Yes |
| Previous stable minor | Critical/High for 90 days |
| Prereleases from a supported line | Best effort |
| Unreleased `main` snapshots | Best effort; no release artifacts |
| Older releases | No |

Support means that a validated vulnerability can receive a private fix,
coordinated disclosure, and a signed patch release. It does not guarantee an
SLA. Unsupported releases may receive public mitigation advice but should be
upgraded before a fix is expected. Platform support also requires the native
release-commit evidence listed in the [platform capability record].

## Diagnostic evidence and external commands

`jobman show evidence` excludes direct commands, paths, and environment names
by default and always excludes environment values, secret-reference
identifiers and values, input, notification destinations, and credentials.
The `--command`, `--paths`, and `--environment-names` controls explicitly admit
bounded, typed, configured-redaction-aware execution context.
Log content is collected only with `--logs tail`, is bounded, and is sanitized
with the configured redactor before the evidence digest is sealed. Target
output can still contain unknown credentials or malicious instructions; review
it before exporting or sharing it. Core Jobman does not send evidence to a
network service.

Core emits `configured_value_redaction_v1` only when a value-aware configured
redaction rule was active. The optional companion requires this capability in
addition to explicit profile and CLI approval before projecting log content to
a generator. It is evidence of policy activation, not a guarantee that target
output is secret-free.

Failure fingerprints are HMAC-SHA-256 values scoped to one state store. Their
32-byte key remains inside the private SQLite database and its backups; it is
never logged or exported. Evidence marks fingerprints and safe cross-job match
summaries `local_only`. `--similar N` is explicit and returns no job names,
specifications, paths, environment, logs, or notifier data from matching jobs.

An explicitly named unknown lowercase command can resolve to `jobman-NAME` on
the caller's `PATH`. Such an extension is native code with the invoking user's
authority, not a sandboxed plugin. Install only verified extensions, keep
untrusted directories out of `PATH`, or disable dispatch with
`--no-extensions` or `JOBMAN_NO_EXTENSIONS=1`. Built-ins cannot be shadowed,
and help or completion does not discover or execute extensions.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Prefer a private
[GitHub security advisory] so the report, discussion, and coordinated fix remain
confidential. If that is not possible, email Ryan Wallace at
<ryancswallace@gmail.com>.

Include the affected version, reproduction steps or a proof of concept,
potential impact, and any known mitigation. Remove unrelated credentials and
personal data.

You should receive an acknowledgement within seven days. The maintainer will
coordinate validation, remediation, release timing, and disclosure with the
reporter. Please allow a reasonable remediation period before publishing
details.

[GitHub security advisory]: https://github.com/ryancswallace/jobman/security/advisories/new
[platform capability record]: docs/design/PLATFORM_CAPABILITIES.md
