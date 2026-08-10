# Shell completions

`make gen-completions` generates completion scripts for Bash, Zsh, and
PowerShell beneath this directory. GoReleaser includes them in portable archives
and installs the Bash and Zsh variants, together with their shell completion
runtimes, in native Linux packages. Debian packages use Zsh's
`vendor-completions` directory; RPM and APK packages use `site-functions`.

Start a new shell after installing a native package. An already-running Bash or
Zsh process may have cached the completions that were present when it started.

When Jobman's state directory is available, the generated scripts dynamically
complete job IDs and unique job names for command arguments and flags that
accept job selectors. ID prefixes offer every matching ID known to the bounded
job listing. Names are suggested only when they identify one listed job, so a
known ambiguous name is not offered. Completion lookup failures are silent and
do not fall back to filesystem paths.

Root completion also advertises the supported optional `diagnose` companion,
so `jobman dia<TAB>` completes to `jobman diagnose`. The first diagnosis
argument uses the same job ID and name completion. This static suggestion does
not search or execute programs on `PATH`; invoking it still requires an
installed `jobman-diagnose` executable. The companion remains responsible for
completion of its own flags and non-job values.

The generated files are ignored by Git because they are derived from the Cobra
command tree. Do not edit them directly; update the commands or the generator in
`devel/autocomplete/` instead.
