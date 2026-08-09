---
layout: default
title: Compare Jobman
parent: User guides
nav_order: 0
permalink: /guides/comparison/
---

# Jobman, `nohup`, tmux, and `systemd-run`

These tools overlap at one useful capability: they can keep a command from
being tied to the terminal that launched it. Their primary jobs are different,
so the best choice depends on what you need after the command starts.

## At a glance

| Concern | `nohup` | tmux | `systemd-run` | Jobman |
| --- | --- | --- | --- | --- |
| Primary abstraction | One command that ignores hangups | Persistent terminal sessions, windows, and panes | A transient unit managed by systemd | A durable local job with recorded runs |
| Background manager | None | A per-user tmux server while sessions exist | The system or user systemd manager | No shared Jobman daemon; one supervisor per active job |
| Survives terminal disconnect | Yes, when launched correctly | Yes | Yes, subject to the selected systemd manager and unit mode | Yes, within the operating-system user session |
| Full terminal reattachment | No | Yes | No | No; foreground pipes and optional bounded live input are available |
| Status after the command exits | Shell- and file-based | Session or pane state; no built-in durable job record | Unit state and journal, subject to manager and journal policy | Durable metadata, run history, outcomes, and versioned JSON |
| Retries and backoff | Wrapper script required | Wrapper script required | Restart behavior through systemd service properties | Native bounded retries, failure classification, backoff, and jitter |
| Timeouts | Another tool or wrapper required | Another tool or wrapper required | Unit properties | Native per-run and whole-job timeouts |
| Output | Redirection, often `nohup.out` | Terminal history or configured capture | Usually the systemd journal | Separate retained stdout and stderr with follow, rotation, and retention |
| Dependencies and capacity | Shell logic | Manual or scripted | systemd unit relationships and resource controls | Job dependencies, wait conditions, global slots, and named pools |
| Typical platforms | POSIX and Unix-like systems | Unix-like systems where tmux is available | Linux systems running systemd | Linux, macOS, and Windows |

The table compares built-in behavior, not everything that can be assembled
around each tool. A shell script can add retry policy to `nohup`; tmux can be
scripted; and systemd has a much broader service-management and cgroup model
than one table can show.

## Choose `nohup` for the smallest one-off solution

`nohup` runs a utility with the hangup signal ignored. GNU `nohup` also
redirects terminal-connected input and output according to its documented
rules. It is a good fit when all of these are true:

- the command only needs to outlive a terminal disconnect;
- shell job control or a PID is enough to find it while it runs;
- ordinary file redirection is enough for output; and
- you do not need durable status, retry policy, dependencies, or later
  lifecycle controls.

```sh
nohup ./import-data >import-data.log 2>&1 &
```

If that command grows a PID file, retry loop, timeout wrapper, status file, and
log-cleanup script, Jobman can replace those pieces with one recorded policy:

```sh
jobman run --name import-data --retries 3 --run-timeout 20m -- ./import-data
```

See the [GNU `nohup` manual page](https://man7.org/linux/man-pages/man1/nohup.1.html)
for the exact signal and redirection behavior.

## Choose tmux when the terminal is part of the work

tmux is a terminal multiplexer. Programs run inside panes managed by a tmux
server; sessions can be detached and later reattached from another terminal.
That makes tmux the better choice for:

- shells, editors, debuggers, dashboards, and other terminal interfaces;
- work where you need to see and resume the same terminal state;
- several interactive panes and windows; or
- collaborative or mobile workflows that attach from different terminals.

Jobman does not provide a pseudo-terminal or reconstruct an interactive screen.
Its live-input feature sends bytes to an opted-in detached process through a
private local endpoint; it is not a replacement for tmux.

If you use tmux only to keep a noninteractive command alive, Jobman trades
terminal reattachment for durable outcomes, structured status, retry and
timeout policy, separate logs, and lifecycle commands.

See the official [tmux getting-started guide](https://github.com/tmux/tmux/wiki/Getting-Started)
for its session, server, detach, and attach model.

## Choose `systemd-run` when systemd should own the process

`systemd-run` asks the system or per-user systemd manager to create a transient
service, scope, or timer unit. It is a strong fit on a Linux system that already
uses systemd when you want:

- system- or user-service-manager ownership;
- systemd unit relationships and restart semantics;
- cgroup-based resource accounting and controls;
- journal integration; or
- a transient unit that participates in the host's operational model.

This is broader host service management, not a daemonless model: the selected
systemd manager is the resident coordinator. User units also follow systemd's
login and lingering configuration, which administrators should choose
deliberately.

Jobman is a better fit when you want the same per-user job workflow on Linux,
macOS, and Windows, do not want to define or operate service-manager policy,
and value a job-oriented CLI with immutable retry, timeout, log, dependency,
and notification settings.

Read the
[`systemd-run` manual page](https://man7.org/linux/man-pages/man1/systemd-run.1.html),
systemd's [transient-unit settings](https://systemd.io/TRANSIENT-SETTINGS/),
and the
[transient control-group interface](https://systemd.io/CONTROL_GROUP_INTERFACE/)
before choosing unit properties or user-session behavior.

## Choose Jobman for a durable local job lifecycle

Jobman is the focused choice when the work is noninteractive and you want most
of the following without a continuously running shared daemon:

- an ID and durable status for every accepted job;
- bounded retries, backoff, jitter, repetition, and timeouts;
- raw retained logs that remain available after the process exits;
- dependencies, delayed starts, wait conditions, and local concurrency pools;
- wait, cancel, rerun, pause, resume, and notification workflows; and
- stable machine-readable output across Linux, macOS, and Windows.

Jobman is not the best choice for a fully interactive terminal, a system boot
service, or work coordinated across several hosts. Those boundaries are part
of its design, not features waiting to be enabled.

Ready to try it? [Install Jobman]({{ site.baseurl }}/getting-started/installation/)
and complete [Your first job]({{ site.baseurl }}/getting-started/first-job/).
For the product rationale, read [Why Jobman]({{ site.baseurl }}/why-jobman/).
