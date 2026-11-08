---
layout: default
title: Configuration
parent: User guides
nav_order: 1
permalink: /guides/configuration/
---

# Configuration and reusable policies

Jobman configuration is strict, versioned YAML. Use it for defaults, reusable
job specifications, wait conditions, profiles, concurrency pools, retention,
secret references, notifiers, and diagnostic redaction.

## Inspect before changing

```console
$ jobman config paths
$ jobman config validate
$ jobman config show
$ jobman config show --origins
```

`paths` lists every candidate and selected source. `validate` rejects unknown
or duplicate keys, invalid scalars, contradictory policy, and unresolved named
references. `show` emits the effective merged configuration as JSON;
`--origins` includes field provenance without resolving secret values.

## Precedence

From lowest to highest precedence, Jobman combines:

1. built-in defaults;
2. the platform system file;
3. the per-user file;
4. an opted-in project `.jobman.yml`;
5. the file selected by `--config PATH`;
6. documented `JOBMAN_` environment overrides; and
7. flags for the submitted job.

Maps merge recursively. Scalars and lists replace the lower-precedence value.
Project discovery is disabled unless its canonical root appears in
`trusted_project_roots` from a user-controlled or explicitly selected source.

## Start with a named job

```yaml
schema_version: 1

job_specs:
  report:
    command: [/usr/local/bin/build-report, --format, json]
    name: report
    working_directory: /srv/reports
    completion:
      max_runs: 4
      success_target: 1
      failure_limit: 4
      retryable_exit_codes: [1, 2]
      retry_timeouts: true
    delay:
      strategy: exponential
      initial: 5s
      max_delay: 1m
      base: 2
      jitter: 1s
    timeouts:
      run: 10m
      job: 1h
```

Validate and submit it:

```console
$ jobman config validate ./jobman.yml
$ jobman --config ./jobman.yml run --job-spec report
```

A direct command after `--` may replace the configured command while retaining
the selected policy. Repeat `--profile NAME` to apply explicit profiles in
argument order.

## Apply durable settings deliberately

Global and named-pool concurrency capacities are durable scheduler state.
`run` and `rerun` synchronize them before submission. Use this command to apply
the same settings without submitting a job:

```console
$ jobman config apply
```

Inspection, lifecycle-emergency commands, `doctor`, `clean JOB`, `clean --all`,
and explicit `clean --older-than` never apply configuration. They remain
available when a configuration file is malformed.

## Continue to the schema

The complete [configuration schema]({{ site.baseurl }}/reference/configuration/)
documents defaults, scalar forms, every registry, notifier constraints, and
safety rules. Download the commented
[sample configuration]({{ site.baseurl }}/assets/examples/jobman.yml) as a
starting point.

## Deployment integration

`jobman config schema` emits the native field schema. Use
`jobman config validate --isolated PATH` to validate a generated deployment file
without reading user, system, project, or environment configuration. Optional
`presentation` fields select organization text, help links, and a message of the
day; `jobman about` displays them without changing job or JSON output.

The separately packaged agent accepts strict YAML through `--config PATH` or
`JOBMAN_AGENT_CONFIG`. Use `jobman-agent config schema`, `config validate PATH`,
and `config show PATH` to inspect the native contract and provenance.
