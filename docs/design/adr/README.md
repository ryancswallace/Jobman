# Architecture decision records

Architecture decision records (ADRs) capture choices that are expensive to
reverse or whose rationale is not obvious from code. The
[design specification](../SPEC.md) remains authoritative for product behavior;
ADRs explain how and why the implementation intends to satisfy it.

| ADR | Status | Decision |
| --- | --- | --- |
| [0001](0001-per-job-supervisor.md) | Accepted | Use one detached supervisor process per active job. |
| [0002](0002-sqlite-metadata-and-filesystem-logs.md) | Accepted | Use pure-Go SQLite for metadata and private filesystem files for logs. |
| [0003](0003-external-subcommands.md) | Accepted | Discover optional `jobman-NAME` commands as direct external executables. |

## Statuses

- **Proposed**: ready for review but not yet an implementation commitment.
- **Accepted**: approved and governing implementation.
- **Superseded**: replaced by a later ADR, which must be linked.
- **Rejected**: considered but not selected.

Accepted ADRs are not immutable. A change is made by adding a superseding ADR,
preserving the history and consequences of both decisions.
