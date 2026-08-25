# Jobman shared protocol

This package contains the dependency-light, versioned contracts shared by
Jobman clients, agents, and Jobman Control. The package itself is declarative;
the pre-release named-host implementation lives in `internal/agent`, and none
of it changes Jobman's frozen standalone behavior.

`SealWorkload`, `SealJobRequest`, `SealEffectiveExecution`, and
`SealAgentAssignment` accept values, materialize documented defaults, normalize
unordered sets, validate semantic constraints, and return deterministic JSON
plus SHA-256 digests. The package also defines the separately validated
acceptance, launch-authorization, execution-event, desired-action,
acknowledgement, and process-result documents used after assignment delivery.
The schemas under `schema/` describe the wire forms. API clients written in
other languages must apply the same defaults and pass the conformance fixtures.

An `EffectiveExecution` pins a workload to durable job, run, execution, target,
and target-generation identities. An `AgentAssignment` is only a redeliverable
offer containing that effective specification. Receiving or decoding an
assignment does not authorize an agent to stage or launch work; the separate
durable acceptance handshake remains mandatory. A replay-stable
`LaunchAuthorization` is the only permission to create target-side effects.
Ordered, replay-safe `ExecutionEvent` documents report process start and
completion, while `DesiredAction` and `ActionAcknowledgement` documents make
cancellation intent durable on both sides of a lost connection.

Canonical documents are UTF-8 JSON with no insignificant whitespace and object
keys sorted lexicographically by their UTF-8 bytes. Typed numeric fields use
their normalized integer form. Numbers inside namespaced extension objects use
an exact normalized scientific-decimal form: no positive sign or insignificant
zeros, one digit before an optional decimal point, and a lowercase `e` only
when the exponent is nonzero. For example, `100`, `100.0`, `1e2`, and `0.01e4`
all become `1e2`. Negative zero becomes `0`; no binary floating-point rounding
is involved. Invalid UTF-8, duplicate object keys, excessive nesting, and
trailing JSON values are rejected.

The `v1alpha1` designation allows compatible design iteration before shared
mode is released. Once a schema version is stable, incompatible changes require
a new version rather than reinterpretation of stored documents.
