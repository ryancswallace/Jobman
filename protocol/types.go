package protocol

import (
	"encoding/json"
	"time"
)

const (
	// V1Alpha1 is the first portable shared-mode contract version.
	V1Alpha1 = "jobman/v1alpha1"
	// WorkloadKind identifies a portable workload document.
	WorkloadKind = "Workload"
	// JobRequestKind identifies a shared-mode job submission document.
	JobRequestKind = "JobRequest"
	// CollectionRequestKind identifies a bounded group of independent child
	// jobs. Array compilation is a target-side optimization and never changes
	// the child lifecycle contract.
	CollectionRequestKind = "CollectionRequest"
	// GraphRequestKind identifies an immutable directed acyclic graph of jobs.
	GraphRequestKind = "GraphRequest"
	// EffectiveExecutionKind identifies the immutable specification resolved by
	// Jobman Control for one execution.
	EffectiveExecutionKind = "EffectiveExecution"
	// AgentAssignmentKind identifies a redeliverable assignment envelope. An
	// assignment is intent only; agents must use the separate acceptance
	// protocol before performing side effects.
	AgentAssignmentKind = "AgentAssignment"
	// AgentAcceptanceKind identifies an agent's compare-and-swap request to
	// accept one offered execution.
	AgentAcceptanceKind = "AgentAcceptance"
	// LaunchAuthorizationKind identifies the control plane's durable permission
	// for exactly one accepted execution to begin target-side effects.
	LaunchAuthorizationKind = "LaunchAuthorization"
	// ExecutionEventKind identifies one ordered, replay-safe agent observation.
	ExecutionEventKind = "ExecutionEvent"
	// DesiredActionKind identifies durable control-plane intent for an accepted
	// execution.
	DesiredActionKind = "DesiredAction"
	// ActionAcknowledgementKind identifies an agent's durable observation of a
	// desired action.
	ActionAcknowledgementKind = "ActionAcknowledgement"
)

// Workload is an immutable, placement-independent declaration of work.
type Workload struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Metadata   WorkloadMetadata `json:"metadata"`
	Spec       WorkloadSpec     `json:"spec"`
}

// WorkloadMetadata carries human-facing workload metadata.
type WorkloadMetadata struct {
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// WorkloadSpec describes command, runtime, resources, artifacts, and policy.
type WorkloadSpec struct {
	Command          Command                    `json:"command"`
	WorkingDirectory string                     `json:"workingDirectory"`
	Environment      *Environment               `json:"environment,omitempty"`
	Resources        *Resources                 `json:"resources,omitempty"`
	Runtime          Runtime                    `json:"runtime"`
	Artifacts        *Artifacts                 `json:"artifacts,omitempty"`
	Policy           ExecutionPolicy            `json:"policy"`
	Requirements     *Requirements              `json:"requirements,omitempty"`
	Extensions       map[string]json.RawMessage `json:"extensions,omitempty"`
}

// Command is either a direct executable invocation or an explicit shell
// program. Exactly one form is valid.
type Command struct {
	Executable string        `json:"executable,omitempty"`
	Args       []string      `json:"args,omitempty"`
	Shell      *ShellCommand `json:"shell,omitempty"`
}

// ShellCommand makes shell interpretation explicit and capability-gated.
type ShellCommand struct {
	Capability string `json:"capability"`
	Script     string `json:"script"`
}

// Environment declares non-secret values, secret references, and an optional
// administrator-controlled baseline profile.
type Environment struct {
	Profile string            `json:"profile,omitempty"`
	Values  map[string]string `json:"values,omitempty"`
	Secrets []SecretBinding   `json:"secrets,omitempty"`
}

// SecretBinding exposes one late-bound secret reference to the workload.
type SecretBinding struct {
	Name     string         `json:"name"`
	Source   string         `json:"source"`
	ExposeAs SecretExposure `json:"exposeAs"`
}

// SecretExposure selects exactly one target-side exposure mechanism.
type SecretExposure struct {
	Environment string `json:"environment,omitempty"`
	File        string `json:"file,omitempty"`
}

// Resources contains portable resource intent rather than backend flags.
type Resources struct {
	CPU              int    `json:"cpu,omitempty"`
	Memory           string `json:"memory,omitempty"`
	GPU              int    `json:"gpu,omitempty"`
	Nodes            int    `json:"nodes,omitempty"`
	Tasks            int    `json:"tasks,omitempty"`
	TemporaryStorage string `json:"temporaryStorage,omitempty"`
	WallTime         string `json:"wallTime,omitempty"`
}

// Runtime selects native or container execution.
type Runtime struct {
	Kind      string            `json:"kind"`
	Container *ContainerRuntime `json:"container,omitempty"`
}

// ContainerRuntime contains portable container intent. Target policy resolves
// the concrete engine and enforces registries, mounts, users, and privileges.
type ContainerRuntime struct {
	Image      string `json:"image"`
	PullPolicy string `json:"pullPolicy"`
	Network    string `json:"network"`
}

// Artifacts declares immutable inputs and expected outputs.
type Artifacts struct {
	Inputs  []InputArtifact  `json:"inputs,omitempty"`
	Outputs []OutputArtifact `json:"outputs,omitempty"`
}

// InputArtifact maps a logical artifact to an input sandbox path.
type InputArtifact struct {
	Name     string `json:"name"`
	Source   string `json:"source"`
	Target   string `json:"target"`
	Checksum string `json:"checksum,omitempty"`
}

// OutputArtifact publishes one declared output path.
type OutputArtifact struct {
	Name        string `json:"name"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Required    bool   `json:"required"`
}

// ExecutionPolicy defines portable timeout, retry, and duplicate-risk policy.
type ExecutionPolicy struct {
	RunTimeout    string      `json:"runTimeout,omitempty"`
	Retry         RetryPolicy `json:"retry"`
	DuplicateRisk string      `json:"duplicateRisk"`
}

// RetryPolicy defines bounded Jobman-controlled runs.
type RetryPolicy struct {
	MaxRuns int    `json:"maxRuns"`
	Backoff string `json:"backoff,omitempty"`
}

// Requirements constrain targets without selecting one.
type Requirements struct {
	OperatingSystems []string `json:"operatingSystems,omitempty"`
	Architectures    []string `json:"architectures,omitempty"`
	Capabilities     []string `json:"capabilities,omitempty"`
}

// JobRequest binds a sealed workload to namespace metadata and placement.
type JobRequest struct {
	APIVersion string             `json:"apiVersion"`
	Kind       string             `json:"kind"`
	Metadata   JobRequestMetadata `json:"metadata"`
	Spec       JobRequestSpec     `json:"spec"`
}

// JobRequestMetadata identifies the namespace-scoped job.
type JobRequestMetadata struct {
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// JobRequestSpec contains an immutable workload binding and explicit
// placement intent.
type JobRequestSpec struct {
	Workload  WorkloadBinding `json:"workload"`
	Placement Placement       `json:"placement"`
}

// CollectionRequest groups explicit portable jobs beneath one namespace
// identity. Each item remains independently observable and cancellable.
type CollectionRequest struct {
	APIVersion string                    `json:"apiVersion"`
	Kind       string                    `json:"kind"`
	Metadata   CollectionRequestMetadata `json:"metadata"`
	Spec       CollectionRequestSpec     `json:"spec"`
}

// CollectionRequestMetadata identifies the namespace-scoped collection.
type CollectionRequestMetadata struct {
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// CollectionRequestSpec defines dispatch and failure policy.
type CollectionRequestSpec struct {
	Items         []CollectionItem `json:"items"`
	MaxActive     int              `json:"maxActive"`
	FailurePolicy string           `json:"failurePolicy"`
	ArrayPolicy   string           `json:"arrayPolicy"`
}

// CollectionItem is one explicit sealed child. No string interpolation or
// implicit matrix expansion occurs at execution time.
type CollectionItem struct {
	Name      string          `json:"name"`
	Workload  WorkloadBinding `json:"workload"`
	Placement Placement       `json:"placement"`
}

// GraphRequest declares a finite immutable dependency graph. Nodes remain
// ordinary independently observable jobs; graph edges only control readiness.
type GraphRequest struct {
	APIVersion string               `json:"apiVersion"`
	Kind       string               `json:"kind"`
	Metadata   GraphRequestMetadata `json:"metadata"`
	Spec       GraphRequestSpec     `json:"spec"`
}

// GraphRequestMetadata identifies one namespace-scoped graph.
type GraphRequestMetadata struct {
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// GraphRequestSpec contains immutable nodes, edges, and scheduling policy.
type GraphRequestSpec struct {
	Nodes             []GraphNode `json:"nodes"`
	Edges             []GraphEdge `json:"edges,omitempty"`
	MaxActive         int         `json:"maxActive"`
	UnsatisfiedPolicy string      `json:"unsatisfiedPolicy"`
}

// GraphNode is one independently sealed portable job.
type GraphNode struct {
	Name      string          `json:"name"`
	Workload  WorkloadBinding `json:"workload"`
	Placement Placement       `json:"placement"`
}

// GraphEdge gates a downstream node on an upstream terminal outcome.
// Predicate is success, failure, any-terminal, or outcomes. Outcomes is used
// only by the outcomes predicate.
type GraphEdge struct {
	From      string   `json:"from"`
	To        string   `json:"to"`
	Predicate string   `json:"predicate"`
	Outcomes  []string `json:"outcomes,omitempty"`
}

// WorkloadBinding couples canonical workload content to its semantic digest.
type WorkloadBinding struct {
	Digest   string   `json:"digest"`
	Document Workload `json:"document"`
}

// Placement selects a registered target and optional Slurm partition.
type Placement struct {
	Target    string `json:"target"`
	Partition string `json:"partition,omitempty"`
}

// EffectiveExecution is the immutable, target-resolved input to an agent.
// It deliberately excludes credentials and resolved secret values.
type EffectiveExecution struct {
	APIVersion string                     `json:"apiVersion"`
	Kind       string                     `json:"kind"`
	Metadata   EffectiveExecutionMetadata `json:"metadata"`
	Spec       EffectiveExecutionSpec     `json:"spec"`
}

// EffectiveExecutionMetadata binds an effective specification to durable
// Jobman lifecycle identities.
type EffectiveExecutionMetadata struct {
	ExecutionID string             `json:"executionId"`
	RunID       string             `json:"runId"`
	JobID       string             `json:"jobId"`
	Namespace   string             `json:"namespace"`
	SlurmArray  *SlurmArrayBinding `json:"slurmArray,omitempty"`
}

// SlurmArrayBinding records the immutable collection-to-scheduler task
// mapping selected by the control plane. Every task remains an independently
// accepted and authorized Jobman execution.
type SlurmArrayBinding struct {
	CollectionID string `json:"collectionId"`
	TaskIndex    int    `json:"taskIndex"`
	TaskCount    int    `json:"taskCount"`
	MaxParallel  int    `json:"maxParallel"`
}

// EffectiveExecutionSpec contains the sealed workload and resolved placement.
type EffectiveExecutionSpec struct {
	Workload       WorkloadBinding        `json:"workload"`
	Placement      EffectivePlacement     `json:"placement"`
	ArtifactStores []ArtifactStoreBinding `json:"artifactStores,omitempty"`
}

// ArtifactStoreBinding pins a logical store to an immutable target mapping.
// Physical local, NFS, or S3 configuration remains outside portable documents.
type ArtifactStoreBinding struct {
	Name    string `json:"name"`
	Version int64  `json:"version"`
}

// EffectivePlacement pins the execution to an immutable target generation.
type EffectivePlacement struct {
	TargetID           string `json:"targetId"`
	TargetGenerationID string `json:"targetGenerationId"`
	Target             string `json:"target"`
	Partition          string `json:"partition,omitempty"`
	ExecutionBackend   string `json:"executionBackend"`
}

// AgentAssignment is a redeliverable envelope for one effective execution.
// Receipt of this document never grants permission to launch work.
type AgentAssignment struct {
	APIVersion string                  `json:"apiVersion"`
	Kind       string                  `json:"kind"`
	Metadata   AgentAssignmentMetadata `json:"metadata"`
	Spec       AgentAssignmentSpec     `json:"spec"`
}

// AgentAssignmentMetadata identifies one stable delivery and its intended
// agent. The delivery ID is reused for every redelivery.
type AgentAssignmentMetadata struct {
	DeliveryID string `json:"deliveryId"`
	AgentID    string `json:"agentId"`
}

// AgentAssignmentSpec seals the effective execution with its digest.
type AgentAssignmentSpec struct {
	EffectiveExecutionDigest string             `json:"effectiveExecutionDigest"`
	EffectiveExecution       EffectiveExecution `json:"effectiveExecution"`
}

// AgentAcceptance requests durable ownership of one offered execution. It is
// safe to replay verbatim and never authorizes a different execution.
type AgentAcceptance struct {
	APIVersion string                  `json:"apiVersion"`
	Kind       string                  `json:"kind"`
	Metadata   AgentAcceptanceMetadata `json:"metadata"`
	Spec       AgentAcceptanceSpec     `json:"spec"`
}

// AgentAcceptanceMetadata binds acceptance to one stable delivery and agent.
type AgentAcceptanceMetadata struct {
	DeliveryID  string `json:"deliveryId"`
	ExecutionID string `json:"executionId"`
	AgentID     string `json:"agentId"`
}

// AgentAcceptanceSpec proves which immutable target generation and effective
// execution the agent committed to its local journal.
type AgentAcceptanceSpec struct {
	TargetGenerationID       string `json:"targetGenerationId"`
	EffectiveExecutionDigest string `json:"effectiveExecutionDigest"`
}

// LaunchAuthorization is returned only after acceptance is durable. Replaying
// the same acceptance returns the same authorization identity.
type LaunchAuthorization struct {
	APIVersion string                      `json:"apiVersion"`
	Kind       string                      `json:"kind"`
	Metadata   LaunchAuthorizationMetadata `json:"metadata"`
	Spec       LaunchAuthorizationSpec     `json:"spec"`
}

// LaunchAuthorizationMetadata identifies the accepted execution revision.
type LaunchAuthorizationMetadata struct {
	AuthorizationID string    `json:"authorizationId"`
	ExecutionID     string    `json:"executionId"`
	AgentID         string    `json:"agentId"`
	Revision        int64     `json:"revision"`
	AcceptedAt      time.Time `json:"acceptedAt"`
}

// LaunchAuthorizationSpec repeats the immutable facts an agent must verify
// before launching.
type LaunchAuthorizationSpec struct {
	TargetGenerationID       string `json:"targetGenerationId"`
	EffectiveExecutionDigest string `json:"effectiveExecutionDigest"`
}

// ExecutionEvent is an ordered target-side observation. Source sequence is
// monotonic per execution and makes redelivery idempotent.
type ExecutionEvent struct {
	APIVersion string                 `json:"apiVersion"`
	Kind       string                 `json:"kind"`
	Metadata   ExecutionEventMetadata `json:"metadata"`
	Spec       ExecutionEventSpec     `json:"spec"`
}

// ExecutionEventMetadata identifies the source and ordering of an event.
type ExecutionEventMetadata struct {
	EventID     string    `json:"eventId"`
	ExecutionID string    `json:"executionId"`
	AgentID     string    `json:"agentId"`
	Sequence    int64     `json:"sequence"`
	ObservedAt  time.Time `json:"observedAt"`
}

// ExecutionEventSpec contains one bounded lifecycle observation.
type ExecutionEventSpec struct {
	Type      string                `json:"type"`
	NativeID  string                `json:"nativeId,omitempty"`
	Scheduler *SchedulerObservation `json:"scheduler,omitempty"`
	Result    *ProcessResult        `json:"result,omitempty"`
	Artifacts []PublishedArtifact   `json:"artifacts,omitempty"`
}

// PublishedArtifact records immutable output metadata. Artifact bytes never
// transit through the control service.
type PublishedArtifact struct {
	Name         string `json:"name"`
	StoreName    string `json:"storeName"`
	StoreVersion int64  `json:"storeVersion"`
	ObjectKey    string `json:"objectKey"`
	ByteLength   int64  `json:"byteLength"`
	Checksum     string `json:"checksum"`
}

// SchedulerObservation records normalized scheduler evidence without exposing
// scheduler-specific command output as part of the portable contract.
type SchedulerObservation struct {
	Backend string `json:"backend"`
	State   string `json:"state"`
	Reason  string `json:"reason,omitempty"`
	Cluster string `json:"cluster,omitempty"`
}

// ProcessResult records process facts independently from the effective Jobman
// outcome. ExitCode is optional because signal and start failures have none.
type ProcessResult struct {
	Outcome     string `json:"outcome"`
	ExitCode    *int   `json:"exitCode,omitempty"`
	Signal      string `json:"signal,omitempty"`
	FailureCode string `json:"failureCode,omitempty"`
}

// DesiredAction is durable control intent polled by an agent.
type DesiredAction struct {
	APIVersion string                `json:"apiVersion"`
	Kind       string                `json:"kind"`
	Metadata   DesiredActionMetadata `json:"metadata"`
	Spec       DesiredActionSpec     `json:"spec"`
}

// DesiredActionMetadata identifies one versioned action delivery.
type DesiredActionMetadata struct {
	ActionID    string    `json:"actionId"`
	ExecutionID string    `json:"executionId"`
	AgentID     string    `json:"agentId"`
	Revision    int64     `json:"revision"`
	RequestedAt time.Time `json:"requestedAt"`
}

// DesiredActionSpec contains the requested target-side effect.
type DesiredActionSpec struct {
	Type string `json:"type"`
}

// ActionAcknowledgement records that an agent durably observed an action. It
// does not claim the requested native effect has completed.
type ActionAcknowledgement struct {
	APIVersion string                        `json:"apiVersion"`
	Kind       string                        `json:"kind"`
	Metadata   ActionAcknowledgementMetadata `json:"metadata"`
}

// ActionAcknowledgementMetadata binds acknowledgement to an action revision.
type ActionAcknowledgementMetadata struct {
	ActionID    string    `json:"actionId"`
	ExecutionID string    `json:"executionId"`
	AgentID     string    `json:"agentId"`
	Revision    int64     `json:"revision"`
	ObservedAt  time.Time `json:"observedAt"`
}

// SealedWorkload is normalized, validated canonical workload content.
type SealedWorkload struct {
	Document      Workload
	CanonicalJSON []byte
	Digest        string
}

// SealedJobRequest is normalized, validated canonical submission content.
type SealedJobRequest struct {
	Document       JobRequest
	CanonicalJSON  []byte
	RequestDigest  string
	WorkloadDigest string
}

// SealedCollectionRequest is normalized canonical collection content and the
// independently sealed workload digests of its ordered children.
type SealedCollectionRequest struct {
	Document        CollectionRequest
	CanonicalJSON   []byte
	RequestDigest   string
	WorkloadDigests []string
}

// SealedGraphRequest is normalized canonical graph content and its node
// workload digests.
type SealedGraphRequest struct {
	Document        GraphRequest
	CanonicalJSON   []byte
	RequestDigest   string
	WorkloadDigests []string
}

// SealedEffectiveExecution is normalized, validated effective execution
// content and its semantic digest.
type SealedEffectiveExecution struct {
	Document      EffectiveExecution
	CanonicalJSON []byte
	Digest        string
}

// SealedAgentAssignment is a normalized, validated assignment envelope.
type SealedAgentAssignment struct {
	Document                 AgentAssignment
	CanonicalJSON            []byte
	EffectiveExecutionDigest string
}

// SealedAgentAcceptance contains canonical acceptance content and its digest.
type SealedAgentAcceptance struct {
	Document      AgentAcceptance
	CanonicalJSON []byte
	Digest        string
}

// SealedExecutionEvent contains canonical event content and its digest.
type SealedExecutionEvent struct {
	Document      ExecutionEvent
	CanonicalJSON []byte
	Digest        string
}

// DecodeLimits bounds untrusted portable-contract decoding. Zero values use
// safe defaults.
type DecodeLimits struct {
	MaxBytes int64
	MaxDepth int
}
