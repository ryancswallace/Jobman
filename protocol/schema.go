package protocol

import _ "embed"

var (
	//go:embed schema/workload-v1alpha1.schema.json
	workloadSchema []byte
	//go:embed schema/job-request-v1alpha1.schema.json
	jobRequestSchema []byte
	//go:embed schema/collection-request-v1alpha1.schema.json
	collectionRequestSchema []byte
	//go:embed schema/graph-request-v1alpha1.schema.json
	graphRequestSchema []byte
	//go:embed schema/effective-execution-v1alpha1.schema.json
	effectiveExecutionSchema []byte
	//go:embed schema/agent-assignment-v1alpha1.schema.json
	agentAssignmentSchema []byte
	//go:embed schema/agent-acceptance-v1alpha1.schema.json
	agentAcceptanceSchema []byte
	//go:embed schema/launch-authorization-v1alpha1.schema.json
	launchAuthorizationSchema []byte
	//go:embed schema/execution-event-v1alpha1.schema.json
	executionEventSchema []byte
	//go:embed schema/desired-action-v1alpha1.schema.json
	desiredActionSchema []byte
	//go:embed schema/action-acknowledgement-v1alpha1.schema.json
	actionAcknowledgementSchema []byte
)

// WorkloadSchema returns a copy of the canonical v1alpha1 workload JSON
// Schema.
func WorkloadSchema() []byte {
	return append([]byte(nil), workloadSchema...)
}

// JobRequestSchema returns a copy of the canonical v1alpha1 job-request JSON
// Schema.
func JobRequestSchema() []byte {
	return append([]byte(nil), jobRequestSchema...)
}

// CollectionRequestSchema returns a copy of the canonical v1alpha1
// collection-request JSON Schema.
func CollectionRequestSchema() []byte {
	return append([]byte(nil), collectionRequestSchema...)
}

// GraphRequestSchema returns a copy of the canonical v1alpha1 dependency-
// graph request JSON Schema.
func GraphRequestSchema() []byte {
	return append([]byte(nil), graphRequestSchema...)
}

// EffectiveExecutionSchema returns a copy of the canonical v1alpha1
// effective-execution JSON Schema.
func EffectiveExecutionSchema() []byte {
	return append([]byte(nil), effectiveExecutionSchema...)
}

// AgentAssignmentSchema returns a copy of the canonical v1alpha1 agent-
// assignment JSON Schema.
func AgentAssignmentSchema() []byte {
	return append([]byte(nil), agentAssignmentSchema...)
}

// AgentAcceptanceSchema returns a copy of the canonical v1alpha1 acceptance
// JSON Schema.
func AgentAcceptanceSchema() []byte {
	return append([]byte(nil), agentAcceptanceSchema...)
}

// LaunchAuthorizationSchema returns a copy of the canonical v1alpha1 launch-
// authorization JSON Schema.
func LaunchAuthorizationSchema() []byte {
	return append([]byte(nil), launchAuthorizationSchema...)
}

// ExecutionEventSchema returns a copy of the canonical v1alpha1 execution-
// event JSON Schema.
func ExecutionEventSchema() []byte {
	return append([]byte(nil), executionEventSchema...)
}

// DesiredActionSchema returns a copy of the canonical v1alpha1 desired-action
// JSON Schema.
func DesiredActionSchema() []byte {
	return append([]byte(nil), desiredActionSchema...)
}

// ActionAcknowledgementSchema returns a copy of the canonical v1alpha1 action-
// acknowledgement JSON Schema.
func ActionAcknowledgementSchema() []byte {
	return append([]byte(nil), actionAcknowledgementSchema...)
}
