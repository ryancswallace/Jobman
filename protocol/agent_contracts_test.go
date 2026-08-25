package protocol

import (
	"strings"
	"testing"
	"time"
)

const (
	testDeliveryID  = "018f1f2e-7b54-4ab0-8f7c-1234567890ab"
	testExecutionID = "018f1f2e-7b54-4ab0-8f7c-1234567890ac"
	testAgentID     = "018f1f2e-7b54-4ab0-8f7c-1234567890ad"
	testGeneration  = "018f1f2e-7b54-4ab0-8f7c-1234567890ae"
	testEventID     = "018f1f2e-7b54-4ab0-8f7c-1234567890af"
	testActionID    = "018f1f2e-7b54-4ab0-8f7c-1234567890aa"
	testDigest      = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
)

func TestAgentAcceptanceRoundTrip(t *testing.T) {
	t.Parallel()
	value := AgentAcceptance{
		APIVersion: V1Alpha1, Kind: AgentAcceptanceKind,
		Metadata: AgentAcceptanceMetadata{
			DeliveryID: testDeliveryID, ExecutionID: testExecutionID, AgentID: testAgentID,
		},
		Spec: AgentAcceptanceSpec{
			TargetGenerationID: testGeneration, EffectiveExecutionDigest: testDigest,
		},
	}
	sealed, err := SealAgentAcceptance(value)
	if err != nil {
		t.Fatalf("SealAgentAcceptance() error = %v", err)
	}
	decoded, err := DecodeAgentAcceptance(strings.NewReader(string(sealed.CanonicalJSON)), DecodeLimits{})
	if err != nil || decoded.Digest != sealed.Digest {
		t.Fatalf("DecodeAgentAcceptance() = %#v, %v", decoded, err)
	}
}

func TestAgentExecutionMessagesValidate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	authorization := LaunchAuthorization{
		APIVersion: V1Alpha1, Kind: LaunchAuthorizationKind,
		Metadata: LaunchAuthorizationMetadata{
			AuthorizationID: testDeliveryID, ExecutionID: testExecutionID,
			AgentID: testAgentID, Revision: 1, AcceptedAt: now,
		},
		Spec: LaunchAuthorizationSpec{
			TargetGenerationID: testGeneration, EffectiveExecutionDigest: testDigest,
		},
	}
	if err := ValidateLaunchAuthorization(authorization); err != nil {
		t.Fatalf("ValidateLaunchAuthorization() error = %v", err)
	}
	started := ExecutionEvent{
		APIVersion: V1Alpha1, Kind: ExecutionEventKind,
		Metadata: ExecutionEventMetadata{
			EventID: testEventID, ExecutionID: testExecutionID, AgentID: testAgentID,
			Sequence: 1, ObservedAt: now,
		},
		Spec: ExecutionEventSpec{Type: "process.started", NativeID: "opaque-process-identity"},
	}
	sealed, err := SealExecutionEvent(started)
	if err != nil {
		t.Fatalf("SealExecutionEvent() error = %v", err)
	}
	if _, err = DecodeExecutionEvent(strings.NewReader(string(sealed.CanonicalJSON)), DecodeLimits{}); err != nil {
		t.Fatalf("DecodeExecutionEvent() error = %v", err)
	}
	exitCode := 0
	started.Metadata.Sequence = 2
	started.Spec = ExecutionEventSpec{
		Type: "process.completed", Result: &ProcessResult{Outcome: "success", ExitCode: &exitCode},
	}
	if _, err = SealExecutionEvent(started); err != nil {
		t.Fatalf("SealExecutionEvent(completed) error = %v", err)
	}
	started.Metadata.Sequence = 3
	started.Spec = ExecutionEventSpec{
		Type: "scheduler.submitted", NativeID: "12345",
		Scheduler: &SchedulerObservation{Backend: "slurm", State: "queued", Cluster: "on-prem"},
	}
	if _, err = SealExecutionEvent(started); err != nil {
		t.Fatalf("SealExecutionEvent(scheduler submitted) error = %v", err)
	}
	started.Metadata.Sequence = 4
	started.Spec = ExecutionEventSpec{
		Type: "scheduler.completed", NativeID: "12345",
		Scheduler: &SchedulerObservation{Backend: "slurm", State: "completed"},
		Result:    &ProcessResult{Outcome: "success", ExitCode: &exitCode},
	}
	if _, err = SealExecutionEvent(started); err != nil {
		t.Fatalf("SealExecutionEvent(scheduler completed) error = %v", err)
	}
	action := DesiredAction{
		APIVersion: V1Alpha1, Kind: DesiredActionKind,
		Metadata: DesiredActionMetadata{
			ActionID: testActionID, ExecutionID: testExecutionID, AgentID: testAgentID,
			Revision: 1, RequestedAt: now,
		},
		Spec: DesiredActionSpec{Type: "cancel"},
	}
	if err = ValidateDesiredAction(action); err != nil {
		t.Fatalf("ValidateDesiredAction() error = %v", err)
	}
	acknowledgement := ActionAcknowledgement{
		APIVersion: V1Alpha1, Kind: ActionAcknowledgementKind,
		Metadata: ActionAcknowledgementMetadata{
			ActionID: testActionID, ExecutionID: testExecutionID, AgentID: testAgentID,
			Revision: 1, ObservedAt: now,
		},
	}
	if err = ValidateActionAcknowledgement(acknowledgement); err != nil {
		t.Fatalf("ValidateActionAcknowledgement() error = %v", err)
	}
}

func TestAgentExecutionMessagesRejectInvalidFacts(t *testing.T) {
	t.Parallel()
	acceptance := AgentAcceptance{APIVersion: V1Alpha1, Kind: AgentAcceptanceKind}
	if err := ValidateAgentAcceptance(acceptance); err == nil {
		t.Fatal("ValidateAgentAcceptance() accepted missing identities")
	}
	event := ExecutionEvent{
		APIVersion: V1Alpha1, Kind: ExecutionEventKind,
		Metadata: ExecutionEventMetadata{
			EventID: testEventID, ExecutionID: testExecutionID, AgentID: testAgentID,
			Sequence: 1, ObservedAt: time.Now().UTC(),
		},
		Spec: ExecutionEventSpec{Type: "process.completed", Result: &ProcessResult{Outcome: "success"}},
	}
	if err := ValidateExecutionEvent(event); err == nil {
		t.Fatal("ValidateExecutionEvent() accepted inconsistent success")
	}
}

func TestPublishedArtifactValidation(t *testing.T) {
	valid := PublishedArtifact{
		Name: "result", StoreName: "department-nfs", StoreVersion: 2,
		ObjectKey: "research/results/result.txt", ByteLength: 7,
		Checksum: "sha256:" + strings.Repeat("a", 64),
	}
	if err := validatePublishedArtifacts([]PublishedArtifact{valid}); err != nil {
		t.Fatalf("validatePublishedArtifacts() error = %v", err)
	}
	tests := [][]PublishedArtifact{
		append(make([]PublishedArtifact, maximumArtifacts+1), valid),
		{{Name: "Bad", StoreName: "department-nfs", StoreVersion: 2, ObjectKey: "key", Checksum: valid.Checksum}},
		{valid, valid},
		{
			{Name: "z-result", StoreName: "department-nfs", StoreVersion: 2, ObjectKey: "z", Checksum: valid.Checksum},
			{Name: "a-result", StoreName: "department-nfs", StoreVersion: 2, ObjectKey: "a", Checksum: valid.Checksum},
		},
	}
	for index, values := range tests {
		if err := validatePublishedArtifacts(values); err == nil {
			t.Errorf("validatePublishedArtifacts(%d) accepted invalid metadata", index)
		}
	}

	exitCode := 0
	event := ExecutionEvent{
		APIVersion: V1Alpha1, Kind: ExecutionEventKind,
		Metadata: ExecutionEventMetadata{
			EventID: testEventID, ExecutionID: testExecutionID, AgentID: testAgentID,
			Sequence: 1, ObservedAt: time.Now().UTC(),
		},
		Spec: ExecutionEventSpec{
			Type: "process.completed", Result: &ProcessResult{Outcome: "success", ExitCode: &exitCode},
			Artifacts: []PublishedArtifact{valid},
		},
	}
	if err := ValidateExecutionEvent(event); err != nil {
		t.Fatalf("ValidateExecutionEvent(artifacts) error = %v", err)
	}
}

func TestSchedulerExecutionEventVariants(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	base := ExecutionEvent{
		APIVersion: V1Alpha1, Kind: ExecutionEventKind,
		Metadata: ExecutionEventMetadata{
			EventID: testEventID, ExecutionID: testExecutionID, AgentID: testAgentID,
			Sequence: 1, ObservedAt: now,
		},
	}
	valid := []ExecutionEventSpec{
		{
			Type: "scheduler.uncertain",
			Scheduler: &SchedulerObservation{
				Backend: "slurm", State: "uncertain", Reason: "submission outcome is uncertain",
			},
		},
		{
			Type: "scheduler.observed", NativeID: "123",
			Scheduler: &SchedulerObservation{Backend: "slurm", State: "running"},
		},
	}
	for index, spec := range valid {
		value := base
		value.Metadata.Sequence = int64(index + 1)
		value.Spec = spec
		if err := ValidateExecutionEvent(value); err != nil {
			t.Errorf("valid scheduler event %d: %v", index, err)
		}
	}

	exitCode := 1
	invalid := []ExecutionEventSpec{
		{
			Type: "scheduler.uncertain", NativeID: "123",
			Scheduler: &SchedulerObservation{Backend: "slurm", State: "uncertain"},
		},
		{
			Type: "scheduler.submitted", NativeID: "123",
			Scheduler: &SchedulerObservation{Backend: "slurm", State: "running"},
		},
		{
			Type: "scheduler.observed", NativeID: "123",
			Scheduler: &SchedulerObservation{Backend: "slurm", State: "completed"},
		},
		{
			Type: "scheduler.completed", NativeID: "123",
			Scheduler: &SchedulerObservation{Backend: "slurm", State: "running"},
			Result:    &ProcessResult{Outcome: "failure", ExitCode: &exitCode, FailureCode: "failed"},
		},
		{
			Type: "scheduler.observed", NativeID: "123",
			Scheduler: &SchedulerObservation{Backend: "future", State: "running"},
		},
		{
			Type: "scheduler.observed", NativeID: "123",
			Scheduler: &SchedulerObservation{Backend: "slurm", State: "running", Reason: "bad\x00reason"},
		},
	}
	for index, spec := range invalid {
		value := base
		value.Spec = spec
		if err := ValidateExecutionEvent(value); err == nil {
			t.Errorf("invalid scheduler event %d was accepted", index)
		}
	}
}

func TestAgentExecutionValidatorsRejectEachInvalidFactClass(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	authorization := LaunchAuthorization{
		APIVersion: V1Alpha1, Kind: LaunchAuthorizationKind,
		Metadata: LaunchAuthorizationMetadata{
			AuthorizationID: testDeliveryID, ExecutionID: testExecutionID,
			AgentID: testAgentID, Revision: 1, AcceptedAt: now,
		},
		Spec: LaunchAuthorizationSpec{
			TargetGenerationID: testGeneration, EffectiveExecutionDigest: testDigest,
		},
	}
	authorizationMutations := []func(*LaunchAuthorization){
		func(value *LaunchAuthorization) { value.APIVersion = "unsupported" },
		func(value *LaunchAuthorization) { value.Metadata.AgentID = "invalid" },
		func(value *LaunchAuthorization) { value.Metadata.Revision = 0 },
		func(value *LaunchAuthorization) { value.Spec.EffectiveExecutionDigest = "invalid" },
	}
	for index, mutate := range authorizationMutations {
		value := authorization
		mutate(&value)
		if err := ValidateLaunchAuthorization(value); err == nil {
			t.Errorf("authorization mutation %d was accepted", index)
		}
	}

	started := ExecutionEvent{
		APIVersion: V1Alpha1, Kind: ExecutionEventKind,
		Metadata: ExecutionEventMetadata{
			EventID: testEventID, ExecutionID: testExecutionID, AgentID: testAgentID,
			Sequence: 1, ObservedAt: now,
		},
		Spec: ExecutionEventSpec{Type: "process.started", NativeID: "42"},
	}
	eventMutations := []func(*ExecutionEvent){
		func(value *ExecutionEvent) { value.Kind = "Unsupported" },
		func(value *ExecutionEvent) { value.Metadata.EventID = "invalid" },
		func(value *ExecutionEvent) { value.Metadata.Sequence = 0 },
		func(value *ExecutionEvent) { value.Spec.NativeID = "" },
		func(value *ExecutionEvent) {
			value.Spec = ExecutionEventSpec{Type: "process.completed"}
		},
		func(value *ExecutionEvent) { value.Spec.Type = "unsupported" },
	}
	for index, mutate := range eventMutations {
		value := started
		mutate(&value)
		if err := ValidateExecutionEvent(value); err == nil {
			t.Errorf("event mutation %d was accepted", index)
		}
	}

	exitCode := 256
	processResults := []ProcessResult{
		{Outcome: "unsupported", FailureCode: "failure"},
		{Outcome: "failure", ExitCode: &exitCode},
		{Outcome: "failure", FailureCode: strings.Repeat("x", maximumNameBytes+1)},
		{Outcome: "failure"},
	}
	for index, value := range processResults {
		if err := validateProcessResult(value); err == nil {
			t.Errorf("process result %d was accepted", index)
		}
	}

	action := DesiredAction{
		APIVersion: V1Alpha1, Kind: DesiredActionKind,
		Metadata: DesiredActionMetadata{
			ActionID: testActionID, ExecutionID: testExecutionID, AgentID: testAgentID,
			Revision: 1, RequestedAt: now,
		},
		Spec: DesiredActionSpec{Type: "cancel"},
	}
	actionMutations := []func(*DesiredAction){
		func(value *DesiredAction) { value.Kind = "Unsupported" },
		func(value *DesiredAction) { value.Metadata.ExecutionID = "invalid" },
		func(value *DesiredAction) { value.Spec.Type = "pause" },
	}
	for index, mutate := range actionMutations {
		value := action
		mutate(&value)
		if err := ValidateDesiredAction(value); err == nil {
			t.Errorf("action mutation %d was accepted", index)
		}
	}

	acknowledgement := ActionAcknowledgement{
		APIVersion: V1Alpha1, Kind: ActionAcknowledgementKind,
		Metadata: ActionAcknowledgementMetadata{
			ActionID: testActionID, ExecutionID: testExecutionID, AgentID: testAgentID,
			Revision: 1, ObservedAt: now,
		},
	}
	acknowledgementMutations := []func(*ActionAcknowledgement){
		func(value *ActionAcknowledgement) { value.APIVersion = "unsupported" },
		func(value *ActionAcknowledgement) { value.Metadata.ActionID = "invalid" },
		func(value *ActionAcknowledgement) { value.Metadata.Revision = 0 },
	}
	for index, mutate := range acknowledgementMutations {
		value := acknowledgement
		mutate(&value)
		if err := ValidateActionAcknowledgement(value); err == nil {
			t.Errorf("acknowledgement mutation %d was accepted", index)
		}
	}
}

func TestAgentMessageDecodersRejectMalformedInput(t *testing.T) {
	t.Parallel()
	if _, err := DecodeAgentAcceptance(strings.NewReader(`{`), DecodeLimits{}); err == nil {
		t.Fatal("DecodeAgentAcceptance() accepted malformed JSON")
	}
	if _, err := DecodeAgentAcceptance(strings.NewReader(`{"unknown":true}`), DecodeLimits{}); err == nil {
		t.Fatal("DecodeAgentAcceptance() accepted unknown fields")
	}
	if _, err := DecodeExecutionEvent(strings.NewReader(`{`), DecodeLimits{}); err == nil {
		t.Fatal("DecodeExecutionEvent() accepted malformed JSON")
	}
	if _, err := DecodeExecutionEvent(strings.NewReader(`{"unknown":true}`), DecodeLimits{}); err == nil {
		t.Fatal("DecodeExecutionEvent() accepted unknown fields")
	}
}

func TestAgentExecutionSchemaGettersReturnCopies(t *testing.T) {
	t.Parallel()
	getters := []func() []byte{
		AgentAcceptanceSchema, LaunchAuthorizationSchema, ExecutionEventSchema,
		DesiredActionSchema, ActionAcknowledgementSchema,
	}
	for _, getter := range getters {
		value := getter()
		if len(value) == 0 {
			t.Fatal("schema is empty")
		}
		value[0] = 'x'
		if getter()[0] != '{' {
			t.Fatal("schema getter exposed mutable storage")
		}
	}
}
