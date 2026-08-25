package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"time"
)

const (
	processOutcomeCancelled = "cancelled" //nolint:misspell // Frozen v1alpha1 wire value.
	processOutcomeFailure   = "failure"
	processOutcomeLost      = "lost"
	processOutcomeSuccess   = "success"
	processOutcomeTimedOut  = "timed_out"
)

var (
	schedulerNonterminalStates = []string{"queued", "running", "suspended", "completing", "unknown"}
	schedulerTerminalStates    = []string{
		"completed", "failed", processOutcomeCancelled, processOutcomeTimedOut, "preempted", "node_failed",
		"out_of_memory", "boot_failed", "deadline", processOutcomeLost,
	}
)

// DecodeAgentAcceptance reads, bounds, validates, and seals one acceptance
// request.
func DecodeAgentAcceptance(source io.Reader, limits DecodeLimits) (SealedAgentAcceptance, error) {
	encoded, err := readCanonical(source, limits)
	if err != nil {
		return SealedAgentAcceptance{}, fmt.Errorf("decode agent acceptance: %w", err)
	}
	var value AgentAcceptance
	if err = strictUnmarshal(encoded, &value); err != nil {
		return SealedAgentAcceptance{}, fmt.Errorf("decode agent acceptance: %w", err)
	}

	return SealAgentAcceptance(value)
}

// SealAgentAcceptance validates and returns canonical acceptance content.
func SealAgentAcceptance(value AgentAcceptance) (SealedAgentAcceptance, error) {
	cloned, err := cloneJSON(value)
	if err != nil {
		return SealedAgentAcceptance{}, fmt.Errorf("seal agent acceptance: clone: %w", err)
	}
	if validationErr := ValidateAgentAcceptance(cloned); validationErr != nil {
		return SealedAgentAcceptance{}, validationErr
	}
	canonical, err := marshalCanonical(cloned)
	if err != nil {
		return SealedAgentAcceptance{}, fmt.Errorf("seal agent acceptance: %w", err)
	}

	return SealedAgentAcceptance{Document: cloned, CanonicalJSON: canonical, Digest: digest(canonical)}, nil
}

// ValidateAgentAcceptance verifies immutable assignment ownership facts.
func ValidateAgentAcceptance(value AgentAcceptance) error {
	if value.APIVersion != V1Alpha1 || value.Kind != AgentAcceptanceKind {
		return errors.New("validate agent acceptance: unsupported contract")
	}
	for name, identifier := range map[string]string{
		"delivery ID": value.Metadata.DeliveryID, executionIDLabel: value.Metadata.ExecutionID,
		agentIDLabel: value.Metadata.AgentID, targetGenerationIDLabel: value.Spec.TargetGenerationID,
	} {
		if !idPattern.MatchString(identifier) {
			return fmt.Errorf("validate agent acceptance: invalid %s", name)
		}
	}
	if !digestPattern.MatchString(value.Spec.EffectiveExecutionDigest) {
		return errors.New("validate agent acceptance: invalid effective execution digest")
	}

	return nil
}

// ValidateLaunchAuthorization verifies a server-generated launch decision.
func ValidateLaunchAuthorization(value LaunchAuthorization) error {
	if value.APIVersion != V1Alpha1 || value.Kind != LaunchAuthorizationKind {
		return errors.New("validate launch authorization: unsupported contract")
	}
	for name, identifier := range map[string]string{
		"authorization ID": value.Metadata.AuthorizationID,
		executionIDLabel:   value.Metadata.ExecutionID, agentIDLabel: value.Metadata.AgentID,
		targetGenerationIDLabel: value.Spec.TargetGenerationID,
	} {
		if !idPattern.MatchString(identifier) {
			return fmt.Errorf("validate launch authorization: invalid %s", name)
		}
	}
	if value.Metadata.Revision < 1 || !validProtocolTime(value.Metadata.AcceptedAt) {
		return errors.New("validate launch authorization: invalid acceptance metadata")
	}
	if !digestPattern.MatchString(value.Spec.EffectiveExecutionDigest) {
		return errors.New("validate launch authorization: invalid effective execution digest")
	}

	return nil
}

// DecodeExecutionEvent reads, bounds, validates, and seals one ordered event.
func DecodeExecutionEvent(source io.Reader, limits DecodeLimits) (SealedExecutionEvent, error) {
	encoded, err := readCanonical(source, limits)
	if err != nil {
		return SealedExecutionEvent{}, fmt.Errorf("decode execution event: %w", err)
	}
	var value ExecutionEvent
	if err = strictUnmarshal(encoded, &value); err != nil {
		return SealedExecutionEvent{}, fmt.Errorf("decode execution event: %w", err)
	}

	return SealExecutionEvent(value)
}

// SealExecutionEvent validates and returns canonical event content.
func SealExecutionEvent(value ExecutionEvent) (SealedExecutionEvent, error) {
	cloned, err := cloneJSON(value)
	if err != nil {
		return SealedExecutionEvent{}, fmt.Errorf("seal execution event: clone: %w", err)
	}
	cloned.Metadata.ObservedAt = cloned.Metadata.ObservedAt.UTC()
	if validationErr := ValidateExecutionEvent(cloned); validationErr != nil {
		return SealedExecutionEvent{}, validationErr
	}
	canonical, err := marshalCanonical(cloned)
	if err != nil {
		return SealedExecutionEvent{}, fmt.Errorf("seal execution event: %w", err)
	}

	return SealedExecutionEvent{Document: cloned, CanonicalJSON: canonical, Digest: digest(canonical)}, nil
}

// ValidateExecutionEvent verifies source ordering and event-specific facts.
//
//nolint:cyclop,gocognit // Event variants deliberately validate each mutually exclusive fact.
func ValidateExecutionEvent(value ExecutionEvent) error {
	if value.APIVersion != V1Alpha1 || value.Kind != ExecutionEventKind {
		return errors.New("validate execution event: unsupported contract")
	}
	for name, identifier := range map[string]string{
		"event ID": value.Metadata.EventID, executionIDLabel: value.Metadata.ExecutionID,
		agentIDLabel: value.Metadata.AgentID,
	} {
		if !idPattern.MatchString(identifier) {
			return fmt.Errorf("validate execution event: invalid %s", name)
		}
	}
	if value.Metadata.Sequence < 1 || !validProtocolTime(value.Metadata.ObservedAt) {
		return errors.New("validate execution event: invalid ordering metadata")
	}
	switch value.Spec.Type {
	case "process.started":
		if value.Spec.NativeID == "" || len(value.Spec.NativeID) > maximumDescriptionBytes ||
			value.Spec.Scheduler != nil || value.Spec.Result != nil || len(value.Spec.Artifacts) != 0 {
			return errors.New("validate execution event: invalid process started facts")
		}
	case "process.completed":
		if value.Spec.NativeID != "" || value.Spec.Scheduler != nil || value.Spec.Result == nil {
			return errors.New("validate execution event: invalid process completion facts")
		}
		if err := validateProcessResult(*value.Spec.Result); err != nil {
			return fmt.Errorf("validate execution event: %w", err)
		}
		if err := validatePublishedArtifacts(value.Spec.Artifacts); err != nil {
			return fmt.Errorf("validate execution event: %w", err)
		}
	case "scheduler.uncertain":
		if value.Spec.NativeID != "" || value.Spec.Result != nil || value.Spec.Scheduler == nil ||
			value.Spec.Scheduler.State != "uncertain" || len(value.Spec.Artifacts) != 0 {
			return errors.New("validate execution event: invalid uncertain scheduler facts")
		}
		if err := validateSchedulerObservation(*value.Spec.Scheduler); err != nil {
			return fmt.Errorf("validate execution event: %w", err)
		}
	case "scheduler.submitted":
		if value.Spec.NativeID == "" || value.Spec.Result != nil || value.Spec.Scheduler == nil ||
			!slices.Contains([]string{"queued", "unknown"}, value.Spec.Scheduler.State) || len(value.Spec.Artifacts) != 0 {
			return errors.New("validate execution event: invalid scheduler submission facts")
		}
		if err := validateSchedulerObservation(*value.Spec.Scheduler); err != nil {
			return fmt.Errorf("validate execution event: %w", err)
		}
	case "scheduler.observed":
		if value.Spec.NativeID == "" || value.Spec.Result != nil || value.Spec.Scheduler == nil ||
			!slices.Contains(schedulerNonterminalStates, value.Spec.Scheduler.State) || len(value.Spec.Artifacts) != 0 {
			return errors.New("validate execution event: invalid scheduler observation facts")
		}
		if err := validateSchedulerObservation(*value.Spec.Scheduler); err != nil {
			return fmt.Errorf("validate execution event: %w", err)
		}
	case "scheduler.completed":
		if value.Spec.NativeID == "" || value.Spec.Result == nil || value.Spec.Scheduler == nil ||
			!slices.Contains(schedulerTerminalStates, value.Spec.Scheduler.State) {
			return errors.New("validate execution event: invalid scheduler completion facts")
		}
		if err := validateSchedulerObservation(*value.Spec.Scheduler); err != nil {
			return fmt.Errorf("validate execution event: %w", err)
		}
		if err := validateProcessResult(*value.Spec.Result); err != nil {
			return fmt.Errorf("validate execution event: %w", err)
		}
		if err := validatePublishedArtifacts(value.Spec.Artifacts); err != nil {
			return fmt.Errorf("validate execution event: %w", err)
		}
	default:
		return errors.New("validate execution event: unsupported event type")
	}

	return nil
}

func validatePublishedArtifacts(values []PublishedArtifact) error {
	if len(values) > maximumArtifacts {
		return errors.New("too many published artifacts")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if err := validateName("published artifact name", value.Name); err != nil ||
			validateName("artifact store", value.StoreName) != nil || value.StoreVersion < 1 ||
			value.ObjectKey == "" || len(value.ObjectKey) > maximumArgumentBytes ||
			strings.HasPrefix(value.ObjectKey, "/") || strings.ContainsAny(value.ObjectKey, "\\\x00") ||
			value.ByteLength < 0 || !digestPattern.MatchString(value.Checksum) {
			return errors.New("invalid published artifact metadata")
		}
		if _, duplicate := seen[value.Name]; duplicate {
			return errors.New("duplicate published artifact name")
		}
		seen[value.Name] = struct{}{}
	}
	if !slices.IsSortedFunc(values, func(left, right PublishedArtifact) int {
		return strings.Compare(left.Name, right.Name)
	}) {
		return errors.New("published artifacts are not normalized")
	}

	return nil
}

func validateSchedulerObservation(value SchedulerObservation) error {
	knownState := value.State == "uncertain" || slices.Contains(schedulerNonterminalStates, value.State) ||
		slices.Contains(schedulerTerminalStates, value.State)
	if value.Backend != slurmBackend || !knownState {
		return errors.New("unsupported scheduler observation")
	}
	if len(value.Reason) > maximumDescriptionBytes || len(value.Cluster) > maximumNameBytes ||
		strings.ContainsRune(value.Reason, 0) || strings.ContainsRune(value.Cluster, 0) {
		return errors.New("scheduler observation text is invalid")
	}

	return nil
}

//nolint:cyclop // Terminal facts require explicit cross-field consistency checks.
func validateProcessResult(value ProcessResult) error {
	if !slices.Contains([]string{
		processOutcomeSuccess, processOutcomeFailure, processOutcomeCancelled, processOutcomeTimedOut, "aborted", processOutcomeLost,
	}, value.Outcome) {
		return errors.New("unsupported process outcome")
	}
	if value.ExitCode != nil && (*value.ExitCode < -1 || *value.ExitCode > 255) {
		return errors.New("invalid exit code")
	}
	if len(value.Signal) > maximumNameBytes || len(value.FailureCode) > maximumNameBytes {
		return errors.New("process result text is too long")
	}
	if value.Outcome == processOutcomeSuccess && (value.ExitCode == nil || *value.ExitCode != 0 || value.Signal != "" || value.FailureCode != "") {
		return errors.New("success result is inconsistent")
	}
	if value.ExitCode == nil && value.Signal == "" && value.FailureCode == "" && value.Outcome != processOutcomeLost {
		return errors.New("process result has no terminal fact")
	}

	return nil
}

// ValidateDesiredAction verifies a server-generated execution action.
func ValidateDesiredAction(value DesiredAction) error {
	if value.APIVersion != V1Alpha1 || value.Kind != DesiredActionKind {
		return errors.New("validate desired action: unsupported contract")
	}
	for name, identifier := range map[string]string{
		"action ID": value.Metadata.ActionID, executionIDLabel: value.Metadata.ExecutionID,
		agentIDLabel: value.Metadata.AgentID,
	} {
		if !idPattern.MatchString(identifier) {
			return fmt.Errorf("validate desired action: invalid %s", name)
		}
	}
	if value.Metadata.Revision < 1 || !validProtocolTime(value.Metadata.RequestedAt) || value.Spec.Type != "cancel" {
		return errors.New("validate desired action: invalid action")
	}

	return nil
}

// ValidateActionAcknowledgement verifies an agent's durable action receipt.
func ValidateActionAcknowledgement(value ActionAcknowledgement) error {
	if value.APIVersion != V1Alpha1 || value.Kind != ActionAcknowledgementKind {
		return errors.New("validate action acknowledgement: unsupported contract")
	}
	for name, identifier := range map[string]string{
		"action ID": value.Metadata.ActionID, executionIDLabel: value.Metadata.ExecutionID,
		agentIDLabel: value.Metadata.AgentID,
	} {
		if !idPattern.MatchString(identifier) {
			return fmt.Errorf("validate action acknowledgement: invalid %s", name)
		}
	}
	if value.Metadata.Revision < 1 || !validProtocolTime(value.Metadata.ObservedAt) {
		return errors.New("validate action acknowledgement: invalid acknowledgement metadata")
	}

	return nil
}

func marshalCanonical(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	canonical, err := canonicalJSON(encoded, defaultMaximumJSONDepth)
	if err != nil {
		return nil, fmt.Errorf("canonicalize: %w", err)
	}

	return canonical, nil
}

func validProtocolTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && reflect.DeepEqual(value, value.Round(0))
}
