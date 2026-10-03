package diagnostic

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Shared-specific facts avoid changing the meaning of local-store fact codes.
const (
	CodeSharedDesiredState          = "control.job.desired_state"
	CodeSharedObservationConfidence = "control.job.observation_confidence"
	CodeSharedSchedulerObservation  = "control.scheduler.observation"
	CodeSharedDependencyObservation = "control.dependency.observation"
	CodeSharedLifecycleEvent        = "control.lifecycle.event"
	OmissionSharedDisclosure        = "shared_disclosure_excluded"
	OmissionSharedUnsupportedFact   = "shared_fact_unsupported"
	OmissionSharedExecutionIdentity = "execution_identity_unavailable"
)

// SharedSchedulerObservation preserves scheduler facts separately from Jobman
// lifecycle and transport freshness. Reason is a scheduler reason code, never
// arbitrary scheduler stdout/stderr. Unknown enum values remain valid tokens.
type SharedSchedulerObservation struct {
	State      string    `json:"state"`
	Reason     string    `json:"reason,omitempty"`
	ObservedAt time.Time `json:"observed_at"`
}

// SharedDependencyObservation is the source's decision about one prerequisite;
// the collector never reimplements graph scheduling or invents satisfaction.
type SharedDependencyObservation struct {
	JobID           string `json:"job_id"`
	Predicate       string `json:"predicate"`
	ObservedOutcome string `json:"observed_outcome,omitempty"`
	Satisfied       bool   `json:"satisfied"`
	Disposition     string `json:"disposition,omitempty"`
}

// SharedLifecycleEvent attributes a durable transition, without including raw
// command, environment, agent payloads or free-form failure messages.
type SharedLifecycleEvent struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	RunID       string    `json:"run_id,omitempty"`
	ExecutionID string    `json:"execution_id,omitempty"`
	Phase       string    `json:"phase,omitempty"`
	Outcome     string    `json:"outcome,omitempty"`
	ObservedAt  time.Time `json:"observed_at"`
	RecordedAt  time.Time `json:"recorded_at"`
}

func validateSharedItem(item Item) error {
	if item.Disclosure != DisclosureMetadata || !sharedFactAllowed(item.Code) {
		return fmt.Errorf("validate shared fact: unsupported disclosure or code for %q", item.ID)
	}
	if _, err := canonicalSharedValue(item.Value); err != nil {
		return err
	}
	if !validSharedUUID(item.Source.EntityID) || item.Source.ArtifactID != "" ||
		item.Source.ByteStart != 0 || item.Source.ByteEnd != 0 {
		return errors.New("validate shared fact: invalid source identity")
	}
	if !sharedToken(item.Source.Kind, false) {
		return errors.New("validate shared fact: invalid source kind")
	}
	switch item.Code {
	case CodeSharedSchedulerObservation:
		return validateSchedulerObservation(item)
	case CodeSharedDependencyObservation:
		return validateDependencyObservation(item)
	case CodeSharedLifecycleEvent:
		return validateLifecycleEvent(item)
	default:
		return validateSharedScalar(item)
	}
}

func validateSharedFactJoin(item Item, evidence Evidence) error {
	if item.Code == CodeSharedLifecycleEvent {
		return validateSharedEventJoin(item, evidence.Shared.Runs)
	}
	if item.Source.EntityID == evidence.Subject.JobID {
		if strings.HasPrefix(item.Code, "jobman.run.") || strings.HasPrefix(item.Code, "jobman.log.") {
			return errors.New("validate shared fact: run observation must identify its run")
		}
		if item.Source.Revision != 0 && item.Source.Revision != evidence.Subject.JobRevision {
			return errors.New("validate shared fact: job revision mismatch")
		}
		return validateSharedJobFact(item, evidence.Subject)
	}
	for _, run := range evidence.Shared.Runs {
		if item.Source.EntityID == run.ID {
			if strings.HasPrefix(item.Code, "jobman.job.") || strings.HasPrefix(item.Code, "control.job.") {
				return errors.New("validate shared fact: job observation must identify its job")
			}
			return nil
		}
	}
	return errors.New("validate shared fact: source is outside selected job and runs")
}

func validateSharedJobFact(item Item, subject Subject) error {
	var want any
	switch item.Code {
	case CodeJobPhase:
		want = subject.Phase
	case CodeJobOutcome:
		want = subject.Outcome
	case CodeJobRevision:
		want = subject.JobRevision
	default:
		return nil
	}
	encoded, err := JSONValue(want)
	if err != nil {
		return err
	}
	if !bytes.Equal(item.Value, encoded) {
		return errors.New("validate shared fact: job observation contradicts selected snapshot")
	}
	return nil
}

func validateSharedEventJoin(item Item, runs []SharedRun) error {
	var event SharedLifecycleEvent
	if err := decodeKnownValue(item.Value, &event); err != nil {
		return err
	}
	if item.Source.EntityID != event.ID {
		return errors.New("validate shared fact: lifecycle event source identity mismatch")
	}
	if event.RunID == "" && event.ExecutionID == "" {
		return nil
	}
	for _, run := range runs {
		if run.ID == event.RunID && (event.ExecutionID == "" || run.ExecutionID == event.ExecutionID) {
			return nil
		}
	}
	return errors.New("validate shared fact: lifecycle event references an unselected run")
}

func sharedFactAllowed(code string) bool {
	switch code {
	case CodeJobPhase, CodeJobOutcome, CodeJobRevision, CodeJobSubmittedAt,
		CodeJobClaimedAt, CodeJobStartedAt, CodeJobCompletedAt, CodeJobCancellationAt,
		CodeRunPhase, CodeRunOutcome, CodeRunRevision, CodeRunReservedAt,
		CodeRunStartedAt, CodeRunCompletedAt, CodeRunExitCode, CodeRunExitSignal,
		CodeRunTimeoutScope, CodeRunStopReason, CodeLogAvailable, CodeLogIntegrity,
		CodeLogRecordingHealth, CodeLogStdoutBytes, CodeLogStderrBytes,
		CodeSharedDesiredState, CodeSharedObservationConfidence,
		CodeSharedSchedulerObservation, CodeSharedDependencyObservation, CodeSharedLifecycleEvent:
		return true
	default:
		return false
	}
}

func validateSharedScalar(item Item) error {
	if bytes.Equal(item.Value, []byte("null")) {
		return errors.New("validate shared fact: scalar cannot be null")
	}
	switch item.Code {
	case CodeJobRevision, CodeRunRevision, CodeLogStdoutBytes, CodeLogStderrBytes:
		var value uint64
		return decodeKnownValue(item.Value, &value)
	case CodeRunExitCode:
		var value int64
		return decodeKnownValue(item.Value, &value)
	case CodeLogAvailable:
		var value bool
		return decodeKnownValue(item.Value, &value)
	case CodeJobSubmittedAt, CodeJobClaimedAt, CodeJobStartedAt, CodeJobCompletedAt,
		CodeJobCancellationAt, CodeRunReservedAt, CodeRunStartedAt, CodeRunCompletedAt:
		var value time.Time
		if err := decodeKnownValue(item.Value, &value); err != nil {
			return err
		}
		return validateUTC("shared fact timestamp", value)
	default:
		var value string
		if err := decodeKnownValue(item.Value, &value); err != nil || !sharedToken(value, false) {
			return fmt.Errorf("validate shared fact: invalid token for %q", item.ID)
		}
		return nil
	}
}

func validateSchedulerObservation(item Item) error {
	var observation SharedSchedulerObservation
	if err := decodeKnownValue(item.Value, &observation); err != nil ||
		!sharedToken(observation.State, false) || !sharedToken(observation.Reason, true) {
		return errors.New("validate shared fact: invalid scheduler observation")
	}
	return validateUTC("scheduler observed_at", observation.ObservedAt)
}

func validateDependencyObservation(item Item) error {
	var observation SharedDependencyObservation
	if err := decodeKnownValue(item.Value, &observation); err != nil ||
		!validSharedUUID(observation.JobID) || !sharedToken(observation.Predicate, false) ||
		!sharedToken(observation.ObservedOutcome, true) || !sharedToken(observation.Disposition, true) {
		return errors.New("validate shared fact: invalid dependency observation")
	}
	return nil
}

func validateLifecycleEvent(item Item) error {
	var event SharedLifecycleEvent
	if err := decodeKnownValue(item.Value, &event); err != nil ||
		!validSharedUUID(event.ID) || !sharedToken(event.Type, false) ||
		(event.RunID != "" && !validSharedUUID(event.RunID)) ||
		(event.ExecutionID != "" && !validSharedUUID(event.ExecutionID)) ||
		!sharedToken(event.Phase, true) || !sharedToken(event.Outcome, true) {
		return errors.New("validate shared fact: invalid lifecycle event")
	}
	if err := validateUTC("event observed_at", event.ObservedAt); err != nil {
		return err
	}
	return validateUTC("event recorded_at", event.RecordedAt)
}

func sharedToken(value string, optional bool) bool {
	if value == "" {
		return optional
	}
	if len(value) > 160 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}
