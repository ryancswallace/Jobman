package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// NewSubmittedJob creates the first durable job snapshot and launch effect.
func NewSubmittedJob(
	id JobID,
	specification JobSpec,
	credentialHash CredentialHash,
	submittedAt time.Time,
	claimDeadline time.Time,
) (TransitionResult, error) {
	submittedAt = normalizeTime(submittedAt)
	claimDeadline = normalizeTime(claimDeadline)

	state := JobState{
		ID:                   id,
		Spec:                 specification,
		Phase:                JobPhaseSubmitting,
		Revision:             1,
		SubmittedAt:          submittedAt,
		LaunchCredentialHash: credentialHash,
		ClaimDeadline:        timePointer(claimDeadline),
	}
	if err := state.Validate(); err != nil {
		return TransitionResult{}, err
	}

	return TransitionResult{
		Job: state,
		Events: []EventDraft{jobEvent(
			state,
			EventJobSubmitted,
			"",
			string(JobPhaseSubmitting),
			submittedAt,
		)},
		Effects: []Effect{{Type: EffectLaunchSupervisor}},
	}, nil
}

// ClaimJob atomically transfers an unexpired submission to one supervisor.
func ClaimJob(
	job JobState,
	credential []byte,
	supervisorID SupervisorID,
	identity ProcessIdentity,
	claimedAt time.Time,
	leaseExpiresAt time.Time,
) (TransitionResult, error) {
	if err := job.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate job before claim: %w", err)
	}
	if job.Phase != JobPhaseSubmitting {
		return TransitionResult{}, jobConflict(job, "claim", JobPhaseSubmitting)
	}
	if !supervisorID.Valid() {
		return TransitionResult{}, invalid("supervisor ID", "must be a canonical UUIDv7")
	}
	if err := identity.Validate(); err != nil {
		return TransitionResult{}, err
	}

	claimedAt = normalizeTime(claimedAt)
	leaseExpiresAt = normalizeTime(leaseExpiresAt)
	if job.ClaimDeadline == nil || !claimedAt.Before(*job.ClaimDeadline) {
		return TransitionResult{}, jobConflictValue(job, "claim", "claim deadline expired")
	}
	if !job.LaunchCredentialHash.Matches(credential) {
		return TransitionResult{}, jobConflictValue(job, "claim", "launch credential mismatch")
	}
	if !leaseExpiresAt.After(claimedAt) {
		return TransitionResult{}, invalid("supervisor lease expiry", "must follow claim time")
	}

	priorPhase := job.Phase
	job.Phase = JobPhaseStarting
	job.Revision++
	job.ClaimedAt = timePointer(claimedAt)
	job.SupervisorID = supervisorID
	job.LaunchCredentialHash = CredentialHash{}
	job.ClaimDeadline = nil

	supervisor := SupervisorState{
		ID:             supervisorID,
		JobID:          job.ID,
		Revision:       1,
		Process:        identity,
		ClaimedAt:      claimedAt,
		LeaseRenewedAt: claimedAt,
		LeaseExpiresAt: leaseExpiresAt,
	}
	if err := job.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate claimed job: %w", err)
	}
	if err := supervisor.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate claimed supervisor: %w", err)
	}

	return TransitionResult{
		Job:        job,
		Supervisor: &supervisor,
		Events: []EventDraft{
			jobEvent(job, EventSupervisorClaimed, string(priorPhase), string(job.Phase), claimedAt),
			supervisorEvent(supervisor, EventSupervisorClaimed, "", "claimed", claimedAt),
		},
	}, nil
}

// ReserveRun durably allocates a run number and private log paths before target
// creation.
func ReserveRun(
	job JobState,
	runID RunID,
	runNumber uint64,
	logs LogMetadata,
	reservedAt time.Time,
) (TransitionResult, error) {
	if err := job.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate job before run reservation: %w", err)
	}
	if job.Phase != JobPhaseStarting {
		return TransitionResult{}, jobConflict(job, "reserve run for", JobPhaseStarting)
	}
	if job.ActiveRunID != "" {
		return TransitionResult{}, jobConflictValue(job, "reserve run for", "active run already exists")
	}
	if !runID.Valid() {
		return TransitionResult{}, invalid("run ID", "must be a canonical UUIDv7")
	}
	if runNumber == 0 {
		return TransitionResult{}, invalid("run number", "must be positive")
	}
	if err := logs.Validate(); err != nil {
		return TransitionResult{}, err
	}

	reservedAt = normalizeTime(reservedAt)
	if job.ClaimedAt == nil || reservedAt.Before(*job.ClaimedAt) {
		return TransitionResult{}, invalid("run reserve time", "must not precede supervisor claim")
	}

	job.ActiveRunID = runID
	job.Revision++
	run := RunState{
		ID:         runID,
		JobID:      job.ID,
		Number:     runNumber,
		Phase:      RunPhaseStarting,
		Revision:   1,
		ReservedAt: reservedAt,
		Logs:       logs,
	}
	if err := job.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate job after run reservation: %w", err)
	}
	if err := run.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate reserved run: %w", err)
	}

	return TransitionResult{
		Job: job,
		Run: &run,
		Events: []EventDraft{
			jobEvent(job, EventRunReserved, string(job.Phase), string(job.Phase), reservedAt),
			runEvent(run, EventRunReserved, "", string(run.Phase), reservedAt),
		},
		Effects: []Effect{{Type: EffectStartTarget}},
	}, nil
}

// MarkProcessStarted publishes verified target identity after process creation.
func MarkProcessStarted(
	job JobState,
	run RunState,
	resolvedExecutable string,
	identity ProcessIdentity,
	startedAt time.Time,
) (TransitionResult, error) {
	if err := validateJobRunPair(job, run); err != nil {
		return TransitionResult{}, err
	}
	if job.Phase != JobPhaseStarting || run.Phase != RunPhaseStarting {
		return TransitionResult{}, pairConflict(job, run, "mark process started", JobPhaseStarting, RunPhaseStarting)
	}
	if resolvedExecutable == "" || strings.ContainsRune(resolvedExecutable, '\x00') {
		return TransitionResult{}, invalid("resolved executable", "must be nonempty and contain no NUL")
	}
	if err := identity.Validate(); err != nil {
		return TransitionResult{}, err
	}

	startedAt = normalizeTime(startedAt)
	if startedAt.Before(run.ReservedAt) {
		return TransitionResult{}, invalid("process start time", "must not precede run reservation")
	}

	priorJobPhase := job.Phase
	priorRunPhase := run.Phase
	job.Phase = JobPhaseRunning
	job.Revision++
	job.StartedAt = timePointer(startedAt)
	run.Phase = RunPhaseRunning
	run.Revision++
	run.ResolvedExecutable = resolvedExecutable
	run.Process = processPointer(identity)
	run.StartedAt = timePointer(startedAt)
	if err := job.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate running job: %w", err)
	}
	if err := run.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate running run: %w", err)
	}

	return TransitionResult{
		Job: job,
		Run: &run,
		Events: []EventDraft{
			jobEvent(job, EventProcessStarted, string(priorJobPhase), string(job.Phase), startedAt),
			runEvent(run, EventProcessStarted, string(priorRunPhase), string(run.Phase), startedAt),
		},
	}, nil
}

// MarkStartFailed finalizes a reserved run whose target could not be safely
// published.
func MarkStartFailed(
	job JobState,
	run RunState,
	logs LogMetadata,
	diagnosticCode string,
	failedAt time.Time,
) (TransitionResult, error) {
	if diagnosticCode == "" {
		return TransitionResult{}, invalid("start failure diagnostic code", "must not be empty")
	}

	result, err := completeRunTransition(
		job,
		run,
		RunOutcomeStartFailed,
		nil,
		logs,
		diagnosticCode,
		failedAt,
		EventStartFailed,
	)
	if err != nil {
		return TransitionResult{}, err
	}

	return result, nil
}

// RequestCancellation durably records cancellation before any signal effect.
// A nil run is valid while a claimed supervisor has not yet reserved a run.
func RequestCancellation(job JobState, run *RunState, requestedAt time.Time) (TransitionResult, error) {
	if err := job.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate job before cancellation: %w", err)
	}
	if cancellationAlreadyAccepted(job) {
		return unchanged(job, run), nil
	}
	if err := validateCancellationTarget(job, run); err != nil {
		return TransitionResult{}, err
	}

	requestedAt = normalizeTime(requestedAt)
	if requestedAt.Before(job.SubmittedAt) {
		return TransitionResult{}, invalid("cancellation time", "must not precede submission")
	}

	priorJobPhase := job.Phase
	job.Phase = JobPhaseStopping
	job.Revision++
	job.Cancellation = &CancellationIntent{
		RequestedAt: requestedAt,
		Reason:      StopReasonCancellation,
	}
	events := []EventDraft{
		jobEvent(job, EventCancellation, string(priorJobPhase), string(job.Phase), requestedAt),
	}
	updatedRun, runEventDraft, effect := cancelRun(run, requestedAt)
	if runEventDraft != nil {
		events = append(events, *runEventDraft)
	}

	if err := job.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate stopping job: %w", err)
	}
	if updatedRun != nil {
		if err := updatedRun.Validate(); err != nil {
			return TransitionResult{}, fmt.Errorf("validate stopping run: %w", err)
		}
	}

	return TransitionResult{Job: job, Run: updatedRun, Events: events, Effects: effect}, nil
}

// FinalizeRun records a proven target result and completes the one-run job.
func FinalizeRun(
	job JobState,
	run RunState,
	outcome RunOutcome,
	exit *ExitInfo,
	logs LogMetadata,
	completedAt time.Time,
) (TransitionResult, error) {
	if outcome == RunOutcomeStartFailed {
		return TransitionResult{}, invalid("run outcome", "use MarkStartFailed for a start failure")
	}
	if !outcome.Valid() {
		return TransitionResult{}, invalid("run outcome", "is unknown")
	}
	if job.Cancellation != nil {
		expected := RunOutcomeCancelled
		if job.Cancellation.Reason == StopReasonTimeout {
			expected = RunOutcomeTimedOut
		}
		if outcome != expected {
			return TransitionResult{}, invalid("run outcome", "must match durable stop intent")
		}
	}

	return completeRunTransition(job, run, outcome, exit, logs, "", completedAt, EventRunCompleted)
}

// FinalizeCancellationWithoutRun completes cancellation accepted before a run
// was reserved.
func FinalizeCancellationWithoutRun(job JobState, completedAt time.Time) (TransitionResult, error) {
	if err := job.Validate(); err != nil {
		return TransitionResult{}, err
	}
	if job.Phase != JobPhaseStopping || job.Cancellation == nil || job.ActiveRunID != "" {
		return TransitionResult{}, jobConflictValue(
			job,
			"finalize cancellation",
			"requires stopping cancellation without an active run",
		)
	}

	completedAt = normalizeTime(completedAt)
	if completedAt.Before(job.Cancellation.RequestedAt) {
		return TransitionResult{}, invalid("cancellation completion time", "must not precede request")
	}

	priorPhase := job.Phase
	job.Phase = JobPhaseCompleted
	job.Outcome = JobOutcomeCancelled
	job.Revision++
	job.CompletedAt = timePointer(completedAt)
	if err := job.Validate(); err != nil {
		return TransitionResult{}, err
	}

	return TransitionResult{
		Job: job,
		Events: []EventDraft{jobEvent(
			job,
			EventJobCompleted,
			string(priorPhase),
			string(job.Phase),
			completedAt,
		)},
	}, nil
}

// MarkSubmissionFailed finalizes a job that no supervisor claimed before its
// deadline.
func MarkSubmissionFailed(job JobState, diagnosticCode string, failedAt time.Time) (TransitionResult, error) {
	if err := job.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate job before submission failure: %w", err)
	}
	if job.Phase != JobPhaseSubmitting {
		return TransitionResult{}, jobConflict(job, "mark submission failed", JobPhaseSubmitting)
	}
	if diagnosticCode == "" {
		return TransitionResult{}, invalid("submission failure diagnostic code", "must not be empty")
	}

	failedAt = normalizeTime(failedAt)
	if job.ClaimDeadline == nil || failedAt.Before(*job.ClaimDeadline) {
		return TransitionResult{}, jobConflictValue(job, "mark submission failed", "claim deadline has not expired")
	}

	priorPhase := job.Phase
	job.Phase = JobPhaseCompleted
	job.Outcome = JobOutcomeSubmissionFailed
	job.Revision++
	job.CompletedAt = timePointer(failedAt)
	job.LaunchCredentialHash = CredentialHash{}
	job.ClaimDeadline = nil
	job.LastDiagnosticCode = diagnosticCode
	if err := job.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate submission-failed job: %w", err)
	}

	return TransitionResult{
		Job: job,
		Events: []EventDraft{jobEventWithDetails(
			job,
			EventSubmissionFailed,
			string(priorPhase),
			string(job.Phase),
			failedAt,
			diagnosticDetails(diagnosticCode),
		)},
	}, nil
}

// MarkOwnershipLost records uncertainty without fabricating a process result.
func MarkOwnershipLost(
	job JobState,
	run *RunState,
	logs *LogMetadata,
	diagnosticCode string,
	lostAt time.Time,
) (TransitionResult, error) {
	if diagnosticCode == "" {
		return TransitionResult{}, invalid("ownership-loss diagnostic code", "must not be empty")
	}
	if run == nil {
		if err := job.Validate(); err != nil {
			return TransitionResult{}, err
		}
		validWithoutRun := job.Phase == JobPhaseStarting || job.Phase == JobPhaseWaiting ||
			job.Phase == JobPhaseQueued || job.Phase == JobPhaseBackoff || job.Phase == JobPhasePaused ||
			job.Phase == JobPhaseStopping && job.Cancellation != nil
		if !validWithoutRun || job.ActiveRunID != "" {
			return TransitionResult{}, jobConflictValue(job, "mark ownership lost", "run state is required")
		}

		lostAt = normalizeTime(lostAt)
		priorPhase := job.Phase
		job.Phase = JobPhaseCompleted
		job.Outcome = JobOutcomeLost
		job.Revision++
		job.CompletedAt = timePointer(lostAt)
		job.LastDiagnosticCode = diagnosticCode
		if err := job.Validate(); err != nil {
			return TransitionResult{}, err
		}

		return TransitionResult{
			Job: job,
			Events: []EventDraft{jobEventWithDetails(
				job,
				EventOwnershipLost,
				string(priorPhase),
				string(job.Phase),
				lostAt,
				diagnosticDetails(diagnosticCode),
			)},
		}, nil
	}
	if logs == nil {
		return TransitionResult{}, invalid("lost-run log metadata", "must be supplied")
	}

	return completeRunTransition(
		job,
		*run,
		RunOutcomeLost,
		nil,
		*logs,
		diagnosticCode,
		lostAt,
		EventOwnershipLost,
	)
}

// RenewSupervisorLease advances a live supervisor lease.
func RenewSupervisorLease(
	state SupervisorState,
	renewedAt time.Time,
	expiresAt time.Time,
) (SupervisorState, EventDraft, error) {
	if err := state.Validate(); err != nil {
		return SupervisorState{}, EventDraft{}, err
	}
	if state.ReleasedAt != nil {
		return SupervisorState{}, EventDraft{}, supervisorConflict(state, "renew lease", "active")
	}

	renewedAt = normalizeTime(renewedAt)
	expiresAt = normalizeTime(expiresAt)
	if renewedAt.Before(state.LeaseRenewedAt) {
		return SupervisorState{}, EventDraft{}, invalid("lease renewal time", "must not move backward")
	}
	if !expiresAt.After(renewedAt) {
		return SupervisorState{}, EventDraft{}, invalid("lease expiry", "must follow renewal")
	}

	state.Revision++
	state.LeaseRenewedAt = renewedAt
	state.LeaseExpiresAt = expiresAt
	if err := state.Validate(); err != nil {
		return SupervisorState{}, EventDraft{}, err
	}

	return state, supervisorEvent(state, EventSupervisorRenewed, "claimed", "claimed", renewedAt), nil
}

// ReleaseSupervisor records that a supervisor no longer owns active work.
func ReleaseSupervisor(state SupervisorState, releasedAt time.Time) (SupervisorState, EventDraft, error) {
	if err := state.Validate(); err != nil {
		return SupervisorState{}, EventDraft{}, err
	}
	if state.ReleasedAt != nil {
		return state, EventDraft{}, nil
	}

	releasedAt = normalizeTime(releasedAt)
	if releasedAt.Before(state.ClaimedAt) {
		return SupervisorState{}, EventDraft{}, invalid("supervisor release time", "must not precede claim")
	}

	state.Revision++
	state.ReleasedAt = timePointer(releasedAt)
	if err := state.Validate(); err != nil {
		return SupervisorState{}, EventDraft{}, err
	}

	return state, supervisorEvent(state, EventSupervisorReleased, "claimed", "released", releasedAt), nil
}

func completeRunTransition(
	job JobState,
	run RunState,
	runOutcome RunOutcome,
	exit *ExitInfo,
	logs LogMetadata,
	diagnosticCode string,
	completedAt time.Time,
	eventType EventType,
) (TransitionResult, error) {
	if err := validateJobRunPair(job, run); err != nil {
		return TransitionResult{}, err
	}
	if !activeJobPhase(job.Phase) || !activeRunPhase(run.Phase) {
		return TransitionResult{}, pairConflict(
			job,
			run,
			"finalize",
			JobPhaseStarting,
			RunPhaseStarting,
		)
	}
	if !runOutcome.Valid() {
		return TransitionResult{}, invalid("run outcome", "is unknown")
	}
	if err := logs.Validate(); err != nil {
		return TransitionResult{}, err
	}

	completedAt = normalizeTime(completedAt)
	if completedAt.Before(run.ReservedAt) {
		return TransitionResult{}, invalid("completion time", "must not precede run reservation")
	}
	if exit != nil {
		value := *exit
		value.ObservedAt = normalizeTime(value.ObservedAt)
		if err := value.Validate(); err != nil {
			return TransitionResult{}, err
		}
		exit = &value
	}

	jobOutcome, err := jobOutcomeForRun(runOutcome)
	if err != nil {
		return TransitionResult{}, err
	}
	priorJobPhase := job.Phase
	priorRunPhase := run.Phase
	job.Phase = JobPhaseCompleted
	job.Outcome = jobOutcome
	job.Revision++
	job.CompletedAt = timePointer(completedAt)
	job.ActiveRunID = ""
	job.LastDiagnosticCode = diagnosticCode
	run.Phase = RunPhaseCompleted
	run.Outcome = runOutcome
	run.Revision++
	run.CompletedAt = timePointer(completedAt)
	run.Exit = exit
	run.Logs = logs
	run.LastDiagnosticCode = diagnosticCode
	if err := job.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate completed job: %w", err)
	}
	if err := run.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate completed run: %w", err)
	}

	details := diagnosticDetails(diagnosticCode)
	return TransitionResult{
		Job: job,
		Run: &run,
		Events: []EventDraft{
			jobEventWithDetails(
				job,
				EventJobCompleted,
				string(priorJobPhase),
				string(job.Phase),
				completedAt,
				details,
			),
			runEventWithDetails(
				run,
				eventType,
				string(priorRunPhase),
				string(run.Phase),
				completedAt,
				details,
			),
		},
	}, nil
}

func validateJobRunPair(job JobState, run RunState) error {
	if err := job.Validate(); err != nil {
		return fmt.Errorf("validate job: %w", err)
	}
	if err := run.Validate(); err != nil {
		return fmt.Errorf("validate run: %w", err)
	}
	if run.JobID != job.ID || job.ActiveRunID != run.ID {
		return invalid("job/run relationship", "run must be the job's active run")
	}

	return nil
}

func validateCancellationTarget(job JobState, run *RunState) error {
	if job.Phase != JobPhaseWaiting && job.Phase != JobPhaseQueued &&
		job.Phase != JobPhaseStarting && job.Phase != JobPhaseRunning &&
		job.Phase != JobPhaseBackoff && job.Phase != JobPhasePaused {
		return jobConflict(
			job,
			"cancel",
			JobPhaseWaiting,
			JobPhaseQueued,
			JobPhaseStarting,
			JobPhaseRunning,
			JobPhaseBackoff,
			JobPhasePaused,
		)
	}
	if run == nil {
		if job.ActiveRunID != "" {
			return invalid("cancellation run", "must be supplied for the active run")
		}

		return nil
	}
	if err := validateJobRunPair(job, *run); err != nil {
		return err
	}
	if run.Phase != RunPhaseStarting && run.Phase != RunPhaseRunning && run.Phase != RunPhasePaused {
		return runConflict(*run, "cancel", RunPhaseStarting, RunPhaseRunning, RunPhasePaused)
	}

	return nil
}

func cancelRun(run *RunState, requestedAt time.Time) (*RunState, *EventDraft, []Effect) {
	if run == nil {
		return nil, nil, nil
	}

	value := *run
	priorPhase := value.Phase
	value.Phase = RunPhaseStopping
	value.Revision++
	value.StopRequestedAt = timePointer(requestedAt)
	value.StopReason = StopReasonCancellation
	event := runEvent(value, EventCancellation, string(priorPhase), string(value.Phase), requestedAt)
	if value.Process == nil {
		return &value, &event, nil
	}

	return &value, &event, []Effect{{Type: EffectStopTarget}}
}

func jobOutcomeForRun(outcome RunOutcome) (JobOutcome, error) {
	switch outcome {
	case RunOutcomeSuccess:
		return JobOutcomeSuccess, nil
	case RunOutcomeFailure, RunOutcomeStartFailed:
		return JobOutcomeFailure, nil
	case RunOutcomeTimedOut:
		return JobOutcomeTimedOut, nil
	case RunOutcomeCancelled:
		return JobOutcomeCancelled, nil
	case RunOutcomeLost:
		return JobOutcomeLost, nil
	default:
		return "", invalid("run outcome", "is unknown")
	}
}

func activeJobPhase(phase JobPhase) bool {
	return phase == JobPhaseStarting || phase == JobPhaseRunning || phase == JobPhaseStopping
}

func activeRunPhase(phase RunPhase) bool {
	return phase == RunPhaseStarting || phase == RunPhaseRunning || phase == RunPhaseStopping
}

func cancellationAlreadyAccepted(job JobState) bool {
	return job.Cancellation != nil &&
		(job.Phase == JobPhaseStopping ||
			(job.Phase == JobPhaseCompleted && job.Outcome == JobOutcomeCancelled))
}

func unchanged(job JobState, run *RunState) TransitionResult {
	result := TransitionResult{Job: job}
	if run != nil {
		clone := *run
		result.Run = &clone
	}

	return result
}

func jobEvent(state JobState, eventType EventType, from, to string, at time.Time) EventDraft {
	return jobEventWithDetails(state, eventType, from, to, at, nil)
}

func jobEventWithDetails(
	state JobState,
	eventType EventType,
	from string,
	to string,
	at time.Time,
	details json.RawMessage,
) EventDraft {
	return EventDraft{
		JobID:      state.ID,
		Entity:     EntityJob,
		EntityID:   state.ID.String(),
		Type:       eventType,
		FromPhase:  from,
		ToPhase:    to,
		ToOutcome:  string(state.Outcome),
		Revision:   state.Revision,
		OccurredAt: normalizeTime(at),
		Details:    details,
	}
}

func runEvent(state RunState, eventType EventType, from, to string, at time.Time) EventDraft {
	return runEventWithDetails(state, eventType, from, to, at, nil)
}

func runEventWithDetails(
	state RunState,
	eventType EventType,
	from string,
	to string,
	at time.Time,
	details json.RawMessage,
) EventDraft {
	return EventDraft{
		JobID:      state.JobID,
		RunID:      state.ID,
		Entity:     EntityRun,
		EntityID:   state.ID.String(),
		Type:       eventType,
		FromPhase:  from,
		ToPhase:    to,
		ToOutcome:  string(state.Outcome),
		Revision:   state.Revision,
		OccurredAt: normalizeTime(at),
		Details:    details,
	}
}

func supervisorEvent(
	state SupervisorState,
	eventType EventType,
	from string,
	to string,
	at time.Time,
) EventDraft {
	return EventDraft{
		JobID:        state.JobID,
		SupervisorID: state.ID,
		Entity:       EntitySupervisor,
		EntityID:     state.ID.String(),
		Type:         eventType,
		FromPhase:    from,
		ToPhase:      to,
		Revision:     state.Revision,
		OccurredAt:   normalizeTime(at),
	}
}

func diagnosticDetails(code string) json.RawMessage {
	if code == "" {
		return nil
	}
	record, recordErr := NewDiagnosticRecord(DiagnosticCode(code), nil)
	if recordErr != nil {
		encoded, err := json.Marshal(struct {
			DiagnosticCode string `json:"diagnostic_code"`
		}{DiagnosticCode: code})
		if err != nil {
			panic(fmt.Sprintf("encode legacy diagnostic details: %v", err))
		}

		return encoded
	}

	encoded, err := json.Marshal(struct {
		DiagnosticCode string           `json:"diagnostic_code"`
		Diagnostic     DiagnosticRecord `json:"diagnostic"`
	}{DiagnosticCode: code, Diagnostic: record})
	if err != nil {
		panic(fmt.Sprintf("encode static diagnostic details: %v", err))
	}

	return encoded
}

func normalizeTime(value time.Time) time.Time {
	return value.UTC().Round(0)
}

func timePointer(value time.Time) *time.Time {
	normalized := normalizeTime(value)

	return &normalized
}

func processPointer(identity ProcessIdentity) *ProcessIdentity {
	return &identity
}

func jobConflict(job JobState, operation string, allowed ...JobPhase) error {
	expected := make([]string, len(allowed))
	for index, phase := range allowed {
		expected[index] = string(phase)
	}

	return &ConflictError{
		Entity:    EntityJob,
		ID:        job.ID.String(),
		Operation: operation,
		Actual:    string(job.Phase),
		Allowed:   expected,
	}
}

func jobConflictValue(job JobState, operation, actual string) error {
	return &ConflictError{
		Entity:    EntityJob,
		ID:        job.ID.String(),
		Operation: operation,
		Actual:    actual,
	}
}

func runConflict(run RunState, operation string, allowed ...RunPhase) error {
	expected := make([]string, len(allowed))
	for index, phase := range allowed {
		expected[index] = string(phase)
	}

	return &ConflictError{
		Entity:    EntityRun,
		ID:        run.ID.String(),
		Operation: operation,
		Actual:    string(run.Phase),
		Allowed:   expected,
	}
}

func supervisorConflict(state SupervisorState, operation string, allowed ...string) error {
	return &ConflictError{
		Entity:    EntitySupervisor,
		ID:        state.ID.String(),
		Operation: operation,
		Actual:    "released",
		Allowed:   allowed,
	}
}

func pairConflict(
	job JobState,
	run RunState,
	operation string,
	expectedJob JobPhase,
	expectedRun RunPhase,
) error {
	return &ConflictError{
		Entity:    EntityJob,
		ID:        job.ID.String(),
		Operation: operation,
		Actual:    fmt.Sprintf("job=%s run=%s", job.Phase, run.Phase),
		Allowed:   []string{fmt.Sprintf("job=%s run=%s", expectedJob, expectedRun)},
	}
}
