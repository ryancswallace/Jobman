package model

import (
	"encoding/json"
	"fmt"
	"time"
)

// RunDisposition tells a transition whether a completed run terminates its job
// or leaves the same supervisor responsible for another run.
type RunDisposition struct {
	TerminalOutcome JobOutcome
	NextPhase       JobPhase
	NextRunAt       *time.Time
	Reason          string
}

// MoveJob advances an owned job through pre-run waiting and admission phases.
// It deliberately has a narrow transition graph so callers cannot use it as a
// general-purpose state mutator.
func MoveJob(job JobState, target JobPhase, at time.Time, reason string) (TransitionResult, error) {
	if err := job.Validate(); err != nil {
		return TransitionResult{}, err
	}
	if job.ActiveRunID != "" {
		return TransitionResult{}, jobConflictValue(job, "move job", "active run exists")
	}
	if !allowedJobMove(job.Phase, target) {
		return TransitionResult{}, jobConflictValue(job, "move job", string(job.Phase)+" -> "+string(target))
	}

	at = normalizeTime(at)
	prior := job.Phase
	job.Phase = target
	job.Revision++
	if err := job.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate moved job: %w", err)
	}

	var eventType EventType
	switch target {
	case JobPhaseWaiting:
		eventType = EventJobWaiting
	case JobPhaseStarting:
		eventType = EventJobStarting
	case JobPhaseQueued:
		eventType = EventJobQueued
	default:
		return TransitionResult{}, invalid("job target phase", "is not a supported move target")
	}

	return TransitionResult{
		Job: job,
		Events: []EventDraft{jobEventWithDetails(
			job,
			eventType,
			string(prior),
			string(target),
			at,
			reasonDetails(reason),
		)},
	}, nil
}

func allowedJobMove(from, to JobPhase) bool {
	switch to {
	case JobPhaseWaiting:
		return from == JobPhaseStarting || from == JobPhaseQueued
	case JobPhaseQueued:
		return from == JobPhaseWaiting || from == JobPhaseBackoff || from == JobPhaseStarting
	case JobPhaseStarting:
		return from == JobPhaseQueued || from == JobPhaseBackoff || from == JobPhaseWaiting
	default:
		return false
	}
}

// CompleteRun applies a policy decision after one invocation has been reaped.
// A disposition with TerminalOutcome completes the job; otherwise NextPhase
// must be queued or backoff and ownership remains with the supervisor.
func CompleteRun(
	job JobState,
	run RunState,
	runOutcome RunOutcome,
	exit *ExitInfo,
	logs LogMetadata,
	diagnosticCode string,
	completedAt time.Time,
	disposition RunDisposition,
) (TransitionResult, error) {
	if disposition.TerminalOutcome != "" {
		if !disposition.TerminalOutcome.Valid() {
			return TransitionResult{}, invalid("terminal job outcome", "is unknown")
		}
		result, err := completeRunTransition(
			job,
			run,
			runOutcome,
			exit,
			logs,
			diagnosticCode,
			completedAt,
			EventRunCompleted,
		)
		if err != nil {
			return TransitionResult{}, err
		}
		result.Job.Outcome = disposition.TerminalOutcome
		result.Events[0].ToOutcome = string(disposition.TerminalOutcome)
		if err := result.Job.Validate(); err != nil {
			return TransitionResult{}, err
		}

		return result, nil
	}

	return completeRetryRun(job, run, runOutcome, exit, logs, diagnosticCode, completedAt, disposition)
}

func completeRetryRun(
	job JobState,
	run RunState,
	runOutcome RunOutcome,
	exit *ExitInfo,
	logs LogMetadata,
	diagnosticCode string,
	completedAt time.Time,
	disposition RunDisposition,
) (TransitionResult, error) {
	if disposition.NextPhase != JobPhaseQueued && disposition.NextPhase != JobPhaseBackoff {
		return TransitionResult{}, invalid("next job phase", "must be queued or backoff")
	}
	if err := validateJobRunPair(job, run); err != nil {
		return TransitionResult{}, err
	}
	if !activeJobPhase(job.Phase) || !activeRunPhase(run.Phase) {
		return TransitionResult{}, pairConflict(job, run, "complete run", JobPhaseRunning, RunPhaseRunning)
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

	priorJobPhase := job.Phase
	priorRunPhase := run.Phase
	job.Phase = disposition.NextPhase
	job.Revision++
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
		return TransitionResult{}, fmt.Errorf("validate scheduled job: %w", err)
	}
	if err := run.Validate(); err != nil {
		return TransitionResult{}, fmt.Errorf("validate completed run: %w", err)
	}

	details := retryDetails(disposition, diagnosticCode)
	return TransitionResult{
		Job: job,
		Run: &run,
		Events: []EventDraft{
			jobEventWithDetails(
				job,
				EventRetryScheduled,
				string(priorJobPhase),
				string(job.Phase),
				completedAt,
				details,
			),
			runEventWithDetails(
				run,
				EventRunCompleted,
				string(priorRunPhase),
				string(run.Phase),
				completedAt,
				diagnosticDetails(diagnosticCode),
			),
		},
	}, nil
}

// RequestTimeout records a timeout before any terminating signal is sent.
func RequestTimeout(job JobState, run *RunState, requestedAt time.Time) (TransitionResult, error) {
	if err := job.Validate(); err != nil {
		return TransitionResult{}, err
	}
	if job.Phase == JobPhaseCompleted {
		return unchanged(job, run), nil
	}
	if run != nil {
		if err := validateJobRunPair(job, *run); err != nil {
			return TransitionResult{}, err
		}
	}
	if job.Cancellation != nil && job.Cancellation.Reason == StopReasonTimeout {
		return unchanged(job, run), nil
	}
	requestedAt = normalizeTime(requestedAt)
	priorJob := job.Phase
	job.Phase = JobPhaseStopping
	job.Revision++
	job.Cancellation = &CancellationIntent{RequestedAt: requestedAt, Reason: StopReasonTimeout}

	events := []EventDraft{jobEvent(job, EventTimeout, string(priorJob), string(job.Phase), requestedAt)}
	var updatedRun *RunState
	var effects []Effect
	if run != nil {
		value := *run
		priorRun := value.Phase
		value.Phase = RunPhaseStopping
		value.Revision++
		value.StopRequestedAt = timePointer(requestedAt)
		value.StopReason = StopReasonTimeout
		updatedRun = &value
		events = append(events, runEvent(value, EventTimeout, string(priorRun), string(value.Phase), requestedAt))
		if value.Process != nil {
			effects = []Effect{{Type: EffectStopTarget}}
		}
	}
	if err := job.Validate(); err != nil {
		return TransitionResult{}, err
	}
	if updatedRun != nil {
		if err := updatedRun.Validate(); err != nil {
			return TransitionResult{}, err
		}
	}

	return TransitionResult{Job: job, Run: updatedRun, Events: events, Effects: effects}, nil
}

// RequestRunTimeout records a per-run timeout without converting it into a
// whole-job timeout. Completion policy may therefore retry the timed-out run.
func RequestRunTimeout(job JobState, run RunState, requestedAt time.Time) (TransitionResult, error) {
	if job.Phase == JobPhaseCompleted || run.Phase == RunPhaseCompleted {
		if err := job.Validate(); err != nil {
			return TransitionResult{}, fmt.Errorf("validate job: %w", err)
		}
		if err := run.Validate(); err != nil {
			return TransitionResult{}, fmt.Errorf("validate run: %w", err)
		}
		if run.JobID != job.ID {
			return TransitionResult{}, invalid("job/run relationship", "run must belong to the job")
		}

		return unchanged(job, &run), nil
	}
	if err := validateJobRunPair(job, run); err != nil {
		return TransitionResult{}, err
	}
	if run.StopRequestedAt != nil && run.StopReason == StopReasonTimeout {
		return unchanged(job, &run), nil
	}
	if !activeJobPhase(job.Phase) || !activeRunPhase(run.Phase) {
		return TransitionResult{}, pairConflict(job, run, "request run timeout", JobPhaseRunning, RunPhaseRunning)
	}
	requestedAt = normalizeTime(requestedAt)
	priorJob := job.Phase
	priorRun := run.Phase
	job.Phase = JobPhaseStopping
	job.Revision++
	run.Phase = RunPhaseStopping
	run.Revision++
	run.StopRequestedAt = timePointer(requestedAt)
	run.StopReason = StopReasonTimeout
	if err := job.Validate(); err != nil {
		return TransitionResult{}, err
	}
	if err := run.Validate(); err != nil {
		return TransitionResult{}, err
	}
	effects := []Effect{}
	if run.Process != nil {
		effects = append(effects, Effect{Type: EffectStopTarget})
	}

	return TransitionResult{
		Job: job,
		Run: &run,
		Events: []EventDraft{
			jobEventWithDetails(job, EventTimeout, string(priorJob), string(job.Phase), requestedAt,
				reasonDetails("run_timeout")),
			runEventWithDetails(run, EventTimeout, string(priorRun), string(run.Phase), requestedAt,
				reasonDetails("run_timeout")),
		},
		Effects: effects,
	}, nil
}

// CompleteWithoutRun finalizes a waiting, queued, backoff, or pre-reservation
// job. It is used for unsatisfied dependencies, exhausted waits, and job-level
// timeouts where no active invocation exists.
func CompleteWithoutRun(
	job JobState,
	outcome JobOutcome,
	diagnosticCode string,
	completedAt time.Time,
) (TransitionResult, error) {
	if err := job.Validate(); err != nil {
		return TransitionResult{}, err
	}
	if !outcome.Valid() || outcome == JobOutcomeSuccess {
		return TransitionResult{}, invalid("job outcome", "must be a supported non-success terminal outcome")
	}
	if job.ActiveRunID != "" || job.Phase == JobPhaseSubmitting || job.Phase == JobPhaseCompleted {
		return TransitionResult{}, jobConflictValue(job, "complete without run", string(job.Phase))
	}
	completedAt = normalizeTime(completedAt)
	prior := job.Phase
	job.Phase = JobPhaseCompleted
	job.Outcome = outcome
	job.CompletedAt = timePointer(completedAt)
	job.Revision++
	job.LastDiagnosticCode = diagnosticCode
	if err := job.Validate(); err != nil {
		return TransitionResult{}, err
	}
	eventType := EventJobAborted
	if outcome == JobOutcomeTimedOut {
		eventType = EventTimeout
	}

	return TransitionResult{
		Job: job,
		Events: []EventDraft{jobEventWithDetails(
			job,
			eventType,
			string(prior),
			string(job.Phase),
			completedAt,
			diagnosticDetails(diagnosticCode),
		)},
	}, nil
}

// PauseJob records a best-effort pause. The caller must perform any process
// suspension effect only after this transition commits.
func PauseJob(job JobState, run *RunState, pausedAt time.Time) (TransitionResult, JobPhase, error) {
	if err := job.Validate(); err != nil {
		return TransitionResult{}, "", err
	}
	if job.Phase == JobPhasePaused {
		return unchanged(job, run), JobPhasePaused, nil
	}
	if job.Phase != JobPhaseWaiting && job.Phase != JobPhaseQueued && job.Phase != JobPhaseBackoff &&
		job.Phase != JobPhaseRunning {
		return TransitionResult{}, "", jobConflictValue(job, "pause", string(job.Phase))
	}
	if run != nil {
		if err := validateJobRunPair(job, *run); err != nil {
			return TransitionResult{}, "", err
		}
	}
	pausedAt = normalizeTime(pausedAt)
	priorJob := job.Phase
	job.Phase = JobPhasePaused
	job.Revision++
	events := []EventDraft{jobEvent(job, EventJobPaused, string(priorJob), string(job.Phase), pausedAt)}
	var updatedRun *RunState
	var effects []Effect
	if run != nil && run.Process != nil {
		value := *run
		priorRun := value.Phase
		value.Phase = RunPhasePaused
		value.Revision++
		updatedRun = &value
		events = append(events, runEvent(value, EventJobPaused, string(priorRun), string(value.Phase), pausedAt))
		effects = []Effect{{Type: EffectPauseTarget}}
	}
	if err := job.Validate(); err != nil {
		return TransitionResult{}, "", err
	}
	if updatedRun != nil {
		if err := updatedRun.Validate(); err != nil {
			return TransitionResult{}, "", err
		}
	}

	return TransitionResult{Job: job, Run: updatedRun, Events: events, Effects: effects}, priorJob, nil
}

// ResumeJob restores a paused job to its recorded prior phase.
func ResumeJob(job JobState, run *RunState, prior JobPhase, resumedAt time.Time) (TransitionResult, error) {
	if err := job.Validate(); err != nil {
		return TransitionResult{}, err
	}
	if job.Phase != JobPhasePaused {
		return TransitionResult{}, jobConflict(job, "resume", JobPhasePaused)
	}
	if prior == JobPhasePaused || !prior.Valid() || prior == JobPhaseCompleted || prior == JobPhaseSubmitting {
		return TransitionResult{}, invalid("resume phase", "is not resumable")
	}
	resumedAt = normalizeTime(resumedAt)
	job.Phase = prior
	job.Revision++
	events := []EventDraft{jobEvent(job, EventJobResumed, string(JobPhasePaused), string(prior), resumedAt)}
	var updatedRun *RunState
	var effects []Effect
	if run != nil {
		if err := validateJobRunPair(job, *run); err != nil {
			// validateJobRunPair sees the restored job and the still-paused run;
			// their identity relationship remains the invariant we need here.
			return TransitionResult{}, err
		}
		value := *run
		if value.Phase != RunPhasePaused {
			return TransitionResult{}, runConflict(value, "resume", RunPhasePaused)
		}
		value.Phase = RunPhaseRunning
		value.Revision++
		updatedRun = &value
		events = append(events, runEvent(value, EventJobResumed, string(RunPhasePaused), string(value.Phase), resumedAt))
		effects = []Effect{{Type: EffectResumeTarget}}
	}
	if err := job.Validate(); err != nil {
		return TransitionResult{}, err
	}

	return TransitionResult{Job: job, Run: updatedRun, Events: events, Effects: effects}, nil
}

func retryDetails(disposition RunDisposition, diagnosticCode string) json.RawMessage {
	value := struct {
		NextRunAt      *time.Time        `json:"next_run_at,omitempty"`
		Reason         string            `json:"reason,omitempty"`
		DiagnosticCode string            `json:"diagnostic_code,omitempty"`
		Diagnostic     *DiagnosticRecord `json:"diagnostic,omitempty"`
	}{NextRunAt: disposition.NextRunAt, Reason: disposition.Reason, DiagnosticCode: diagnosticCode}
	if diagnosticCode != "" {
		record, err := NewDiagnosticRecord(DiagnosticCode(diagnosticCode), nil)
		if err == nil {
			value.Diagnostic = &record
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("encode retry details: %v", err))
	}

	return encoded
}

func reasonDetails(reason string) json.RawMessage {
	if reason == "" {
		return nil
	}
	encoded, err := json.Marshal(struct {
		Reason string `json:"reason"`
	}{Reason: reason})
	if err != nil {
		panic(fmt.Sprintf("encode phase reason: %v", err))
	}

	return encoded
}
