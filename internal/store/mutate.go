package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ryancswallace/jobman/internal/model"
)

// Submit validates and atomically inserts one immutable submitted job plus its
// initial transition event.
func (s *Store) Submit(
	ctx context.Context,
	id model.JobID,
	specification model.JobSpec,
	credentialHash model.CredentialHash,
	submittedAt time.Time,
	claimDeadline time.Time,
) (model.TransitionResult, error) {
	return s.SubmitWithDependencies(
		ctx, id, specification, credentialHash, submittedAt, claimDeadline, nil,
	)
}

// SubmitWithDependencies atomically inserts the job, immutable dependency
// edges, runtime row, and initial transition event. A caller can never observe
// a submitted job missing its prerequisites.
func (s *Store) SubmitWithDependencies(
	ctx context.Context,
	id model.JobID,
	specification model.JobSpec,
	credentialHash model.CredentialHash,
	submittedAt time.Time,
	claimDeadline time.Time,
	dependencies []Dependency,
) (model.TransitionResult, error) {
	result, transitionErr := model.NewSubmittedJob(id, specification, credentialHash, submittedAt, claimDeadline)
	if transitionErr != nil {
		return model.TransitionResult{}, transitionErr
	}
	if validationErr := validateTransition(result); validationErr != nil {
		return model.TransitionResult{}, validationErr
	}
	events, eventErr := s.completeEvents(result.Events)
	if eventErr != nil {
		return model.TransitionResult{}, eventErr
	}
	if writeErr := s.writeTransaction(ctx, "submit job with dependencies", func(tx *sql.Tx) error {
		if err := applyJobTransition(ctx, tx, result, true); err != nil {
			return err
		}
		if err := insertDependencies(ctx, tx, id, dependencies); err != nil {
			return err
		}

		return insertEvents(ctx, tx, events)
	}); writeErr != nil {
		return model.TransitionResult{}, writeErr
	}

	return result, nil
}

// Claim atomically transfers an unexpired submitted job to one supervisor.
func (s *Store) Claim(
	ctx context.Context,
	jobID model.JobID,
	credential []byte,
	supervisorID model.SupervisorID,
	identity model.ProcessIdentity,
	claimedAt time.Time,
	leaseExpiresAt time.Time,
) (model.TransitionResult, error) {
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	result, err := model.ClaimJob(job, credential, supervisorID, identity, claimedAt, leaseExpiresAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.commitTransition(ctx, result); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// ReserveRun allocates a durable run number and log locations.
func (s *Store) ReserveRun(
	ctx context.Context,
	jobID model.JobID,
	runID model.RunID,
	runNumber uint64,
	logs model.LogMetadata,
	reservedAt time.Time,
) (model.TransitionResult, error) {
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	result, err := model.ReserveRun(job, runID, runNumber, logs, reservedAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.commitTransition(ctx, result); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// MarkProcessStarted atomically publishes a verified target identity.
func (s *Store) MarkProcessStarted(
	ctx context.Context,
	jobID model.JobID,
	runID model.RunID,
	resolvedExecutable string,
	identity model.ProcessIdentity,
	startedAt time.Time,
) (model.TransitionResult, error) {
	job, run, err := s.getJobRun(ctx, jobID, runID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	result, err := model.MarkProcessStarted(job, run, resolvedExecutable, identity, startedAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.commitTransition(ctx, result); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// MarkStartFailed records a failed process start and terminal one-run job.
func (s *Store) MarkStartFailed(
	ctx context.Context,
	jobID model.JobID,
	runID model.RunID,
	logs model.LogMetadata,
	diagnosticCode string,
	failedAt time.Time,
) (model.TransitionResult, error) {
	s.supervisorLeaseMu.Lock()
	defer s.supervisorLeaseMu.Unlock()

	job, run, err := s.getJobRun(ctx, jobID, runID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	result, err := model.MarkStartFailed(job, run, logs, diagnosticCode, failedAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.attachSupervisorRelease(ctx, &result, failedAt); err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.commitTransition(ctx, result); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// FinalizeRun records one proven target result and terminal job outcome.
func (s *Store) FinalizeRun(
	ctx context.Context,
	jobID model.JobID,
	runID model.RunID,
	outcome model.RunOutcome,
	exit *model.ExitInfo,
	logs model.LogMetadata,
	completedAt time.Time,
) (model.TransitionResult, error) {
	s.supervisorLeaseMu.Lock()
	defer s.supervisorLeaseMu.Unlock()

	job, run, err := s.getJobRun(ctx, jobID, runID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	result, err := model.FinalizeRun(job, run, outcome, exit, logs, completedAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.attachSupervisorRelease(ctx, &result, completedAt); err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.commitTransition(ctx, result); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// RequestCancellation records durable cancellation intent before returning a
// stop effect to the caller.
func (s *Store) RequestCancellation(
	ctx context.Context,
	jobID model.JobID,
	requestedAt time.Time,
) (model.TransitionResult, error) {
	// Startup and completion may advance the snapshot before its CAS commits.
	// Recompute intent and effects, but leave model and other storage failures
	// intact. Bound contention independently of the caller's deadline.
	var err error
	for range 8 {
		if contextErr := ctx.Err(); contextErr != nil {
			return model.TransitionResult{}, contextErr
		}
		var result model.TransitionResult
		result, err = s.tryCancellation(ctx, jobID, requestedAt)
		var conflict *RevisionConflictError
		if !errors.As(err, &conflict) {
			return result, err
		}
	}
	return model.TransitionResult{}, err
}

func (s *Store) tryCancellation(
	ctx context.Context,
	jobID model.JobID,
	requestedAt time.Time,
) (model.TransitionResult, error) {
	job, run, err := s.cancellationSnapshot(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	result, err := model.RequestCancellation(job, run, requestedAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if len(result.Events) == 0 {
		return result, nil
	}
	if err := s.commitTransition(ctx, result); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

func (s *Store) cancellationSnapshot(
	ctx context.Context,
	jobID model.JobID,
) (job model.JobState, run *model.RunState, err error) {
	// A transition updates the job and run together. Observe them together too,
	// so a mixed snapshot cannot masquerade as a genuine model conflict.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return model.JobState{}, nil, fmt.Errorf("begin cancellation snapshot: %w", classifySQLite("begin cancellation snapshot", err))
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rollback cancellation snapshot: %w", rollbackErr))
		}
	}()
	job, err = getJobWithQueryer(ctx, tx, jobID)
	if err != nil {
		return model.JobState{}, nil, err
	}
	if job.ActiveRunID != "" {
		value, getErr := getRunWithQueryer(ctx, tx, job.ActiveRunID)
		if getErr != nil {
			return model.JobState{}, nil, getErr
		}
		run = &value
	}
	if err := tx.Commit(); err != nil {
		return model.JobState{}, nil, fmt.Errorf("commit cancellation snapshot: %w", err)
	}
	return job, run, nil
}

// FinalizeCancellationWithoutRun completes cancellation accepted after claim
// but before any run was reserved and releases the supervisor atomically.
func (s *Store) FinalizeCancellationWithoutRun(
	ctx context.Context,
	jobID model.JobID,
	completedAt time.Time,
) (model.TransitionResult, error) {
	s.supervisorLeaseMu.Lock()
	defer s.supervisorLeaseMu.Unlock()

	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	result, err := model.FinalizeCancellationWithoutRun(job, completedAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.attachSupervisorRelease(ctx, &result, completedAt); err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.commitTransition(ctx, result); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// MarkSubmissionFailed finalizes an unclaimed expired submission.
func (s *Store) MarkSubmissionFailed(
	ctx context.Context,
	jobID model.JobID,
	diagnosticCode string,
	failedAt time.Time,
) (model.TransitionResult, error) {
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	result, err := model.MarkSubmissionFailed(job, diagnosticCode, failedAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.commitTransition(ctx, result); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// MarkOwnershipLost records uncertainty without fabricating process outcome.
func (s *Store) MarkOwnershipLost(
	ctx context.Context,
	jobID model.JobID,
	logs *model.LogMetadata,
	diagnosticCode string,
	lostAt time.Time,
) (model.TransitionResult, error) {
	s.supervisorLeaseMu.Lock()
	defer s.supervisorLeaseMu.Unlock()

	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return model.TransitionResult{}, err
	}
	var run *model.RunState
	if job.ActiveRunID != "" {
		value, getErr := s.GetRun(ctx, job.ActiveRunID)
		if getErr != nil {
			return model.TransitionResult{}, getErr
		}
		run = &value
	}
	result, err := model.MarkOwnershipLost(job, run, logs, diagnosticCode, lostAt)
	if err != nil {
		return model.TransitionResult{}, err
	}
	if err := s.attachSupervisorRelease(ctx, &result, lostAt); err != nil {
		return model.TransitionResult{}, err
	}
	runIncrement := 0
	if result.Run != nil {
		runIncrement = 1
	}
	if err := s.commitTransitionWithRuntime(ctx, result, func(tx *sql.Tx) error {
		update, updateErr := tx.ExecContext(ctx, `
			UPDATE job_runtime
			SET revision = revision + 1,
			    run_count = run_count + ?,
			    failure_count = failure_count + ?,
			    next_run_at_ns = NULL,
			    waiting_reason = ?,
			    updated_at_ns = ?
			WHERE job_id = ?`,
			runIncrement,
			runIncrement,
			nullableString(diagnosticCode),
			lostAt.UTC().UnixNano(),
			jobID.String(),
		)
		if updateErr != nil {
			return updateErr
		}
		if _, releaseErr := tx.ExecContext(ctx, `
			UPDATE admissions SET released_at_ns = COALESCE(released_at_ns, ?)
			WHERE job_id = ?`, lostAt.UTC().UnixNano(), jobID.String()); releaseErr != nil {
			return releaseErr
		}
		if _, deleteErr := tx.ExecContext(ctx, `
			DELETE FROM admission_requests WHERE job_id = ?`, jobID.String()); deleteErr != nil {
			return deleteErr
		}

		return requireOneUpdate(update, "job runtime", jobID.String(), 0, "")
	}); err != nil {
		return model.TransitionResult{}, err
	}

	return result, nil
}

// RenewLease advances one live supervisor lease by optimistic revision.
func (s *Store) RenewLease(
	ctx context.Context,
	supervisorID model.SupervisorID,
	renewedAt time.Time,
	expiresAt time.Time,
) (model.SupervisorState, error) {
	s.supervisorLeaseMu.Lock()
	defer s.supervisorLeaseMu.Unlock()

	current, err := s.GetSupervisor(ctx, supervisorID)
	if err != nil {
		return model.SupervisorState{}, err
	}
	updated, draft, err := model.RenewSupervisorLease(current, renewedAt, expiresAt)
	if err != nil {
		return model.SupervisorState{}, err
	}
	if err := s.commitSupervisor(ctx, updated, draft); err != nil {
		return model.SupervisorState{}, err
	}

	return updated, nil
}

// ReleaseSupervisor records that a supervisor no longer owns active work.
// Terminal job operations already release the linked supervisor atomically, so
// callers do not need a second release after FinalizeRun, MarkStartFailed, or
// MarkOwnershipLost. Repeating release is idempotent.
func (s *Store) ReleaseSupervisor(
	ctx context.Context,
	supervisorID model.SupervisorID,
	releasedAt time.Time,
) (model.SupervisorState, error) {
	current, err := s.GetSupervisor(ctx, supervisorID)
	if err != nil {
		return model.SupervisorState{}, err
	}
	updated, draft, err := model.ReleaseSupervisor(current, releasedAt)
	if err != nil {
		return model.SupervisorState{}, err
	}
	if draft.Type == "" {
		return updated, nil
	}
	if err := s.commitSupervisor(ctx, updated, draft); err != nil {
		return model.SupervisorState{}, err
	}

	return updated, nil
}

func (s *Store) getJobRun(
	ctx context.Context,
	jobID model.JobID,
	runID model.RunID,
) (model.JobState, model.RunState, error) {
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return model.JobState{}, model.RunState{}, err
	}
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return model.JobState{}, model.RunState{}, err
	}

	return job, run, nil
}

func (s *Store) attachSupervisorRelease(
	ctx context.Context,
	result *model.TransitionResult,
	releasedAt time.Time,
) error {
	if result.Job.SupervisorID == "" {
		return nil
	}
	current, err := s.GetSupervisor(ctx, result.Job.SupervisorID)
	if err != nil {
		return fmt.Errorf("load terminal job supervisor: %w", err)
	}
	updated, event, err := model.ReleaseSupervisor(current, releasedAt)
	if err != nil {
		return fmt.Errorf("release terminal job supervisor: %w", err)
	}
	result.Supervisor = &updated
	if event.Type != "" {
		result.Events = append(result.Events, event)
	}

	return nil
}

func (s *Store) commitTransition(ctx context.Context, result model.TransitionResult) error {
	if err := validateTransition(result); err != nil {
		return err
	}
	events, err := s.completeEvents(result.Events)
	if err != nil {
		return err
	}

	return s.writeTransaction(ctx, "state transition", func(tx *sql.Tx) error {
		if err := applyJobTransition(ctx, tx, result, false); err != nil {
			return err
		}
		if err := applyRunTransition(ctx, tx, result); err != nil {
			return err
		}
		if err := applySupervisorTransition(ctx, tx, result); err != nil {
			return err
		}

		return insertEvents(ctx, tx, events)
	})
}

func validateTransition(result model.TransitionResult) error {
	if err := result.Job.Validate(); err != nil {
		return fmt.Errorf("commit transition: validate job: %w", err)
	}
	if result.Run != nil {
		if err := result.Run.Validate(); err != nil {
			return fmt.Errorf("commit transition: validate run: %w", err)
		}
	}
	if result.Supervisor != nil {
		if err := result.Supervisor.Validate(); err != nil {
			return fmt.Errorf("commit transition: validate supervisor: %w", err)
		}
	}

	return nil
}

func applyJobTransition(ctx context.Context, tx *sql.Tx, result model.TransitionResult, create bool) error {
	if create {
		if err := insertJob(ctx, tx, result.Job); err != nil {
			return err
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO job_runtime(job_id, updated_at_ns) VALUES (?, ?)`,
			result.Job.ID.String(),
			result.Job.SubmittedAt.UnixNano(),
		); err != nil {
			return fmt.Errorf("insert job runtime %s: %w", result.Job.ID, classifySQLite("insert job runtime", err))
		}

		return nil
	}
	event, ok := eventForEntity(result.Events, model.EntityJob)
	if !ok {
		return nil
	}

	return updateJob(ctx, tx, result.Job, event)
}

func applyRunTransition(ctx context.Context, tx *sql.Tx, result model.TransitionResult) error {
	if result.Run == nil {
		return nil
	}
	event, ok := eventForEntity(result.Events, model.EntityRun)
	if !ok {
		return nil
	}
	if event.FromPhase == "" {
		return insertRun(ctx, tx, *result.Run)
	}

	return updateRun(ctx, tx, *result.Run, event)
}

func applySupervisorTransition(ctx context.Context, tx *sql.Tx, result model.TransitionResult) error {
	if result.Supervisor == nil {
		return nil
	}
	event, ok := eventForEntity(result.Events, model.EntitySupervisor)
	if !ok {
		return nil
	}
	if event.FromPhase == "" {
		return insertSupervisor(ctx, tx, *result.Supervisor)
	}

	return updateSupervisor(ctx, tx, *result.Supervisor, event)
}

func (s *Store) commitSupervisor(
	ctx context.Context,
	state model.SupervisorState,
	draft model.EventDraft,
) error {
	events, err := s.completeEvents([]model.EventDraft{draft})
	if err != nil {
		return err
	}
	return s.writeTransaction(ctx, "supervisor transition", func(tx *sql.Tx) error {
		if err := updateSupervisor(ctx, tx, state, draft); err != nil {
			return err
		}

		return insertEvents(ctx, tx, events)
	})
}

func (s *Store) completeEvents(drafts []model.EventDraft) ([]model.StateEvent, error) {
	events := make([]model.StateEvent, 0, len(drafts))
	for _, draft := range drafts {
		id, err := s.eventIDs.NewEventID()
		if err != nil {
			return nil, fmt.Errorf("generate state event ID: %w", err)
		}
		event, err := draft.WithID(id)
		if err != nil {
			return nil, fmt.Errorf("complete state event: %w", err)
		}
		events = append(events, event)
	}

	return events, nil
}

func eventForEntity(events []model.EventDraft, entity model.EntityKind) (model.EventDraft, bool) {
	for _, event := range events {
		if event.Entity == entity {
			return event, true
		}
	}

	return model.EventDraft{}, false
}

func insertJob(ctx context.Context, tx *sql.Tx, state model.JobState) error {
	values, err := jobValues(state)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO jobs (
		id, name, spec_json, phase, outcome, revision,
		submitted_at_ns, claimed_at_ns, started_at_ns, completed_at_ns,
		active_run_id, supervisor_id,
		cancellation_requested_at_ns, cancellation_reason,
		last_diagnostic_code, launch_credential_hash, claim_deadline_ns
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, values...)
	if err != nil {
		return fmt.Errorf("insert job %s: %w", state.ID, classifySQLite("insert job", err))
	}

	return nil
}

func updateJob(ctx context.Context, tx *sql.Tx, state model.JobState, event model.EventDraft) error {
	values, err := jobValues(state)
	if err != nil {
		return err
	}
	expectedRevision := state.Revision - 1
	values = append(values, state.ID.String(), expectedRevision, event.FromPhase)
	result, err := tx.ExecContext(ctx, `UPDATE jobs SET
		id = ?, name = ?, spec_json = ?, phase = ?, outcome = ?, revision = ?,
		submitted_at_ns = ?, claimed_at_ns = ?, started_at_ns = ?, completed_at_ns = ?,
		active_run_id = ?, supervisor_id = ?,
		cancellation_requested_at_ns = ?, cancellation_reason = ?,
		last_diagnostic_code = ?, launch_credential_hash = ?, claim_deadline_ns = ?
	WHERE id = ? AND revision = ? AND phase = ?`, values...)
	if err != nil {
		return fmt.Errorf("update job %s: %w", state.ID, classifySQLite("update job", err))
	}

	return requireOneUpdate(result, "job", state.ID.String(), expectedRevision, event.FromPhase)
}

func jobValues(state model.JobState) ([]any, error) {
	specificationJSON, err := state.Spec.CanonicalJSON()
	if err != nil {
		return nil, fmt.Errorf("encode job specification: %w", err)
	}
	revision, err := databaseUint("job revision", state.Revision)
	if err != nil {
		return nil, err
	}
	var credential any
	if !state.LaunchCredentialHash.Empty() {
		credential = state.LaunchCredentialHash.Bytes()
	}

	return []any{
		state.ID.String(),
		nullableString(state.Spec.Name()),
		string(specificationJSON),
		string(state.Phase),
		nullableString(string(state.Outcome)),
		revision,
		state.SubmittedAt.UnixNano(),
		nullableTime(state.ClaimedAt),
		nullableTime(state.StartedAt),
		nullableTime(state.CompletedAt),
		nullableString(state.ActiveRunID.String()),
		nullableString(state.SupervisorID.String()),
		cancellationTime(state.Cancellation),
		cancellationReason(state.Cancellation),
		nullableString(state.LastDiagnosticCode),
		credential,
		nullableTime(state.ClaimDeadline),
	}, nil
}

func insertRun(ctx context.Context, tx *sql.Tx, state model.RunState) error {
	values, err := runValues(state)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO runs (
		id, job_id, run_number, phase, outcome, revision, resolved_executable,
		reserved_at_ns, started_at_ns, stop_requested_at_ns, stop_reason, completed_at_ns,
		process_pid, process_identity_json,
		exit_code, exit_signal, exit_platform_reason, exit_observed_at_ns,
		stdout_path, stderr_path, index_path, stdout_size, stderr_size,
		log_index_version, log_integrity, recording_health, log_diagnostic_code, last_diagnostic_code
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, values...)
	if err != nil {
		return fmt.Errorf("insert run %s: %w", state.ID, classifySQLite("insert run", err))
	}

	return nil
}

func updateRun(ctx context.Context, tx *sql.Tx, state model.RunState, event model.EventDraft) error {
	values, err := runValues(state)
	if err != nil {
		return err
	}
	expectedRevision := state.Revision - 1
	values = append(values, state.ID.String(), expectedRevision, event.FromPhase)
	result, err := tx.ExecContext(ctx, `UPDATE runs SET
		id = ?, job_id = ?, run_number = ?, phase = ?, outcome = ?, revision = ?, resolved_executable = ?,
		reserved_at_ns = ?, started_at_ns = ?, stop_requested_at_ns = ?, stop_reason = ?, completed_at_ns = ?,
		process_pid = ?, process_identity_json = ?,
		exit_code = ?, exit_signal = ?, exit_platform_reason = ?, exit_observed_at_ns = ?,
		stdout_path = ?, stderr_path = ?, index_path = ?, stdout_size = ?, stderr_size = ?,
		log_index_version = ?, log_integrity = ?, recording_health = ?, log_diagnostic_code = ?,
		last_diagnostic_code = ?
	WHERE id = ? AND revision = ? AND phase = ?`, values...)
	if err != nil {
		return fmt.Errorf("update run %s: %w", state.ID, classifySQLite("update run", err))
	}

	return requireOneUpdate(result, "run", state.ID.String(), expectedRevision, event.FromPhase)
}

func runValues(state model.RunState) ([]any, error) {
	runNumber, err := databaseUint("run number", state.Number)
	if err != nil {
		return nil, err
	}
	revision, err := databaseUint("run revision", state.Revision)
	if err != nil {
		return nil, err
	}
	var processPID any
	var processJSON any
	if state.Process != nil {
		processPID = state.Process.PID
		encoded, encodeErr := jsonValue(state.Process)
		if encodeErr != nil {
			return nil, encodeErr
		}
		processJSON = string(encoded)
	}
	var exitCode any
	var exitSignal any
	var exitPlatformReason any
	var exitObservedAt any
	if state.Exit != nil {
		if state.Exit.ExitCode != nil {
			exitCode = *state.Exit.ExitCode
		}
		exitSignal = nullableString(state.Exit.Signal)
		exitPlatformReason = nullableString(state.Exit.PlatformReason)
		exitObservedAt = state.Exit.ObservedAt.UnixNano()
	}

	return []any{
		state.ID.String(),
		state.JobID.String(),
		runNumber,
		string(state.Phase),
		nullableString(string(state.Outcome)),
		revision,
		nullableString(state.ResolvedExecutable),
		state.ReservedAt.UnixNano(),
		nullableTime(state.StartedAt),
		nullableTime(state.StopRequestedAt),
		nullableString(string(state.StopReason)),
		nullableTime(state.CompletedAt),
		processPID,
		processJSON,
		exitCode,
		exitSignal,
		exitPlatformReason,
		exitObservedAt,
		state.Logs.StdoutPath,
		state.Logs.StderrPath,
		state.Logs.IndexPath,
		state.Logs.StdoutSize,
		state.Logs.StderrSize,
		state.Logs.IndexVersion,
		string(state.Logs.Integrity),
		string(state.Logs.RecordingHealth),
		nullableString(state.Logs.DiagnosticCode),
		nullableString(state.LastDiagnosticCode),
	}, nil
}

func insertSupervisor(ctx context.Context, tx *sql.Tx, state model.SupervisorState) error {
	values, err := supervisorValues(state)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO supervisors (
		id, job_id, revision, process_pid, process_identity_json,
		claimed_at_ns, lease_renewed_at_ns, lease_expires_at_ns, released_at_ns
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, values...)
	if err != nil {
		return fmt.Errorf("insert supervisor %s: %w", state.ID, classifySQLite("insert supervisor", err))
	}

	return nil
}

func updateSupervisor(
	ctx context.Context,
	tx *sql.Tx,
	state model.SupervisorState,
	event model.EventDraft,
) error {
	values, err := supervisorValues(state)
	if err != nil {
		return err
	}
	expectedRevision := state.Revision - 1
	values = append(values, state.ID.String(), expectedRevision)
	result, err := tx.ExecContext(ctx, `UPDATE supervisors SET
		id = ?, job_id = ?, revision = ?, process_pid = ?, process_identity_json = ?,
		claimed_at_ns = ?, lease_renewed_at_ns = ?, lease_expires_at_ns = ?, released_at_ns = ?
	WHERE id = ? AND revision = ?`, values...)
	if err != nil {
		return fmt.Errorf("update supervisor %s: %w", state.ID, classifySQLite("update supervisor", err))
	}

	return requireOneUpdate(result, "supervisor", state.ID.String(), expectedRevision, event.FromPhase)
}

func supervisorValues(state model.SupervisorState) ([]any, error) {
	revision, err := databaseUint("supervisor revision", state.Revision)
	if err != nil {
		return nil, err
	}
	processJSON, err := jsonValue(state.Process)
	if err != nil {
		return nil, err
	}

	return []any{
		state.ID.String(),
		state.JobID.String(),
		revision,
		state.Process.PID,
		string(processJSON),
		state.ClaimedAt.UnixNano(),
		state.LeaseRenewedAt.UnixNano(),
		state.LeaseExpiresAt.UnixNano(),
		nullableTime(state.ReleasedAt),
	}, nil
}

func insertEvents(ctx context.Context, tx *sql.Tx, events []model.StateEvent) error {
	for _, event := range events {
		details := event.Details
		if len(details) == 0 {
			details = json.RawMessage(`{}`)
		}
		revision, err := databaseUint("event entity revision", event.Revision)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO state_events (
			id, job_id, run_id, supervisor_id, entity_kind, entity_id,
			event_type, from_phase, to_phase, from_outcome, to_outcome,
			entity_revision, occurred_at_ns, details_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			event.ID.String(),
			event.JobID.String(),
			nullableString(event.RunID.String()),
			nullableString(event.SupervisorID.String()),
			string(event.Entity),
			event.EntityID,
			string(event.Type),
			nullableString(event.FromPhase),
			event.ToPhase,
			nullableString(event.FromOutcome),
			nullableString(event.ToOutcome),
			revision,
			event.OccurredAt.UnixNano(),
			string(details),
		)
		if err != nil {
			return fmt.Errorf("insert state event %s: %w", event.ID, classifySQLite("insert state event", err))
		}
	}

	return queueNotificationsForStateEvents(ctx, tx, events)
}

func requireOneUpdate(
	result sql.Result,
	entity string,
	id string,
	expectedRevision uint64,
	expectedPhase string,
) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read %s update result: %w", entity, err)
	}
	if rows != 1 {
		return &RevisionConflictError{
			Entity:           entity,
			ID:               id,
			ExpectedRevision: expectedRevision,
			ExpectedPhase:    expectedPhase,
		}
	}

	return nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}

	return value
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}

	return value.UnixNano()
}

func cancellationTime(intent *model.CancellationIntent) any {
	if intent == nil {
		return nil
	}

	return intent.RequestedAt.UnixNano()
}

func cancellationReason(intent *model.CancellationIntent) any {
	if intent == nil {
		return nil
	}

	return string(intent.Reason)
}
